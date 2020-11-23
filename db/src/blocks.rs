//! Implements a block store.
//!
//! The references of a block is the set of `Cid`s that are contained within a block. A closure
//! is the recursive set of references, containing all `Cid`s that are reachable from a block.
//!
//! When aliasing a block the closure of the block is added to the bag containing the blocks
//! reachable from all aliases. A bag is a set where each value can be inserted multiple times.
use crate::id::{Id, Ids, LiveSet};
use async_std::sync::Mutex;
use fnv::FnvHashSet;
use futures::future::Future;
use futures::stream::Stream;
use ipfs_embed_core::{Block, Cid, Error, Result, StorageEvent, StoreParams};
use libipld::codec::Decode;
use libipld::error::BlockNotFound;
use libipld::ipld::Ipld;
use sled::transaction::TransactionError;
use sled::{IVec, Transactional, Tree};
use std::convert::TryFrom;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;

fn map_tx_error(e: TransactionError<Error>) -> Error {
    match e {
        TransactionError::Abort(e) => e,
        TransactionError::Storage(e) => e.into(),
    }
}

#[derive(Debug, Error)]
#[error("Id {0:?} not found.")]
pub struct IdNotFound(Id);

/// Implements a reference counted lru cache.
#[derive(Clone)]
pub struct Blocks<S: StoreParams> {
    _marker: PhantomData<S>,
    // cid -> id
    lookup: Tree,
    // id -> cid
    cid: Tree,
    // id -> data
    data: Tree,
    // id -> ids
    refs: Tree,
    // id -> atime
    atime: Tree,
    // atime -> id
    lru: Tree,
}

impl<S: StoreParams> Blocks<S>
where
    Ipld: Decode<S::Codecs>,
{
    /// Opens the store.
    pub fn open(db: &sled::Db) -> Result<Self> {
        Ok(Self {
            _marker: PhantomData,
            lookup: db.open_tree("lookup")?,
            cid: db.open_tree("cid")?,
            data: db.open_tree("data")?,
            refs: db.open_tree("refs")?,
            atime: db.open_tree("atime")?,
            lru: db.open_tree("lru")?,
        })
    }

    /// Checks if the store contains an `Id`.
    pub fn contains_id(&self, id: &Id) -> Result<bool> {
        Ok(self.cid.contains_key(id)?)
    }

    /// Checks if the store contains a `Cid`.
    pub fn contains_cid(&self, cid: &Cid) -> Result<bool> {
        Ok(self.lookup.contains_key(&cid.to_bytes())?)
    }

    /// The number of blocks contained in the store.
    pub fn len(&self) -> usize {
        self.lookup.len()
    }

    /// Returns the `Id` of the block with a given `Cid`.
    pub fn lookup_id(&self, cid: &Cid) -> Result<Option<Id>> {
        Ok(self.lookup.get(&cid.to_bytes())?.map(From::from))
    }

    /// Returns the `Cid` of the block with a given `Id`.
    pub fn lookup_cid(&self, id: &Id) -> Result<Option<Cid>> {
        if let Some(bytes) = self.cid.get(id)? {
            Ok(Some(Cid::try_from(&bytes[..])?))
        } else {
            Ok(None)
        }
    }

    /// Returns the set of references of a block.
    pub fn refs(&self, id: &Id) -> Result<Ids> {
        if let Some(refs) = self.refs.get(id)?.map(From::from) {
            return Ok(refs);
        }
        let cid = self.lookup_cid(id)?.ok_or_else(|| IdNotFound(id.clone()))?;
        let data = self.data.get(id)?.ok_or_else(|| IdNotFound(id.clone()))?;
        let block = Block::<S>::new_unchecked(cid, data.to_vec());
        let cid_refs = block.ipld()?.references();
        let mut refs = Vec::with_capacity(cid_refs.len() * 8);
        for cid in &cid_refs {
            let id = self.lookup_id(cid)?.ok_or_else(|| BlockNotFound(*cid))?;
            refs.extend_from_slice(id.as_ref());
        }
        let ids = Ids::from(IVec::from(refs));
        self.refs.insert(id, &ids)?;
        Ok(ids)
    }

    /// Returns the recursive set of references of a block.
    pub fn closure(&self, id: Id) -> Result<Ids> {
        let mut refs = FnvHashSet::default();
        let mut todo = Vec::new();
        todo.push(id);
        while let Some(id) = todo.pop() {
            if refs.contains(&id) {
                continue;
            }
            todo.extend(self.refs(&id)?.iter());
            refs.insert(id);
        }
        Ok(Ids::from(&refs))
    }

    /// Returns an iterator of `Id`s sorted by least recently used.
    pub fn lru(&self) -> impl Iterator<Item = Result<Id>> {
        self.lru
            .iter()
            .values()
            .map(|v| v.map(Into::into).map_err(Into::into))
    }

    /// Returns the data of a block and increments the access time.
    pub fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        if let Some(id) = self.lookup_id(cid)? {
            if let Some(data) = self.data.get(&id)? {
                (&self.atime, &self.lru)
                    .transaction(|(tatime, tlru)| {
                        let atime: Id = tlru.generate_id()?.into();
                        if let Some(atime) = tatime.remove(&id)? {
                            tlru.remove(atime)?;
                        }
                        tlru.insert(&atime, &id)?;
                        tatime.insert(&id, &atime)?;
                        Ok(())
                    })
                    .map_err(map_tx_error)?;
                //log::trace!("hit {}", id);
                return Ok(Some(data.to_vec()));
            }
        }
        //log::trace!("miss {}", cid.to_string());
        Ok(None)
    }

    /// Inserts a block into the store.
    pub fn insert(&self, block: &Block<S>) -> Result<()> {
        let cid = IVec::from(block.cid().to_bytes());
        let data = block.data();
        let id = (&self.lookup, &self.cid, &self.data, &self.atime, &self.lru)
            .transaction(|(tlookup, tcid, tdata, tatime, tlru)| {
                if let Some(id) = tlookup.get(&cid)? {
                    return Ok(Id::from(id));
                }
                let id: Id = tlookup.generate_id()?.into();
                let atime: Id = tlru.generate_id()?.into();
                tlookup.insert(&cid, &id)?;
                tcid.insert(&id, &cid)?;
                tdata.insert(&id, data)?;
                tatime.insert(&id, &atime)?;
                tlru.insert(&atime, &id)?;
                Ok(id)
            })
            .map_err(map_tx_error)?;
        log::debug!("insert {}", id);
        Ok(())
    }

    /// Removes a block from the store.
    pub fn remove(&self, id: &Id) -> Result<()> {
        (
            &self.lookup,
            &self.cid,
            &self.data,
            &self.refs,
            &self.atime,
            &self.lru,
        )
            .transaction(|(tlookup, tcid, tdata, trefs, tatime, tlru)| {
                if let Some(cid) = tcid.remove(id)? {
                    tdata.remove(id)?;
                    tlookup.remove(&cid)?;
                    trefs.remove(id)?;
                    if let Some(atime) = tatime.remove(id)? {
                        tlru.remove(atime)?;
                    }
                }
                Ok(())
            })
            .map_err(map_tx_error)?;
        log::debug!("remove {}", id);
        Ok(())
    }

    /// Returns a subscription of store events.
    pub fn subscribe(&self) -> Subscription {
        let subscriber = self.lookup.watch_prefix([]);
        let keys = self.lookup.scan_prefix([]);
        Subscription {
            keys: Some(keys),
            subscriber,
        }
    }
}

pub struct Subscription {
    keys: Option<sled::Iter>,
    subscriber: sled::Subscriber,
}

impl Stream for Subscription {
    type Item = StorageEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(keys) = self.keys.as_mut() {
            if let Some(Ok((key, _))) = keys.next() {
                let cid = Cid::try_from(&key[..]).unwrap();
                let entry = StorageEvent::Insert(cid);
                return Poll::Ready(Some(entry));
            }
        }
        self.keys = None;
        match Pin::new(&mut self.subscriber).poll(cx) {
            Poll::Ready(Some(sled::Event::Insert { key, .. })) => {
                let cid = Cid::try_from(&key[..]).unwrap();
                let entry = StorageEvent::Insert(cid);
                Poll::Ready(Some(entry))
            }
            Poll::Ready(Some(sled::Event::Remove { key })) => {
                let cid = Cid::try_from(&key[..]).unwrap();
                let entry = StorageEvent::Remove(cid);
                Poll::Ready(Some(entry))
            }
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}

#[derive(Clone)]
pub struct Aliases<S: StoreParams> {
    blocks: Blocks<S>,
    // [u8] -> id
    alias: Tree,
    // live
    filter: Arc<Mutex<LiveSet>>,
    // id -> [u64]
    closure: Tree,
}

impl<S: StoreParams> Aliases<S>
where
    Ipld: Decode<S::Codecs>,
{
    /// Opens the store and initializes the live set.
    pub fn open(db: &sled::Db) -> Result<Self> {
        let blocks = Blocks::open(db)?;
        let alias = db.open_tree("alias")?;
        let closure = db.open_tree("closure")?;
        let mut filter = LiveSet::new();
        for res in alias.iter().values() {
            let id = res?;
            for id in Ids::from(closure.get(&id)?.unwrap()).iter() {
                filter.add(&id)?;
            }
        }
        Ok(Self {
            blocks,
            alias,
            closure,
            filter: Arc::new(Mutex::new(filter)),
        })
    }

    /// Checks if the store contains a block.
    pub fn contains(&self, cid: &Cid) -> Result<bool> {
        self.blocks.contains_cid(cid)
    }

    /// Returns the block from the store.
    pub fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        self.blocks.get(cid)
    }

    /// Inserts a block into the store.
    pub fn insert(&self, block: &Block<S>) -> Result<()> {
        self.blocks.insert(block)
    }

    /// Aliases a block.
    pub async fn alias(&self, alias: &[u8], cid: Option<&Cid>) -> Result<()> {
        let id = if let Some(cid) = cid {
            self.blocks.lookup_id(cid)?
        } else {
            None
        };
        let closure = if let Some(id) = id.as_ref() {
            if let Some(closure) = self.closure.get(id)? {
                Ids::from(closure)
            } else {
                self.blocks.closure(id.clone())?
            }
        } else {
            Default::default()
        };
        log::debug!("alias {:?} {:?}", alias, id.as_ref());

        let prev_id = self.alias.get(alias)?.map(Id::from);
        let prev_closure = if let Some(id) = prev_id.as_ref() {
            self.closure.get(id)?.map(Ids::from).unwrap_or_default()
        } else {
            Default::default()
        };

        let mut filter = self.filter.lock().await;
        for id in closure.iter() {
            if !self.blocks.contains_id(&id)? {
                return Err(IdNotFound(id).into());
            }
        }
        for id in closure.iter() {
            filter.add(&id).unwrap();
        }
        for id in prev_closure.iter() {
            filter.delete(&id);
        }

        let res = (&self.alias, &self.closure)
            .transaction(|(talias, tclosure)| {
                if prev_id.is_some() {
                    talias.remove(alias)?;
                }
                if let Some(id) = id.as_ref() {
                    talias.insert(alias, id)?;
                    tclosure.insert(id, &closure)?;
                }
                Ok(())
            })
            .map_err(map_tx_error);

        if res.is_err() {
            for id in prev_closure.iter() {
                filter.add(&id).unwrap();
            }
            for id in closure.iter() {
                filter.delete(&id);
            }
        }
        drop(filter);

        res
    }

    /// Resolves the alias to a `Cid`.
    pub fn resolve(&self, alias: &[u8]) -> Result<Option<Cid>> {
        if let Some(id) = self.alias.get(alias)? {
            self.blocks.lookup_cid(&id.into())
        } else {
            Ok(None)
        }
    }

    /// Checks if the block is pinned. If the block is pinned it
    /// means that it can't be evicted.
    pub async fn pinned(&self, cid: &Cid) -> Result<Option<bool>> {
        if let Some(id) = self.blocks.lookup_id(cid)? {
            let filter = self.filter.lock().await;
            Ok(Some(filter.contains(&id)))
        } else {
            Ok(None)
        }
    }

    /// Evicts least recently used blocks until there are no more
    /// than `cache_size` number of unpinned blocks.
    pub async fn evict(&self, cache_size: usize) -> Result<()> {
        let filter = self.filter.lock().await;
        let nblocks = self.blocks.len();
        let nlive = filter.len();
        let ncache = nblocks - nlive;
        if ncache <= cache_size {
            return Ok(());
        }
        let mut nevict = ncache - cache_size;
        log::debug!("evicting {} blocks", nevict);
        for res in self.blocks.lru() {
            if nevict < 1 {
                break;
            }
            let id = res?;
            if !filter.contains(&id) {
                self.blocks.remove(&id)?;
                self.closure.remove(&id)?;
                nevict -= 1;
            }
        }
        Ok(())
    }

    /// Subscribes to store events.
    pub fn subscribe(&self) -> Subscription {
        self.blocks.subscribe()
    }
}
