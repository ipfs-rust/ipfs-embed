mod metadata;
mod tx;
mod wal;

use crate::metadata::{Metadata, Subscription};
use crate::tx::StateBuilder;
use crate::wal::Wal;
use ipfs_embed_core::{Result, Storage};
use libipld::cid::Cid;
use libipld::codec::Decode as IpldDecode;
use libipld::ipld::Ipld;
use libipld::store::{Op, StoreParams, Transaction};
use parity_db::Db;
use parity_scale_codec::{Decode, Encode};
use std::collections::HashSet;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Mutex;
use thiserror::Error;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Decode, Encode)]
enum StoreOp {
    /// Inserts a block with an id and a cid.
    Insert(u64, Cid),
    /// Inserts a reference from block a to b.
    InsertReference(u64, u64),
    /// Removes a reference from block a to b.
    RemoveReference(u64, u64),
    /// Sets the pin count of a block.
    SetPin(u64, u64),
    /// Removes a block.
    Remove(u64),
}

fn cid_to_key(cid: &Cid) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf.copy_from_slice(&cid.hash().digest()[..32]);
    buf
}

pub struct StorageService<S: StoreParams> {
    _marker: PhantomData<S>,
    wal: Mutex<Wal<StoreOp>>,
    blocks: Db,
    metadata: Metadata,
}

impl<S: StoreParams + 'static> StorageService<S> {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let wal = Wal::open(path.as_ref().join("wal"))?;
        let options = parity_db::Options {
            path: path.as_ref().join("blocks"),
            columns: vec![parity_db::ColumnOptions {
                // the key is the hash of the value
                preimage: true,
                // and has an uniform distribution
                uniform: true,
                // table value sizes (maximum size supported by parity_db is 65533)
                sizes: [
                    96, 128, 192, 256, 320, 512, 768, 1024, 1536, 2048, 3072, 4096, 8192, 16384,
                    32768,
                ],
                // we do our own refcounting
                ref_counted: false,
            }],
            // we do our own durability
            sync: false,
            stats: false,
        };
        let blocks = Db::open(&options).map_err(ParityDbError)?;
        let db = sled::open(path.as_ref().join("metadata"))?;
        let tree = db.open_tree("metadata")?;
        let metadata = Metadata::new(tree);

        let me = Self {
            _marker: PhantomData,
            wal: Mutex::new(wal),
            metadata,
            blocks,
        };
        me.replay()?;

        Ok(me)
    }

    pub fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        let key = cid_to_key(cid);
        Ok(self.blocks.get(0, &key[..]).map_err(ParityDbError)?)
    }

    pub fn commit(&self, tx: Transaction<S>) -> Result<()>
    where
        Ipld: IpldDecode<S::Codecs>,
    {
        let mut wal = self.wal.lock().unwrap();
        let mut ops = HashSet::new();
        let mut state = StateBuilder::new(self.metadata.clone(), wal.id());
        for op in &tx {
            if let Op::Insert(block, _) = op {
                let cid = block.cid();
                let id = state.insert(cid)?;
                let op = StoreOp::Insert(id, cid.clone());
                ops.insert(op);
            }
        }
        for op in &tx {
            match op {
                Op::Insert(block, refs) => {
                    let cid = block.cid();
                    let id1 = state.get(cid)?;
                    for cid in refs {
                        let id2 = state.get(cid)?;
                        let op = StoreOp::InsertReference(id1, id2);
                        ops.insert(op);
                    }
                }
                Op::Pin(cid) => {
                    state.pin(cid)?;
                }
                Op::Unpin(cid) => {
                    state.unpin(cid)?;
                }
            }
        }
        let mut gc = Vec::new();
        for (id, pin) in state.pins() {
            let op = StoreOp::SetPin(id, pin);
            ops.insert(op);
            if pin < 1 {
                gc.push(id);
            }
        }
        while let Some(id) = gc.pop() {
            if state.is_live(id)? {
                log::trace!("live: {}", id);
                continue;
            }

            // TODO: can leak if the transaction inserts and removes the same block
            for id2 in self.metadata.references(id) {
                let id2 = id2?;
                let op = StoreOp::RemoveReference(id, id2);
                ops.insert(op);
                state.remove_reference((id, id2));
                gc.push(id2);
            }

            let op = StoreOp::Remove(id);
            ops.insert(op);
        }
        for op in &ops {
            wal.op(op)?;
        }
        let next_id = state.begin();
        wal.begin(next_id)?;

        self.blocks
            .commit(tx.into_iter().filter_map(|op| {
                if let Op::Insert(block, _) = op {
                    let key = cid_to_key(block.cid());
                    let (_, data) = block.into_inner();
                    Some((0, key, Some(data)))
                } else {
                    None
                }
            }))
            .map_err(ParityDbError)?;

        while let Err(err) = self.execute(ops.iter()) {
            log::error!("{}", err);
        }

        self.metadata.flush()?;

        wal.end(next_id)?;
        Ok(())
    }

    pub fn contains(&self, cid: &Cid) -> Result<bool> {
        Ok(self.metadata.id(cid)?.is_some())
    }

    fn execute<'a, I>(&self, ops: I) -> Result<()>
    where
        I: Iterator<Item = &'a StoreOp> + 'a,
    {
        for op in ops {
            match op {
                StoreOp::Insert(id, cid) => self.metadata.insert(cid, *id)?,
                StoreOp::InsertReference(id1, id2) => self.metadata.insert_reference(*id1, *id2)?,
                StoreOp::RemoveReference(id1, id2) => self.metadata.remove_reference(*id1, *id2)?,
                StoreOp::SetPin(id, pin) => self.metadata.set_pin(*id, *pin)?,
                StoreOp::Remove(id) => {
                    if let Some(cid) = self.metadata.cid(*id)? {
                        let key = cid_to_key(&cid);
                        self.blocks
                            .commit(std::iter::once((0, key, None)))
                            .map_err(ParityDbError)?;
                        self.metadata.remove(&cid, *id)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn replay(&self) -> Result<()> {
        let mut wal = self.wal.lock().unwrap();
        if let Some((next_id, ops)) = wal.replay() {
            let mut abort = false;
            for op in &ops {
                if let StoreOp::Insert(_, cid) = op {
                    if self.get(cid)?.is_none() {
                        abort = true;
                        break;
                    }
                }
            }

            if abort {
                log::info!("aborting transaction");
                for op in &ops {
                    if let StoreOp::Insert(_, cid) = op {
                        let key = cid_to_key(cid);
                        self.blocks
                            .commit(std::iter::once((0, key, None)))
                            .map_err(ParityDbError)?;
                    }
                }
                let next_id = wal.id();
                wal.end(next_id)?;
            } else {
                log::info!("executing partial transaction");
                while let Err(err) = self.execute(ops.iter()) {
                    log::error!("{}", err);
                }
                wal.end(next_id)?;
            }
        }
        Ok(())
    }

    pub fn blocks(&self) -> Result<Vec<(u64, Cid, u64)>> {
        let mut blocks = Vec::new();
        for id in self.metadata.blocks() {
            let id = id?;
            let cid = if let Some(cid) = self.metadata.cid(id)? {
                cid
            } else {
                continue;
            };
            let pin = if let Some(pin) = self.metadata.pin(id)? {
                pin
            } else {
                continue;
            };
            blocks.push((id, cid, pin));
        }
        Ok(blocks)
    }

    pub fn references(&self, id: u64) -> Result<Vec<(u64, Cid)>> {
        let mut blocks = Vec::new();
        for id in self.metadata.references(id) {
            let id = id?;
            if let Some(cid) = self.metadata.cid(id)? {
                blocks.push((id, cid));
            }
        }
        Ok(blocks)
    }

    pub fn referrers(&self, id: u64) -> Result<Vec<(u64, Cid)>> {
        let mut blocks = Vec::new();
        for id in self.metadata.referrers(id) {
            let id = id?;
            if let Some(cid) = self.metadata.cid(id)? {
                blocks.push((id, cid));
            }
        }
        Ok(blocks)
    }
}

impl<S: StoreParams + Unpin + 'static> Storage<S> for StorageService<S>
where
    Ipld: IpldDecode<S::Codecs>,
{
    type Subscription = Subscription;

    fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        self.get(cid)
    }

    fn commit(&self, tx: Transaction<S>) -> Result<()> {
        self.commit(tx)
    }

    fn contains(&self, cid: &Cid) -> Result<bool> {
        self.contains(cid)
    }

    fn blocks(&self) -> Result<Vec<(u64, Cid, u64)>> {
        self.blocks()
    }

    fn references(&self, id: u64) -> Result<Vec<(u64, Cid)>> {
        self.references(id)
    }

    fn referrers(&self, id: u64) -> Result<Vec<(u64, Cid)>> {
        self.referrers(id)
    }

    fn subscribe(&self) -> Self::Subscription {
        self.metadata.subscribe()
    }
}

#[derive(Debug, Error)]
#[error("{0}")]
pub struct ParityDbError(parity_db::Error);

impl From<parity_db::Error> for ParityDbError {
    fn from(error: parity_db::Error) -> Self {
        Self(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libipld::cbor::DagCborCodec;
    use libipld::multihash::SHA2_256;
    use libipld::{ipld, Block, DefaultStoreParams, Ipld, Transaction};
    use tempdir::TempDir;

    fn create_block(ipld: &Ipld) -> Block<DefaultStoreParams> {
        Block::encode(DagCborCodec, SHA2_256, ipld).unwrap()
    }

    #[test]
    fn test_gc() -> Result<()> {
        env_logger::try_init().ok();
        let tmp = TempDir::new("db").unwrap();
        let store = StorageService::open(tmp.path()).unwrap();
        let a = create_block(&ipld!({ "a": [] }));
        let b = create_block(&ipld!({ "b": [a.cid()] }));
        let c = create_block(&ipld!({ "c": [a.cid()] }));

        let mut tx = Transaction::<DefaultStoreParams>::with_capacity(5);
        tx.insert(a.clone())?;
        tx.insert(b.clone())?;
        tx.insert(c.clone())?;
        tx.pin(b.cid().clone());
        tx.pin(c.cid().clone());
        store.commit(tx).unwrap();
        assert_eq!(store.contains(a.cid())?, true); // 0 2
        assert_eq!(store.contains(b.cid())?, true); // 1 0
        assert_eq!(store.contains(c.cid())?, true); // 1 0

        let mut tx = Transaction::<DefaultStoreParams>::with_capacity(1);
        tx.unpin(b.cid().clone());
        store.commit(tx).unwrap();
        assert_eq!(store.contains(a.cid())?, true); // 0 1
        assert_eq!(store.contains(b.cid())?, false);
        assert_eq!(store.contains(c.cid())?, true); // 1 0

        let mut tx = Transaction::<DefaultStoreParams>::with_capacity(1);
        tx.unpin(c.cid().clone());
        store.commit(tx).unwrap();
        assert_eq!(store.contains(a.cid())?, false);
        assert_eq!(store.contains(b.cid())?, false);
        assert_eq!(store.contains(c.cid())?, false);

        Ok(())
    }

    #[test]
    fn test_gc_2() -> Result<()> {
        env_logger::try_init().ok();
        let tmp = TempDir::new("db").unwrap();
        let store = StorageService::open(tmp.path()).unwrap();
        let a = create_block(&ipld!({ "a": [] }));
        let b = create_block(&ipld!({ "b": [a.cid()] }));
        let c = b.clone();

        let mut tx = Transaction::with_capacity(5);
        tx.insert(a.clone())?;
        tx.insert(b.clone())?;
        tx.insert(c.clone())?;
        tx.pin(b.cid().clone());
        tx.pin(c.cid().clone());
        store.commit(tx).unwrap();
        assert_eq!(store.contains(a.cid())?, true); // 0 1
        assert_eq!(store.contains(b.cid())?, true); // 2 0
        assert_eq!(store.contains(c.cid())?, true); // 2 0

        let mut tx = Transaction::<DefaultStoreParams>::with_capacity(1);
        tx.unpin(b.cid().clone());
        store.commit(tx).unwrap();
        assert_eq!(store.contains(a.cid())?, true); // 0 1
        assert_eq!(store.contains(b.cid())?, true); // 1 0
        assert_eq!(store.contains(c.cid())?, true); // 1 0

        let mut tx = Transaction::<DefaultStoreParams>::with_capacity(1);
        tx.unpin(c.cid().clone());
        store.commit(tx).unwrap();
        assert_eq!(store.contains(a.cid())?, false);
        assert_eq!(store.contains(b.cid())?, false);
        assert_eq!(store.contains(c.cid())?, false);

        Ok(())
    }
}
