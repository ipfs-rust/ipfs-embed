pub use anyhow::{Error, Result};
pub use async_trait::async_trait;
use futures::channel::oneshot;
use futures::stream::Stream;
pub use libipld::block::Block;
pub use libipld::cid::Cid;
use libipld::codec::Decode;
pub use libipld::error::BlockNotFound;
use libipld::ipld::Ipld;
pub use libipld::multihash::{MultihashCode, U64};
pub use libipld::store::{Store, StoreParams};
pub use libp2p_bitswap::{BitswapStore, BitswapSync, Query, QueryResult, QueryType};
pub use libp2p_core::{Multiaddr, PeerId};
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum NetworkEvent {
    QueryComplete(Query, QueryResult),
}

pub trait Network<S: StoreParams>: Send + Sync + 'static {
    type Subscription: Stream<Item = NetworkEvent> + Send + Unpin;
    fn local_peer_id(&self) -> &PeerId;
    fn listeners(&self, tx: oneshot::Sender<Vec<Multiaddr>>);
    fn external_addresses(&self, tx: oneshot::Sender<Vec<Multiaddr>>);
    fn get(&self, cid: Cid);
    fn cancel_get(&self, cid: Cid);
    fn sync(&self, cid: Cid, syncer: Arc<dyn BitswapSync>);
    fn cancel_sync(&self, cid: Cid);
    fn provide(&self, cid: Cid);
    fn unprovide(&self, cid: Cid);
    fn subscribe(&self) -> Self::Subscription;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageEvent {
    Insert(Cid),
    Remove(Cid),
}

#[async_trait]
pub trait Storage<S: StoreParams>: Send + Sync + 'static {
    type Subscription: Stream<Item = StorageEvent> + Send + Unpin;
    fn contains(&self, cid: &Cid) -> Result<bool>;
    fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>>;
    fn insert(&self, block: &Block<S>) -> Result<()>;
    async fn alias<T: AsRef<[u8]> + Send + Sync>(&self, alias: T, cid: Option<&Cid>) -> Result<()>;
    fn resolve<T: AsRef<[u8]> + Send + Sync>(&self, alias: T) -> Result<Option<Cid>>;
    async fn pinned(&self, cid: &Cid) -> Result<Option<bool>>;
    fn subscribe(&self) -> Self::Subscription;
}

pub struct BitswapStorage<S: StoreParams, T: Storage<S>> {
    marker: PhantomData<S>,
    store: Arc<T>,
}

impl<S: StoreParams, T: Storage<S>> BitswapStorage<S, T> {
    pub fn new(store: Arc<T>) -> Self {
        Self {
            marker: PhantomData,
            store,
        }
    }
}

impl<S: StoreParams, T: Storage<S>> BitswapStore<S> for BitswapStorage<S, T> {
    fn contains(&self, cid: &Cid) -> bool {
        match self.store.contains(cid) {
            Ok(b) => b,
            Err(err) => {
                log::error!("contains: {:?}", err);
                false
            }
        }
    }

    fn get(&self, cid: &Cid) -> Option<Vec<u8>> {
        match self.store.get(cid) {
            Ok(data) => data,
            Err(err) => {
                log::error!("contains: {:?}", err);
                None
            }
        }
    }

    fn insert(&self, cid: Cid, data: Vec<u8>) {
        let block = Block::new_unchecked(cid, data);
        if let Err(err) = self.store.insert(&block) {
            log::error!("insert: {:?}", err);
        }
    }
}

impl<S: StoreParams, T: Storage<S>> BitswapSync for BitswapStorage<S, T>
where
    Ipld: Decode<S::Codecs>,
{
    fn references(&self, cid: &Cid) -> Box<dyn Iterator<Item = Cid>> {
        if let Some(data) = self.get(cid) {
            let block = Block::<S>::new_unchecked(*cid, data);
            if let Ok(refs) = block.references() {
                return Box::new(refs.into_iter());
            }
        }
        Box::new(std::iter::empty())
    }

    fn contains(&self, cid: &Cid) -> bool {
        BitswapStore::<S>::contains(self, cid)
    }
}
