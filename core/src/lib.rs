pub use anyhow::{Error, Result};
pub use async_trait::async_trait;
use futures::channel::oneshot;
use futures::stream::Stream;
pub use libipld::block::Block;
pub use libipld::cid::Cid;
pub use libipld::error::BlockNotFound;
pub use libipld::multihash::{MultihashCode, U64};
pub use libipld::store::{Store, StoreParams};
pub use libp2p_core::{Multiaddr, PeerId};

#[derive(Clone, Debug)]
pub enum NetworkEvent {
    Request(PeerId, Cid),
    Response(Cid, Vec<u8>),
}

pub type QueryResult = core::result::Result<(), BlockNotFound>;

pub trait Network<S: StoreParams>: Send + Sync + 'static {
    type Subscription: Stream<Item = NetworkEvent> + Send + Unpin;
    fn local_peer_id(&self) -> &PeerId;
    fn listeners(&self, tx: oneshot::Sender<Vec<Multiaddr>>);
    fn external_addresses(&self, tx: oneshot::Sender<Vec<Multiaddr>>);
    fn get(&self, cid: Cid, tx: oneshot::Sender<QueryResult>);
    fn sync(&self, cid: Cid, tx: oneshot::Sender<QueryResult>);
    fn provide(&self, cid: Cid);
    fn unprovide(&self, cid: Cid);
    fn send(&self, peer_id: PeerId, cid: Cid, data: Option<Vec<u8>>);
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
    fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>>;
    fn insert(&self, block: &Block<S>) -> Result<()>;
    async fn alias<T: AsRef<[u8]> + Send + Sync>(&self, alias: T, cid: Option<&Cid>) -> Result<()>;
    fn resolve<T: AsRef<[u8]> + Send + Sync>(&self, alias: T) -> Result<Option<Cid>>;
    async fn pinned(&self, cid: &Cid) -> Result<Option<bool>>;
    fn subscribe(&self) -> Self::Subscription;
}
