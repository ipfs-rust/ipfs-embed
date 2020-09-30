pub use anyhow::{Error, Result};
pub use async_trait::async_trait;
pub use futures::stream::Stream;
pub use libipld::block::Block;
pub use libipld::cid::Cid;
pub use libipld::error::BlockNotFound;
pub use libipld::multihash::MultihashDigest;
pub use libipld::store::{Store, StoreParams};
pub use libp2p_core::{Multiaddr, PeerId};

#[derive(Clone, Debug)]
pub enum NetworkEvent<QueryId> {
    Listeners(Vec<Multiaddr>),
    ExternalAddresses(Vec<Multiaddr>),
    Request(PeerId, Cid),
    Response(QueryId, Cid, Vec<u8>),
    QueryResult(QueryId, std::result::Result<(), BlockNotFound>),
}

pub trait Network<S: StoreParams>: Send + Sync + 'static {
    type QueryId: Copy;
    type Subscription: Stream<Item = NetworkEvent<Self::QueryId>> + Send + Unpin;
    fn local_peer_id(&self) -> &PeerId;
    fn listeners(&self);
    fn external_addresses(&self);
    fn get(&self, cid: Cid) -> Self::QueryId;
    fn sync(&self, cid: Cid) -> Self::QueryId;
    fn cancel(&self, query_id: Self::QueryId);
    fn provide(&self, cid: Cid);
    fn unprovide(&self, cid: Cid);
    fn send(&self, peer_id: PeerId, cid: Cid, data: Vec<u8>);
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
