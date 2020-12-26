pub use anyhow::{Error, Result};
pub use async_trait::async_trait;
use fnv::FnvHashSet;
use futures::channel::oneshot;
use futures::stream::Stream;
pub use libipld::block::Block;
pub use libipld::cid::Cid;
pub use libipld::error::BlockNotFound;
pub use libipld::multihash::{MultihashDigest, U64};
pub use libipld::store::{Store, StoreParams};
pub use libp2p::gossipsub::{error::PublishError, GossipsubEvent, Topic};
pub use libp2p::swarm::AddressRecord;
pub use libp2p::{Multiaddr, PeerId};
pub use libp2p_bitswap::{Channel, Query, QueryResult, QueryType};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum NetworkEvent<S: StoreParams> {
    QueryComplete(Query, QueryResult),
    HaveBlock(Arc<Channel>, Cid),
    WantBlock(Arc<Channel>, Cid),
    ReceivedBlock(Block<S>),
    MissingBlocks(Cid),
    Gossip(GossipsubEvent),
}

pub trait Network<S: StoreParams>: Send + Sync + 'static {
    type Subscription: Stream<Item = NetworkEvent<S>> + Send + Unpin;
    fn local_peer_id(&self) -> &PeerId;
    fn listeners(&self, tx: oneshot::Sender<Vec<Multiaddr>>);
    fn external_addresses(&self, tx: oneshot::Sender<Vec<AddressRecord>>);
    fn get(&self, cid: Cid);
    fn cancel_get(&self, cid: Cid);
    fn sync(&self, cid: Cid, missing: FnvHashSet<Cid>);
    fn cancel_sync(&self, cid: Cid);
    fn add_missing(&self, cid: Cid, missing: FnvHashSet<Cid>);
    fn provide(&self, cid: Cid);
    fn unprovide(&self, cid: Cid);
    fn send_have(&self, ch: Channel, have: bool);
    fn send_block(&self, ch: Channel, block: Option<Vec<u8>>);
    fn subscribe_topic(&self, topic: Topic);
    fn unsubscribe_topic(&self, topic: Topic);
    fn publish(&self, topic: Topic, msg: Vec<u8>, tx: oneshot::Sender<Result<(), PublishError>>);
    fn subscribe(&self) -> Self::Subscription;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageEvent {
    Remove(Cid),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageStats {
    /// Total number of blocks in the store
    pub count: u64,
    /// Total size of blocks in the store
    pub size: u64,
}

#[async_trait]
pub trait Storage<S: StoreParams>: Send + Sync + 'static {
    type TempPin: Send + Sync;
    type Iterator: Iterator<Item = Cid>;
    type Subscription: Stream<Item = StorageEvent> + Send + Unpin;
    async fn temp_pin(&self) -> Result<Self::TempPin>;
    async fn iter(&self) -> Result<Self::Iterator>;
    async fn contains(&self, cid: &Cid) -> Result<bool>;
    async fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>>;
    async fn insert(&self, block: &Block<S>, alias: Option<&Self::TempPin>) -> Result<()>;
    async fn evict(&self) -> Result<()>;
    async fn alias<T: AsRef<[u8]> + Send + Sync>(&self, alias: T, cid: Option<&Cid>) -> Result<()>;
    async fn resolve<T: AsRef<[u8]> + Send + Sync>(&self, alias: T) -> Result<Option<Cid>>;
    async fn pinned(&self, cid: &Cid) -> Result<Option<Vec<Vec<u8>>>>;
    async fn missing_blocks(&self, cid: &Cid) -> Result<FnvHashSet<Cid>>;
    async fn stats(&self) -> Result<StorageStats>;
    fn subscribe(&self) -> Self::Subscription;
}
