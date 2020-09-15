pub use anyhow::Result;
pub use futures::stream::Stream;
pub use libipld::block::Block;
pub use libipld::cid::Cid;
pub use libipld::multihash::MultihashDigest;
pub use libipld::store::{Store, StoreParams, Transaction};
pub use libp2p_core::{Multiaddr, PeerId};
use std::collections::HashSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NetworkEvent {
    BootstrapComplete,
    Providers(Cid, HashSet<PeerId>),
    GetProvidersFailed(Cid),
    Providing(Cid),
    StartProvidingFailed(Cid),
    ReceivedBlock(PeerId, Cid, Vec<u8>),
    ReceivedWant(PeerId, Cid, i32),
}

pub trait Network<S: StoreParams>: Send + Sync + 'static {
    type Subscription: Stream<Item = NetworkEvent> + Send + Unpin;
    fn local_peer_id(&self) -> &PeerId;
    fn external_addresses(&self) -> Vec<Multiaddr>;
    fn providers(&self, cid: &Cid);
    fn provide(&self, cid: &Cid);
    fn unprovide(&self, cid: &Cid);
    fn connect(&self, peer_id: PeerId);
    fn want(&self, cid: Cid, priority: i32);
    fn cancel(&self, cid: Cid);
    fn send_to(&self, peer_id: PeerId, cid: Cid, data: Vec<u8>);
    fn send(&self, cid: Cid, data: Vec<u8>);
    fn subscribe(&self) -> Self::Subscription;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageEvent {
    Insert(Cid),
    Remove(Cid),
}

pub trait Storage<S: StoreParams>: Send + Sync + 'static {
    type Subscription: Stream<Item = StorageEvent> + Send + Unpin;
    fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>>;
    fn commit(&self, tx: Transaction<S>) -> Result<()>;
    fn contains(&self, cid: &Cid) -> Result<bool>;
    fn blocks(&self) -> Result<Vec<(u64, Cid, u64)>>;
    fn references(&self, id: u64) -> Result<Vec<(u64, Cid)>>;
    fn referrers(&self, id: u64) -> Result<Vec<(u64, Cid)>>;
    fn subscribe(&self) -> Self::Subscription;
}
