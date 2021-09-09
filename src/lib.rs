//! Ipfs embed is a small, fast and reliable ipfs implementation designed for embedding in to
//! complex p2p applications.
//!
//! ```no_run
//! # #[async_std::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # use ipfs_embed::{Config, DefaultParams, Ipfs};
//! let ipfs = Ipfs::<DefaultParams>::new(Config::default()).await?;
//! ipfs.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
//! # Ok(()) }
//! ```
use crate::db::StorageService;
pub use crate::db::{StorageConfig, TempPin};
pub use crate::net::{
    generate_keypair, AddressRecord, AddressSource, BitswapConfig, BroadcastConfig, DnsConfig,
    DocId, Event, GossipEvent, GossipsubConfig, Head, IdentifyConfig, KadConfig, Key, Keypair,
    ListenerEvent, ListenerId, LocalStreamWriter, MdnsConfig, Multiaddr, NetworkConfig, PeerId,
    PeerInfo, PeerRecord, PingConfig, PublicKey, Quorum, Record, SecretKey, SignedHead, StreamId,
    StreamReader, SwarmEvents, SyncEvent, SyncQuery, ToLibp2p, TransportConfig,
};
use crate::net::{BitswapStore, NetworkService};
#[cfg(feature = "telemetry")]
pub use crate::telemetry::telemetry;
use async_trait::async_trait;
pub use db::Batch;
use executor::Executor;
use futures::stream::Stream;
use libipld::codec::References;
use libipld::error::BlockNotFound;
pub use libipld::store::DefaultParams;
use libipld::store::{Store, StoreParams};
pub use libipld::{Block, Cid};
use libipld::{Ipld, Result};
use libp2p::kad::kbucket::Key as BucketKey;
pub use libp2p::multiaddr;
use prometheus::Registry;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

mod db;
mod executor;
mod net;
#[cfg(feature = "telemetry")]
mod telemetry;
#[cfg(test)]
mod test_util;

/// Ipfs configuration.
#[derive(Debug)]
pub struct Config {
    /// Storage configuration.
    pub storage: StorageConfig,
    /// Network configuration.
    pub network: NetworkConfig,
}

impl Config {
    /// Creates a default configuration from a `path` and a `cache_size`. If the `path` is `None`,
    /// ipfs will use an in-memory block store.
    pub fn new(path: &Path, keypair: Keypair) -> Self {
        let sweep_interval = std::time::Duration::from_millis(10000);
        let storage = StorageConfig::new(Some(path.join("blocks")), None, 0, sweep_interval);
        let network = NetworkConfig::new(path.join("streams"), keypair);
        Self { storage, network }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new(Path::new("."), generate_keypair())
    }
}

/// Ipfs node.
#[derive(Clone)]
pub struct Ipfs<P: StoreParams> {
    storage: StorageService<P>,
    network: NetworkService<P>,
}

impl<P: StoreParams> std::fmt::Debug for Ipfs<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Ipfs").finish()
    }
}

struct BitswapStorage<P: StoreParams>(StorageService<P>);

impl<P: StoreParams> BitswapStore for BitswapStorage<P>
where
    Ipld: References<P::Codecs>,
{
    type Params = P;

    fn contains(&mut self, cid: &Cid) -> Result<bool> {
        self.0.contains(cid)
    }

    fn get(&mut self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        self.0.get(cid)
    }

    fn insert(&mut self, block: &Block<P>) -> Result<()> {
        self.0.insert(block)
    }

    fn missing_blocks(&mut self, cid: &Cid) -> Result<Vec<Cid>> {
        self.0.missing_blocks(cid)
    }
}

impl<P: StoreParams> Ipfs<P>
where
    Ipld: References<P::Codecs>,
{
    /// Creates a new `Ipfs` from a `Config`.
    ///
    /// This starts three background tasks. The swarm, garbage collector and the dht cleanup
    /// tasks run in the background.
    pub async fn new(config: Config) -> Result<Self> {
        let executor = Executor::new();
        Self::new0(config, executor).await
    }
    async fn new0(config: Config, executor: Executor) -> Result<Self> {
        let storage = StorageService::open(config.storage, executor.clone())?;
        let bitswap = BitswapStorage(storage.clone());
        let network = NetworkService::new(config.network, bitswap, executor).await?;
        Ok(Self { storage, network })
    }

    /// Returns the local `PublicKey`.
    pub fn local_public_key(&self) -> PublicKey {
        self.network.local_public_key()
    }

    /// Returns the local `PeerId`.
    pub fn local_peer_id(&self) -> PeerId {
        self.network.local_peer_id()
    }

    /// Returns the local node name.
    pub fn local_node_name(&self) -> String {
        self.network.local_node_name()
    }

    /// Listens on a new `Multiaddr`.
    pub fn listen_on(&self, addr: Multiaddr) -> Result<impl Stream<Item = ListenerEvent>> {
        self.network.listen_on(addr)
    }

    /// Returns the currently active listener addresses.
    pub fn listeners(&self) -> Vec<Multiaddr> {
        self.network.listeners()
    }

    /// Adds an external address.
    pub fn add_external_address(&self, addr: Multiaddr) {
        self.network.add_external_address(addr)
    }

    /// Returns the currently used external addresses.
    pub fn external_addresses(&self) -> Vec<AddressRecord> {
        self.network.external_addresses()
    }

    /// Adds a known `Multiaddr` for a `PeerId`.
    pub fn add_address(&self, peer: &PeerId, addr: Multiaddr) {
        self.network.add_address(peer, addr)
    }

    /// Removes a `Multiaddr` for a `PeerId`.
    pub fn remove_address(&self, peer: &PeerId, addr: &Multiaddr) {
        self.network.remove_address(peer, addr)
    }

    /// Dials a `PeerId` using a known address.
    pub fn dial(&self, peer: &PeerId) {
        self.network.dial(peer);
    }

    /// Dials a `PeerId` using `Multiaddr`.
    pub fn dial_address(&self, peer: &PeerId, addr: Multiaddr) {
        self.network.add_address(peer, addr);
        self.network.dial(peer);
    }

    /// Bans a `PeerId` from the swarm, dropping all existing connections and
    /// preventing new connections from the peer.
    pub fn ban(&self, peer: PeerId) {
        self.network.ban(peer)
    }

    /// Unbans a previously banned `PeerId`.
    pub fn unban(&self, peer: PeerId) {
        self.network.unban(peer)
    }

    /// Returns the known peers.
    pub fn peers(&self) -> Vec<PeerId> {
        self.network.peers()
    }

    /// Returns a list of connected peers.
    pub fn connections(&self) -> Vec<(PeerId, Multiaddr)> {
        self.network.connections()
    }

    /// Returns `true` if there is a connection to peer.
    pub fn is_connected(&self, peer: &PeerId) -> bool {
        self.network.is_connected(peer)
    }

    /// Returns the `PeerInfo` of a peer.
    pub fn peer_info(&self, peer: &PeerId) -> Option<PeerInfo> {
        self.network.peer_info(peer)
    }

    /// Bootstraps the dht using a set of bootstrap nodes. After bootstrap completes it
    /// provides all blocks in the block store.
    pub async fn bootstrap(&self, nodes: &[(PeerId, Multiaddr)]) -> Result<()> {
        self.network.bootstrap(nodes).await?;
        Ok(())
    }

    /// Returns true if the dht was bootstrapped.
    pub fn is_bootstrapped(&self) -> bool {
        self.network.is_bootstrapped()
    }

    /// Gets the closest peer to a key. Useful for finding the `Multiaddr` of a `PeerId`.
    pub async fn get_closest_peers<K>(&self, key: K) -> Result<()>
    where
        K: Into<BucketKey<K>> + Into<Vec<u8>> + Clone,
    {
        self.network.get_closest_peers(key).await?;
        Ok(())
    }

    /// Gets providers of a key from the dht.
    pub async fn providers(&self, key: Key) -> Result<HashSet<PeerId>> {
        self.network.providers(key).await
    }

    /// Provides a key in the dht.
    pub async fn provide(&self, key: Key) -> Result<()> {
        self.network.provide(key).await
    }

    /// Stops providing a key in the dht.
    pub fn unprovide(&self, key: &Key) {
        self.network.unprovide(key)
    }

    /// Gets a record from the dht.
    pub async fn get_record(&self, key: &Key, quorum: Quorum) -> Result<Vec<PeerRecord>> {
        self.network.get_record(key, quorum).await
    }

    /// Puts a new record in the dht.
    pub async fn put_record(&self, record: Record, quorum: Quorum) -> Result<()> {
        self.network.put_record(record, quorum).await
    }

    /// Removes a record from the dht.
    pub fn remove_record(&self, key: &Key) {
        self.network.remove_record(key)
    }

    /// Subscribes to a `topic` returning a `Stream` of messages. If all `Stream`s for
    /// a topic are dropped it unsubscribes from the `topic`.
    pub fn subscribe(&self, topic: &str) -> Result<impl Stream<Item = GossipEvent>> {
        self.network.subscribe(topic)
    }

    /// Publishes a new message in a `topic`, sending the message to all subscribed peers.
    pub fn publish(&self, topic: &str, msg: Vec<u8>) -> Result<()> {
        self.network.publish(topic, msg)
    }

    /// Publishes a new message in a `topic`, sending the message to all subscribed connected peers.
    pub fn broadcast(&self, topic: &str, msg: Vec<u8>) -> Result<()> {
        self.network.broadcast(topic, msg)
    }

    /// Creates a temporary pin in the block store. A temporary pin is not persisted to disk
    /// and is released once it is dropped.
    pub fn create_temp_pin(&self) -> Result<TempPin> {
        self.storage.create_temp_pin()
    }

    /// Adds a new root to a temporary pin.
    pub fn temp_pin(&self, tmp: &TempPin, cid: &Cid) -> Result<()> {
        self.storage.temp_pin(tmp, std::iter::once(*cid))
    }

    /// Returns an `Iterator` of `Cid`s stored in the block store.
    pub fn iter(&self) -> Result<impl Iterator<Item = Cid>> {
        self.storage.iter()
    }

    /// Checks if the block is in the block store.
    pub fn contains(&self, cid: &Cid) -> Result<bool> {
        self.storage.contains(cid)
    }

    /// Returns a block from the block store.
    pub fn get(&self, cid: &Cid) -> Result<Block<P>> {
        if let Some(data) = self.storage.get(cid)? {
            let block = Block::new_unchecked(*cid, data);
            Ok(block)
        } else {
            Err(BlockNotFound(*cid).into())
        }
    }

    /// Either returns a block if it's in the block store or tries to retrieve it from
    /// a peer.
    pub async fn fetch(&self, cid: &Cid, providers: Vec<PeerId>) -> Result<Block<P>> {
        if let Some(data) = self.storage.get(cid)? {
            let block = Block::new_unchecked(*cid, data);
            return Ok(block);
        }
        if !providers.is_empty() {
            self.network.get(*cid, providers.into_iter()).await?;
            if let Some(data) = self.storage.get(cid)? {
                let block = Block::new_unchecked(*cid, data);
                return Ok(block);
            }
            tracing::error!("block evicted too soon. use a temp pin to keep the block around.");
        }
        Err(BlockNotFound(*cid).into())
    }

    /// Inserts a block in to the block store.
    pub fn insert(&self, block: &Block<P>) -> Result<()> {
        self.storage.insert(block)?;
        Ok(())
    }

    /// Manually runs garbage collection to completion. This is mainly useful for testing and
    /// administrative interfaces. During normal operation, the garbage collector automatically
    /// runs in the background.
    pub async fn evict(&self) -> Result<()> {
        self.storage.evict().await
    }

    pub fn sync(&self, cid: &Cid, providers: Vec<PeerId>) -> SyncQuery<P> {
        let missing = self.storage.missing_blocks(cid).ok().unwrap_or_default();
        self.network.sync(*cid, providers, missing)
    }

    /// Creates, updates or removes an alias with a new root `Cid`.
    pub fn alias<T: AsRef<[u8]> + Send + Sync>(&self, alias: T, cid: Option<&Cid>) -> Result<()> {
        self.storage.alias(alias.as_ref(), cid)
    }

    /// Returns the root of an alias.
    pub fn resolve<T: AsRef<[u8]> + Send + Sync>(&self, alias: T) -> Result<Option<Cid>> {
        self.storage.resolve(alias.as_ref())
    }

    /// Returns a list of aliases preventing a `Cid` from being garbage collected.
    pub fn reverse_alias(&self, cid: &Cid) -> Result<Option<Vec<Vec<u8>>>> {
        self.storage.reverse_alias(cid)
    }

    /// Flushes the block store. After `flush` completes successfully it is guaranteed that
    /// all writes have been persisted to disk.
    pub async fn flush(&self) -> Result<()> {
        self.storage.flush().await
    }

    /// Perform a set of storage operations in a batch, and discards the result.
    ///
    /// You should keep a batch open for as short as possible.
    /// You *must not* create a batch inside a batch, or use the outer ipfs.
    pub fn read_batch<F: FnOnce(&mut Batch<'_, P>) -> Result<R>, R>(&self, f: F) -> Result<R> {
        self.storage.ro("read_batch", f)
    }

    /// Perform a set of storage operations in a batch, and stores the result.
    ///
    /// You should keep a batch open for as short as possible.
    /// You *must not* create a batch inside a batch, or use the outer ipfs.
    pub fn write_batch<R>(&self, f: impl FnOnce(&mut Batch<'_, P>) -> Result<R>) -> Result<R> {
        self.storage.rw("write_batch", f)
    }

    /// Registers prometheus metrics in a registry.
    pub fn register_metrics(&self, registry: &Registry) -> Result<()> {
        self.storage.register_metrics(registry)?;
        self.network.register_metrics(registry)?;
        Ok(())
    }

    /// Subscribes to the swarm event stream.
    pub fn swarm_events(&self) -> SwarmEvents {
        self.network.swarm_events()
    }

    /// Returns the documents both local and replicated.
    pub fn docs(&self) -> Result<Vec<DocId>> {
        self.network.docs()
    }

    /// Returns the streams both local and replicated.
    pub fn streams(&self) -> Result<Vec<StreamId>> {
        self.network.streams()
    }

    /// Returns the streams both local and replicated for the given document id.
    pub fn substreams(&self, doc: DocId) -> Result<Vec<StreamId>> {
        self.network.substreams(doc)
    }

    /// Adds the peers to a replicated stream.
    pub fn stream_add_peers(&self, doc: DocId, peers: impl Iterator<Item = PeerId>) {
        self.network.stream_add_peers(doc, peers)
    }

    /// Returns the current head of a stream.
    pub fn stream_head(&self, id: &StreamId) -> Result<Option<SignedHead>> {
        self.network.stream_head(id)
    }

    /// Returns a reader for the slice.
    pub fn stream_slice(&self, id: &StreamId, start: u64, len: u64) -> Result<StreamReader> {
        self.network.stream_slice(id, start, len)
    }

    /// Removes a local or replicated stream.
    pub fn stream_remove(&self, id: &StreamId) -> Result<()> {
        self.network.stream_remove(id)
    }

    /// Returns a writter to append to a local stream.
    pub fn stream_append(&self, id: DocId) -> Result<LocalStreamWriter> {
        self.network.stream_append(id)
    }

    /// Subscribes to a replicated stream.
    pub fn stream_subscribe(&self, id: &StreamId) -> Result<()> {
        self.network.stream_subscribe(id)
    }

    /// Updates the head of a replicated stream.
    pub fn stream_update_head(&self, head: SignedHead) {
        self.network.stream_update_head(head)
    }
}

#[async_trait]
impl<P: StoreParams> Store for Ipfs<P>
where
    Ipld: References<P::Codecs>,
{
    type Params = P;
    type TempPin = Arc<TempPin>;

    fn create_temp_pin(&self) -> Result<Self::TempPin> {
        Ok(Arc::new(Ipfs::create_temp_pin(self)?))
    }

    fn temp_pin(&self, tmp: &Self::TempPin, cid: &Cid) -> Result<()> {
        Ipfs::temp_pin(self, tmp, cid)
    }

    fn contains(&self, cid: &Cid) -> Result<bool> {
        Ipfs::contains(self, cid)
    }

    fn get(&self, cid: &Cid) -> Result<Block<P>> {
        Ipfs::get(self, cid)
    }

    fn insert(&self, block: &Block<P>) -> Result<()> {
        let _ = Ipfs::insert(self, block)?;
        Ok(())
    }

    fn alias<T: AsRef<[u8]> + Send + Sync>(&self, alias: T, cid: Option<&Cid>) -> Result<()> {
        Ipfs::alias(self, alias, cid)
    }

    fn resolve<T: AsRef<[u8]> + Send + Sync>(&self, alias: T) -> Result<Option<Cid>> {
        Ipfs::resolve(self, alias)
    }

    fn reverse_alias(&self, cid: &Cid) -> Result<Option<Vec<Vec<u8>>>> {
        Ipfs::reverse_alias(self, cid)
    }

    async fn flush(&self) -> Result<()> {
        Ipfs::flush(self).await
    }

    async fn fetch(&self, cid: &Cid) -> Result<Block<Self::Params>> {
        Ipfs::fetch(self, cid, self.peers()).await
    }

    async fn sync(&self, cid: &Cid) -> Result<()> {
        Ipfs::sync(self, cid, self.peers()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::future::timeout;
    use futures::join;
    use futures::stream::StreamExt;
    use libipld::cbor::DagCborCodec;
    use libipld::multihash::Code;
    use libipld::raw::RawCodec;
    use libipld::store::DefaultParams;
    use libipld::{alias, ipld};
    use std::time::Duration;
    use tempdir::TempDir;

    fn tracing_try_init() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init()
            .ok();
    }

    async fn create_store(enable_mdns: bool) -> Result<(Ipfs<DefaultParams>, TempDir)> {
        let tmp = TempDir::new("ipfs-embed")?;
        let sweep_interval = Duration::from_millis(10000);
        let storage = StorageConfig::new(None, None, 10, sweep_interval);

        let mut network = NetworkConfig::new(tmp.path().into(), generate_keypair());
        if !enable_mdns {
            network.mdns = None;
        }

        let ipfs = Ipfs::new(Config { storage, network }).await?;
        ipfs.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())?
            .next()
            .await
            .unwrap();
        Ok((ipfs, tmp))
    }

    fn create_block(bytes: &[u8]) -> Result<Block<DefaultParams>> {
        Block::encode(RawCodec, Code::Blake3_256, bytes)
    }

    #[async_std::test]
    async fn test_local_store() -> Result<()> {
        tracing_try_init();
        let (store, _tmp) = create_store(false).await?;
        let block = create_block(b"test_local_store")?;
        let tmp = store.create_temp_pin()?;
        store.temp_pin(&tmp, block.cid())?;
        let _ = store.insert(&block)?;
        let block2 = store.get(block.cid())?;
        assert_eq!(block.data(), block2.data());
        Ok(())
    }

    #[async_std::test]
    #[ignore] // test is too unreliable for ci
    async fn test_exchange_mdns() -> Result<()> {
        tracing_try_init();
        let (store1, _tmp) = create_store(true).await?;
        let (store2, _tmp) = create_store(true).await?;
        let block = create_block(b"test_exchange_mdns")?;
        let tmp1 = store1.create_temp_pin()?;
        store1.temp_pin(&tmp1, block.cid())?;
        let _ = store1.insert(&block)?;
        store1.flush().await?;
        let tmp2 = store2.create_temp_pin()?;
        store2.temp_pin(&tmp2, block.cid())?;
        let block2 = store2
            .fetch(block.cid(), vec![store1.local_peer_id()])
            .await?;
        assert_eq!(block.data(), block2.data());
        Ok(())
    }

    #[async_std::test]
    #[ignore] // test is too unreliable for ci
    async fn test_exchange_kad() -> Result<()> {
        tracing_try_init();
        let (store, _tmp) = create_store(false).await?;
        let (store1, _tmp) = create_store(false).await?;
        let (store2, _tmp) = create_store(false).await?;

        let addr = store.listeners()[0].clone();
        let peer_id = store.local_peer_id();
        let nodes = [(peer_id, addr)];

        let b1 = store1.bootstrap(&nodes);
        let b2 = store2.bootstrap(&nodes);
        let (r1, r2) = join!(b1, b2);
        r1.unwrap();
        r2.unwrap();

        let block = create_block(b"test_exchange_kad")?;
        let key = Key::new(&block.cid().to_bytes());
        let tmp1 = store1.create_temp_pin()?;
        store1.temp_pin(&tmp1, block.cid())?;
        store1.insert(&block)?;
        store1.provide(key.clone()).await?;
        store1.flush().await?;

        let tmp2 = store2.create_temp_pin()?;
        store2.temp_pin(&tmp2, block.cid())?;
        let providers = store2.providers(key).await?;
        let block2 = store2
            .fetch(block.cid(), providers.into_iter().collect())
            .await?;
        assert_eq!(block.data(), block2.data());
        Ok(())
    }

    #[async_std::test]
    async fn test_provider_not_found() -> Result<()> {
        tracing_try_init();
        let (store1, _tmp) = create_store(true).await?;
        let block = create_block(b"test_provider_not_found")?;
        if store1
            .fetch(block.cid(), vec![store1.local_peer_id()])
            .await
            .unwrap_err()
            .downcast_ref::<BlockNotFound>()
            .is_none()
        {
            panic!("expected block not found error");
        }
        Ok(())
    }

    macro_rules! assert_pinned {
        ($store:expr, $block:expr) => {
            assert_eq!(
                $store
                    .reverse_alias($block.cid())
                    .unwrap()
                    .map(|a| !a.is_empty()),
                Some(true)
            );
        };
    }

    macro_rules! assert_unpinned {
        ($store:expr, $block:expr) => {
            assert_eq!(
                $store
                    .reverse_alias($block.cid())
                    .unwrap()
                    .map(|a| !a.is_empty()),
                Some(false)
            );
        };
    }

    fn create_ipld_block(ipld: &Ipld) -> Result<Block<DefaultParams>> {
        Block::encode(DagCborCodec, Code::Blake3_256, ipld)
    }

    #[async_std::test]
    async fn test_sync() -> Result<()> {
        tracing_try_init();
        let (local1, _tmp) = create_store(false).await?;
        let (local2, _tmp) = create_store(false).await?;
        local1.add_address(&local2.local_peer_id(), local2.listeners()[0].clone());
        local2.add_address(&local1.local_peer_id(), local1.listeners()[0].clone());

        let a1 = create_ipld_block(&ipld!({ "a": 0 }))?;
        let b1 = create_ipld_block(&ipld!({ "b": 0 }))?;
        let c1 = create_ipld_block(&ipld!({ "c": [a1.cid(), b1.cid()] }))?;
        let b2 = create_ipld_block(&ipld!({ "b": 1 }))?;
        let c2 = create_ipld_block(&ipld!({ "c": [a1.cid(), b2.cid()] }))?;
        let x = alias!(x);

        let _ = local1.insert(&a1)?;
        let _ = local1.insert(&b1)?;
        let _ = local1.insert(&c1)?;
        local1.alias(x, Some(c1.cid()))?;
        local1.flush().await?;
        assert_pinned!(&local1, &a1);
        assert_pinned!(&local1, &b1);
        assert_pinned!(&local1, &c1);

        local2.alias(&x, Some(c1.cid()))?;
        local2.sync(c1.cid(), vec![local1.local_peer_id()]).await?;
        local2.flush().await?;
        assert_pinned!(&local2, &a1);
        assert_pinned!(&local2, &b1);
        assert_pinned!(&local2, &c1);

        let _ = local2.insert(&b2)?;
        let _ = local2.insert(&c2)?;
        local2.alias(x, Some(c2.cid()))?;
        local2.flush().await?;
        assert_pinned!(&local2, &a1);
        assert_unpinned!(&local2, &b1);
        assert_unpinned!(&local2, &c1);
        assert_pinned!(&local2, &b2);
        assert_pinned!(&local2, &c2);

        local1.alias(x, Some(c2.cid()))?;
        local1.sync(c2.cid(), vec![local2.local_peer_id()]).await?;
        local1.flush().await?;
        assert_pinned!(&local1, &a1);
        assert_unpinned!(&local1, &b1);
        assert_unpinned!(&local1, &c1);
        assert_pinned!(&local1, &b2);
        assert_pinned!(&local1, &c2);

        local2.alias(x, None)?;
        local2.flush().await?;
        assert_unpinned!(&local2, &a1);
        assert_unpinned!(&local2, &b1);
        assert_unpinned!(&local2, &c1);
        assert_unpinned!(&local2, &b2);
        assert_unpinned!(&local2, &c2);

        local1.alias(x, None)?;
        local2.flush().await?;
        assert_unpinned!(&local1, &a1);
        assert_unpinned!(&local1, &b1);
        assert_unpinned!(&local1, &c1);
        assert_unpinned!(&local1, &b2);
        assert_unpinned!(&local1, &c2);
        Ok(())
    }

    #[async_std::test]
    #[allow(clippy::eval_order_dependence)]
    async fn test_dht_record() -> Result<()> {
        tracing_try_init();
        let stores = [create_store(false).await?, create_store(false).await?];
        async_std::task::sleep(Duration::from_millis(100)).await;
        stores[0]
            .0
            .bootstrap(&[(
                stores[1].0.local_peer_id(),
                stores[1].0.listeners()[0].clone(),
            )])
            .await?;
        stores[1]
            .0
            .bootstrap(&[(
                stores[0].0.local_peer_id(),
                stores[0].0.listeners()[0].clone(),
            )])
            .await?;
        let key: Key = b"key".to_vec().into();

        stores[0]
            .0
            .put_record(
                Record::new(key.clone(), b"hello world".to_vec()),
                Quorum::One,
            )
            .await?;
        let records = stores[1].0.get_record(&key, Quorum::One).await?;
        assert_eq!(records.len(), 1);
        Ok(())
    }

    #[async_std::test]
    #[allow(clippy::eval_order_dependence)]
    async fn test_gossip_and_broadcast() -> Result<()> {
        tracing_try_init();
        let stores = [
            create_store(false).await?,
            create_store(false).await?,
            create_store(false).await?,
            create_store(false).await?,
            create_store(false).await?,
            create_store(false).await?,
        ];
        let mut subscriptions = vec![];
        let topic = "topic";
        for (store, _) in &stores {
            for (other, _) in &stores {
                if store.local_peer_id() != other.local_peer_id() {
                    store.dial_address(&other.local_peer_id(), other.listeners()[0].clone());
                }
            }
        }

        async_std::task::sleep(Duration::from_millis(500)).await;
        // Make sure everyone is peered before subscribing
        for (store, _) in &stores {
            subscriptions.push(store.subscribe(topic)?);
        }
        async_std::task::sleep(Duration::from_millis(500)).await;

        stores[0]
            .0
            .publish(topic, b"hello gossip".to_vec())
            .unwrap();

        for (idx, subscription) in subscriptions.iter_mut().enumerate() {
            let mut expected = stores
                .iter()
                .enumerate()
                .filter_map(|(i, s)| {
                    if i == idx {
                        None
                    } else {
                        Some(s.0.local_peer_id())
                    }
                })
                .flat_map(|p| {
                    // once for gossipsub, once for broadcast
                    vec![GossipEvent::Subscribed(p), GossipEvent::Subscribed(p)].into_iter()
                })
                .chain(if idx != 0 {
                    // store 0 is the sender
                    Box::new(std::iter::once(GossipEvent::Message(
                        stores[0].0.local_peer_id(),
                        b"hello gossip".to_vec().into(),
                    ))) as Box<dyn Iterator<Item = GossipEvent>>
                } else {
                    Box::new(std::iter::empty())
                })
                .collect::<Vec<GossipEvent>>();
            while !expected.is_empty() {
                let ev = timeout(Duration::from_millis(100), subscription.next())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(expected.contains(&ev));
                if let Some(idx) = expected.iter().position(|e| e == &ev) {
                    // Can't retain, as there might be multiple messages
                    expected.remove(idx);
                }
            }
        }

        // Check broadcast subscription
        stores[0]
            .0
            .broadcast(topic, b"hello broadcast".to_vec())
            .unwrap();

        for subscription in &mut subscriptions[1..] {
            if let GossipEvent::Message(p, data) = subscription.next().await.unwrap() {
                assert_eq!(p, stores[0].0.local_peer_id());
                assert_eq!(data[..], b"hello broadcast"[..]);
            } else {
                panic!()
            }
        }

        // trigger cleanup
        stores[0]
            .0
            .broadcast(topic, b"r u still listening?".to_vec())
            .unwrap();

        let mut last_sub = subscriptions.drain(..1).next().unwrap();
        drop(subscriptions);
        let mut expected = stores[1..]
            .iter()
            .map(|s| s.0.local_peer_id())
            .flat_map(|p| {
                // once for gossipsub, once for broadcast
                vec![GossipEvent::Unsubscribed(p), GossipEvent::Unsubscribed(p)].into_iter()
            })
            .collect::<Vec<_>>();
        while !expected.is_empty() {
            let ev = timeout(Duration::from_millis(100), last_sub.next())
                .await
                .unwrap()
                .unwrap();
            assert!(expected.contains(&ev));
            if let Some(idx) = expected.iter().position(|e| e == &ev) {
                // Can't retain, as there might be multiple messages
                expected.remove(idx);
            }
        }
        Ok(())
    }

    #[async_std::test]
    async fn test_batch_read() -> Result<()> {
        tracing_try_init();
        let tmp = TempDir::new("ipfs-embed")?;
        let network = NetworkConfig::new(tmp.path().into(), generate_keypair());
        let storage = StorageConfig::new(None, None, 1000000, Duration::from_secs(3600));
        let ipfs = Ipfs::<DefaultParams>::new(Config { storage, network }).await?;
        let a = create_block(b"a")?;
        let b = create_block(b"b")?;
        ipfs.insert(&a)?;
        ipfs.insert(&b)?;
        let has_blocks =
            ipfs.read_batch(|db| Ok(db.contains(a.cid())? && db.contains(b.cid())?))?;
        assert!(has_blocks);
        Ok(())
    }

    #[async_std::test]
    async fn test_batch_write() -> Result<()> {
        tracing_try_init();
        let tmp = TempDir::new("ipfs-embed")?;
        let network = NetworkConfig::new(tmp.path().into(), generate_keypair());
        let storage = StorageConfig::new(None, None, 1000000, Duration::from_secs(3600));
        let ipfs = Ipfs::<DefaultParams>::new(Config { storage, network }).await?;
        let a = create_block(b"a")?;
        let b = create_block(b"b")?;
        let c = create_block(b"c")?;
        let d = create_block(b"d")?;
        ipfs.write_batch(|db| {
            db.insert(&a)?;
            db.insert(&b)?;
            Ok(())
        })?;
        assert!(ipfs.contains(a.cid())? && ipfs.contains(b.cid())?);
        let _: anyhow::Result<()> = ipfs.write_batch(|db| {
            db.insert(&c)?;
            db.insert(&d)?;
            anyhow::bail!("nope!");
        });
        assert!(!ipfs.contains(c.cid())? && ipfs.contains(b.cid())?);
        Ok(())
    }

    #[async_std::test]
    #[ignore]
    async fn test_bitswap_sync_chain() -> Result<()> {
        use std::time::Instant;
        tracing_try_init();
        let (a, _tmp) = create_store(true).await?;
        let (b, _tmp) = create_store(true).await?;
        let root = alias!(root);

        let (cid, blocks) = test_util::build_tree(1, 1000)?;
        a.alias(root, Some(&cid))?;
        b.alias(root, Some(&cid))?;

        let size: usize = blocks.iter().map(|block| block.data().len()).sum();
        tracing::info!("chain built {} blocks, {} bytes", blocks.len(), size);
        for block in blocks.iter() {
            let _ = a.insert(block)?;
        }
        a.flush().await?;

        let t0 = Instant::now();
        let _ = b
            .sync(&cid, vec![a.local_peer_id()])
            .for_each(|x| async move { tracing::debug!("sync progress {:?}", x) })
            .await;
        b.flush().await?;
        tracing::info!(
            "chain sync complete {} ms {} blocks {} bytes!",
            t0.elapsed().as_millis(),
            blocks.len(),
            size
        );
        for block in blocks {
            let data = b.get(block.cid())?;
            assert_eq!(data, block);
        }

        Ok(())
    }

    #[async_std::test]
    #[ignore]
    async fn test_bitswap_sync_tree() -> Result<()> {
        use std::time::Instant;
        tracing_try_init();
        let (a, _tmp) = create_store(true).await?;
        let (b, _tmp) = create_store(true).await?;
        let root = alias!(root);

        let (cid, blocks) = test_util::build_tree(10, 4)?;
        a.alias(root, Some(&cid))?;
        b.alias(root, Some(&cid))?;

        let size: usize = blocks.iter().map(|block| block.data().len()).sum();
        tracing::info!("chain built {} blocks, {} bytes", blocks.len(), size);
        for block in blocks.iter() {
            let _ = a.insert(block)?;
        }
        a.flush().await?;

        let t0 = Instant::now();
        let _ = b
            .sync(&cid, vec![a.local_peer_id()])
            .for_each(|x| async move { tracing::debug!("sync progress {:?}", x) })
            .await;
        b.flush().await?;
        tracing::info!(
            "tree sync complete {} ms {} blocks {} bytes!",
            t0.elapsed().as_millis(),
            blocks.len(),
            size
        );
        for block in blocks {
            let data = b.get(block.cid())?;
            assert_eq!(data, block);
        }
        Ok(())
    }
}
