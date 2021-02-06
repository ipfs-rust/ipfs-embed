//! IpfsEmbed is an embeddable ipfs implementation.
//!
//! ```
//! # #[async_std::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # use ipfs_embed::{Config, DefaultParams, Ipfs};
//! # let cache_size = 100;
//! let ipfs = Ipfs::<DefaultParams>::new(Config::new(None, cache_size)).await?;
//! ipfs.listen_on("/ip4/0.0.0.0/tcp/0".parse()?).await?;
//! # Ok(()) }
//! ```
use async_trait::async_trait;
use futures::channel::mpsc;
use futures::stream::{Stream, StreamExt};
pub use ipfs_embed_net::SyncEvent;
pub use ipfs_embed_net::{
    AddressRecord, Key, Multiaddr, NetworkConfig, PeerId, PeerRecord, Quorum, Record, SyncQuery,
};
use ipfs_embed_net::{BitswapStore, NetworkService};
pub use ipfs_embed_sqlite::{StorageConfig, TempPin};
use ipfs_embed_sqlite::{StorageEvent, StorageService};
use libipld::codec::References;
use libipld::error::BlockNotFound;
pub use libipld::store::DefaultParams;
use libipld::store::{Store, StoreParams};
use libipld::{Block, Cid, Ipld, Result};
use prometheus::{Encoder, Registry};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct Config {
    pub storage: StorageConfig,
    pub network: NetworkConfig,
}

impl Config {
    pub fn new(path: Option<std::path::PathBuf>, cache_size: u64) -> Self {
        let sweep_interval = std::time::Duration::from_millis(10000);
        let storage = StorageConfig::new(path, cache_size, sweep_interval);
        let network = NetworkConfig::new();
        Self { storage, network }
    }
}

#[derive(Clone)]
pub struct Ipfs<P: StoreParams> {
    storage: StorageService<P>,
    network: NetworkService<P>,
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
    pub async fn new(config: Config) -> Result<Self> {
        let (tx, mut storage_events) = mpsc::unbounded();
        let storage = StorageService::open(config.storage, tx)?;
        let bitswap = BitswapStorage(storage.clone());
        let network = NetworkService::new(config.network, bitswap).await?;
        let network2 = network.clone();
        async_global_executor::spawn(async move {
            while let Some(StorageEvent::Remove(cid)) = storage_events.next().await {
                network2.unprovide(cid);
            }
        })
        .detach();
        Ok(Self { storage, network })
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.network.local_peer_id()
    }

    pub async fn listen_on(&self, addr: Multiaddr) -> Result<Multiaddr> {
        self.network.listen_on(addr).await
    }

    pub fn listeners(&self) -> Vec<Multiaddr> {
        self.network.listeners()
    }

    pub fn add_external_address(&self, addr: Multiaddr) {
        self.network.add_external_address(addr)
    }

    pub fn external_addresses(&self) -> Vec<AddressRecord> {
        self.network.external_addresses()
    }

    pub fn add_address(&self, peer: &PeerId, addr: Multiaddr) {
        self.network.add_address(peer, addr)
    }

    pub fn remove_address(&self, peer: &PeerId, addr: &Multiaddr) {
        self.network.remove_address(peer, addr)
    }

    pub fn dial(&self, peer: &PeerId) -> Result<()> {
        self.network.dial(peer)
    }

    pub fn dial_address(&self, peer: &PeerId, addr: Multiaddr) -> Result<()> {
        self.network.add_address(peer, addr);
        self.network.dial(peer)
    }

    pub fn ban(&self, peer: PeerId) {
        self.network.ban(peer)
    }

    pub fn unban(&self, peer: PeerId) {
        self.network.unban(peer)
    }

    pub async fn bootstrap(&self, nodes: &[(PeerId, Multiaddr)]) -> Result<()> {
        self.network.bootstrap(nodes).await?;
        for cid in self.storage.iter()? {
            let _ = self.network.provide(cid);
        }
        Ok(())
    }

    pub async fn get_record(&self, key: &Key, quorum: Quorum) -> Result<Vec<PeerRecord>> {
        self.network.get_record(key, quorum).await
    }

    pub async fn put_record(&self, record: Record, quorum: Quorum) -> Result<()> {
        self.network.put_record(record, quorum).await
    }

    pub fn remove_record(&self, key: &Key) {
        self.network.remove_record(key)
    }

    pub fn subscribe(&self, topic: &str) -> Result<impl Stream<Item = Vec<u8>>> {
        self.network.subscribe(topic)
    }

    pub fn publish(&self, topic: &str, msg: Vec<u8>) -> Result<()> {
        self.network.publish(topic, msg)
    }

    pub fn create_temp_pin(&self) -> Result<TempPin> {
        self.storage.create_temp_pin()
    }

    pub fn temp_pin(&self, tmp: &TempPin, cid: &Cid) -> Result<()> {
        self.storage.temp_pin(tmp, std::iter::once(*cid))
    }

    pub fn iter(&self) -> Result<impl Iterator<Item = Cid>> {
        self.storage.iter()
    }

    pub fn contains(&self, cid: &Cid) -> Result<bool> {
        self.storage.contains(cid)
    }

    pub fn get(&self, cid: &Cid) -> Result<Block<P>> {
        if let Some(data) = self.storage.get(cid)? {
            let block = Block::new_unchecked(*cid, data);
            Ok(block)
        } else {
            Err(BlockNotFound(*cid).into())
        }
    }

    pub async fn fetch(&self, cid: &Cid) -> Result<Block<P>> {
        if let Some(data) = self.storage.get(cid)? {
            let block = Block::new_unchecked(*cid, data);
            return Ok(block);
        }
        self.network.get(*cid).await?;
        if let Some(data) = self.storage.get(cid)? {
            let block = Block::new_unchecked(*cid, data);
            return Ok(block);
        }
        tracing::error!("block evicted too soon. use a temp pin to keep the block around.");
        Err(BlockNotFound(*cid).into())
    }

    pub fn insert(&self, block: &Block<P>) -> Result<impl Future<Output = Result<()>> + '_> {
        let cid = *block.cid();
        self.storage.insert(block)?;
        Ok(self.network.provide(cid))
    }

    pub async fn evict(&self) -> Result<()> {
        self.storage.evict().await
    }

    pub fn sync(&self, cid: &Cid) -> SyncQuery<P> {
        let missing = self.storage.missing_blocks(cid).ok().unwrap_or_default();
        self.network.sync(*cid, missing.into_iter())
    }

    pub fn alias<T: AsRef<[u8]> + Send + Sync>(&self, alias: T, cid: Option<&Cid>) -> Result<()> {
        self.storage.alias(alias.as_ref(), cid)
    }

    pub fn resolve<T: AsRef<[u8]> + Send + Sync>(&self, alias: T) -> Result<Option<Cid>> {
        self.storage.resolve(alias.as_ref())
    }

    pub fn reverse_alias(&self, cid: &Cid) -> Result<Option<Vec<Vec<u8>>>> {
        self.storage.reverse_alias(cid)
    }

    pub async fn flush(&self) -> Result<()> {
        self.storage.flush().await
    }

    pub fn register_metrics(&self, registry: &Registry) -> Result<()> {
        self.storage.register_metrics(registry)?;
        self.network.register_metrics(registry)?;
        Ok(())
    }
}

/// Telemetry server
pub fn telemetry<P: StoreParams>(addr: SocketAddr, ipfs: &Ipfs<P>) -> Result<()>
where
    Ipld: References<P::Codecs>,
{
    let registry = prometheus::default_registry();
    ipfs.register_metrics(registry)?;
    let mut s = tide::new();
    s.at("/metrics").get(get_metric);
    async_global_executor::spawn(async move { s.listen(addr).await }).detach();
    Ok(())
}

/// Return metrics to prometheus
async fn get_metric(_: tide::Request<()>) -> tide::Result {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];

    encoder.encode(&metric_families, &mut buffer).unwrap();

    let response = tide::Response::builder(200)
        .content_type("text/plain; version=0.0.4")
        .body(tide::Body::from(buffer))
        .build();

    Ok(response)
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
        Ipfs::fetch(self, cid).await
    }

    async fn sync(&self, cid: &Cid) -> Result<()> {
        Ipfs::sync(self, cid).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::join;
    use libipld::cbor::DagCborCodec;
    use libipld::multihash::Code;
    use libipld::raw::RawCodec;
    use libipld::store::DefaultParams;
    use libipld::{alias, ipld};
    use std::time::Duration;

    fn tracing_try_init() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init()
            .ok();
    }

    async fn create_store(enable_mdns: bool) -> Result<Ipfs<DefaultParams>> {
        let sweep_interval = Duration::from_millis(10000);
        let storage = StorageConfig::new(None, 10, sweep_interval);

        let mut network = NetworkConfig::new();
        network.enable_mdns = enable_mdns;
        network.allow_non_globals_in_dht = true;

        let ipfs = Ipfs::new(Config { storage, network }).await?;
        ipfs.listen_on("/ip4/127.0.0.1/tcp/0".parse()?).await?;
        Ok(ipfs)
    }

    fn create_block(bytes: &[u8]) -> Result<Block<DefaultParams>> {
        Block::encode(RawCodec, Code::Blake3_256, bytes)
    }

    #[async_std::test]
    async fn test_local_store() -> Result<()> {
        tracing_try_init();
        let store = create_store(false).await?;
        let block = create_block(b"test_local_store")?;
        let tmp = store.create_temp_pin()?;
        store.temp_pin(&tmp, block.cid())?;
        let _ = store.insert(&block)?;
        let block2 = store.get(block.cid())?;
        assert_eq!(block.data(), block2.data());
        Ok(())
    }

    #[async_std::test]
    #[cfg(not(target_os = "macos"))] // mdns doesn't work on macos in github actions
    async fn test_exchange_mdns() -> Result<()> {
        tracing_try_init();
        let store1 = create_store(true).await?;
        let store2 = create_store(true).await?;
        let block = create_block(b"test_exchange_mdns")?;
        let tmp1 = store1.create_temp_pin()?;
        store1.temp_pin(&tmp1, block.cid())?;
        let _ = store1.insert(&block)?;
        store1.flush().await?;
        let tmp2 = store2.create_temp_pin()?;
        store2.temp_pin(&tmp2, block.cid())?;
        let block2 = store2.fetch(block.cid()).await?;
        assert_eq!(block.data(), block2.data());
        Ok(())
    }

    #[async_std::test]
    async fn test_exchange_kad() -> Result<()> {
        tracing_try_init();
        let store = create_store(false).await?;
        let store1 = create_store(false).await?;
        let store2 = create_store(false).await?;

        let addr = store.listeners()[0].clone();
        let peer_id = store.local_peer_id();
        let nodes = [(peer_id, addr)];

        let b1 = store1.bootstrap(&nodes);
        let b2 = store2.bootstrap(&nodes);
        let (r1, r2) = join!(b1, b2);
        r1.unwrap();
        r2.unwrap();

        let block = create_block(b"test_exchange_kad")?;
        let tmp1 = store1.create_temp_pin()?;
        store1.temp_pin(&tmp1, block.cid())?;
        store1.insert(&block)?.await?;
        store1.flush().await?;

        let tmp2 = store2.create_temp_pin()?;
        store2.temp_pin(&tmp2, block.cid())?;
        let block2 = store2.fetch(block.cid()).await?;
        assert_eq!(block.data(), block2.data());
        Ok(())
    }

    #[async_std::test]
    async fn test_provider_not_found() -> Result<()> {
        tracing_try_init();
        let store1 = create_store(true).await?;
        let block = create_block(b"test_provider_not_found")?;
        if store1
            .fetch(block.cid())
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
        let local1 = create_store(true).await?;
        let local2 = create_store(true).await?;
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
        local2.sync(c1.cid()).await?;
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
        local1.sync(c2.cid()).await?;
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
        stores[0]
            .bootstrap(&[(stores[1].local_peer_id(), stores[1].listeners()[0].clone())])
            .await?;
        stores[1]
            .bootstrap(&[(stores[0].local_peer_id(), stores[0].listeners()[0].clone())])
            .await?;
        let key: Key = b"key".to_vec().into();

        stores[0]
            .put_record(
                Record::new(key.clone(), b"hello world".to_vec()),
                Quorum::One,
            )
            .await?;
        let records = stores[1].get_record(&key, Quorum::One).await?;
        assert_eq!(records.len(), 1);
        Ok(())
    }

    #[async_std::test]
    #[allow(clippy::eval_order_dependence)]
    async fn test_gossip() -> Result<()> {
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
        for store in &stores {
            for other in &stores {
                if store.local_peer_id() != other.local_peer_id() {
                    store.dial_address(&other.local_peer_id(), other.listeners()[0].clone())?;
                }
            }
            subscriptions.push(store.subscribe(topic)?);
        }

        async_std::task::sleep(Duration::from_millis(500)).await;

        stores[0].publish(&topic, b"hello world".to_vec()).unwrap();

        for subscription in &mut subscriptions[1..] {
            if let Some(msg) = subscription.next().await {
                assert_eq!(msg.as_slice(), &b"hello world"[..]);
            }
        }
        Ok(())
    }
}
