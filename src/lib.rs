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
use futures::stream::FuturesUnordered;
use futures::{select, stream::FusedStream, FutureExt, Stream, StreamExt};
use ipfs_embed_net::{
    AddressRecord, Multiaddr, NetworkConfig, NetworkEvent, NetworkService, NetworkStats, PeerId,
};
use ipfs_embed_sqlite::{AsyncTempPin, StorageConfig, StorageEvent, StorageService, StorageStats};
use libipld::codec::References;
use libipld::error::BlockNotFound;
pub use libipld::store::DefaultParams;
use libipld::store::{Store, StoreParams};
use libipld::{Block, Cid, Ipld, Result};
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};

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

pub struct Stats {
    pub storage: StorageStats,
    pub network: NetworkStats,
}

#[derive(Clone)]
pub struct Ipfs<P: StoreParams> {
    storage: StorageService<P>,
    network: NetworkService<P>,
}

struct Forever<S>(S);

impl<S> Deref for Forever<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S> DerefMut for Forever<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<S: Stream + Unpin> Stream for Forever<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.0).poll_next(cx) {
            Poll::Ready(None) => Poll::Pending,
            p => p,
        }
    }
}

impl<S: Stream + Unpin> FusedStream for Forever<S> {
    fn is_terminated(&self) -> bool {
        false
    }
}

impl<P: StoreParams> Ipfs<P>
where
    Ipld: References<P::Codecs>,
{
    pub async fn new(config: Config) -> Result<Self> {
        let (tx, mut storage_events) = mpsc::unbounded();
        let storage = StorageService::open(config.storage, tx)?;
        let storage2 = storage.clone();
        let (tx, mut network_events) = mpsc::unbounded();
        let network = NetworkService::new(config.network, tx).await?;
        let network2 = network.clone();
        async_global_executor::spawn(async move {
            let mut missing_blocks = Forever(FuturesUnordered::new());
            let mut contains = Forever(FuturesUnordered::new());
            let mut get = Forever(FuturesUnordered::new());
            let mut insert = Forever(FuturesUnordered::new());
            loop {
                select! {
                    ev = network_events.next() => {
                        if let Some(ev) = ev {
                            match ev {
                                NetworkEvent::MissingBlocks(id, cid) => {
                                    missing_blocks.push(storage.missing_blocks(cid).map(move |res| (res, id)));
                                }
                                NetworkEvent::Have(ch, _, cid) => {
                                    let storage = storage.clone();
                                    contains.push(async move {
                                        (storage.contains(&cid).await, ch)
                                    });
                                }
                                NetworkEvent::Block(ch, _, cid) => {
                                    get.push(storage.get(cid).map(move |res| (res, ch)));
                                }
                                NetworkEvent::Received(_, _, block) => {
                                    insert.push(storage.insert(block, None));
                                }
                            }
                        }
                    }
                    ev = storage_events.next() => {
                        if let Some(ev) = ev {
                            match ev {
                                StorageEvent::Remove(cid) => {
                                    network.unprovide(cid);
                                }
                            }
                        }
                    }
                    ev = missing_blocks.next() => {
                        if let Some((res, id)) = ev {
                            match res {
                                Ok(missing) => { network.inject_missing_blocks(id, missing); }
                                Err(err) => { tracing::error!("{}", err); }
                            }
                        }
                    }
                    ev = contains.next() => {
                        if let Some((res, ch)) = ev {
                            match res {
                                Ok(have) => { network.inject_have(ch, have); }
                                Err(err) => { tracing::error!("{}", err); }
                            }
                        }
                    }
                    ev = get.next() => {
                        if let Some((res, ch)) = ev {
                            match res {
                                Ok(block) => { network.inject_block(ch, block); }
                                Err(err) => { tracing::error!("{}", err); }
                            }
                        }
                    }
                    ev = insert.next() => {
                        if let Some(res) = ev {
                            match res {
                                Ok(()) => {}
                                Err(err) => { tracing::error!("{}", err); }
                            }
                        }
                    }
                }
            }
        })
        .detach();
        Ok(Self {
            storage: storage2,
            network: network2,
        })
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

    pub fn ban(&self, peer: PeerId) {
        self.network.ban(peer)
    }

    pub fn unban(&self, peer: PeerId) {
        self.network.unban(peer)
    }

    pub async fn bootstrap(&self, nodes: &[(PeerId, Multiaddr)]) -> Result<()> {
        self.network.bootstrap(nodes).await?;
        for cid in self.storage.iter().await? {
            let _ = self.network.provide(cid);
        }
        Ok(())
    }

    pub async fn temp_pin(&self) -> Result<AsyncTempPin> {
        self.storage.temp_pin().await
    }

    pub async fn iter(&self) -> Result<std::vec::IntoIter<Cid>> {
        self.storage.iter().await
    }

    pub async fn contains(&self, cid: &Cid) -> Result<bool> {
        self.storage.contains(cid).await
    }

    pub async fn get(&self, cid: Cid) -> Result<Block<P>> {
        if let Some(data) = self.storage.get(cid).await? {
            let block = Block::new_unchecked(cid, data);
            return Ok(block);
        }
        self.network.get(cid).await?;
        if let Some(data) = self.storage.get(cid).await? {
            let block = Block::new_unchecked(cid, data);
            return Ok(block);
        }
        tracing::error!("block evicted too soon");
        Err(BlockNotFound(cid).into())
    }

    pub async fn insert(
        &self,
        block: Block<P>,
        tmp: Option<&AsyncTempPin>,
    ) -> Result<impl Future<Output = Result<()>> + '_> {
        let cid = *block.cid();
        self.storage.insert(block, tmp).await?;
        Ok(self.network.provide(cid))
    }

    pub async fn evict(&self) -> Result<()> {
        self.storage.evict().await
    }

    pub async fn alias<T: AsRef<[u8]> + Send + Sync>(
        &self,
        alias: T,
        cid: Option<Cid>,
    ) -> Result<()> {
        self.storage.alias(alias.as_ref().to_vec(), cid).await?;
        if let Some(cid) = cid {
            let missing = self.storage.missing_blocks(cid).await?;
            if !missing.is_empty() {
                self.network.sync(cid, missing.into_iter()).await?;
            }
        }
        Ok(())
    }

    pub async fn resolve<T: AsRef<[u8]> + Send + Sync>(&self, alias: T) -> Result<Option<Cid>> {
        self.storage.resolve(alias.as_ref().to_vec()).await
    }

    pub async fn pinned(&self, cid: Cid) -> Result<Option<Vec<Vec<u8>>>> {
        self.storage.pinned(cid).await
    }

    pub async fn stats(&self) -> Result<Stats> {
        Ok(Stats {
            storage: self.storage.stats().await?,
            network: self.network.stats(),
        })
    }
}

#[async_trait]
impl<P: StoreParams> Store for Ipfs<P>
where
    Ipld: References<P::Codecs>,
{
    type Params = P;

    async fn get(&self, cid: &Cid) -> Result<Block<P>> {
        Ipfs::get(self, *cid).await
    }

    async fn insert(&self, block: &Block<P>) -> Result<()> {
        let _ = Ipfs::insert(self, block.clone(), None).await?;
        Ok(())
    }

    async fn alias<T: AsRef<[u8]> + Send + Sync>(&self, alias: T, cid: Option<&Cid>) -> Result<()> {
        Ipfs::alias(self, alias, cid.copied()).await
    }

    async fn resolve<T: AsRef<[u8]> + Send + Sync>(&self, alias: T) -> Result<Option<Cid>> {
        Ipfs::resolve(self, alias).await
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

    async fn create_store(enable_mdns: bool) -> Ipfs<DefaultParams> {
        let cache_size = 10;
        let sweep_interval = Duration::from_millis(10000);
        let storage = StorageConfig::new(None, cache_size, sweep_interval);

        let mut network = NetworkConfig::new();
        network.enable_mdns = enable_mdns;
        network.allow_non_globals_in_dht = true;

        let ipfs = Ipfs::new(Config { storage, network }).await.unwrap();
        ipfs.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .await
            .unwrap();
        ipfs
    }

    fn create_block(bytes: &[u8]) -> Block<DefaultParams> {
        Block::encode(RawCodec, Code::Blake3_256, bytes).unwrap()
    }

    #[async_std::test]
    async fn test_local_store() {
        tracing_try_init();
        let store = create_store(false).await;
        let block = create_block(b"test_local_store");
        let _ = store.insert(block.clone(), None).await.unwrap();
        let block2 = store.get(*block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    #[cfg(not(target_os = "macos"))] // mdns doesn't work on macos in github actions
    async fn test_exchange_mdns() {
        tracing_try_init();
        let store1 = create_store(true).await;
        let store2 = create_store(true).await;
        let block = create_block(b"test_exchange_mdns");
        let _ = store1.insert(block.clone(), None).await.unwrap();
        let block2 = store2.get(*block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    async fn test_exchange_kad() {
        tracing_try_init();
        let store = create_store(false).await;
        let store1 = create_store(false).await;
        let store2 = create_store(false).await;

        let addr = store.listeners()[0].clone();
        let peer_id = store.local_peer_id();
        let nodes = [(peer_id, addr)];

        let b1 = store1.bootstrap(&nodes);
        let b2 = store2.bootstrap(&nodes);
        let (r1, r2) = join!(b1, b2);
        r1.unwrap();
        r2.unwrap();

        let block = create_block(b"test_exchange_kad");
        store1
            .insert(block.clone(), None)
            .await
            .unwrap()
            .await
            .unwrap();

        let block2 = store2.get(*block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    async fn test_provider_not_found() {
        tracing_try_init();
        let store1 = create_store(true).await;
        let block = create_block(b"test_provider_not_found");
        if store1
            .get(*block.cid())
            .await
            .unwrap_err()
            .downcast_ref::<BlockNotFound>()
            .is_none()
        {
            panic!("expected block not found error");
        }
    }

    macro_rules! assert_pinned {
        ($store:expr, $block:expr) => {
            assert_eq!(
                $store
                    .pinned(*$block.cid())
                    .await
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
                    .pinned(*$block.cid())
                    .await
                    .unwrap()
                    .map(|a| !a.is_empty()),
                Some(false)
            );
        };
    }

    fn create_ipld_block(ipld: &Ipld) -> Block<DefaultParams> {
        Block::encode(DagCborCodec, Code::Blake3_256, ipld).unwrap()
    }

    #[async_std::test]
    async fn test_sync() {
        tracing_try_init();
        let local1 = create_store(true).await;
        let local2 = create_store(true).await;
        let a1 = create_ipld_block(&ipld!({ "a": 0 }));
        let b1 = create_ipld_block(&ipld!({ "b": 0 }));
        let c1 = create_ipld_block(&ipld!({ "c": [a1.cid(), b1.cid()] }));
        let b2 = create_ipld_block(&ipld!({ "b": 1 }));
        let c2 = create_ipld_block(&ipld!({ "c": [a1.cid(), b2.cid()] }));
        let x = alias!(x);

        let _ = local1.insert(a1.clone(), None).await.unwrap();
        let _ = local1.insert(b1.clone(), None).await.unwrap();
        let _ = local1.insert(c1.clone(), None).await.unwrap();
        local1.alias(x, Some(*c1.cid())).await.unwrap();
        assert_pinned!(&local1, &a1);
        assert_pinned!(&local1, &b1);
        assert_pinned!(&local1, &c1);

        local2.alias(x, Some(*c1.cid())).await.unwrap();
        assert_pinned!(&local2, &a1);
        assert_pinned!(&local2, &b1);
        assert_pinned!(&local2, &c1);

        let _ = local2.insert(b2.clone(), None).await.unwrap();
        let _ = local2.insert(c2.clone(), None).await.unwrap();
        local2.alias(x, Some(*c2.cid())).await.unwrap();
        assert_pinned!(&local2, &a1);
        assert_unpinned!(&local2, &b1);
        assert_unpinned!(&local2, &c1);
        assert_pinned!(&local2, &b2);
        assert_pinned!(&local2, &c2);

        local1.alias(x, Some(*c2.cid())).await.unwrap();
        assert_pinned!(&local1, &a1);
        assert_unpinned!(&local1, &b1);
        assert_unpinned!(&local1, &c1);
        assert_pinned!(&local1, &b2);
        assert_pinned!(&local1, &c2);

        local2.alias(x, None).await.unwrap();
        assert_unpinned!(&local2, &a1);
        assert_unpinned!(&local2, &b1);
        assert_unpinned!(&local2, &c1);
        assert_unpinned!(&local2, &b2);
        assert_unpinned!(&local2, &c2);

        local1.alias(x, None).await.unwrap();
        assert_unpinned!(&local1, &a1);
        assert_unpinned!(&local1, &b1);
        assert_unpinned!(&local1, &c1);
        assert_unpinned!(&local1, &b2);
        assert_unpinned!(&local1, &c2);
    }
}
