//! IpfsEmbed is an embeddable ipfs implementation.
//!
//! ```
//! # #[async_std::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use ipfs_embed::Ipfs;
//! let cache_size = 100;
//! let _ipfs = Ipfs::default(None, cache_size).await?;
//! # Ok(()) }
//! ```
use async_trait::async_trait;
use futures::channel::mpsc;
use futures::select;
use futures::stream::StreamExt;
use ipfs_embed_net::{
    AddressRecord, Multiaddr, NetworkConfig, NetworkEvent, NetworkService, NetworkStats, PeerId,
};
use ipfs_embed_sqlite::{AsyncTempPin, StorageConfig, StorageEvent, StorageService, StorageStats};
use libipld::codec::References;
use libipld::error::BlockNotFound;
use libipld::store::{Store, StoreParams};
use libipld::{Block, Cid, Ipld, Result};

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

impl<P: StoreParams> Ipfs<P>
where
    Ipld: References<P::Codecs>,
{
    pub async fn new(config: Config) -> Result<Self> {
        let bootstrap = !config.network.boot_nodes.is_empty();
        let (tx, mut storage_events) = mpsc::unbounded();
        let storage = StorageService::open(config.storage, tx)?;
        let storage2 = storage.clone();
        let (tx, mut network_events) = mpsc::unbounded();
        let network = NetworkService::new(config.network, tx).await?;
        let network2 = network.clone();
        /*if bootstrap {
            network.bootstrap().await?;
            for cid in storage.iter().await? {
                let _ = network.provide(cid).await;
            }
        }*/
        async_global_executor::spawn(async move {
            loop {
                let res: Result<()> = async {
                    select! {
                        ev = network_events.next() => {
                            if let Some(ev) = ev {
                                match ev {
                                    NetworkEvent::MissingBlocks(id, cid) => {
                                        let missing = storage.missing_blocks(cid).await?;
                                        network.inject_missing_blocks(id, missing).await;
                                    }
                                    NetworkEvent::Have(ch, _, cid) => {
                                        let have = storage.contains(&cid).await?;
                                        network.inject_have(ch, have).await;
                                    }
                                    NetworkEvent::Block(ch, _, cid) => {
                                        let block = storage.get(cid).await?;
                                        network.inject_block(ch, block).await;
                                    }
                                    NetworkEvent::Received(_, _, block) => {
                                        storage.insert(block, None).await?;
                                    }
                                }
                            }
                        }
                        ev = storage_events.next() => {
                            if let Some(ev) = ev {
                                match ev {
                                    StorageEvent::Remove(cid) => {
                                        network.unprovide(cid).await;
                                    }
                                }
                            }
                        }
                    }
                    Ok(())
                }
                .await;

                if let Err(err) = res {
                    tracing::error!("{}", err);
                }
            }
        })
        .detach();
        Ok(Self { storage: storage2, network: network2 })
    }

    pub async fn local_peer_id(&self) -> PeerId {
        self.network.local_peer_id().await
    }

    pub async fn listeners(&self) -> Vec<Multiaddr> {
        self.network.listeners().await
    }

    pub async fn external_addresses(&self) -> Vec<AddressRecord> {
        self.network.external_addresses().await
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

    pub async fn insert(&self, block: Block<P>, tmp: Option<&AsyncTempPin>) -> Result<()> {
        let cid = *block.cid();
        self.storage.insert(block, tmp).await?;
        let _ = self.network.provide(cid).await;
        Ok(())
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
            self.network.sync(cid, std::iter::empty()).await?;
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
            network: self.network.stats().await,
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
        Ipfs::insert(self, block.clone(), None).await
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
    use async_std::task;
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

    async fn create_store(bootstrap: Vec<(Multiaddr, PeerId)>) -> Ipfs<DefaultParams> {
        let cache_size = 10;
        let sweep_interval = Duration::from_millis(10000);
        let storage = StorageConfig::new(None, cache_size, sweep_interval);

        let mut network = NetworkConfig::new();
        network.enable_mdns = bootstrap.is_empty();
        network.boot_nodes = bootstrap;
        network.allow_non_globals_in_dht = true;

        Ipfs::new(Config { storage, network }).await.unwrap()
    }

    fn create_block(bytes: &[u8]) -> Block<DefaultParams> {
        Block::encode(RawCodec, Code::Blake3_256, bytes).unwrap()
    }

    #[async_std::test]
    async fn test_local_store() {
        tracing_try_init();
        let store = create_store(vec![]).await;
        let block = create_block(b"test_local_store");
        store.insert(block.clone(), None).await.unwrap();
        let block2 = store.get(*block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    #[cfg(not(target_os = "macos"))] // mdns doesn't work on macos in github actions
    async fn test_exchange_mdns() {
        tracing_try_init();
        let store1 = create_store(vec![]).await;
        let store2 = create_store(vec![]).await;
        let block = create_block(b"test_exchange_mdns");
        store1.insert(block.clone(), None).await.unwrap();
        task::sleep(Duration::from_secs(3)).await;
        let block2 = store2.get(*block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    async fn test_exchange_kad() {
        tracing_try_init();
        let store = create_store(vec![]).await;
        // make sure bootstrap node has started
        task::sleep(Duration::from_secs(3)).await;
        let bootstrap = vec![(store.listeners().await[0].clone(), store.local_peer_id().await)];
        let store1 = create_store(bootstrap.clone()).await;
        let store2 = create_store(bootstrap).await;
        let block = create_block(b"test_exchange_kad");
        store1.insert(block.clone(), None).await.unwrap();
        // wait for entry to propagate
        task::sleep(Duration::from_secs(3)).await;
        let block2 = store2.get(*block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    async fn test_provider_not_found() {
        tracing_try_init();
        let store1 = create_store(vec![]).await;
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
        let local1 = create_store(vec![]).await;
        let local2 = create_store(vec![]).await;
        let a1 = create_ipld_block(&ipld!({ "a": 0 }));
        let b1 = create_ipld_block(&ipld!({ "b": 0 }));
        let c1 = create_ipld_block(&ipld!({ "c": [a1.cid(), b1.cid()] }));
        let b2 = create_ipld_block(&ipld!({ "b": 1 }));
        let c2 = create_ipld_block(&ipld!({ "c": [a1.cid(), b2.cid()] }));
        let x = alias!(x);

        local1.insert(a1.clone(), None).await.unwrap();
        local1.insert(b1.clone(), None).await.unwrap();
        local1.insert(c1.clone(), None).await.unwrap();
        local1.alias(x, Some(*c1.cid())).await.unwrap();
        assert_pinned!(&local1, &a1);
        assert_pinned!(&local1, &b1);
        assert_pinned!(&local1, &c1);
        task::sleep(Duration::from_secs(3)).await;

        local2.alias(x, Some(*c1.cid())).await.unwrap();
        assert_pinned!(&local2, &a1);
        assert_pinned!(&local2, &b1);
        assert_pinned!(&local2, &c1);

        local2.insert(b2.clone(), None).await.unwrap();
        local2.insert(c2.clone(), None).await.unwrap();
        local2.alias(x, Some(*c2.cid())).await.unwrap();
        assert_pinned!(&local2, &a1);
        assert_unpinned!(&local2, &b1);
        assert_unpinned!(&local2, &c1);
        assert_pinned!(&local2, &b2);
        assert_pinned!(&local2, &c2);
        task::sleep(Duration::from_secs(3)).await;

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
