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
use fnv::{FnvHashMap, FnvHashSet};
use futures::channel::{mpsc, oneshot};
use futures::future::Future;
use futures::sink::SinkExt;
use futures::stream::Stream;
use ipfs_embed_core::{
    AddressRecord, Block, Cid, Multiaddr, Network, NetworkEvent, PeerId, Query, QueryResult,
    QueryType, Result, Storage, StorageEvent, StoreParams,
};
use libipld::codec::References;
use libipld::error::BlockNotFound;
use libipld::ipld::Ipld;
use libipld::store::Store;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub use ipfs_embed_core as core;
#[cfg(feature = "net")]
pub use ipfs_embed_net as net;
#[cfg(feature = "db")]
pub use ipfs_embed_sqlite as db;

enum Command {
    Get(Cid, oneshot::Sender<QueryResult>),
    Sync(Cid, FnvHashSet<Cid>, oneshot::Sender<QueryResult>),
}

pub struct Ipfs<P, S, N> {
    _marker: PhantomData<P>,
    storage: Arc<S>,
    network: Arc<N>,
    tx: mpsc::Sender<Command>,
}

impl<P, S, N> Clone for Ipfs<P, S, N> {
    fn clone(&self) -> Self {
        Self {
            _marker: self._marker,
            storage: self.storage.clone(),
            network: self.network.clone(),
            tx: self.tx.clone(),
        }
    }
}

impl<P, S, N> Ipfs<P, S, N>
where
    P: StoreParams + Unpin + 'static,
    S: Storage<P>,
    N: Network<P>,
    Ipld: References<P::Codecs>,
{
    pub async fn new(storage: Arc<S>, network: Arc<N>) -> Result<Self> {
        for cid in storage.iter().await? {
            network.provide(cid);
        }
        let (tx, rx) = mpsc::channel(0);
        async_global_executor::spawn(IpfsTask::new(storage.clone(), network.clone(), rx)).detach();
        Ok(Self {
            _marker: PhantomData,
            storage,
            network,
            tx,
        })
    }

    pub fn local_peer_id(&self) -> &PeerId {
        self.network.local_peer_id()
    }

    pub async fn listeners(&self) -> Vec<Multiaddr> {
        let (tx, rx) = oneshot::channel();
        self.network.listeners(tx);
        rx.await.unwrap()
    }

    pub async fn external_addresses(&self) -> Vec<AddressRecord> {
        let (tx, rx) = oneshot::channel();
        self.network.external_addresses(tx);
        rx.await.unwrap()
    }

    pub async fn pinned(&self, cid: &Cid) -> Result<Option<Vec<Vec<u8>>>> {
        self.storage.pinned(cid).await
    }
}

#[cfg(all(feature = "db", feature = "net"))]
use libipld::store::DefaultParams;
#[cfg(all(feature = "db", feature = "net"))]
pub type DefaultIpfs =
    Ipfs<DefaultParams, db::StorageService<DefaultParams>, net::NetworkService<DefaultParams>>;
#[cfg(all(feature = "db", feature = "net"))]
impl DefaultIpfs {
    /// If no path is provided a temporary db will be created.
    pub async fn default(path: Option<std::path::PathBuf>, cache_size: u64) -> Result<Self> {
        let sweep_interval = std::time::Duration::from_millis(10000);
        let storage_config = db::StorageConfig::new(path, cache_size, sweep_interval);
        let storage = Arc::new(db::StorageService::open(storage_config)?);
        let network_config = net::NetworkConfig::new();
        let network = Arc::new(net::NetworkService::new(network_config).await?);
        Ok(Self::new(storage, network).await?)
    }
}

#[async_trait]
impl<P, S, N> Store for Ipfs<P, S, N>
where
    P: StoreParams + Unpin + 'static,
    S: Storage<P>,
    N: Network<P>,
    Ipld: References<P::Codecs>,
{
    type Params = P;

    async fn get(&self, cid: &Cid) -> Result<Block<P>> {
        if let Some(data) = self.storage.get(cid).await? {
            let block = Block::new_unchecked(*cid, data);
            return Ok(block);
        }
        let (tx, rx) = oneshot::channel();
        self.tx.clone().send(Command::Get(*cid, tx)).await?;
        rx.await??;
        if let Some(data) = self.storage.get(cid).await? {
            let block = Block::new_unchecked(*cid, data);
            return Ok(block);
        }
        log::error!("block evicted too soon");
        Err(BlockNotFound(*cid).into())
    }

    async fn insert(&self, block: &Block<P>) -> Result<()> {
        self.storage.insert(block, None).await?;
        self.network.provide(*block.cid());
        Ok(())
    }

    async fn alias<T: AsRef<[u8]> + Send + Sync>(&self, alias: T, cid: Option<&Cid>) -> Result<()> {
        if let Some(cid) = cid {
            let missing = self.storage.missing_blocks(cid).await?;
            if !missing.is_empty() {
                let (tx, rx) = oneshot::channel();
                self.tx
                    .clone()
                    .send(Command::Sync(*cid, missing, tx))
                    .await?;
                rx.await??;
            }
        }
        self.storage.alias(alias.as_ref(), cid).await?;
        Ok(())
    }

    async fn resolve<T: AsRef<[u8]> + Send + Sync>(&self, alias: T) -> Result<Option<Cid>> {
        self.storage.resolve(alias.as_ref()).await
    }
}

struct IpfsTask<P: StoreParams, S: Storage<P>, N: Network<P>> {
    _marker: PhantomData<P>,
    storage: Arc<S>,
    storage_events: S::Subscription,
    network: Arc<N>,
    network_events: N::Subscription,
    queries: FnvHashMap<Query, Vec<oneshot::Sender<QueryResult>>>,
    rx: mpsc::Receiver<Command>,
}

impl<P, S, N> IpfsTask<P, S, N>
where
    P: StoreParams + Unpin + 'static,
    S: Storage<P>,
    N: Network<P>,
    Ipld: References<P::Codecs>,
{
    pub fn new(storage: Arc<S>, network: Arc<N>, rx: mpsc::Receiver<Command>) -> Self {
        let storage_events = storage.subscribe();
        let network_events = network.subscribe();
        Self {
            _marker: PhantomData,
            storage,
            network,
            storage_events,
            network_events,
            queries: Default::default(),
            rx,
        }
    }
}

impl<P, S, N> Future for IpfsTask<P, S, N>
where
    P: StoreParams + Unpin + 'static,
    S: Storage<P>,
    N: Network<P>,
    Ipld: References<P::Codecs>,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        loop {
            let cmd = match Pin::new(&mut self.rx).poll_next(ctx) {
                Poll::Ready(Some(query)) => query,
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            };
            match cmd {
                Command::Get(cid, tx) => {
                    let query = Query {
                        cid,
                        ty: QueryType::Get,
                    };
                    self.queries.entry(query).or_default().push(tx);
                    self.network.get(cid);
                }
                Command::Sync(cid, missing, tx) => {
                    let query = Query {
                        cid,
                        ty: QueryType::Sync,
                    };
                    self.queries.entry(query).or_default().push(tx);
                    self.network.sync(cid, missing);
                }
            }
        }

        loop {
            let event = match Pin::new(&mut self.network_events).poll_next(ctx) {
                Poll::Ready(Some(event)) => event,
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            };
            match event {
                NetworkEvent::QueryComplete(query, res) => {
                    if let Some(txs) = self.queries.remove(&query) {
                        for tx in txs {
                            tx.send(res).ok();
                        }
                    }
                }
                NetworkEvent::HaveBlock(ch, cid) => {
                    let storage = self.storage.clone();
                    let network = self.network.clone();
                    let ch = Arc::try_unwrap(ch).expect("one ref");
                    async_global_executor::spawn(async move {
                        if let Ok(have) = storage.contains(&cid).await {
                            network.send_have(ch, have);
                        }
                    })
                    .detach();
                }
                NetworkEvent::WantBlock(ch, cid) => {
                    let storage = self.storage.clone();
                    let network = self.network.clone();
                    let ch = Arc::try_unwrap(ch).expect("one ref");
                    async_global_executor::spawn(async move {
                        if let Ok(block) = storage.get(&cid).await {
                            network.send_block(ch, block);
                        }
                    })
                    .detach();
                }
                NetworkEvent::ReceivedBlock(block) => {
                    let storage = self.storage.clone();
                    let network = self.network.clone();
                    async_global_executor::spawn(async move {
                        if storage.insert(&block, None).await.is_ok() {
                            network.provide(*block.cid());
                        }
                    })
                    .detach();
                }
                NetworkEvent::MissingBlocks(cid) => {
                    let storage = self.storage.clone();
                    let network = self.network.clone();
                    async_global_executor::spawn(async move {
                        if let Ok(missing) = storage.missing_blocks(&cid).await {
                            network.add_missing(cid, missing);
                        }
                    })
                    .detach();
                }
            }
        }

        loop {
            let event = match Pin::new(&mut self.storage_events).poll_next(ctx) {
                Poll::Ready(Some(event)) => event,
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            };
            match event {
                StorageEvent::Remove(cid) => self.network.unprovide(cid),
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use ipfs_embed_net::{NetworkConfig, NetworkService};
    use ipfs_embed_sqlite::{StorageConfig, StorageService};
    use libipld::block::Block;
    use libipld::cbor::DagCborCodec;
    use libipld::multihash::Code;
    use libipld::raw::RawCodec;
    use libipld::store::DefaultParams;
    use libipld::{alias, ipld};
    use std::time::Duration;

    type Storage = StorageService<DefaultParams>;
    type Network = NetworkService<DefaultParams>;
    type DefaultIpfs = Ipfs<DefaultParams, Storage, Network>;

    async fn create_store(bootstrap: Vec<(Multiaddr, PeerId)>) -> DefaultIpfs {
        let cache_size = 10;
        let sweep_interval = Duration::from_millis(10000);
        let storage_config = StorageConfig::new(None, cache_size, sweep_interval);

        let mut net_config = NetworkConfig::new();
        net_config.enable_mdns = bootstrap.is_empty();
        net_config.boot_nodes = bootstrap;
        net_config.allow_non_globals_in_dht = true;

        let storage = Arc::new(StorageService::open(storage_config).unwrap());
        let network = Arc::new(NetworkService::new(net_config).await.unwrap());
        Ipfs::new(storage, network).await.unwrap()
    }

    fn create_block(bytes: &[u8]) -> Block<DefaultParams> {
        Block::encode(RawCodec, Code::Blake3_256, bytes).unwrap()
    }

    #[async_std::test]
    async fn test_local_store() {
        env_logger::try_init().ok();
        let store = create_store(vec![]).await;
        let block = create_block(b"test_local_store");
        store.insert(&block).await.unwrap();
        let block2 = store.get(block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    #[cfg(not(target_os = "macos"))] // mdns doesn't work on macos in github actions
    async fn test_exchange_mdns() {
        env_logger::try_init().ok();
        let store1 = create_store(vec![]).await;
        let store2 = create_store(vec![]).await;
        let block = create_block(b"test_exchange_mdns");
        store1.insert(&block).await.unwrap();
        task::sleep(Duration::from_secs(3)).await;
        let block2 = store2.get(block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    async fn test_exchange_kad() {
        env_logger::try_init().ok();
        let store = create_store(vec![]).await;
        // make sure bootstrap node has started
        task::sleep(Duration::from_secs(3)).await;
        let bootstrap = vec![(store.listeners().await[0].clone(), *store.local_peer_id())];
        let store1 = create_store(bootstrap.clone()).await;
        let store2 = create_store(bootstrap).await;
        let block = create_block(b"test_exchange_kad");
        store1.insert(&block).await.unwrap();
        // wait for entry to propagate
        task::sleep(Duration::from_secs(3)).await;
        let block2 = store2.get(block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    async fn test_provider_not_found() {
        env_logger::try_init().ok();
        let store1 = create_store(vec![]).await;
        let block = create_block(b"test_provider_not_found");
        if store1
            .get(block.cid())
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
                    .pinned($block.cid())
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
                    .pinned($block.cid())
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
        env_logger::try_init().ok();
        let local1 = create_store(vec![]).await;
        let local2 = create_store(vec![]).await;
        let a1 = create_ipld_block(&ipld!({ "a": 0 }));
        let b1 = create_ipld_block(&ipld!({ "b": 0 }));
        let c1 = create_ipld_block(&ipld!({ "c": [a1.cid(), b1.cid()] }));
        let b2 = create_ipld_block(&ipld!({ "b": 1 }));
        let c2 = create_ipld_block(&ipld!({ "c": [a1.cid(), b2.cid()] }));
        let x = alias!(x);

        local1.insert(&a1).await.unwrap();
        local1.insert(&b1).await.unwrap();
        local1.insert(&c1).await.unwrap();
        local1.alias(x, Some(c1.cid())).await.unwrap();
        assert_pinned!(&local1, &a1);
        assert_pinned!(&local1, &b1);
        assert_pinned!(&local1, &c1);
        task::sleep(Duration::from_secs(3)).await;

        local2.alias(x, Some(c1.cid())).await.unwrap();
        assert_pinned!(&local2, &a1);
        assert_pinned!(&local2, &b1);
        assert_pinned!(&local2, &c1);

        local2.insert(&b2).await.unwrap();
        local2.insert(&c2).await.unwrap();
        local2.alias(x, Some(c2.cid())).await.unwrap();
        assert_pinned!(&local2, &a1);
        assert_unpinned!(&local2, &b1);
        assert_unpinned!(&local2, &c1);
        assert_pinned!(&local2, &b2);
        assert_pinned!(&local2, &c2);
        task::sleep(Duration::from_secs(3)).await;

        local1.alias(x, Some(c2.cid())).await.unwrap();
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
