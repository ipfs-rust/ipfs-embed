//! IpfsEmbed is an embeddable ipfs implementation.
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use ipfs_embed::Ipfs;
//! use ipfs_embed::core::BitswapStorage;
//! use ipfs_embed::db::StorageService;
//! use ipfs_embed::net::{NetworkConfig, NetworkService};
//! use libipld::DefaultParams;
//! use std::sync::Arc;
//! use std::time::Duration;
//! let sled_config = sled::Config::new().temporary(true);
//! let cache_size = 10;
//! let sweep_interval = Duration::from_millis(10000);
//! let network_timeout = Duration::from_secs(5);
//! let net_config = NetworkConfig::new();
//! let storage = Arc::new(StorageService::open(&sled_config, cache_size, sweep_interval).unwrap());
//! let bitswap_storage = BitswapStorage::new(storage.clone());
//! let network = Arc::new(NetworkService::new(net_config, bitswap_storage).unwrap());
//! let ipfs = Ipfs::<DefaultParams, _, _>::new(storage, network);
//! # Ok(()) }
//! ```
use async_std::task;
use async_trait::async_trait;
use fnv::FnvHashMap;
use futures::channel::{mpsc, oneshot};
use futures::future::Future;
use futures::sink::SinkExt;
use futures::stream::Stream;
use ipfs_embed_core::{
    BitswapStorage, Block, Cid, Multiaddr, Network, NetworkEvent, PeerId, Query, QueryResult,
    QueryType, Result, Storage, StorageEvent, StoreParams,
};
use libipld::codec::Decode;
use libipld::error::BlockNotFound;
use libipld::ipld::Ipld;
use libipld::store::Store;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub use ipfs_embed_core as core;
#[cfg(feature = "db")]
pub use ipfs_embed_db as db;
#[cfg(feature = "net")]
pub use ipfs_embed_net as net;

pub struct Ipfs<P, S, N> {
    _marker: PhantomData<P>,
    storage: Arc<S>,
    network: Arc<N>,
    tx: mpsc::Sender<(Query, oneshot::Sender<QueryResult>)>,
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
    Ipld: Decode<P::Codecs>,
{
    pub fn new(storage: Arc<S>, network: Arc<N>) -> Self {
        let (tx, rx) = mpsc::channel(0);
        task::spawn(IpfsTask::new(storage.clone(), network.clone(), rx));
        Self {
            _marker: PhantomData,
            storage,
            network,
            tx,
        }
    }

    pub fn local_peer_id(&self) -> &PeerId {
        self.network.local_peer_id()
    }

    pub async fn listeners(&self) -> Vec<Multiaddr> {
        let (tx, rx) = oneshot::channel();
        self.network.listeners(tx);
        rx.await.unwrap()
    }

    pub async fn external_addresses(&self) -> Vec<Multiaddr> {
        let (tx, rx) = oneshot::channel();
        self.network.external_addresses(tx);
        rx.await.unwrap()
    }

    pub async fn pinned(&self, cid: &Cid) -> Result<Option<bool>> {
        self.storage.pinned(cid).await
    }
}

#[async_trait]
impl<P, S, N> Store for Ipfs<P, S, N>
where
    P: StoreParams + Unpin + 'static,
    S: Storage<P>,
    N: Network<P>,
    Ipld: Decode<P::Codecs>,
{
    type Params = P;

    async fn get(&self, cid: &Cid) -> Result<Block<P>> {
        if let Some(data) = self.storage.get(cid)? {
            let block = Block::new_unchecked(*cid, data);
            return Ok(block);
        }
        let (tx, rx) = oneshot::channel();
        self.tx
            .clone()
            .send((
                Query {
                    cid: *cid,
                    ty: QueryType::Get,
                },
                tx,
            ))
            .await?;
        rx.await??;
        if let Some(data) = self.storage.get(cid)? {
            let block = Block::new_unchecked(*cid, data);
            return Ok(block);
        }
        log::error!("block evicted too soon");
        Err(BlockNotFound(*cid).into())
    }

    async fn insert(&self, block: &Block<P>) -> Result<()> {
        self.storage.insert(block)?;
        Ok(())
    }

    async fn alias<T: AsRef<[u8]> + Send + Sync>(&self, alias: T, cid: Option<&Cid>) -> Result<()> {
        if let Some(cid) = cid {
            let (tx, rx) = oneshot::channel();
            self.tx
                .clone()
                .send((
                    Query {
                        cid: *cid,
                        ty: QueryType::Sync,
                    },
                    tx,
                ))
                .await?;
            rx.await??;
        }
        self.storage.alias(alias.as_ref(), cid).await?;
        Ok(())
    }

    async fn resolve<T: AsRef<[u8]> + Send + Sync>(&self, alias: T) -> Result<Option<Cid>> {
        self.storage.resolve(alias.as_ref())
    }
}

struct IpfsTask<P: StoreParams, S: Storage<P>, N: Network<P>> {
    _marker: PhantomData<P>,
    storage: Arc<S>,
    storage_events: S::Subscription,
    network: Arc<N>,
    network_events: N::Subscription,
    queries: FnvHashMap<Query, Vec<oneshot::Sender<QueryResult>>>,
    rx: mpsc::Receiver<(Query, oneshot::Sender<QueryResult>)>,
}

impl<P, S, N> IpfsTask<P, S, N>
where
    P: StoreParams + Unpin + 'static,
    S: Storage<P>,
    N: Network<P>,
    Ipld: Decode<P::Codecs>,
{
    pub fn new(
        storage: Arc<S>,
        network: Arc<N>,
        rx: mpsc::Receiver<(Query, oneshot::Sender<QueryResult>)>,
    ) -> Self {
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
    Ipld: Decode<P::Codecs>,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        loop {
            let (query, tx) = match Pin::new(&mut self.rx).poll_next(ctx) {
                Poll::Ready(Some(query)) => query,
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            };
            self.queries.entry(query).or_default().push(tx);
            match query.ty {
                QueryType::Get => self.network.get(query.cid),
                QueryType::Sync => {
                    let syncer = BitswapStorage::new(self.storage.clone());
                    self.network.sync(query.cid, Arc::new(syncer))
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
            }
        }

        loop {
            let event = match Pin::new(&mut self.storage_events).poll_next(ctx) {
                Poll::Ready(Some(event)) => event,
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            };
            match event {
                StorageEvent::Insert(cid) => self.network.provide(cid),
                StorageEvent::Remove(cid) => self.network.unprovide(cid),
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ipfs_embed_db::StorageService;
    use ipfs_embed_net::{NetworkConfig, NetworkService};
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

    fn create_store(bootstrap: Vec<(Multiaddr, PeerId)>) -> DefaultIpfs {
        let sled_config = sled::Config::new().temporary(true);
        let cache_size = 10;
        let sweep_interval = Duration::from_millis(10000);

        let mut net_config = NetworkConfig::new();
        net_config.enable_mdns = bootstrap.is_empty();
        net_config.boot_nodes = bootstrap;
        net_config.allow_non_globals_in_dht = true;

        let storage =
            Arc::new(StorageService::open(&sled_config, cache_size, sweep_interval).unwrap());
        let bitswap_storage = BitswapStorage::new(storage.clone());
        let network = Arc::new(NetworkService::new(net_config, bitswap_storage).unwrap());
        Ipfs::new(storage, network)
    }

    fn create_block(bytes: &[u8]) -> Block<DefaultParams> {
        Block::encode(RawCodec, Code::Blake3_256, bytes).unwrap()
    }

    #[async_std::test]
    async fn test_local_store() {
        env_logger::try_init().ok();
        let store = create_store(vec![]);
        let block = create_block(b"test_local_store");
        store.insert(&block).await.unwrap();
        let block2 = store.get(block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    #[cfg(not(target_os = "macos"))] // mdns doesn't work on macos in github actions
    async fn test_exchange_mdns() {
        env_logger::try_init().ok();
        let store1 = create_store(vec![]);
        let store2 = create_store(vec![]);
        let block = create_block(b"test_exchange_mdns");
        store1.insert(&block).await.unwrap();
        task::sleep(Duration::from_millis(1000)).await;
        let block2 = store2.get(block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    async fn test_exchange_kad() {
        env_logger::try_init().ok();
        let store = create_store(vec![]);
        // make sure bootstrap node has started
        task::sleep(Duration::from_millis(1000)).await;
        let bootstrap = vec![(
            store.listeners().await[0].clone(),
            store.local_peer_id().clone(),
        )];
        let store1 = create_store(bootstrap.clone());
        let store2 = create_store(bootstrap);
        let block = create_block(b"test_exchange_kad");
        store1.insert(&block).await.unwrap();
        // wait for entry to propagate
        task::sleep(Duration::from_millis(1000)).await;
        let block2 = store2.get(block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    async fn test_provider_not_found() {
        env_logger::try_init().ok();
        let store1 = create_store(vec![]);
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
            assert_eq!($store.pinned($block.cid()).await.unwrap(), Some(true));
        };
    }

    macro_rules! assert_unpinned {
        ($store:expr, $block:expr) => {
            assert_eq!($store.pinned($block.cid()).await.unwrap(), Some(false));
        };
    }

    fn create_ipld_block(ipld: &Ipld) -> Block<DefaultParams> {
        Block::encode(DagCborCodec, Code::Blake3_256, ipld).unwrap()
    }

    #[async_std::test]
    async fn test_sync() {
        env_logger::try_init().ok();
        let local1 = create_store(vec![]);
        let local2 = create_store(vec![]);
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
        task::sleep(Duration::from_secs(1)).await;

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
        task::sleep(Duration::from_secs(1)).await;

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
