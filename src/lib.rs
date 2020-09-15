//! IpfsEmbed is an embeddable ipfs implementation.
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use ipfs_embed::{Config, Store, Multicodec, Multihash};
//! let config = Config::from_path_local("/tmp/db")?;
//! let store = Store::<Multicodec, Multihash>::new(config)?;
//! # Ok(()) }
//! ```
use async_std::stream::{interval, Interval};
use async_std::task;
use async_trait::async_trait;
use futures::channel::{mpsc, oneshot};
use futures::future::Future;
use futures::sink::SinkExt;
use futures::stream::Stream;
use ipfs_embed_core::{
    Block, Cid, Multiaddr, Network, NetworkEvent, PeerId, Result, Storage, StorageEvent,
    StoreParams, Transaction,
};
use libipld::codec::Decode;
use libipld::error::BlockNotFound;
use libipld::ipld::Ipld;
use libipld::store::Store;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use std::time::Instant;

pub struct Ipfs<P, S, N> {
    _marker: PhantomData<P>,
    storage: Arc<S>,
    network: Arc<N>,
    tx: mpsc::Sender<(Cid, oneshot::Sender<Block<P>>)>,
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
    pub fn new(storage: Arc<S>, network: Arc<N>, timeout: Duration) -> Self {
        let (tx, rx) = mpsc::channel(0);
        task::spawn(IpfsTask::new(storage.clone(), network.clone(), rx, timeout));
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

    pub fn external_addresses(&self) -> Vec<Multiaddr> {
        self.network.external_addresses()
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
            let block = Block::new_unchecked(cid.clone(), data);
            return Ok(block);
        }
        let (tx, rx) = oneshot::channel();
        self.tx.clone().send((cid.clone(), tx)).await?;
        if let Ok(block) = rx.await {
            return Ok(block);
        }
        Err(BlockNotFound(cid.to_string()).into())
    }

    async fn commit(&self, tx: Transaction<P>) -> Result<()> {
        self.storage.commit(tx)?;
        Ok(())
    }
}

struct Wanted<P: StoreParams> {
    ch: Vec<oneshot::Sender<Block<P>>>,
    timestamp: Instant,
}

impl<P: StoreParams> Default for Wanted<P> {
    fn default() -> Self {
        Self {
            ch: Default::default(),
            timestamp: Instant::now(),
        }
    }
}

impl<S: StoreParams> Wanted<S> {
    fn add_receiver(&mut self, ch: oneshot::Sender<Block<S>>) {
        self.ch.push(ch);
    }

    fn received(self, block: &Block<S>) {
        log::info!("received block");
        for tx in self.ch {
            tx.send(block.clone()).ok();
        }
    }
}

struct IpfsTask<P: StoreParams, S: Storage<P>, N: Network<P>> {
    _marker: PhantomData<P>,
    storage: Arc<S>,
    storage_events: S::Subscription,
    network: Arc<N>,
    network_events: N::Subscription,
    rx: mpsc::Receiver<(Cid, oneshot::Sender<Block<P>>)>,
    wanted: HashMap<Cid, Wanted<P>>,
    interval: Interval,
    timeout: Duration,
    bootstrap_complete: bool,
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
        rx: mpsc::Receiver<(Cid, oneshot::Sender<Block<P>>)>,
        timeout: Duration,
    ) -> Self {
        let storage_events = storage.subscribe();
        let network_events = network.subscribe();
        Self {
            _marker: PhantomData,
            storage,
            network,
            storage_events,
            network_events,
            rx,
            wanted: Default::default(),
            timeout,
            interval: interval(timeout),
            bootstrap_complete: false,
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
            match Pin::new(&mut self.rx).poll_next(ctx) {
                Poll::Ready(Some((cid, tx))) => {
                    let entry = self.wanted.entry(cid.clone()).or_default();
                    entry.add_receiver(tx);
                    self.network.providers(&cid);
                    self.network.want(cid, 1000);
                }
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            }
        }

        loop {
            let event = match Pin::new(&mut self.network_events).poll_next(ctx) {
                Poll::Ready(Some(event)) => event,
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            };
            log::trace!("{:?}", event);
            match event {
                NetworkEvent::Providers(_cid, providers) => {
                    // TODO: smarter querying
                    let peer_id = providers.into_iter().next().unwrap();
                    self.network.connect(peer_id);
                }
                NetworkEvent::GetProvidersFailed(cid) => {
                    if let Some(_wanted) = self.wanted.remove(&cid) {
                        self.network.cancel(cid);
                    }
                }
                NetworkEvent::Providing(cid) => {
                    log::trace!("providing {}", cid.to_string());
                }
                NetworkEvent::StartProvidingFailed(cid) => {
                    log::trace!("providing {} failed", cid.to_string());
                }
                NetworkEvent::ReceivedBlock(_, cid, data) => {
                    let block = Block::new_unchecked(cid, data.to_vec());
                    if let Some(wanted) = self.wanted.remove(block.cid()) {
                        wanted.received(&block);
                    }
                }
                NetworkEvent::ReceivedWant(peer_id, cid, _) => match self.storage.get(&cid) {
                    Ok(Some(data)) => self.network.send_to(peer_id, cid, data),
                    Ok(None) => log::trace!("don't have local block {}", cid.to_string()),
                    Err(err) => log::error!("failed to get local block {:?}", err),
                },
                NetworkEvent::BootstrapComplete => self.bootstrap_complete = true,
            }
        }

        while self.bootstrap_complete {
            let event = match Pin::new(&mut self.storage_events).poll_next(ctx) {
                Poll::Ready(Some(event)) => event,
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            };
            log::trace!("{:?}", event);
            match event {
                StorageEvent::Insert(cid) => match self.storage.get(&cid) {
                    Ok(Some(data)) => {
                        self.network.provide(&cid);
                        self.network.send(cid, data);
                    }
                    Ok(None) => {
                        log::warn!("block {} not in store", cid.to_string());
                    }
                    Err(e) => {
                        log::warn!("error {:?} retrieving block {}", e, cid.to_string());
                    }
                },
                StorageEvent::Remove(cid) => self.network.unprovide(&cid),
            }
        }

        loop {
            match Pin::new(&mut self.interval).poll_next(ctx) {
                Poll::Ready(Some(())) => {}
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            }
            let timedout = Instant::now() - self.timeout;
            let mut wanted = std::mem::replace(&mut self.wanted, HashMap::with_capacity(0));
            wanted.retain(|cid, wanted| {
                if wanted.timestamp > timedout {
                    true
                } else {
                    self.network.cancel(cid.clone());
                    false
                }
            });
            let _ = std::mem::replace(&mut self.wanted, wanted);
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
    use libipld::multihash::SHA2_256;
    use libipld::raw::RawCodec;
    use libipld::store::DefaultStoreParams;
    use std::time::Duration;
    use tempdir::TempDir;

    type Storage = StorageService<DefaultStoreParams>;
    type Network = NetworkService<DefaultStoreParams>;
    type DefaultIpfs = Ipfs<DefaultStoreParams, Storage, Network>;

    fn create_store(bootstrap: Vec<(Multiaddr, PeerId)>) -> (DefaultIpfs, TempDir) {
        let tmp = TempDir::new("").unwrap();
        let mut config = NetworkConfig::new();
        config.enable_mdns = bootstrap.is_empty();
        config.boot_nodes = bootstrap;
        let storage = Arc::new(StorageService::open(tmp.path()).unwrap());
        let network = Arc::new(NetworkService::new(config).unwrap());
        let ipfs = Ipfs::new(storage, network, Duration::from_secs(5));
        (ipfs, tmp)
    }

    fn create_block(bytes: &[u8]) -> Block<DefaultStoreParams> {
        Block::encode(RawCodec, SHA2_256, bytes).unwrap()
    }

    #[async_std::test]
    async fn test_local_store() {
        env_logger::try_init().ok();
        let (store, _) = create_store(vec![]);
        let block = create_block(b"test_local_store");
        store.insert(block.clone()).await.unwrap();
        let block2 = store.get(block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    #[cfg(not(target_os = "macos"))] // mdns doesn't work on macos in github actions
    async fn test_exchange_mdns() {
        env_logger::try_init().ok();
        let (store1, _) = create_store(vec![]);
        let (store2, _) = create_store(vec![]);
        let block = create_block(b"test_exchange_mdns");
        store1.insert(block.clone()).await.unwrap();
        let block2 = store2.get(block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    #[cfg(not(target_os = "macos"))] // mdns doesn't work on macos in github action
    async fn test_received_want_before_insert() {
        env_logger::try_init().ok();
        let (store1, _) = create_store(vec![]);
        let (store2, _) = create_store(vec![]);
        let block = create_block(b"test_received_want_before_insert");

        let get_cid = block.cid().clone();
        let get = task::spawn(async move { store2.get(&get_cid).await });

        task::sleep(Duration::from_millis(100)).await;

        store1.insert(block.clone()).await.unwrap();

        let block2 = get.await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    async fn test_exchange_kad() {
        let logger = env_logger::Builder::from_default_env().build();
        async_log::Logger::wrap(logger, || {
            let task_id = async_std::task::current().id();
            format!("{}", task_id).parse().unwrap()
        })
        .start(log::LevelFilter::Trace)
        .ok();

        let (store, _) = create_store(vec![]);
        // make sure bootstrap node has started
        task::sleep(Duration::from_millis(1000)).await;
        let bootstrap = vec![(
            store.external_addresses()[0].clone(),
            store.local_peer_id().clone(),
        )];
        let (store1, _) = create_store(bootstrap.clone());
        let (store2, _) = create_store(bootstrap);
        let block = create_block(b"test_exchange_kad");
        store1.insert(block.clone()).await.unwrap();
        // wait for entry to propagate
        task::sleep(Duration::from_millis(1000)).await;
        let block2 = store2.get(block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    async fn test_provider_not_found() {
        env_logger::try_init().ok();
        let (store1, _) = create_store(vec![]);
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
}
