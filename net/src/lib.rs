use crate::behaviour::NetworkBackendBehaviour;
use futures::stream::Stream;
use futures::{future, pin_mut};
use libipld::store::StoreParams;
use libipld::{Cid, Result};
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::Transport;
use libp2p::dns::DnsConfig;
use libp2p::mplex::MplexConfig;
use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
use libp2p::swarm::{AddressScore, Swarm, SwarmBuilder, SwarmEvent};
use libp2p::tcp::TcpConfig;
use prometheus::Registry;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

mod behaviour;
mod config;

pub use crate::behaviour::{QueryId, SyncEvent};
pub use crate::config::NetworkConfig;
pub use libp2p::gossipsub::{GossipsubEvent, GossipsubMessage, MessageId, Topic, TopicHash};
pub use libp2p::kad::record::{Key, Record};
pub use libp2p::kad::{PeerRecord, Quorum};
pub use libp2p::swarm::AddressRecord;
pub use libp2p::{Multiaddr, PeerId};
pub use libp2p_bitswap::BitswapStore;

#[derive(Clone)]
pub struct NetworkService<P: StoreParams> {
    swarm: Arc<Mutex<Swarm<NetworkBackendBehaviour<P>>>>,
}

impl<P: StoreParams> NetworkService<P> {
    pub async fn new<S: BitswapStore<Params = P>>(config: NetworkConfig, store: S) -> Result<Self> {
        let dh_key = Keypair::<X25519Spec>::new()
            .into_authentic(&config.node_key)
            .unwrap();
        let transport = DnsConfig::new(
            TcpConfig::new()
                .nodelay(true)
                .upgrade(Version::V1)
                .authenticate(NoiseConfig::xx(dh_key).into_authenticated())
                .multiplex(MplexConfig::new())
                .timeout(Duration::from_secs(5))
                .boxed(),
        )?;

        let peer_id = config.peer_id();
        let behaviour = NetworkBackendBehaviour::<P>::new(config.clone(), store).await?;
        let swarm = SwarmBuilder::new(transport.boxed(), behaviour, peer_id)
            .executor(Box::new(|fut| {
                async_global_executor::spawn(fut).detach();
            }))
            .build();

        let swarm = Arc::new(Mutex::new(swarm));
        let swarm2 = swarm.clone();
        async_global_executor::spawn::<_, ()>(future::poll_fn(move |cx| {
            let mut guard = swarm.lock().unwrap();
            while let Poll::Ready(_) = {
                let swarm = &mut *guard;
                pin_mut!(swarm);
                swarm.poll_next(cx)
            } {}
            Poll::Pending
        }))
        .detach();

        Ok(Self { swarm: swarm2 })
    }

    pub fn local_peer_id(&self) -> PeerId {
        let swarm = self.swarm.lock().unwrap();
        *Swarm::local_peer_id(&swarm)
    }

    pub async fn listen_on(&self, addr: Multiaddr) -> Result<Multiaddr> {
        let mut swarm = self.swarm.lock().unwrap();
        Swarm::listen_on(&mut swarm, addr)?;
        loop {
            match swarm.next_event().await {
                SwarmEvent::NewListenAddr(addr) => {
                    tracing::info!("listening on {}", addr);
                    return Ok(addr);
                }
                SwarmEvent::ListenerClosed {
                    reason: Err(err), ..
                } => return Err(err.into()),
                _ => continue,
            }
        }
    }

    pub fn listeners(&self) -> Vec<Multiaddr> {
        let swarm = self.swarm.lock().unwrap();
        Swarm::listeners(&swarm).cloned().collect()
    }

    pub fn add_external_address(&self, addr: Multiaddr) {
        let mut swarm = self.swarm.lock().unwrap();
        Swarm::add_external_address(&mut swarm, addr, AddressScore::Infinite);
    }

    pub fn external_addresses(&self) -> Vec<AddressRecord> {
        let swarm = self.swarm.lock().unwrap();
        Swarm::external_addresses(&swarm).cloned().collect()
    }

    pub fn add_address(&self, peer: &PeerId, addr: Multiaddr) {
        let mut swarm = self.swarm.lock().unwrap();
        swarm.add_address(peer, addr);
    }

    pub fn remove_address(&self, peer: &PeerId, addr: &Multiaddr) {
        let mut swarm = self.swarm.lock().unwrap();
        swarm.remove_address(peer, addr);
    }

    pub fn dial(&self, peer: &PeerId) -> Result<()> {
        let mut swarm = self.swarm.lock().unwrap();
        Ok(Swarm::dial(&mut swarm, peer)?)
    }

    pub fn ban(&self, peer: PeerId) {
        let mut swarm = self.swarm.lock().unwrap();
        Swarm::ban_peer_id(&mut swarm, peer)
    }

    pub fn unban(&self, peer: PeerId) {
        let mut swarm = self.swarm.lock().unwrap();
        Swarm::unban_peer_id(&mut swarm, peer)
    }

    pub async fn bootstrap(&self, peers: &[(PeerId, Multiaddr)]) -> Result<()> {
        for (peer, addr) in peers {
            self.add_address(peer, addr.clone());
            self.dial(peer)?;
        }
        let rx = {
            let mut swarm = self.swarm.lock().unwrap();
            swarm.bootstrap()
        };
        tracing::trace!("started bootstrap");
        rx.await??;
        tracing::trace!("boostrap complete");
        Ok(())
    }

    pub async fn get_record(&self, key: &Key, quorum: Quorum) -> Result<Vec<PeerRecord>> {
        let rx = {
            let mut swarm = self.swarm.lock().unwrap();
            swarm.get_record(key, quorum)
        };
        Ok(rx.await??)
    }

    pub async fn put_record(&self, record: Record, quorum: Quorum) -> Result<()> {
        let rx = {
            let mut swarm = self.swarm.lock().unwrap();
            swarm.put_record(record, quorum)
        };
        rx.await??;
        Ok(())
    }

    pub fn subscribe(&self, topic: &str) -> Result<impl Stream<Item = Vec<u8>>> {
        let mut swarm = self.swarm.lock().unwrap();
        swarm.subscribe(topic)
    }

    pub fn publish(&self, topic: &str, msg: Vec<u8>) -> Result<()> {
        let mut swarm = self.swarm.lock().unwrap();
        swarm.publish(topic, msg)
    }

    pub fn remove_record(&self, key: &Key) {
        let mut swarm = self.swarm.lock().unwrap();
        swarm.remove_record(key)
    }

    pub async fn get(&self, cid: Cid) -> Result<()> {
        let query = {
            let mut swarm = self.swarm.lock().unwrap();
            let (rx, id) = swarm.get(cid);
            Query {
                swarm: Some(self.swarm.clone()),
                id,
                rx,
            }
        };
        query.await??;
        Ok(())
    }

    pub fn sync(
        &self,
        cid: Cid,
        missing: impl Iterator<Item = Cid>,
    ) -> impl Stream<Item = SyncEvent> {
        let query = {
            let mut swarm = self.swarm.lock().unwrap();
            let (rx, id) = swarm.sync(cid, missing);
            Query {
                swarm: Some(self.swarm.clone()),
                id,
                rx,
            }
        };
        query
    }

    pub async fn provide(&self, cid: Cid) -> Result<()> {
        let rx = {
            let mut swarm = self.swarm.lock().unwrap();
            swarm.provide(cid)
        };
        rx.await??;
        Ok(())
    }

    pub fn unprovide(&self, cid: Cid) {
        let mut swarm = self.swarm.lock().unwrap();
        swarm.unprovide(cid)
    }

    pub fn register_metrics(&self, registry: &Registry) -> Result<()> {
        let swarm = self.swarm.lock().unwrap();
        swarm.register_metrics(registry)
    }
}

pub struct Query<P: StoreParams, T> {
    swarm: Option<Arc<Mutex<Swarm<NetworkBackendBehaviour<P>>>>>,
    id: QueryId,
    rx: T,
}

impl<P: StoreParams, T: Future + Unpin> Future for Query<P, T> {
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        Pin::new(&mut self.rx).poll(cx)
    }
}

impl<P: StoreParams, T: Stream + Unpin> Stream for Query<P, T> {
    type Item = T::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_next(cx)
    }
}

impl<P: StoreParams, T> Drop for Query<P, T> {
    fn drop(&mut self) {
        let swarm = self.swarm.take().unwrap();
        let mut swarm = swarm.lock().unwrap();
        swarm.cancel(self.id);
    }
}
