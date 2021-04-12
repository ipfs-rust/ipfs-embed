use crate::net::behaviour::{GetChannel, NetworkBackendBehaviour, SyncChannel};
use async_global_executor::Task;
use futures::stream::{Stream, StreamExt};
use futures::task::AtomicWaker;
use futures::{future, pin_mut};
use libipld::store::StoreParams;
use libipld::{Cid, Result};
use libp2p::core::either::{EitherOutput, EitherTransport};
use libp2p::core::transport::Transport;
use libp2p::core::upgrade::{SelectUpgrade, Version};
use libp2p::dns::DnsConfig;
use libp2p::kad::kbucket::Key as BucketKey;
use libp2p::mplex::MplexConfig;
use libp2p::noise::{self, NoiseConfig, X25519Spec};
use libp2p::pnet::{PnetConfig, PreSharedKey};
use libp2p::swarm::{AddressScore, Swarm, SwarmBuilder};
use libp2p::tcp::TcpConfig;
use libp2p::yamux::YamuxConfig;
use libp2p_quic::{QuicConfig, ToLibp2p};
use parking_lot::Mutex;
use prometheus::Registry;
use secrecy::ExposeSecret;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

mod behaviour;
mod config;
mod peers;

pub use crate::net::behaviour::{QueryId, SyncEvent};
pub use crate::net::config::*;
pub use crate::net::peers::{AddressSource, Event, PeerInfo};
pub use libp2p::core::connection::ListenerId;
pub use libp2p::gossipsub::{GossipsubEvent, GossipsubMessage, MessageId, Topic, TopicHash};
pub use libp2p::kad::record::{Key, Record};
pub use libp2p::kad::{PeerRecord, Quorum};
pub use libp2p::swarm::AddressRecord;
pub use libp2p::{Multiaddr, PeerId};
pub use libp2p_bitswap::BitswapStore;

pub enum ListenerEvent {
    NewListenAddr(Multiaddr),
    ExpiredListenAddr(Multiaddr),
}

#[derive(Clone)]
pub struct NetworkService<P: StoreParams> {
    swarm: Arc<Mutex<Swarm<NetworkBackendBehaviour<P>>>>,
    waker: Arc<AtomicWaker>,
    _swarm_task: Arc<Task<()>>,
}

impl<P: StoreParams> NetworkService<P> {
    pub async fn new<S: BitswapStore<Params = P>>(
        mut config: NetworkConfig,
        store: S,
    ) -> Result<Self> {
        let peer_id = config.node_key.to_peer_id();
        let behaviour = NetworkBackendBehaviour::<P>::new(&mut config, store).await?;

        let tcp = {
            let transport = TcpConfig::new().nodelay(true);
            let transport = if let Some(psk) = config.psk {
                let psk = PreSharedKey::new(*psk.expose_secret());
                EitherTransport::Left(
                    transport.and_then(move |socket, _| PnetConfig::new(psk).handshake(socket)),
                )
            } else {
                EitherTransport::Right(transport)
            };
            let dh_key = noise::Keypair::<X25519Spec>::new()
                .into_authentic(&config.node_key.to_keypair())
                .unwrap();
            transport
                .upgrade(Version::V1)
                .authenticate(NoiseConfig::xx(dh_key).into_authenticated())
                .multiplex(SelectUpgrade::new(
                    YamuxConfig::default(),
                    MplexConfig::new(),
                ))
                .timeout(Duration::from_secs(5))
                .boxed()
        };
        let quic = {
            QuicConfig {
                keypair: config.node_key,
                transport: config.quic,
                ..Default::default()
            }
            .listen_on("/ip4/0.0.0.0/udp/0/quic".parse().unwrap())
            .await?
            .boxed()
        };
        let quic_or_tcp = quic.or_transport(tcp).map(|either, _| match either {
            EitherOutput::First(first) => first,
            EitherOutput::Second(second) => second,
        });
        let transport = if let Some(config) = config.dns {
            DnsConfig::custom(quic_or_tcp, config.config, config.opts)
                .await?
                .boxed()
        } else {
            DnsConfig::system(quic_or_tcp).await?.boxed()
        };
        let swarm = SwarmBuilder::new(transport, behaviour, peer_id)
            .executor(Box::new(|fut| {
                async_global_executor::spawn(fut).detach();
            }))
            .build();

        let swarm = Arc::new(Mutex::new(swarm));
        let swarm2 = swarm.clone();
        let waker = Arc::new(AtomicWaker::new());
        let waker2 = waker.clone();
        let swarm_task = async_global_executor::spawn::<_, ()>(future::poll_fn(move |cx| {
            waker.register(cx.waker());
            let mut guard = swarm.lock();
            while {
                let swarm = &mut *guard;
                pin_mut!(swarm);
                swarm.poll_next(cx).is_ready()
            } {}
            Poll::Pending
        }));

        Ok(Self {
            swarm: swarm2,
            waker: waker2,
            _swarm_task: Arc::new(swarm_task),
        })
    }

    pub fn local_peer_id(&self) -> PeerId {
        let swarm = self.swarm.lock();
        *swarm.local_peer_id()
    }

    pub fn local_node_name(&self) -> String {
        let swarm = self.swarm.lock();
        swarm.behaviour().local_node_name().to_string()
    }

    pub fn listen_on(&self, addr: Multiaddr) -> Result<impl Stream<Item = ListenerEvent>> {
        let mut swarm = self.swarm.lock();
        let stream = swarm.behaviour_mut().event_stream();
        let listener = swarm.listen_on(addr)?;
        Ok(stream
            .take_while(move |event| match event {
                Event::ListenerClosed(id) if *id == listener => future::ready(false),
                _ => future::ready(true),
            })
            .filter_map(move |event| match event {
                Event::NewListenAddr(id, addr) if id == listener => {
                    future::ready(Some(ListenerEvent::NewListenAddr(addr)))
                }
                Event::ExpiredListenAddr(id, addr) if id == listener => {
                    future::ready(Some(ListenerEvent::ExpiredListenAddr(addr)))
                }
                _ => future::ready(None),
            }))
    }

    pub fn listeners(&self) -> Vec<Multiaddr> {
        let swarm = self.swarm.lock();
        swarm.listeners().cloned().collect()
    }

    pub fn add_external_address(&self, addr: Multiaddr) {
        let mut swarm = self.swarm.lock();
        swarm.add_external_address(addr, AddressScore::Infinite);
    }

    pub fn external_addresses(&self) -> Vec<AddressRecord> {
        let swarm = self.swarm.lock();
        swarm.external_addresses().cloned().collect()
    }

    pub fn add_address(&self, peer: &PeerId, addr: Multiaddr) {
        let mut swarm = self.swarm.lock();
        swarm
            .behaviour_mut()
            .add_address(peer, addr, AddressSource::User);
        self.waker.wake();
    }

    pub fn remove_address(&self, peer: &PeerId, addr: &Multiaddr) {
        let mut swarm = self.swarm.lock();
        swarm.behaviour_mut().remove_address(peer, addr);
        self.waker.wake();
    }

    pub fn dial(&self, peer: &PeerId) -> Result<()> {
        let mut swarm = self.swarm.lock();
        Ok(swarm.dial(peer)?)
    }

    pub fn ban(&self, peer: PeerId) {
        let mut swarm = self.swarm.lock();
        swarm.ban_peer_id(peer)
    }

    pub fn unban(&self, peer: PeerId) {
        let mut swarm = self.swarm.lock();
        swarm.unban_peer_id(peer)
    }

    pub fn peers(&self) -> Vec<PeerId> {
        let swarm = self.swarm.lock();
        swarm.behaviour().peers().copied().collect()
    }

    pub fn connections(&self) -> Vec<(PeerId, Multiaddr)> {
        let swarm = self.swarm.lock();
        swarm
            .behaviour()
            .connections()
            .map(|(peer_id, addr)| (*peer_id, addr.clone()))
            .collect()
    }

    pub fn is_connected(&self, peer: &PeerId) -> bool {
        let swarm = self.swarm.lock();
        swarm.behaviour().is_connected(peer)
    }

    pub fn peer_info(&self, peer: &PeerId) -> Option<PeerInfo> {
        let swarm = self.swarm.lock();
        swarm.behaviour().info(peer).cloned()
    }

    pub async fn bootstrap(&self, peers: &[(PeerId, Multiaddr)]) -> Result<()> {
        for (peer, addr) in peers {
            self.add_address(peer, addr.clone());
            self.dial(peer)?;
        }
        let rx = {
            let mut swarm = self.swarm.lock();
            swarm.behaviour_mut().bootstrap()
        };
        tracing::trace!("started bootstrap");
        rx.await??;
        tracing::trace!("boostrap complete");
        Ok(())
    }

    pub async fn get_closest_peers<K>(&self, key: K) -> Result<Vec<PeerId>>
    where
        K: Into<BucketKey<K>> + Into<Vec<u8>> + Clone,
    {
        let rx = {
            let mut swarm = self.swarm.lock();
            swarm.behaviour_mut().get_closest_peers(key)
        };
        Ok(rx.await??)
    }

    pub async fn providers(&self, key: Key) -> Result<HashSet<PeerId>> {
        let rx = {
            let mut swarm = self.swarm.lock();
            swarm.behaviour_mut().providers(key)
        };
        Ok(rx.await??)
    }

    pub async fn provide(&self, key: Key) -> Result<()> {
        let rx = {
            let mut swarm = self.swarm.lock();
            swarm.behaviour_mut().provide(key)
        };
        Ok(rx.await??)
    }

    pub fn unprovide(&self, key: &Key) {
        let mut swarm = self.swarm.lock();
        swarm.behaviour_mut().unprovide(key)
    }

    pub async fn get_record(&self, key: &Key, quorum: Quorum) -> Result<Vec<PeerRecord>> {
        let rx = {
            let mut swarm = self.swarm.lock();
            swarm.behaviour_mut().get_record(key, quorum)
        };
        Ok(rx.await??)
    }

    pub async fn put_record(&self, record: Record, quorum: Quorum) -> Result<()> {
        let rx = {
            let mut swarm = self.swarm.lock();
            swarm.behaviour_mut().put_record(record, quorum)
        };
        rx.await??;
        Ok(())
    }

    pub fn remove_record(&self, key: &Key) {
        let mut swarm = self.swarm.lock();
        swarm.behaviour_mut().remove_record(key)
    }

    pub fn subscribe(&self, topic: &str) -> Result<impl Stream<Item = Vec<u8>>> {
        let mut swarm = self.swarm.lock();
        swarm.behaviour_mut().subscribe(topic)
    }

    pub fn publish(&self, topic: &str, msg: Vec<u8>) -> Result<()> {
        let mut swarm = self.swarm.lock();
        swarm.behaviour_mut().publish(topic, msg)
    }

    pub fn broadcast(&self, topic: &str, msg: Vec<u8>) -> Result<()> {
        let mut swarm = self.swarm.lock();
        swarm.behaviour_mut().broadcast(topic, msg)
    }

    pub fn get(&self, cid: Cid, providers: impl Iterator<Item = PeerId>) -> GetQuery<P> {
        let mut swarm = self.swarm.lock();
        let (rx, id) = swarm.behaviour_mut().get(cid, providers);
        self.waker.wake();
        GetQuery {
            swarm: Some(self.swarm.clone()),
            id,
            rx,
        }
    }

    pub fn sync(
        &self,
        cid: Cid,
        providers: Vec<PeerId>,
        missing: impl Iterator<Item = Cid>,
    ) -> SyncQuery<P> {
        let mut swarm = self.swarm.lock();
        let (rx, id) = swarm.behaviour_mut().sync(cid, providers, missing);
        self.waker.wake();
        SyncQuery {
            swarm: Some(self.swarm.clone()),
            id,
            rx,
        }
    }

    pub fn register_metrics(&self, registry: &Registry) -> Result<()> {
        let swarm = self.swarm.lock();
        swarm.behaviour().register_metrics(registry)
    }

    pub fn event_stream(&self) -> impl Stream<Item = Event> {
        let mut swarm = self.swarm.lock();
        swarm.behaviour_mut().event_stream()
    }
}

pub struct GetQuery<P: StoreParams> {
    swarm: Option<Arc<Mutex<Swarm<NetworkBackendBehaviour<P>>>>>,
    id: QueryId,
    rx: GetChannel,
}

impl<P: StoreParams> Future for GetQuery<P> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<P: StoreParams> Drop for GetQuery<P> {
    fn drop(&mut self) {
        let swarm = self.swarm.take().unwrap();
        let mut swarm = swarm.lock();
        swarm.behaviour_mut().cancel(self.id);
    }
}

/// A `bitswap` sync query.
pub struct SyncQuery<P: StoreParams> {
    swarm: Option<Arc<Mutex<Swarm<NetworkBackendBehaviour<P>>>>>,
    id: QueryId,
    rx: SyncChannel,
}

impl<P: StoreParams> Future for SyncQuery<P> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        loop {
            match Pin::new(&mut self.rx).poll_next(cx) {
                Poll::Ready(Some(SyncEvent::Complete(result))) => return Poll::Ready(result),
                Poll::Ready(_) => continue,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<P: StoreParams> Stream for SyncQuery<P> {
    type Item = SyncEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_next(cx)
    }
}

impl<P: StoreParams> Drop for SyncQuery<P> {
    fn drop(&mut self) {
        let swarm = self.swarm.take().unwrap();
        let mut swarm = swarm.lock();
        swarm.behaviour_mut().cancel(self.id);
    }
}
