use async_std::task;
use futures::channel::mpsc;
use futures::future::Future;
use futures::stream::Stream;
use ipfs_embed_core::{Cid, Network, NetworkEvent, PeerId, Result, StoreParams};
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::Transport;
use libp2p::dns::DnsConfig;
use libp2p::mplex::MplexConfig;
use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
use libp2p::swarm::Swarm;
use libp2p::tcp::TcpConfig;
//use libp2p::yamux::Config as YamuxConfig;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

mod behaviour;
mod config;
mod query;

use behaviour::NetworkBackendBehaviour;
pub use config::NetworkConfig;

pub struct NetworkService<S: StoreParams> {
    _marker: PhantomData<S>,
    tx: mpsc::UnboundedSender<SwarmMsg>,
    query_id: AtomicU64,
    local_peer_id: PeerId,
}

impl<S: StoreParams> NetworkService<S> {
    pub fn new(config: NetworkConfig) -> Result<Self> {
        let dh_key = Keypair::<X25519Spec>::new()
            .into_authentic(&config.node_key)
            .unwrap();
        let transport = DnsConfig::new(
            TcpConfig::new()
                .nodelay(true)
                .upgrade(Version::V1)
                .authenticate(NoiseConfig::xx(dh_key).into_authenticated())
                .multiplex(MplexConfig::new())
                .timeout(Duration::from_secs(5)),
        )?;

        let peer_id = config.peer_id();
        let behaviour = NetworkBackendBehaviour::<S>::new(config.clone())?;
        let mut swarm = Swarm::new(transport, behaviour, peer_id.clone());
        for addr in config.listen_addresses {
            Swarm::listen_on(&mut swarm, addr)?;
        }
        for addr in config.public_addresses {
            Swarm::add_external_address(&mut swarm, addr);
        }

        let (tx, rx) = mpsc::unbounded();
        task::spawn(NetworkWorker {
            swarm,
            rx,
            subscriptions: Default::default(),
        });

        Ok(Self {
            _marker: PhantomData,
            tx,
            query_id: Default::default(),
            local_peer_id: peer_id,
        })
    }
}

type QueryId = u64;

enum SwarmMsg {
    Get(Cid, QueryId),
    Sync(Cid, QueryId),
    Cancel(QueryId),
    Provide(Cid),
    Unprovide(Cid),
    Send(PeerId, Cid, Vec<u8>),
    Listeners,
    ExternalAddresses,
    Subscribe(mpsc::UnboundedSender<NetworkEvent<QueryId>>),
}

impl<S: StoreParams + 'static> Network<S> for NetworkService<S> {
    type QueryId = QueryId;
    type Subscription = mpsc::UnboundedReceiver<NetworkEvent<Self::QueryId>>;

    fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    fn listeners(&self) {
        self.tx.unbounded_send(SwarmMsg::Listeners).ok();
    }

    fn external_addresses(&self) {
        self.tx.unbounded_send(SwarmMsg::ExternalAddresses).ok();
    }

    fn get(&self, cid: Cid) -> Self::QueryId {
        let query_id = self.query_id.fetch_add(1, Ordering::SeqCst);
        self.tx.unbounded_send(SwarmMsg::Get(cid, query_id)).ok();
        query_id
    }

    fn sync(&self, cid: Cid) -> Self::QueryId {
        let query_id = self.query_id.fetch_add(1, Ordering::SeqCst);
        self.tx.unbounded_send(SwarmMsg::Sync(cid, query_id)).ok();
        query_id
    }

    fn cancel(&self, query_id: Self::QueryId) {
        self.tx.unbounded_send(SwarmMsg::Cancel(query_id)).ok();
    }

    fn provide(&self, cid: Cid) {
        self.tx.unbounded_send(SwarmMsg::Provide(cid)).ok();
    }

    fn unprovide(&self, cid: Cid) {
        self.tx.unbounded_send(SwarmMsg::Unprovide(cid)).ok();
    }

    fn send(&self, peer_id: PeerId, cid: Cid, data: Vec<u8>) {
        self.tx
            .unbounded_send(SwarmMsg::Send(peer_id, cid, data))
            .ok();
    }

    fn subscribe(&self) -> Self::Subscription {
        let (tx, rx) = mpsc::unbounded();
        self.tx.unbounded_send(SwarmMsg::Subscribe(tx)).ok();
        rx
    }
}

struct NetworkWorker<S: StoreParams> {
    swarm: Swarm<NetworkBackendBehaviour<S>>,
    rx: mpsc::UnboundedReceiver<SwarmMsg>,
    subscriptions: Vec<mpsc::UnboundedSender<NetworkEvent<QueryId>>>,
}

impl<S: StoreParams> NetworkWorker<S> {
    fn send_event(&mut self, ev: NetworkEvent<QueryId>) {
        self.subscriptions
            .retain(|s| s.unbounded_send(ev.clone()).is_ok())
    }
}

impl<S: StoreParams> Future for NetworkWorker<S> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        loop {
            let cmd = match Pin::new(&mut self.rx).poll_next(ctx) {
                Poll::Ready(Some(cmd)) => cmd,
                Poll::Pending => break,
                Poll::Ready(None) => return Poll::Ready(()),
            };
            match cmd {
                SwarmMsg::Get(cid, query_id) => self.swarm.get(cid, query_id),
                SwarmMsg::Sync(cid, query_id) => self.swarm.sync(cid, query_id),
                SwarmMsg::Cancel(query_id) => self.swarm.cancel(query_id),
                SwarmMsg::Provide(cid) => self.swarm.provide(cid),
                SwarmMsg::Unprovide(cid) => self.swarm.unprovide(cid),
                SwarmMsg::Send(peer_id, cid, data) => self.swarm.send(peer_id, cid, data),
                SwarmMsg::Listeners => {
                    let listeners = Swarm::listeners(&mut self.swarm).cloned().collect();
                    self.send_event(NetworkEvent::Listeners(listeners));
                }
                SwarmMsg::ExternalAddresses => {
                    let external_addresses = Swarm::external_addresses(&mut self.swarm)
                        .cloned()
                        .collect();
                    self.send_event(NetworkEvent::ExternalAddresses(external_addresses));
                }
                SwarmMsg::Subscribe(tx) => self.subscriptions.push(tx),
            }
        }
        loop {
            match Pin::new(&mut self.swarm).poll_next(ctx) {
                Poll::Ready(Some(ev)) => self.send_event(ev),
                Poll::Pending => break,
                Poll::Ready(None) => return Poll::Ready(()),
            }
        }
        Poll::Pending
    }
}
