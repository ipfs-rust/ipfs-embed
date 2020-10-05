use async_std::task;
use futures::channel::{mpsc, oneshot};
use futures::future::Future;
use futures::stream::Stream;
use ipfs_embed_core::{
    BitswapStore, BitswapSync, Cid, Multiaddr, Network, NetworkEvent, PeerId, Result, StoreParams,
};
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
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

mod behaviour;
mod config;

use behaviour::NetworkBackendBehaviour;
pub use config::NetworkConfig;

pub struct NetworkService<P: StoreParams> {
    _marker: PhantomData<P>,
    tx: mpsc::UnboundedSender<SwarmMsg>,
    local_peer_id: PeerId,
}

impl<P: StoreParams> NetworkService<P> {
    pub fn new<S: BitswapStore<P>>(config: NetworkConfig, store: S) -> Result<Self> {
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
        let behaviour = NetworkBackendBehaviour::<P>::new(config.clone(), store)?;
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
            local_peer_id: peer_id,
        })
    }
}

enum SwarmMsg {
    Get(Cid),
    CancelGet(Cid),
    Sync(Cid, Arc<dyn BitswapSync>),
    CancelSync(Cid),
    Provide(Cid),
    Unprovide(Cid),
    Listeners(oneshot::Sender<Vec<Multiaddr>>),
    ExternalAddresses(oneshot::Sender<Vec<Multiaddr>>),
    Subscribe(mpsc::UnboundedSender<NetworkEvent>),
}

impl<P: StoreParams + 'static> Network<P> for NetworkService<P> {
    type Subscription = mpsc::UnboundedReceiver<NetworkEvent>;

    fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    fn listeners(&self, tx: oneshot::Sender<Vec<Multiaddr>>) {
        self.tx.unbounded_send(SwarmMsg::Listeners(tx)).ok();
    }

    fn external_addresses(&self, tx: oneshot::Sender<Vec<Multiaddr>>) {
        self.tx.unbounded_send(SwarmMsg::ExternalAddresses(tx)).ok();
    }

    fn get(&self, cid: Cid) {
        self.tx.unbounded_send(SwarmMsg::Get(cid)).ok();
    }

    fn cancel_get(&self, cid: Cid) {
        self.tx.unbounded_send(SwarmMsg::CancelGet(cid)).ok();
    }

    fn sync(&self, cid: Cid, syncer: Arc<dyn BitswapSync>) {
        self.tx.unbounded_send(SwarmMsg::Sync(cid, syncer)).ok();
    }

    fn cancel_sync(&self, cid: Cid) {
        self.tx.unbounded_send(SwarmMsg::CancelSync(cid)).ok();
    }

    fn provide(&self, cid: Cid) {
        self.tx.unbounded_send(SwarmMsg::Provide(cid)).ok();
    }

    fn unprovide(&self, cid: Cid) {
        self.tx.unbounded_send(SwarmMsg::Unprovide(cid)).ok();
    }

    fn subscribe(&self) -> Self::Subscription {
        let (tx, rx) = mpsc::unbounded();
        self.tx.unbounded_send(SwarmMsg::Subscribe(tx)).ok();
        rx
    }
}

struct NetworkWorker<P: StoreParams> {
    swarm: Swarm<NetworkBackendBehaviour<P>>,
    rx: mpsc::UnboundedReceiver<SwarmMsg>,
    subscriptions: Vec<mpsc::UnboundedSender<NetworkEvent>>,
}

impl<P: StoreParams> NetworkWorker<P> {
    fn send_event(&mut self, ev: NetworkEvent) {
        self.subscriptions
            .retain(|s| s.unbounded_send(ev.clone()).is_ok())
    }
}

impl<P: StoreParams> Future for NetworkWorker<P> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        loop {
            let cmd = match Pin::new(&mut self.rx).poll_next(ctx) {
                Poll::Ready(Some(cmd)) => cmd,
                Poll::Pending => break,
                Poll::Ready(None) => return Poll::Ready(()),
            };
            match cmd {
                SwarmMsg::Get(cid) => self.swarm.get(cid),
                SwarmMsg::CancelGet(cid) => self.swarm.cancel_get(cid),
                SwarmMsg::Sync(cid, syncer) => self.swarm.sync(cid, syncer),
                SwarmMsg::CancelSync(cid) => self.swarm.cancel_sync(cid),
                SwarmMsg::Provide(cid) => self.swarm.provide(cid),
                SwarmMsg::Unprovide(cid) => self.swarm.unprovide(cid),
                SwarmMsg::Listeners(tx) => {
                    let listeners = Swarm::listeners(&self.swarm).cloned().collect();
                    tx.send(listeners).ok();
                }
                SwarmMsg::ExternalAddresses(tx) => {
                    let external_addresses =
                        Swarm::external_addresses(&self.swarm).cloned().collect();
                    tx.send(external_addresses).ok();
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
