use async_std::task;
use futures::channel::mpsc;
use futures::future::Future;
use futures::stream::Stream;
use ipfs_embed_core::{Cid, MultihashDigest, Network, NetworkEvent, PeerId, Result, StoreParams};
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::Transport;
use libp2p::core::Multiaddr;
use libp2p::kad::record::Key;
use libp2p::mplex::MplexConfig;
use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::tcp::TcpConfig;
//use libp2p::yamux::Config as YamuxConfig;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

mod behaviour;
mod config;

use behaviour::NetworkBackendBehaviour;
pub use config::NetworkConfig;

pub struct NetworkService<S: StoreParams> {
    _marker: PhantomData<S>,
    tx: mpsc::UnboundedSender<SwarmMsg>,
    local_peer_id: PeerId,
    external_addresses: Vec<Multiaddr>,
}

impl<S: StoreParams> NetworkService<S> {
    pub fn new(config: NetworkConfig) -> Result<Self> {
        let dh_key = Keypair::<X25519Spec>::new()
            .into_authentic(&config.node_key)
            .unwrap();
        let transport = TcpConfig::new()
            .nodelay(true)
            .upgrade(Version::V1)
            .authenticate(NoiseConfig::xx(dh_key).into_authenticated())
            .multiplex(MplexConfig::new())
            .timeout(Duration::from_secs(5));

        let peer_id = config.peer_id();
        let behaviour = NetworkBackendBehaviour::<S::Hashes>::new(config.clone())?;
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
            external_addresses: Default::default(),
        })
    }
}

enum SwarmMsg {
    Provide(Key),
    Unprovide(Key),
    Providers(Key),
    Want(Cid, i32),
    Cancel(Cid),
    SendTo(PeerId, Cid, Vec<u8>),
    Send(Cid, Vec<u8>),
    Subscribe(mpsc::UnboundedSender<NetworkEvent>),
}

impl<S: StoreParams + 'static> Network<S> for NetworkService<S> {
    type Subscription = mpsc::UnboundedReceiver<NetworkEvent>;

    fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    fn external_addresses(&self) -> Vec<Multiaddr> {
        self.external_addresses.clone()
    }

    fn provide(&self, cid: &Cid) {
        let key = Key::new(&cid.to_bytes());
        self.tx.unbounded_send(SwarmMsg::Provide(key)).ok();
    }

    fn unprovide(&self, cid: &Cid) {
        let key = Key::new(&cid.to_bytes());
        self.tx.unbounded_send(SwarmMsg::Unprovide(key)).ok();
    }

    fn providers(&self, cid: &Cid) {
        let key = Key::new(&cid.to_bytes());
        self.tx.unbounded_send(SwarmMsg::Providers(key)).ok();
    }

    fn want(&self, cid: Cid, priority: i32) {
        self.tx.unbounded_send(SwarmMsg::Want(cid, priority)).ok();
    }

    fn cancel(&self, cid: Cid) {
        self.tx.unbounded_send(SwarmMsg::Cancel(cid)).ok();
    }

    fn send_to(&self, peer_id: PeerId, cid: Cid, data: Vec<u8>) {
        self.tx.unbounded_send(SwarmMsg::SendTo(peer_id, cid, data)).ok();
    }

    fn send(&self, cid: Cid, data: Vec<u8>) {
        self.tx.unbounded_send(SwarmMsg::Send(cid, data)).ok();
    }

    fn subscribe(&self) -> Self::Subscription {
        let (tx, rx) = mpsc::unbounded();
        self.tx.unbounded_send(SwarmMsg::Subscribe(tx)).ok();
        rx
    }
}

struct NetworkWorker<M: MultihashDigest> {
    swarm: Swarm<NetworkBackendBehaviour<M>>,
    rx: mpsc::UnboundedReceiver<SwarmMsg>,
    subscriptions: Vec<mpsc::UnboundedSender<NetworkEvent>>,
}

impl<M: MultihashDigest> Future for NetworkWorker<M> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        loop {
            let cmd = match Pin::new(&mut self.rx).poll_next(ctx) {
                Poll::Ready(Some(cmd)) => cmd,
                Poll::Pending => break,
                Poll::Ready(None) => return Poll::Ready(()),
            };
            match cmd {
                SwarmMsg::Provide(cid) => {
                    let _ = self.swarm.kad().start_providing(cid);
                }
                SwarmMsg::Unprovide(cid) => self.swarm.kad().stop_providing(&cid),
                SwarmMsg::Providers(cid) => {
                    let _ = self.swarm.kad().get_providers(cid);
                }
                SwarmMsg::Want(cid, priority) => self.swarm.bitswap().want_block(cid, priority),
                SwarmMsg::Cancel(cid) => self.swarm.bitswap().cancel_block(&cid),
                SwarmMsg::SendTo(peer_id, cid, data) => {
                    self.swarm
                        .bitswap()
                        .send_block(&peer_id, cid, data.into_boxed_slice())
                }
                SwarmMsg::Send(cid, data) => self.swarm.bitswap().send_block_all(&cid, &data),
                SwarmMsg::Subscribe(tx) => self.subscriptions.push(tx),
            }
        }
        loop {
            let ev = match Pin::new(&mut self.swarm).poll_next(ctx) {
                Poll::Ready(Some(ev)) => ev,
                Poll::Pending => break,
                Poll::Ready(None) => return Poll::Ready(()),
            };
            self.subscriptions.retain(|s| {
                s.unbounded_send(ev.clone()).is_ok()
            })
        }
        Poll::Pending
    }
}
