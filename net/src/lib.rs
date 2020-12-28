use fnv::FnvHashSet;
use futures::channel::{mpsc, oneshot};
use futures::future::Future;
use futures::stream::Stream;
use ipfs_embed_core::{
    Channel, Cid, Multiaddr, Network, NetworkEvent, PeerId, Result, StoreParams,
};
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::Transport;
use libp2p::dns::DnsConfig;
use libp2p::mplex::MplexConfig;
use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
use libp2p::swarm::{AddressRecord, AddressScore, Swarm, SwarmBuilder};
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

pub struct NetworkService<P: StoreParams> {
    _marker: PhantomData<P>,
    tx: mpsc::UnboundedSender<SwarmMsg<P>>,
    local_peer_id: PeerId,
}

impl<P: StoreParams> NetworkService<P> {
    pub async fn new(config: NetworkConfig) -> Result<Self> {
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
        let behaviour = NetworkBackendBehaviour::<P>::new(config.clone()).await?;
        let mut swarm = SwarmBuilder::new(transport.boxed(), behaviour, peer_id)
            .executor(Box::new(|fut| {
                async_global_executor::spawn(fut).detach();
            }))
            .build();
        for addr in config.listen_addresses {
            Swarm::listen_on(&mut swarm, addr)?;
        }
        for addr in config.public_addresses {
            Swarm::add_external_address(&mut swarm, addr, AddressScore::Infinite);
        }

        let (tx, rx) = mpsc::unbounded();
        async_global_executor::spawn(NetworkWorker {
            swarm,
            rx,
            subscription: None,
        })
        .detach();

        Ok(Self {
            _marker: PhantomData,
            tx,
            local_peer_id: peer_id,
        })
    }
}

enum SwarmMsg<P: StoreParams> {
    Get(Cid),
    CancelGet(Cid),
    Sync(Cid, FnvHashSet<Cid>),
    CancelSync(Cid),
    AddMissing(Cid, FnvHashSet<Cid>),
    SendHave(Channel, bool),
    SendBlock(Channel, Option<Vec<u8>>),
    Provide(Cid),
    Unprovide(Cid),
    Listeners(oneshot::Sender<Vec<Multiaddr>>),
    ExternalAddresses(oneshot::Sender<Vec<AddressRecord>>),
    Subscribe(mpsc::UnboundedSender<NetworkEvent<P>>),
}

impl<P: StoreParams + 'static> Network<P> for NetworkService<P> {
    type Subscription = mpsc::UnboundedReceiver<NetworkEvent<P>>;

    fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    fn listeners(&self, tx: oneshot::Sender<Vec<Multiaddr>>) {
        self.tx.unbounded_send(SwarmMsg::Listeners(tx)).ok();
    }

    fn external_addresses(&self, tx: oneshot::Sender<Vec<AddressRecord>>) {
        self.tx.unbounded_send(SwarmMsg::ExternalAddresses(tx)).ok();
    }

    fn get(&self, cid: Cid) {
        self.tx.unbounded_send(SwarmMsg::Get(cid)).ok();
    }

    fn cancel_get(&self, cid: Cid) {
        self.tx.unbounded_send(SwarmMsg::CancelGet(cid)).ok();
    }

    fn sync(&self, cid: Cid, missing: FnvHashSet<Cid>) {
        self.tx.unbounded_send(SwarmMsg::Sync(cid, missing)).ok();
    }

    fn cancel_sync(&self, cid: Cid) {
        self.tx.unbounded_send(SwarmMsg::CancelSync(cid)).ok();
    }

    fn add_missing(&self, cid: Cid, missing: FnvHashSet<Cid>) {
        self.tx
            .unbounded_send(SwarmMsg::AddMissing(cid, missing))
            .ok();
    }

    fn send_have(&self, ch: Channel, have: bool) {
        self.tx.unbounded_send(SwarmMsg::SendHave(ch, have)).ok();
    }

    fn send_block(&self, ch: Channel, block: Option<Vec<u8>>) {
        self.tx.unbounded_send(SwarmMsg::SendBlock(ch, block)).ok();
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
    rx: mpsc::UnboundedReceiver<SwarmMsg<P>>,
    subscription: Option<mpsc::UnboundedSender<NetworkEvent<P>>>,
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
                SwarmMsg::Sync(cid, missing) => self.swarm.sync(cid, missing),
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
                SwarmMsg::AddMissing(cid, missing) => self.swarm.add_missing(cid, missing),
                SwarmMsg::SendHave(ch, have) => self.swarm.send_have(ch, have),
                SwarmMsg::SendBlock(ch, block) => self.swarm.send_block(ch, block),
                SwarmMsg::Subscribe(tx) => {
                    self.subscription = Some(tx);
                }
            }
        }
        loop {
            match Pin::new(&mut self.swarm).poll_next(ctx) {
                Poll::Ready(Some(ev)) => {
                    if let Some(tx) = self.subscription.as_mut() {
                        if tx.unbounded_send(ev).is_err() {
                            self.subscription = None;
                        }
                    }
                }
                Poll::Pending => break,
                Poll::Ready(None) => return Poll::Ready(()),
            }
        }
        Poll::Pending
    }
}
