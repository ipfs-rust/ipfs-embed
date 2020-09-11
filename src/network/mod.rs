use async_std::prelude::*;
use async_std::stream::{interval, Interval};
use futures::channel::{mpsc, oneshot};
use ipfs_embed_db::{BlockStore, Event, Subscription};
use libipld::block::Block;
use libipld::cid::Cid;
use libipld::codec::Decode;
use libipld::error::Result;
use libipld::ipld::Ipld;
use libipld::store::StoreParams;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::Transport;
use libp2p::core::Multiaddr;
use libp2p::mplex::MplexConfig;
use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::tcp::TcpConfig;
//use libp2p::yamux::Config as YamuxConfig;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

mod behaviour;
mod config;

use behaviour::NetworkBackendBehaviour;
pub use behaviour::NetworkEvent;
pub use config::NetworkConfig;

pub struct Network<S: StoreParams> {
    _marker: PhantomData<S>,
    swarm: Swarm<NetworkBackendBehaviour<S::Hashes>>,
    blocks: Arc<BlockStore>,
    rx: mpsc::Receiver<(Cid, oneshot::Sender<Block<S>>)>,
    bootstrap_complete: bool,
    wanted: HashMap<Cid, Wanted<S>>,
    interval: Interval,
    timeout: Duration,
    subscription: Subscription,
}

struct Wanted<S: StoreParams> {
    ch: Vec<oneshot::Sender<Block<S>>>,
    timestamp: Instant,
}

impl<S: StoreParams> Default for Wanted<S> {
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

impl<S: StoreParams> Network<S> {
    pub async fn new(
        config: NetworkConfig,
        blocks: Arc<BlockStore>,
        rx: mpsc::Receiver<(Cid, oneshot::Sender<Block<S>>)>,
    ) -> Result<(Self, Multiaddr)> {
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
        let behaviour = NetworkBackendBehaviour::new(config.clone())?;
        let mut swarm = Swarm::new(transport, behaviour, peer_id);
        for addr in config.listen_addresses {
            Swarm::listen_on(&mut swarm, addr)?;
        }
        for addr in config.public_addresses {
            Swarm::add_external_address(&mut swarm, addr);
        }

        let addr = loop {
            match swarm.next_event().await {
                SwarmEvent::NewListenAddr(addr) => break addr,
                SwarmEvent::ListenerClosed { reason, .. } => reason?,
                _ => {}
            }
        };
        let subscription = blocks.subscribe();

        Ok((
            Self {
                _marker: PhantomData,
                swarm,
                blocks,
                rx,
                bootstrap_complete: false,
                wanted: Default::default(),
                interval: interval(config.timeout),
                timeout: config.timeout,
                subscription,
            },
            addr,
        ))
    }
}

impl<S: StoreParams + Unpin> Future for Network<S>
where
    Ipld: Decode<S::Codecs>,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        loop {
            match Pin::new(&mut self.rx).poll_next(ctx) {
                Poll::Ready(Some((cid, tx))) => {
                    let entry = self.wanted.entry(cid.clone()).or_default();
                    entry.add_receiver(tx);
                    self.swarm.want_block(cid, 1000);
                }
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            }
        }

        if self.bootstrap_complete {
            loop {
                let event = match Pin::new(&mut self.subscription).poll_next(ctx) {
                    Poll::Ready(Some(event)) => event,
                    Poll::Ready(None) => return Poll::Ready(()),
                    Poll::Pending => break,
                };
                log::trace!("{:?}", event);
                match event {
                    Event::Insert(cid) => {
                        if let Err(err) = match self.blocks.get(&cid) {
                            Ok(Some(data)) => self.swarm.provide_and_send_block(&cid, &data),
                            _ => self.swarm.provide_block(&cid),
                        } {
                            log::error!(
                                "{}: error providing block {:?}",
                                self.swarm.node_name(),
                                err
                            );
                        }
                    }
                    Event::Remove(cid) => self.swarm.unprovide_block(&cid),
                }
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
                    self.swarm.cancel_block(cid);
                    false
                }
            });
            let _ = std::mem::replace(&mut self.wanted, wanted);
        }

        // polling the swarm needs to happen last as calling methods on swarm can
        // make the swarm ready, but won't register a waker.
        loop {
            let event = match Pin::new(&mut self.swarm).poll_next(ctx) {
                Poll::Ready(Some(event)) => event,
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            };
            log::trace!("{}: {:?}", self.swarm.node_name(), event);
            match event {
                NetworkEvent::BootstrapComplete => {
                    self.bootstrap_complete = true;
                }
                NetworkEvent::Providers(_cid, providers) => {
                    let peer_id = providers.into_iter().next().unwrap();
                    self.swarm.connect(peer_id);
                }
                NetworkEvent::NoProviders(cid) => {
                    if let Some(_wanted) = self.wanted.remove(&cid) {
                        self.swarm.cancel_block(&cid);
                    }
                }
                NetworkEvent::ReceivedBlock(_, cid, data) => {
                    let block = Block::new_unchecked(cid, data.to_vec());
                    if let Some(wanted) = self.wanted.remove(block.cid()) {
                        wanted.received(&block);
                    }
                }
                NetworkEvent::ReceivedWant(peer_id, cid) => match self.blocks.get(&cid) {
                    Ok(Some(block)) => {
                        let data = block.to_vec().into_boxed_slice();
                        self.swarm.send_block(&peer_id, cid, data)
                    }
                    Ok(None) => log::trace!(
                        "{}: don't have local block {}",
                        self.swarm.node_name(),
                        cid.to_string()
                    ),
                    Err(err) => log::error!(
                        "{}: failed to get local block {:?}",
                        self.swarm.node_name(),
                        err
                    ),
                },
            }
        }
        Poll::Pending
    }
}
