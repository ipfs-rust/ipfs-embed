use async_std::prelude::*;
use async_std::task::{Context, Poll};
use core::pin::Pin;
use libipld::store::Visibility;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::Transport;
use libp2p::core::Multiaddr;
use libp2p::mplex::MplexConfig;
use libp2p::secio::SecioConfig;
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::tcp::TcpConfig;
//use libp2p::yamux::Config as YamuxConfig;
use std::time::Duration;

mod behaviour;
mod config;

use crate::error::Error;
use crate::storage::{
    NetworkEvent as StorageEvent, NetworkSubscriber as StorageSubscriber, Storage,
};
use behaviour::NetworkBackendBehaviour;
pub use behaviour::NetworkEvent;
pub use config::NetworkConfig;

pub struct Network {
    swarm: Swarm<NetworkBackendBehaviour>,
    storage: Storage,
    subscriber: StorageSubscriber,
}

impl Network {
    pub async fn new(config: NetworkConfig, storage: Storage) -> Result<(Self, Multiaddr), Error> {
        let transport = TcpConfig::new()
            .nodelay(true)
            .upgrade(Version::V1)
            .authenticate(SecioConfig::new(config.keypair.clone()))
            .multiplex(MplexConfig::new())
            .timeout(Duration::from_secs(20));

        let peer_id = config.peer_id();
        let behaviour = NetworkBackendBehaviour::new(config)?;
        let mut swarm = Swarm::new(transport, behaviour, peer_id);
        Swarm::listen_on(&mut swarm, "/ip4/0.0.0.0/tcp/0".parse().unwrap())?;
        let addr = loop {
            match swarm.next_event().await {
                SwarmEvent::NewListenAddr(addr) => break addr,
                SwarmEvent::ListenerClosed { reason, .. } => reason?,
                _ => {}
            }
        };

        let subscriber = storage.watch_network();
        Ok((
            Self {
                swarm,
                storage,
                subscriber,
            },
            addr,
        ))
    }
}

impl Future for Network {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        loop {
            let event = match Pin::new(&mut self.subscriber).poll_next(ctx) {
                Poll::Ready(Some(event)) => event,
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            };
            log::trace!("{:?}", event);
            match event {
                StorageEvent::Want(cid) => self.swarm.want_block(cid, 1000),
                StorageEvent::Cancel(cid) => self.swarm.cancel_block(&cid),
                StorageEvent::Provide(cid) => {
                    if let Err(err) = match self.storage.get_local(&cid) {
                        Ok(Some(block)) => self.swarm.provide_and_send_block(&cid, &block),
                        _ => self.swarm.provide_block(&cid),
                    } {
                        log::error!("error providing block {:?}", err);
                    }
                }
                StorageEvent::Unprovide(cid) => self.swarm.unprovide_block(&cid),
            }
        }
        // polling the swarm needs to happen last as calling methods on swarm can
        // make the swarm ready, but won't register a waker.
        loop {
            let event = match Pin::new(&mut self.swarm).poll_next(ctx) {
                Poll::Ready(Some(event)) => event,
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            };
            log::trace!("{:?}", event);
            match event {
                NetworkEvent::ReceivedBlock(_, cid, data) => {
                    if let Err(err) =
                        self.storage
                            .insert(&cid, data.to_vec().into(), Visibility::Public)
                    {
                        log::error!("failed to insert received block {:?}", err);
                    }
                }
                NetworkEvent::ReceivedWant(peer_id, cid) => match self.storage.get_local(&cid) {
                    Ok(Some(block)) => {
                        let data = block.to_vec().into_boxed_slice();
                        self.swarm.send_block(&peer_id, cid, data)
                    }
                    Ok(None) => log::trace!("don't have local block {}", cid.to_string()),
                    Err(err) => log::error!("failed to get local block {:?}", err),
                },
                NetworkEvent::Providers(_cid, providers) => {
                    let peer_id = providers.into_iter().next().unwrap();
                    self.swarm.connect(peer_id);
                }
                NetworkEvent::NoProviders(_cid) => {
                    log::info!("TODO no providers");
                    // abort get
                }
                NetworkEvent::BootstrapComplete => {
                    for public in self.storage.public() {
                        match public.map(|cid| self.swarm.provide_block(&cid)) {
                            Ok(Ok(())) => {}
                            Ok(Err(err)) => log::error!("error providing block {:?}", err),
                            Err(err) => log::error!("error reading public blocks {:?}", err),
                        }
                    }
                }
            }
        }
        Poll::Pending
    }
}
