use async_std::prelude::*;
use async_std::task::{self, Context, Poll};
use core::pin::Pin;
use libipld_core::store::Visibility;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::Transport;
use libp2p::core::Multiaddr;
use libp2p::secio::SecioConfig;
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::tcp::TcpConfig;
use libp2p::yamux::Config as YamuxConfig;
use std::time::Duration;

mod behaviour;
mod config;
mod kad;
mod mdns;
mod ping;

use crate::error::Error;
use crate::storage::{Storage, NetworkEvent as StorageEvent, NetworkSubscriber as StorageSubscriber};
use behaviour::NetworkBackendBehaviour;
pub use behaviour::NetworkEvent;
pub use config::NetworkConfig;

pub struct Network {
    swarm: Swarm<NetworkBackendBehaviour>,
    storage: Storage,
    subscriber: StorageSubscriber,
}

impl Network {
    pub fn new(config: NetworkConfig, storage: Storage) -> Result<(Self, Multiaddr), Error> {
        let transport = TcpConfig::new()
            .nodelay(true)
            .upgrade(Version::V1)
            .authenticate(SecioConfig::new(config.keypair.clone()))
            .multiplex(YamuxConfig::default())
            .timeout(Duration::from_secs(20));

        let peer_id = config.peer_id();
        let behaviour = NetworkBackendBehaviour::new(config);
        let mut swarm = Swarm::new(transport, behaviour, peer_id.clone());
        Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap())?;
        let event = task::block_on(swarm.next_event());
        let addr = if let SwarmEvent::NewListenAddr(addr) = event {
            addr
        } else {
            // TODO: Can this happen?
            panic!("failed to start listener");
        };

        for want in storage.wanted() {
            swarm.want_block(want?, 10);
        }
        for public in storage.public() {
            swarm.provide_block(&public?);
        }
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
            let network = match Pin::new(&mut self.swarm).poll_next(ctx) {
                Poll::Ready(Some(event)) => Some(event),
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => None,
            };
            let storage = match Pin::new(&mut self.subscriber).poll_next(ctx) {
                Poll::Ready(Some(event)) => Some(event),
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => None,
            };
            if network.is_none() && storage.is_none() {
                return Poll::Pending;
            }
            if let Some(event) = network {
                match event {
                    NetworkEvent::ReceivedBlock(_, cid, data) => {
                        if let Err(err) = self.storage.insert(&cid, data.into(), Visibility::Public)
                        {
                            log::error!("failed to insert received block {:?}", err);
                        }
                    }
                    NetworkEvent::ReceivedWant(peer_id, cid) => {
                        match self.storage.get_local(&cid) {
                            Ok(Some(block)) => self.swarm.send_block(&peer_id, cid, block),
                            Ok(None) => log::trace!("don't have local block {}", cid.to_string()),
                            Err(err) => log::error!("failed to get local block {:?}", err),
                        }
                    }
                }
            }
            if let Some(event) = storage {
                match event {
                    StorageEvent::Want(cid) => self.swarm.want_block(cid, 1000),
                    StorageEvent::Cancel(cid) => self.swarm.cancel_block(&cid),
                    StorageEvent::Provide(cid) => self.swarm.provide_block(&cid),
                    StorageEvent::Unprovide(cid) => self.swarm.unprovide_block(&cid),
                }
            }
        }
    }
}
