use crate::network::NetworkConfig;
use core::task::{Context, Poll};
use libipld::cid::Cid;
use libp2p::core::PeerId;
use libp2p::kad::record::store::MemoryStore;
use libp2p::kad::Kademlia;
use libp2p::mdns::Mdns;
use libp2p::ping::Ping;
use libp2p::swarm::toggle::Toggle;
use libp2p::swarm::{NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters};
use libp2p::NetworkBehaviour;
use libp2p_bitswap::{Bitswap, BitswapEvent, Priority};
use std::collections::VecDeque;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NetworkEvent {
    ReceivedBlock(PeerId, Cid, Box<[u8]>),
    ReceivedWant(PeerId, Cid),
}

/// Behaviour type.
#[derive(NetworkBehaviour)]
#[behaviour(poll_method = "custom_poll", out_event = "NetworkEvent")]
pub struct NetworkBackendBehaviour {
    mdns: Toggle<Mdns>,
    kad: Kademlia<MemoryStore>,
    ping: Toggle<Ping>,
    bitswap: Bitswap,
    #[behaviour(ignore)]
    events: VecDeque<NetworkEvent>,
}

impl NetworkBehaviourEventProcess<BitswapEvent> for NetworkBackendBehaviour {
    fn inject_event(&mut self, event: BitswapEvent) {
        let event = match event {
            BitswapEvent::ReceivedBlock(peer_id, cid, data) => {
                log::debug!("received block {}", cid.to_string());
                NetworkEvent::ReceivedBlock(peer_id, cid, data)
            }
            BitswapEvent::ReceivedWant(peer_id, cid, _) => {
                log::debug!("received want {}", cid.to_string());
                NetworkEvent::ReceivedWant(peer_id, cid)
            }
            BitswapEvent::ReceivedCancel(_, _) => return,
        };
        self.events.push_back(event);
    }
}

impl NetworkBackendBehaviour {
    /// Create a Kademlia behaviour with the IPFS bootstrap nodes.
    pub fn new(config: NetworkConfig) -> Self {
        let mdns = if config.enable_mdns {
            Some(Mdns::new().expect("Failed to create mDNS service"))
        } else {
            None
        }
        .into();

        let store = MemoryStore::new(config.peer_id());
        let mut kad = Kademlia::new(config.peer_id(), store);
        for (addr, peer_id) in &config.bootstrap_nodes {
            kad.add_address(peer_id, addr.to_owned());
        }

        let ping = if config.enable_ping {
            Some(Ping::default())
        } else {
            None
        }
        .into();

        let bitswap = Bitswap::new();

        Self {
            mdns,
            kad,
            ping,
            bitswap,
            events: Default::default(),
        }
    }

    pub fn connect(&mut self, peer_id: PeerId) {
        self.bitswap.connect(peer_id);
    }

    pub fn send_block(&mut self, peer_id: &PeerId, cid: Cid, data: Box<[u8]>) {
        log::debug!("send {}", cid.to_string());
        self.bitswap.send_block(peer_id, cid, data);
    }

    pub fn want_block(&mut self, cid: Cid, priority: Priority) {
        log::debug!("want {}", cid.to_string());
        let key = cid.hash().to_owned().into();
        self.kad.get_providers(key);
        self.bitswap.want_block(cid, priority);
    }

    pub fn cancel_block(&mut self, cid: &Cid) {
        log::debug!("cancel {}", cid.to_string());
        self.bitswap.cancel_block(cid);
    }

    pub fn provide_block(&mut self, cid: &Cid) {
        log::debug!("provide {}", cid.to_string());
        let key = cid.hash().to_owned().into();
        self.kad.start_providing(key);
    }

    pub fn provide_and_send_block(&mut self, cid: &Cid, data: &[u8]) {
        self.provide_block(&cid);
        self.bitswap.send_block_all(&cid, &data);
    }

    pub fn unprovide_block(&mut self, cid: &Cid) {
        log::debug!("unprovide {}", cid.to_string());
        let key = cid.hash().to_owned().into();
        self.kad.stop_providing(&key);
    }

    pub fn custom_poll<T>(
        &mut self,
        _: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<T, NetworkEvent>> {
        if let Some(event) = self.events.pop_front() {
            Poll::Ready(NetworkBehaviourAction::GenerateEvent(event))
        } else {
            Poll::Pending
        }
    }
}
