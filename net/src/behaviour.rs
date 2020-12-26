use crate::config::NetworkConfig;
use fnv::FnvHashSet;
use ip_network::IpNetwork;
use ipfs_embed_core::{Cid, NetworkEvent, Result, StoreParams};
use libp2p::core::PeerId;
use libp2p::gossipsub::{
    error::PublishError, Gossipsub, GossipsubConfig, GossipsubEvent, MessageAuthenticity, Topic,
};
use libp2p::identify::{Identify, IdentifyEvent};
use libp2p::kad::record::store::MemoryStore;
use libp2p::kad::record::Key;
use libp2p::kad::{
    BootstrapError, BootstrapOk, GetProvidersOk, Kademlia, KademliaEvent,
    QueryResult as KademliaQueryResult,
};
use libp2p::mdns::{Mdns, MdnsEvent};
use libp2p::multiaddr::Protocol;
use libp2p::ping::{Ping, PingEvent};
use libp2p::swarm::toggle::Toggle;
use libp2p::swarm::{NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters};
use libp2p::NetworkBehaviour;
use libp2p_bitswap::{Bitswap, BitswapConfig, BitswapEvent, Channel};
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;

/// Behaviour type.
#[derive(NetworkBehaviour)]
#[behaviour(poll_method = "custom_poll", out_event = "NetworkEvent<P>")]
pub struct NetworkBackendBehaviour<P: StoreParams> {
    #[behaviour(ignore)]
    peer_id: PeerId,
    #[behaviour(ignore)]
    allow_non_globals_in_dht: bool,

    kad: Kademlia<MemoryStore>,
    mdns: Toggle<Mdns>,
    ping: Toggle<Ping>,
    gossipsub: Toggle<Gossipsub>,
    identify: Identify,
    bitswap: Bitswap<P>,

    #[behaviour(ignore)]
    provided_before_bootstrap: Option<Vec<Key>>,
    #[behaviour(ignore)]
    mdns_peers: FnvHashSet<PeerId>,
    #[behaviour(ignore)]
    events: VecDeque<NetworkEvent<P>>,
}

impl<P: StoreParams> NetworkBehaviourEventProcess<MdnsEvent> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, event: MdnsEvent) {
        match event {
            MdnsEvent::Discovered(list) => {
                for (peer_id, _) in list {
                    if self.mdns_peers.insert(peer_id) {
                        log::trace!("discovered {}", peer_id);
                    }
                }
            }
            MdnsEvent::Expired(list) => {
                for (peer_id, _) in list {
                    if self.mdns_peers.remove(&peer_id) {
                        log::trace!("expired {}", peer_id);
                    }
                }
            }
        }
    }
}

impl<P: StoreParams> NetworkBehaviourEventProcess<KademliaEvent> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, event: KademliaEvent) {
        if let KademliaEvent::QueryResult { result, .. } = event {
            match result {
                KademliaQueryResult::GetProviders(Ok(GetProvidersOk {
                    key, providers, ..
                })) => {
                    if let Ok(cid) = Cid::try_from(key.as_ref()) {
                        for provider in providers {
                            self.bitswap.add_provider(cid, provider);
                        }
                        self.bitswap.complete_get_providers(cid);
                    }
                }
                KademliaQueryResult::GetProviders(Err(err)) => {
                    if let Ok(cid) = Cid::try_from(err.into_key().as_ref()) {
                        self.bitswap.complete_get_providers(cid);
                    }
                }
                KademliaQueryResult::Bootstrap(Ok(BootstrapOk { num_remaining, .. })) => {
                    if num_remaining == 0 {
                        self.bootstrap_complete();
                    }
                }
                KademliaQueryResult::Bootstrap(Err(BootstrapError::Timeout {
                    num_remaining,
                    ..
                })) => match num_remaining {
                    Some(0) => {
                        self.bootstrap_complete();
                    }
                    None => {
                        self.kad.bootstrap().ok();
                    }
                    _ => {}
                },
                _ => {}
            }
        }
    }
}

impl<P: StoreParams> NetworkBehaviourEventProcess<BitswapEvent<P>> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, event: BitswapEvent<P>) {
        match event {
            BitswapEvent::GetProviders(cid) => {
                for peer in &self.mdns_peers {
                    self.bitswap.add_provider(cid, *peer);
                }
                let key = Key::new(&cid.to_bytes());
                self.kad.get_providers(key);
            }
            BitswapEvent::QueryComplete(query, result) => {
                self.events
                    .push_back(NetworkEvent::QueryComplete(query, result));
            }
            BitswapEvent::HaveBlock(ch, cid) => {
                self.events
                    .push_back(NetworkEvent::HaveBlock(Arc::new(ch), cid));
            }
            BitswapEvent::WantBlock(ch, cid) => {
                self.events
                    .push_back(NetworkEvent::WantBlock(Arc::new(ch), cid));
            }
            BitswapEvent::ReceivedBlock(block) => {
                self.events.push_back(NetworkEvent::ReceivedBlock(block));
            }
            BitswapEvent::MissingBlocks(cid) => {
                self.events.push_back(NetworkEvent::MissingBlocks(cid));
            }
        }
    }
}

impl<P: StoreParams> NetworkBehaviourEventProcess<PingEvent> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, _event: PingEvent) {
        // Don't really need to do anything here as ping handles disconnecting automatically.
    }
}

impl<P: StoreParams> NetworkBehaviourEventProcess<IdentifyEvent> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, event: IdentifyEvent) {
        // When a peer opens a connection we only have it's outgoing address. The identify
        // protocol sends the listening address which needs to be registered with kademlia.
        if let IdentifyEvent::Received {
            peer_id,
            info,
            observed_addr,
        } = event
        {
            log::debug!("has external address {}", observed_addr);
            self.kad.add_address(&self.peer_id, observed_addr);
            for addr in info.listen_addrs {
                let global = match addr.iter().next() {
                    Some(Protocol::Ip4(ip)) => IpNetwork::from(ip).is_global(),
                    Some(Protocol::Ip6(ip)) => IpNetwork::from(ip).is_global(),
                    Some(Protocol::Dns(_)) => true,
                    Some(Protocol::Dns4(_)) => true,
                    Some(Protocol::Dns6(_)) => true,
                    _ => false,
                };
                if self.allow_non_globals_in_dht || global {
                    log::debug!("adding kademlia address {} {}", info.agent_version, addr);
                    self.kad.add_address(&peer_id, addr);
                } else {
                    log::debug!(
                        "not adding kademlia address {} {}",
                        info.agent_version,
                        addr,
                    );
                }
            }
        }
    }
}

impl<P: StoreParams> NetworkBehaviourEventProcess<GossipsubEvent> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, event: GossipsubEvent) {
        self.events.push_back(NetworkEvent::Gossip(event));
    }
}

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadRecordError(pub libp2p::kad::record::store::Error);

impl<P: StoreParams> NetworkBackendBehaviour<P> {
    /// Create a Kademlia behaviour with the IPFS bootstrap nodes.
    pub async fn new(config: NetworkConfig) -> Result<Self> {
        let peer_id = config.peer_id();

        let mdns = if config.enable_mdns {
            Some(Mdns::new().await?)
        } else {
            None
        }
        .into();

        let kad_store = MemoryStore::new(peer_id);
        let mut kad = Kademlia::new(peer_id, kad_store);
        for (addr, peer_id) in &config.boot_nodes {
            kad.add_address(peer_id, addr.to_owned());
        }
        if !config.boot_nodes.is_empty() {
            kad.bootstrap().expect("bootstrap nodes not empty");
        }

        let ping = if config.enable_ping {
            Some(Ping::default())
        } else {
            None
        }
        .into();

        let gossipsub = if config.enable_gossipsub {
            Some(Gossipsub::new(
                MessageAuthenticity::Signed(config.node_key.clone()),
                GossipsubConfig::default(),
            ))
        } else {
            None
        }
        .into();

        let public = config.public();
        let identify = Identify::new("/ipfs-embed/1.0".into(), config.node_name.clone(), public);

        let mut bitswap_config = BitswapConfig::new();
        bitswap_config.request_timeout = config.bitswap_request_timeout;
        bitswap_config.connection_keep_alive = config.bitswap_connection_keepalive;
        bitswap_config.receive_limit = config.bitswap_receive_limit;
        let bitswap = Bitswap::new(bitswap_config);

        Ok(Self {
            peer_id,
            allow_non_globals_in_dht: config.allow_non_globals_in_dht,
            mdns,
            kad,
            ping,
            identify,
            bitswap,
            gossipsub,
            events: Default::default(),
            mdns_peers: Default::default(),
            provided_before_bootstrap: Some(Default::default()),
        })
    }

    pub fn get(&mut self, cid: Cid) {
        self.bitswap.get(cid);
    }

    pub fn cancel_get(&mut self, cid: Cid) {
        self.bitswap.cancel_get(cid);
    }

    pub fn sync(&mut self, cid: Cid, missing: FnvHashSet<Cid>) {
        self.bitswap.sync(cid, missing);
    }

    pub fn cancel_sync(&mut self, cid: Cid) {
        self.bitswap.cancel_sync(cid);
    }

    pub fn add_missing(&mut self, cid: Cid, missing: FnvHashSet<Cid>) {
        self.bitswap.add_missing(cid, missing);
    }

    pub fn send_have(&mut self, ch: Channel, have: bool) {
        self.bitswap.send_have(ch, have);
    }

    pub fn send_block(&mut self, ch: Channel, block: Option<Vec<u8>>) {
        self.bitswap.send_block(ch, block);
    }

    pub fn provide(&mut self, cid: Cid) {
        let key = Key::new(&cid.to_bytes());
        if let Some(provide) = self.provided_before_bootstrap.as_mut() {
            provide.push(key);
        } else if let Err(err) = self.kad.start_providing(key) {
            log::error!("kad: provide: {:?}", err);
        }
    }

    pub fn unprovide(&mut self, cid: Cid) {
        let key = Key::new(&cid.to_bytes());
        self.kad.stop_providing(&key);
    }

    pub fn subscribe(&mut self, topic: Topic) -> bool {
        if let Some(gossipsub) = self.gossipsub.as_mut() {
            gossipsub.subscribe(topic)
        } else {
            false
        }
    }

    pub fn unsubscribe(&mut self, topic: Topic) -> bool {
        if let Some(gossipsub) = self.gossipsub.as_mut() {
            gossipsub.unsubscribe(topic)
        } else {
            false
        }
    }

    pub fn publish(&mut self, topic: &Topic, bytes: Vec<u8>) -> Result<(), PublishError> {
        if let Some(gossipsub) = self.gossipsub.as_mut() {
            gossipsub.publish(topic, bytes)
        } else {
            Ok(())
        }
    }

    fn bootstrap_complete(&mut self) {
        log::info!("bootstrap complete");
        if let Some(provide) = self.provided_before_bootstrap.take() {
            for key in provide {
                if let Err(err) = self.kad.start_providing(key) {
                    log::error!("kad: provide: {:?}", err);
                }
            }
        }
    }

    pub fn custom_poll<T>(
        &mut self,
        _: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<T, NetworkEvent<P>>> {
        if let Some(event) = self.events.pop_front() {
            Poll::Ready(NetworkBehaviourAction::GenerateEvent(event))
        } else {
            Poll::Pending
        }
    }
}
