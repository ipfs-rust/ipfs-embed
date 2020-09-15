use crate::config::NetworkConfig;
use ip_network::IpNetwork;
use ipfs_embed_core::{Cid, MultihashDigest, NetworkEvent, Result};
use libp2p::core::PeerId;
use libp2p::identify::{Identify, IdentifyEvent};
use libp2p::kad::record::store::MemoryStore;
use libp2p::kad::{GetProvidersOk, Kademlia, KademliaEvent, QueryResult};
use libp2p::mdns::{Mdns, MdnsEvent};
use libp2p::multiaddr::Protocol;
use libp2p::ping::{Ping, PingEvent};
use libp2p::swarm::toggle::Toggle;
use libp2p::swarm::{NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters};
use libp2p::NetworkBehaviour;
use libp2p_bitswap::{Bitswap, BitswapEvent};
use std::collections::{HashMap, VecDeque};
use std::convert::TryFrom;
use std::task::{Context, Poll};
use thiserror::Error;

/// Behaviour type.
#[derive(NetworkBehaviour)]
#[behaviour(poll_method = "custom_poll", out_event = "NetworkEvent")]
pub struct NetworkBackendBehaviour<M: MultihashDigest> {
    #[behaviour(ignore)]
    node_name: String,
    #[behaviour(ignore)]
    peer_id: PeerId,
    #[behaviour(ignore)]
    peers: HashMap<PeerId, String>,

    kad: Kademlia<MemoryStore>,
    #[behaviour(ignore)]
    allow_non_globals_in_dht: bool,

    mdns: Toggle<Mdns>,
    ping: Toggle<Ping>,
    identify: Identify,
    bitswap: Bitswap<M>,

    #[behaviour(ignore)]
    events: VecDeque<NetworkEvent>,
}

impl<M: MultihashDigest> NetworkBehaviourEventProcess<MdnsEvent> for NetworkBackendBehaviour<M> {
    fn inject_event(&mut self, event: MdnsEvent) {
        match event {
            MdnsEvent::Discovered(list) => {
                for (peer, _) in list {
                    self.bitswap().connect(peer);
                }
            }
            MdnsEvent::Expired(_) => {}
        }
    }
}

impl<M: MultihashDigest> NetworkBehaviourEventProcess<KademliaEvent>
    for NetworkBackendBehaviour<M>
{
    fn inject_event(&mut self, event: KademliaEvent) {
        match event {
            KademliaEvent::QueryResult { result, .. } => match result {
                QueryResult::GetProviders(Ok(GetProvidersOk { key, providers, .. })) => {
                    if let Ok(cid) = Cid::try_from(key.as_ref()) {
                        self.events
                            .push_back(NetworkEvent::Providers(cid, providers));
                    }
                }
                QueryResult::GetProviders(Err(err)) => {
                    if let Ok(cid) = Cid::try_from(err.into_key().as_ref()) {
                        self.events.push_back(NetworkEvent::GetProvidersFailed(cid));
                    }
                }
                QueryResult::Bootstrap(Ok(_)) => {
                    log::info!("{}: bootstrap complete", self.node_name);
                }
                QueryResult::Bootstrap(Err(err)) => {
                    log::info!("{}: bootstrap error {:?}", self.node_name, err);
                }
                _ => {}
            },
            KademliaEvent::UnroutablePeer { peer } => {
                log::info!(
                    "{}: unroutable peer {}",
                    self.node_name,
                    self.peer_name(&peer)
                );
            }
            KademliaEvent::RoutablePeer { peer, .. } => {
                log::info!(
                    "{}: routable peer {}",
                    self.node_name,
                    self.peer_name(&peer)
                );
            }
            KademliaEvent::PendingRoutablePeer { peer, .. } => {
                log::info!(
                    "{}: pending routable peer {}",
                    self.node_name,
                    self.peer_name(&peer)
                );
            }
            KademliaEvent::RoutingUpdated { peer, .. } => {
                log::info!(
                    "{}: routing updated peer {}",
                    self.node_name,
                    self.peer_name(&peer)
                );
            }
        }
    }
}

impl<M: MultihashDigest> NetworkBehaviourEventProcess<PingEvent> for NetworkBackendBehaviour<M> {
    fn inject_event(&mut self, event: PingEvent) {
        // Don't really need to do anything here as ping handles disconnecting automatically.
        if let Err(err) = &event.result {
            log::debug!(
                "{}: ping: {} {:?}",
                self.node_name,
                event.peer.to_base58(),
                err
            );
        }
    }
}

impl<M: MultihashDigest> NetworkBehaviourEventProcess<IdentifyEvent>
    for NetworkBackendBehaviour<M>
{
    fn inject_event(&mut self, event: IdentifyEvent) {
        // When a peer opens a connection we only have it's outgoing address. The identify
        // protocol sends the listening address which needs to be registered with kademlia.
        if let IdentifyEvent::Received {
            peer_id,
            info,
            observed_addr,
        } = event
        {
            log::info!("{}: has external address {}", self.node_name, observed_addr);
            self.peers
                .insert(peer_id.clone(), info.agent_version.clone());
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
                    log::info!(
                        "{}: adding kademlia address {} {}",
                        self.node_name,
                        info.agent_version,
                        addr
                    );
                    self.kad.add_address(&peer_id, addr);
                } else {
                    log::info!(
                        "{}: not adding kademlia address {} {}",
                        self.node_name,
                        info.agent_version,
                        addr,
                    );
                }
            }
        }
    }
}

impl<M: MultihashDigest> NetworkBehaviourEventProcess<BitswapEvent> for NetworkBackendBehaviour<M> {
    fn inject_event(&mut self, event: BitswapEvent) {
        // Propagate bitswap events to the swarm.
        let event = match event {
            BitswapEvent::ReceivedBlock(peer_id, cid, data) => {
                log::debug!("{}: received block {}", self.node_name, cid.to_string());
                NetworkEvent::ReceivedBlock(peer_id, cid, data.to_vec())
            }
            BitswapEvent::ReceivedWant(peer_id, cid, priority) => {
                log::debug!("{}: received want {}", self.node_name, cid.to_string());
                NetworkEvent::ReceivedWant(peer_id, cid, priority)
            }
            BitswapEvent::ReceivedCancel(_, _) => return,
        };
        self.events.push_back(event);
    }
}

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadRecordError(pub libp2p::kad::record::store::Error);

impl<M: MultihashDigest> NetworkBackendBehaviour<M> {
    /// Create a Kademlia behaviour with the IPFS bootstrap nodes.
    pub fn new(config: NetworkConfig) -> Result<Self> {
        let peer_id = config.peer_id();

        let mdns = if config.enable_mdns {
            Some(Mdns::new()?)
        } else {
            None
        }
        .into();

        let store = MemoryStore::new(peer_id.clone());
        let mut kad = Kademlia::new(peer_id.clone(), store);
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

        let public = config.public();
        let identify = Identify::new("/ipfs-embed/1.0".into(), config.node_name.clone(), public);

        let bitswap = Bitswap::new();

        Ok(Self {
            node_name: config.node_name,
            peer_id,
            allow_non_globals_in_dht: config.allow_non_globals_in_dht,
            mdns,
            kad,
            ping,
            identify,
            bitswap,
            events: Default::default(),
            peers: Default::default(),
        })
    }

    pub fn peer_name(&self, peer_id: &PeerId) -> String {
        self.peers
            .get(peer_id)
            .cloned()
            .unwrap_or_else(|| peer_id.to_string())
    }

    pub fn kad(&mut self) -> &mut Kademlia<MemoryStore> {
        &mut self.kad
    }

    pub fn bitswap(&mut self) -> &mut Bitswap<M> {
        &mut self.bitswap
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
