use crate::config::NetworkConfig;
use crate::query::{QueryEvent, QueryManager};
use ip_network::IpNetwork;
use ipfs_embed_core::{Cid, NetworkEvent, Query, Result, StoreParams};
use libp2p::core::PeerId;
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
use libp2p::swarm::{
    DialPeerCondition, NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters,
};
use libp2p::NetworkBehaviour;
use libp2p_bitswap::{Bitswap, BitswapConfig, BitswapEvent};
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::task::{Context, Poll};
use thiserror::Error;

enum Event {
    Generate(NetworkEvent),
    Dial(PeerId),
}

/// Behaviour type.
#[derive(NetworkBehaviour)]
#[behaviour(poll_method = "custom_poll", out_event = "NetworkEvent")]
pub struct NetworkBackendBehaviour<S: StoreParams> {
    #[behaviour(ignore)]
    peer_id: PeerId,

    kad: Kademlia<MemoryStore>,
    #[behaviour(ignore)]
    allow_non_globals_in_dht: bool,

    mdns: Toggle<Mdns>,
    ping: Toggle<Ping>,
    identify: Identify,
    bitswap: Bitswap<S::Hashes>,

    #[behaviour(ignore)]
    queries: QueryManager,
    #[behaviour(ignore)]
    provided_before_bootstrap: Option<Vec<Key>>,
    #[behaviour(ignore)]
    events: VecDeque<Event>,
}

impl<S: StoreParams> NetworkBehaviourEventProcess<MdnsEvent> for NetworkBackendBehaviour<S> {
    fn inject_event(&mut self, event: MdnsEvent) {
        match event {
            MdnsEvent::Discovered(list) => {
                for (peer_id, _) in list {
                    if self.queries.discover_mdns(peer_id.clone()) {
                        self.events.push_back(Event::Dial(peer_id));
                    }
                }
            }
            MdnsEvent::Expired(list) => {
                for (peer_id, _) in list {
                    self.queries.expired_mdns(&peer_id);
                }
            }
        }
    }
}

impl<S: StoreParams> NetworkBehaviourEventProcess<KademliaEvent> for NetworkBackendBehaviour<S> {
    fn inject_event(&mut self, event: KademliaEvent) {
        match event {
            KademliaEvent::QueryResult { result, .. } => match result {
                KademliaQueryResult::GetProviders(Ok(GetProvidersOk {
                    key, providers, ..
                })) => {
                    if let Ok(cid) = Cid::try_from(key.as_ref()) {
                        self.queries
                            .complete_get_providers(cid, providers.into_iter().collect());
                    }
                }
                KademliaQueryResult::GetProviders(Err(err)) => {
                    if let Ok(cid) = Cid::try_from(err.into_key().as_ref()) {
                        self.queries.complete_get_providers(cid, Default::default());
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
            },
            _ => {}
        }
    }
}

impl<S: StoreParams> NetworkBehaviourEventProcess<BitswapEvent> for NetworkBackendBehaviour<S> {
    fn inject_event(&mut self, event: BitswapEvent) {
        match event {
            BitswapEvent::Have { peer_id, cid, have } => {
                self.queries.complete_have_query(cid, peer_id, have);
            }
            BitswapEvent::Block { peer_id, cid, data } => {
                self.events
                    .push_back(Event::Generate(NetworkEvent::Response(cid, data)));
                self.queries.complete_want_query(cid, peer_id);
            }
            BitswapEvent::Want { peer_id, cid } => {
                self.events
                    .push_back(Event::Generate(NetworkEvent::Request(peer_id, cid)));
            }
        }
    }
}

impl<S: StoreParams> NetworkBehaviourEventProcess<PingEvent> for NetworkBackendBehaviour<S> {
    fn inject_event(&mut self, _event: PingEvent) {
        // Don't really need to do anything here as ping handles disconnecting automatically.
    }
}

impl<S: StoreParams> NetworkBehaviourEventProcess<IdentifyEvent> for NetworkBackendBehaviour<S> {
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

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadRecordError(pub libp2p::kad::record::store::Error);

impl<S: StoreParams> NetworkBackendBehaviour<S> {
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

        let mut bitswap_config = BitswapConfig::default();
        bitswap_config.max_packet_size = S::MAX_BLOCK_SIZE;
        let bitswap = Bitswap::new(bitswap_config);

        Ok(Self {
            peer_id,
            allow_non_globals_in_dht: config.allow_non_globals_in_dht,
            mdns,
            kad,
            ping,
            identify,
            bitswap,
            events: Default::default(),
            queries: Default::default(),
            provided_before_bootstrap: Some(Default::default()),
        })
    }

    pub fn query(&mut self, query: Query) {
        self.queries.start(query);
    }

    pub fn provide(&mut self, cid: Cid) {
        if let Err(err) = self.bitswap.have_block(&cid) {
            log::error!("bitswap: provide: {:?}", err);
        }
        let key = Key::new(&cid.to_bytes());
        if let Some(provide) = self.provided_before_bootstrap.as_mut() {
            provide.push(key);
        } else {
            if let Err(err) = self.kad.start_providing(key) {
                log::error!("kad: provide: {:?}", err);
            }
        }
    }

    pub fn unprovide(&mut self, cid: Cid) {
        self.bitswap.dont_have_block(&cid);
        let key = Key::new(&cid.to_bytes());
        self.kad.stop_providing(&key);
    }

    pub fn send(&mut self, peer_id: PeerId, cid: Cid, data: Option<Vec<u8>>) {
        self.bitswap.send_block(peer_id, cid, data);
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
    ) -> Poll<NetworkBehaviourAction<T, NetworkEvent>> {
        while let Some(event) = self.queries.next() {
            match event {
                QueryEvent::GetProviders(cid) => {
                    log::debug!("query providers {}", cid);
                    let key = Key::new(&cid.to_bytes());
                    self.kad.get_providers(key);
                }
                QueryEvent::Have(peer_id, cid) => {
                    log::debug!("query have {} {}", peer_id, cid);
                    self.bitswap.has_block(peer_id, cid);
                }
                QueryEvent::Want(peer_id, cid) => {
                    log::debug!("query want {} {}", peer_id, cid);
                    self.bitswap.want_block(peer_id, cid);
                }
                QueryEvent::Complete(query, res) => {
                    self.events
                        .push_back(Event::Generate(NetworkEvent::Complete(query, res)));
                }
            }
        }
        if let Some(event) = self.events.pop_front() {
            match event {
                Event::Generate(event) => {
                    return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event))
                }
                Event::Dial(peer_id) => {
                    return Poll::Ready(NetworkBehaviourAction::DialPeer {
                        peer_id,
                        condition: DialPeerCondition::NotDialing,
                    })
                }
            }
        }
        Poll::Pending
    }
}
