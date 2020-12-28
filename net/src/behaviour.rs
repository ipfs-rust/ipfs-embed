use crate::config::NetworkConfig;
use fnv::{FnvHashMap, FnvHashSet};
use futures::channel::{mpsc, oneshot};
use ip_network::IpNetwork;
use libipld::store::StoreParams;
use libipld::{Block, Cid, Result};
use libp2p::identify::{Identify, IdentifyEvent};
use libp2p::kad::record::store::MemoryStore;
use libp2p::kad::record::Key;
use libp2p::kad::{
    AddProviderOk, BootstrapOk, GetProvidersOk, Kademlia, KademliaEvent, QueryResult,
};
use libp2p::mdns::{Mdns, MdnsEvent};
use libp2p::multiaddr::Protocol;
use libp2p::ping::{Ping, PingEvent};
use libp2p::swarm::toggle::Toggle;
use libp2p::swarm::NetworkBehaviourEventProcess;
use libp2p::NetworkBehaviour;
use libp2p::{Multiaddr, PeerId};
use libp2p_bitswap::{Bitswap, BitswapConfig, BitswapEvent, Channel};
use prometheus::Registry;
use thiserror::Error;

#[derive(Debug)]
pub enum NetworkEvent<S: StoreParams> {
    MissingBlocks(QueryId, Cid),
    Have(Channel, PeerId, Cid),
    Block(Channel, PeerId, Cid),
    Received(QueryId, PeerId, Block<S>),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct QueryId(InnerQueryId);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum InnerQueryId {
    Bitswap(libp2p_bitswap::QueryId),
    Kad(libp2p::kad::QueryId),
}

impl From<libp2p_bitswap::QueryId> for QueryId {
    fn from(id: libp2p_bitswap::QueryId) -> Self {
        Self(InnerQueryId::Bitswap(id))
    }
}

impl From<libp2p::kad::QueryId> for QueryId {
    fn from(id: libp2p::kad::QueryId) -> Self {
        Self(InnerQueryId::Kad(id))
    }
}

/// Behaviour type.
#[derive(NetworkBehaviour)]
pub struct NetworkBackendBehaviour<P: StoreParams> {
    #[behaviour(ignore)]
    peer_id: PeerId,
    #[behaviour(ignore)]
    allow_non_globals_in_dht: bool,
    #[behaviour(ignore)]
    bootstrap_complete: bool,

    kad: Kademlia<MemoryStore>,
    mdns: Toggle<Mdns>,
    ping: Ping,
    identify: Identify,
    bitswap: Bitswap<P>,

    #[behaviour(ignore)]
    provider_queries: FnvHashMap<libp2p::kad::QueryId, libp2p_bitswap::QueryId>,
    #[behaviour(ignore)]
    queries: FnvHashMap<QueryId, oneshot::Sender<Result<()>>>,
    #[behaviour(ignore)]
    mdns_peers: FnvHashSet<PeerId>,
    #[behaviour(ignore)]
    events: mpsc::UnboundedSender<NetworkEvent<P>>,
}

impl<P: StoreParams> NetworkBehaviourEventProcess<MdnsEvent> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, event: MdnsEvent) {
        match event {
            MdnsEvent::Discovered(list) => {
                for (peer_id, _addr) in list {
                    if self.mdns_peers.insert(peer_id) {
                        tracing::trace!("discovered {}", peer_id);
                    }
                }
            }
            MdnsEvent::Expired(list) => {
                for (peer_id, _addr) in list {
                    if self.mdns_peers.remove(&peer_id) {
                        tracing::trace!("expired {}", peer_id);
                    }
                }
            }
        }
    }
}

#[derive(Debug, Error)]
#[error("Trying to use kad before bootstrap completed successfully.")]
pub struct NotBootstrapped;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadStoreError(pub libp2p::kad::record::store::Error);

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadAddProviderError(pub libp2p::kad::AddProviderError);

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadBootstrapError(pub libp2p::kad::BootstrapError);

impl<P: StoreParams> NetworkBehaviourEventProcess<KademliaEvent> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, event: KademliaEvent) {
        tracing::trace!("kademlia event {:?}", event);
        if let KademliaEvent::QueryResult { id, result, .. } = event {
            match result {
                QueryResult::GetProviders(Ok(GetProvidersOk { providers, .. })) => {
                    if let Some(id) = self.provider_queries.remove(&id) {
                        self.bitswap
                            .inject_providers(id, providers.into_iter().collect());
                    }
                }
                QueryResult::GetProviders(Err(err)) => {
                    tracing::trace!("{:?}", err);
                    if let Some(id) = self.provider_queries.remove(&id) {
                        self.bitswap.inject_providers(id, vec![]);
                    }
                }
                QueryResult::Bootstrap(Ok(BootstrapOk { num_remaining, .. })) => {
                    tracing::trace!("remaining {}", num_remaining);
                    if num_remaining == 0 {
                        self.bootstrap_complete = true;
                        if let Some(ch) = self.queries.remove(&id.into()) {
                            ch.send(Ok(())).ok();
                        }
                    }
                }
                QueryResult::Bootstrap(Err(err)) => {
                    tracing::trace!("{:?}", err);
                    if let Some(ch) = self.queries.remove(&id.into()) {
                        ch.send(Err(KadBootstrapError(err).into())).ok();
                    }
                }
                QueryResult::StartProviding(Ok(AddProviderOk { .. })) => {
                    if let Some(ch) = self.queries.remove(&id.into()) {
                        ch.send(Ok(())).ok();
                    }
                }
                QueryResult::StartProviding(Err(err)) => {
                    tracing::trace!("{:?}", err);
                    if let Some(ch) = self.queries.remove(&id.into()) {
                        ch.send(Err(KadAddProviderError(err).into())).ok();
                    }
                }
                _ => {}
            }
        }
    }
}

impl<P: StoreParams> NetworkBehaviourEventProcess<BitswapEvent<P>> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, event: BitswapEvent<P>) {
        match event {
            BitswapEvent::Providers(id, cid) => {
                if self.bootstrap_complete {
                    let key = Key::new(&cid.to_bytes());
                    let kad_id = self.kad.get_providers(key);
                    self.provider_queries.insert(kad_id, id);
                } else {
                    let providers = self.mdns_peers.iter().copied().collect();
                    self.bitswap.inject_providers(id, providers);
                }
            }
            BitswapEvent::Complete(id, result) => {
                if let Some(ch) = self.queries.remove(&id.into()) {
                    ch.send(result.map_err(Into::into)).ok();
                }
            }
            BitswapEvent::Have(ch, peer, cid) => {
                self.events
                    .unbounded_send(NetworkEvent::Have(ch, peer, cid))
                    .ok();
            }
            BitswapEvent::Block(ch, peer, cid) => {
                self.events
                    .unbounded_send(NetworkEvent::Block(ch, peer, cid))
                    .ok();
            }
            BitswapEvent::Received(id, peer, block) => {
                self.events
                    .unbounded_send(NetworkEvent::Received(id.into(), peer, block))
                    .ok();
            }
            BitswapEvent::MissingBlocks(id, cid) => {
                self.events
                    .unbounded_send(NetworkEvent::MissingBlocks(id.into(), cid))
                    .ok();
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
            peer_id: _,
            info,
            observed_addr,
        } = event
        {
            tracing::debug!("has external address {}", observed_addr);
            let peer_id = self.peer_id;
            self.add_address(&peer_id, observed_addr);
            for addr in info.listen_addrs {
                self.add_address(&peer_id, addr);
            }
        }
    }
}

impl<P: StoreParams> NetworkBackendBehaviour<P> {
    /// Create a Kademlia behaviour with the IPFS bootstrap nodes.
    pub async fn new(
        config: NetworkConfig,
        events: mpsc::UnboundedSender<NetworkEvent<P>>,
    ) -> Result<Self> {
        let peer_id = config.peer_id();
        let mdns = if config.enable_mdns {
            Some(Mdns::new().await?)
        } else {
            None
        }
        .into();
        let kad_store = MemoryStore::new(peer_id);
        let kad = Kademlia::new(peer_id, kad_store);
        let ping = Ping::default();
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
            bootstrap_complete: false,
            mdns,
            kad,
            ping,
            identify,
            bitswap,
            events,
            mdns_peers: Default::default(),
            provider_queries: Default::default(),
            queries: Default::default(),
        })
    }

    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        let is_global = match addr.iter().next() {
            Some(Protocol::Ip4(ip)) => IpNetwork::from(ip).is_global(),
            Some(Protocol::Ip6(ip)) => IpNetwork::from(ip).is_global(),
            Some(Protocol::Dns(_)) => true,
            Some(Protocol::Dns4(_)) => true,
            Some(Protocol::Dns6(_)) => true,
            _ => false,
        };
        if self.allow_non_globals_in_dht || is_global {
            tracing::trace!("adding address {}", addr);
            self.kad.add_address(&peer_id, addr);
        } else {
            tracing::trace!("not adding local address {}", addr);
        }
    }

    pub fn remove_address(&mut self, peer_id: &PeerId, addr: &Multiaddr) {
        tracing::trace!("removing address {}", addr);
        self.kad.remove_address(peer_id, addr);
    }

    pub fn get(&mut self, cid: Cid) -> (oneshot::Receiver<Result<()>>, QueryId) {
        let (tx, rx) = oneshot::channel();
        let id = self.bitswap.get(cid, self.mdns_peers.iter().copied());
        self.queries.insert(id.into(), tx);
        (rx, id.into())
    }

    pub fn sync(
        &mut self,
        cid: Cid,
        missing: impl Iterator<Item = Cid>,
    ) -> (oneshot::Receiver<Result<()>>, QueryId) {
        let (tx, rx) = oneshot::channel();
        let id = self.bitswap.sync(cid, missing);
        self.queries.insert(id.into(), tx);
        (rx, id.into())
    }

    pub fn cancel(&mut self, id: QueryId) {
        self.queries.remove(&id);
        if let QueryId(InnerQueryId::Bitswap(id)) = id {
            self.bitswap.cancel(id);
        }
    }

    pub fn inject_missing_blocks(&mut self, id: QueryId, missing: Vec<Cid>) {
        if let QueryId(InnerQueryId::Bitswap(id)) = id {
            self.bitswap.inject_missing_blocks(id, missing);
        }
    }

    pub fn inject_have(&mut self, ch: Channel, have: bool) {
        self.bitswap.inject_have(ch, have);
    }

    pub fn inject_block(&mut self, ch: Channel, block: Option<Vec<u8>>) {
        self.bitswap.inject_block(ch, block);
    }

    pub fn bootstrap(&mut self) -> oneshot::Receiver<Result<()>> {
        let (tx, rx) = oneshot::channel();
        match self.kad.bootstrap() {
            Ok(id) => {
                self.queries.insert(id.into(), tx);
            }
            Err(err) => {
                tx.send(Err(err.into())).ok();
            }
        }
        rx
    }

    pub fn provide(&mut self, cid: Cid) -> oneshot::Receiver<Result<()>> {
        let (tx, rx) = oneshot::channel();
        if self.bootstrap_complete {
            let key = Key::new(&cid.to_bytes());
            match self.kad.start_providing(key) {
                Ok(id) => {
                    self.queries.insert(id.into(), tx);
                }
                Err(err) => {
                    tx.send(Err(KadStoreError(err).into())).ok();
                }
            }
        } else {
            tx.send(Err(NotBootstrapped.into())).ok();
        }
        rx
    }

    pub fn unprovide(&mut self, cid: Cid) {
        let key = Key::new(&cid.to_bytes());
        self.kad.stop_providing(&key);
    }

    pub fn register_metrics(&self, registry: &Registry) -> Result<()> {
        self.bitswap.register_metrics(registry)?;
        Ok(())
    }
}
