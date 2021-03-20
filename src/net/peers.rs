use anyhow::Result;
use fnv::FnvHashMap;
use futures::channel::mpsc;
use futures::stream::Stream;
use lazy_static::lazy_static;
use libp2p::core::connection::{ConnectedPoint, ConnectionId, ListenerId};
use libp2p::identify::IdentifyInfo;
use libp2p::swarm::protocols_handler::DummyProtocolsHandler;
use libp2p::swarm::{NetworkBehaviour, NetworkBehaviourAction, PollParameters};
use libp2p::{Multiaddr, PeerId};
use prometheus::{IntCounter, IntGauge, Registry};
use std::task::{Context, Poll};
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    NewListenAddr(Multiaddr),
    ExpiredListenAddr(Multiaddr),
    NewExternalAddr(Multiaddr),
    Discovered(PeerId),
    Connected(PeerId),
    Disconnected(PeerId),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerInfo {
    protocol_version: Option<String>,
    agent_version: Option<String>,
    protocols: Vec<String>,
    addresses: FnvHashMap<Multiaddr, AddressSource>,
    rtt: Option<Duration>,
}

impl PeerInfo {
    pub fn protocol_version(&self) -> Option<&str> {
        self.protocol_version.as_deref()
    }

    pub fn agent_version(&self) -> Option<&str> {
        self.agent_version.as_deref()
    }

    pub fn protocols(&self) -> impl Iterator<Item = &str> + '_ {
        self.protocols.iter().map(|s| &**s)
    }

    pub fn addresses(&self) -> impl Iterator<Item = (&Multiaddr, AddressSource)> + '_ {
        self.addresses.iter().map(|(addr, source)| (addr, *source))
    }

    pub fn rtt(&self) -> Option<Duration> {
        self.rtt
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddressSource {
    Mdns,
    Kad,
    Peer,
    User,
}

lazy_static! {
    pub static ref CONNECTED: IntGauge =
        IntGauge::new("peers_connected", "Number of connected peers.").unwrap();
    pub static ref DISCOVERED: IntCounter =
        IntCounter::new("peers_discovered_total", "Number of discovered peers.").unwrap();
    pub static ref LISTENERS: IntGauge =
        IntGauge::new("peers_listeners", "Number of listeners.").unwrap();
    pub static ref EXTERNAL_ADDRS: IntCounter = IntCounter::new(
        "peers_external_addrs_total",
        "Number of external addresses.",
    )
    .unwrap();
}

#[derive(Debug)]
pub struct AddressBook {
    local_peer_id: PeerId,
    peers: FnvHashMap<PeerId, PeerInfo>,
    connections: FnvHashMap<PeerId, Multiaddr>,
    event_stream: Vec<mpsc::UnboundedSender<Event>>,
}

impl AddressBook {
    pub fn new(local_peer_id: PeerId) -> Self {
        Self {
            local_peer_id,
            peers: Default::default(),
            connections: Default::default(),
            event_stream: Default::default(),
        }
    }

    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    pub fn add_address(&mut self, peer: &PeerId, address: Multiaddr, source: AddressSource) {
        if peer == self.local_peer_id() {
            return;
        }
        tracing::trace!(
            "adding address {} for peer {} from {:?}",
            address,
            peer,
            source
        );
        let info = self.peers.entry(*peer).or_default();
        info.addresses.insert(address, source);
        if !self.is_connected(peer) {
            self.notify(Event::Discovered(*peer));
        }
    }

    pub fn remove_address(&mut self, peer: &PeerId, address: &Multiaddr) {
        if let Some(info) = self.peers.get_mut(peer) {
            tracing::trace!("removing address {} for peer {}", address, peer);
            info.addresses.remove(address);
        }
    }

    pub fn peers(&self) -> impl Iterator<Item = &PeerId> + '_ {
        self.peers.keys()
    }

    pub fn connections(&self) -> impl Iterator<Item = (&PeerId, &Multiaddr)> + '_ {
        self.connections.iter().map(|(peer, addr)| (peer, addr))
    }

    pub fn is_connected(&self, peer: &PeerId) -> bool {
        self.connections.contains_key(peer) || peer == self.local_peer_id()
    }

    pub fn info(&self, peer_id: &PeerId) -> Option<&PeerInfo> {
        self.peers.get(peer_id)
    }

    pub fn set_rtt(&mut self, peer_id: &PeerId, rtt: Option<Duration>) {
        if let Some(info) = self.peers.get_mut(peer_id) {
            info.rtt = rtt;
        }
    }

    pub fn set_info(&mut self, peer_id: &PeerId, identify: IdentifyInfo) {
        if let Some(info) = self.peers.get_mut(peer_id) {
            info.protocol_version = Some(identify.protocol_version);
            info.agent_version = Some(identify.agent_version);
            info.protocols = identify.protocols;
        }
    }

    pub fn event_stream(&mut self) -> impl Stream<Item = Event> {
        let (tx, rx) = mpsc::unbounded();
        self.event_stream.push(tx);
        rx
    }

    pub fn notify(&mut self, event: Event) {
        self.event_stream
            .retain(|tx| tx.unbounded_send(event.clone()).is_ok());
    }

    pub fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(CONNECTED.clone()))?;
        registry.register(Box::new(DISCOVERED.clone()))?;
        registry.register(Box::new(LISTENERS.clone()))?;
        registry.register(Box::new(EXTERNAL_ADDRS.clone()))?;
        Ok(())
    }
}

impl NetworkBehaviour for AddressBook {
    type ProtocolsHandler = DummyProtocolsHandler;
    type OutEvent = void::Void;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        Default::default()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        if let Some(info) = self.peers.get(peer_id) {
            info.addresses().map(|(addr, _)| addr.clone()).collect()
        } else {
            vec![]
        }
    }

    fn inject_connected(&mut self, peer_id: &PeerId) {
        tracing::trace!("connected to {}", peer_id);
        CONNECTED.inc();
        self.notify(Event::Connected(*peer_id));
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId) {
        tracing::trace!("disconnected from {}", peer_id);
        CONNECTED.dec();
        self.notify(Event::Disconnected(*peer_id));
    }

    fn inject_event(&mut self, _peer_id: PeerId, _connection: ConnectionId, _event: void::Void) {}

    fn poll(
        &mut self,
        _cx: &mut Context,
        _params: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<void::Void, void::Void>> {
        Poll::Pending
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        _: &ConnectionId,
        conn: &ConnectedPoint,
    ) {
        self.add_address(
            peer_id,
            conn.get_remote_address().clone(),
            AddressSource::Peer,
        );
        self.connections
            .insert(*peer_id, conn.get_remote_address().clone());
    }

    fn inject_address_change(
        &mut self,
        peer_id: &PeerId,
        _: &ConnectionId,
        _old: &ConnectedPoint,
        new: &ConnectedPoint,
    ) {
        self.add_address(
            peer_id,
            new.get_remote_address().clone(),
            AddressSource::Peer,
        );
        self.connections
            .insert(*peer_id, new.get_remote_address().clone());
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        _: &ConnectionId,
        _conn: &ConnectedPoint,
    ) {
        self.connections.remove(&peer_id);
    }

    fn inject_addr_reach_failure(
        &mut self,
        peer_id: Option<&PeerId>,
        addr: &Multiaddr,
        error: &dyn std::error::Error,
    ) {
        if let Some(peer_id) = peer_id {
            tracing::trace!("inject_addr_reach_failure {}", error);
            self.remove_address(peer_id, addr);
        }
    }

    fn inject_dial_failure(&mut self, peer_id: &PeerId) {
        tracing::trace!("dial failure {}", peer_id);
        self.peers.remove(peer_id);
    }

    fn inject_new_listen_addr(&mut self, addr: &Multiaddr) {
        tracing::trace!("new listen addr {}", addr);
        LISTENERS.inc();
        self.notify(Event::NewListenAddr(addr.clone()));
    }

    fn inject_expired_listen_addr(&mut self, addr: &Multiaddr) {
        tracing::trace!("expired listen addr {}", addr);
        LISTENERS.dec();
        self.notify(Event::ExpiredListenAddr(addr.clone()));
    }

    fn inject_listener_error(&mut self, id: ListenerId, err: &(dyn std::error::Error + 'static)) {
        tracing::trace!("listener {:?} had an error {}", id, err);
    }

    fn inject_listener_closed(&mut self, id: ListenerId, reason: Result<(), &std::io::Error>) {
        tracing::trace!("listener {:?} closed for reason {:?}", id, reason);
    }

    fn inject_new_external_addr(&mut self, addr: &Multiaddr) {
        tracing::trace!("new external addr {}", addr);
        EXTERNAL_ADDRS.inc();
        self.notify(Event::NewExternalAddr(addr.clone()));
    }
}
