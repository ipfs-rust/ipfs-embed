use anyhow::Result;
use fnv::FnvHashMap;
use futures::channel::mpsc;
use futures::stream::Stream;
use lazy_static::lazy_static;
use libp2p::core::connection::{ConnectedPoint, ConnectionId, ListenerId};
use libp2p::identify::IdentifyInfo;
use libp2p::multiaddr::Protocol;
use libp2p::swarm::protocols_handler::DummyProtocolsHandler;
use libp2p::swarm::{DialPeerCondition, NetworkBehaviour, NetworkBehaviourAction, PollParameters};
use libp2p::{Multiaddr, PeerId};
use libp2p_blake_streams::Head;
use libp2p_quic::PublicKey;
use prometheus::{IntCounter, IntGauge, Registry};
use std::borrow::Cow;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    NewListener(ListenerId),
    NewListenAddr(ListenerId, Multiaddr),
    ExpiredListenAddr(ListenerId, Multiaddr),
    ListenerClosed(ListenerId),
    NewExternalAddr(Multiaddr),
    ExpiredExternalAddr(Multiaddr),
    Discovered(PeerId),
    Unreachable(PeerId),
    Connected(PeerId),
    Disconnected(PeerId),
    Subscribed(PeerId, String),
    Unsubscribed(PeerId, String),
    NewHead(Head),
    Bootstrapped,
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
    pub static ref LISTENERS: IntGauge =
        IntGauge::new("peers_listeners", "Number of listeners.").unwrap();
    pub static ref LISTEN_ADDRS: IntGauge =
        IntGauge::new("peers_listen_addrs", "Number of listen addrs.",).unwrap();
    pub static ref EXTERNAL_ADDRS: IntGauge =
        IntGauge::new("peers_external_addrs", "Number of external addresses.",).unwrap();
    pub static ref DISCOVERED: IntGauge =
        IntGauge::new("peers_discovered", "Number of discovered peers.").unwrap();
    pub static ref CONNECTED: IntGauge =
        IntGauge::new("peers_connected", "Number of connected peers.").unwrap();
    pub static ref CONNECTIONS: IntGauge =
        IntGauge::new("peers_connections", "Number of connections.").unwrap();
    pub static ref LISTENER_ERROR: IntCounter = IntCounter::new(
        "peers_listener_error",
        "Number of non fatal listener errors."
    )
    .unwrap();
    pub static ref ADDRESS_REACH_FAILURE: IntCounter = IntCounter::new(
        "peers_address_reach_failure",
        "Number of address reach failures."
    )
    .unwrap();
    pub static ref DIAL_FAILURE: IntCounter =
        IntCounter::new("peers_dial_failure", "Number of dial failures.").unwrap();
}

#[inline]
pub(crate) fn normalize_addr(addr: &mut Multiaddr, peer: &PeerId) {
    if let Some(Protocol::P2p(_)) = addr.iter().last() {
    } else {
        addr.push(Protocol::P2p((*peer).into()));
    }
}

#[inline]
fn normalize_addr_ref<'a>(addr: &'a Multiaddr, peer: &PeerId) -> Cow<'a, Multiaddr> {
    if let Some(Protocol::P2p(_)) = addr.iter().last() {
        Cow::Borrowed(addr)
    } else {
        let mut addr = addr.clone();
        addr.push(Protocol::P2p((*peer).into()));
        Cow::Owned(addr)
    }
}

trait MultiaddrExt {
    fn is_loopback(&self) -> bool;
}

impl MultiaddrExt for Multiaddr {
    fn is_loopback(&self) -> bool {
        if let Some(Protocol::Ip4(addr)) = self.iter().next() {
            if !addr.is_loopback() {
                return false;
            }
        }
        true
    }
}

#[derive(Debug)]
pub struct AddressBook {
    enable_loopback: bool,
    local_node_name: String,
    local_peer_id: PeerId,
    local_public_key: PublicKey,
    peers: FnvHashMap<PeerId, PeerInfo>,
    connections: FnvHashMap<PeerId, Multiaddr>,
    event_stream: Vec<mpsc::UnboundedSender<Event>>,
    actions: VecDeque<NetworkBehaviourAction<void::Void, void::Void>>,
}

impl AddressBook {
    pub fn new(
        local_peer_id: PeerId,
        local_node_name: String,
        local_public_key: PublicKey,
        enable_loopback: bool,
    ) -> Self {
        Self {
            enable_loopback,
            local_node_name,
            local_peer_id,
            local_public_key,
            peers: Default::default(),
            connections: Default::default(),
            event_stream: Default::default(),
            actions: Default::default(),
        }
    }

    pub fn local_public_key(&self) -> &PublicKey {
        &self.local_public_key
    }

    pub fn local_node_name(&self) -> &str {
        &self.local_node_name
    }

    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    pub fn dial(&mut self, peer: &PeerId) {
        if peer == self.local_peer_id() {
            tracing::error!("attempting to dial self");
            return;
        }
        tracing::trace!("dialing {}", peer);
        self.actions.push_back(NetworkBehaviourAction::DialPeer {
            peer_id: *peer,
            condition: DialPeerCondition::Disconnected,
        });
    }

    pub fn add_address(&mut self, peer: &PeerId, mut address: Multiaddr, source: AddressSource) {
        if peer == self.local_peer_id() {
            return;
        }
        if !self.enable_loopback && address.is_loopback() {
            return;
        }
        let discovered = !self.peers.contains_key(peer);
        let info = self.peers.entry(*peer).or_default();
        normalize_addr(&mut address, peer);
        #[allow(clippy::map_entry)]
        if !info.addresses.contains_key(&address) {
            tracing::trace!("adding address {} from {:?}", address, source);
            info.addresses.insert(address, source);
        }
        if discovered {
            self.notify(Event::Discovered(*peer));
        }
    }

    pub fn remove_address(&mut self, peer: &PeerId, address: &Multiaddr) {
        let address = normalize_addr_ref(address, peer);
        if let Some(info) = self.peers.get_mut(peer) {
            tracing::trace!("removing address {}", address);
            info.addresses.remove(&address);
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

    pub fn swarm_events(&mut self) -> SwarmEvents {
        let (tx, rx) = mpsc::unbounded();
        self.event_stream.push(tx);
        SwarmEvents(rx)
    }

    pub fn notify(&mut self, event: Event) {
        tracing::trace!("{:?}", event);
        self.event_stream
            .retain(|tx| tx.unbounded_send(event.clone()).is_ok());
    }

    pub fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(LISTENERS.clone()))?;
        registry.register(Box::new(LISTEN_ADDRS.clone()))?;
        registry.register(Box::new(EXTERNAL_ADDRS.clone()))?;
        registry.register(Box::new(DISCOVERED.clone()))?;
        registry.register(Box::new(CONNECTED.clone()))?;
        registry.register(Box::new(CONNECTIONS.clone()))?;
        registry.register(Box::new(LISTENER_ERROR.clone()))?;
        registry.register(Box::new(ADDRESS_REACH_FAILURE.clone()))?;
        registry.register(Box::new(DIAL_FAILURE.clone()))?;
        Ok(())
    }
}

pub struct SwarmEvents(mpsc::UnboundedReceiver<Event>);

impl Stream for SwarmEvents {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
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

    fn inject_event(&mut self, _peer_id: PeerId, _connection: ConnectionId, _event: void::Void) {}

    fn poll(
        &mut self,
        _cx: &mut Context,
        _params: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<void::Void, void::Void>> {
        if let Some(action) = self.actions.pop_front() {
            Poll::Ready(action)
        } else {
            Poll::Pending
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

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        _: &ConnectionId,
        conn: &ConnectedPoint,
    ) {
        let mut address = conn.get_remote_address().clone();
        normalize_addr(&mut address, peer_id);
        self.add_address(peer_id, address.clone(), AddressSource::Peer);
        self.connections.insert(*peer_id, address);
    }

    fn inject_address_change(
        &mut self,
        peer_id: &PeerId,
        _: &ConnectionId,
        _old: &ConnectedPoint,
        new: &ConnectedPoint,
    ) {
        let mut new = new.get_remote_address().clone();
        normalize_addr(&mut new, peer_id);
        self.add_address(peer_id, new.clone(), AddressSource::Peer);
        self.connections.insert(*peer_id, new);
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
            if self.is_connected(peer_id) {
                return;
            }
            tracing::trace!("address reach failure {}", error);
            ADDRESS_REACH_FAILURE.inc();
            self.remove_address(peer_id, addr);
        }
    }

    fn inject_dial_failure(&mut self, peer_id: &PeerId) {
        // If an address was added after the peer was dialed retry dialing the
        // peer.
        if let Some(peer) = self.peers.get(peer_id) {
            if !peer.addresses.is_empty() {
                tracing::trace!("redialing with new addresses");
                self.dial(peer_id);
                return;
            }
        }
        tracing::trace!("dial failure {}", peer_id);
        DIAL_FAILURE.inc();
        if self.peers.remove(peer_id).is_some() {
            DISCOVERED.dec();
            self.notify(Event::Unreachable(*peer_id));
        }
    }

    fn inject_new_listener(&mut self, id: ListenerId) {
        tracing::trace!("listener {:?}: created", id);
        LISTENERS.inc();
        self.notify(Event::NewListener(id));
    }

    fn inject_new_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        tracing::trace!("listener {:?}: new listen addr {}", id, addr);
        LISTEN_ADDRS.inc();
        self.notify(Event::NewListenAddr(id, addr.clone()));
    }

    fn inject_expired_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        tracing::trace!("listener {:?}: expired listen addr {}", id, addr);
        LISTEN_ADDRS.dec();
        self.notify(Event::ExpiredListenAddr(id, addr.clone()));
    }

    fn inject_listener_error(&mut self, id: ListenerId, err: &(dyn std::error::Error + 'static)) {
        tracing::trace!("listener {:?}: listener error {}", id, err);
        LISTENER_ERROR.inc();
    }

    fn inject_listener_closed(&mut self, id: ListenerId, reason: Result<(), &std::io::Error>) {
        tracing::trace!("listener {:?}: closed for reason {:?}", id, reason);
        LISTENERS.dec();
        self.notify(Event::ListenerClosed(id));
    }

    fn inject_new_external_addr(&mut self, addr: &Multiaddr) {
        tracing::trace!("new external addr {}", addr);
        EXTERNAL_ADDRS.inc();
        self.notify(Event::NewExternalAddr(addr.clone()));
    }

    fn inject_expired_external_addr(&mut self, addr: &Multiaddr) {
        tracing::trace!("expired external addr {}", addr);
        EXTERNAL_ADDRS.dec();
        self.notify(Event::ExpiredExternalAddr(addr.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate_keypair;
    use futures::stream::StreamExt;

    #[async_std::test]
    async fn test_dial_basic() {
        let mut book = AddressBook::new(
            PeerId::random(),
            "".into(),
            generate_keypair().public,
            false,
        );
        let mut stream = book.swarm_events();
        let peer_a = PeerId::random();
        let addr_1: Multiaddr = "/ip4/1.1.1.1/tcp/3333".parse().unwrap();
        let mut addr_1_2 = addr_1.clone();
        addr_1_2.push(Protocol::P2p(peer_a.into()));
        let addr_2: Multiaddr = "/ip4/2.2.2.2/tcp/3333".parse().unwrap();
        let error = std::io::Error::new(std::io::ErrorKind::Other, "");
        book.add_address(&peer_a, addr_1.clone(), AddressSource::Mdns);
        book.add_address(&peer_a, addr_1_2, AddressSource::User);
        book.add_address(&peer_a, addr_2.clone(), AddressSource::Peer);
        assert_eq!(stream.next().await, Some(Event::Discovered(peer_a)));
        let peers = book.peers().collect::<Vec<_>>();
        assert_eq!(peers, vec![&peer_a]);
        book.inject_addr_reach_failure(Some(&peer_a), &addr_1, &error);
        book.inject_addr_reach_failure(Some(&peer_a), &addr_2, &error);
        book.inject_dial_failure(&peer_a);
        assert_eq!(stream.next().await, Some(Event::Unreachable(peer_a)));
        let peers = book.peers().collect::<Vec<_>>();
        assert!(peers.is_empty());
    }

    #[async_std::test]
    async fn test_dial_with_added_addrs() {
        let mut book = AddressBook::new(
            PeerId::random(),
            "".into(),
            generate_keypair().public,
            false,
        );
        let mut stream = book.swarm_events();
        let peer_a = PeerId::random();
        let addr_1: Multiaddr = "/ip4/1.1.1.1/tcp/3333".parse().unwrap();
        let addr_2: Multiaddr = "/ip4/2.2.2.2/tcp/3333".parse().unwrap();
        let error = std::io::Error::new(std::io::ErrorKind::Other, "");
        book.add_address(&peer_a, addr_1.clone(), AddressSource::Mdns);
        assert_eq!(stream.next().await, Some(Event::Discovered(peer_a)));
        book.add_address(&peer_a, addr_2.clone(), AddressSource::Peer);
        book.inject_addr_reach_failure(Some(&peer_a), &addr_1, &error);
        book.inject_dial_failure(&peer_a);
        // book.poll
        let peers = book.peers().collect::<Vec<_>>();
        assert_eq!(peers, vec![&peer_a]);
        book.inject_addr_reach_failure(Some(&peer_a), &addr_2, &error);
        book.inject_dial_failure(&peer_a);
        assert_eq!(stream.next().await, Some(Event::Unreachable(peer_a)));
        let peers = book.peers().collect::<Vec<_>>();
        assert!(peers.is_empty());
    }
}
