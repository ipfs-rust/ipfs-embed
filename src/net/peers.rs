use super::peer_info::{AddressSource, Direction, PeerInfo};
use crate::net::peer_info::ConnectionFailure;
use anyhow::Result;
use chrono::{DateTime, Utc};
use fnv::{FnvHashMap, FnvHashSet};
use futures::{channel::mpsc, stream::Stream};
use lazy_static::lazy_static;
use libp2p::{
    core::connection::{ConnectedPoint, ConnectionId, ListenerId},
    identify::IdentifyInfo,
    multiaddr::Protocol,
    swarm::{
        protocols_handler::DummyProtocolsHandler, DialPeerCondition, NetworkBehaviour,
        NetworkBehaviourAction, PollParameters,
    },
    Multiaddr, PeerId,
};
use libp2p_blake_streams::Head;
use libp2p_quic::PublicKey;
use prometheus::{IntCounter, IntGauge, Registry};
use std::{
    borrow::Cow,
    collections::VecDeque,
    convert::TryInto,
    net::IpAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    /// a new listener has been created
    NewListener(ListenerId),
    /// the given listener started listening on this address
    NewListenAddr(ListenerId, Multiaddr),
    /// the given listener stopped listening on this address
    ExpiredListenAddr(ListenerId, Multiaddr),
    /// the given listener experienced an error
    ListenerError(ListenerId, String),
    /// the given listener was closed
    ListenerClosed(ListenerId),
    /// we received an observed address for ourselves from a peer
    NewExternalAddr(Multiaddr),
    /// an address observed earlier for ourselves has been retired since it was
    /// not refreshed
    ExpiredExternalAddr(Multiaddr),
    /// an address was added for the given peer, following a successful dailling
    /// attempt
    Discovered(PeerId),
    /// a dialling attempt for the given peer has failed
    DialFailure(PeerId, Multiaddr, String),
    /// a peer could not be reached by any known address
    ///
    /// if `prune_addresses == true` then it has been removed from the address
    /// book
    Unreachable(PeerId),
    /// a new connection has been opened to the given peer
    ConnectionEstablished(PeerId, ConnectedPoint),
    /// a connection to the given peer has been closed
    // FIXME add termination reason
    ConnectionClosed(PeerId, ConnectedPoint),
    /// the given peer signaled that its address has changed
    AddressChanged(PeerId, ConnectedPoint, ConnectedPoint),
    /// we are now connected to the given peer
    Connected(PeerId),
    /// the last connection to the given peer has been closed
    Disconnected(PeerId),
    /// the given peer subscribed to the given gossipsub or broadcast topic
    Subscribed(PeerId, String),
    /// the given peer unsubscribed from the given gossipsub or broadcast topic
    Unsubscribed(PeerId, String),
    NewHead(Head),
    Bootstrapped,
    /// the peer-info for the given peer has been updated with new information
    NewInfo(PeerId),
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
    fn peer_id(&self) -> Option<PeerId>;
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
    fn peer_id(&self) -> Option<PeerId> {
        match self.iter().last() {
            Some(Protocol::P2p(p)) => p.try_into().ok(),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct AddressBook {
    port_reuse: bool,
    enable_loopback: bool,
    prune_addresses: bool,
    local_node_name: String,
    local_peer_id: PeerId,
    local_public_key: PublicKey,
    listeners: FnvHashSet<Multiaddr>,
    peers: FnvHashMap<PeerId, PeerInfo>,
    event_stream: Vec<mpsc::UnboundedSender<Event>>,
    pub(crate) actions: VecDeque<NetworkBehaviourAction<void::Void, void::Void>>,
}

impl AddressBook {
    pub fn new(
        local_peer_id: PeerId,
        local_node_name: String,
        local_public_key: PublicKey,
        port_reuse: bool,
        enable_loopback: bool,
        prune_addresses: bool,
    ) -> Self {
        Self {
            port_reuse,
            enable_loopback,
            prune_addresses,
            local_node_name,
            local_peer_id,
            local_public_key,
            listeners: Default::default(),
            peers: Default::default(),
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
        tracing::debug!("request dialing {}", peer);
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
        tracing::trace!(peer = %peer, "adding address {} from {:?}", address, source);
        if let Some(address) = info.ingest_address(address, source) {
            let mut tcp = address.clone();
            tcp.pop();
            if !self.listeners.contains(&tcp) {
                self.actions
                    .push_back(NetworkBehaviourAction::DialAddress { address })
            }
        }
        if discovered {
            self.notify(Event::Discovered(*peer));
        }
        self.notify(Event::NewInfo(*peer));
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

    pub fn connections(
        &self,
    ) -> impl Iterator<Item = (PeerId, &Multiaddr, DateTime<Utc>, Direction)> {
        self.peers.iter().flat_map(|(peer, info)| {
            info.connections
                .iter()
                .map(move |(a, t)| (*peer, a, t.0, t.1))
        })
    }

    pub fn is_connected(&self, peer: &PeerId) -> bool {
        self.peers
            .get(peer)
            .map(|info| !info.connections.is_empty())
            .unwrap_or(false)
            || peer == self.local_peer_id()
    }

    pub fn info(&self, peer_id: &PeerId) -> Option<&PeerInfo> {
        self.peers.get(peer_id)
    }

    pub fn set_rtt(&mut self, peer_id: &PeerId, rtt: Option<Duration>) {
        if let Some(info) = self.peers.get_mut(peer_id) {
            info.set_rtt(rtt);
            self.notify(Event::NewInfo(*peer_id));
        }
    }

    pub fn set_info(&mut self, peer_id: &PeerId, identify: IdentifyInfo) {
        if let Some(info) = self.peers.get_mut(peer_id) {
            info.protocol_version = Some(identify.protocol_version);
            info.agent_version = Some(identify.agent_version);
            info.protocols = identify.protocols;
            info.listeners = identify.listen_addrs;

            let listen_port = info
                .listeners
                .iter()
                .filter(|a| {
                    // discount the addresses to which we are currently connected:
                    // if they are directly reachable then the address will be found in any case,
                    // and if they are NATed addresses then they likely got there by our own
                    // observation sent via Identify
                    !info
                        .connections
                        .contains_key(normalize_addr_ref(a, peer_id).as_ref())
                })
                .filter_map(ip_port)
                .collect::<Vec<_>>();

            // collect all advertised listen ports (which includes actual listeners as well
            // as observed addresses, which may be NATed) so that we can at
            // least try to guess a reasonable port where the NAT may have a
            // hole configured
            let common_port = listen_port
                .iter()
                .map(|(_a, p)| *p)
                .collect::<FnvHashSet<_>>();

            // in the absence of port_reuse or the presence of NAT the remote port on an
            // incoming connection won’t be reachable for us, so attempt a
            // translation that is then validated by dailling the resulting
            // address
            let mut translated = FnvHashSet::default();
            for addr in info.addresses_to_translate() {
                if let Some((ip, _p)) = ip_port(addr) {
                    let mut added = false;
                    for (_a, lp) in listen_port.iter().filter(|(a, _p)| *a == ip) {
                        translated.insert(addr.replace(1, |_| Some(Protocol::Tcp(*lp))).unwrap());
                        added = true;
                    }
                    if !added {
                        for cp in &common_port {
                            translated
                                .insert(addr.replace(1, |_| Some(Protocol::Tcp(*cp))).unwrap());
                            added = true;
                        }
                    }
                    if !added {
                        // no idea for a translation, so add it for validation
                        translated.insert(addr.clone());
                    }
                }
            }

            info.addresses.retain(|_a, (s, _dt)| !s.is_to_translate());

            let loopback = self.enable_loopback;
            translated.extend(
                info.listeners
                    .iter()
                    .filter(|a| loopback || !a.is_loopback())
                    .map(|a| normalize_addr_ref(a, peer_id).into_owned()),
            );

            for addr in translated {
                let mut tcp = addr.clone();
                tcp.pop();
                if self.listeners.contains(&tcp) {
                    // diallling our own listener somehow breaks the Swarm
                    continue;
                }
                if let Some(address) = info.ingest_address(addr, AddressSource::Listen) {
                    // no point trying to dial if we’re already connected and port_reuse==true since
                    // a second connection is fundamentally impossible in this
                    // case
                    if self.port_reuse && info.connections.contains_key(&address) {
                        // this will offer the address as soon as the Swarm asks for one for this
                        // peer, leading to a dial attempt that will answer
                        // the question
                        info.ingest_address(address, AddressSource::Candidate);
                    } else {
                        self.actions
                            .push_back(NetworkBehaviourAction::DialAddress { address })
                    }
                }
            }
            self.notify(Event::NewInfo(*peer_id));
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

fn ip_port(m: &Multiaddr) -> Option<(IpAddr, u16)> {
    let mut iter = m.iter();
    let addr = match iter.next()? {
        Protocol::Ip4(ip) => IpAddr::V4(ip),
        Protocol::Ip6(ip) => IpAddr::V6(ip),
        _ => return None,
    };
    let port = match iter.next()? {
        Protocol::Tcp(p) => p,
        _ => return None,
    };
    Some((addr, port))
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
            info.confirmed_addresses().cloned().collect()
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
        tracing::debug!(
            addr = display(&address),
            out = conn.is_dialer(),
            "connection established"
        );
        let src = if conn.is_dialer() {
            AddressSource::Dial
        } else {
            AddressSource::Incoming
        };
        self.add_address(peer_id, address.clone(), src);
        self.peers
            .entry(*peer_id)
            .or_default()
            .connections
            .insert(address, (Utc::now(), Direction::from(conn)));
        self.notify(Event::ConnectionEstablished(*peer_id, conn.clone()));
    }

    fn inject_address_change(
        &mut self,
        peer_id: &PeerId,
        _: &ConnectionId,
        old: &ConnectedPoint,
        new: &ConnectedPoint,
    ) {
        let mut old_addr = old.get_remote_address().clone();
        normalize_addr(&mut old_addr, peer_id);
        let mut new_addr = new.get_remote_address().clone();
        normalize_addr(&mut new_addr, peer_id);
        tracing::debug!(
            old = display(old.get_remote_address()),
            new = display(&new_addr),
            out = new.is_dialer(),
            "address changed"
        );
        let src = if new.is_dialer() {
            AddressSource::Dial
        } else {
            AddressSource::Incoming
        };
        self.add_address(peer_id, new_addr.clone(), src);
        let entry = self.peers.entry(*peer_id).or_default();
        entry.connections.remove(&old_addr);
        entry
            .connections
            .insert(new_addr, (Utc::now(), Direction::from(new)));
        self.notify(Event::AddressChanged(*peer_id, old.clone(), new.clone()));
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        _: &ConnectionId,
        conn: &ConnectedPoint,
    ) {
        let mut addr = conn.get_remote_address().clone();
        normalize_addr(&mut addr, peer_id);
        tracing::debug!(
            addr = display(&addr),
            out = conn.is_dialer(),
            "connection closed"
        );
        let entry = self.peers.entry(*peer_id).or_default();
        let was_outbound = if let Some((_dt, dir)) = entry.connections.remove(&addr) {
            dir == Direction::Outbound
        } else {
            true
        };
        entry.push_failure(
            ConnectionFailure::WeDisconnected(addr, Utc::now(), "unknown".to_owned()),
            was_outbound,
        );
        self.notify(Event::ConnectionClosed(*peer_id, conn.clone()));
        self.notify(Event::NewInfo(*peer_id));
    }

    fn inject_addr_reach_failure(
        &mut self,
        peer_id: Option<&PeerId>,
        addr: &Multiaddr,
        error: &dyn std::error::Error,
    ) {
        let peer_id = peer_id.copied().or_else(|| addr.peer_id());
        if let Some(ref peer_id) = peer_id {
            let still_connected = self.is_connected(peer_id);
            let mut naddr = addr.clone();
            normalize_addr(&mut naddr, peer_id);
            let error = format!("{:#}", error);
            tracing::debug!(
                addr = display(&naddr),
                error = display(&error),
                still_connected = still_connected,
                "dial failure"
            );
            self.peers.entry(*peer_id).or_default().push_failure(
                ConnectionFailure::DialError(addr.clone(), Utc::now(), error.clone()),
                true,
            );
            self.notify(Event::DialFailure(*peer_id, addr.clone(), error));
            if still_connected {
                self.notify(Event::NewInfo(*peer_id));
                return;
            }
            ADDRESS_REACH_FAILURE.inc();
            if self.prune_addresses {
                self.remove_address(peer_id, addr);
            }
            self.notify(Event::NewInfo(*peer_id));
        } else {
            tracing::debug!(addr = display(addr), error = display(error), "dial failure");
        }
    }

    fn inject_dial_failure(&mut self, peer_id: &PeerId) {
        if self.prune_addresses {
            // If an address was added after the peer was dialed retry dialing the
            // peer.
            if let Some(peer) = self.peers.get(peer_id) {
                if !peer.addresses.is_empty() {
                    tracing::debug!(peer = display(peer_id), "redialing with new addresses");
                    self.dial(peer_id);
                    return;
                }
            }
        }
        tracing::trace!("dial failure {}", peer_id);
        DIAL_FAILURE.inc();
        if let Some(peer) = self.peers.get(peer_id) {
            DISCOVERED.dec();
            if self.prune_addresses && peer.connections.is_empty() {
                self.peers.remove(peer_id);
                self.notify(Event::NewInfo(*peer_id));
            }
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
        self.listeners.insert(addr.clone());
        self.notify(Event::NewListenAddr(id, addr.clone()));
    }

    fn inject_expired_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        tracing::trace!("listener {:?}: expired listen addr {}", id, addr);
        LISTEN_ADDRS.dec();
        self.listeners.remove(addr);
        self.notify(Event::ExpiredListenAddr(id, addr.clone()));
    }

    fn inject_listener_error(&mut self, id: ListenerId, err: &(dyn std::error::Error + 'static)) {
        let err = format!("{:#}", err);
        tracing::trace!("listener {:?}: listener error {}", id, err);
        LISTENER_ERROR.inc();
        self.notify(Event::ListenerError(id, err));
    }

    fn inject_listener_closed(&mut self, id: ListenerId, reason: Result<(), &std::io::Error>) {
        tracing::trace!("listener {:?}: closed for reason {:?}", id, reason);
        LISTENERS.dec();
        self.notify(Event::ListenerClosed(id));
    }

    fn inject_new_external_addr(&mut self, addr: &Multiaddr) {
        let mut addr = addr.clone();
        normalize_addr(&mut addr, self.local_peer_id());
        tracing::trace!("new external addr {}", addr);
        EXTERNAL_ADDRS.inc();
        self.notify(Event::NewExternalAddr(addr));
    }

    fn inject_expired_external_addr(&mut self, addr: &Multiaddr) {
        let mut addr = addr.clone();
        normalize_addr(&mut addr, self.local_peer_id());
        tracing::trace!("expired external addr {}", addr);
        EXTERNAL_ADDRS.dec();
        self.notify(Event::ExpiredExternalAddr(addr));
    }
}
