use anyhow::Result;
use chrono::{DateTime, SecondsFormat::Millis, Utc};
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
    /// an address observed earlier for ourselves has been retired since it was not refreshed
    ExpiredExternalAddr(Multiaddr),
    /// an address was added for the given peer, following a successful dailling attempt
    Discovered(PeerId),
    /// a dialling attempt for the given peer has failed
    DialFailure(PeerId, Multiaddr, String),
    /// a peer could not be reached by any known address
    ///
    /// if `prune_addresses == true` then it has been removed from the address book
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerInfo {
    protocol_version: Option<String>,
    agent_version: Option<String>,
    protocols: Vec<String>,
    listeners: Vec<Multiaddr>,
    addresses: FnvHashMap<Multiaddr, (AddressSource, DateTime<Utc>)>,
    connections: FnvHashMap<Multiaddr, DateTime<Utc>>,
    failures: VecDeque<ConnectionFailure>,
    rtt: Option<Rtt>,
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

    pub fn listen_addresses(&self) -> impl Iterator<Item = &Multiaddr> {
        self.listeners.iter()
    }

    pub fn addresses(
        &self,
    ) -> impl Iterator<Item = (&Multiaddr, AddressSource, DateTime<Utc>)> + '_ {
        self.addresses
            .iter()
            .map(|(addr, (source, dt))| (addr, *source, *dt))
    }

    pub fn connections(&self) -> impl Iterator<Item = (&Multiaddr, DateTime<Utc>)> {
        self.connections.iter().map(|(a, dt)| (a, *dt))
    }

    pub fn rtt(&self) -> Option<Duration> {
        self.rtt.map(|x| x.current)
    }

    pub fn full_rtt(&self) -> Option<Rtt> {
        self.rtt
    }

    pub fn recent_failures(&self) -> impl Iterator<Item = &ConnectionFailure> {
        self.failures.iter()
    }

    fn push_failure(&mut self, f: ConnectionFailure) {
        if self.failures.len() > 9 {
            self.failures.pop_back();
        }
        if self
            .addresses
            .get(f.addr())
            .iter()
            .any(|(s, _dt)| s.is_to_probe())
        {
            self.addresses.remove(f.addr());
        }
        self.failures.push_front(f);
    }

    pub fn confirmed_addresses(&self) -> impl Iterator<Item = &Multiaddr> {
        self.addresses
            .iter()
            .filter(|x| x.1 .0.is_confirmed())
            .map(|x| x.0)
    }

    pub fn addresses_to_probe(&self) -> impl Iterator<Item = &Multiaddr> {
        self.addresses
            .iter()
            .filter(|x| x.1 .0.is_to_probe())
            .map(|x| x.0)
    }

    pub fn addresses_to_translate(&self) -> impl Iterator<Item = &Multiaddr> {
        self.addresses
            .iter()
            .filter(|x| x.1 .0.is_to_translate())
            .map(|x| x.0)
    }

    fn ingest_address(&mut self, addr: Multiaddr, source: AddressSource) -> Option<Multiaddr> {
        let inserted = if let Some((src, dt)) = self.addresses.get_mut(&addr) {
            if source >= *src {
                *dt = Utc::now();
                *src = source;
                true
            } else {
                false
            }
        } else {
            self.addresses.insert(addr.clone(), (source, Utc::now()));
            true
        };
        (inserted && source.is_to_probe()).then(|| {
            tracing::debug!("triggering dial to {} ({:?})", addr, source);
            addr
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Rtt {
    current: Duration,
    decay_3: Duration,
    decay_10: Duration,
    failures: u32,
}

impl Rtt {
    pub fn new(current: Duration) -> Self {
        Self {
            current,
            decay_3: current,
            decay_10: current,
            failures: 0,
        }
    }

    pub fn register(&mut self, current: Duration) {
        self.current = current;
        self.decay_3 = self.decay_3 * 7 / 10 + current * 3 / 10;
        self.decay_10 = self.decay_10 * 9 / 10 + current / 10;
        self.failures = 0;
    }

    pub fn register_failure(&mut self) {
        self.failures += 1;
    }

    /// Get a reference to the rtt's current.
    pub fn current(&self) -> Duration {
        self.current
    }

    /// Get a reference to the rtt's decay 3.
    pub fn decay_3(&self) -> Duration {
        self.decay_3
    }

    /// Get a reference to the rtt's decay 10.
    pub fn decay_10(&self) -> Duration {
        self.decay_10
    }

    /// Get a reference to the rtt's failures.
    pub fn failures(&self) -> u32 {
        self.failures
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum AddressSource {
    Incoming,
    Listen,
    Kad,
    Mdns,
    User,
    Dial,
}

impl AddressSource {
    pub fn is_confirmed(&self) -> bool {
        matches!(self, AddressSource::Dial | AddressSource::User)
    }
    pub fn is_to_probe(&self) -> bool {
        matches!(
            self,
            AddressSource::Listen | AddressSource::Kad | AddressSource::Mdns
        )
    }
    pub fn is_to_translate(&self) -> bool {
        matches!(self, AddressSource::Incoming)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionFailure {
    DialError(Multiaddr, DateTime<Utc>, String),
    #[allow(unused)]
    PeerDisconnected(Multiaddr, DateTime<Utc>),
    WeDisconnected(Multiaddr, DateTime<Utc>, String),
}

impl ConnectionFailure {
    pub fn addr(&self) -> &Multiaddr {
        match self {
            ConnectionFailure::DialError(a, _, _) => a,
            ConnectionFailure::PeerDisconnected(a, _) => a,
            ConnectionFailure::WeDisconnected(a, _, _) => a,
        }
    }

    pub fn time(&self) -> DateTime<Utc> {
        match self {
            ConnectionFailure::DialError(_, t, _) => *t,
            ConnectionFailure::PeerDisconnected(_, t) => *t,
            ConnectionFailure::WeDisconnected(_, t, _) => *t,
        }
    }
}

impl std::fmt::Display for ConnectionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionFailure::DialError(a, t, e) => write!(
                f,
                "outbound dial error for {} at {}: {}",
                a,
                t.to_rfc3339_opts(Millis, true),
                e
            ),
            ConnectionFailure::PeerDisconnected(a, t) => write!(
                f,
                "{} disconnected at {}",
                a,
                t.to_rfc3339_opts(Millis, true)
            ),
            ConnectionFailure::WeDisconnected(a, t, e) => write!(
                f,
                "we disconnected from {} at {}: {}",
                a,
                t.to_rfc3339_opts(Millis, true),
                e
            ),
        }
    }
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
    enable_loopback: bool,
    prune_addresses: bool,
    local_node_name: String,
    local_peer_id: PeerId,
    local_public_key: PublicKey,
    listeners: FnvHashSet<Multiaddr>,
    peers: FnvHashMap<PeerId, PeerInfo>,
    event_stream: Vec<mpsc::UnboundedSender<Event>>,
    actions: VecDeque<NetworkBehaviourAction<void::Void, void::Void>>,
}

impl AddressBook {
    pub fn new(
        local_peer_id: PeerId,
        local_node_name: String,
        local_public_key: PublicKey,
        enable_loopback: bool,
        prune_addresses: bool,
    ) -> Self {
        Self {
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
            self.actions
                .push_back(NetworkBehaviourAction::DialAddress { address })
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

    pub fn connections(&self) -> impl Iterator<Item = (&PeerId, &Multiaddr, &DateTime<Utc>)> + '_ {
        self.peers
            .iter()
            .flat_map(|(peer, info)| info.connections.iter().map(move |(a, t)| (peer, a, t)))
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
            if let Some(duration) = rtt {
                if let Some(ref mut rtt) = info.rtt {
                    rtt.register(duration);
                } else {
                    info.rtt = Some(Rtt::new(duration));
                }
            } else if let Some(ref mut rtt) = info.rtt {
                rtt.register_failure();
            }
            self.notify(Event::NewInfo(*peer_id))
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
                .filter_map(ip_port)
                .collect::<FnvHashMap<_, _>>();

            let common_port = listen_port.values().copied().collect::<FnvHashSet<_>>();
            let common_port = if common_port.len() == 1 {
                common_port.into_iter().next()
            } else {
                None
            };

            let mut translated = FnvHashSet::default();
            let mut confirmed = FnvHashSet::default();
            for addr in info.addresses_to_translate() {
                if let Some((ip, p)) = ip_port(addr) {
                    if let Some(lp) = listen_port.get(&ip) {
                        if *lp == p && info.addresses[addr].0 == AddressSource::Incoming {
                            confirmed.insert(addr.clone());
                        } else {
                            translated
                                .insert(addr.replace(1, |_p| Some(Protocol::Tcp(*lp))).unwrap());
                        }
                    } else if let Some(cp) = common_port {
                        translated.insert(addr.replace(1, |_p| Some(Protocol::Tcp(cp))).unwrap());
                    }
                }
            }

            info.addresses.retain(|_a, (s, _dt)| !s.is_to_translate());
            info.addresses.extend(
                confirmed
                    .into_iter()
                    .map(|addr| (addr, (AddressSource::Dial, Utc::now()))),
            );

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
                    continue;
                }
                if let Some(address) = info.ingest_address(addr, AddressSource::Listen) {
                    self.actions
                        .push_back(NetworkBehaviourAction::DialAddress { address })
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
            .insert(address, Utc::now());
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
        entry.connections.insert(new_addr, Utc::now());
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
        entry.connections.remove(&addr);
        entry.push_failure(ConnectionFailure::WeDisconnected(
            addr,
            Utc::now(),
            "unknown".to_owned(),
        ));
        self.notify(Event::ConnectionClosed(*peer_id, conn.clone()));
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
            self.peers
                .entry(*peer_id)
                .or_default()
                .push_failure(ConnectionFailure::DialError(
                    addr.clone(),
                    Utc::now(),
                    error.clone(),
                ));
            self.notify(Event::DialFailure(*peer_id, addr.clone(), error));
            if self.is_connected(peer_id) {
                return;
            }
            ADDRESS_REACH_FAILURE.inc();
            if self.prune_addresses {
                self.remove_address(peer_id, addr);
            }
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
        if self.peers.contains_key(peer_id) {
            DISCOVERED.dec();
            self.notify(Event::Unreachable(*peer_id));
            if self.prune_addresses {
                self.peers.remove(peer_id);
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate_keypair;
    use async_executor::LocalExecutor;
    use futures::{future::ready, stream::StreamExt};
    use libp2p_quic::ToLibp2p;
    use regex::Regex;
    use std::cell::RefCell;
    use Event::*;

    #[test]
    fn address_source_order() {
        use AddressSource::*;
        assert!(Dial > User);
        assert!(User > Mdns);
        assert!(Mdns > Kad);
        assert!(Kad > Listen);
        assert!(Listen > Incoming);
    }

    #[async_std::test]
    async fn test_dial_basic() {
        let mut book = AddressBook::new(
            PeerId::random(),
            "".into(),
            generate_keypair().public,
            false,
            true,
        );
        let mut stream = book.swarm_events();
        let peer_a = PeerId::random();
        let addr_1: Multiaddr = "/ip4/1.1.1.1/tcp/3333".parse().unwrap();
        let mut addr_1_2 = addr_1.clone();
        addr_1_2.push(Protocol::P2p(peer_a.into()));
        let addr_2: Multiaddr = "/ip4/2.2.2.2/tcp/3333".parse().unwrap();
        let error = std::io::Error::new(std::io::ErrorKind::Other, "my error");
        book.add_address(&peer_a, addr_1.clone(), AddressSource::Mdns);
        book.add_address(&peer_a, addr_1_2, AddressSource::User);
        book.add_address(&peer_a, addr_2.clone(), AddressSource::Incoming);
        assert_eq!(stream.next().await, Some(Event::Discovered(peer_a)));
        let peers = book.peers().collect::<Vec<_>>();
        assert_eq!(peers, vec![&peer_a]);
        book.inject_addr_reach_failure(Some(&peer_a), &addr_1, &error);
        book.inject_addr_reach_failure(Some(&peer_a), &addr_2, &error);
        book.inject_dial_failure(&peer_a);
        assert_eq!(
            stream.next().await,
            Some(Event::DialFailure(
                peer_a,
                addr_1.clone(),
                "my error".to_owned()
            ))
        );
        assert_eq!(
            stream.next().await,
            Some(Event::DialFailure(
                peer_a,
                addr_2.clone(),
                "my error".to_owned()
            ))
        );
        assert_eq!(stream.next().await, Some(Event::Unreachable(peer_a)));
        #[allow(clippy::needless_collect)]
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
            true,
        );
        let mut stream = book.swarm_events();
        let peer_a = PeerId::random();
        let addr_1: Multiaddr = "/ip4/1.1.1.1/tcp/3333".parse().unwrap();
        let addr_2: Multiaddr = "/ip4/2.2.2.2/tcp/3333".parse().unwrap();
        let error = std::io::Error::new(std::io::ErrorKind::Other, "my error");
        book.add_address(&peer_a, addr_1.clone(), AddressSource::Mdns);
        assert_eq!(stream.next().await, Some(Event::Discovered(peer_a)));
        book.add_address(&peer_a, addr_2.clone(), AddressSource::Incoming);
        book.inject_addr_reach_failure(Some(&peer_a), &addr_1, &error);
        book.inject_dial_failure(&peer_a);
        // book.poll
        let peers = book.peers().collect::<Vec<_>>();
        assert_eq!(peers, vec![&peer_a]);
        book.inject_addr_reach_failure(Some(&peer_a), &addr_2, &error);
        book.inject_dial_failure(&peer_a);
        assert_eq!(
            stream.next().await,
            Some(Event::DialFailure(
                peer_a,
                addr_1.clone(),
                "my error".to_owned()
            ))
        );
        assert_eq!(
            stream.next().await,
            Some(Event::DialFailure(
                peer_a,
                addr_2.clone(),
                "my error".to_owned()
            ))
        );
        assert_eq!(stream.next().await, Some(Event::Unreachable(peer_a)));
        #[allow(clippy::needless_collect)]
        let peers = book.peers().collect::<Vec<_>>();
        assert!(peers.is_empty());
    }

    #[test]
    fn from_docker_host() {
        let peer_a = PeerId::random();
        let addr_a_1: Multiaddr = "/ip4/10.0.0.2/tcp/4001".parse().unwrap();

        let mut book = AddressBook::new(
            peer_a,
            "name".to_owned(),
            generate_keypair().public,
            false,
            false,
        );
        let events = RefCell::new(Vec::new());
        let next = {
            let swarm = LocalExecutor::new();
            swarm
                .spawn(book.swarm_events().for_each(|e| {
                    events.borrow_mut().push(e);
                    ready(())
                }))
                .detach();
            let events = &events;
            move || {
                while swarm.try_tick() {}
                std::mem::take(&mut *events.borrow_mut())
            }
        };

        let key_b = generate_keypair().to_public();
        let peer_b = PeerId::from(&key_b);
        let addr_b_1: Multiaddr = "/ip4/10.0.0.10/tcp/57634".parse().unwrap();
        let addr_b_1p = addr_b_1.clone().with(Protocol::P2p(peer_b.into()));
        let addr_b_2: Multiaddr = "/ip4/10.0.0.10/tcp/4001".parse().unwrap();
        let addr_b_2p = addr_b_2.clone().with(Protocol::P2p(peer_b.into()));
        let addr_b_3: Multiaddr = "/ip4/172.17.0.3/tcp/4001".parse().unwrap();
        let addr_b_3p = addr_b_3.clone().with(Protocol::P2p(peer_b.into()));
        let addr_b_4: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse().unwrap();

        let id = ConnectionId::new(1);
        let cp = ConnectedPoint::Listener {
            local_addr: addr_a_1.clone(),
            send_back_addr: addr_b_1,
        };
        book.inject_connection_established(&peer_b, &id, &cp);
        assert_eq!(
            next(),
            vec![
                Discovered(peer_b),
                NewInfo(peer_b),
                ConnectionEstablished(peer_b, cp.clone())
            ]
        );
        assert_eq!(dials(&mut book), vec![]);
        assert_eq!(
            addrs(&book, peer_b),
            vec![(addr_b_1p, AddressSource::Incoming)]
        );

        let info = IdentifyInfo {
            public_key: key_b,
            protocol_version: "my protocol".to_owned(),
            agent_version: "my agent".to_owned(),
            listen_addrs: vec![addr_b_2, addr_b_3, addr_b_4],
            protocols: vec!["my proto".to_owned()],
            observed_addr: addr_a_1,
        };
        book.set_info(&peer_b, info);
        assert_eq!(next(), vec![NewInfo(peer_b)]);
        assert_eq!(dials(&mut book), vec![addr_b_2p.clone(), addr_b_3p.clone()]);
        assert_eq!(
            addrs(&book, peer_b),
            vec![
                (addr_b_2p.clone(), AddressSource::Listen),
                (addr_b_3p.clone(), AddressSource::Listen)
            ]
        );

        let id2 = ConnectionId::new(2);
        let cp2 = ConnectedPoint::Dialer {
            address: addr_b_2p.clone(),
        };
        book.inject_connection_established(&peer_b, &id2, &cp2);
        assert_eq!(
            next(),
            vec![NewInfo(peer_b), ConnectionEstablished(peer_b, cp2)]
        );
        assert_eq!(dials(&mut book), vec![]);
        assert_eq!(
            addrs(&book, peer_b),
            vec![
                (addr_b_2p.clone(), AddressSource::Dial),
                (addr_b_3p.clone(), AddressSource::Listen)
            ]
        );

        let error = anyhow::anyhow!("didn’t work, mate!");
        book.inject_addr_reach_failure(Some(&peer_b), &addr_b_3p, error.as_ref());
        assert_eq!(
            next(),
            vec![DialFailure(
                peer_b,
                addr_b_3p,
                "didn’t work, mate!".to_owned()
            )]
        );
        assert_eq!(dials(&mut book), vec![]);
        assert_eq!(addrs(&book, peer_b), vec![(addr_b_2p, AddressSource::Dial)]);
    }

    #[test]
    fn from_docker_container() {
        let peer_a = PeerId::random();
        let addr_a_1: Multiaddr = "/ip4/10.0.0.2/tcp/4001".parse().unwrap();

        let mut book = AddressBook::new(
            peer_a,
            "name".to_owned(),
            generate_keypair().public,
            false,
            false,
        );
        let events = RefCell::new(Vec::new());
        let next = {
            let swarm = LocalExecutor::new();
            swarm
                .spawn(book.swarm_events().for_each(|e| {
                    events.borrow_mut().push(e);
                    ready(())
                }))
                .detach();
            let events = &events;
            move || {
                while swarm.try_tick() {}
                std::mem::take(&mut *events.borrow_mut())
            }
        };

        let key_b = generate_keypair().to_public();
        let peer_b = PeerId::from(&key_b);
        let addr_b_1: Multiaddr = "/ip4/10.0.0.10/tcp/57634".parse().unwrap();
        let addr_b_1p = addr_b_1.clone().with(Protocol::P2p(peer_b.into()));
        let addr_b_2: Multiaddr = "/ip4/10.0.0.10/tcp/4001".parse().unwrap();
        let addr_b_2p = addr_b_2.clone().with(Protocol::P2p(peer_b.into()));
        let addr_b_3: Multiaddr = "/ip4/172.17.0.3/tcp/4001".parse().unwrap();
        let addr_b_3p = addr_b_3.clone().with(Protocol::P2p(peer_b.into()));
        let addr_b_4: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse().unwrap();

        let id = ConnectionId::new(1);
        let cp = ConnectedPoint::Listener {
            local_addr: addr_a_1.clone(),
            send_back_addr: addr_b_1,
        };
        book.inject_connection_established(&peer_b, &id, &cp);
        assert_eq!(
            next(),
            vec![
                Discovered(peer_b),
                NewInfo(peer_b),
                ConnectionEstablished(peer_b, cp.clone())
            ]
        );
        assert_eq!(dials(&mut book), vec![]);
        assert_eq!(
            addrs(&book, peer_b),
            vec![(addr_b_1p, AddressSource::Incoming)]
        );

        let info = IdentifyInfo {
            public_key: key_b.clone(),
            protocol_version: "my protocol".to_owned(),
            agent_version: "my agent".to_owned(),
            listen_addrs: vec![addr_b_3.clone(), addr_b_4.clone()],
            protocols: vec!["my proto".to_owned()],
            observed_addr: addr_a_1.clone(),
        };
        book.set_info(&peer_b, info);
        assert_eq!(next(), vec![NewInfo(peer_b)]);
        assert_eq!(dials(&mut book), vec![addr_b_2p.clone(), addr_b_3p.clone()]);
        assert_eq!(
            addrs(&book, peer_b),
            vec![
                (addr_b_2p.clone(), AddressSource::Listen),
                (addr_b_3p.clone(), AddressSource::Listen)
            ]
        );

        // here we assume that our observeration of that peer’s address will eventually be
        // included by that peer in its Identify info

        let info = IdentifyInfo {
            public_key: key_b,
            protocol_version: "my protocol".to_owned(),
            agent_version: "my agent".to_owned(),
            listen_addrs: vec![addr_b_2, addr_b_3, addr_b_4],
            protocols: vec!["my proto".to_owned()],
            observed_addr: addr_a_1,
        };
        book.set_info(&peer_b, info);
        assert_eq!(next(), vec![NewInfo(peer_b)]);
        assert_eq!(dials(&mut book), vec![addr_b_2p.clone(), addr_b_3p.clone()]);
        assert_eq!(
            addrs(&book, peer_b),
            vec![
                (addr_b_2p.clone(), AddressSource::Listen),
                (addr_b_3p.clone(), AddressSource::Listen)
            ]
        );

        let error = anyhow::anyhow!("play it again, Sam");
        book.inject_addr_reach_failure(Some(&peer_b), &addr_b_3p, error.as_ref());
        assert_eq!(
            next(),
            vec![DialFailure(
                peer_b,
                addr_b_3p,
                "play it again, Sam".to_owned()
            )]
        );
        assert_eq!(dials(&mut book), vec![]);
        assert_eq!(
            addrs(&book, peer_b),
            vec![(addr_b_2p.clone(), AddressSource::Listen)]
        );

        let id2 = ConnectionId::new(2);
        let cp2 = ConnectedPoint::Dialer {
            address: addr_b_2p.clone(),
        };
        book.inject_connection_established(&peer_b, &id2, &cp2);
        assert_eq!(
            next(),
            vec![NewInfo(peer_b), ConnectionEstablished(peer_b, cp2)]
        );
        assert_eq!(dials(&mut book), vec![]);
        assert_eq!(
            addrs(&book, peer_b),
            vec![(addr_b_2p.clone(), AddressSource::Dial)]
        );

        book.inject_addr_reach_failure(None, &addr_b_2p, error.as_ref());
        assert_eq!(
            next(),
            vec![DialFailure(
                peer_b,
                addr_b_2p.clone(),
                "play it again, Sam".to_owned()
            )]
        );
        assert_eq!(dials(&mut book), vec![]);
        assert_eq!(addrs(&book, peer_b), vec![(addr_b_2p, AddressSource::Dial)]);

        let dates = Regex::new(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z").unwrap();
        let addrs =
            Regex::new(r"/ip./(?P<a>[0-9a-fA-F.:]+)/tcp/(?P<p>\d+)/p2p/[0-9a-zA-Z]+").unwrap();
        assert_eq!(
            book.info(&peer_b)
                .unwrap()
                .recent_failures()
                .map(|f| addrs
                    .replace_all(dates.replace_all(&*f.to_string(), "XXX").as_ref(), "$a:$p")
                    .into_owned())
                .collect::<Vec<_>>(),
            vec![
                "outbound dial error for 10.0.0.10:4001 at XXX: play it again, Sam",
                "outbound dial error for 172.17.0.3:4001 at XXX: play it again, Sam"
            ]
        );
    }

    fn addrs(book: &AddressBook, peer_id: PeerId) -> Vec<(Multiaddr, AddressSource)> {
        let mut v = book
            .info(&peer_id)
            .unwrap()
            .addresses()
            .map(|(a, s, _dt)| (a.clone(), s))
            .collect::<Vec<_>>();
        v.sort();
        v
    }

    fn dials(book: &mut AddressBook) -> Vec<Multiaddr> {
        let mut v = book
            .actions
            .drain(..)
            .filter_map(|a| match a {
                NetworkBehaviourAction::DialAddress { address } => Some(address),
                _ => None,
            })
            .collect::<Vec<_>>();
        v.sort();
        v
    }
}
