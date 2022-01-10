use super::{
    address_handler::IntoAddressHandler,
    behaviour::MyHandlerError,
    peer_info::{AddressSource, Direction, PeerInfo},
};
use crate::net::peer_info::ConnectionFailure;
use anyhow::Result;
use chrono::{DateTime, Utc};
use fnv::{FnvHashMap, FnvHashSet};
use futures::{
    channel::mpsc,
    future::BoxFuture,
    stream::{FuturesUnordered, Stream},
    FutureExt, StreamExt,
};
use futures_timer::Delay;
use lazy_static::lazy_static;
use libp2p::{
    core::connection::{ConnectedPoint, ConnectionError, ConnectionId, ListenerId},
    identify::IdentifyInfo,
    identity::ed25519::PublicKey,
    multiaddr::Protocol,
    swarm::{
        dial_opts::{DialOpts, PeerCondition},
        protocols_handler::NodeHandlerWrapperError,
        DialError, NetworkBehaviour, NetworkBehaviourAction, PollParameters,
    },
    Multiaddr, PeerId,
};
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
    local_node_name: String,
    local_peer_id: PeerId,
    local_public_key: PublicKey,
    listeners: FnvHashSet<Multiaddr>,
    peers: FnvHashMap<PeerId, PeerInfo>,
    event_stream: Vec<mpsc::UnboundedSender<Event>>,
    pub(crate) actions: VecDeque<NetworkBehaviourAction<void::Void, IntoAddressHandler>>,
    deferred: FuturesUnordered<
        BoxFuture<'static, NetworkBehaviourAction<void::Void, IntoAddressHandler>>,
    >,
}

impl AddressBook {
    pub fn new(
        local_peer_id: PeerId,
        local_node_name: String,
        local_public_key: PublicKey,
        port_reuse: bool,
        enable_loopback: bool,
    ) -> Self {
        Self {
            port_reuse,
            enable_loopback,
            local_node_name,
            local_peer_id,
            local_public_key,
            listeners: Default::default(),
            peers: Default::default(),
            event_stream: Default::default(),
            actions: Default::default(),
            deferred: Default::default(),
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
        let handler = self.new_handler();
        self.actions.push_back(NetworkBehaviourAction::Dial {
            opts: DialOpts::peer_id(*peer).build(),
            handler,
        });
    }

    pub fn dial_address(&mut self, peer: &PeerId, addr: Multiaddr) {
        if peer == self.local_peer_id() {
            tracing::error!("attempting to dial self");
            return;
        }
        let target = normalize_addr_ref(&addr, peer);
        if let Some(info) = self.peers.get(peer) {
            if info.connections.contains_key(target.as_ref()) {
                tracing::debug!(peer = %peer, addr = %&addr,
                    "skipping dial since already connected");
            }
        }
        tracing::debug!(peer = %peer, addr = %&addr, "request dialing");
        let handler = IntoAddressHandler(Some((target.into_owned(), 3)));
        self.actions.push_back(NetworkBehaviourAction::Dial {
            opts: DialOpts::peer_id(*peer).addresses(vec![addr]).build(),
            handler,
        });
    }

    pub fn add_address(&mut self, peer: &PeerId, mut address: Multiaddr, source: AddressSource) {
        if peer == self.local_peer_id() {
            return;
        }
        if !self.enable_loopback && address.is_loopback() {
            return;
        }
        let discovered = self
            .peers
            .get(peer)
            .filter(|info| info.confirmed_addresses().next().is_some())
            .is_none();
        let addr_full = match normalize_addr_ref(&address, peer) {
            Cow::Borrowed(a) => {
                let ret = a.clone();
                address.pop();
                ret
            }
            Cow::Owned(a) => a,
        };
        if !self.listeners.contains(&address) {
            // addr_full is with peerId, address is guaranteed without
            tracing::debug!(peer = %peer, "adding address {} from {:?}", address, source);
            let info = self.peers.entry(*peer).or_default();
            if info.ingest_address(addr_full.clone(), source)
                && !info.connections.contains_key(&addr_full)
            {
                self.actions.push_back(NetworkBehaviourAction::Dial {
                    opts: DialOpts::peer_id(*peer)
                        .condition(PeerCondition::Always)
                        .addresses(vec![address])
                        .build(),
                    handler: IntoAddressHandler(Some((addr_full, 3))),
                });
            }
            if discovered && source.is_confirmed() {
                self.notify(Event::Discovered(*peer));
            }
            self.notify(Event::NewInfo(*peer));
        } else {
            tracing::debug!(peer = %peer, addr = %&address,
                "ignoring peer address from unreachable scope");
        }
    }

    pub fn remove_address(&mut self, peer: &PeerId, address: &Multiaddr) {
        let address = normalize_addr_ref(address, peer);
        if let Some(info) = self.peers.get_mut(peer) {
            tracing::trace!("removing address {}", address);
            info.addresses.remove(&address);
        }
    }

    pub fn prune_peers(&mut self, min_age: Duration) {
        let _span = tracing::trace_span!("prune_peers").entered();
        let now = Utc::now();
        let mut remove = Vec::new();
        'l: for (peer, info) in self.peers.iter() {
            if info.connections().next().is_some() {
                tracing::trace!(peer = %peer, "keeping connected");
                continue;
            }
            if let Some(f) = info.recent_failures().next() {
                // do not remove if most recent failure is younger than min_age
                if diff_time(f.time(), now) < min_age {
                    tracing::trace!(peer = %peer, "keeping recently failed");
                    continue;
                }
            }
            for (a, s, dt) in info.addresses() {
                if s.is_confirmed() {
                    // keep those that have confirmed addresses
                    tracing::trace!(peer = %peer, addr = %a, "keeping confirmed");
                    continue 'l;
                }
                if s.is_to_probe() && diff_time(dt, now) < min_age.max(Duration::from_secs(10)) {
                    // keep those which we are presumably trying to probe
                    tracing::trace!(peer = %peer, addr = %a, "keeping probed");
                    continue 'l;
                }
            }
            tracing::trace!(peer = %peer, "pruning");
            remove.push(*peer);
        }
        for peer in remove {
            self.peers.remove(&peer);
            self.notify(Event::NewInfo(peer));
        }
    }

    pub fn connection_closed(
        &mut self,
        peer: PeerId,
        conn: ConnectedPoint,
        num_established: u32,
        error: Option<ConnectionError<NodeHandlerWrapperError<MyHandlerError>>>,
    ) {
        use libp2p::core::either::EitherError::*;
        use ConnectionError::Handler as ConnHandler;
        use NodeHandlerWrapperError::Handler as NodeHandler;

        let mut addr = conn.get_remote_address().clone();
        normalize_addr(&mut addr, &peer);

        let (reason, closed) = match error {
            Some(ConnHandler(NodeHandler(A(A(A(A(A(A(A(e)))))))))) => void::unreachable(e),
            Some(ConnHandler(NodeHandler(A(A(A(A(A(A(B(e)))))))))) => {
                (format!("Kademlia I/O error: {}", e), false)
            }
            Some(ConnHandler(NodeHandler(A(A(A(A(A(B(e))))))))) => void::unreachable(e),
            Some(ConnHandler(NodeHandler(A(A(A(A(B(e)))))))) => {
                (format!("Ping failure: {}", e), false)
            }
            Some(ConnHandler(NodeHandler(A(A(A(B(e))))))) => {
                (format!("Identify I/O error: {}", e), false)
            }
            Some(ConnHandler(NodeHandler(A(A(B(e)))))) => (format!("Bitswap error: {}", e), false),
            Some(ConnHandler(NodeHandler(A(B(e))))) => (format!("Gossipsub error: {}", e), false),
            Some(ConnHandler(NodeHandler(B(e)))) => (format!("Broadcast error: {}", e), false),
            Some(ConnHandler(NodeHandlerWrapperError::KeepAliveTimeout)) => {
                ("we closed due to missing keepalive".to_owned(), false)
            }
            Some(ConnectionError::IO(e)) => (format!("connection I/O error: {}", e), true),
            None => ("we closed".to_owned(), false),
        };

        tracing::debug!(
            addr = display(&addr),
            outbound = conn.is_dialer(),
            conn_left = %num_established,
            "connection closed ({})",
            reason
        );

        let entry = self.peers.entry(peer).or_default();
        entry.connections.remove(&addr);
        let failure = if closed {
            ConnectionFailure::WeDisconnected(addr, Utc::now(), reason)
        } else {
            ConnectionFailure::PeerDisconnected(addr, Utc::now(), reason)
        };
        entry.push_failure(failure, false);
        self.notify(Event::ConnectionClosed(peer, conn));
        self.notify(Event::NewInfo(peer));
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
        let _span = tracing::trace_span!("set_info", peer = %peer_id).entered();
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
            tracing::trace!(lp = ?&listen_port);

            // collect all advertised listen ports (which includes actual listeners as well
            // as observed addresses, which may be NATed) so that we can at
            // least try to guess a reasonable port where the NAT may have a
            // hole configured
            let common_port = listen_port
                .iter()
                .map(|(_a, p)| *p)
                .collect::<FnvHashSet<_>>();
            tracing::trace!(cp = ?&common_port);

            // in the absence of port_reuse or the presence of NAT the remote port on an
            // incoming connection won’t be reachable for us, so attempt a
            // translation that is then validated by dailling the resulting
            // address
            let mut translated = FnvHashSet::default();
            for addr in info.addresses_to_translate() {
                if let Some((ip, _p)) = ip_port(addr) {
                    let mut added = false;
                    for (_a, lp) in listen_port.iter().filter(|(a, _p)| *a == ip) {
                        tracing::trace!("adding lp {} -> {}", addr, lp);
                        translated.insert(addr.replace(1, |_| Some(Protocol::Tcp(*lp))).unwrap());
                        added = true;
                    }
                    if !added {
                        for cp in &common_port {
                            tracing::trace!("adding cp {} -> {}", addr, cp);
                            translated
                                .insert(addr.replace(1, |_| Some(Protocol::Tcp(*cp))).unwrap());
                            added = true;
                        }
                    }
                    if !added {
                        // no idea for a translation, so add it for validation
                        tracing::trace!("adding raw {}", addr);
                        translated.insert(addr.clone());
                    }
                } else {
                    tracing::trace!("ignoring {}", addr);
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
                    tracing::trace!("not adding self-addr {}", tcp);
                    continue;
                }
                tracing::debug!(peer = %peer_id, addr = %&tcp,
                    "adding address derived from Identify");
                if info.ingest_address(addr.clone(), AddressSource::Listen) {
                    // no point trying to dial if we’re already connected and port_reuse==true since
                    // a second connection is fundamentally impossible in this
                    // case
                    if self.port_reuse && info.connections.contains_key(&addr) {
                        // this will offer the address as soon as the Swarm asks for one for this
                        // peer, leading to a dial attempt that will answer
                        // the question
                        info.ingest_address(addr, AddressSource::Candidate);
                    } else {
                        self.actions.push_back(NetworkBehaviourAction::Dial {
                            opts: DialOpts::peer_id(*peer_id)
                                .condition(PeerCondition::Always)
                                .addresses(vec![tcp])
                                .build(),
                            handler: IntoAddressHandler(Some((addr, 3))),
                        })
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

fn diff_time(former: DateTime<Utc>, latter: DateTime<Utc>) -> Duration {
    latter
        .signed_duration_since(former)
        .to_std()
        .unwrap_or(Duration::ZERO)
}

pub struct SwarmEvents(mpsc::UnboundedReceiver<Event>);

impl Stream for SwarmEvents {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

impl NetworkBehaviour for AddressBook {
    type ProtocolsHandler = IntoAddressHandler;
    type OutEvent = void::Void;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        IntoAddressHandler(None)
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
        cx: &mut Context,
        _params: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<void::Void, IntoAddressHandler>> {
        if let Some(action) = self.actions.pop_front() {
            Poll::Ready(action)
        } else if !self.deferred.is_empty() {
            self.deferred.poll_next_unpin(cx).map(|p| p.unwrap())
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
        _failures: Option<&Vec<Multiaddr>>,
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

    fn inject_dial_failure(
        &mut self,
        peer_id: Option<PeerId>,
        handler: Self::ProtocolsHandler,
        error: &DialError,
    ) {
        let peer_id = if let Some(peer_id) = handler.peer_id().or(peer_id) {
            peer_id
        } else {
            tracing::debug!("dial failure without peer ID: {}", error);
            return;
        };
        if let Some(info) = self.peers.get_mut(&peer_id) {
            if let IntoAddressHandler(Some((addr, retries))) = handler {
                // this was our own validation dial
                let probe_result = matches!(
                    error,
                    DialError::InvalidPeerId | DialError::ConnectionIo(_) | DialError::Transport(_)
                );
                let transport = matches!(error, DialError::Transport(_));
                let error = error.to_string();
                let failure = ConnectionFailure::DialError(addr.clone(), Utc::now(), error.clone());
                tracing::debug!(addr = %&addr, error = %&error, active = probe_result,
                    "validation dial failure");
                info.push_failure(failure, probe_result);
                if transport
                    && retries > 0
                    && error.contains("Other(A(B(Apply(Io(Kind(InvalidData))))))")
                {
                    // TCP simultaneous open leads to both sides being initiator in the Noise
                    // handshake, which yields this particular error
                    let delay = Duration::from_secs(1) * rand::random::<u32>() / u32::MAX;
                    let action = NetworkBehaviourAction::Dial {
                        opts: DialOpts::peer_id(peer_id)
                            .addresses(vec![addr.clone()])
                            .build(),
                        handler: IntoAddressHandler(Some((addr.clone(), retries - 1))),
                    };
                    self.deferred
                        .push(Delay::new(delay).map(move |_| action).boxed());
                }
                self.notify(Event::DialFailure(peer_id, addr, error));
                self.notify(Event::NewInfo(peer_id));
            } else if let DialError::Transport(v) = error {
                let mut events = Vec::with_capacity(v.len());
                let mut deferred = Vec::new();
                for (addr, error) in v {
                    let error = format!("{:?}", error);
                    let failure =
                        ConnectionFailure::DialError(addr.clone(), Utc::now(), error.clone());
                    tracing::debug!(addr = %&addr, error = %&error, "non-validation dial failure");
                    info.push_failure(failure, true);
                    // TCP simultaneous open leads to both sides being initiator in the Noise
                    // handshake, which yields this particular error
                    if error.contains("Other(A(B(Apply(Io(Kind(InvalidData))))))") {
                        tracing::debug!("scheduling redial after presumed TCP simultaneous open");
                        deferred.push(NetworkBehaviourAction::Dial {
                            opts: DialOpts::peer_id(peer_id)
                                .addresses(vec![addr.clone()])
                                .build(),
                            handler: IntoAddressHandler(Some((addr.clone(), 3))),
                        })
                    }
                    events.push(Event::DialFailure(peer_id, addr.clone(), error));
                }
                for event in events {
                    self.notify(event);
                }
                if deferred.is_empty() {
                    self.notify(Event::Unreachable(peer_id));
                }
                for action in deferred {
                    let delay = Duration::from_secs(1) * rand::random::<u32>() / u32::MAX;
                    self.deferred
                        .push(Delay::new(delay).map(move |_| action).boxed());
                }
                self.notify(Event::NewInfo(peer_id));
            } else {
                tracing::debug!(peer = %peer_id, error = %error, "dial failure");
                if !matches!(error, DialError::Banned | DialError::LocalPeerId) {
                    self.notify(Event::Unreachable(peer_id));
                }
            }
        } else {
            tracing::debug!(peer = %peer_id, error = %error, "dial failure for unknown peer");
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
