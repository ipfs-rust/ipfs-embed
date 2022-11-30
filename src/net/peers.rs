use super::{
    address_handler::IntoAddressHandler,
    behaviour::MyHandlerError,
    peer_info::{AddressSource, Direction, PeerInfo},
};
use crate::{net::peer_info::ConnectionFailure, variable::Writer};
use anyhow::Result;
use chrono::{DateTime, Utc};
use fnv::{FnvHashMap, FnvHashSet};
use futures::{
    channel::mpsc::{self, UnboundedSender},
    future::BoxFuture,
    stream::{FuturesUnordered, Stream},
    FutureExt, StreamExt,
};
use futures_timer::Delay;
use lazy_static::lazy_static;
use libp2p::{
    core::{
        connection::ConnectedPoint,
        either::EitherError,
        transport::{timeout::TransportTimeoutError, ListenerId},
        UpgradeError,
    },
    dns::DnsErr,
    identify,
    multiaddr::Protocol,
    noise::NoiseError,
    swarm::{
        derive_prelude::FromSwarm,
        dial_opts::{DialOpts, PeerCondition},
        AddressRecord, ConnectionError, DialError, NetworkBehaviour, NetworkBehaviourAction,
        PollParameters,
    },
    Multiaddr, PeerId, TransportError,
};
use prometheus::{IntCounter, IntGauge, Registry};
use std::{
    borrow::Cow,
    collections::VecDeque,
    convert::TryInto,
    io::ErrorKind,
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

const SIM_OPEN_RETRIES: u8 = 10;

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

fn without_peer_id(addr: &Multiaddr) -> Multiaddr {
    let mut addr = addr.clone();
    if let Some(Protocol::P2p(_)) = addr.iter().last() {
        addr.pop();
    }
    addr
}

fn normalize_connected_point(
    cp: &ConnectedPoint,
    local: &PeerId,
    remote: &PeerId,
) -> ConnectedPoint {
    match cp {
        ConnectedPoint::Dialer {
            address,
            role_override,
        } => ConnectedPoint::Dialer {
            address: normalize_addr_ref(address, remote).into_owned(),
            role_override: *role_override,
        },
        ConnectedPoint::Listener {
            local_addr,
            send_back_addr,
        } => ConnectedPoint::Listener {
            local_addr: normalize_addr_ref(local_addr, local).into_owned(),
            send_back_addr: normalize_addr_ref(send_back_addr, remote).into_owned(),
        },
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
    keep_alive: bool,
    local_peer_id: PeerId,
    listeners: Writer<FnvHashSet<Multiaddr>>,
    peers: Writer<FnvHashMap<PeerId, PeerInfo>>,
    external: Writer<Vec<AddressRecord>>,
    refresh_external: bool,
    event_stream: Vec<mpsc::UnboundedSender<Event>>,
    pub(crate) actions: VecDeque<NetworkBehaviourAction<void::Void, IntoAddressHandler>>,
    deferred: FuturesUnordered<
        BoxFuture<'static, NetworkBehaviourAction<void::Void, IntoAddressHandler>>,
    >,
}

impl AddressBook {
    pub fn new(
        local_peer_id: PeerId,
        port_reuse: bool,
        enable_loopback: bool,
        keep_alive: bool,
        listeners: Writer<FnvHashSet<Multiaddr>>,
        peers: Writer<FnvHashMap<PeerId, PeerInfo>>,
        external: Writer<Vec<AddressRecord>>,
    ) -> Self {
        Self {
            port_reuse,
            enable_loopback,
            keep_alive,
            local_peer_id,
            listeners,
            peers,
            external,
            refresh_external: true,
            event_stream: Default::default(),
            actions: Default::default(),
            deferred: Default::default(),
        }
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
        let mut peers = self.peers.write();
        let info = peers.entry(*peer).or_default();
        if info.connections.contains_key(target.as_ref()) {
            tracing::debug!(peer = %peer, addr = %&addr,
                    "skipping dial since already connected");
            return;
        }
        drop(peers);
        tracing::debug!(peer = %peer, addr = %&addr, "request dialing");
        let handler = IntoAddressHandler(
            Some((target.into_owned(), SIM_OPEN_RETRIES + 1)),
            self.keep_alive,
        );
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
            .read()
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
        let is_listener = self.listeners.read().contains(&address);
        if !is_listener {
            // addr_full is with peerId, address is guaranteed without
            tracing::debug!(peer = %peer, "adding address {} from {:?}", address, source);
            let mut peers = self.peers.write();
            let info = peers.entry(*peer).or_default();
            let result = info.ingest_address(addr_full.clone(), source)
                && !info.connections.contains_key(&addr_full);
            drop(peers);
            if result {
                self.actions.push_back(NetworkBehaviourAction::Dial {
                    opts: DialOpts::peer_id(*peer)
                        .condition(PeerCondition::Always)
                        .addresses(vec![address])
                        .build(),
                    handler: IntoAddressHandler(Some((addr_full, SIM_OPEN_RETRIES + 1)), false),
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
        if let Some(info) = self.peers.write().get_mut(peer) {
            let address = normalize_addr_ref(address, peer);
            tracing::trace!("removing address {}", address);
            info.addresses.remove(&address);
        }
    }

    pub fn prune_peers(&mut self, min_age: Duration) {
        let _span = tracing::trace_span!("prune_peers").entered();
        let now = Utc::now();
        let mut remove = Vec::new();
        'l: for (peer, info) in self.peers.read().iter() {
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
            self.peers.write().remove(&peer);
            self.notify(Event::NewInfo(peer));
        }
    }

    pub fn connection_closed(
        &mut self,
        peer: PeerId,
        conn: ConnectedPoint,
        num_established: u32,
        error: Option<ConnectionError<MyHandlerError>>,
    ) {
        use libp2p::core::either::EitherError::*;
        use ConnectionError::Handler as ConnHandler;

        let conn = normalize_connected_point(&conn, &self.local_peer_id, &peer);
        let addr = conn.get_remote_address();

        let debug = format!("{:?}", error);
        let (reason, peer_closed) = match error {
            Some(ConnHandler(A(A(A(A(A(A(A(e))))))))) => void::unreachable(e),
            Some(ConnHandler(A(A(A(A(A(A(B(e))))))))) => {
                (format!("Kademlia I/O error: {}", e), false)
            }
            Some(ConnHandler(A(A(A(A(A(B(e)))))))) => void::unreachable(e),
            Some(ConnHandler(A(A(A(A(B(e))))))) => (format!("Ping failure: {}", e), false),
            Some(ConnHandler(A(A(A(B(e)))))) => (format!("Identify I/O error: {}", e), false),
            Some(ConnHandler(A(A(B(e))))) => (format!("Bitswap error: {}", e), false),
            Some(ConnHandler(A(B(e)))) => (format!("Gossipsub error: {}", e), false),
            Some(ConnHandler(B(e))) => (format!("Broadcast error: {}", e), false),
            Some(ConnectionError::IO(e)) => (format!("connection I/O error: {}", e), true),
            Some(ConnectionError::KeepAliveTimeout) => {
                ("we closed due to missing keepalive".to_owned(), false)
            }
            None => ("we closed".to_owned(), false),
        };

        tracing::debug!(
            addr = display(&addr),
            outbound = conn.is_dialer(),
            conn_left = %num_established,
            "connection closed ({})",
            reason
        );

        let mut peers = self.peers.write();
        let entry = peers.entry(peer).or_default();
        entry.connections.remove(addr);
        let addr_no_peer = without_peer_id(addr);
        let failure = if peer_closed {
            ConnectionFailure::them(addr_no_peer, reason, debug)
        } else {
            ConnectionFailure::us(addr_no_peer, reason, debug)
        };
        entry.push_failure(addr, failure, false);
        drop(peers);

        self.notify(Event::ConnectionClosed(peer, conn));
        if num_established == 0 {
            self.notify(Event::Disconnected(peer));
        }
        self.notify(Event::NewInfo(peer));
    }

    #[cfg(test)]
    pub fn peers(&self) -> Vec<PeerId> {
        self.peers.read().keys().copied().collect()
    }

    #[cfg(test)]
    pub fn info(&self, peer_id: &PeerId) -> Option<PeerInfo> {
        self.peers.read().get(peer_id).cloned()
    }

    pub fn set_rtt(&mut self, peer_id: &PeerId, rtt: Option<Duration>) {
        let mut peers = self.peers.write();
        if let Some(info) = peers.get_mut(peer_id) {
            info.set_rtt(rtt);
            drop(peers);
            self.notify(Event::NewInfo(*peer_id));
        }
    }

    pub fn set_info(&mut self, peer_id: &PeerId, identify: identify::Info) {
        let _span = tracing::trace_span!("set_info", peer = %peer_id).entered();
        let mut peers = self.peers.write();
        if let Some(info) = peers.get_mut(peer_id) {
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
                if self.listeners.read().contains(&tcp) {
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
                            handler: IntoAddressHandler(Some((addr, 4)), false),
                        })
                    }
                }
            }
            drop(peers);
            self.notify(Event::NewInfo(*peer_id));
        }
    }

    pub fn swarm_events(&mut self, tx: UnboundedSender<Event>) {
        self.event_stream.push(tx);
    }

    pub fn notify(&mut self, event: Event) {
        tracing::trace!("{:?}", event);
        self.event_stream
            .retain(|tx| tx.unbounded_send(event.clone()).is_ok());
    }

    pub(crate) fn dial_failure(
        &mut self,
        handler: IntoAddressHandler,
        peer_id: Option<PeerId>,
        error: &DialError,
    ) {
        let peer_id = if let Some(peer_id) = handler.peer_id().or(peer_id) {
            peer_id
        } else {
            tracing::debug!("dial failure without peer ID: {}", error);
            return;
        };
        let mut peer = self.peers.write();
        if let Some(info) = peer.get_mut(&peer_id) {
            if let IntoAddressHandler(Some((addr, retries)), keep_alive) = handler {
                // this was our own validation dial
                let transport = matches!(error, DialError::Transport(_));
                let wrong_peer = matches!(error, DialError::WrongPeerId { .. });
                let probe_result =
                    transport || wrong_peer || matches!(error, DialError::ConnectionIo(_));
                let failure = ConnectionFailure::dial(without_peer_id(&addr), error);
                let is_sim_open = if let DialError::Transport(v) = error {
                    v.iter().any(|(_, e)| is_sim_open(e))
                } else {
                    false
                };

                let error = error.to_string();
                tracing::debug!(addr = %&addr, error = %&error, active = probe_result,
                    "validation dial failure");
                info.push_failure(&addr, failure, probe_result);
                if wrong_peer {
                    // we know who we dialled and we know someone else answered => kill the address
                    // regardless of whether it was confirmed
                    info.addresses.remove(&addr);
                }
                if is_sim_open && retries > 0 {
                    // TCP simultaneous open leads to both sides being initiator in the Noise
                    // handshake, which yields this particular error
                    if retries == SIM_OPEN_RETRIES + 1 {
                        tracing::debug!("scheduling redial after presumed TCP simultaneous open");
                    }
                    let delay = Duration::from_secs(1) * rand::random::<u32>() / u32::MAX;
                    let action = NetworkBehaviourAction::Dial {
                        opts: DialOpts::peer_id(peer_id)
                            .addresses(vec![addr.clone()])
                            .build(),
                        handler: IntoAddressHandler(Some((addr.clone(), retries - 1)), keep_alive),
                    };
                    self.deferred
                        .push(Delay::new(delay).map(move |_| action).boxed());
                }
                drop(peer);

                self.notify(Event::DialFailure(peer_id, addr, error));
                self.notify(Event::NewInfo(peer_id));
            } else if let DialError::Transport(v) = error {
                let mut events = Vec::with_capacity(v.len());
                let mut deferred = Vec::new();
                for (addr, error) in v {
                    let is_sim_open = is_sim_open(error);
                    let failure = ConnectionFailure::transport(without_peer_id(addr), error);
                    let error = format!("{:?}", error);
                    tracing::debug!(addr = %&addr, error = %&error, "non-validation dial failure");
                    info.push_failure(normalize_addr_ref(addr, &peer_id).as_ref(), failure, true);
                    // TCP simultaneous open leads to both sides being initiator in the Noise
                    // handshake, which yields this particular error
                    if is_sim_open {
                        tracing::debug!("scheduling redial after presumed TCP simultaneous open");
                        deferred.push(NetworkBehaviourAction::Dial {
                            opts: DialOpts::peer_id(peer_id)
                                .addresses(vec![addr.clone()])
                                .build(),
                            handler: IntoAddressHandler(
                                Some((addr.clone(), SIM_OPEN_RETRIES)),
                                self.keep_alive,
                            ),
                        })
                    }
                    events.push(Event::DialFailure(peer_id, addr.clone(), error));
                }
                drop(peer);
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
            } else if let DialError::DialPeerConditionFalse(d) = error {
                tracing::trace!(peer = %peer_id, cond = ?d, "dial condition not satisfied");
            } else {
                drop(peer);
                tracing::debug!(peer = %peer_id, error = %error, "dial failure");
                if !matches!(error, DialError::Banned | DialError::LocalPeerId) {
                    self.notify(Event::Unreachable(peer_id));
                }
            }
        } else {
            tracing::debug!(peer = %peer_id, error = %error, "dial failure for unknown peer");
        }
    }
}

pub fn register_metrics(registry: &Registry) -> Result<()> {
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

#[derive(Debug)]
pub struct SwarmEvents(mpsc::UnboundedReceiver<Event>);

impl SwarmEvents {
    pub fn new(channel: mpsc::UnboundedReceiver<Event>) -> Self {
        Self(channel)
    }
}

impl Stream for SwarmEvents {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

impl NetworkBehaviour for AddressBook {
    type ConnectionHandler = IntoAddressHandler;
    type OutEvent = void::Void;

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        IntoAddressHandler(None, self.keep_alive)
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        if let Some(info) = self.peers.read().get(peer_id) {
            info.confirmed_addresses().cloned().collect()
        } else {
            vec![]
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context,
        params: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<void::Void, IntoAddressHandler>> {
        if self.refresh_external {
            self.refresh_external = false;
            *self.external.write() = params.external_addresses().collect();
        }
        if let Some(action) = self.actions.pop_front() {
            Poll::Ready(action)
        } else if !self.deferred.is_empty() {
            self.deferred.poll_next_unpin(cx).map(|p| p.unwrap())
        } else {
            Poll::Pending
        }
    }

    fn on_swarm_event(&mut self, event: FromSwarm<Self::ConnectionHandler>) {
        match event {
            FromSwarm::ConnectionEstablished(c) => {
                let conn = normalize_connected_point(c.endpoint, &self.local_peer_id, &c.peer_id);
                let address = conn.get_remote_address();
                tracing::debug!(
                    addr = %address,
                    out = conn.is_dialer(),
                    "connection established"
                );
                let src = if conn.is_dialer() {
                    AddressSource::Dial
                } else {
                    AddressSource::Incoming
                };
                self.add_address(&c.peer_id, address.clone(), src);
                self.peers
                    .write()
                    .entry(c.peer_id)
                    .or_default()
                    .connections
                    .insert(address.clone(), (Utc::now(), Direction::from(&conn)));
                if c.other_established == 0 {
                    self.notify(Event::Connected(c.peer_id));
                }
                self.notify(Event::ConnectionEstablished(c.peer_id, conn));
            }
            FromSwarm::ConnectionClosed(_) => {
                // handled via external SwarmEvent since that is the only way to get the reason
            }
            FromSwarm::AddressChange(a) => {
                let old = normalize_connected_point(a.old, &self.local_peer_id, &a.peer_id);
                let new = normalize_connected_point(a.new, &self.local_peer_id, &a.peer_id);
                let old_addr = old.get_remote_address();
                let new_addr = new.get_remote_address();
                tracing::debug!(
                    old = %old.get_remote_address(),
                    new = %new_addr,
                    out = new.is_dialer(),
                    "address changed"
                );
                let src = if new.is_dialer() {
                    AddressSource::Dial
                } else {
                    AddressSource::Incoming
                };
                self.add_address(&a.peer_id, new_addr.clone(), src);
                let mut peers = self.peers.write();
                let entry = peers.entry(a.peer_id).or_default();
                entry.connections.remove(old_addr);
                entry
                    .connections
                    .insert(new_addr.clone(), (Utc::now(), Direction::from(&new)));
                drop(peers);

                self.notify(Event::AddressChanged(a.peer_id, old, new));
            }
            FromSwarm::DialFailure(d) => self.dial_failure(d.handler, d.peer_id, d.error),
            FromSwarm::ListenFailure(_) => {}
            FromSwarm::NewListener(l) => {
                tracing::trace!("listener {:?}: created", l.listener_id);
                LISTENERS.inc();
                self.notify(Event::NewListener(l.listener_id));
            }
            FromSwarm::NewListenAddr(l) => {
                tracing::trace!("listener {:?}: new listen addr {}", l.listener_id, l.addr);
                if self.listeners.write().insert(l.addr.clone()) {
                    LISTEN_ADDRS.inc();
                }
                self.notify(Event::NewListenAddr(l.listener_id, l.addr.clone()));
            }
            FromSwarm::ExpiredListenAddr(l) => {
                tracing::trace!(
                    "listener {:?}: expired listen addr {}",
                    l.listener_id,
                    l.addr
                );
                if self.listeners.write().remove(l.addr) {
                    LISTEN_ADDRS.dec();
                }
                self.notify(Event::ExpiredListenAddr(l.listener_id, l.addr.clone()));
            }
            FromSwarm::ListenerError(l) => {
                let err = format!("{:#}", l.err);
                tracing::trace!("listener {:?}: listener error {}", l.listener_id, err);
                LISTENER_ERROR.inc();
                self.notify(Event::ListenerError(l.listener_id, err));
            }
            FromSwarm::ListenerClosed(l) => {
                tracing::trace!(
                    "listener {:?}: closed for reason {:?}",
                    l.listener_id,
                    l.reason
                );
                LISTENERS.dec();
                self.notify(Event::ListenerClosed(l.listener_id));
            }
            FromSwarm::NewExternalAddr(a) => {
                self.refresh_external = true;
                let mut addr = a.addr.clone();
                normalize_addr(&mut addr, self.local_peer_id());
                tracing::trace!("new external addr {}", addr);
                EXTERNAL_ADDRS.inc();
                self.notify(Event::NewExternalAddr(addr));
            }
            FromSwarm::ExpiredExternalAddr(a) => {
                self.refresh_external = true;
                let mut addr = a.addr.clone();
                normalize_addr(&mut addr, self.local_peer_id());
                tracing::trace!("expired external addr {}", addr);
                EXTERNAL_ADDRS.dec();
                self.notify(Event::ExpiredExternalAddr(addr));
            }
        }
    }
}

fn is_sim_open(error: &TransportError<std::io::Error>) -> bool {
    match error {
        libp2p::TransportError::MultiaddrNotSupported(_x) => false,
        libp2p::TransportError::Other(err) => {
            let err = err
                .get_ref()
                .and_then(|e| e.downcast_ref::<DnsErr<std::io::Error>>());
            if let Some(DnsErr::Transport(err)) = err {
                let err = err
                    .get_ref()
                    .and_then(|e| e.downcast_ref::<super::TransportError>());
                if let Some(TransportTimeoutError::Other(EitherError::A(EitherError::B(
                    UpgradeError::Apply(NoiseError::Io(err)),
                )))) = err
                {
                    err.kind() == ErrorKind::InvalidData
                } else {
                    false
                }
            } else {
                false
            }
        }
    }
}
