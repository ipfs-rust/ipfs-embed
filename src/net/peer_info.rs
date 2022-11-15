use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use libp2p::{
    core::ConnectedPoint, multiaddr::Protocol, swarm::DialError, Multiaddr, TransportError,
};
use std::{
    borrow::Cow, cmp::Ordering, collections::VecDeque, error::Error, fmt::Write, io, time::Duration,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerInfo {
    pub(crate) protocol_version: Option<String>,
    pub(crate) agent_version: Option<String>,
    pub(crate) protocols: Vec<String>,
    pub(crate) listeners: Vec<Multiaddr>,
    pub(crate) addresses: FnvHashMap<Multiaddr, (AddressSource, DateTime<Utc>)>,
    pub(crate) connections: FnvHashMap<Multiaddr, (DateTime<Utc>, Direction)>,
    failures: VecDeque<ConnectionFailure>,
    rtt: Option<Rtt>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Inbound,
    Outbound,
}

impl From<&ConnectedPoint> for Direction {
    fn from(cp: &ConnectedPoint) -> Self {
        match cp {
            ConnectedPoint::Dialer { .. } => Direction::Outbound,
            ConnectedPoint::Listener { .. } => Direction::Inbound,
        }
    }
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

    pub fn connections(&self) -> impl Iterator<Item = (&Multiaddr, DateTime<Utc>, Direction)> {
        self.connections.iter().map(|(a, (dt, dir))| (a, *dt, *dir))
    }

    pub fn rtt(&self) -> Option<Duration> {
        self.rtt.map(|x| x.current)
    }

    pub fn full_rtt(&self) -> Option<Rtt> {
        self.rtt
    }

    pub(crate) fn set_rtt(&mut self, rtt: Option<Duration>) {
        if let Some(duration) = rtt {
            if let Some(ref mut rtt) = self.rtt {
                rtt.register(duration);
            } else {
                self.rtt = Some(Rtt::new(duration));
            }
        } else if let Some(ref mut rtt) = self.rtt {
            rtt.register_failure();
        }
    }

    pub fn recent_failures(&self) -> impl Iterator<Item = &ConnectionFailure> {
        self.failures.iter()
    }

    pub(crate) fn push_failure(
        &mut self,
        addr: &Multiaddr,
        f: ConnectionFailure,
        probe_result: bool,
    ) {
        if self.failures.len() > 9 {
            self.failures.pop_back();
        }
        if probe_result
            && self
                .addresses
                .get(addr)
                .iter()
                .any(|(s, _dt)| s.is_to_probe())
        {
            self.addresses.remove(addr);
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

    pub(crate) fn ingest_address(&mut self, addr: Multiaddr, source: AddressSource) -> bool {
        if let Some((src, dt)) = self.addresses.get_mut(&addr) {
            *dt = Utc::now();
            match source.cmp(src) {
                Ordering::Less => false,
                Ordering::Equal => false,
                Ordering::Greater => {
                    *src = source;
                    source.is_to_probe()
                }
            }
        } else {
            debug_assert!(matches!(addr.iter().last(), Some(Protocol::P2p(_))));
            self.addresses.insert(addr, (source, Utc::now()));
            source.is_to_probe()
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Rtt {
    current: Duration,
    decay_3: Duration,
    decay_10: Duration,
    failures: u32,
    failure_rate: u32,
}

impl Rtt {
    pub fn new(current: Duration) -> Self {
        Self {
            current,
            decay_3: current,
            decay_10: current,
            failures: 0,
            failure_rate: 0,
        }
    }

    pub fn register(&mut self, current: Duration) {
        self.current = current;
        self.decay_3 = self.decay_3 * 7 / 10 + current * 3 / 10;
        self.decay_10 = self.decay_10 * 9 / 10 + current / 10;
        self.failures = 0;
        self.failure_rate = self.failure_rate * 99 / 100;
    }

    pub fn register_failure(&mut self) {
        self.failures += 1;
        // failures decay at 1% rate, failure_rate is 1_000_000 for only failures
        self.failure_rate = self.failure_rate * 99 / 100 + 10_000;
    }

    /// Get the rtt's last recent value.
    pub fn current(&self) -> Duration {
        self.current
    }

    /// Get the rtt's exponentially weighted moving average
    ///
    /// Decay parameter is 30%.
    pub fn decay_3(&self) -> Duration {
        self.decay_3
    }

    /// Get the rtt's exponentially weighted moving average
    ///
    /// Decay parameter is 10%.
    pub fn decay_10(&self) -> Duration {
        self.decay_10
    }

    /// Get the rtt's failure counter.
    pub fn failures(&self) -> u32 {
        self.failures
    }

    /// Get the rtt's exponentially weighted moving average of the failure rate
    ///
    /// Decay parameter is 1% and the returned value is rate * 1e6.
    pub fn failure_rate(&self) -> u32 {
        self.failure_rate
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum AddressSource {
    Incoming,
    Listen,
    Kad,
    Mdns,
    Candidate,
    User,
    Dial,
}

impl AddressSource {
    pub fn is_confirmed(&self) -> bool {
        matches!(
            self,
            AddressSource::Dial | AddressSource::User | AddressSource::Candidate
        )
    }
    pub fn is_to_probe(&self) -> bool {
        matches!(
            self,
            AddressSource::Listen
                | AddressSource::Kad
                | AddressSource::Mdns
                | AddressSource::Candidate
        )
    }
    pub fn is_to_translate(&self) -> bool {
        matches!(self, AddressSource::Incoming)
    }
}

#[test]
fn address_source_order() {
    use AddressSource::*;
    assert!(Dial > User);
    assert!(User > Mdns);
    assert!(Mdns > Kad);
    assert!(Kad > Listen);
    assert!(Listen > Incoming);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionFailure {
    kind: ConnectionFailureKind,
    addr: Multiaddr,
    time: DateTime<Utc>,
    display: String,
    debug: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionFailureKind {
    DialError,
    PeerDisconnected,
    WeDisconnected,
}

fn without_peer(a: &Multiaddr) -> Cow<'_, Multiaddr> {
    if matches!(a.iter().last(), Some(Protocol::P2p(_))) {
        let mut a = a.clone();
        a.pop();
        Cow::Owned(a)
    } else {
        Cow::Borrowed(a)
    }
}

impl ConnectionFailure {
    pub(crate) fn dial(addr: Multiaddr, error: &DialError) -> Self {
        let display = match error {
            DialError::ConnectionIo(e) => format!("I/O error: {}", e),
            DialError::Transport(e) => {
                if e.len() == 1 {
                    // this should always be the case since we only get here for validation dials
                    format!("transport error: {}", D(&e[0].1))
                } else {
                    let mut s = "transport errors:".to_owned();
                    for (addr, err) in e {
                        s.push('\n');
                        let _ = write!(&mut s, "{} {}", without_peer(addr), D(err));
                    }
                    s
                }
            }
            x => x.to_string(),
        };
        Self {
            kind: ConnectionFailureKind::DialError,
            addr: without_peer(&addr).into_owned(),
            time: Utc::now(),
            display,
            debug: format!("{:?}", error),
        }
    }

    pub(crate) fn transport(addr: Multiaddr, error: &TransportError<std::io::Error>) -> Self {
        Self {
            kind: ConnectionFailureKind::DialError,
            addr: without_peer(&addr).into_owned(),
            time: Utc::now(),
            display: format!("transport error: {}", D(error)),
            debug: format!("{:?}", error),
        }
    }

    pub(crate) fn us(addr: Multiaddr, display: String, debug: String) -> Self {
        Self {
            kind: ConnectionFailureKind::WeDisconnected,
            addr,
            time: Utc::now(),
            display,
            debug,
        }
    }

    pub(crate) fn them(addr: Multiaddr, display: String, debug: String) -> Self {
        Self {
            kind: ConnectionFailureKind::PeerDisconnected,
            addr,
            time: Utc::now(),
            display,
            debug,
        }
    }

    pub fn kind(&self) -> ConnectionFailureKind {
        self.kind
    }

    pub fn addr(&self) -> &Multiaddr {
        &self.addr
    }

    pub fn time(&self) -> DateTime<Utc> {
        self.time
    }

    pub fn display(&self) -> &str {
        self.display.as_str()
    }

    pub fn debug(&self) -> &str {
        self.debug.as_str()
    }
}

struct D<'a>(&'a TransportError<io::Error>);

impl<'a> std::fmt::Display for D<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)?;
        if let Some(cause) = self.0.source() {
            write!(f, "{}", cause)?;
        }
        Ok(())
    }
}
