use chrono::{DateTime, SecondsFormat::Millis, Utc};
use fnv::FnvHashMap;
use libp2p::{core::ConnectedPoint, Multiaddr};
use std::{cmp::Ordering, collections::VecDeque, time::Duration};

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

    pub(crate) fn push_failure(&mut self, f: ConnectionFailure, probe_result: bool) {
        if self.failures.len() > 9 {
            self.failures.pop_back();
        }
        if probe_result
            && self
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
pub enum ConnectionFailure {
    DialError(Multiaddr, DateTime<Utc>, String),
    PeerDisconnected(Multiaddr, DateTime<Utc>, String),
    WeDisconnected(Multiaddr, DateTime<Utc>, String),
}

impl ConnectionFailure {
    pub fn addr(&self) -> &Multiaddr {
        match self {
            ConnectionFailure::DialError(a, _, _) => a,
            ConnectionFailure::PeerDisconnected(a, _, _) => a,
            ConnectionFailure::WeDisconnected(a, _, _) => a,
        }
    }

    pub fn time(&self) -> DateTime<Utc> {
        match self {
            ConnectionFailure::DialError(_, t, _) => *t,
            ConnectionFailure::PeerDisconnected(_, t, _) => *t,
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
            ConnectionFailure::PeerDisconnected(a, t, e) => write!(
                f,
                "{} disconnected at {} ({})",
                a,
                t.to_rfc3339_opts(Millis, true),
                e
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
