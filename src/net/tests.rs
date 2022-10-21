use super::{address_handler::IntoAddressHandler, *};
use crate::net::peers::AddressBook;
use async_executor::LocalExecutor;
use futures::{future::ready, stream::StreamExt};
use libp2p::{
    core::{connection::ConnectionId, ConnectedPoint, Endpoint},
    identify,
    identity::ed25519::Keypair,
    multiaddr::Protocol,
    swarm::{DialError, NetworkBehaviour, NetworkBehaviourAction},
    TransportError,
};
use std::{cell::RefCell, collections::HashMap, io::ErrorKind};
use tracing_subscriber::EnvFilter;
use Event::*;

struct Events<'a> {
    events: &'a RefCell<Vec<Event>>,
    swarm: LocalExecutor<'a>,
}

impl<'a> Events<'a> {
    fn new(s: SwarmEvents, events: &'a RefCell<Vec<Event>>) -> Self {
        let swarm = LocalExecutor::new();
        swarm
            .spawn(s.for_each(move |e| {
                events.borrow_mut().push(e);
                ready(())
            }))
            .detach();
        Self { events, swarm }
    }

    pub fn next(&self) -> Vec<Event> {
        while self.swarm.try_tick() {}
        std::mem::take(&mut *self.events.borrow_mut())
    }
}

#[test]
fn test_dial_basic() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::default())
        .try_init()
        .ok();

    let mut book = AddressBook::new(
        PeerId::random(),
        false,
        false,
        Writer::new(HashSet::default()),
        Writer::new(HashMap::default()),
        Writer::new(vec![]),
    );

    let events = Default::default();
    let (tx, rx) = mpsc::unbounded();
    book.swarm_events(tx);
    let events = Events::new(SwarmEvents::new(rx), &events);

    let peer_a = PeerId::random();
    let addr_1: Multiaddr = "/ip4/1.1.1.1/tcp/3333".parse().unwrap();
    let addr_1p = addr_1.clone().with(Protocol::P2p(peer_a.into()));
    let addr_2: Multiaddr = "/ip4/2.2.2.2/tcp/3333".parse().unwrap();
    let addr_2p = addr_2.clone().with(Protocol::P2p(peer_a.into()));
    let error = std::io::Error::new(ErrorKind::Other, "my error");
    book.add_address(&peer_a, addr_1.clone(), AddressSource::Mdns);
    assert_eq!(events.next(), vec!(NewInfo(peer_a)));
    assert_eq!(dials(&mut book), vec!(Dial::A(addr_1p.clone())));
    book.add_address(&peer_a, addr_1p.clone(), AddressSource::User);
    assert_eq!(events.next(), vec!(Discovered(peer_a), NewInfo(peer_a)));
    book.add_address(&peer_a, addr_2.clone(), AddressSource::Incoming);
    assert_eq!(events.next(), vec!(NewInfo(peer_a)));
    let peers = book.peers();
    assert_eq!(peers, vec![peer_a]);
    book.inject_dial_failure(
        Some(peer_a),
        IntoAddressHandler(Some((addr_1.clone(), 3))),
        &DialError::ConnectionIo(error),
    );
    assert_eq!(
        events.next(),
        vec!(
            DialFailure(
                peer_a,
                addr_1,
                "Dial error: An I/O error occurred on the connection: \
                Custom { kind: Other, error: \"my error\" }."
                    .to_owned()
            ),
            NewInfo(peer_a)
        )
    );
    let error = std::io::Error::new(ErrorKind::Other, "my other error");
    book.inject_dial_failure(
        Some(peer_a),
        IntoAddressHandler(None),
        &DialError::Transport(vec![(addr_2.clone(), TransportError::Other(error))]),
    );
    assert_eq!(
        events.next(),
        vec!(
            DialFailure(
                peer_a,
                addr_2.clone(),
                "Other(Custom { kind: Other, error: \"my other error\" })".to_owned()
            ),
            Unreachable(peer_a),
            NewInfo(peer_a)
        )
    );
    assert_eq!(book.peers().into_iter().next(), Some(peer_a));
    assert_eq!(dials(&mut book), vec![]);

    assert_eq!(
        addrs(&book, peer_a),
        vec![
            (addr_1p.clone(), AddressSource::User),
            (addr_2p.clone(), AddressSource::Incoming)
        ]
    );
    book.remove_address(&peer_a, &addr_1p);
    book.remove_address(&peer_a, &addr_2p);
    assert_eq!(addrs(&book, peer_a), vec![]);

    book.add_address(&peer_a, addr_2.clone(), AddressSource::Kad);
    assert_eq!(
        addrs(&book, peer_a),
        vec![(addr_2p.clone(), AddressSource::Kad)]
    );
    assert_eq!(events.next(), vec![NewInfo(peer_a)]);

    book.add_address(&peer_a, addr_2, AddressSource::User);
    assert_eq!(addrs(&book, peer_a), vec![(addr_2p, AddressSource::User)]);
    assert_eq!(events.next(), vec![Discovered(peer_a), NewInfo(peer_a)]);
}

// #[test]
// fn test_dial_with_added_addrs() {
//     let mut book = AddressBook::new(
//         PeerId::random(),
//         "".into(),
//         Keypair::generate().public(),
//         false,
//         false,
//     );

//     let events = Default::default();
//     let events = Events::new(book.swarm_events(), &events);

//     let peer_a = PeerId::random();
//     let addr_1: Multiaddr = "/ip4/1.1.1.1/tcp/3333".parse().unwrap();
//     let addr_1p = addr_1.clone().with(Protocol::P2p(peer_a.into()));
//     let addr_2: Multiaddr = "/ip4/2.2.2.2/tcp/3333".parse().unwrap();
//     let error = std::io::Error::new(std::io::ErrorKind::Other, "my error");

//     book.add_address(&peer_a, addr_1.clone(), AddressSource::Mdns);
//     assert_eq!(events.next(), vec!(Discovered(peer_a), NewInfo(peer_a)));
//     assert_eq!(dials(&mut book), vec!(Dial::A(addr_1p)));

//     book.add_address(&peer_a, addr_2.clone(), AddressSource::Incoming);
//     assert_eq!(events.next(), vec!(NewInfo(peer_a)));

//     book.inject_dial_failure(
//         Some(peer_a),
//         IntoAddressHandler(Some(addr_1)),
//         &DialError::ConnectionIo(error),
//     );
//     assert_eq!(
//         events.next(),
//         vec!(
//             DialFailure(peer_a, addr_1.clone(), "my error".to_owned()),
//             NewInfo(peer_a)
//         )
//     );

//     book.inject_dial_failure(peer_a);
//     // Incoming addresses are not eligible for dialling until verified
//     assert_eq!(dials(&mut book), vec!(Dial::P(peer_a, vec!())));
//     let peers = book.peers().collect::<Vec<_>>();
//     assert_eq!(peers, vec![&peer_a]);

//     book.inject_dial_failure(Some(&peer_a), &addr_2, &error);
//     assert_eq!(
//         events.next(),
//         vec!(
//             DialFailure(peer_a, addr_2.clone(), "my error".to_owned()),
//             NewInfo(peer_a)
//         )
//     );

//     book.inject_dial_failure(&peer_a);
//     assert_eq!(events.next(), vec!(NewInfo(peer_a), Unreachable(peer_a)));
//     assert!(book.peers().next().is_none());
// }

#[test]
fn from_docker_host() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::default())
        .try_init()
        .ok();

    let peer_a = PeerId::random();
    let addr_a_1: Multiaddr = "/ip4/10.0.0.2/tcp/4001".parse().unwrap();
    let addr_a_1p = addr_a_1.clone().with(Protocol::P2p(peer_a.into()));

    let mut book = AddressBook::new(
        peer_a,
        false,
        false,
        Writer::new(HashSet::default()),
        Writer::new(HashMap::default()),
        Writer::new(vec![]),
    );
    let events = Default::default();
    let (tx, rx) = mpsc::unbounded();
    book.swarm_events(tx);
    let events = Events::new(SwarmEvents::new(rx), &events);

    let key_b = libp2p::identity::PublicKey::Ed25519(Keypair::generate().public());
    let peer_b = PeerId::from(&key_b);
    let addr_b_1: Multiaddr = "/ip4/10.0.0.10/tcp/57634".parse().unwrap();
    let addr_b_1p = addr_b_1.with(Protocol::P2p(peer_b.into()));
    let addr_b_2: Multiaddr = "/ip4/10.0.0.10/tcp/4001".parse().unwrap();
    let addr_b_2p = addr_b_2.clone().with(Protocol::P2p(peer_b.into()));
    let addr_b_3: Multiaddr = "/ip4/172.17.0.3/tcp/4001".parse().unwrap();
    let addr_b_3p = addr_b_3.clone().with(Protocol::P2p(peer_b.into()));
    let addr_b_4: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse().unwrap();

    let id = ConnectionId::new(1);
    let cp = ConnectedPoint::Listener {
        local_addr: addr_a_1p,
        send_back_addr: addr_b_1p.clone(),
    };
    book.inject_connection_established(&peer_b, &id, &cp, None, 0);
    assert_eq!(
        events.next(),
        vec![
            NewInfo(peer_b),
            Connected(peer_b),
            ConnectionEstablished(peer_b, cp.clone())
        ]
    );
    assert_eq!(dials(&mut book), vec![]);
    assert_eq!(
        addrs(&book, peer_b),
        vec![(addr_b_1p, AddressSource::Incoming)]
    );

    let info = identify::Info {
        public_key: key_b,
        protocol_version: "my protocol".to_owned(),
        agent_version: "my agent".to_owned(),
        listen_addrs: vec![addr_b_2, addr_b_3, addr_b_4],
        protocols: vec!["my proto".to_owned()],
        observed_addr: addr_a_1,
    };
    book.set_info(&peer_b, info);
    assert_eq!(events.next(), vec![NewInfo(peer_b)]);
    assert_eq!(
        dials(&mut book),
        vec![Dial::A(addr_b_2p.clone()), Dial::A(addr_b_3p.clone())]
    );
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
        role_override: Endpoint::Dialer,
    };
    book.inject_connection_established(&peer_b, &id2, &cp2, None, 0);
    assert_eq!(
        events.next(),
        vec![
            Discovered(peer_b),
            NewInfo(peer_b),
            Connected(peer_b),
            ConnectionEstablished(peer_b, cp2)
        ]
    );
    assert_eq!(dials(&mut book), vec![]);
    assert_eq!(
        addrs(&book, peer_b),
        vec![
            (addr_b_2p.clone(), AddressSource::Dial),
            (addr_b_3p.clone(), AddressSource::Listen)
        ]
    );

    let error = std::io::Error::new(ErrorKind::Other, "didn’t work, mate!");
    book.inject_dial_failure(
        Some(peer_b),
        IntoAddressHandler(Some((addr_b_3p.clone(), 3))),
        &DialError::ConnectionIo(error),
    );
    assert_eq!(
        events.next(),
        vec![
            DialFailure(
                peer_b,
                addr_b_3p,
                "Dial error: An I/O error occurred on the connection: \
                Custom { kind: Other, error: \"didn’t work, mate!\" }."
                    .to_owned()
            ),
            NewInfo(peer_b)
        ]
    );
    assert_eq!(dials(&mut book), vec![]);
    assert_eq!(addrs(&book, peer_b), vec![(addr_b_2p, AddressSource::Dial)]);
}

#[test]
fn from_docker_container() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::default())
        .try_init()
        .ok();

    let peer_a = PeerId::random();
    let addr_a_1: Multiaddr = "/ip4/10.0.0.2/tcp/4001".parse().unwrap();
    let addr_a_1p = addr_a_1.clone().with(Protocol::P2p(peer_a.into()));

    let mut book = AddressBook::new(
        peer_a,
        false,
        false,
        Writer::new(HashSet::default()),
        Writer::new(HashMap::default()),
        Writer::new(vec![]),
    );
    let events = Default::default();
    let (tx, rx) = mpsc::unbounded();
    book.swarm_events(tx);
    let events = Events::new(SwarmEvents::new(rx), &events);

    let key_b = libp2p::identity::PublicKey::Ed25519(Keypair::generate().public());
    let peer_b = PeerId::from(&key_b);
    let addr_b_1: Multiaddr = "/ip4/10.0.0.10/tcp/57634".parse().unwrap();
    let addr_b_1p = addr_b_1.with(Protocol::P2p(peer_b.into()));
    let addr_b_2: Multiaddr = "/ip4/10.0.0.10/tcp/4001".parse().unwrap();
    let addr_b_2p = addr_b_2.clone().with(Protocol::P2p(peer_b.into()));
    let addr_b_3: Multiaddr = "/ip4/172.17.0.3/tcp/4001".parse().unwrap();
    let addr_b_3p = addr_b_3.clone().with(Protocol::P2p(peer_b.into()));
    let addr_b_4: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse().unwrap();

    let id = ConnectionId::new(1);
    let cp = ConnectedPoint::Listener {
        local_addr: addr_a_1p,
        send_back_addr: addr_b_1p.clone(),
    };
    book.inject_connection_established(&peer_b, &id, &cp, None, 0);
    assert_eq!(
        events.next(),
        vec![
            NewInfo(peer_b),
            Connected(peer_b),
            ConnectionEstablished(peer_b, cp.clone())
        ]
    );
    assert_eq!(dials(&mut book), vec![]);
    assert_eq!(
        addrs(&book, peer_b),
        vec![(addr_b_1p, AddressSource::Incoming)]
    );

    let info = identify::Info {
        public_key: key_b.clone(),
        protocol_version: "my protocol".to_owned(),
        agent_version: "my agent".to_owned(),
        listen_addrs: vec![addr_b_3.clone(), addr_b_4.clone()],
        protocols: vec!["my proto".to_owned()],
        observed_addr: addr_a_1.clone(),
    };
    book.set_info(&peer_b, info);
    assert_eq!(events.next(), vec![NewInfo(peer_b)]);
    assert_eq!(
        dials(&mut book),
        vec![Dial::A(addr_b_2p.clone()), Dial::A(addr_b_3p.clone())]
    );
    assert_eq!(
        addrs(&book, peer_b),
        vec![
            (addr_b_2p.clone(), AddressSource::Listen),
            (addr_b_3p.clone(), AddressSource::Listen)
        ]
    );

    // here we assume that our observeration of that peer’s address will eventually
    // be included by that peer in its Identify info

    let info = identify::Info {
        public_key: key_b,
        protocol_version: "my protocol".to_owned(),
        agent_version: "my agent".to_owned(),
        listen_addrs: vec![addr_b_2, addr_b_3, addr_b_4],
        protocols: vec!["my proto".to_owned()],
        observed_addr: addr_a_1,
    };
    book.set_info(&peer_b, info);
    assert_eq!(events.next(), vec![NewInfo(peer_b)]);
    // no dials: we dialled all addresses last time already
    assert_eq!(dials(&mut book), vec![]);
    assert_eq!(
        addrs(&book, peer_b),
        vec![
            (addr_b_2p.clone(), AddressSource::Listen),
            (addr_b_3p.clone(), AddressSource::Listen)
        ]
    );

    let error = std::io::Error::new(ErrorKind::Other, "play it again, Sam");
    book.inject_dial_failure(
        Some(peer_b),
        IntoAddressHandler(Some((addr_b_3p.clone(), 3))),
        &DialError::ConnectionIo(error),
    );
    assert_eq!(
        events.next(),
        vec![
            DialFailure(
                peer_b,
                addr_b_3p,
                "Dial error: An I/O error occurred on the connection: \
                Custom { kind: Other, error: \"play it again, Sam\" }."
                    .to_owned()
            ),
            NewInfo(peer_b)
        ]
    );
    assert_eq!(dials(&mut book), vec![]);
    assert_eq!(
        addrs(&book, peer_b),
        vec![(addr_b_2p.clone(), AddressSource::Listen)]
    );

    let id2 = ConnectionId::new(2);
    let cp2 = ConnectedPoint::Dialer {
        address: addr_b_2p.clone(),
        role_override: Endpoint::Dialer,
    };
    book.inject_connection_established(&peer_b, &id2, &cp2, None, 0);
    assert_eq!(
        events.next(),
        vec![
            Discovered(peer_b),
            NewInfo(peer_b),
            Connected(peer_b),
            ConnectionEstablished(peer_b, cp2)
        ]
    );
    assert_eq!(dials(&mut book), vec![]);
    assert_eq!(
        addrs(&book, peer_b),
        vec![(addr_b_2p.clone(), AddressSource::Dial)]
    );

    let error = std::io::Error::new(ErrorKind::Other, "play it yet another time, Sam");
    book.inject_dial_failure(
        Some(peer_b),
        IntoAddressHandler(Some((addr_b_2p.clone(), 3))),
        &DialError::ConnectionIo(error),
    );
    assert_eq!(
        events.next(),
        vec![
            DialFailure(
                peer_b,
                addr_b_2p.clone(),
                "Dial error: An I/O error occurred on the connection: \
                Custom { kind: Other, error: \"play it yet another time, Sam\" }."
                    .to_owned()
            ),
            NewInfo(peer_b)
        ]
    );
    assert_eq!(dials(&mut book), vec![]);
    assert_eq!(addrs(&book, peer_b), vec![(addr_b_2p, AddressSource::Dial)]);

    assert_eq!(
        book.info(&peer_b)
            .unwrap()
            .recent_failures()
            .map(|cf| (
                cf.addr().to_string(),
                cf.display().to_owned(),
                cf.debug().to_owned()
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                "/ip4/10.0.0.10/tcp/4001".to_owned(),
                "I/O error: play it yet another time, Sam".to_owned(),
                "ConnectionIo(Custom { kind: Other, error: \"play it yet another time, Sam\" })"
                    .to_owned(),
            ),
            (
                "/ip4/172.17.0.3/tcp/4001".to_owned(),
                "I/O error: play it again, Sam".to_owned(),
                "ConnectionIo(Custom { kind: Other, error: \"play it again, Sam\" })".to_owned(),
            )
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Dial {
    A(Multiaddr),
    P(PeerId, Vec<Multiaddr>),
}

fn dials(book: &mut AddressBook) -> Vec<Dial> {
    let mut v = book
        .actions
        .drain(..)
        .filter_map(|a| match a {
            NetworkBehaviourAction::Dial {
                handler: IntoAddressHandler(Some((address, _retries))),
                ..
            } => Some(Dial::A(address)),
            NetworkBehaviourAction::Dial { opts, .. } => opts
                .get_peer_id()
                .map(|peer_id| Dial::P(peer_id, Vec::new())),
            _ => None,
        })
        .collect::<Vec<_>>();
    v.sort();
    for d in &mut v {
        match d {
            Dial::A(_) => {}
            Dial::P(peer, addrs) => addrs.extend(book.addresses_of_peer(peer)),
        }
    }
    v
}
