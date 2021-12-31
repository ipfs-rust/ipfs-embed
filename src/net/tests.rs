use super::*;
use crate::{generate_keypair, net::peers::AddressBook};
use async_executor::LocalExecutor;
use futures::{future::ready, stream::StreamExt};
use libp2p::{
    core::{connection::ConnectionId, ConnectedPoint},
    identify::IdentifyInfo,
    multiaddr::Protocol,
    swarm::{NetworkBehaviour, NetworkBehaviourAction},
};
use libp2p_quic::ToLibp2p;
use regex::Regex;
use std::cell::RefCell;
use Event::*;

#[async_std::test]
async fn test_dial_basic() {
    let mut book = AddressBook::new(
        PeerId::random(),
        "".into(),
        generate_keypair().public,
        false,
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
    let addrs = Regex::new(r"/ip./(?P<a>[0-9a-fA-F.:]+)/tcp/(?P<p>\d+)/p2p/[0-9a-zA-Z]+").unwrap();
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
