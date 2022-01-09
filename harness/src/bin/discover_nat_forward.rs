#[cfg(target_os = "linux")]
fn main() -> anyhow::Result<()> {
    use anyhow::Context;
    use harness::{MachineExt, MultiaddrExt, MyFutureExt, NetsimExt};
    use ipfs_embed_cli::{Command, Config, Event};
    use libp2p::{multiaddr::Protocol, Multiaddr};
    use maplit::hashmap;
    use netsim_embed::{Ipv4Range, NatConfig};
    use std::{net::SocketAddrV4, time::Instant};

    fn sock_addr(m: &Multiaddr) -> SocketAddrV4 {
        let mut iter = m.iter();
        let ip = match iter.next().unwrap() {
            Protocol::Ip4(ip) => ip,
            _ => panic!("need IPv4"),
        };
        let port = match iter.next().unwrap() {
            Protocol::Tcp(p) => p,
            _ => panic!("need TCP"),
        };
        SocketAddrV4::new(ip, port)
    }

    harness::build_bin()?;

    harness::run_netsim(|mut sim, opts, net_a, temp_dir| async move {
        let range_b = {
            let range_a = sim.network(net_a).range();
            loop {
                let r = Ipv4Range::random_local_subnet();
                if r != range_a {
                    break r;
                }
            }
        };
        let net_b = sim.spawn_network(range_b);

        let consumers = opts.n_nodes..2 * opts.n_nodes;
        for i in consumers.clone() {
            let cfg = Config {
                path: Some(temp_dir.path().join(i.to_string())),
                node_name: Some(format!("consumer-{}", i)),
                keypair: i as _,
                listen_on: vec!["/ip4/0.0.0.0/tcp/30000".parse().unwrap()],
                bootstrap: vec![],
                external: vec![],
                enable_mdns: opts.enable_mdns,
                disable_port_reuse: opts.disable_port_reuse,
            };
            let cmd = async_process::Command::from(cfg);
            let machine = sim.spawn_machine(cmd, None).await;
            sim.plug(machine, net_b, None).await;
            let m = sim.machine(machine);
            tracing::warn!(
                "{} started with address {} and peer id {}",
                machine,
                m.addr(),
                m.peer_id(),
            );
        }

        let providers = sim.nodes(0..opts.n_nodes);
        let consumers = sim.nodes(consumers);

        let m_nat = sim.machines()[opts.n_nodes].id();
        let nat = NatConfig {
            forward_ports: vec![
                (netsim_embed::Protocol::Tcp, 30000, sock_addr(&consumers[&m_nat].1))
            ],
            ..Default::default()
        };
        sim.add_nat_route(nat, net_a, net_b);

        let started = Instant::now();
        for id in providers.keys().chain(consumers.keys()) {
            let m = sim.machine(*id);
            m.select(|e| matches!(e, Event::NewListenAddr(a) if !a.is_loopback()).then(|| ()))
                .deadline(started, 5).await. unwrap();
        }

        for id in consumers.keys() {
            let m = sim.machine(*id);
            for (peer, addr) in providers.values() {
                m.send(Command::AddAddress(*peer, addr.clone()));
                m.send(Command::Dial(*peer));
            }
        }

        let started = Instant::now();
        for id in consumers.keys() {
            let m = sim.machine(*id);
            for (peer, addr) in providers.values() {
                m.select(|e| {
                    matches!(e, Event::PeerInfo(p, i)
                        if p == peer && i.addresses == hashmap!(addr.clone() => "Dial".to_owned())
                    )
                    .then(|| ())
                })
                .deadline(started, 5).await.unwrap();
            }
            tracing::info!("consumer {} done", id);
        }

        let started = Instant::now();
        if opts.disable_port_reuse {
            for id in providers.keys() {
                let m = sim.machine(*id);
                for (m_id, (peer, _addr)) in consumers.iter() {
                    if *m_id == m_nat {
                        let a_nat = m.select(|e| match e {
                            Event::PeerInfo(p, i) if p == peer => {
                                let a = i.connections[0].clone();
                                a.replace(1, |_| Some(Protocol::Tcp(30000)))
                            }
                            _ => None
                        }).deadline(started, 5).await.unwrap().unwrap();
                        tracing::info!("NAT addr is {}", a_nat);
                        m.select(|e| {
                            matches!(e, Event::PeerInfo(p, i) if p == peer &&
                                i.addresses == hashmap!(a_nat.clone() => "Dial".to_owned())
                            )
                                .then(|| ())
                        })
                        .deadline(started, 10).await.unwrap();
                    } else {
                        m.select(|e| {
                            matches!(e, Event::PeerInfo(p, i) if p == peer && i.addresses.is_empty())
                                .then(|| ())
                        })
                        .deadline(started, 10).await.unwrap();
                    }
                    tracing::info!("provider {} done with {}", id, m_id);
                }
            }
        } else {
            // first wait until we have seen and Identify’ed all incoming connections
            for id in providers.keys() {
                let m = sim.machine(*id);
                for (m_id, (peer, _addr)) in consumers.iter() {
                    let a_1 = m.select(|e| match e {
                        Event::PeerInfo(p, i) if p == peer => Some(i.connections[0].clone()),
                        _ => None
                    }).timeout(1).await.unwrap().unwrap();
                    tracing::info!("first address is {}", a_1);
                    // the NAT may give us the correct port in a_1 already, so no second entry to
                    // check
                    let a_nat = a_1
                        .replace(1, |_| Some(Protocol::Tcp(30000)))
                        .filter(|a| *m_id == m_nat && *a != a_1);
                    m.select(|e| {
                        matches!(e, Event::PeerInfo(p, i) if p == peer && (
                            // port_reuse unfortunately means that the NATed port is added to
                            // listeners by GenTcp, sent via Identify, but not falsifiable because
                            // we can’t attempt to dial while the connection exists
                            i.addresses.get(&a_1).map(|x| x.as_str()) == Some("Candidate") &&
                            a_nat.iter().all(|a_nat| {
                                i.addresses.get(a_nat).map(|x| x.as_str()) == Some("Dial")
                            }))
                        )
                        .then(|| ())
                    })
                    .deadline(started, 5).await.unwrap();
                    tracing::info!("provider {} identified {}", id, m_id);
                }
                m.drain();
            }

            // now disconnect the consumers so that the providers will try to dial and falsify the
            // addresses
            for id in consumers.keys() {
                sim.machine(*id).down();
            }

            let started = Instant::now();
            for id in providers.keys() {
                // first wait until all connections are down
                let m = sim.machine(*id);
                for (m_id, (peer, _addr)) in consumers.iter() {
                    m.select(|e| {
                        matches!(e, Event::Disconnected(p) if p == peer).then(|| ())
                    })
                    .deadline(started, 30).await.unwrap();
                    tracing::info!("provider {} saw close from {}", id, m_id);
                }
                m.drain();
            }

            // and check behaviour
            for id in providers.keys() {
                let m = sim.machine(*id);
                for (m_id, (peer, _addr)) in consumers.iter() {
                    m.send(Command::Dial(*peer));
                    m.select(|e| matches!(e, Event::DialFailure(p, ..) if p == peer).then(|| ()))
                        .timeout(10).await.unwrap();
                    m.send(Command::PrunePeers);
                    m.select(|e| matches!(e, Event::PeerRemoved(p) if p == peer).then(|| ()))
                        .timeout(10).await.unwrap();
                    tracing::info!("provider {} done with {}", id, m_id);
                }
            }
        }

        Ok(())
    })
    .context("netsim")
}

#[cfg(not(target_os = "linux"))]
fn main() {}
