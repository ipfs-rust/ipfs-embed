#[cfg(target_os = "linux")]
fn main() -> anyhow::Result<()> {
    use ipfs_embed_cli::{Command, Config, Event};
    use ipfs_embed_harness::{MachineExt, MultiaddrExt};
    use libp2p::multiaddr::Protocol;
    use netsim_embed::{DelayBuffer, Ipv4Range, Netsim};
    use std::net::Ipv4Addr;
    use std::time::Duration;

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    netsim_embed::unshare_user()?;
    async_global_executor::block_on(async move {
        let mut sim = Netsim::new();
        let net_a = sim.spawn_network(Ipv4Range::new([1, 1, 1, 0].into(), 24));
        let net_b = sim.spawn_network(Ipv4Range::random_local_subnet());
        let net_c = sim.spawn_network(Ipv4Range::random_local_subnet());

        sim.add_nat_route(Default::default(), net_a, net_b);
        sim.add_nat_route(Default::default(), net_a, net_c);

        let mut config = Config::new(0);
        config.enable_relay = true;
        config.listen_on = vec!["/ip4/1.1.1.42/tcp/30000".parse()?];
        config.external = vec!["/ip4/1.1.1.42/tcp/30000".parse()?];
        let a = sim.spawn_machine(config.into(), None).await;
        sim.plug(a, net_a, Some(Ipv4Addr::new(1, 1, 1, 42))).await;

        loop {
            if let Some(Event::NewListenAddr(_addr)) = sim.machine(a).recv().await {
                break;
            }
        }
        tracing::info!("relay ready");

        let addr = sim.machine(a).multiaddr()
            .with(Protocol::P2p(sim.machine(a).peer_id().into()))
            .with(Protocol::P2pCircuit);

        let mut wire = DelayBuffer::new();
        wire.set_delay(Duration::from_millis(100));
        let mut config = Config::new(1);
        config.listen_on.push(addr.clone());
        let b = sim.spawn_machine(config.into(), Some(wire)).await;
        sim.plug(b, net_b, None).await;
        let peer_id = sim.machine(b).peer_id();

        loop {
            if let Some(Event::NewListenAddr(addr)) = sim.machine(b).recv().await {
                if addr.is_relay() {
                    break;
                }
            }
        }
        tracing::info!("listening on relayed address");

        let mut wire = DelayBuffer::new();
        wire.set_delay(Duration::from_millis(100));
        let c = sim.spawn_machine(Config::new(2).into(), Some(wire)).await;
        sim.plug(c, net_c, None).await;

        loop {
            if let Some(Event::NewListenAddr(addr)) = sim.machine(c).recv().await {
                if !addr.is_loopback() && addr.is_tcp() {
                    break;
                }
            }
        }

        sim.machine(c).send(Command::AddAddress(peer_id, addr));
        sim.machine(c).send(Command::Dial(peer_id));

        loop {
            match sim.machine(c).recv().await {
                Some(Event::Connected(id)) => {
                    if id == peer_id {
                        tracing::info!("connected to relayed address");
                        break;
                    }
                }
                Some(Event::Unreachable(id)) => {
                    if id == peer_id {
                        return Err(anyhow::anyhow!("relay failed"));
                    }
                }
                _ => {}
            }
        }

        let peer_id = sim.machine(c).peer_id();
        loop {
            match sim.machine(b).recv().await {
                Some(Event::Connected(id)) => {
                    if id == peer_id {
                        break;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    })
}

#[cfg(not(target_os = "linux"))]
fn main() {}
