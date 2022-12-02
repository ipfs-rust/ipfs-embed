#[cfg(target_os = "linux")]
fn main() -> anyhow::Result<()> {
    use futures::FutureExt;
    use harness::{MachineExt, MultiaddrExt, MyFutureExt};
    use ipfs_embed_cli::{Command, Config, Event};
    use netsim_embed::{DelayBuffer, Ipv4Range, Netsim};
    use std::time::{Duration, Instant};

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    harness::build_bin()?;
    netsim_embed::unshare_user()?;

    async_global_executor::block_on(
        async move {
            let mut sim = Netsim::new();
            let net = sim.spawn_network(Ipv4Range::random_local_subnet());

            let mut wire = DelayBuffer::new();
            wire.set_delay(Duration::from_millis(100));
            let a = sim.spawn_machine(Config::new(0).into(), Some(wire)).await;
            sim.plug(a, net, None).await;

            let mut wire = DelayBuffer::new();
            wire.set_delay(Duration::from_millis(100));
            let b = sim.spawn_machine(Config::new(1).into(), Some(wire)).await;
            sim.plug(b, net, None).await;

            let ms = sim.machines_mut();
            let (a, ms) = ms.split_at_mut(1);
            let a = &mut a[0];
            let (b, _) = ms.split_at_mut(1);
            let b = &mut b[0];
            let a_id = a.peer_id();
            let a_addr = a.multiaddr();
            let b_id = b.peer_id();
            let b_addr = b.multiaddr();

            loop {
                if let Some(Event::NewListenAddr(addr)) = a.recv().await {
                    if !addr.is_loopback() {
                        break;
                    }
                }
            }

            loop {
                if let Some(Event::NewListenAddr(addr)) = b.recv().await {
                    if !addr.is_loopback() {
                        break;
                    }
                }
            }

            a.send(Command::AddAddress(b_id, b_addr));
            b.send(Command::AddAddress(a_id, a_addr));
            a.send(Command::Dial(b_id));
            b.send(Command::Dial(a_id));

            let now = Instant::now();
            loop {
                match a.recv().deadline(now, 15).await.unwrap() {
                    Some(Event::Connected(id)) => {
                        if id == b_id {
                            break;
                        }
                    }
                    Some(Event::Unreachable(id)) => {
                        if id == b_id {
                            return Err(anyhow::anyhow!("sim open failed"));
                        }
                    }
                    _ => {}
                }
            }
            loop {
                match b.recv().deadline(now, 15).await.unwrap() {
                    Some(Event::Connected(id)) => {
                        if id == a_id {
                            break;
                        }
                    }
                    Some(Event::Unreachable(id)) => {
                        if id == a_id {
                            return Err(anyhow::anyhow!("sim open failed"));
                        }
                    }
                    _ => {}
                }
            }
            Ok(())
        }
        .timeout(120)
        .map(|r| r.unwrap_or_else(|e| Err(e.into()))),
    )
}

#[cfg(not(target_os = "linux"))]
fn main() {}
