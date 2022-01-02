use anyhow::Result;
use async_std::stream::StreamExt;
use ipfs_embed::{DefaultParams, Ipfs, NetworkConfig, StorageConfig};
use ipfs_embed_cli::{keypair, Command, Config, Event};
use parking_lot::Mutex;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use structopt::StructOpt;
use tracing_subscriber::fmt::format::FmtSpan;

#[async_std::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(FmtSpan::ACTIVE | FmtSpan::CLOSE)
        .init();
    if let Err(err) = run().await {
        tracing::error!("{}", err);
    }
}

async fn run() -> Result<()> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut line = String::with_capacity(4096);

    let config = Config::from_args();
    let sweep_interval = Duration::from_millis(10000);
    let storage = StorageConfig::new(config.path, None, 10, sweep_interval);

    let mut network = NetworkConfig {
        node_key: keypair(config.keypair),
        mdns: if config.enable_mdns {
            Some(Default::default())
        } else {
            None
        },
        kad: None,
        port_reuse: !config.disable_port_reuse,
        ..Default::default()
    };
    let node_name = if let Some(node_name) = config.node_name {
        node_name
    } else {
        format!("node{}", config.keypair)
    };
    network.identify.as_mut().unwrap().agent_version = node_name;

    let ipfs = Ipfs::<DefaultParams>::new(ipfs_embed::Config { storage, network }).await?;
    let mut events = ipfs.swarm_events();

    for addr in config.listen_on {
        let _ = ipfs.listen_on(addr)?;
    }

    for addr in config.external {
        ipfs.add_external_address(addr);
    }

    if !config.bootstrap.is_empty() {
        //ipfs.bootstrap(config.bootstrap)
        unimplemented!()
    }

    let ipfs = Arc::new(Mutex::new(ipfs));
    let ipfs2 = ipfs.clone();

    async_std::task::spawn(async move {
        while let Some(event) = events.next().await {
            let event = match event {
                ipfs_embed::Event::NewListener(_) => Some(Event::NewListener),
                ipfs_embed::Event::NewListenAddr(_, addr) => Some(Event::NewListenAddr(addr)),
                ipfs_embed::Event::ExpiredListenAddr(_, addr) => {
                    Some(Event::ExpiredListenAddr(addr))
                }
                ipfs_embed::Event::ListenerClosed(_) => Some(Event::ListenerClosed),
                ipfs_embed::Event::NewExternalAddr(addr) => Some(Event::NewExternalAddr(addr)),
                ipfs_embed::Event::ExpiredExternalAddr(addr) => {
                    Some(Event::ExpiredExternalAddr(addr))
                }
                ipfs_embed::Event::Discovered(peer_id) => Some(Event::Discovered(peer_id)),
                ipfs_embed::Event::Unreachable(peer_id) => Some(Event::Unreachable(peer_id)),
                ipfs_embed::Event::Connected(peer_id) => Some(Event::Connected(peer_id)),
                ipfs_embed::Event::Disconnected(peer_id) => Some(Event::Disconnected(peer_id)),
                ipfs_embed::Event::Subscribed(peer_id, topic) => {
                    Some(Event::Subscribed(peer_id, topic))
                }
                ipfs_embed::Event::Unsubscribed(peer_id, topic) => {
                    Some(Event::Unsubscribed(peer_id, topic))
                }
                ipfs_embed::Event::Bootstrapped => Some(Event::Bootstrapped),
                ipfs_embed::Event::NewHead(head) => Some(Event::NewHead(*head.id(), head.len())),
                ipfs_embed::Event::NewInfo(peer) => match ipfs2.lock().peer_info(&peer) {
                    Some(info) => Some(Event::PeerInfo(peer, info.into())),
                    None => Some(Event::PeerRemoved(peer)),
                },
                ipfs_embed::Event::ListenerError(_, _) => None,
                ipfs_embed::Event::DialFailure(p, a, e) => Some(Event::DialFailure(p, a, e)),
                ipfs_embed::Event::ConnectionEstablished(p, a) => Some(
                    Event::ConnectionEstablished(p, a.get_remote_address().clone()),
                ),
                ipfs_embed::Event::ConnectionClosed(p, a) => {
                    Some(Event::ConnectionClosed(p, a.get_remote_address().clone()))
                }
                ipfs_embed::Event::AddressChanged(_, _, _) => None,
            };
            if let Some(event) = event {
                println!("{}", event);
            }
        }
    });

    loop {
        line.clear();
        stdin.read_line(&mut line)?;
        match line.parse()? {
            Command::AddAddress(peer, addr) => {
                ipfs.lock().add_address(&peer, addr);
            }
            Command::Dial(peer) => {
                ipfs.lock().dial(&peer);
            }
            Command::Get(cid) => {
                let block = ipfs.lock().get(&cid)?;
                writeln!(stdout, "{}", Event::Block(block))?;
            }
            Command::Insert(block) => {
                ipfs.lock().insert(&block)?;
            }
            Command::Alias(alias, cid) => {
                ipfs.lock().alias(&alias, cid.as_ref())?;
            }
            Command::Flush => {
                ipfs.lock().flush().await?;
                writeln!(stdout, "{}", Event::Flushed)?;
            }
            Command::Sync(cid) => {
                let providers = ipfs.lock().peers();
                tracing::debug!("sync {} from {:?}", cid, providers);
                ipfs.lock().sync(&cid, providers).await?;
                writeln!(stdout, "{}", Event::Synced)?;
            }
        }
    }
}
