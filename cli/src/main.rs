use anyhow::Result;
use async_std::stream::StreamExt;
use ipfs_embed::{DefaultParams, Ipfs, NetworkConfig, StorageConfig};
use ipfs_embed_cli::{keypair, Command, Config, Event};
use std::io::Write;
use std::time::Duration;
use structopt::StructOpt;

#[async_std::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
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
                ipfs.add_address(&peer, addr);
            }
            Command::Dial(peer) => {
                ipfs.dial(&peer);
            }
            Command::Get(cid) => {
                let block = ipfs.get(&cid)?;
                writeln!(stdout, "{}", Event::Block(block))?;
            }
            Command::Insert(block) => {
                ipfs.insert(&block)?;
            }
            Command::Alias(alias, cid) => {
                ipfs.alias(&alias, cid.as_ref())?;
            }
            Command::Flush => {
                ipfs.flush().await?;
                writeln!(stdout, "{}", Event::Flushed)?;
            }
            Command::Sync(cid) => {
                ipfs.sync(&cid, ipfs.peers()).await?;
                writeln!(stdout, "{}", Event::Synced)?;
            }
        }
    }
}
