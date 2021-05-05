use anyhow::Result;
use async_std::stream::StreamExt;
use ipfs_embed::multiaddr::Protocol;
use ipfs_embed::{DefaultParams, Ipfs, ListenerEvent};
use ipfs_embed_cli::{Command, Config, Event};
use std::io::Write;
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

    let ipfs = Ipfs::<DefaultParams>::new(Config::from_args().into()).await?;
    let peer_id = ipfs.local_peer_id();

    loop {
        line.clear();
        stdin.read_line(&mut line)?;
        match line.parse()? {
            Command::ListenOn(addr) => {
                let mut listener = ipfs.listen_on(addr)?;
                let addr = loop {
                    match listener.next().await {
                        Some(ListenerEvent::NewListenAddr(addr)) => {
                            if let Some(Protocol::Ip4(ip)) = addr.iter().next() {
                                if !ip.is_loopback() {
                                    break addr;
                                }
                            }
                        }
                        _ => return Err(anyhow::anyhow!("failed to bind address")),
                    }
                };
                writeln!(stdout, "{}", Event::ListeningOn(peer_id, addr))?;
            }
            Command::DialAddress(peer, addr) => {
                if peer != peer_id {
                    ipfs.dial_address(&peer, addr);
                }
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
            Command::Exit => {
                break;
            }
        }
    }
    Ok(())
}
