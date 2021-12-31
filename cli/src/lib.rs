use anyhow::Result;
use ed25519_dalek::{PublicKey, SecretKey};
use ipfs_embed::{
    Block, Cid, DefaultParams, Keypair, Multiaddr, PeerId, PeerInfo, StreamId, ToLibp2p,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "ipfs_embed")]
pub struct Config {
    #[structopt(long)]
    pub path: Option<PathBuf>,
    #[structopt(long)]
    pub node_name: Option<String>,
    #[structopt(long)]
    pub keypair: u64,
    #[structopt(long)]
    pub enable_mdns: bool,
    #[structopt(long)]
    pub listen_on: Vec<Multiaddr>,
    #[structopt(long)]
    pub bootstrap: Vec<Multiaddr>,
    #[structopt(long)]
    pub external: Vec<Multiaddr>,
    #[structopt(long)]
    pub disable_port_reuse: bool,
}

impl Config {
    pub fn new(keypair: u64) -> Self {
        Self {
            path: None,
            node_name: None,
            keypair,
            listen_on: vec!["/ip4/0.0.0.0/tcp/30000".parse().unwrap()],
            bootstrap: vec![],
            external: vec![],
            enable_mdns: false,
            disable_port_reuse: false,
        }
    }
}

impl From<Config> for async_process::Command {
    fn from(config: Config) -> Self {
        let ipfs_embed_cli = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .join("ipfs-embed-cli");
        if !ipfs_embed_cli.exists() {
            panic!(
                "failed to find the ipfs-embed-cli binary at {}",
                ipfs_embed_cli.display()
            );
        }
        let mut cmd = Self::new(ipfs_embed_cli);
        if let Some(path) = config.path.as_ref() {
            cmd.arg("--path").arg(path);
        }
        if let Some(node_name) = config.node_name.as_ref() {
            cmd.arg("--node-name").arg(node_name);
        }
        cmd.arg("--keypair").arg(config.keypair.to_string());
        for listen_on in &config.listen_on {
            cmd.arg("--listen-on").arg(listen_on.to_string());
        }
        for bootstrap in &config.bootstrap {
            cmd.arg("--bootstrap").arg(bootstrap.to_string());
        }
        for external in &config.external {
            cmd.arg("--external").arg(external.to_string());
        }
        if config.enable_mdns {
            cmd.arg("--enable-mdns");
        }
        if config.disable_port_reuse {
            cmd.arg("--disable-port-reuse");
        }
        cmd
    }
}

pub fn keypair(i: u64) -> Keypair {
    let mut keypair = [0; 32];
    keypair[..8].copy_from_slice(&i.to_be_bytes());
    let secret = SecretKey::from_bytes(&keypair).unwrap();
    let public = PublicKey::from(&secret);
    Keypair { secret, public }
}

pub fn peer_id(i: u64) -> PeerId {
    keypair(i).to_peer_id()
}

#[derive(Debug, Eq, PartialEq)]
pub enum Command {
    AddAddress(PeerId, Multiaddr),
    Dial(PeerId),
    Get(Cid),
    Insert(Block<DefaultParams>),
    Alias(String, Option<Cid>),
    Flush,
    Sync(Cid),
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::AddAddress(peer, addr) => write!(f, ">add-address {} {}", peer, addr)?,
            Self::Dial(peer) => write!(f, ">dial {}", peer)?,
            Self::Get(cid) => write!(f, ">get {}", cid)?,
            Self::Insert(block) => {
                write!(f, ">insert {} ", block.cid())?;
                for byte in block.data() {
                    write!(f, "{:02x}", byte)?;
                }
            }
            Self::Alias(alias, cid) => {
                write!(f, ">alias {}", alias)?;
                if let Some(cid) = cid.as_ref() {
                    write!(f, " {}", cid)?;
                }
            }
            Self::Flush => write!(f, ">flush")?,
            Self::Sync(cid) => write!(f, ">sync {}", cid)?,
        }
        Ok(())
    }
}

impl std::str::FromStr for Command {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let mut parts = s.split_whitespace();
        Ok(match parts.next() {
            Some(">add-address") => {
                let peer = parts.next().unwrap().parse()?;
                let addr = parts.next().unwrap().parse()?;
                Self::AddAddress(peer, addr)
            }
            Some(">dial") => {
                let peer = parts.next().unwrap().parse()?;
                Self::Dial(peer)
            }
            Some(">get") => {
                let cid = parts.next().unwrap().parse()?;
                Self::Get(cid)
            }
            Some(">insert") => {
                let cid = parts.next().unwrap().parse()?;
                let str_data = parts.next().unwrap();
                let mut data = Vec::with_capacity(str_data.len() / 2);
                for chunk in str_data.as_bytes().chunks(2) {
                    let s = std::str::from_utf8(chunk)?;
                    data.push(u8::from_str_radix(s, 16)?);
                }
                let block = Block::new(cid, data)?;
                Self::Insert(block)
            }
            Some(">alias") => {
                let alias = parts.next().unwrap().to_string();
                let cid = parts.next().map(|s| s.parse()).transpose()?;
                Self::Alias(alias, cid)
            }
            Some(">flush") => Self::Flush,
            Some(">sync") => {
                let cid = parts.next().unwrap().parse()?;
                Self::Sync(cid)
            }
            _ => return Err(anyhow::anyhow!("invalid command `{}`", s)),
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Event {
    NewListener,
    NewListenAddr(Multiaddr),
    ExpiredListenAddr(Multiaddr),
    ListenerClosed,
    NewExternalAddr(Multiaddr),
    ExpiredExternalAddr(Multiaddr),
    Discovered(PeerId),
    Unreachable(PeerId),
    Connected(PeerId),
    Disconnected(PeerId),
    Subscribed(PeerId, String),
    Unsubscribed(PeerId, String),
    Block(Block<DefaultParams>),
    Flushed,
    Synced,
    Bootstrapped,
    NewHead(StreamId, u64),
    PeerInfo(PeerId, PeerInfoIo),
    PeerRemoved(PeerId),
    DialFailure(PeerId, Multiaddr, String),
    ConnectionEstablished(PeerId, Multiaddr),
    ConnectionClosed(PeerId, Multiaddr),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInfoIo {
    pub protocol_version: Option<String>,
    pub agent_version: Option<String>,
    pub protocols: Vec<String>,
    pub listeners: Vec<Multiaddr>,
    pub addresses: HashMap<Multiaddr, String>,
    pub connections: Vec<Multiaddr>,
    pub failures: Vec<String>,
}

impl From<PeerInfo> for PeerInfoIo {
    fn from(info: PeerInfo) -> Self {
        Self {
            protocol_version: info.protocol_version().map(ToOwned::to_owned),
            agent_version: info.agent_version().map(ToOwned::to_owned),
            protocols: info.protocols().map(ToOwned::to_owned).collect(),
            listeners: info.listen_addresses().cloned().collect(),
            addresses: info
                .addresses()
                .map(|(a, s, _dt)| (a.clone(), format!("{:?}", s)))
                .collect(),
            connections: info.connections().map(|(a, ..)| a.clone()).collect(),
            failures: info.recent_failures().map(ToString::to_string).collect(),
        }
    }
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::NewListener => write!(f, "<new-listener")?,
            Self::NewListenAddr(addr) => write!(f, "<new-listen-addr {}", addr)?,
            Self::ExpiredListenAddr(addr) => write!(f, "<expired-listen-addr {}", addr)?,
            Self::ListenerClosed => write!(f, "<listener-closed")?,
            Self::NewExternalAddr(addr) => write!(f, "<new-external-addr {}", addr)?,
            Self::ExpiredExternalAddr(addr) => write!(f, "<expired-external-addr {}", addr)?,
            Self::Discovered(peer) => write!(f, "<discovered {}", peer)?,
            Self::Unreachable(peer) => write!(f, "<unreachable {}", peer)?,
            Self::Connected(peer) => write!(f, "<connected {}", peer)?,
            Self::Disconnected(peer) => write!(f, "<disconnected {}", peer)?,
            Self::Subscribed(peer, topic) => write!(f, "<subscribed {} {}", peer, topic)?,
            Self::Unsubscribed(peer, topic) => write!(f, "<unsubscribed {} {}", peer, topic)?,
            Self::Block(block) => {
                write!(f, "<block {} ", block.cid())?;
                for byte in block.data() {
                    write!(f, "{:02x}", byte)?;
                }
            }
            Self::Flushed => write!(f, "<flushed")?,
            Self::Synced => write!(f, "<synced")?,
            Self::Bootstrapped => write!(f, "<bootstrapped")?,
            Self::NewHead(id, offset) => write!(f, "<newhead {} {}", id, offset)?,
            Self::PeerInfo(p, i) => {
                write!(f, "<peer-info {} {}", p, serde_json::to_string(i).unwrap())?
            }
            Self::PeerRemoved(id) => write!(f, "<peer-removed {}", id)?,
            Self::DialFailure(p, a, e) => write!(f, "<dial-failure {} {} {}", p, a, e)?,
            Self::ConnectionEstablished(p, a) => write!(f, "<connection-established {} {}", p, a)?,
            Self::ConnectionClosed(p, a) => write!(f, "<connection-closed {} {}", p, a)?,
        }
        Ok(())
    }
}

impl std::str::FromStr for Event {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let mut parts = s.split_whitespace();
        Ok(match parts.next() {
            Some("<new-listener") => Self::NewListener,
            Some("<new-listen-addr") => {
                let addr = parts.next().unwrap().parse()?;
                Self::NewListenAddr(addr)
            }
            Some("<expired-listen-addr") => {
                let addr = parts.next().unwrap().parse()?;
                Self::ExpiredListenAddr(addr)
            }
            Some("<listener-closed") => Self::ListenerClosed,
            Some("<new-external-addr") => {
                let addr = parts.next().unwrap().parse()?;
                Self::NewExternalAddr(addr)
            }
            Some("<expired-external-addr") => {
                let addr = parts.next().unwrap().parse()?;
                Self::ExpiredExternalAddr(addr)
            }
            Some("<discovered") => {
                let peer = parts.next().unwrap().parse()?;
                Self::Discovered(peer)
            }
            Some("<unreachable") => {
                let peer = parts.next().unwrap().parse()?;
                Self::Unreachable(peer)
            }
            Some("<connected") => {
                let peer = parts.next().unwrap().parse()?;
                Self::Connected(peer)
            }
            Some("<disconnected") => {
                let peer = parts.next().unwrap().parse()?;
                Self::Disconnected(peer)
            }
            Some("<subscribed") => {
                let peer = parts.next().unwrap().parse()?;
                let topic = parts.next().unwrap().to_string();
                Self::Subscribed(peer, topic)
            }
            Some("<unsubscribed") => {
                let peer = parts.next().unwrap().parse()?;
                let topic = parts.next().unwrap().to_string();
                Self::Unsubscribed(peer, topic)
            }
            Some("<block") => {
                let cid = parts.next().unwrap().parse()?;
                let str_data = parts.next().unwrap();
                let mut data = Vec::with_capacity(str_data.len() / 2);
                for chunk in str_data.as_bytes().chunks(2) {
                    let s = std::str::from_utf8(chunk)?;
                    data.push(u8::from_str_radix(s, 16)?);
                }
                let block = Block::new(cid, data)?;
                Self::Block(block)
            }
            Some("<flushed") => Self::Flushed,
            Some("<synced") => Self::Synced,
            Some("<bootstrapped") => Self::Bootstrapped,
            Some("<newhead") => {
                let id = parts.next().unwrap().parse()?;
                let offset = parts.next().unwrap().parse()?;
                Self::NewHead(id, offset)
            }
            Some("<peer-info") => {
                let id = parts.next().unwrap().parse()?;
                let s = parts.collect::<Vec<_>>().join(" ");
                let info = serde_json::from_str(s.as_str())?;
                Self::PeerInfo(id, info)
            }
            Some("<peer-removed") => {
                let id = parts.next().unwrap().parse()?;
                Self::PeerRemoved(id)
            }
            Some("<dial-failure") => {
                let id = parts.next().unwrap().parse()?;
                let addr = parts.next().unwrap().parse()?;
                let error = parts.collect::<Vec<_>>().join(" ");
                Self::DialFailure(id, addr, error)
            }
            Some("<connection-established") => {
                let id = parts.next().unwrap().parse()?;
                let addr = parts.next().unwrap().parse()?;
                Self::ConnectionEstablished(id, addr)
            }
            Some("<connection-closed") => {
                let id = parts.next().unwrap().parse()?;
                let addr = parts.next().unwrap().parse()?;
                Self::ConnectionClosed(id, addr)
            }
            _ => anyhow::bail!("invalid event `{}`", s),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command() -> Result<()> {
        let command = &[
            Command::AddAddress(
                "12D3KooWHQK1bB6y354a2ydJZMo13kLByVgDLBZdtYNFvc5of4Kd".parse()?,
                "/ip4/10.152.124.10/tcp/42399".parse()?,
            ),
            //Command::Get("".parse()?),
            //Command::Insert(Block::new_unchecked("".parse()?, "hello world")),
            Command::Alias("alias".to_string(), None),
            //Command::Alias("alias".to_string(), Some("".parse()?)),
            Command::Flush,
            //Command::Sync("".parse()?),
        ];
        for cmd in command.iter() {
            let cmd2: Command = cmd.to_string().parse()?;
            assert_eq!(cmd, &cmd2);
        }
        Ok(())
    }

    #[test]
    fn test_event() -> Result<()> {
        let event = &[
            Event::NewListenAddr("/ip4/10.152.124.10/tcp/42399".parse()?),
            //Event::Block(Block::new_unchecked("".parse()?, "hello world")),
            Event::Flushed,
            Event::Synced,
        ];
        for ev in event.iter() {
            let ev2: Event = ev.to_string().parse()?;
            assert_eq!(ev, &ev2);
        }
        Ok(())
    }
}
