use anyhow::Result;
use ipfs_embed::{Block, Cid, DefaultParams, Multiaddr, NetworkConfig, PeerId, StorageConfig};
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "ipfs_embed")]
pub struct Config {
    #[structopt(long)]
    path: Option<PathBuf>,

    #[structopt(long)]
    node_name: String,
}

impl From<Config> for ipfs_embed::Config {
    fn from(config: Config) -> Self {
        let sweep_interval = Duration::from_millis(10000);
        let storage = StorageConfig::new(config.path, 10, sweep_interval);

        let mut network = NetworkConfig {
            mdns: None,
            kad: None,
            ..Default::default()
        };
        network.identify.as_mut().unwrap().agent_version = config.node_name;

        Self { storage, network }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Command {
    ListenOn(Multiaddr),
    DialAddress(PeerId, Multiaddr),
    Get(Cid),
    Insert(Block<DefaultParams>),
    Alias(String, Option<Cid>),
    Flush,
    Sync(Cid),
    Exit,
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ListenOn(addr) => write!(f, ">listen_on {}", addr)?,
            Self::DialAddress(peer, addr) => write!(f, ">dial_address {} {}", peer, addr)?,
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
            Self::Exit => write!(f, ">exit")?,
        }
        Ok(())
    }
}

impl std::str::FromStr for Command {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let mut parts = s.split_whitespace();
        Ok(match parts.next() {
            Some(">listen_on") => {
                let addr = parts.next().unwrap().parse()?;
                Self::ListenOn(addr)
            }
            Some(">dial_address") => {
                let peer = parts.next().unwrap().parse()?;
                let addr = parts.next().unwrap().parse()?;
                Self::DialAddress(peer, addr)
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
            Some(">exit") => Self::Exit,
            _ => return Err(anyhow::anyhow!("invalid command `{}`", s)),
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Event {
    ListeningOn(PeerId, Multiaddr),
    Block(Block<DefaultParams>),
    Flushed,
    Synced,
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ListeningOn(peer, addr) => write!(f, "<listening_on {} {}", peer, addr)?,
            Self::Block(block) => {
                write!(f, "<block {} ", block.cid())?;
                for byte in block.data() {
                    write!(f, "{:02x}", byte)?;
                }
            }
            Self::Flushed => write!(f, "<flushed")?,
            Self::Synced => write!(f, "<synced")?,
        }
        Ok(())
    }
}

impl std::str::FromStr for Event {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let mut parts = s.split_whitespace();
        Ok(match parts.next() {
            Some("<listening_on") => {
                let peer = parts.next().unwrap().parse()?;
                let addr = parts.next().unwrap().parse()?;
                Self::ListeningOn(peer, addr)
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
            _ => return Err(anyhow::anyhow!("invalid event `{}`", s)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command() -> Result<()> {
        let command = &[
            Command::ListenOn("/ip4/0.0.0.0/tcp/0".parse()?),
            Command::DialAddress(
                "12D3KooWHQK1bB6y354a2ydJZMo13kLByVgDLBZdtYNFvc5of4Kd".parse()?,
                "/ip4/10.152.124.10/tcp/42399".parse()?,
            ),
            //Command::Get("".parse()?),
            //Command::Insert(Block::new_unchecked("".parse()?, "hello world")),
            Command::Alias("alias".to_string(), None),
            //Command::Alias("alias".to_string(), Some("".parse()?)),
            Command::Flush,
            //Command::Sync("".parse()?),
            Command::Exit,
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
            Event::ListeningOn(
                "12D3KooWHQK1bB6y354a2ydJZMo13kLByVgDLBZdtYNFvc5of4Kd".parse()?,
                "/ip4/10.152.124.10/tcp/42399".parse()?,
            ),
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
