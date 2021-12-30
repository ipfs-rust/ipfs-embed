#![cfg(target_os = "linux")]

use anyhow::{Context, Result};
use futures::prelude::*;
use ipfs_embed_cli::{Command, Config, Event};
use libipld::cbor::DagCborCodec;
use libipld::multihash::Code;
use libipld::{Block, Cid, DagCbor, DefaultParams};
use libp2p::multiaddr::Protocol;
use libp2p::{multiaddr, Multiaddr, PeerId};
use netsim_embed::{DelayBuffer, Ipv4Range, MachineId, Netsim, NetworkId};
use rand::RngCore;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::ops::Range;
use std::str::FromStr;
use std::time::Duration;
use structopt::StructOpt;
use tempdir::TempDir;

#[derive(StructOpt)]
pub struct HarnessOpts {
    #[structopt(long, default_value = "2")]
    pub n_nodes: usize,

    #[structopt(long, default_value = "1")]
    pub n_providers: usize,

    #[structopt(long, default_value = "1")]
    pub n_consumers: usize,

    #[structopt(long, default_value = "0")]
    pub n_spam: usize,

    #[structopt(long, default_value = "0")]
    pub delay_ms: u64,

    #[structopt(long)]
    pub enable_mdns: bool,

    #[structopt(long, default_value = "10")]
    pub tree_width: u64,

    #[structopt(long, default_value = "4")]
    pub tree_depth: u64,

    #[structopt(long)]
    pub disable_port_reuse: bool,
}

pub trait MachineExt {
    fn peer_id(&self) -> PeerId;
    fn multiaddr(&self) -> Multiaddr;
}

impl<C, E> MachineExt for netsim_embed::Machine<C, E> {
    fn peer_id(&self) -> PeerId {
        ipfs_embed_cli::peer_id(self.id().0 as u64)
    }

    fn multiaddr(&self) -> Multiaddr {
        format!("/ip4/{}/tcp/30000", self.addr()).parse().unwrap()
    }
}

pub trait MultiaddrExt {
    fn is_loopback(&self) -> bool;
}

impl MultiaddrExt for Multiaddr {
    fn is_loopback(&self) -> bool {
        if let Some(multiaddr::Protocol::Ip4(addr)) = self.iter().next() {
            if !addr.is_loopback() {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Provider,
    Consumer,
    Idle,
}
pub trait NetsimExt {
    fn role(&self, opts: &HarnessOpts, r: Role) -> HashMap<MachineId, (PeerId, Multiaddr)>;
    fn nodes(&self, range: Range<usize>) -> HashMap<MachineId, (PeerId, Multiaddr)>;
}
impl<C, E> NetsimExt for Netsim<C, E>
where
    C: Display + Send + 'static,
    E: FromStr + Display + Send + 'static,
    E::Err: Display + Debug + Send + Sync,
{
    fn role(&self, opts: &HarnessOpts, r: Role) -> HashMap<MachineId, (PeerId, Multiaddr)> {
        let range = match r {
            Role::Provider => 0..opts.n_providers,
            Role::Consumer => opts.n_providers..(opts.n_providers + opts.n_consumers),
            Role::Idle => (opts.n_providers + opts.n_consumers)..opts.n_nodes,
        };
        self.nodes(range)
    }
    fn nodes(&self, range: Range<usize>) -> HashMap<MachineId, (PeerId, Multiaddr)> {
        self.machines()[range]
            .iter()
            .map(|m| {
                let peer = m.peer_id();
                let mut addr = m.multiaddr();
                addr.push(Protocol::P2p(peer.into()));
                (m.id(), (peer, addr))
            })
            .collect()
    }
}

pub fn run_netsim<F, F2>(mut f: F) -> Result<()>
where
    F: FnMut(Netsim<Command, Event>, HarnessOpts, NetworkId, TempDir) -> F2,
    F2: Future<Output = Result<()>>,
{
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    netsim_embed::unshare_user().context("unshare_user")?;
    let opts = HarnessOpts::from_args();
    let temp_dir = TempDir::new("ipfs-embed-harness")?;
    async_global_executor::block_on(async move {
        let mut sim = Netsim::new();
        let net = sim.spawn_network(Ipv4Range::random_local_subnet());
        tracing::warn!("using network {:?}", sim.network(net).range());
        for i in 0..opts.n_nodes {
            let name = if i < opts.n_providers {
                format!("provider{}", i)
            } else if i < opts.n_providers + opts.n_consumers {
                format!("consumer{}", i - opts.n_providers)
            } else {
                format!("idle{}", i - opts.n_providers - opts.n_consumers)
            };
            let cfg = Config {
                path: Some(temp_dir.path().join(i.to_string())),
                node_name: Some(name),
                keypair: i as _,
                listen_on: vec!["/ip4/0.0.0.0/tcp/30000".parse().unwrap()],
                bootstrap: vec![],
                external: vec![],
                enable_mdns: opts.enable_mdns,
                disable_port_reuse: opts.disable_port_reuse,
            };
            let mut delay = DelayBuffer::new();
            delay.set_delay(Duration::from_millis(opts.delay_ms));
            let cmd = async_process::Command::from(cfg);
            let machine = sim.spawn_machine(cmd, Some(delay)).await;
            sim.plug(machine, net, None).await;
            let m = sim.machine(machine);
            tracing::warn!(
                "{} started with address {} and peer id {}",
                machine,
                m.addr(),
                m.peer_id(),
            );
        }
        f(sim, opts, net, temp_dir).await
    })
}

#[derive(Debug, DagCbor)]
pub struct Node {
    pub links: Vec<Cid>,
    pub depth: u64,
    pub payload: Box<[u8]>,
}

fn create_block(links: Vec<Cid>, depth: u64) -> Result<Block<DefaultParams>> {
    let payload = if links.is_empty() {
        let mut payload = [0u8; 1024 * 16];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut payload);
        payload.to_vec().into_boxed_slice()
    } else {
        let mut payload = [0u8; 512];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut payload);
        payload.to_vec().into_boxed_slice()
    };
    let node = Node {
        links,
        depth,
        payload,
    };
    Block::encode(DagCborCodec, Code::Blake3_256, &node)
}

fn build_tree_0(width: u64, depth: u64, blocks: &mut Vec<Block<DefaultParams>>) -> Result<Cid> {
    let links = if depth == 0 {
        vec![]
    } else {
        let mut links = Vec::with_capacity(width as usize);
        for _ in 0..width {
            let cid = build_tree_0(width, depth - 1, blocks)?;
            links.push(cid);
        }
        links
    };
    let block = create_block(links, depth)?;
    let cid = *block.cid();
    blocks.push(block);
    Ok(cid)
}

pub fn build_tree(width: u64, depth: u64) -> Result<(Cid, Vec<Block<DefaultParams>>)> {
    let mut blocks = vec![];
    let cid = build_tree_0(width, depth, &mut blocks).context("build_tree")?;
    Ok((cid, blocks))
}

pub fn build_bin() -> Result<()> {
    use escargot::CargoBuild;

    for msg in CargoBuild::new()
        .manifest_path("cli/Cargo.toml")
        .bin("ipfs-embed-cli")
        .current_release()
        .exec()?
    {
        match msg?.decode()? {
            escargot::format::Message::BuildFinished(x) => eprintln!("{:?}", x),
            escargot::format::Message::CompilerArtifact(x) => {
                if !x.fresh {
                    eprintln!("{:?}", x.package_id);
                }
            }
            escargot::format::Message::CompilerMessage(x) => {
                if let Some(msg) = x.message.rendered {
                    eprintln!("{}", msg);
                }
            }
            escargot::format::Message::BuildScriptExecuted(_) => {}
            escargot::format::Message::Unknown => {}
        }
    }
    Ok(())
}
