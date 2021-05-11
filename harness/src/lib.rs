#![cfg(target_os = "linux")]

use anyhow::Result;
use futures::prelude::*;
use ipfs_embed_cli::{Command, Event};
use libipld::cbor::DagCborCodec;
use libipld::multihash::Code;
use libipld::{Block, Cid, DagCbor, DefaultParams};
use netsim_embed::{Ipv4Range, Network, NetworkBuilder, Wire};
use rand::RngCore;
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
}

pub fn run_netsim<F, F2>(mut f: F) -> Result<()>
where
    F: FnMut(Network<Command, Event>, HarnessOpts) -> F2,
    F2: Future<Output = Network<Command, Event>> + Send,
{
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let opts = HarnessOpts::from_args();
    let ipfs_embed_cli = std::env::current_exe()?
        .parent()
        .unwrap()
        .join("ipfs-embed-cli");
    if !ipfs_embed_cli.exists() {
        return Err(anyhow::anyhow!(
            "failed to find the ipfs-embed-cli binary at {}",
            ipfs_embed_cli.display()
        ));
    }
    let temp_dir = TempDir::new("ipfs-embed-harness")?;
    netsim_embed::namespace::unshare_user()?;
    async_global_executor::block_on(async move {
        let mut builder = NetworkBuilder::new(Ipv4Range::random_local_subnet());
        for i in 0..opts.n_nodes {
            let ipfs_embed_cli = ipfs_embed_cli.clone();
            let path = temp_dir.path().join(i.to_string());
            let mut wire = Wire::new();
            wire.set_delay(Duration::from_millis(opts.delay_ms));
            let mut cmd = async_process::Command::new(ipfs_embed_cli);
            cmd.arg("--path").arg(path);
            let name = if i < opts.n_providers {
                format!("provider{}", i)
            } else if i < opts.n_providers + opts.n_consumers {
                format!("consumer{}", i - opts.n_providers)
            } else {
                format!("idle{}", i - opts.n_providers - opts.n_consumers)
            };
            cmd.arg("--node-name").arg(name);
            if opts.enable_mdns {
                cmd.arg("--enable-mdns");
            }
            builder.spawn_machine_with_command(wire, cmd);
        }
        let network = builder.spawn();
        let mut network = f(network, opts).await;
        for machine in network.machines_mut() {
            machine.send(Command::Exit).await;
        }
    });
    Ok(())
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
    let cid = build_tree_0(width, depth, &mut blocks)?;
    Ok((cid, blocks))
}
