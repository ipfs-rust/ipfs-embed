use anyhow::Result;
use futures::prelude::*;
use ipfs_embed::{Config, DefaultParams, Ipfs, NetworkConfig, StorageConfig};
use libipld::{alias, cbor::DagCborCodec, multihash::Code, Block, Cid, DagCbor};
use rand::RngCore;
use std::time::{Instant, Duration};
use std::path::PathBuf;
use structopt::StructOpt;
use tempdir::TempDir;

#[derive(Debug, StructOpt)]
#[structopt(name = "bench")]
struct BenchOpts {
    #[structopt(long, default_value = "1")]
    n_providers: usize,

    #[structopt(long, default_value = "2")]
    n_nodes: usize,

    #[structopt(long, default_value = "0")]
    n_spam: usize,

    #[structopt(long)]
    in_memory: bool,
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

fn tracing_try_init() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();
}

async fn create_store(path: Option<PathBuf>, enable_mdns: bool) -> Result<Ipfs<DefaultParams>> {
    let sweep_interval = Duration::from_millis(10000);
    let storage = StorageConfig::new(path, 10, sweep_interval);

    let mut network = NetworkConfig::new();
    network.enable_mdns = enable_mdns;
    network.allow_non_globals_in_dht = true;
    // network.enable_kad = false;
    network.bitswap.receive_limit = std::num::NonZeroU16::new(u16::MAX).unwrap();

    let ipfs = Ipfs::new(Config { storage, network }).await?;
    ipfs.listen_on("/ip4/127.0.0.1/tcp/0".parse()?).await?;
    Ok(ipfs)
}

/// Parametrized test for sync
///
/// `name` name of the test
/// `n_nodes` total number of nodes to participate in the test. Must be >= 2
/// `n_providers` total number of providers for the data to be synced. Must be >= 1
/// `n_spam` number of times the test data will be repeated as passive data on all nodes
/// `create_test_data` generator for test data
async fn run_test(
    name: &str,
    n_nodes: usize,
    n_providers: usize,
    n_spam: usize,
    on_disk: bool,
    create_test_data: impl Fn() -> Result<(Cid, Vec<Block<DefaultParams>>)>,
) -> Result<()> {
    assert!(n_nodes >= 2);
    assert!(n_providers >= 1);
    assert!(n_providers < n_nodes);

    let temp_dir = if on_disk {
        let res = TempDir::new(name)?;
        println!("creating test database on disk in {}", res.path().display());
        Some(res)
    } else {
        println!("creating test database in memory");
        None
    };

    // create n_nodes nodes. They will find each other via mdns
    let mut nodes = Vec::new();
    for i in 0..n_nodes {
        let path = temp_dir.as_ref().map(|dir| dir.path().join(&format!("node-{}.sqlite", i)));
        nodes.push(create_store(path, true).await?);
    }

    // create some blocks in each node that will not participate in the sync
    if n_spam > 0 {
        println!("creating spam data");
    }
    for i in 0..n_spam {
        let alias = format!("passive-{}", i);
        let (cid, blocks) = create_test_data()?;
        for node in &nodes {
            node.alias(&alias, Some(&cid))?;
            for block in blocks.iter() {
                let _ = node.insert(block)?.await;
            }
            node.flush().await?;
        }
    }

    // create the blocks to be synced in n_providers nodes
    println!("creating test data");
    let root = alias!(root);
    let (cid, blocks) = create_test_data()?;
    for i in 0..n_providers {
        let node = &nodes[i];
        node.alias(root, Some(&cid))?;
        for block in blocks.iter() {
            let _ = node.insert(block)?.await;
        }
        node.flush().await?;
    }

    // wait until all nodes are connected
    while nodes.iter().any(|node| node.connections().len() < n_nodes -1) {
        std::thread::sleep(Duration::from_secs(1));
    }
    println!("nodes fully connected");

    // compute total size of data to be synced
    let size: usize = blocks.iter().map(|block| block.data().len()).sum();
    tracing::info!("test data built {} blocks, {} bytes", blocks.len(), size);

    let target = || nodes.last().unwrap();

    target().alias(root, Some(&cid))?;
    let t0 = Instant::now();
    let _ = target()
        .sync(&cid)
        .for_each(|x| async move { tracing::debug!("sync progress {:?}", x) })
        .await;
    target().flush().await?;
    println!(
        "{}, tree sync complete {} ms {} blocks {} bytes!",
        name,
        t0.elapsed().as_millis(),
        blocks.len(),
        size
    );
    // check that data is indeed synced
    for block in blocks {
        let data = target().get(block.cid())?;
        assert_eq!(data, block);
    }
    Ok(())
}

#[async_std::main]
async fn main() -> Result<()> {
    tracing_try_init();
    let opt = BenchOpts::from_args();
    run_test("2 nodes", opt.n_nodes, opt.n_providers, opt.n_spam, !opt.in_memory, || build_tree(10, 4)).await
}
