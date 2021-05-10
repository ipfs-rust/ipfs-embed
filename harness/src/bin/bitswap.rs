use anyhow::Result;
use async_process::Stdio;
use futures::channel::mpsc;
use futures::prelude::*;
use futures_lite::io::BufReader;
use ipfs_embed_cli::{Command, Event};
use libipld::{alias, cbor::DagCborCodec, multihash::Code, Block, Cid, DagCbor, DefaultParams};
use netsim_embed::{Ipv4Range, NetworkBuilder, Wire};
use rand::RngCore;
use std::io::Write;
use std::time::{Duration, Instant};
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

    #[structopt(long, default_value = "0")]
    delay_ms: u64,
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
    delay: Duration,
    create_test_data: impl Fn() -> Result<(Cid, Vec<Block<DefaultParams>>)>,
) -> Result<()> {
    assert!(n_nodes >= 2);
    assert!(n_providers >= 1);
    assert!(n_providers < n_nodes);

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

    let temp_dir = if on_disk {
        let res = TempDir::new(name)?;
        println!("creating test database on disk in {}", res.path().display());
        Some(res)
    } else {
        println!("creating test database in memory");
        None
    };
    let path = temp_dir.as_ref().map(|d| d.path().to_owned());

    let mut builder = NetworkBuilder::new(Ipv4Range::random_local_subnet());
    for i in 0..n_nodes {
        let path = path.clone();
        let ipfs_embed_cli = ipfs_embed_cli.clone();
        let mut wire = Wire::new();
        wire.set_delay(delay);
        builder.spawn_machine(
            wire,
            move |mut cmd: mpsc::UnboundedReceiver<Command>,
                  mut event: mpsc::UnboundedSender<Event>| async move {
                let node_name = format!("node-{}", i);
                let path = path.map(|path| path.join(&node_name));
                let mut c = async_process::Command::new(ipfs_embed_cli);
                c.stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .arg("--node-name")
                    .arg(node_name);
                if let Some(path) = path {
                    c.arg("--path").arg(path);
                }
                let mut child = c.spawn().unwrap();
                let mut stdout = BufReader::new(child.stdout.take().unwrap()).lines().fuse();
                let mut stdin = child.stdin.unwrap();
                let mut buf = Vec::with_capacity(4096);
                loop {
                    futures::select! {
                        cmd = cmd.next() => {
                            if let Some(cmd) = cmd {
                                buf.clear();
                                writeln!(buf, "{}", cmd).unwrap();
                                stdin.write_all(&buf).await.unwrap();
                            } else {
                                break;
                            }
                        }
                        ev = stdout.next() => {
                            if let Some(ev) = ev {
                                let ev = ev.unwrap();
                                if ev.starts_with('<') {
                                    event.send(ev.parse().unwrap()).await.unwrap();
                                } else {
                                    println!("{}", ev);
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
            },
        );
    }
    let mut network = builder.spawn();
    let mut peers = Vec::with_capacity(n_nodes);
    for i in 0..n_nodes {
        let node = network.machine(i);
        node.send(Command::ListenOn("/ip4/0.0.0.0/tcp/0".parse()?))
            .await;
        if let Some(Event::ListeningOn(peer, addr)) = node.recv().await {
            peers.push((peer, addr));
        } else {
            unreachable!();
        }
    }

    for i in 0..n_nodes {
        let node = network.machine(i);
        for (peer, addr) in &peers {
            node.send(Command::DialAddress(*peer, addr.clone())).await;
        }
    }

    // create some blocks in each node that will not participate in the sync
    if n_spam > 0 {
        println!("creating spam data");
    }
    for i in 0..n_spam {
        let alias = format!("passive-{}", i);
        let (cid, blocks) = create_test_data()?;
        for i in 0..n_nodes {
            let node = network.machine(i);
            node.send(Command::Alias(alias.clone(), Some(cid))).await;
            for block in blocks.iter().rev() {
                node.send(Command::Insert(block.clone())).await;
            }
        }
    }

    // create the blocks to be synced in n_providers nodes
    println!("creating test data");
    let root = alias!(root);
    let (cid, blocks) = create_test_data()?;
    for i in 0..n_providers {
        let node = network.machine(i);
        node.send(Command::Alias(root.to_string(), Some(cid))).await;
        for block in blocks.iter().rev() {
            node.send(Command::Insert(block.clone())).await;
        }
    }

    for i in 0..n_nodes {
        let node = network.machine(i);
        node.send(Command::Flush).await;
    }
    for i in 0..n_nodes {
        let node = network.machine(i);
        assert_eq!(node.recv().await, Some(Event::Flushed));
    }

    /* TODO
    // wait until all nodes are connected
    while nodes.iter().any(|node| node.connections().len() < n_nodes -1) {
        std::thread::sleep(Duration::from_secs(1));
    }
    println!("nodes fully connected");
    */

    // compute total size of data to be synced
    let size: usize = blocks.iter().map(|block| block.data().len()).sum();
    println!("test data built {} blocks, {} bytes", blocks.len(), size);

    let target = network.machine(n_nodes - 1);

    target
        .send(Command::Alias(root.to_string(), Some(cid)))
        .await;
    let t0 = Instant::now();
    target.send(Command::Sync(cid)).await;
    assert_eq!(target.recv().await, Some(Event::Synced));

    target.send(Command::Flush).await;
    assert_eq!(target.recv().await, Some(Event::Flushed));

    println!(
        "{}, tree sync complete {} ms {} blocks {} bytes!",
        name,
        t0.elapsed().as_millis(),
        blocks.len(),
        size
    );
    // check that data is indeed synced
    for block in blocks {
        target.send(Command::Get(*block.cid())).await;
        let data = if let Some(Event::Block(data)) = target.recv().await {
            data
        } else {
            unreachable!();
        };
        assert_eq!(data, block);
    }

    for i in 0..n_nodes {
        let node = network.machine(i);
        node.send(Command::Exit).await;
    }
    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let opt = BenchOpts::from_args();
    netsim_embed::namespace::unshare_user()?;
    async_global_executor::block_on(run_test(
        "cli",
        opt.n_nodes,
        opt.n_providers,
        opt.n_spam,
        !opt.in_memory,
        Duration::from_millis(opt.delay_ms),
        || build_tree(10, 4),
    ))
}
