use anyhow::Result;
use fnv::FnvHashSet;
use ipfs_embed::{
    generate_keypair, Block, Cid, Config, DefaultParams, Ipfs, NetworkConfig, StorageConfig,
};
use libipld::cbor::DagCborCodec;
use libipld::multihash::Code;
use libipld::DagCbor;
use rand::{thread_rng, RngCore};
use std::time::{Duration, Instant};
use tempdir::TempDir;

fn tracing_try_init() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();
}

#[async_std::test]
async fn gc() -> Result<()> {
    tracing_try_init();
    let mut builder = DagBuilder::new().await?;
    let now = Instant::now();
    for _ in 0..100 {
        builder.add_head()?;
    }
    println!("created dags in {}ms", now.elapsed().as_millis());
    let now = Instant::now();
    builder.check().await?;
    println!("checked dags in {}ms", now.elapsed().as_millis());
    for _ in 0..builder.heads() {
        let now = Instant::now();
        builder.remove_head()?;
        builder.check().await?;
        println!("removed dag in {}ms", now.elapsed().as_millis());
    }
    assert!(builder.ipfs.iter()?.next().is_none());
    Ok(())
}

#[derive(DagCbor)]
struct Node {
    nonce: u64,
    children: Vec<Cid>,
}

struct DagBuilder {
    ipfs: Ipfs<DefaultParams>,
    heads: FnvHashSet<Cid>,
    _tmp: TempDir,
}

impl DagBuilder {
    async fn new() -> Result<Self> {
        let tmp = TempDir::new("gc-test")?;
        let config = Config {
            storage: StorageConfig::new(None, None, 0, Duration::from_secs(1000)),
            network: NetworkConfig::new(tmp.path().into(), generate_keypair()),
        };
        let ipfs = Ipfs::new(config).await?;
        Ok(Self {
            ipfs,
            heads: Default::default(),
            _tmp: tmp,
        })
    }

    fn heads(&self) -> usize {
        self.heads.len()
    }

    fn add_head(&mut self) -> Result<()> {
        let mut rng = thread_rng();
        let n_children = if self.heads.is_empty() {
            0
        } else {
            rng.next_u32() as usize % self.heads.len()
        };
        let n_children_rm = if n_children == 0 {
            0
        } else {
            rng.next_u32() as usize % n_children
        };
        let nonce = rng.next_u64();
        let mut children = Vec::with_capacity(n_children);
        children.extend(self.heads.iter().take(n_children));
        let node = Node { nonce, children };
        let block = Block::encode(DagCborCodec, Code::Blake3_256, &node)?;
        self.ipfs.alias(block.cid().to_bytes(), Some(block.cid()))?;
        self.ipfs.insert(&block)?;
        for cid in node.children.into_iter().take(n_children_rm) {
            self.ipfs.alias(cid.to_bytes(), None)?;
            self.heads.remove(&cid);
        }
        self.heads.insert(*block.cid());
        Ok(())
    }

    fn remove_head(&mut self) -> Result<()> {
        if let Some(cid) = self.heads.iter().next().copied() {
            self.ipfs.alias(cid.to_bytes(), None)?;
            self.heads.remove(&cid);
        }
        Ok(())
    }

    async fn check(&self) -> Result<()> {
        let now = Instant::now();
        self.ipfs.flush().await?;
        println!("flushed in {}ms", now.elapsed().as_millis());

        let now = Instant::now();
        self.ipfs.evict().await?;
        println!("gc in {}ms", now.elapsed().as_millis());

        let now = Instant::now();
        let mut live = FnvHashSet::default();
        let mut stack = self.heads.clone();
        while let Some(cid) = stack.iter().next().copied() {
            stack.remove(&cid);
            if live.contains(&cid) {
                continue;
            }
            self.ipfs.get(&cid)?.references(&mut stack)?;
            live.insert(cid);
        }
        println!("computed closure in {}ms", now.elapsed().as_millis());
        Ok(())
    }
}
