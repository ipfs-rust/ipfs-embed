use anyhow::Result;
use futures::stream::StreamExt;
use ipfs_embed::{Config, Ipfs};
use libipld::cbor::DagCborCodec;
use libipld::multihash::Code;
use libipld::store::DefaultParams;
use libipld::{alias, Block, Cid, DagCbor};
use rand::RngCore;

const ROOT: &str = alias!(root);

#[derive(Debug, Default, DagCbor)]
pub struct Node {
    pub prev: Option<Cid>,
    pub id: u32,
    pub payload: Box<[u8]>,
}

#[derive(Default)]
pub struct NodeBuilder {
    id: u32,
    prev: Option<Cid>,
}

impl NodeBuilder {
    pub fn create(&mut self) -> Result<Block<DefaultParams>> {
        let mut payload = [0u8; 4096];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut payload);
        let node = Node {
            prev: self.prev,
            id: self.id,
            payload: payload.to_vec().into_boxed_slice(),
        };
        let block = Block::encode(DagCborCodec, Code::Blake3_256, &node)?;
        self.id += 1;
        self.prev = Some(*block.cid());
        Ok(block)
    }
}

#[async_std::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let mut config = Config::new(Some("/tmp/local1".into()), 1000);
    config.network.kad = None;
    let a = Ipfs::<DefaultParams>::new(config).await?;
    a.listen_on("/ip4/127.0.0.1/tcp/0".parse()?)?
        .next()
        .await
        .unwrap();

    let mut config = Config::new(Some("/tmp/local2".into()), 1000);
    config.network.kad = None;
    let b = Ipfs::<DefaultParams>::new(config).await?;

    println!("starting import");
    let start = std::time::Instant::now();

    let tmp = a.create_temp_pin()?;
    let mut builder = NodeBuilder::default();
    for _ in 0..1000 {
        let block = builder.create()?;
        a.temp_pin(&tmp, block.cid())?;
        let _ = a.insert(&block)?;
    }
    a.alias(ROOT, builder.prev.as_ref())?;
    a.flush().await?;

    let end = std::time::Instant::now();
    println!("time to import {}ms", end.duration_since(start).as_millis());

    println!("starting sync");
    let start = std::time::Instant::now();

    b.alias(ROOT, builder.prev.as_ref())?;
    b.sync(builder.prev.as_ref().unwrap(), vec![a.local_peer_id()])
        .await?;
    b.flush().await?;

    let end = std::time::Instant::now();
    println!("time to sync {}ms", end.duration_since(start).as_millis());

    Ok(())
}
