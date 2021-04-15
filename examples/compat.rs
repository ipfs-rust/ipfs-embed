use futures::prelude::*;
use ipfs_embed::{Config, Ipfs, Multiaddr, PeerId};
use libipld::{store::StoreParams, Cid, IpldCodec};

#[derive(Debug, Clone)]
struct Sp;

impl StoreParams for Sp {
    type Hashes = libipld::multihash::Code;
    type Codecs = IpldCodec;
    const MAX_BLOCK_SIZE: usize = 1024 * 1024 * 4;
}

fn tracing_try_init() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init()
}

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    tracing_try_init();
    let config = Config::new(None, 1024 * 1024);
    let ipfs = Ipfs::<Sp>::new(config).await?;
    let peer: PeerId = "QmRSGx67Kq8w7xSBDia7hQfbfuvauMQGgxcwSWw976x4BS".parse()?;
    let addr: Multiaddr = "/ip4/54.173.33.96/tcp/4001".parse()?;
    ipfs.dial_address(&peer, addr)?;

    // 10 random bytes
    let _cid_rand10: Cid = "QmXQsqVRpp2W7fbYZHi4aB2Xkqfd3DpwWskZoLVEYigMKC".parse()?;
    // a dag-cbor leaf
    let cid_leaf_cbor: Cid =
        "bafyreigrorafarnec53q4pdeq7de45gf6zv3bq2ljm6rumywffwgbtwur4".parse()?;
    // a very simple dag-cbor dag with 1 child
    let cid_simple_dag: Cid =
        "bafyreidrtb53vnjjxnhf6pu5tankvyzsjrbgp23ypx3v34a7eccqboalry".parse()?;
    // a unixfs v1 movie
    let _cid_movie: Cid = "QmWhFbSZ6gr3sz5EpxjmxhPCfj4JYH43y4p6o1gNzSMzow".parse()?;
    let block = ipfs.fetch(&cid_leaf_cbor, vec![peer]).await?;
    println!("got single block. len = {}", block.data().len());

    let block = ipfs.fetch(&cid_simple_dag, vec![peer]).await?;
    println!("got single block. len = {}", block.data().len());

    let mut updates = ipfs.sync(&cid_simple_dag, vec![peer]);
    println!("starting sync of large file");
    while let Some(update) = updates.next().await {
        println!("{:?}", update);
    }
    Ok(())
}
