use ipfs_embed_core::Storage;
use ipfs_embed_db::StorageService;
use libipld::block::Block;
use libipld::cbor::DagCborCodec;
use libipld::cid::Cid;
use libipld::multihash::Code;
use libipld::store::DefaultParams;
use libipld::DagCbor;
use sled::Config;
use tracing_subscriber::prelude::*;

const PATH: &str = "alias.folded";

#[derive(DagCbor)]
struct ChainElement {
    prev: Option<Cid>,
}

fn block(prev: Option<Cid>) -> Block<DefaultParams> {
    let elem = ChainElement { prev };
    Block::encode(DagCborCodec, Code::Blake3_256, &elem).unwrap()
}

fn main() {
    let (flame_layer, guard) = tracing_flame::FlameLayer::with_file(PATH).unwrap();
    let subscriber = tracing_subscriber::Registry::default().with(flame_layer);
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let config = Config::new().temporary(true);
    let db = StorageService::<DefaultParams>::open(&config, 0, None).unwrap();
    let mut prev = block(None);
    db.insert(&prev).unwrap();
    for _ in 1..1024 {
        prev = block(Some(*prev.cid()));
        db.insert(&prev).unwrap();
    }
    async_std::task::block_on(async {
        db.alias("test", Some(prev.cid())).await.unwrap();
    });
    for _ in 0..1024 {
        prev = block(Some(*prev.cid()));
        db.insert(&prev).unwrap();
    }
    async_std::task::block_on(async {
        db.alias("test", Some(prev.cid())).await.unwrap();
    });
    for _ in 0..1024 {
        prev = block(Some(*prev.cid()));
        db.insert(&prev).unwrap();
    }
    async_std::task::block_on(async {
        db.alias("test", Some(prev.cid())).await.unwrap();
    });

    drop(guard);
    let inf = std::fs::File::open(PATH).unwrap();
    let reader = std::io::BufReader::new(inf);
    let out = std::fs::File::create("alias.svg").unwrap();
    let writer = std::io::BufWriter::new(out);
    let mut opts = inferno::flamegraph::Options::default();
    inferno::flamegraph::from_reader(&mut opts, reader, writer).unwrap();
}
