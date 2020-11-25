use criterion::{criterion_group, criterion_main, Criterion};
use ipfs_embed_core::Storage;
use ipfs_embed_db::StorageService;
use libipld::DagCbor;
use libipld::block::Block;
use libipld::cbor::DagCborCodec;
use libipld::cid::Cid;
use libipld::multihash::Code;
use libipld::store::DefaultParams;
use sled::Config;

#[derive(DagCbor)]
struct ChainElement {
    prev: Option<Cid>,
}

fn block(prev: Option<Cid>) -> Block<DefaultParams> {
    let elem = ChainElement { prev };
    Block::encode(DagCborCodec, Code::Blake3_256, &elem).unwrap()
}

fn alias(c: &mut Criterion) {
    let config = Config::new().temporary(true);
    let db = StorageService::<DefaultParams>::open(&config, 0, None).unwrap();
    let mut prev = block(None);
    db.insert(&prev).unwrap();
    for _ in 1..1024 {
        prev = block(Some(*prev.cid()));
        db.insert(&prev).unwrap();
    }
    c.bench_function("alias 1024 blocks", |b| {
        b.iter(|| {
            async_std::task::block_on(async {
                db.alias("test", Some(prev.cid())).await.unwrap();
                db.alias("test", None).await.unwrap();
            });
        });
    });
    let checkpoint = prev.clone();
    async_std::task::block_on(async {
        db.alias("test", Some(checkpoint.cid())).await.unwrap();
    });
    for _ in 0..1024 {
        prev = block(Some(*prev.cid()));
        db.insert(&prev).unwrap();
    }
    c.bench_function("alias 1024 blocks", |b| {
        b.iter(|| {
            async_std::task::block_on(async {
                db.alias("test", Some(prev.cid())).await.unwrap();
                db.alias("test", Some(checkpoint.cid())).await.unwrap();
            });
        });
    });
    let checkpoint = prev.clone();
    async_std::task::block_on(async {
        db.alias("test", Some(checkpoint.cid())).await.unwrap();
    });
    for _ in 0..1024 {
        prev = block(Some(*prev.cid()));
        db.insert(&prev).unwrap();
    }
    c.bench_function("alias 1024 blocks", |b| {
        b.iter(|| {
            async_std::task::block_on(async {
                db.alias("test", Some(prev.cid())).await.unwrap();
                db.alias("test", Some(checkpoint.cid())).await.unwrap();
            });
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = alias
}

criterion_main!(benches);
