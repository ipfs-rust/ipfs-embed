use criterion::{criterion_group, criterion_main, Criterion};
use ipfs_embed_core::Storage;
use ipfs_embed_db::StorageService;
use libipld::block::Block;
use libipld::cbor::DagCborCodec;
use libipld::cid::Cid;
use libipld::multihash::Code;
use libipld::store::DefaultParams;
use libipld::DagCbor;
use sled::Config;
use std::time::{Duration, Instant};

#[derive(DagCbor)]
struct ChainElement {
    prev: Option<Cid>,
}

fn block(prev: Option<Cid>) -> Block<DefaultParams> {
    let elem = ChainElement { prev };
    Block::encode(DagCborCodec, Code::Blake3_256, &elem).unwrap()
}

fn time_alias(
    db: &StorageService<DefaultParams>,
    old: Option<&Cid>,
    new: Option<&Cid>,
    n: u64,
) -> Duration {
    let mut time = Duration::new(0, 0);
    for _ in 0..n {
        async_std::task::block_on(async {
            let start = Instant::now();
            db.alias("test", new).await.unwrap();
            time += start.elapsed();
            db.alias("test", old).await.unwrap();
        });
    }
    time
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
        b.iter_custom(|n| time_alias(&db, None, Some(prev.cid()), n));
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
        b.iter_custom(|n| time_alias(&db, Some(checkpoint.cid()), Some(prev.cid()), n));
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
        b.iter_custom(|n| time_alias(&db, Some(checkpoint.cid()), Some(prev.cid()), n));
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = alias
}

criterion_main!(benches);
