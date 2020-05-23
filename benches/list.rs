use async_std::task::block_on;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ipfs_embed::{Config, Store};
use ipld_collections::List;
use libipld::mem::MemStore;

fn baseline(c: &mut Criterion) {
    c.bench_function("Create Vec 1024xi128. size: 1024 * 16", |b| {
        b.iter(|| {
            let mut data = Vec::with_capacity(1024);
            for i in 0..1024 {
                data.push(i as i64);
            }
            black_box(data);
        })
    });
}

fn from_mem(c: &mut Criterion) {
    let mut data = Vec::with_capacity(1024);
    for i in 0..1024 {
        data.push(i as i64);
    }

    let store = MemStore::default();

    c.bench_function("from_mem: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            black_box(block_on(async {
                let (store, data) = (black_box(store.clone()), black_box(data.clone()));
                List::from(store, 64, 256, data.into_iter()).await.unwrap();
            }));
        })
    });
}

fn from_embed(c: &mut Criterion) {
    let mut data = Vec::with_capacity(1024);
    for i in 0..1024 {
        data.push(i as i64);
    }

    let config = Config::from_path("/tmp/from_embed").unwrap();
    let store = Store::new(config).unwrap();

    c.bench_function("from_embed: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            black_box(block_on(async {
                let (store, data) = (black_box(store.clone()), black_box(data.clone()));
                List::from(store, 64, 256, data.into_iter()).await.unwrap();
            }));
        })
    });
}

fn push_mem(c: &mut Criterion) {
    let store = MemStore::default();

    c.bench_function("push_mem: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            black_box(block_on(async {
                let store = black_box(store.clone());
                let mut list = List::new(store, 64, 256).await.unwrap();
                for i in 0..1024 {
                    list.push(i as i64).await.unwrap();
                }
            }));
        })
    });
}

fn push_embed(c: &mut Criterion) {
    let config = Config::from_path("/tmp/push_embed").unwrap();
    let store = Store::new(config).unwrap();

    c.bench_function("push_embed: 1024xi128; n: 4; width: 256; size: 4096", |b| {
        b.iter(|| {
            black_box(block_on(async {
                let store = black_box(store.clone());
                let mut list = List::new(store, 64, 256).await.unwrap();
                for i in 0..1024 {
                    list.push(i as i64).await.unwrap();
                }
            }));
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = baseline, from_mem, from_embed, push_mem, push_embed
}

criterion_main!(benches);
