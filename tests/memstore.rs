use async_std::task;
use core::future::Future;
use futures::join;
use ipfs_embed::{Config, Store};
use libipld::cid::{Cid, Codec};
use libipld::error::StoreError;
use libipld::mem::MemStore;
use libipld::multihash::Sha2_256;
use libipld::store::{ReadonlyStore, Store as _, Visibility};
use model::*;
use std::time::Duration;
use tempdir::TempDir;

fn create_store() -> (Store, TempDir) {
    let tmp = TempDir::new("").unwrap();
    let mut config = Config::from_path(tmp.path()).unwrap();
    config.network.enable_mdns = false;
    config.network.bootstrap_nodes = vec![];
    config.timeout = Duration::from_millis(100);
    let store = Store::new(config).unwrap();
    (store, tmp)
}

fn create_block(n: usize) -> (Cid, Box<[u8]>) {
    let data = n.to_ne_bytes().to_vec().into_boxed_slice();
    let hash = Sha2_256::digest(&data);
    let cid = Cid::new_v1(Codec::Raw, hash);
    (cid, data)
}

async fn block_not_found<T: Send>(
    future: impl Future<Output = Result<T, StoreError>>,
) -> Option<T> {
    let result = future.await;
    if let Err(StoreError::BlockNotFound(_)) = result {
        None
    } else {
        Some(result.unwrap())
    }
}

fn join<T: Send>(
    f1: impl Future<Output = Result<T, StoreError>> + Send,
    f2: impl Future<Output = Result<T, StoreError>> + Send,
) -> (Option<T>, Option<T>) {
    task::block_on(async { join!(block_not_found(f1), block_not_found(f2)) })
}

#[test]
#[ignore]
fn store_eqv() {
    const LEN: usize = 4;
    let blocks: Vec<_> = (0..LEN).map(create_block).collect();
    model! {
        Model => let mem = MemStore::default(),
        Implementation => let (store, _) = create_store(),
        Get(usize)(i in 0..LEN) => {
            let (cid, _) = &blocks[i];
            let mem = mem.get(cid);
            let store = store.get(cid);
            let (mem, store) = join(mem, store);
            // garbage collector may not have collected yet
            if mem.is_some() {
                assert_eq!(mem, store);
            }
        },
        Insert(usize)(i in 0..LEN) => {
            let (cid, data) = &blocks[i];
            let mem = mem.insert(cid, data.clone(), Visibility::Public);
            let store = store.insert(cid, data.clone(), Visibility::Public);
            join(mem, store);
        },
        Unpin(usize)(i in 0..LEN) => {
            let (cid, _) = &blocks[i];
            let mem = mem.unpin(&cid);
            let store = store.unpin(&cid);
            join(mem, store);
        }
    }
}

#[test]
#[ignore]
#[allow(clippy::clone_on_copy)]
fn linearizable() {
    const LEN: usize = 4;
    let blocks: Vec<_> = (0..LEN).map(create_block).collect();
    let blocks = Shared::new(blocks);
    let (store, _) = create_store();
    linearizable! {
        Implementation => let store = Shared::new(store.clone()),
        Get(usize)(i in 0..LEN) -> Option<Box<[u8]>> {
            let (cid, _) = &blocks[i];
            task::block_on(block_not_found(store.get(cid)))
        },
        Insert(usize)(i in 0..LEN) -> () {
            let (cid, data) = &blocks[i];
            task::block_on(store.insert(cid, data.clone(), Visibility::Public)).unwrap()
        },
        Unpin(usize)(i in 0..LEN) -> () {
            let (cid, _) = &blocks[i];
            task::block_on(store.unpin(cid)).unwrap()
        }
    };
}
