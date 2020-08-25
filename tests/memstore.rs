use async_std::task;
use core::future::Future;
use futures::join;
use ipfs_embed::{Config, Store};
use libipld::error::{BlockNotFound, Result};
use libipld::mem::MemStore;
use libipld::multihash::SHA2_256;
use libipld::prelude::*;
use libipld::raw::RawCodec;
use libipld::store::Store as _;
use libipld::{Block, Multicodec, Multihash};
use model::*;
use std::time::Duration;
use tempdir::TempDir;

fn create_store() -> (Store<Multicodec, Multihash>, TempDir) {
    let tmp = TempDir::new("").unwrap();
    let mut config = Config::from_path_local(tmp.path()).unwrap();
    config.network.enable_mdns = false;
    config.timeout = Duration::from_millis(100);
    let store = Store::new(config).unwrap();
    (store, tmp)
}

fn create_block(n: usize) -> Block<Multicodec, Multihash> {
    Block::encode(RawCodec, SHA2_256, &n.to_ne_bytes()[..]).unwrap()
}

async fn block_not_found<T: Send>(future: impl Future<Output = Result<T>>) -> Option<T> {
    match future.await {
        Ok(b) => Some(b),
        Err(e) if e.downcast_ref::<BlockNotFound>().is_some() => None,
        Err(e) => panic!("{:?}", e),
    }
}

fn join<T: Send>(
    f1: impl Future<Output = Result<T>> + Send,
    f2: impl Future<Output = Result<T>> + Send,
) -> (Option<T>, Option<T>) {
    task::block_on(async { join!(block_not_found(f1), block_not_found(f2)) })
}

#[test]
#[ignore]
fn store_eqv() {
    const LEN: usize = 4;
    let blocks: Vec<_> = (0..LEN).map(create_block).collect();
    model! {
        Model => let mem = MemStore::new(),
        Implementation => let (store, _) = create_store(),
        Get(usize)(i in 0..LEN) => {
            let block = &blocks[i];
            let mem = mem.get(block.cid.clone());
            let store = store.get(block.cid.clone());
            let (mem, store) = join(mem, store);
            // garbage collector may not have collected yet
            if mem.is_some() {
                assert_eq!(mem, store);
            }
        },
        Insert(usize)(i in 0..LEN) => {
            let block = &blocks[i];
            let mem = mem.insert(block);
            let store = store.insert(block);
            join(mem, store);
        },
        Unpin(usize)(i in 0..LEN) => {
            let block = &blocks[i];
            let mem = mem.unpin(&block.cid);
            let store = store.unpin(&block.cid);
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
        Get(usize)(i in 0..LEN) -> Option<Block<Multicodec, Multihash>> {
            let block = &blocks[i];
            task::block_on(block_not_found(store.get(block.cid.clone())))
        },
        Insert(usize)(i in 0..LEN) -> () {
            let block = &blocks[i];
            task::block_on(store.insert(block)).unwrap()
        },
        Unpin(usize)(i in 0..LEN) -> () {
            let block = &blocks[i];
            task::block_on(store.unpin(&block.cid)).unwrap()
        }
    };
}
