use async_std::task;
use ipfs_embed::{Config, Store};
use ipld_collections::List;
use model::*;
use std::time::Duration;
use tempdir::TempDir;

fn create_store() -> (Store, TempDir) {
    let tmp = TempDir::new("").unwrap();
    let mut config = Config::from_path_local(tmp.path()).unwrap();
    config.network.enable_mdns = false;
    config.timeout = Duration::from_millis(100);
    let store = Store::new(config).unwrap();
    (store, tmp)
}

#[test]
#[ignore]
fn list_eqv() {
    const LEN: usize = 25;
    model! {
        Model => let mut vec = Vec::new(),
        Implementation => let mut list = {
            let (store, _) = create_store();
            let fut = List::new(store, LEN, 3);
            task::block_on(fut).unwrap()
        },
        Push(usize)(i in 0..LEN) => {
            vec.push(i as i64);
            task::block_on(list.push(i as i64)).unwrap();
        },
        Get(usize)(i in 0..LEN) => {
            let r1 = vec.get(i).cloned();
            let r2 = task::block_on(list.get(i)).unwrap();
            assert_eq!(r1, r2);
        },
        Len(usize)(_ in 0..LEN) => {
            let r1 = vec.len();
            let r2 = task::block_on(list.len()).unwrap();
            assert_eq!(r1, r2);
        },
        IsEmpty(usize)(_ in 0..LEN) => {
            let r1 = vec.is_empty();
            let r2 = task::block_on(list.is_empty()).unwrap();
            assert_eq!(r1, r2);
        }
    }
}
