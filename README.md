# ipfs-embed
A small embeddable ipfs implementation compatible with libipld and with a concurrent garbage
collector. It supports
* node discovery via mdns
* provider discovery via kademlia
* exchange blocks via bitswap
* lru eviction policy
* aliases, an abstraction of recursively named pins with customizable syncing of dags

## Getting started
```rust
use ipfs_embed::Ipfs;
use ipfs_embed::core::BitswapStorage;
use ipfs_embed::db::StorageService;
use ipfs_embed::net::{NetworkConfig, NetworkService};
use libipld::DagCbor;
use libipld::store::{DefaultParams, Store};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, DagCbor, Debug, Eq, PartialEq)]
struct Identity {
    id: u64,
    name: String,
    age: u8,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sled_config = sled::Config::new().temporary(true);
    let cache_size = 10;
    let sweep_interval = Duration::from_millis(10000);
    let net_config = NetworkConfig::new();
    let storage = Arc::new(StorageService::open(&sled_config, cache_size, sweep_interval).unwrap());
    let bitswap_storage = BitswapStorage::new(storage.clone());
    let network = Arc::new(NetworkService::new(net_config, bitswap_storage).unwrap());
    let ipfs = Ipfs::<DefaultParams, _, _>::new(storage, network);

    let identity = Identity {
        id: 0,
        name: "David Craven".into(),
        age: 26,
    };
    let cid = ipfs.insert(&identity).await?;
    let identity2 = ipfs.get(&cid).await?;
    assert_eq!(identity, identity2);
    println!("identity cid is {}", cid);

    Ok(())
}
```

## Alias concept
TODO

## License
MIT OR Apache-2.0
