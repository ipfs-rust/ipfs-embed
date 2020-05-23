# ipfs-embed
A small embeddable ipfs implementation compatible with libipld and with a concurrent garbage
collector. It supports
* node discovery via mdns
* provider discovery via kademlia
* exchange blocks via bitswap

The `ipld-block-builder` can be used for using the store effectively. It also supports
* creating encrypted/private block stores for sensitive data
* caching of native data types to maximize performance

`ipld-collections` provide high level multiblock abstractions using the `ipld-block-builder`.

## Getting started
```rust
use ipfs_embed::{Config, Store};
use ipld_block_builder::{BlockBuilder, Codec};
use libipld::DagCbor;

#[derive(Clone, DagCbor, Debug, Eq, PartialEq)]
struct Identity {
    id: u64,
    name: String,
    age: u8,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_path("/tmp/db")?;
    let store = Store::new(config)?;
    let codec = Codec::new(key);
    let builder = BlockBuilder::new(store, codec);

    let identity = Identity {
        id: 0,
        name: "David Craven".into(),
        age: 26,
    };
    let cid = builder.insert(&identity).await?;
    let identity2 = builder.get(&cid).await?;
    assert_eq!(identity, identity2);
    println!("identity cid is {}", cid);

    Ok(())
}
```

## Concurrent garbage collection
```rust
    // #roots = {} #live = {}
    let a = builder.insert(&ipld!({ "a": [] })).await?;
    // #roots = {a} #live = {a}
    let b = builder.insert(&ipld!({ "b": [&a] })).await?;
    // #roots = {a, b} #live = {a, b}

    // block `b` contains a reference to block `a`, so the
    // garbage collector won't remove it until block `b` gets
    // unpinned
    builder.unpin(&a).await?;
    // #roots = {b} #live = {a, b}

    let c  = builder.insert(&ipld!({ "c": [&a] })).await?;
    // #roots = {b, c} #live = {a, b, c}

    // the backing storage is content addressed, but inserting
    // a block multiple times increases the ref count.
    let c2 = builder.insert(&ipld!({ "c": [&a] })).await?;
    // #roots = {b, c, c} #live = {a, b, c}

    builder.unpin(&b).await?;
    // #roots = {b, c} #live = {a, c}
    builder.unpin(&c2).await?;
    // #roots = {c} #live = {a, c}
    builder.unpin(&c).await?;
    // #roots = {} #live = {}
```

## Multiblock collections
```rust
use ipfs_embed::{Config, Store};
use ipld_collections::List;

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_path("/tmp/list")?;
    let store = Store::new(config)?;

    let mut list = List::new(store, 64, 256).await?;
    // push: 1024xi128; n: 4; width: 256; size: 4096
    for i in 0..1024 {
        list.push(i as i64).await?;
    }
    Ok(())
}
```

## License
MIT OR Apache-2.0
