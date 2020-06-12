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
    let codec = Codec::new();
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

## Debugging with the cli tool

List blocks in the store:

```sh
ipfs-embed-cli --path ~/.config/cli-identity/db ls
pins refs pub cid
   1    0 pub bafy2bzaceantsvvwqnget77qyjkol62sacerqijmew2i6ertspavvvzopxffc
```

and read the content:

```sh
ipfs-embed-cli --path ~/.config/cli-identity/db cat bafy2bzaceantsvvwqnget77qyjkol62sacerqijmew2i6ertspavvvzopxffc
{"claim":{"block":[99,72,157,77,43,88,249,34,49,85,62,244,13,31,24,56,66,2,13,69,45,25,96,21,72,19,60,17,207,52,248,101],"body":{"Ownership":[{"Github":["dvc94ch"]}]},"ctime":1591965518393,"expire_in":18446744073709551615,"genesis":[3,246,228,41,120,121,231,179,151,127,140,158,252,115,44,84,36,88,59,133,147,67,230,114,157,39,199,218,182,118,223,46],"prev":null,"public":"5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY","seqno":1,"uid":0},"signature":[1,126,118,55,106,111,215,158,113,168,56,48,59,225,167,255,125,177,97,234,174,188,152,172,248,210,163,252,8,198,2,170,26,204,11,123,110,35,13,3,107,76,91,18,59,74,135,128,129,147,135,216,216,117,164,134,127,155,3,8,44,122,206,112,128]}
```

## License
MIT OR Apache-2.0
