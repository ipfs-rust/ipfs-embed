# ipfs-embed
A small, fast and reliable ipfs implementation designed for embedding in to complex p2p
applications.

* node discovery via mdns
* provider discovery via kademlia
* exchange blocks via bitswap
* lru eviction policy
* aliases, an abstraction of recursively named pins
* temporary recursive pins for building dags, preventing races with the garbage collector
* efficiently syncing large dags of blocks

Some compatibility with go-ipfs can be enabled with the `compat` feature flag.

## Getting started
```rust
use ipfs_embed::{Config, DefaultParams, Ipfs};
use libipld::DagCbor;
use libipld::store::Store;

#[derive(Clone, DagCbor, Debug, Eq, PartialEq)]
struct Identity {
    id: u64,
    name: String,
    age: u8,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache_size = 10;
    let ipfs = Ipfs::<DefaultParams>::new(Config::new(None, cache_size)).await?;
    ipfs.listen_on("/ip4/0.0.0.0/tcp/0".parse()?).await?;

    let identity = Identity {
        id: 0,
        name: "David Craven".into(),
        age: 26,
    };
    let cid = ipfs.insert(&identity)?;
    let identity2 = ipfs.get(&cid)?;
    assert_eq!(identity, identity2);
    println!("identity cid is {}", cid);

    Ok(())
}
```

# Below is some notes on the history of ipfs-embed. The information is no longer accurrate for the current implementation.

## What is ipfs?

Ipfs is a p2p network for locating and providing chunks of content addressed data
called blocks.

Content addressing means that the data is located via it's hash as opposed to
location addressing.

Unsurprisingly this is done using a distributed hash table. To avoid storing large
amounts of data in the dht, the dht stores which peers have a block. After determining
the peers that are providing a block, the block is requested from those peers.

To verify that the peer is sending the requested block and not an infinite stream of
garbage, blocks need to have a finite size. In practice we'll assume a maximum block
size of 1MB.

To encode arbitrary data in to 1MB blocks imposes two requirements on the codec. It
needs to have a canonical representation to ensure that the same data results in the
same hash and it needs to support linking to other content addressed blocks. Codecs
having these two properties are called ipld codecs.

A property that follows from content addressing (representing edges as hashes of the
node) is that arbitrary graphs of blocks are not possible. A graph of blocks is
guaranteed to be directed and acyclic.

```json
{"a":3}
```

```json
{
  "a": 3,
}
```

```json
{"/":"QmWATWQ7fVPP2EFGu71UkfnqhYXDYH566qy47CnJDgvs8u"}
```

## Block storage

Let's start with a naive model of a persistent block store.

```rust
trait BlockStorage {
    fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>>;
    fn insert(&mut self, cid: &Cid, data: &[u8]) -> Result<()>;
    fn remove(&mut self, cid: &Cid) -> Result<()>;
}
```

Since content addressed blocks form a directed acyclic graph, blocks can't simply
be deleted. A block may be referenced by multiple nodes, so some form of reference
counting and garbage collection is required to determine when a block can safely
be deleted. In the interest of being a good peer on the p2p network, we may want to
keep old blocks around that other peers may want. So thinking of it as a reference
counted cache may be a more appropriate model. We end up with something like this:

```rust
trait BlockStorage {
    fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>>;
    fn insert(&mut self, cid: &Cid, data: &[u8], references: &[Cid]) -> Result<()>;
    fn evict(&mut self) -> Result<()>;
    fn pin(&mut self, cid: &Cid) -> Result<()>;
    fn unpin(&mut self, cid: &Cid) -> Result<()>;
}
```

To mutate a block we need to perform three steps. Get the block, modify and insert the
modified block and finally remove the old one. We also need a map from keys to cids, so
even more steps are required. Any of these steps can fail leaving the block store in an
inconsistent state, leading to data leakage. To prevent data leakage every api consumer
would have to implement a write-ahead-log. To resolve these issues we extend the store
with named pins called aliases.

```rust
trait BlockStorage {
    fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>>;
    fn insert(&mut self, cid: &Cid, data: &[u8], references: &[Cid]) -> Result<()>;
    fn evict(&mut self) -> Result<()>;
    fn alias(&mut self, alias: &[u8], cid: Option<&Cid>) -> Result<()>;
    fn resolve(&self, alias: &[u8]) -> Result<Option<Cid>>;
}
```

Assuming that each operation is atomic and durable, we have the minimal set of operations
required to store dags of content addressed blocks.

## Networked block storage - the ipfs-embed api

```rust
impl Ipfs {
    pub fn new(storage: Arc<S>, network: Arc<N>) -> Self { .. }
    pub fn local_peer_id(&self) -> &PeerId { .. }
    pub async fn listeners(&self) -> Vec<Multiaddr> { .. }
    pub async fn external_addresses(&self) -> Vec<Multiaddr> { .. }
    pub async fn pinned(&self, cid: &Cid) -> Result<Option<bool>> { .. }
    pub async fn get(&self, cid: &Cid) -> Result<Block> {
        if let Some(block) = self.storage.get(cid)? {
            return Ok(block);
        }
        self.network.get(cid).await?;
        if let Some(block) = self.storage.get(cid)? {
            return Ok(block);
        }
        log::error!("block evicted too soon");
        Err(BlockNotFound(*cid))
    }
    pub async fn insert(&self, cid: &Cid) -> Result<()> {
        self.storage.insert(cid)?;
        self.network.provide(cid)?;
        Ok(())
    }
    pub async fn alias(&self, alias: &[u8], cid: Option<&Cid>) -> Result<()> {
        if let Some(cid) = cid {
            self.network.sync(cid).await?;
        }
        self.storage.alias(alias, cid).await?;
        Ok(())
    }
    pub async fn resolve(&self, alias: &[u8]) -> Result<Option<Cid>> {
        self.storage.resolve(alias)?;
        Ok(())
    }
}
```

## Design patterns - ipfs-embed in action

We'll be looking at some patterns used in the `chain` example. The `chain` example uses
`ipfs-embed` to store a chain of blocks. A block is defined as:

```rust
#[derive(Debug, Default, DagCbor)]
pub struct Block {
    prev: Option<Cid>,
    id: u32,
    loopback: Option<Cid>,
    payload: Vec<u8>,
}
```

### Atomicity

We have to different db's in this example. The one managed by `ipfs-embed` that stores
blocks and aliases and one specific to the example that maps the block index to the block
cid, so that we can lookup blocks quickly without having to traverse the entire chain. To
guarantee atomicity we define two aliases and perform the syncing in two steps. This ensures
that the synced chain always has it's blocks indexed.

```rust
const TMP_ROOT: &str = alias!(tmp_root);
const ROOT: &str = alias!(root);

ipfs.alias(TMP_ROOT, Some(new_root)).await?;
for _ in prev_root_id..new_root_id {
    // index block may error for various reasons
}
ipfs.alias(ROOT, Some(new_root)).await?;
```

### Dagification

The recursive syncing algorithm will perform worst when it is syncing a chain, as every block needs
to be synced one after the other, without being able to take advantage of any parallelism. To
resolve this issue we increase the linking of the chain by including loopbacks, to increase the
branching of the dag.

An algorithm was proposed by @rklaehn for this purpose:

```rust
fn loopback(block: usize) -> Option<usize> {
    let x = block.trailing_zeros();
    if x > 1 && block > 0 {
        Some(block - (1 << (x - 1)))
    } else {
        None
    }
}
```

### Selectors

Syncing can take a long time and doesn't allow selecting the subset of data that is needed. For
this purpose there is an experimental `alias_with_syncer` api that allows customizing the syncing
behaviour. In the chain example it is used to provide block validation, to ensure that the blocks
are valid. Altough this api is likely to change in the future.

```rust
pub struct ChainSyncer<S: StoreParams, T: Storage<S>> {
    index: sled::Db,
    storage: BitswapStorage<S, T>,
}

impl<S: StoreParams, T: Storage<S>> BitswapSync for ChainSyncer<S, T>
where
    S::Codecs: Into<DagCborCodec>,
{
    fn references(&self, cid: &Cid) -> Box<dyn Iterator<Item = Cid>> {
        if let Some(data) = self.storage.get(cid) {
            let ipld_block = libipld::Block::<S>::new_unchecked(*cid, data);
            if let Ok(block) = ipld_block.decode::<DagCborCodec, Block>() {
                return Box::new(block.prev.into_iter().chain(block.loopback.into_iter()));
            }
        }
        Box::new(std::iter::empty())
    }

    fn contains(&self, cid: &Cid) -> bool {
        self.storage.contains(cid)
    }
}
```

## Efficient block storage implementation - ipfs-embed internals

Ipfs embed uses SQLite to implement the block store, which is a performant embeddable SQL persistence layer / database.

```rust
type Id = u64;
type Atime = u64;

#[derive(Clone)]
struct BlockCache {
    // Cid -> Id
    lookup: Tree,
    // Id -> Cid
    cid: Tree,
    // Id -> Vec<u8>
    data: Tree,
    // Id -> Vec<Id>
    refs: Tree,
    // Id -> Atime
    atime: Tree,
    // Atime -> Id
    lru: Tree,
}

impl BlockCache {
    // Updates the atime and lru trees and returns the data from the data tree.
    fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>> { .. }
    // Returns an iterator of blocks sorted by least recently used.
    fn lru(&self) -> impl Iterator<Item = Result<Id>> { self.lru.iter().values() }
    // Inserts into all trees.
    fn insert(&self, cid: &Cid, data: &[u8]) -> Result<()> { ... }
    // Removes from all trees.
    fn remove(&self, id: &Id) -> Result<()> { ... }
    // Returns the recursive set of references.
    fn closure(&self, cid: &Cid) -> Result<Vec<Id>> { ... }
    // A stream of insert/remove events, useful for plugging in a network layer.
    fn subscribe(&self) -> impl Stream<Item = StorageEvent> { ... }
}
```

Given the description of operations and how it's structured in terms of trees, these operations
are straight forward to implement.

```rust
#[derive(Clone)]
struct BlockStorage {
    cache: BlockCache,
    // Vec<u8> -> Id
    alias: Tree,
    // Bag of live ids
    filter: Arc<Mutex<CuockooFilter>>,
    // Id -> Vec<Id>
    closure: Tree,
}

impl BlockStorage {
    // get from cache
    fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>> { self.cache.get(cid) }
    // insert to cache
    fn insert(&self, cid: &Cid, data: &[u8]) -> Result<()> { self.cache.insert(cid, data) }
    // returns the value of the alias tree
    fn resolve(&self, alias: &[u8]) -> Result<Option<Cid>> { ... }
    // remove the lru block that is not in the bag of live ids and remove it's closure from
    // the closure tree
    fn evict(&self) -> Result<()> { ... }
    // aliasing is an expensive operation, the implementation is sketched in pseudo code
    fn alias(&self, alias: &[u8], cid: Option<&Cid>) -> Result<()> {
        // precompute the closure
        let prev_id = self.alias.get(alias)?;
        let prev_closure = self.closure.get(&prev_id)?;
        let new_id = self.cache.lookup(&cid);
        let new_closure = self.cache.closure(&cid);

        // lock the filter preventing evictions
        let mut filter = self.filter.lock().unwrap();
        // make sure that new closure wasn't evicted in the mean time
        for id in &new_closure {
            if !self.cache.contains_id(&id) {
                return Err("cannot alias, missing references");
            }
        }
        // update the live set
        for id in &new_closure {
            filter.add(id);
        }
        for id in &prev_closure {
            filter.delete(id);
        }
        // perform transaction
        let res = (&self.alias, &self.closure).transaction(|(talias, tclosure)| {
            if let Some(id) = prev_id.as_ref() {
                talias.remove(alias)?;
            }
            if let Some(id) = id.as_ref() {
                talias.insert(alias, id)?;
                tclosure.insert(id, &closure)?;
            }
            Ok(())
        });
        // if transaction failed revert live set to previous state
        if res.is_err() {
            for id in &prev_closure {
                filter.add(id);
            }
            for id in &closure {
                filter.delete(id)
            }
        }
        res
    }
}
```

## Efficiently syncing dags of blocks - libp2p-bitswap internals

Bitswap is a very simple protocol. It was adapted and simplified for ipfs-embed. The message
format can be represented by the following enums.

```rust
pub enum BitswapRequest {
    Have(Cid),
    Block(Cid),
}

pub enum BitswapResponse {
    Have(bool),
    Block(Vec<u8>),
}
```

The mechanism for locating providers can be abstracted. A dht can be plugged in or a centralized
db query. The bitswap api looks as follows:

```rust
pub enum Query {
    Get(Cid),
    Sync(Cid),
}

pub enum BitswapEvent {
    GetProviders(Cid),
    QueryComplete(Query, Result<()>),
}

impl Bitswap {
    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) { .. }
    pub fn get(&mut self, cid: Cid) { .. }
    pub fn cancel_get(&mut self, cid: Cid) { .. }
    pub fn add_provider(&mut self, cid: Cid, peer_id: PeerId) { .. }
    pub fn complete_get_providers(&mut self, cid: Cid) { .. }
    pub fn poll(&mut self, cx: &mut Context) -> BitswapEvent { .. }
}
```

So what happens when you create a get request? First all the providers in the initial set
are queried with the have request. As an optimization, in every batch of queries a block
request is sent instead. If the get query finds a block it returns a query complete. If the
block wasn't found in the initial set, a `GetProviders(Cid)` event is emitted. This is where
the bitswap consumer tries to locate providers by for example performing a dht lookup. These
providers are registered by calling the `add_provider` method. After the locating of providers
completes, it is signaled by calling `complete_get_providers`. The query manager then performs
bitswap requests using the new provider set which results in the block being found or a block
not found error.

Often we want to sync an entire dag of blocks. We can efficiently sync dags of blocks by adding
a sync query that runs get queries in parallel for all the references of a block. The set of
providers that had a block is used as the initial set in a reference query. For this we extend
our api with the following calls.

```rust
/// Bitswap sync trait for customizing the syncing behaviour.
pub trait BitswapSync {
    /// Returns the list of blocks that need to be synced.
    fn references(&self, cid: &Cid) -> Box<dyn Iterator<Item = Cid>>;
    /// Returns if a cid needs to be synced.
    fn contains(&self, cid: &Cid) -> bool;
}

impl Bitswap {
    pub fn sync(&mut self, cid: Cid, syncer: Arc<dyn BitswapSync>) { .. }
    pub fn cancel_sync(&mut self, cid: Cid) { .. }
}
```

Note that we can customize the syncing behaviour arbitrarily by selecting a subset of blocks
we want to sync. See design patterns for more information.

## License
MIT OR Apache-2.0
