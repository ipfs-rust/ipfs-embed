use ipfs_embed::{Config, Store};
use libipld::cache::{Cache, CacheConfig, IpldCache, ReadonlyCache};
use libipld::cbor::DagCborCodec;
use libipld::{DagCbor, Multicodec, Multihash};

#[derive(Clone, DagCbor, Debug, Eq, PartialEq)]
struct Identity {
    id: u64,
    name: String,
    age: u8,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_path_local("/tmp/db")?;
    let store = Store::<Multicodec, Multihash>::new(config)?;
    let config = CacheConfig::new(store, DagCborCodec);
    let cache = IpldCache::new(config);

    let identity = Identity {
        id: 0,
        name: "David Craven".into(),
        age: 26,
    };
    let cid = cache.insert(identity.clone()).await?;
    let identity2 = cache.get(&cid).await?;
    assert_eq!(identity, identity2);
    println!("identity cid is {}", cid);
    Ok(())
}
