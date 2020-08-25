use ipfs_embed::{Config, Store};
use ipld_collections::List;
use libipld::cache::CacheConfig;
use libipld::cbor::DagCborCodec;
use libipld::{Multicodec, Multihash};

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_path_local("/tmp/list")?;
    let store = Store::<Multicodec, Multihash>::new(config)?;
    let mut cache_config = CacheConfig::new(store, DagCborCodec);
    cache_config.size = 64;

    let mut list = List::new(cache_config, 256).await?;
    // push: 1024xi128; n: 4; width: 256; size: 4096
    for i in 0..1024 {
        list.push(i as i64).await?;
    }
    Ok(())
}
