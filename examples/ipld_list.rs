use ipfs_embed::{Config, Store};
use ipld_collections::List;

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_path_local("/tmp/list")?;
    let store = Store::new(config)?;

    let mut list = List::new(store, 64, 256).await?;
    // push: 1024xi128; n: 4; width: 256; size: 4096
    for i in 0..1024 {
        list.push(i as i64).await?;
    }
    Ok(())
}
