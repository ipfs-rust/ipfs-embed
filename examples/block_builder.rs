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
    let config = Config::from_path_local("/tmp/db")?;
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
