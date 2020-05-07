use ipfs_embed::{Config, Store};
use ipld_block_builder::DagCbor;

#[derive(Clone, DagCbor, Debug, Eq, PartialEq)]
struct Identity {
    id: u64,
    name: String,
    age: u8,
}

mod encrypted_builder {
    use disco::symmetric::{decrypt, encrypt, AuthCiphertext};
    use ipld_block_builder::cbor::DagCbor;
    use ipld_block_builder::cid::Cid;
    use ipld_block_builder::codec::{Codec, Decode, Encode};
    use ipld_block_builder::error::{Error, Result};
    use ipld_block_builder::multihash::{Blake2b256, Code, Multihasher};
    use ipld_block_builder::raw::Raw;
    use ipld_block_builder::store::{ReadonlyStore, Store};
    use ipld_block_builder::GenericBlockBuilder;
    use std::marker::PhantomData;

    pub type EncryptedBlockBuilder<S> = GenericEncryptedBlockBuilder<S, Blake2b256, DagCbor>;

    pub struct GenericEncryptedBlockBuilder<S, H: Multihasher<Code>, C: Codec> {
        marker: PhantomData<C>,
        key: Vec<u8>,
        builder: GenericBlockBuilder<S, H, Raw>,
    }

    impl<S, H: Multihasher<Code>, C: Codec> GenericEncryptedBlockBuilder<S, H, C> {
        pub fn new(store: S, key: Vec<u8>) -> Self {
            Self {
                marker: PhantomData,
                key,
                builder: GenericBlockBuilder::<S, H, Raw>::new_private(store),
            }
        }
    }

    impl<S: ReadonlyStore, H: Multihasher<Code>, C: Codec> GenericEncryptedBlockBuilder<S, H, C> {
        /// Returns the decoded block with cid.
        pub async fn get<D: Decode<C>>(&self, cid: &Cid) -> Result<D> {
            let bytes: Vec<u8> = self.builder.get(cid).await?;
            let ct = AuthCiphertext::from_bytes(bytes).unwrap();
            let data = decrypt(&self.key, ct).unwrap();
            Ok(C::decode(&data).map_err(|e| Error::CodecError(Box::new(e)))?)
        }
    }

    impl<S: Store, H: Multihasher<Code>, C: Codec> GenericEncryptedBlockBuilder<S, H, C> {
        /// Encodes and inserts a block into the store.
        pub async fn insert<E: Encode<C>>(&self, e: &E) -> Result<Cid> {
            let data = C::encode(e).map_err(|e| Error::CodecError(Box::new(e)))?;
            let ct = encrypt(&self.key, data.into_vec()).into_bytes();
            let cid = self.builder.insert(&ct).await?;
            Ok(cid)
        }
    }
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_path("/tmp/db")?;
    let store = Store::new(config)?;
    let key = b"private encryption key".to_vec();
    let builder = encrypted_builder::EncryptedBlockBuilder::new(store, key);

    let identity = Identity {
        id: 0,
        name: "David Craven".into(),
        age: 26,
    };
    let cid = builder.insert(&identity).await?;
    let identity2 = builder.get(&cid).await?;
    assert_eq!(identity, identity2);
    println!("encrypted identity cid is {}", cid);

    Ok(())
}
