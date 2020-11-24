use crate::blocks::{Aliases, Subscription};
use async_std::stream::interval;
use async_std::task;
use futures::stream::StreamExt;
use ipfs_embed_core::{async_trait, Block, Cid, Result, Storage, StoreParams};
use libipld::codec::Decode;
use libipld::ipld::Ipld;
use std::time::Duration;

mod blocks;
mod id;

pub struct StorageService<S: StoreParams> {
    db: sled::Db,
    store: Aliases<S>,
    cache_size: usize,
}

impl<S: StoreParams> StorageService<S>
where
    Ipld: Decode<S::Codecs>,
{
    pub fn open(
        config: &sled::Config,
        cache_size: usize,
        sweep_interval: Duration,
    ) -> Result<Self> {
        let db = config.open()?;
        let store = Aliases::open(&db)?;
        let gc = store.clone();
        task::spawn(async move {
            let mut atime = gc.atime();
            let mut stream = interval(sweep_interval);
            while let Some(()) = stream.next().await {
                let next_atime = gc.atime();
                gc.evict(cache_size, atime).await.ok();
                atime = next_atime;
            }
        });
        Ok(Self {
            db,
            cache_size,
            store,
        })
    }

    pub fn atime(&self) -> u64 {
        self.store.atime()
    }

    pub async fn evict(&self, grace_atime: u64) -> Result<()> {
        self.store.evict(self.cache_size, grace_atime).await
    }

    pub async fn flush(&self) -> Result<()> {
        self.db.flush_async().await?;
        Ok(())
    }
}

#[async_trait]
impl<S: StoreParams> Storage<S> for StorageService<S>
where
    Ipld: Decode<S::Codecs>,
{
    type Subscription = Subscription;

    fn contains(&self, cid: &Cid) -> Result<bool> {
        self.store.contains(cid)
    }

    fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        self.store.get(cid)
    }

    fn insert(&self, block: &Block<S>) -> Result<()> {
        self.store.insert(block)
    }

    async fn alias<T: AsRef<[u8]> + Send + Sync>(&self, alias: T, cid: Option<&Cid>) -> Result<()> {
        self.store.alias(alias.as_ref(), cid).await?;
        self.flush().await
    }

    fn resolve<T: AsRef<[u8]> + Send + Sync>(&self, alias: T) -> Result<Option<Cid>> {
        self.store.resolve(alias.as_ref())
    }

    async fn pinned(&self, cid: &Cid) -> Result<Option<bool>> {
        self.store.pinned(cid).await
    }

    fn subscribe(&self) -> Self::Subscription {
        self.store.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libipld::cbor::DagCborCodec;
    use libipld::multihash::Code;
    use libipld::store::DefaultParams;
    use libipld::{alias, ipld};

    fn create_block(ipld: &Ipld) -> Block<DefaultParams> {
        Block::encode(DagCborCodec, Code::Blake3_256, ipld).unwrap()
    }

    macro_rules! assert_evicted {
        ($store:expr, $block:expr) => {
            assert_eq!($store.pinned($block.cid()).await.unwrap(), None);
        };
    }

    macro_rules! assert_pinned {
        ($store:expr, $block:expr) => {
            assert_eq!($store.pinned($block.cid()).await.unwrap(), Some(true));
        };
    }

    macro_rules! assert_unpinned {
        ($store:expr, $block:expr) => {
            assert_eq!($store.pinned($block.cid()).await.unwrap(), Some(false));
        };
    }

    #[async_std::test]
    async fn test_store_evict() {
        env_logger::try_init().ok();
        let config = sled::Config::new().temporary(true);
        let store = StorageService::open(&config, 2, Duration::from_millis(10000)).unwrap();
        let blocks = [
            create_block(&ipld!(0)),
            create_block(&ipld!(1)),
            create_block(&ipld!(2)),
            create_block(&ipld!(3)),
        ];
        store.insert(&blocks[0]).unwrap();
        store.insert(&blocks[1]).unwrap();
        store.evict(store.atime() + 1).await.unwrap();
        assert_unpinned!(&store, &blocks[0]);
        assert_unpinned!(&store, &blocks[1]);
        store.insert(&blocks[2]).unwrap();
        store.evict(store.atime() + 1).await.unwrap();
        assert_evicted!(&store, &blocks[0]);
        assert_unpinned!(&store, &blocks[1]);
        assert_unpinned!(&store, &blocks[2]);
        store.get(&blocks[1]).unwrap();
        store.insert(&blocks[3]).unwrap();
        store.evict(store.atime() + 1).await.unwrap();
        assert_unpinned!(&store, &blocks[1]);
        assert_evicted!(&store, &blocks[2]);
        assert_unpinned!(&store, &blocks[3]);
    }

    #[async_std::test]
    async fn test_grace_period() {
        env_logger::try_init().ok();
        let config = sled::Config::new().temporary(true);
        let store = StorageService::open(&config, 0, Duration::from_millis(10000)).unwrap();
        let blocks = [create_block(&ipld!(0))];
        store.insert(&blocks[0]).unwrap();
        store.evict(0).await.unwrap();
        assert_unpinned!(&store, &blocks[0]);
        store.evict(store.atime() + 1).await.unwrap();
        assert_evicted!(&store, &blocks[0]);
    }

    #[async_std::test]
    #[allow(clippy::many_single_char_names)]
    async fn test_store_unpin() {
        env_logger::try_init().ok();
        let config = sled::Config::new().temporary(true);
        let store = StorageService::open(&config, 2, Duration::from_millis(10000)).unwrap();
        let a = create_block(&ipld!({ "a": [] }));
        let b = create_block(&ipld!({ "b": [a.cid()] }));
        let c = create_block(&ipld!({ "c": [a.cid()] }));
        let x = alias!(x);
        let y = alias!(y);
        store.insert(&a).unwrap();
        store.insert(&b).unwrap();
        store.insert(&c).unwrap();
        store.alias(x, Some(b.cid())).await.unwrap();
        store.alias(y, Some(c.cid())).await.unwrap();
        assert_pinned!(&store, &a);
        assert_pinned!(&store, &b);
        assert_pinned!(&store, &c);
        store.alias(x, None).await.unwrap();
        assert_pinned!(&store, &a);
        assert_unpinned!(&store, &b);
        assert_pinned!(&store, &c);
        store.alias(y, None).await.unwrap();
        assert_unpinned!(&store, &a);
        assert_unpinned!(&store, &b);
        assert_unpinned!(&store, &c);
    }

    #[async_std::test]
    #[allow(clippy::many_single_char_names)]
    async fn test_store_unpin2() {
        env_logger::try_init().ok();
        let config = sled::Config::new().temporary(true);
        let store = StorageService::open(&config, 2, Duration::from_millis(10000)).unwrap();
        let a = create_block(&ipld!({ "a": [] }));
        let b = create_block(&ipld!({ "b": [a.cid()] }));
        let x = alias!(x);
        let y = alias!(y);
        store.insert(&a).unwrap();
        store.insert(&b).unwrap();
        store.alias(x, Some(b.cid())).await.unwrap();
        store.alias(y, Some(b.cid())).await.unwrap();
        assert_pinned!(&store, &a);
        assert_pinned!(&store, &b);
        store.alias(x, None).await.unwrap();
        assert_pinned!(&store, &a);
        assert_pinned!(&store, &b);
        store.alias(y, None).await.unwrap();
        assert_unpinned!(&store, &a);
        assert_unpinned!(&store, &b);
    }
}
