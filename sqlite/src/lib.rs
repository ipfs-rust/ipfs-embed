use futures::channel::mpsc;
use futures::future::BoxFuture;
pub use ipfs_sqlite_block_store::async_block_store::AsyncTempPin;
use ipfs_sqlite_block_store::{
    async_block_store::{AsyncBlockStore, GcConfig, RuntimeAdapter},
    cache::{BlockInfo, CacheTracker, SqliteCacheTracker},
    BlockStore, Config, SizeTargets,
};
use libipld::codec::References;
use libipld::store::StoreParams;
use libipld::{Block, Cid, Ipld, Result};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
struct AsyncGlobalExecutor;

impl RuntimeAdapter for AsyncGlobalExecutor {
    fn unblock<F, T>(self, f: F) -> BoxFuture<'static, Result<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        Box::pin(async { Ok(async_global_executor::spawn_blocking(f).await) })
    }

    fn sleep(&self, duration: Duration) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            async_io::Timer::after(duration).await;
        })
    }
}

#[derive(Debug)]
struct IpfsCacheTracker<T> {
    tracker: T,
    tx: mpsc::UnboundedSender<StorageEvent>,
}

impl<T: CacheTracker> CacheTracker for IpfsCacheTracker<T> {
    fn blocks_accessed(&mut self, blocks: Vec<BlockInfo>) {
        self.tracker.blocks_accessed(blocks)
    }

    fn blocks_deleted(&mut self, blocks: Vec<BlockInfo>) {
        for block in &blocks {
            self.tx
                .unbounded_send(StorageEvent::Remove(*block.cid()))
                .ok();
        }
        self.tracker.blocks_deleted(blocks)
    }

    fn retain_ids(&mut self, ids: &[i64]) {
        self.tracker.retain_ids(ids)
    }

    fn sort_ids(&self, ids: &mut [i64]) {
        self.tracker.sort_ids(ids)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageConfig {
    pub path: Option<PathBuf>,
    pub cache_size_blocks: u64,
    pub cache_size_bytes: u64,
    pub gc_interval: Duration,
    pub gc_min_blocks: usize,
    pub gc_target_duration: Duration,
}

impl StorageConfig {
    pub fn new(path: Option<PathBuf>, cache_size: u64, gc_interval: Duration) -> Self {
        Self {
            path,
            cache_size_blocks: cache_size,
            cache_size_bytes: u64::MAX,
            gc_interval,
            gc_min_blocks: usize::MAX,
            gc_target_duration: Duration::new(u64::MAX, 1_000_000_000 - 1),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageStats {
    /// Total number of blocks in the store
    pub count: u64,
    /// Total size of blocks in the store
    pub size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageEvent {
    Remove(Cid),
}

#[derive(Clone)]
pub struct StorageService<S: StoreParams> {
    _marker: PhantomData<S>,
    store: AsyncBlockStore<AsyncGlobalExecutor>,
    gc_target_duration: Duration,
    gc_min_blocks: usize,
}

impl<S: StoreParams> StorageService<S>
where
    Ipld: References<S::Codecs>,
{
    pub fn open(config: StorageConfig, tx: mpsc::UnboundedSender<StorageEvent>) -> Result<Self> {
        let size = SizeTargets::new(config.cache_size_blocks, config.cache_size_bytes);
        let store_config = Config::default().with_size_targets(size);
        let gc_config = GcConfig {
            interval: config.gc_interval,
            min_blocks: config.gc_min_blocks,
            target_duration: config.gc_target_duration,
        };
        let store = if let Some(path) = config.path {
            let tracker = SqliteCacheTracker::open(&path, |access, _| Some(access))?;
            let tracker = IpfsCacheTracker { tracker, tx };
            BlockStore::open(path, store_config.with_cache_tracker(tracker))?
        } else {
            let tracker = SqliteCacheTracker::memory(|access, _| Some(access))?;
            let tracker = IpfsCacheTracker { tracker, tx };
            BlockStore::memory(store_config.with_cache_tracker(tracker))?
        };
        let store = AsyncBlockStore::new(AsyncGlobalExecutor, store);
        let gc = store.clone().gc_loop(gc_config);
        async_global_executor::spawn(gc).detach();
        Ok(Self {
            _marker: PhantomData,
            gc_target_duration: config.gc_target_duration,
            gc_min_blocks: config.gc_min_blocks,
            store,
        })
    }

    pub async fn temp_pin(&self) -> Result<AsyncTempPin> {
        Ok(self.store.temp_pin().await?)
    }

    pub async fn iter(&self) -> Result<std::vec::IntoIter<Cid>> {
        Ok(self.store.get_block_cids::<Vec<Cid>>().await?.into_iter())
    }

    pub async fn contains(&self, cid: &Cid) -> Result<bool> {
        Ok(self.store.has_block(cid).await?)
    }

    pub async fn get(&self, cid: Cid) -> Result<Option<Vec<u8>>> {
        Ok(self.store.get_block(cid).await?)
    }

    pub async fn insert(&self, block: Block<S>, alias: Option<&AsyncTempPin>) -> Result<()> {
        Ok(self.store.put_block(block, alias).await?)
    }

    pub async fn evict(&self) -> Result<()> {
        while !self
            .store
            .incremental_gc(self.gc_min_blocks, self.gc_target_duration)
            .await?
        {}
        while !self
            .store
            .incremental_delete_orphaned(self.gc_min_blocks, self.gc_target_duration)
            .await?
        {}
        Ok(())
    }

    pub async fn alias(&self, alias: Vec<u8>, cid: Option<Cid>) -> Result<()> {
        Ok(self.store.alias(alias, cid).await?)
    }

    pub async fn resolve(&self, alias: Vec<u8>) -> Result<Option<Cid>> {
        Ok(self.store.resolve(alias).await?)
    }

    pub async fn pinned(&self, cid: Cid) -> Result<Option<Vec<Vec<u8>>>> {
        Ok(self.store.reverse_alias(cid).await?)
    }

    pub async fn missing_blocks(&self, cid: Cid) -> Result<Vec<Cid>> {
        Ok(self.store.get_missing_blocks(cid).await?)
    }

    pub async fn stats(&self) -> Result<StorageStats> {
        let stats = self.store.get_store_stats().await?;
        Ok(StorageStats {
            count: stats.count(),
            size: stats.size(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::StreamExt;
    use libipld::cbor::DagCborCodec;
    use libipld::multihash::Code;
    use libipld::store::DefaultParams;
    use libipld::{alias, ipld};

    fn create_block(ipld: &Ipld) -> Block<DefaultParams> {
        Block::encode(DagCborCodec, Code::Blake3_256, ipld).unwrap()
    }

    macro_rules! assert_evicted {
        ($store:expr, $block:expr) => {
            assert_eq!($store.pinned(*$block.cid()).await.unwrap(), None);
        };
    }

    macro_rules! assert_pinned {
        ($store:expr, $block:expr) => {
            assert_eq!(
                $store
                    .pinned(*$block.cid())
                    .await
                    .unwrap()
                    .map(|a| !a.is_empty()),
                Some(true)
            );
        };
    }

    macro_rules! assert_unpinned {
        ($store:expr, $block:expr) => {
            assert_eq!(
                $store
                    .pinned(*$block.cid())
                    .await
                    .unwrap()
                    .map(|a| !a.is_empty()),
                Some(false)
            );
        };
    }

    fn tracing_try_init() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init()
            .ok();
    }

    fn create_store() -> (StorageService<DefaultParams>, mpsc::UnboundedReceiver<StorageEvent>) {
        let (tx, rx) = mpsc::unbounded();
        let config = StorageConfig::new(None, 2, Duration::from_secs(100));
        (StorageService::open(config, tx).unwrap(), rx)
    }

    #[async_std::test]
    async fn test_store_evict() {
        tracing_try_init();
        let (store, mut rx) = create_store();
        let blocks = [
            create_block(&ipld!(0)),
            create_block(&ipld!(1)),
            create_block(&ipld!(2)),
            create_block(&ipld!(3)),
        ];
        store.insert(blocks[0].clone(), None).await.unwrap();
        store.insert(blocks[1].clone(), None).await.unwrap();
        store.evict().await.unwrap();
        assert_unpinned!(&store, &blocks[0]);
        assert_unpinned!(&store, &blocks[1]);
        store.insert(blocks[2].clone(), None).await.unwrap();
        store.evict().await.unwrap();
        assert_evicted!(&store, &blocks[0]);
        assert_unpinned!(&store, &blocks[1]);
        assert_unpinned!(&store, &blocks[2]);
        store.get(*blocks[1].cid()).await.unwrap();
        store.insert(blocks[3].clone(), None).await.unwrap();
        store.evict().await.unwrap();
        assert_unpinned!(&store, &blocks[1]);
        assert_evicted!(&store, &blocks[2]);
        assert_unpinned!(&store, &blocks[3]);
        assert_eq!(
            rx.next().await,
            Some(StorageEvent::Remove(*blocks[0].cid()))
        );
        assert_eq!(
            rx.next().await,
            Some(StorageEvent::Remove(*blocks[2].cid()))
        );
    }

    #[async_std::test]
    #[allow(clippy::many_single_char_names)]
    async fn test_store_unpin() {
        tracing_try_init();
        let (store, _) = create_store();
        let a = create_block(&ipld!({ "a": [] }));
        let b = create_block(&ipld!({ "b": [a.cid()] }));
        let c = create_block(&ipld!({ "c": [a.cid()] }));
        let x = alias!(x).as_bytes().to_vec();
        let y = alias!(y).as_bytes().to_vec();
        store.insert(a.clone(), None).await.unwrap();
        store.insert(b.clone(), None).await.unwrap();
        store.insert(c.clone(), None).await.unwrap();
        store.alias(x.clone(), Some(*b.cid())).await.unwrap();
        store.alias(y.clone(), Some(*c.cid())).await.unwrap();
        assert_pinned!(&store, &a);
        assert_pinned!(&store, &b);
        assert_pinned!(&store, &c);
        store.alias(x.clone(), None).await.unwrap();
        assert_pinned!(&store, &a);
        assert_unpinned!(&store, &b);
        assert_pinned!(&store, &c);
        store.alias(y.clone(), None).await.unwrap();
        assert_unpinned!(&store, &a);
        assert_unpinned!(&store, &b);
        assert_unpinned!(&store, &c);
    }

    #[async_std::test]
    #[allow(clippy::many_single_char_names)]
    async fn test_store_unpin2() {
        tracing_try_init();
        let (store, _) = create_store();
        let a = create_block(&ipld!({ "a": [] }));
        let b = create_block(&ipld!({ "b": [a.cid()] }));
        let x = alias!(x).as_bytes().to_vec();
        let y = alias!(y).as_bytes().to_vec();
        store.insert(a.clone(), None).await.unwrap();
        store.insert(b.clone(), None).await.unwrap();
        store.alias(x.clone(), Some(*b.cid())).await.unwrap();
        store.alias(y.clone(), Some(*b.cid())).await.unwrap();
        assert_pinned!(&store, &a);
        assert_pinned!(&store, &b);
        store.alias(x.clone(), None).await.unwrap();
        assert_pinned!(&store, &a);
        assert_pinned!(&store, &b);
        store.alias(y.clone(), None).await.unwrap();
        assert_unpinned!(&store, &a);
        assert_unpinned!(&store, &b);
    }
}
