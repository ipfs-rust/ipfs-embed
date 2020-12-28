use futures::channel::mpsc;
use futures::future::BoxFuture;
use ipfs_embed_core::{
    async_trait, Block, Cid, Result, Storage, StorageEvent, StorageStats, StoreParams,
};
use ipfs_sqlite_block_store::{
    async_block_store::{AsyncBlockStore, AsyncTempPin, GcConfig, RuntimeAdapter},
    cache::{BlockInfo, CacheTracker, SqliteCacheTracker},
    BlockStore, Config, SizeTargets,
};
use libipld::codec::References;
use libipld::ipld::Ipld;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
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
    subscriptions: Arc<Mutex<Vec<mpsc::UnboundedSender<StorageEvent>>>>,
}

impl<T: CacheTracker> CacheTracker for IpfsCacheTracker<T> {
    fn blocks_accessed(&mut self, blocks: Vec<BlockInfo>) {
        self.tracker.blocks_accessed(blocks)
    }

    fn blocks_deleted(&mut self, blocks: Vec<BlockInfo>) {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        for block in &blocks {
            subscriptions.retain(|tx| {
                tx.unbounded_send(StorageEvent::Remove(*block.cid()))
                    .is_ok()
            });
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

pub struct StorageService<S: StoreParams> {
    _marker: PhantomData<S>,
    store: AsyncBlockStore<AsyncGlobalExecutor>,
    gc_target_duration: Duration,
    gc_min_blocks: usize,
    subscriptions: Arc<Mutex<Vec<mpsc::UnboundedSender<StorageEvent>>>>,
}

impl<S: StoreParams> StorageService<S>
where
    Ipld: References<S::Codecs>,
{
    pub fn open(config: StorageConfig) -> Result<Self> {
        let size = SizeTargets::new(config.cache_size_blocks, config.cache_size_bytes);
        let store_config = Config::default().with_size_targets(size);
        let gc_config = GcConfig {
            interval: config.gc_interval,
            min_blocks: config.gc_min_blocks,
            target_duration: config.gc_target_duration,
        };
        let subscriptions = Arc::new(Mutex::new(Default::default()));
        let store = if let Some(path) = config.path {
            let tracker = SqliteCacheTracker::open(&path, |access, _| Some(access))?;
            let tracker = IpfsCacheTracker {
                tracker,
                subscriptions: subscriptions.clone(),
            };
            BlockStore::open(path, store_config.with_cache_tracker(tracker))?
        } else {
            let tracker = SqliteCacheTracker::memory(|access, _| Some(access))?;
            let tracker = IpfsCacheTracker {
                tracker,
                subscriptions: subscriptions.clone(),
            };
            BlockStore::memory(store_config.with_cache_tracker(tracker))?
        };
        let store = AsyncBlockStore::new(AsyncGlobalExecutor, store);
        let gc = store.clone().gc_loop(gc_config);
        async_global_executor::spawn(gc).detach();
        Ok(Self {
            _marker: PhantomData,
            gc_target_duration: config.gc_target_duration,
            gc_min_blocks: config.gc_min_blocks,
            subscriptions,
            store,
        })
    }
}

#[async_trait]
impl<S: StoreParams> Storage<S> for StorageService<S>
where
    Ipld: References<S::Codecs>,
{
    type TempPin = AsyncTempPin;
    type Iterator = std::vec::IntoIter<Cid>;
    type Subscription = mpsc::UnboundedReceiver<StorageEvent>;

    async fn temp_pin(&self) -> Result<Self::TempPin> {
        Ok(self.store.temp_pin().await?)
    }

    async fn iter(&self) -> Result<Self::Iterator> {
        Ok(self.store.get_block_cids::<Vec<Cid>>().await?.into_iter())
    }

    async fn contains(&self, cid: &Cid) -> Result<bool> {
        Ok(self.store.has_block(cid).await?)
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        Ok(self.store.get_block(*cid).await?)
    }

    async fn insert(&self, block: &Block<S>, alias: Option<&Self::TempPin>) -> Result<()> {
        Ok(self.store.put_block(block.clone(), alias).await?)
    }

    async fn evict(&self) -> Result<()> {
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

    async fn alias<T: AsRef<[u8]> + Send + Sync>(&self, alias: T, cid: Option<&Cid>) -> Result<()> {
        Ok(self
            .store
            .alias(alias.as_ref().to_vec(), cid.copied())
            .await?)
    }

    async fn resolve<T: AsRef<[u8]> + Send + Sync>(&self, alias: T) -> Result<Option<Cid>> {
        Ok(self.store.resolve(alias.as_ref().to_vec()).await?)
    }

    async fn pinned(&self, cid: &Cid) -> Result<Option<Vec<Vec<u8>>>> {
        Ok(self.store.reverse_alias(*cid).await?)
    }

    async fn missing_blocks(&self, cid: &Cid) -> Result<fnv::FnvHashSet<Cid>> {
        Ok(self.store.get_missing_blocks(*cid).await?)
    }

    async fn stats(&self) -> Result<StorageStats> {
        let stats = self.store.get_store_stats().await?;
        Ok(StorageStats {
            count: stats.count(),
            size: stats.size(),
        })
    }

    fn subscribe(&self) -> Self::Subscription {
        let (tx, rx) = mpsc::unbounded();
        self.subscriptions.lock().unwrap().push(tx);
        rx
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
            assert_eq!($store.pinned($block.cid()).await.unwrap(), None);
        };
    }

    macro_rules! assert_pinned {
        ($store:expr, $block:expr) => {
            assert_eq!(
                $store
                    .pinned($block.cid())
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
                    .pinned($block.cid())
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

    fn create_store() -> StorageService<DefaultParams> {
        let config = StorageConfig::new(None, 2, Duration::from_secs(100));
        StorageService::open(config).unwrap()
    }

    #[async_std::test]
    async fn test_store_evict() {
        tracing_try_init();
        let store = create_store();
        let blocks = [
            create_block(&ipld!(0)),
            create_block(&ipld!(1)),
            create_block(&ipld!(2)),
            create_block(&ipld!(3)),
        ];
        let mut subscription = store.subscribe();
        store.insert(&blocks[0], None).await.unwrap();
        store.insert(&blocks[1], None).await.unwrap();
        store.evict().await.unwrap();
        assert_unpinned!(&store, &blocks[0]);
        assert_unpinned!(&store, &blocks[1]);
        store.insert(&blocks[2], None).await.unwrap();
        store.evict().await.unwrap();
        assert_evicted!(&store, &blocks[0]);
        assert_unpinned!(&store, &blocks[1]);
        assert_unpinned!(&store, &blocks[2]);
        store.get(&blocks[1]).await.unwrap();
        store.insert(&blocks[3], None).await.unwrap();
        store.evict().await.unwrap();
        assert_unpinned!(&store, &blocks[1]);
        assert_evicted!(&store, &blocks[2]);
        assert_unpinned!(&store, &blocks[3]);
        assert_eq!(
            subscription.next().await,
            Some(StorageEvent::Remove(*blocks[0].cid()))
        );
        assert_eq!(
            subscription.next().await,
            Some(StorageEvent::Remove(*blocks[2].cid()))
        );
    }

    #[async_std::test]
    #[allow(clippy::many_single_char_names)]
    async fn test_store_unpin() {
        tracing_try_init();
        let store = create_store();
        let a = create_block(&ipld!({ "a": [] }));
        let b = create_block(&ipld!({ "b": [a.cid()] }));
        let c = create_block(&ipld!({ "c": [a.cid()] }));
        let x = alias!(x);
        let y = alias!(y);
        store.insert(&a, None).await.unwrap();
        store.insert(&b, None).await.unwrap();
        store.insert(&c, None).await.unwrap();
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
        tracing_try_init();
        let store = create_store();
        let a = create_block(&ipld!({ "a": [] }));
        let b = create_block(&ipld!({ "b": [a.cid()] }));
        let x = alias!(x);
        let y = alias!(y);
        store.insert(&a, None).await.unwrap();
        store.insert(&b, None).await.unwrap();
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
