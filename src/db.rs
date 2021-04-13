use async_global_executor::Task;
pub use ipfs_sqlite_block_store::TempPin;
use ipfs_sqlite_block_store::{
    cache::SqliteCacheTracker, BlockStore, Config, SizeTargets, Synchronous,
};
use lazy_static::lazy_static;
use libipld::codec::References;
use libipld::store::StoreParams;
use libipld::{Block, Cid, Ipld, Result};
use parking_lot::{Condvar, Mutex};
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry};
use std::future::Future;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Storage configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageConfig {
    /// The path to use for the block store. If it is `None` an in-memory block store
    /// will be used.
    pub path: Option<PathBuf>,
    /// The target number of blocks.
    ///
    /// Up to this number, the store will retain everything even if
    /// not pinned. Once this number is exceeded, the store will run garbage collection
    /// of all unpinned blocks until the block criterion is met again.
    ///
    /// To completely disable storing of non-pinned blocks, set this to 0. Even then,
    /// the store will never delete pinned blocks.
    pub cache_size_blocks: u64,
    /// The target store size.
    ///
    /// Up to this size, the store will retain everything even if not pinned. Once this
    /// size is exceeded, the store will run garbage collection of all unpinned blocks
    /// until the size criterion is met again.
    ///
    /// The store will never delete pinned blocks.
    pub cache_size_bytes: u64,
    /// The interval at which the garbage collector is run.
    ///
    /// Note that this is implemented as delays between gcs, so it will not run exactly at this
    /// interval, but there will be some drift if gc takes long.
    pub gc_interval: Duration,
    /// The minimum number of blocks to collect in any case.
    ///
    /// Using this parameter, it is possible to guarantee a minimum rate with which the gc will
    /// be able to keep up. It is `gc_min_blocks` / `gc_interval`.
    pub gc_min_blocks: usize,
    /// The target maximum gc duration of a single garbage collector run.
    ///
    /// This can not be guaranteed, since we guarantee to collect at least `gc_min_blocks`. But
    /// as soon as this duration is exceeded, the incremental gc will stop doing additional work.
    pub gc_target_duration: Duration,
}

impl StorageConfig {
    /// Creates a new `StorageConfig`.
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

#[derive(Clone)]
pub struct StorageService<S: StoreParams> {
    _marker: PhantomData<S>,
    store: Arc<Mutex<BlockStore>>,
    gc_target_duration: Duration,
    gc_min_blocks: usize,
    _gc_task: Arc<Task<()>>,
    exit: Arc<(Mutex<bool>, Condvar)>,
}

impl<S: StoreParams> Drop for StorageService<S> {
    fn drop(&mut self) {
        *self.exit.0.lock() = true;
        self.exit.1.notify_all();
    }
}

impl<S: StoreParams> StorageService<S>
where
    Ipld: References<S::Codecs>,
{
    pub fn open(config: StorageConfig) -> Result<Self> {
        let size = SizeTargets::new(config.cache_size_blocks, config.cache_size_bytes);
        let store_config = Config::default()
            .with_size_targets(size)
            .with_pragma_synchronous(Synchronous::Normal);
        let store = if let Some(path) = config.path {
            let tracker = SqliteCacheTracker::open(&path, |access, _| Some(access))?;
            BlockStore::open(path, store_config.with_cache_tracker(tracker))?
        } else {
            let tracker = SqliteCacheTracker::memory(|access, _| Some(access))?;
            BlockStore::memory(store_config.with_cache_tracker(tracker))?
        };
        let store = Arc::new(Mutex::new(store));
        let gc = store.clone();
        let gc_interval = config.gc_interval;
        let gc_min_blocks = config.gc_min_blocks;
        let gc_target_duration = config.gc_target_duration;
        let exit = Arc::new((Mutex::new(false), Condvar::new()));
        let exit2 = exit.clone();
        let gc_task =
            async_global_executor::spawn(async_global_executor::spawn_blocking(move || {
                enum Phase {
                    Gc,
                    Delete,
                }
                let mut phase = Phase::Gc;
                loop {
                    let mut should_exit = exit.0.lock();
                    let timeout = exit.1.wait_for(&mut should_exit, gc_interval / 2);
                    if *should_exit {
                        break;
                    }
                    if timeout.timed_out() {
                        match phase {
                            Phase::Gc => {
                                tracing::debug!("gc_loop running incremental gc");
                                gc.lock()
                                    .incremental_gc(gc_min_blocks, gc_target_duration)
                                    .ok();
                                phase = Phase::Delete;
                            }
                            Phase::Delete => {
                                tracing::debug!("gc_loop running incremental delete orphaned");
                                gc.lock()
                                    .incremental_delete_orphaned(gc_min_blocks, gc_target_duration)
                                    .ok();
                                phase = Phase::Gc;
                            }
                        }
                    }
                }
            }));
        Ok(Self {
            _marker: PhantomData,
            gc_target_duration: config.gc_target_duration,
            gc_min_blocks: config.gc_min_blocks,
            store,
            _gc_task: Arc::new(gc_task),
            exit: exit2,
        })
    }

    pub fn create_temp_pin(&self) -> Result<TempPin> {
        observe_query::<_, std::io::Error, _>("create_temp_pin", || {
            Ok(self.store.lock().temp_pin())
        })
    }

    pub fn temp_pin(
        &self,
        temp: &TempPin,
        iter: impl IntoIterator<Item = Cid> + Send + 'static,
    ) -> Result<()> {
        observe_query("temp_pin", || {
            self.store.lock().extend_temp_pin(&temp, iter)
        })
    }

    pub fn iter(&self) -> Result<impl Iterator<Item = Cid>> {
        let cids = observe_query("iter", || self.store.lock().get_block_cids::<Vec<Cid>>())?;
        Ok(cids.into_iter())
    }

    pub fn contains(&self, cid: &Cid) -> Result<bool> {
        observe_query("contains", || self.store.lock().has_block(cid))
    }

    pub fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        observe_query("get", || self.store.lock().get_block(cid))
    }

    pub fn insert(&self, block: &Block<S>) -> Result<()> {
        observe_query("insert", || self.store.lock().put_block(block, None))
    }

    pub async fn evict(&self) -> Result<()> {
        let store = self.store.clone();
        let gc_min_blocks = self.gc_min_blocks;
        let gc_target_duration = self.gc_target_duration;
        async_global_executor::spawn_blocking(move || {
            while !store
                .lock()
                .incremental_gc(gc_min_blocks, gc_target_duration)?
            {}
            while !store
                .lock()
                .incremental_delete_orphaned(gc_min_blocks, gc_target_duration)?
            {}
            Ok(())
        })
        .await
    }

    pub fn alias(&self, alias: &[u8], cid: Option<&Cid>) -> Result<()> {
        observe_query("alias", || self.store.lock().alias(alias, cid))
    }

    pub fn resolve(&self, alias: &[u8]) -> Result<Option<Cid>> {
        observe_query("resolve", || self.store.lock().resolve(alias))
    }

    pub fn reverse_alias(&self, cid: &Cid) -> Result<Option<Vec<Vec<u8>>>> {
        observe_query("reverse_alias", || self.store.lock().reverse_alias(cid))
    }

    pub fn missing_blocks(&self, cid: &Cid) -> Result<Vec<Cid>> {
        observe_query("missing_blocks", || {
            self.store.lock().get_missing_blocks(cid)
        })
    }

    pub async fn flush(&self) -> Result<()> {
        let store = self.store.clone();
        let flush = async_global_executor::spawn_blocking(move || store.lock().flush());
        observe_future("flush", flush).await
    }

    pub fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(QUERIES_TOTAL.clone()))?;
        registry.register(Box::new(QUERY_DURATION.clone()))?;
        registry.register(Box::new(SqliteStoreCollector::new(self.store.clone())))?;
        Ok(())
    }
}

lazy_static! {
    pub static ref QUERIES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "block_store_queries_total",
            "Number of block store requests labelled by type."
        ),
        &["type"],
    )
    .unwrap();
    pub static ref QUERY_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "block_store_query_duration",
            "Duration of store queries labelled by type.",
        ),
        &["type"],
    )
    .unwrap();
}

fn observe_query<T, E, F>(name: &'static str, query: F) -> Result<T>
where
    E: std::error::Error + Send + Sync + 'static,
    F: FnOnce() -> Result<T, E>,
{
    QUERIES_TOTAL.with_label_values(&[name]).inc();
    let timer = QUERY_DURATION.with_label_values(&[name]).start_timer();
    let res = query();
    if res.is_ok() {
        timer.observe_duration();
    } else {
        timer.stop_and_discard();
    }
    Ok(res?)
}

async fn observe_future<T, E, F>(name: &'static str, query: F) -> Result<T>
where
    E: std::error::Error + Send + Sync + 'static,
    F: Future<Output = Result<T, E>>,
{
    QUERIES_TOTAL.with_label_values(&[name]).inc();
    let timer = QUERY_DURATION.with_label_values(&[name]).start_timer();
    let res = query.await;
    if res.is_ok() {
        timer.observe_duration();
    } else {
        timer.stop_and_discard();
    }
    Ok(res?)
}

struct SqliteStoreCollector {
    desc: Desc,
    store: Arc<Mutex<BlockStore>>,
}

impl Collector for SqliteStoreCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut family = vec![];

        if let Ok(stats) = self.store.lock().get_store_stats() {
            let store_block_count =
                IntGauge::new("block_store_block_count", "Number of stored blocks").unwrap();
            store_block_count.set(stats.count() as _);
            family.push(store_block_count.collect()[0].clone());

            let store_size =
                IntGauge::new("block_store_size", "Size in bytes of stored blocks").unwrap();
            store_size.set(stats.size() as _);
            family.push(store_size.collect()[0].clone());
        }

        family
    }
}

impl SqliteStoreCollector {
    pub fn new(store: Arc<Mutex<BlockStore>>) -> Self {
        let desc = Desc::new(
            "block_store_stats".into(),
            ".".into(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        Self { store, desc }
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
            assert_eq!($store.reverse_alias($block.cid()).unwrap(), None);
        };
    }

    macro_rules! assert_pinned {
        ($store:expr, $block:expr) => {
            assert_eq!(
                $store
                    .reverse_alias($block.cid())
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
                    .reverse_alias($block.cid())
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
        store.insert(&blocks[0]).unwrap();
        store.insert(&blocks[1]).unwrap();
        store.flush().await.unwrap();
        store.evict().await.unwrap();
        assert_unpinned!(&store, &blocks[0]);
        assert_unpinned!(&store, &blocks[1]);
        store.insert(&blocks[2]).unwrap();
        store.flush().await.unwrap();
        store.evict().await.unwrap();
        assert_evicted!(&store, &blocks[0]);
        assert_unpinned!(&store, &blocks[1]);
        assert_unpinned!(&store, &blocks[2]);
        store.get(blocks[1].cid()).unwrap();
        store.insert(&blocks[3]).unwrap();
        store.flush().await.unwrap();
        store.evict().await.unwrap();
        assert_unpinned!(&store, &blocks[1]);
        assert_evicted!(&store, &blocks[2]);
        assert_unpinned!(&store, &blocks[3]);
    }

    #[async_std::test]
    #[allow(clippy::many_single_char_names)]
    async fn test_store_unpin() {
        tracing_try_init();
        let store = create_store();
        let a = create_block(&ipld!({ "a": [] }));
        let b = create_block(&ipld!({ "b": [a.cid()] }));
        let c = create_block(&ipld!({ "c": [a.cid()] }));
        let x = alias!(x).as_bytes().to_vec();
        let y = alias!(y).as_bytes().to_vec();
        store.insert(&a).unwrap();
        store.insert(&b).unwrap();
        store.insert(&c).unwrap();
        store.alias(&x, Some(b.cid())).unwrap();
        store.alias(&y, Some(c.cid())).unwrap();
        store.flush().await.unwrap();
        assert_pinned!(&store, &a);
        assert_pinned!(&store, &b);
        assert_pinned!(&store, &c);
        store.alias(&x, None).unwrap();
        store.flush().await.unwrap();
        assert_pinned!(&store, &a);
        assert_unpinned!(&store, &b);
        assert_pinned!(&store, &c);
        store.alias(&y, None).unwrap();
        store.flush().await.unwrap();
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
        let x = alias!(x).as_bytes().to_vec();
        let y = alias!(y).as_bytes().to_vec();
        store.insert(&a).unwrap();
        store.insert(&b).unwrap();
        store.alias(&x, Some(b.cid())).unwrap();
        store.alias(&y, Some(b.cid())).unwrap();
        store.flush().await.unwrap();
        assert_pinned!(&store, &a);
        assert_pinned!(&store, &b);
        store.alias(&x, None).unwrap();
        store.flush().await.unwrap();
        assert_pinned!(&store, &a);
        assert_pinned!(&store, &b);
        store.alias(&y, None).unwrap();
        store.flush().await.unwrap();
        assert_unpinned!(&store, &a);
        assert_unpinned!(&store, &b);
    }
}
