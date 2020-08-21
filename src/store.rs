use crate::config::Config;
use crate::gc::GarbageCollector;
use crate::network::Network;
use crate::storage::{Metadata, Storage};
use async_std::future::timeout;
use async_std::task;
use core::marker::PhantomData;
use libipld::block::Block;
use libipld::cid::Cid;
use libipld::codec::Codec;
use libipld::error::{BlockNotFound, Result};
use libipld::multihash::MultihashDigest;
use libipld::store::{AliasStore, ReadonlyStore, Store as WritableStore, StoreResult};
use libp2p::core::{Multiaddr, PeerId};
use sled::IVec;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Store<C: Codec, M: MultihashDigest> {
    _marker: PhantomData<(C, M)>,
    storage: Storage,
    timeout: Duration,
    peer_id: PeerId,
    address: Multiaddr,
}

impl<C: Codec, M: MultihashDigest> Store<C, M> {
    pub fn new(config: Config) -> Result<Self> {
        let Config {
            tree,
            network,
            timeout,
        } = config;
        let node_name = network.node_name.clone();
        let peer_id = network.peer_id();
        let storage = Storage::new(tree)?;
        let (network, address) = task::block_on(Network::<C, M>::new(network, storage.clone()))?;

        let address_str = address.to_string();
        let peer_id_str = peer_id.to_base58();
        task::spawn(async move {
            // make sure async std logs the right task id
            log::info!(
                "{}: listening on {} as {}",
                node_name,
                address_str,
                peer_id_str
            );
            network.await;
        });

        task::spawn(GarbageCollector::new(storage.clone()));

        Ok(Self {
            _marker: PhantomData,
            storage,
            timeout,
            peer_id,
            address,
        })
    }

    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    pub fn address(&self) -> &Multiaddr {
        &self.address
    }

    pub fn blocks(&self) -> impl Iterator<Item = Result<Cid>> {
        self.storage.blocks()
    }

    pub fn metadata(&self, cid: &Cid) -> Result<Metadata> {
        self.storage.metadata(cid)
    }

    pub fn get_local(&self, cid: &Cid) -> Result<Option<IVec>> {
        self.storage.get_local(cid)
    }
}

impl<C: Codec, M: MultihashDigest> ReadonlyStore for Store<C, M> {
    type Codec = C;
    type Multihash = M;

    fn get<'a>(&'a self, cid: Cid) -> StoreResult<'a, Block<C, M>> {
        Box::pin(async move {
            let future = self.storage.get(&cid);
            let block = timeout(self.timeout, future)
                .await
                .map_err(|_| BlockNotFound(cid.to_string()))??;
            Ok(Block::new(cid, block.to_vec().into_boxed_slice()))
        })
    }
}

impl<C: Codec, M: MultihashDigest> WritableStore for Store<C, M> {
    fn insert<'a>(&'a self, block: &'a Block<C, M>) -> StoreResult<'a, ()> {
        Box::pin(async move { Ok(self.storage.insert(block)?) })
    }

    fn insert_batch<'a>(&'a self, batch: &'a [Block<C, M>]) -> StoreResult<'a, Cid> {
        Box::pin(async move { Ok(self.storage.insert_batch(batch)?) })
    }

    fn flush(&self) -> StoreResult<'_, ()> {
        Box::pin(async move { Ok(self.storage.flush().await?) })
    }

    fn unpin<'a>(&'a self, cid: &'a Cid) -> StoreResult<'a, ()> {
        Box::pin(async move { Ok(self.storage.unpin(cid)?) })
    }
}

impl<C: Codec, M: MultihashDigest> AliasStore for Store<C, M> {
    fn alias<'a>(&'a self, alias: &'a [u8], block: &'a Block<C, M>) -> StoreResult<'a, ()> {
        Box::pin(async move { self.storage.alias(alias, block) })
    }

    fn unalias<'a>(&'a self, alias: &'a [u8]) -> StoreResult<'a, ()> {
        Box::pin(async move { self.storage.unalias(alias) })
    }

    fn resolve<'a>(&'a self, alias: &'a [u8]) -> StoreResult<'a, Option<Cid>> {
        Box::pin(async move { self.storage.resolve(alias) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libipld::block::{Block, Visibility};
    use libipld::cbor::DagCborCodec;
    use libipld::cid::DAG_CBOR;
    use libipld::cid::RAW;
    use libipld::codec_impl::Multicodec;
    use libipld::ipld;
    use libipld::ipld::Ipld;
    use libipld::multihash::{Multihash, MultihashDigest, SHA2_256};
    use libipld::raw::RawCodec;
    use std::time::Duration;
    use tempdir::TempDir;

    fn create_store(
        bootstrap: Vec<(Multiaddr, PeerId)>,
    ) -> (Store<Multicodec, Multihash>, TempDir) {
        let tmp = TempDir::new("").unwrap();
        let mut config = Config::from_path_local(tmp.path()).unwrap();
        config.network.enable_mdns = bootstrap.is_empty();
        config.network.boot_nodes = bootstrap;
        let store = Store::new(config).unwrap();
        (store, tmp)
    }

    fn create_block(bytes: &[u8]) -> Block<Multicodec, Multihash> {
        Block::<RawCodec, _>::encode(RAW, SHA2_256, bytes)
            .unwrap()
            .into_codec()
            .unwrap()
    }

    #[async_std::test]
    async fn test_local_store() {
        env_logger::try_init().ok();
        let (store, _) = create_store(vec![]);
        let mut block = create_block(b"test_local_store");
        block.set_visibility(Visibility::Private);
        store.insert(&block).await.unwrap();
        let block2 = store.get(block.cid).await.unwrap();
        assert_eq!(block.data, block2.data);
    }

    #[async_std::test]
    #[cfg(not(target_os = "macos"))] // mdns doesn't work on macos in github actions
    async fn test_exchange_mdns() {
        env_logger::try_init().ok();
        let (store1, _) = create_store(vec![]);
        let (store2, _) = create_store(vec![]);
        let mut block = create_block(b"test_exchange_mdns");
        block.set_visibility(Visibility::Private);
        store1.insert(&block).await.unwrap();
        let block2 = store2.get(block.cid).await.unwrap();
        assert_eq!(block.data, block2.data);
    }

    #[async_std::test]
    #[cfg(not(target_os = "macos"))] // mdns doesn't work on macos in github action
    async fn test_received_want_before_insert() {
        env_logger::try_init().ok();
        let (store1, _) = create_store(vec![]);
        let (store2, _) = create_store(vec![]);
        let block = create_block(b"test_received_want_before_insert");

        let get_cid = block.cid.clone();
        let get = task::spawn(async move { store2.get(get_cid).await });

        task::sleep(Duration::from_millis(100)).await;

        store1.insert(&block).await.unwrap();

        let block2 = get.await.unwrap();
        assert_eq!(block.data, block2.data);
    }

    #[async_std::test]
    async fn test_exchange_kad() {
        let logger = env_logger::Builder::from_default_env().build();
        async_log::Logger::wrap(logger, || {
            let task_id = async_std::task::current().id();
            format!("{}", task_id).parse().unwrap()
        })
        .start(log::LevelFilter::Trace)
        .ok();

        let (store, _) = create_store(vec![]);
        // make sure bootstrap node has started
        task::sleep(Duration::from_millis(500)).await;
        let bootstrap = vec![(store.address().clone(), store.peer_id().clone())];
        let (store1, _) = create_store(bootstrap.clone());
        let (store2, _) = create_store(bootstrap);
        let block = create_block(b"test_exchange_kad");
        store1.insert(&block).await.unwrap();
        // make insert had enough time to propagate
        task::sleep(Duration::from_millis(500)).await;
        let block2 = store2.get(block.cid).await.unwrap();
        assert_eq!(block.data, block2.data);
    }

    #[async_std::test]
    async fn test_provider_not_found() {
        env_logger::try_init().ok();
        let (store1, _) = create_store(vec![]);
        let block = create_block(b"test_provider_not_found");
        if store1
            .get(block.cid)
            .await
            .unwrap_err()
            .downcast_ref::<BlockNotFound>()
            .is_none()
        {
            panic!("expected block not found error");
        }
    }

    async fn get<C: Codec, M: MultihashDigest>(store: &Store<C, M>, cid: &Cid) -> Option<Ipld> {
        store
            .storage
            .get_local(cid)
            .unwrap()
            .map(|bytes| DagCborCodec.decode_ipld(&bytes).unwrap())
    }

    async fn insert<C: Codec, M: MultihashDigest>(store: &Store<C, M>, ipld: &Ipld) -> Cid {
        let block = Block::encode_ipld(DAG_CBOR, SHA2_256, ipld).unwrap();
        store.insert(&block).await.unwrap();
        block.cid
    }

    #[async_std::test]
    async fn test_gc() {
        env_logger::try_init().ok();
        let (store, _) = create_store(vec![]);
        let a = insert(&store, &ipld!({ "a": [] })).await;
        let b = insert(&store, &ipld!({ "b": [&a] })).await;
        store.unpin(&a).await.unwrap();
        task::sleep(Duration::from_millis(200)).await;
        let c = insert(&store, &ipld!({ "c": [&a] })).await;
        assert!(get(&store, &a).await.is_some());
        assert!(get(&store, &b).await.is_some());
        assert!(get(&store, &c).await.is_some());
        store.unpin(&b).await.unwrap();
        task::sleep(Duration::from_millis(200)).await;
        assert!(get(&store, &a).await.is_some());
        assert!(get(&store, &b).await.is_none());
        assert!(get(&store, &c).await.is_some());
        store.unpin(&c).await.unwrap();
        task::sleep(Duration::from_millis(200)).await;
        assert!(get(&store, &a).await.is_none());
        assert!(get(&store, &b).await.is_none());
        assert!(get(&store, &c).await.is_none());
    }

    #[async_std::test]
    async fn test_gc_2() {
        env_logger::try_init().ok();
        let (store, _) = create_store(vec![]);
        let a = insert(&store, &ipld!({ "a": [] })).await;
        let b = insert(&store, &ipld!({ "b": [&a] })).await;
        store.unpin(&a).await.unwrap();
        task::sleep(Duration::from_millis(200)).await;
        let c = insert(&store, &ipld!({ "b": [&a] })).await;
        assert!(get(&store, &a).await.is_some());
        assert!(get(&store, &b).await.is_some());
        assert!(get(&store, &c).await.is_some());
        store.unpin(&b).await.unwrap();
        task::sleep(Duration::from_millis(200)).await;
        assert!(get(&store, &a).await.is_some());
        assert!(get(&store, &b).await.is_some());
        assert!(get(&store, &c).await.is_some());
        store.unpin(&c).await.unwrap();
        task::sleep(Duration::from_millis(200)).await;
        assert!(get(&store, &a).await.is_none());
        assert!(get(&store, &b).await.is_none());
        assert!(get(&store, &c).await.is_none());
    }
}
