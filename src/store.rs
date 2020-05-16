use crate::config::Config;
use crate::error::Error;
use crate::gc::GarbageCollector;
use crate::network::Network;
use crate::storage::Storage;
use async_std::task;
use libipld::cid::Cid;
use libipld::store::{AliasStore, ReadonlyStore, Store as WritableStore, StoreResult, Visibility};
use libp2p::core::{Multiaddr, PeerId};

#[derive(Clone, Debug)]
pub struct Store {
    storage: Storage,
    peer_id: PeerId,
    address: Multiaddr,
}

impl Store {
    pub fn new(config: Config) -> Result<Self, Error> {
        let Config { tree, network } = config;
        let peer_id = network.peer_id();
        let storage = Storage::new(tree);
        let (network, address) = Network::new(network, storage.clone())?;

        let address_str = address.to_string();
        let peer_id_str = peer_id.to_base58();
        task::spawn(async move {
            // make sure async std logs the right task id
            log::info!("Listening on {} as {}", address_str, peer_id_str);
            network.await;
        });

        task::spawn(GarbageCollector::new(storage.clone()));

        Ok(Self {
            storage,
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
}

impl ReadonlyStore for Store {
    fn get<'a>(&'a self, cid: &'a Cid) -> StoreResult<'a, Box<[u8]>> {
        Box::pin(async move { Ok(self.storage.get(cid).await?.to_vec().into_boxed_slice()) })
    }
}

impl WritableStore for Store {
    fn insert<'a>(
        &'a self,
        cid: &'a Cid,
        data: Box<[u8]>,
        visibility: Visibility,
    ) -> StoreResult<'a, ()> {
        Box::pin(async move { Ok(self.storage.insert(cid, data.into(), visibility)?) })
    }

    fn flush(&self) -> StoreResult<'_, ()> {
        Box::pin(async move { Ok(self.storage.flush().await?) })
    }

    fn unpin<'a>(&'a self, cid: &'a Cid) -> StoreResult<'a, ()> {
        Box::pin(async move { Ok(self.storage.unpin(cid)?) })
    }
}

impl AliasStore for Store {
    fn alias<'a>(
        &'a self,
        alias: &'a [u8],
        cid: &'a Cid,
        visibility: Visibility,
    ) -> StoreResult<'a, ()> {
        Box::pin(async move { Ok(self.storage.alias(alias, cid, visibility)?) })
    }

    fn unalias<'a>(&'a self, alias: &'a [u8]) -> StoreResult<'a, ()> {
        Box::pin(async move { Ok(self.storage.unalias(alias)?) })
    }

    fn resolve<'a>(&'a self, alias: &'a [u8]) -> StoreResult<'a, Option<Cid>> {
        Box::pin(async move { Ok(self.storage.resolve(alias)?) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libipld::block::{decode, encode, Block};
    use libipld::cbor::DagCbor;
    use libipld::cid::Codec;
    use libipld::ipld;
    use libipld::ipld::Ipld;
    use libipld::multihash::Sha2_256;
    use std::time::Duration;
    use tempdir::TempDir;

    fn create_store(bootstrap: Vec<(Multiaddr, PeerId)>) -> (Store, TempDir) {
        let tmp = TempDir::new("").unwrap();
        let mut config = Config::from_path(tmp.path()).unwrap();
        config.network.enable_mdns = bootstrap.is_empty();
        config.network.bootstrap_nodes = bootstrap;
        let store = Store::new(config).unwrap();
        (store, tmp)
    }

    fn create_block(bytes: &[u8]) -> (Cid, Box<[u8]>) {
        let hash = Sha2_256::digest(&bytes);
        let cid = Cid::new_v1(Codec::Raw, hash);
        let data = bytes.to_vec().into_boxed_slice();
        (cid, data)
    }

    #[async_std::test]
    async fn test_local_store() {
        env_logger::try_init().ok();
        let (store, _) = create_store(vec![]);
        let (cid, data) = create_block(b"hello world");
        store
            .insert(&cid, data.clone(), Visibility::Private)
            .await
            .unwrap();
        let data2 = store.get(&cid).await.unwrap();
        assert_eq!(data, data2);
    }

    #[async_std::test]
    async fn test_exchange_mdns() {
        env_logger::try_init().ok();
        let (store1, _) = create_store(vec![]);
        let (store2, _) = create_store(vec![]);
        let (cid, data) = create_block(b"hello world");
        store1
            .insert(&cid, data.clone(), Visibility::Private)
            .await
            .unwrap();
        let data2 = store2.get(&cid).await.unwrap();
        assert_eq!(data, data2);
    }

    #[async_std::test]
    async fn test_received_want_before_insert() {
        env_logger::try_init().ok();
        let (store1, _) = create_store(vec![]);
        let (store2, _) = create_store(vec![]);
        let (cid, data) = create_block(b"hello world");

        let get_cid = cid.clone();
        let get = task::spawn(async move { store2.get(&get_cid).await });

        task::sleep(Duration::from_millis(100)).await;

        store1
            .insert(&cid, data.clone(), Visibility::Public)
            .await
            .unwrap();

        let data2 = get.await.unwrap();
        assert_eq!(data, data2);
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
        task::sleep(Duration::from_secs(5)).await;
        let bootstrap = vec![(store.address().clone(), store.peer_id().clone())];
        let (store1, _) = create_store(bootstrap.clone());
        task::sleep(Duration::from_secs(5)).await;
        let (store2, _) = create_store(bootstrap);
        task::sleep(Duration::from_secs(5)).await;
        let (cid, data) = create_block(b"hello world");
        store1
            .insert(&cid, data.clone(), Visibility::Public)
            .await
            .unwrap();
        task::sleep(Duration::from_secs(5)).await;
        let data2 = store2.get(&cid).await.unwrap();
        assert_eq!(data, data2);
    }

    #[async_std::test]
    #[ignore]
    async fn test_provider_not_found_kad() {
        // TODO
    }

    async fn get(store: &Store, cid: &Cid) -> Option<Ipld> {
        store.storage.get_local(cid).unwrap()
            .map(|bytes| decode::<DagCbor, Ipld>(cid, &bytes).unwrap())
    }

    async fn insert(store: &Store, ipld: &Ipld) -> Cid {
        let Block { cid, data } = encode::<DagCbor, Sha2_256, Ipld>(ipld).unwrap();
        store.insert(&cid, data, Visibility::Public).await.unwrap();
        cid
    }

    #[async_std::test]
    async fn test_gc() {
        env_logger::try_init().ok();
        let (store, _) = create_store(vec![]);
        let a = insert(&store, &ipld!({ "a": [] })).await;
        let b = insert(&store, &ipld!({ "b": [&a] })).await;
        store.unpin(&a).await.unwrap();
        task::sleep(Duration::from_millis(100)).await;
        let c = insert(&store, &ipld!({ "c": [&a] })).await;
        assert!(get(&store, &a).await.is_some());
        assert!(get(&store, &b).await.is_some());
        assert!(get(&store, &c).await.is_some());
        store.unpin(&b).await.unwrap();
        task::sleep(Duration::from_millis(100)).await;
        assert!(get(&store, &a).await.is_some());
        assert!(get(&store, &b).await.is_none());
        assert!(get(&store, &c).await.is_some());
        store.unpin(&c).await.unwrap();
        task::sleep(Duration::from_millis(100)).await;
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
        task::sleep(Duration::from_millis(100)).await;
        let c = insert(&store, &ipld!({ "b": [&a] })).await;
        assert!(get(&store, &a).await.is_some());
        assert!(get(&store, &b).await.is_some());
        assert!(get(&store, &c).await.is_some());
        store.unpin(&b).await.unwrap();
        task::sleep(Duration::from_millis(100)).await;
        assert!(get(&store, &a).await.is_some());
        assert!(get(&store, &b).await.is_some());
        assert!(get(&store, &c).await.is_some());
        store.unpin(&c).await.unwrap();
        task::sleep(Duration::from_millis(100)).await;
        assert!(get(&store, &a).await.is_none());
        assert!(get(&store, &b).await.is_none());
        assert!(get(&store, &c).await.is_none());
    }
}
