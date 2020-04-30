use crate::config::StoreConfig;
use crate::network::Network;
use crate::storage::Storage;
use async_std::task;
use libipld_core::cid::Cid;
use libipld_core::store::{AliasStore, ReadonlyStore, Store, StoreResult, Visibility};
use libp2p::core::{Multiaddr, PeerId};

pub struct EmbeddedStore {
    storage: Storage,
    peer_id: PeerId,
    address: Multiaddr,
}

impl EmbeddedStore {
    pub fn new(config: StoreConfig) -> Result<Self, std::io::Error> {
        let StoreConfig { tree, network } = config;
        let peer_id = network.peer_id();
        let storage = Storage::new(tree);
        let (network, address) = Network::new(network, storage.clone())?;
        log::info!(
            "Listening on {} as {}",
            address.to_string(),
            peer_id.to_base58()
        );
        task::spawn(network);
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

    pub fn get_from_peer<'a>(
        &'a self,
        _peer_id: &'a PeerId,
        _cid: &'a Cid,
    ) -> StoreResult<'a, Option<Box<[u8]>>> {
        // TODO
        Box::pin(async move { Ok(Some(b"hello world".to_vec().into_boxed_slice())) })
    }
}

impl ReadonlyStore for EmbeddedStore {
    fn get<'a>(&'a self, cid: &'a Cid) -> StoreResult<'a, Box<[u8]>> {
        Box::pin(async move { Ok(self.storage.get(cid).await?.to_vec().into_boxed_slice()) })
    }
}

impl Store for EmbeddedStore {
    fn insert<'a>(
        &'a self,
        cid: &'a Cid,
        data: Box<[u8]>,
        visibility: Visibility,
    ) -> StoreResult<'a, ()> {
        Box::pin(async move { Ok(self.storage.insert(cid, data.into(), visibility)?) })
    }

    fn flush<'a>(&'a self) -> StoreResult<'a, ()> {
        Box::pin(async move { Ok(self.storage.flush().await?) })
    }

    fn unpin<'a>(&'a self, cid: &'a Cid) -> StoreResult<'a, ()> {
        Box::pin(async move { Ok(self.storage.unpin(cid)?) })
    }
}

impl AliasStore for EmbeddedStore {
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
    use libipld_core::cid::Codec;
    use libipld_core::multihash::Sha2_256;
    use std::time::Duration;
    use tempdir::TempDir;

    fn create_store(bootstrap: Vec<(Multiaddr, PeerId)>) -> (EmbeddedStore, TempDir) {
        let tmp = TempDir::new("").unwrap();
        let mut config = StoreConfig::from_path(tmp.path()).unwrap();
        config.network.enable_mdns = bootstrap.is_empty();
        config.network.bootstrap_nodes = bootstrap;
        let store = EmbeddedStore::new(config).unwrap();
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
    #[ignore]
    async fn test_exchange_private() {
        // TODO
        env_logger::try_init().ok();
        let (store1, _) = create_store(vec![]);
        let (store2, _) = create_store(vec![]);
        let (cid, data) = create_block(b"hello world");
        store1
            .insert(&cid, data.clone(), Visibility::Private)
            .await
            .unwrap();
        let peer_id = store1.peer_id();
        let data2 = store2.get_from_peer(peer_id, &cid).await.unwrap().unwrap();
        assert_eq!(data, data2);
    }

    #[async_std::test]
    async fn test_exchange_mdns() {
        env_logger::try_init().ok();
        let (store1, _) = create_store(vec![]);
        let (store2, _) = create_store(vec![]);
        let (cid, data) = create_block(b"hello world");
        store1
            .insert(&cid, data.clone(), Visibility::Public)
            .await
            .unwrap();
        let data2 = store2.get(&cid).await.unwrap();
        assert_eq!(data, data2);
    }

    #[async_std::test]
    #[ignore]
    async fn test_received_want_before_insert() {
        // TODO
    }

    #[async_std::test]
    #[ignore]
    async fn test_exchange_kad() {
        // TODO
        env_logger::try_init().ok();
        let (store, _) = create_store(vec![]);
        let bootstrap = vec![(store.address().clone(), store.peer_id().clone())];
        let (store1, _) = create_store(bootstrap.clone());
        let (store2, _) = create_store(bootstrap);
        let (cid, data) = create_block(b"hello world");
        task::sleep(Duration::from_secs(10)).await;
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

    #[async_std::test]
    #[ignore]
    async fn test_ping_closes_connection() {
        // TODO
    }
}
