use crate::network::{Network, NetworkConfig};
use async_std::task;
use async_trait::async_trait;
use futures::channel::{mpsc, oneshot};
use futures::sink::SinkExt;
use ipfs_embed_db::BlockStore;
use libipld::block::Block;
use libipld::cid::Cid;
use libipld::codec::{Codec, Decode};
use libipld::error::{BlockNotFound, Result};
use libipld::ipld::Ipld;
use libipld::multihash::MultihashDigest;
use libipld::store::{Store, StoreParams, Transaction};
use libp2p::core::{Multiaddr, PeerId};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct Ipfs<C, M> {
    _marker: PhantomData<(C, M)>,
    storage: Arc<BlockStore>,
    network: mpsc::Sender<(Cid, oneshot::Sender<Block<Self>>)>,
    peer_id: PeerId,
    address: Multiaddr,
}

impl<C: Codec, M: MultihashDigest> StoreParams for Ipfs<C, M> {
    type Codecs = C;
    type Hashes = M;
    // maximum block size supported by parity db is `u16::MAX`.
    const MAX_BLOCK_SIZE: usize = u16::MAX as usize;
}

impl<C: Codec, M: MultihashDigest + Unpin> Ipfs<C, M>
where
    Ipld: Decode<C>,
{
    pub fn new(path: &Path, config: NetworkConfig) -> Result<Self> {
        let node_name = config.node_name.clone();
        let peer_id = config.peer_id();
        let storage = Arc::new(BlockStore::open(path)?);
        let (tx, rx) = mpsc::channel(0);
        let (network, address) = task::block_on(Network::<Self>::new(config, storage.clone(), rx))?;

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

        Ok(Self {
            _marker: PhantomData,
            storage,
            network: tx,
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

#[async_trait]
impl<C: Codec, M: MultihashDigest> Store for Ipfs<C, M>
where
    Ipld: Decode<C>,
{
    type Params = Self;

    async fn get(&self, cid: &Cid) -> Result<Block<Self>> {
        if let Some(data) = self.storage.get(cid)? {
            let block = Block::new_unchecked(cid.clone(), data);
            return Ok(block);
        }
        let (tx, rx) = oneshot::channel();
        self.network.clone().send((cid.clone(), tx)).await?;
        if let Ok(block) = rx.await {
            return Ok(block);
        }
        Err(BlockNotFound(cid.to_string()).into())
    }

    async fn commit(&self, tx: Transaction<Self>) -> Result<()> {
        self.storage.commit(tx)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libipld::block::Block;
    use libipld::codec_impl::Multicodec;
    use libipld::multihash::{Multihash, SHA2_256};
    use libipld::raw::RawCodec;
    use std::time::Duration;
    use tempdir::TempDir;

    fn create_store(bootstrap: Vec<(Multiaddr, PeerId)>) -> (Ipfs<Multicodec, Multihash>, TempDir) {
        let tmp = TempDir::new("").unwrap();
        let mut config = NetworkConfig::new();
        config.enable_mdns = bootstrap.is_empty();
        config.boot_nodes = bootstrap;
        let ipfs = Ipfs::new(tmp.path(), config).unwrap();
        (ipfs, tmp)
    }

    fn create_block(bytes: &[u8]) -> Block<Ipfs<Multicodec, Multihash>> {
        Block::encode(RawCodec, SHA2_256, bytes).unwrap()
    }

    #[async_std::test]
    async fn test_local_store() {
        env_logger::try_init().ok();
        let (store, _) = create_store(vec![]);
        let block = create_block(b"test_local_store");
        store.insert(block.clone()).await.unwrap();
        let block2 = store.get(block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    #[cfg(not(target_os = "macos"))] // mdns doesn't work on macos in github actions
    async fn test_exchange_mdns() {
        env_logger::try_init().ok();
        let (store1, _) = create_store(vec![]);
        let (store2, _) = create_store(vec![]);
        let block = create_block(b"test_exchange_mdns");
        store1.insert(block.clone()).await.unwrap();
        let block2 = store2.get(block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    #[cfg(not(target_os = "macos"))] // mdns doesn't work on macos in github action
    async fn test_received_want_before_insert() {
        env_logger::try_init().ok();
        let (store1, _) = create_store(vec![]);
        let (store2, _) = create_store(vec![]);
        let block = create_block(b"test_received_want_before_insert");

        let get_cid = block.cid().clone();
        let get = task::spawn(async move { store2.get(&get_cid).await });

        task::sleep(Duration::from_millis(100)).await;

        store1.insert(block.clone()).await.unwrap();

        let block2 = get.await.unwrap();
        assert_eq!(block.data(), block2.data());
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
        task::sleep(Duration::from_millis(1000)).await;
        let bootstrap = vec![(store.address().clone(), store.peer_id().clone())];
        let (store1, _) = create_store(bootstrap.clone());
        let (store2, _) = create_store(bootstrap);
        let block = create_block(b"test_exchange_kad");
        store1.insert(block.clone()).await.unwrap();
        // wait for entry to propagate
        task::sleep(Duration::from_millis(1000)).await;
        let block2 = store2.get(block.cid()).await.unwrap();
        assert_eq!(block.data(), block2.data());
    }

    #[async_std::test]
    async fn test_provider_not_found() {
        env_logger::try_init().ok();
        let (store1, _) = create_store(vec![]);
        let block = create_block(b"test_provider_not_found");
        if store1
            .get(block.cid())
            .await
            .unwrap_err()
            .downcast_ref::<BlockNotFound>()
            .is_none()
        {
            panic!("expected block not found error");
        }
    }
}
