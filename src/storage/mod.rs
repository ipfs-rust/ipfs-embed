use crate::error::Error;
use crate::storage::key::Key;
use core::convert::TryFrom;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures::stream::Stream;
use libipld_core::cid::Cid;
use libipld_core::store::Visibility;
use sled::{Event, IVec, Subscriber, Tree};

mod key;

#[derive(Debug, Clone)]
pub struct StorageBackend {
    tree: Tree,
}

impl StorageBackend {
    pub fn new(tree: sled::Tree) -> Self {
        Self { tree }
    }

    pub fn get_local(&self, cid: &Cid) -> Result<Option<IVec>, Error> {
        log::trace!("get_local {}", cid.to_string());
        Ok(self.tree.get(Key::block(cid))?)
    }

    pub fn get<'a>(
        &'a self,
        cid: &Cid,
    ) -> Pin<Box<dyn Future<Output = Result<IVec, Error>> + Send + 'a>> {
        log::trace!("get {}", cid.to_string());
        let key = Key::block(cid);
        match self.tree.get(&key) {
            Ok(Some(block)) => return Box::pin(async move { Ok(block) }),
            Ok(None) => {}
            Err(err) => return Box::pin(async move { Err(err.into()) }),
        }
        let subscription = self.tree.watch_prefix(&key);
        match self.tree.get(&key) {
            Ok(Some(block)) => return Box::pin(async move { Ok(block) }),
            Ok(None) => {}
            Err(err) => return Box::pin(async move { Err(err.into()) }),
        }
        if let Err(err) = self.tree.insert(Key::want(cid), &[]) {
            return Box::pin(async move { Err(err.into()) });
        }
        log::trace!("watching block({}) with prefix {:?}", cid.to_string(), key);
        Box::pin(GetFuture {
            tree: self.tree.clone(),
            subscription,
            key,
            cid: cid.clone(),
        })
    }

    pub fn insert(&self, cid: &Cid, data: IVec, visibility: Visibility) -> Result<(), Error> {
        log::trace!("insert {}", cid.to_string());
        self.tree.transaction(|tree| {
            log::trace!("insert key {:?}", Key::block(cid));
            tree.insert(Key::block(cid), &*data)?;
            tree.insert(Key::pin(cid), &[])?;
            if let Visibility::Public = visibility {
                tree.insert(Key::public(cid), &[])?;
            }
            if tree.get(Key::want(cid))?.is_some() {
                tree.remove(Key::want(cid))?;
            }
            Ok(())
        })?;
        Ok(())
    }

    pub async fn flush(&self) -> Result<(), Error> {
        log::trace!("flush");
        self.tree.flush_async().await?;
        Ok(())
    }

    pub fn unpin(&self, cid: &Cid) -> Result<(), Error> {
        log::trace!("unpin {}", cid.to_string());
        self.tree.remove(Key::pin(cid))?;
        Ok(())
    }

    pub fn remove(&self, cid: &Cid) -> Result<(), Error> {
        log::trace!("remove {}", cid.to_string());
        self.tree.transaction(|tree| {
            if tree.get(Key::pin(cid))?.is_none() {
                tree.remove(Key::block(cid))?;
                if tree.get(Key::public(cid))?.is_some() {
                    tree.remove(Key::public(cid))?;
                }
                if tree.get(Key::want(cid))?.is_some() {
                    tree.remove(Key::want(cid))?;
                }
            }
            Ok(())
        })?;
        Ok(())
    }

    pub fn wanted(&self) -> impl Iterator<Item = Result<Cid, Error>> {
        self.tree
            .scan_prefix(Key::Want.prefix())
            .keys()
            .map(|result| Ok(Cid::try_from(&result?[1..])?))
    }

    pub fn public(&self) -> impl Iterator<Item = Result<Cid, Error>> {
        self.tree
            .scan_prefix(Key::Public.prefix())
            .keys()
            .map(|result| Ok(Cid::try_from(&result?[1..])?))
    }

    pub fn alias(&self, alias: &[u8], cid: &Cid, _visibility: Visibility) -> Result<(), Error> {
        self.tree.insert(Key::alias(alias), cid.to_bytes())?;
        Ok(())
    }

    pub fn unalias(&self, alias: &[u8]) -> Result<(), Error> {
        self.tree.remove(Key::alias(alias))?;
        Ok(())
    }

    pub fn resolve(&self, alias: &[u8]) -> Result<Option<Cid>, Error> {
        let alias = self.tree.get(Key::alias(alias))?;
        if let Some(bytes) = alias {
            let cid = Cid::try_from(bytes.as_ref())?;
            Ok(Some(cid))
        } else {
            Ok(None)
        }
    }

    pub fn watch(&self) -> StorageSubscriber {
        log::trace!("watching public() with prefix {:?}", Key::Public.prefix());
        log::trace!("watching want() with prefix {:?}", Key::Want.prefix());
        StorageSubscriber {
            public: self.tree.watch_prefix(Key::Public.prefix()),
            want: self.tree.watch_prefix(Key::Want.prefix()),
        }
    }
}

pub struct GetFuture {
    tree: Tree,
    key: IVec,
    subscription: Subscriber,
    cid: Cid,
}

impl Future for GetFuture {
    type Output = Result<IVec, Error>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        log::trace!("poll get {}", self.cid.to_string());
        loop {
            match Pin::new(&mut self.subscription).poll(ctx) {
                Poll::Ready(Some(event)) => {
                    if let Event::Insert { key, value } = event {
                        if self.key == key {
                            log::trace!("resolve get {}", self.cid.to_string());
                            return Poll::Ready(Ok(value));
                        }
                    }
                }
                Poll::Ready(None) => unreachable!(),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Drop for GetFuture {
    fn drop(&mut self) {
        if let Err(err) = self.tree.remove(Key::want(&self.cid)) {
            log::error!("failed to remove want {}: {:?}", self.cid.to_string(), err);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageEvent {
    Want(Cid),
    Cancel(Cid),
    Provide(Cid),
    Unprovide(Cid),
}

pub struct StorageSubscriber {
    public: Subscriber,
    want: Subscriber,
}

impl Stream for StorageSubscriber {
    type Item = StorageEvent;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.want).poll(ctx) {
            Poll::Ready(Some(event)) => {
                let key = match &event {
                    Event::Insert { key, .. } => key,
                    Event::Remove { key } => key,
                };
                let cid = Cid::try_from(&key[1..]).expect("valid cid");
                let event = match event {
                    Event::Insert { .. } => {
                        log::trace!("emit want event {}", cid.to_string());
                        StorageEvent::Want(cid)
                    }
                    Event::Remove { .. } => {
                        log::trace!("emit cancel event {}", cid.to_string());
                        StorageEvent::Cancel(cid)
                    }
                };
                return Poll::Ready(Some(event));
            }
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => {}
        }
        match Pin::new(&mut self.public).poll(ctx) {
            Poll::Ready(Some(event)) => {
                let key = match &event {
                    Event::Insert { key, .. } => key,
                    Event::Remove { key } => key,
                };
                let cid = Cid::try_from(&key[1..]).expect("valid cid");
                let event = match event {
                    Event::Insert { .. } => {
                        log::trace!("emit provide event {}", cid.to_string());
                        StorageEvent::Provide(cid)
                    }
                    Event::Remove { .. } => {
                        log::trace!("emit unprovide event {}", cid.to_string());
                        StorageEvent::Unprovide(cid)
                    }
                };
                return Poll::Ready(Some(event));
            }
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => {}
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use futures::stream::StreamExt;
    use libipld_core::cid::Codec;
    use libipld_core::multihash::Sha2_256;
    use tempdir::TempDir;

    fn create_store() -> (StorageBackend, TempDir) {
        let tmp = TempDir::new("").unwrap();
        let db = sled::open(tmp.path()).unwrap();
        let tree = db.open_tree("ipfs_tree").unwrap();
        let storage = StorageBackend::new(tree);
        (storage, tmp)
    }

    fn create_block(bytes: &[u8]) -> (Cid, IVec) {
        let hash = Sha2_256::digest(&bytes);
        let cid = Cid::new_v1(Codec::Raw, hash);
        (cid, bytes.into())
    }

    #[async_std::test]
    async fn test_local() {
        env_logger::try_init().ok();
        let (store, _) = create_store();
        let (cid, data) = create_block(b"test_local");
        let mut sub = store.watch();

        assert_eq!(store.get_local(&cid).unwrap(), None);
        store
            .insert(&cid, data.clone(), Visibility::Private)
            .unwrap();
        assert_eq!(store.get_local(&cid).unwrap(), Some(data.clone()));
        store.remove(&cid).unwrap();
        assert_eq!(store.get_local(&cid).unwrap(), Some(data.clone()));
        store.unpin(&cid).unwrap();
        store.remove(&cid).unwrap();
        assert_eq!(store.get_local(&cid).unwrap(), None);

        drop(store);
        assert_eq!((&mut sub).next().await, None);
    }

    #[async_std::test]
    async fn test_get_want() {
        env_logger::try_init().ok();
        let (store, _) = create_store();
        let (cid, data) = create_block(b"test_get_want");
        let mut sub = store.watch();

        let block = data.clone();
        let storage = store.clone();
        task::spawn(async move {
            if let Some(StorageEvent::Want(cid)) = sub.next().await {
                storage.insert(&cid, block, Visibility::Public).unwrap();
            } else {
                panic!();
            }
        });

        let block = store.get(&cid).await.unwrap();
        assert_eq!(block, data);
    }

    #[async_std::test]
    async fn test_get_cancel() {
        env_logger::try_init().ok();
        let (store, _) = create_store();
        let (cid, _data) = create_block(b"test_get_cancel");
        let mut sub = store.watch();
        let future = store.get(&cid);
        assert_eq!(
            (&mut sub).next().await,
            Some(StorageEvent::Want(cid.clone()))
        );
        drop(future);
        assert_eq!(
            (&mut sub).next().await,
            Some(StorageEvent::Cancel(cid.clone()))
        );
    }

    #[async_std::test]
    async fn test_insert_provide() {
        env_logger::try_init().ok();
        let (store, _) = create_store();
        let (cid, data) = create_block(b"test_insert_provide");
        let mut sub = store.watch();
        store
            .insert(&cid, data.clone(), Visibility::Public)
            .unwrap();
        assert_eq!((&mut sub).next().await, Some(StorageEvent::Provide(cid)));
    }

    #[async_std::test]
    async fn test_remove_unprovide() {
        env_logger::try_init().ok();
        let (store, _) = create_store();
        let (cid, data) = create_block(b"test_remove_unprovide");
        let mut sub = store.watch();
        store
            .insert(&cid, data.clone(), Visibility::Public)
            .unwrap();
        assert_eq!(
            (&mut sub).next().await,
            Some(StorageEvent::Provide(cid.clone()))
        );
        store.unpin(&cid).unwrap();
        store.remove(&cid).unwrap();
        assert_eq!((&mut sub).next().await, Some(StorageEvent::Unprovide(cid)));
    }

    #[async_std::test]
    async fn test_alias() {
        env_logger::try_init().ok();
        let (store, _) = create_store();
        let (cid, data) = create_block(b"test_alias");
        store.insert(&cid, data, Visibility::Private).unwrap();

        assert_eq!(store.resolve(b"test_alias").unwrap(), None);
        store
            .alias(b"test_alias", &cid, Visibility::Private)
            .unwrap();
        assert_eq!(store.resolve(b"test_alias").unwrap(), Some(cid));
        store.unalias(b"test_alias").unwrap();
        assert_eq!(store.resolve(b"test_alias").unwrap(), None);
    }
}
