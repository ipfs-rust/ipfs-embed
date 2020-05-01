use crate::error::Error;
use crate::storage::key::Key;
use core::convert::TryFrom;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use libipld_core::cid::Cid;
use libipld_core::store::Visibility;
use sled::{Event, IVec, Subscriber, Tree};

mod gc;
mod key;
mod network;

pub use gc::{GcEvent, GcSubscriber};
pub use network::{NetworkEvent, NetworkSubscriber};

#[derive(Debug, Clone)]
pub struct Storage {
    tree: Tree,
}

impl Storage {
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::prelude::*;
    use async_std::task;
    use libipld_core::cid::Codec;
    use libipld_core::multihash::Sha2_256;
    use tempdir::TempDir;

    fn create_store() -> (Storage, TempDir) {
        let tmp = TempDir::new("").unwrap();
        let db = sled::open(tmp.path()).unwrap();
        let tree = db.open_tree("ipfs_tree").unwrap();
        let storage = Storage::new(tree);
        (storage, tmp)
    }

    fn create_block(bytes: &[u8]) -> (Cid, IVec) {
        let hash = Sha2_256::digest(&bytes);
        let cid = Cid::new_v1(Codec::Raw, hash);
        (cid, bytes.into())
    }

    struct Tester {
        _tmp: TempDir,
        store: Storage,
        gc: GcSubscriber,
        net: NetworkSubscriber,
        cid: Cid,
        data: IVec,
    }

    impl Tester {
        fn setup() -> Self {
            env_logger::try_init().ok();
            let (store, _tmp) = create_store();
            let (cid, data) = create_block(b"block");
            let gc = store.watch_gc();
            let net = store.watch_network();
            Self {
                _tmp,
                store,
                gc,
                net,
                cid,
                data,
            }
        }

        fn cid(&self) -> Cid {
            self.cid.clone()
        }

        fn data(&self) -> IVec {
            self.data.clone()
        }

        fn get_local(&self) -> Option<IVec> {
            self.store.get_local(&self.cid).unwrap()
        }

        fn get(&self) -> IVec {
            task::block_on(self.store.get(&self.cid)).unwrap()
        }

        fn insert(&self, visibility: Visibility) {
            self.store
                .insert(&self.cid, self.data.clone(), visibility)
                .unwrap();
        }

        fn unpin(&self) {
            self.store.unpin(&self.cid).unwrap();
        }

        fn remove(&self) {
            self.store.remove(&self.cid).unwrap();
        }

        fn alias(&self, alias: &[u8]) {
            self.store
                .alias(alias, &self.cid, Visibility::Private)
                .unwrap();
        }

        fn unalias(&self, alias: &[u8]) {
            self.store.unalias(alias).unwrap();
        }

        fn resolve(&self, alias: &[u8]) -> Option<Cid> {
            self.store.resolve(alias).unwrap()
        }

        fn assert_gc(&mut self, event: GcEvent) {
            assert_eq!(task::block_on((&mut self.gc).next()), Some(event));
        }

        fn assert_pin(&mut self) {
            let event = GcEvent::Pin(self.cid.clone());
            self.assert_gc(event);
        }

        fn assert_unpin(&mut self) {
            let event = GcEvent::Unpin(self.cid.clone());
            self.assert_gc(event);
        }

        fn assert_net(&mut self, event: NetworkEvent) {
            assert_eq!(task::block_on((&mut self.net).next()), Some(event));
        }

        fn assert_want(&mut self) {
            let event = NetworkEvent::Want(self.cid.clone());
            self.assert_net(event);
        }

        fn assert_cancel(&mut self) {
            let event = NetworkEvent::Cancel(self.cid.clone());
            self.assert_net(event);
        }

        fn assert_provide(&mut self) {
            let event = NetworkEvent::Provide(self.cid.clone());
            self.assert_net(event);
        }

        fn assert_unprovide(&mut self) {
            let event = NetworkEvent::Unprovide(self.cid.clone());
            self.assert_net(event);
        }

        fn assert_no_events(mut self) {
            drop(self.store);
            assert_eq!(task::block_on((&mut self.gc).next()), None);
            assert_eq!(task::block_on((&mut self.net).next()), None);
        }
    }

    #[test]
    fn test_insert_remove_private() {
        let mut tester = Tester::setup();
        tester.insert(Visibility::Private);
        tester.assert_pin();
        tester.unpin();
        tester.assert_unpin();
        tester.remove();
        tester.assert_no_events();
    }

    #[test]
    fn test_insert_remove_public() {
        let mut tester = Tester::setup();
        tester.insert(Visibility::Public);
        tester.assert_pin();
        tester.assert_provide();
        tester.unpin();
        tester.assert_unpin();
        tester.remove();
        tester.assert_unprovide();
        tester.assert_no_events();
    }

    #[test]
    fn test_get_local() {
        let tester = Tester::setup();
        tester.insert(Visibility::Private);
        assert_eq!(tester.get_local(), Some(tester.data()));
    }

    #[test]
    fn test_remove_pinned() {
        let tester = Tester::setup();
        tester.insert(Visibility::Private);
        tester.remove();
        assert_eq!(tester.get_local(), Some(tester.data()));
    }

    #[async_std::test]
    async fn test_get() {
        let mut tester = Tester::setup();

        let cid = tester.cid();
        let data = tester.data();
        let store = tester.store.clone();
        let mut net = store.watch_network();
        task::spawn(async move {
            assert_eq!(
                (&mut net).next().await.unwrap(),
                NetworkEvent::Want(cid.clone())
            );
            store.insert(&cid, data, Visibility::Public).unwrap();
        });

        assert_eq!(tester.data(), tester.get());
        tester.assert_pin();
    }

    #[test]
    fn test_get_cancel() {
        let mut tester = Tester::setup();

        let store = tester.store.clone();
        let future = store.get(&tester.cid);
        tester.assert_want();

        drop(future);
        tester.assert_cancel();
    }

    #[test]
    fn test_alias() {
        let tester = Tester::setup();
        tester.insert(Visibility::Private);

        assert_eq!(tester.resolve(b"test_alias"), None);
        tester.alias(b"test_alias");
        assert_eq!(tester.resolve(b"test_alias"), Some(tester.cid()));
        tester.unalias(b"test_alias");
        assert_eq!(tester.resolve(b"test_alias"), None);
    }
}
