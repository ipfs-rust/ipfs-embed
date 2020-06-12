use crate::error::Error;
use crate::storage::key::{Key, Value};
use core::convert::TryFrom;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use libipld::cid::Cid;
use libipld::store::Visibility;
use sled::{Event, IVec, Subscriber, Tree};
use std::collections::HashSet;

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
    pub fn new(tree: sled::Tree) -> Result<Self, Error> {
        // cleanup wanted on startup
        for key in tree.scan_prefix(Key::Want.prefix()).keys() {
            tree.remove(key?)?;
        }
        Ok(Self { tree })
    }

    pub fn get_local(&self, cid: &Cid) -> Result<Option<IVec>, Error> {
        log::trace!("get_local {}", cid.to_string());
        Ok(self.tree.get(Key::block(cid))?)
    }

    pub async fn get(&self, cid: &Cid) -> Result<IVec, Error> {
        log::trace!("get {}", cid.to_string());
        let key = Key::block(cid);
        if let Some(block) = self.tree.get(&key)? {
            return Ok(block);
        }
        let subscription = self.tree.watch_prefix(&key);
        if let Some(block) = self.tree.get(&key)? {
            return Ok(block);
        }
        self.tree.insert(Key::want(cid), Value::from(true))?;
        log::trace!("watching block({}) with prefix {:?}", cid.to_string(), key);
        GetFuture {
            tree: self.tree.clone(),
            subscription,
            key,
            cid: cid.clone(),
        }
        .await
    }

    pub fn insert(&self, cid: &Cid, data: IVec, visibility: Visibility) -> Result<(), Error> {
        log::trace!("insert {}", cid.to_string());
        self.insert_batch(std::iter::once((cid.clone(), data)), visibility)?;
        Ok(())
    }

    pub fn insert_batch(
        &self,
        batch: impl Iterator<Item = (Cid, IVec)>,
        visibility: Visibility,
    ) -> Result<Cid, Error> {
        log::trace!("insert_batch");
        let blocks: Result<Vec<_>, Error> = batch
            .map(|(cid, data)| {
                let ipld = libipld::block::decode_ipld(&cid, &data)?;
                let refs = libipld::block::references(&ipld);
                let encoded = Value::from(&refs);
                Ok((cid, data, refs, encoded))
            })
            .collect();
        let blocks = blocks?;
        if blocks.is_empty() {
            return Err(Error::EmptyBatch);
        }
        self.tree.transaction(|tree| {
            let mut last_cid = None;
            for (cid, data, refs, encoded_refs) in &blocks {
                last_cid = Some(cid);
                if tree.get(Key::block(cid))?.is_some() {
                    continue;
                }
                for cid in refs {
                    let refer_key = Key::refer(cid);
                    let refer: u32 = tree
                        .get(refer_key.clone())?
                        .map(|b| Value::from(b).into())
                        .unwrap_or_default();
                    tree.insert(refer_key, Value::from(refer + 1))?;
                }
                tree.insert(Key::block(cid), data)?;
                tree.insert(Key::refs(cid), encoded_refs.clone())?;
                if let Visibility::Public = visibility {
                    tree.insert(Key::public(cid), Value::from(true))?;
                }
                tree.remove(Key::want(cid))?;
            }
            let last_cid = last_cid.unwrap();
            let pin_key = Key::pin(last_cid);
            if let Some(pin) = tree.get(&pin_key)? {
                log::trace!("duplicate incrementing pin count");
                tree.insert(pin_key, Value::from(u32::from(Value::from(pin)) + 1))?;
            } else {
                tree.insert(pin_key, Value::from(1))?;
            }
            Ok(())
        })?;
        Ok(blocks.into_iter().last().map(|(cid, _, _, _)| cid).unwrap())
    }

    pub async fn flush(&self) -> Result<(), Error> {
        log::trace!("flush");
        self.tree.flush_async().await?;
        Ok(())
    }

    pub fn unpin(&self, cid: &Cid) -> Result<(), Error> {
        log::trace!("unpin {}", cid.to_string());
        self.tree.transaction(|tree| {
            let pin_key = Key::pin(cid);
            if let Some(pin) = tree.remove(&pin_key)? {
                let pin: u32 = Value::from(pin).into();
                if pin > 1 {
                    tree.insert(pin_key, Value::from(pin - 1))?;
                }
            }
            Ok(())
        })?;
        Ok(())
    }

    fn remove_one(&self, cid: &Cid) -> Result<Option<HashSet<Cid>>, Error> {
        log::trace!("remove {}", cid.to_string());
        Ok(self.tree.transaction(|tree| {
            let pinned = tree.get(Key::pin(cid))?.is_some();
            let referers = tree.get(Key::refer(cid))?.is_some();
            if pinned || referers {
                return Ok(None);
            }
            tree.remove(Key::block(cid))?;
            tree.remove(Key::public(cid))?;
            tree.remove(Key::want(cid))?;
            let refs: HashSet<Cid> = Value::from(tree.remove(Key::refs(cid))?.unwrap()).into();
            for cid in &refs {
                let refer_key = Key::refer(cid);
                if let Some(refer) = tree.remove(&refer_key)? {
                    let refer: u32 = Value::from(refer).into();
                    if refer > 1 {
                        tree.insert(refer_key, Value::from(refer - 1))?;
                    }
                }
            }
            Ok(Some(refs))
        })?)
    }

    pub fn remove(&self, cid: &Cid) -> Result<(), Error> {
        if let Some(refs) = self.remove_one(cid)? {
            for cid in refs {
                self.remove(&cid)?;
            }
        }
        Ok(())
    }

    fn iter_prefix(&self, prefix: IVec) -> impl Iterator<Item = Result<Cid, Error>> {
        self.tree
            .scan_prefix(prefix)
            .keys()
            .map(|result| Ok(Cid::try_from(&result?[1..])?))
    }

    pub fn blocks(&self) -> impl Iterator<Item = Result<Cid, Error>> {
        self.iter_prefix(Key::Block.prefix())
    }

    pub fn public(&self) -> impl Iterator<Item = Result<Cid, Error>> {
        self.iter_prefix(Key::Public.prefix())
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
        Ok(self
            .tree
            .get(Key::alias(alias))?
            .map(|bytes| Value::from(bytes).into()))
    }

    pub fn metadata(&self, cid: &Cid) -> Result<Metadata, Error> {
        Ok(self.tree.transaction(|tree| {
            let pins = tree
                .get(Key::pin(cid))?
                .map(|b| Value::from(b).into())
                .unwrap_or_default();
            let public = tree
                .get(Key::public(cid))?
                .map(|b| Value::from(b).into())
                .unwrap_or_default();
            let want = tree
                .get(Key::want(cid))?
                .map(|b| Value::from(b).into())
                .unwrap_or_default();
            let refs = tree
                .get(Key::refs(cid))?
                .map(|b| Value::from(b).into())
                .unwrap_or_default();
            let referers = tree
                .get(Key::refer(cid))?
                .map(|b| Value::from(b).into())
                .unwrap_or_default();
            Ok(Metadata {
                pins,
                public,
                want,
                refs,
                referers,
            })
        })?)
    }
}

pub struct Metadata {
    pub pins: u32,
    pub public: bool,
    pub want: bool,
    pub refs: HashSet<Cid>,
    pub referers: u32,
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
    use futures::future::FutureExt;
    use libipld::cid::Codec;
    use libipld::multihash::Sha2_256;
    use tempdir::TempDir;

    fn create_store() -> (Storage, TempDir) {
        let tmp = TempDir::new("").unwrap();
        let db = sled::open(tmp.path()).unwrap();
        let tree = db.open_tree("ipfs_tree").unwrap();
        let storage = Storage::new(tree).unwrap();
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
        tester.unpin();
        tester.assert_unpin();
        tester.remove();
        tester.assert_no_events();
    }

    #[test]
    fn test_insert_remove_public() {
        let mut tester = Tester::setup();
        tester.insert(Visibility::Public);
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
        let tester = Tester::setup();

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
    }

    #[test]
    fn test_get_cancel() {
        let mut tester = Tester::setup();

        let store = tester.store.clone();
        let cid = tester.cid.clone();
        store.get(&cid).now_or_never();
        tester.assert_want();
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

    #[test]
    fn test_duplicate_no_unpin() {
        let tester = Tester::setup();
        tester.insert(Visibility::Private);
        tester.insert(Visibility::Private);
        tester.unpin();
        tester.assert_no_events();
    }

    #[test]
    fn test_duplicate_unpin() {
        let mut tester = Tester::setup();
        tester.insert(Visibility::Private);
        tester.insert(Visibility::Private);
        tester.unpin();
        tester.unpin();
        tester.assert_unpin();
        tester.assert_no_events();
    }
}
