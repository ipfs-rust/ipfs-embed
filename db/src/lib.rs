use futures::future::Future;
use futures::stream::Stream;
use ipfs_embed_core::{Block, Cid, Result, Storage, StorageEvent, StoreParams};
use std::convert::TryFrom;
use std::marker::PhantomData;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct StorageService<S: StoreParams> {
    _marker: PhantomData<S>,
}

impl<S: StoreParams> StorageService<S> {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        unimplemented!();
    }
}

impl<S: StoreParams> Storage<S> for StorageService<S> {
    type Subscription = Subscription;

    fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        unimplemented!();
    }

    fn insert(&self, block: &Block<S>) -> Result<()> {
        unimplemented!();
    }

    fn alias(&self, alias: &[u8], cid: Option<&Cid>) -> Result<()> {
        unimplemented!();
    }

    fn resolve(&self, alias: &[u8]) -> Result<Option<Cid>> {
        unimplemented!();
    }

    fn status(&self, cid: &Cid) -> Result<Option<bool>> {
        unimplemented!();
    }

    fn subscribe(&self) -> Self::Subscription {
        unimplemented!();
    }
}

pub struct Subscription {
    keys: Option<sled::Iter>,
    subscriber: sled::Subscriber,
}

impl Stream for Subscription {
    type Item = StorageEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(keys) = self.keys.as_mut() {
            if let Some(Ok((key, _))) = keys.next() {
                let cid = Cid::try_from(&key[1..]).unwrap();
                let entry = StorageEvent::Insert(cid);
                return Poll::Ready(Some(entry));
            }
        }
        self.keys = None;
        match Pin::new(&mut self.subscriber).poll(cx) {
            Poll::Ready(Some(sled::Event::Insert { key, .. })) => {
                let cid = Cid::try_from(&key[1..]).unwrap();
                let entry = StorageEvent::Insert(cid);
                Poll::Ready(Some(entry))
            }
            Poll::Ready(Some(sled::Event::Remove { key })) => {
                let cid = Cid::try_from(&key[1..]).unwrap();
                let entry = StorageEvent::Remove(cid);
                Poll::Ready(Some(entry))
            }
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}
