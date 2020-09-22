use anyhow::Result;
use futures::future::Future;
use futures::stream::Stream;
use ipfs_embed_core::StorageEvent;
use libipld::cid::Cid;
use sled::IVec;
use std::borrow::Cow;
use std::convert::TryFrom;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Clone, Debug, Eq, PartialEq)]
enum Key<'a> {
    Id(Cow<'a, Cid>),
    Cid(u64),
    Pin(u64),
    To(u64, u64),
    From(u64, u64),
}

impl<'a> Key<'a> {
    fn encode(&self) -> IVec {
        let mut buf = [0u8; 97];
        match self {
            Self::Id(cid) => {
                let mut cid_buf = &mut buf[1..];
                cid.write_bytes(&mut cid_buf).unwrap();
                let rest = cid_buf.len();
                IVec::from(&buf[..(buf.len() - rest)])
            }
            Self::Cid(id) | Self::Pin(id) => {
                buf[0] = if let Self::Cid(_) = self { 1 } else { 2 };
                let id_buf = &mut buf[1..9];
                id_buf.copy_from_slice(&id.to_be_bytes());
                IVec::from(&buf[..9])
            }
            Self::To(id1, id2) | Self::From(id1, id2) => {
                buf[0] = if let Self::To(_, _) = self { 3 } else { 4 };
                let id1_buf = &mut buf[1..9];
                id1_buf.copy_from_slice(&id1.to_be_bytes());
                let id2_buf = &mut buf[9..17];
                id2_buf.copy_from_slice(&id2.to_be_bytes());
                IVec::from(&buf[..17])
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Metadata {
    tree: sled::Tree,
}

impl Metadata {
    pub fn new(tree: sled::Tree) -> Self {
        Self { tree }
    }

    pub fn insert(&self, cid: &Cid, id: u64) -> Result<()> {
        log::trace!("insert {} {}", id, cid.to_string());
        self.tree
            .insert(Key::Id(Cow::Borrowed(cid)).encode(), &id.to_be_bytes()[..])?;
        self.tree.insert(Key::Cid(id).encode(), cid.to_bytes())?;
        Ok(())
    }

    pub fn remove(&self, cid: &Cid, id: u64) -> Result<()> {
        log::trace!("remove {} {}", id, cid.to_string());
        self.tree.remove(Key::Id(Cow::Borrowed(cid)).encode())?;
        self.tree.remove(Key::Cid(id).encode())?;
        Ok(())
    }

    pub fn insert_reference(&self, id1: u64, id2: u64) -> Result<()> {
        log::trace!("insert_reference {} {}", id1, id2);
        self.tree
            .insert(Key::To(id1, id2).encode(), &id2.to_be_bytes()[..])?;
        self.tree
            .insert(Key::From(id2, id1).encode(), &id1.to_be_bytes()[..])?;
        Ok(())
    }

    pub fn remove_reference(&self, id1: u64, id2: u64) -> Result<()> {
        log::trace!("remove_reference {} {}", id1, id2);
        self.tree.remove(Key::To(id1, id2).encode())?;
        self.tree.remove(Key::From(id2, id1).encode())?;
        Ok(())
    }

    pub fn set_pin(&self, id: u64, pin: u64) -> Result<()> {
        log::trace!("set_pin {} {}", id, pin);
        self.tree
            .insert(Key::Pin(id).encode(), &pin.to_be_bytes()[..])?;
        Ok(())
    }

    pub fn id(&self, cid: &Cid) -> Result<Option<u64>> {
        Ok(self
            .tree
            .get(Key::Id(Cow::Borrowed(cid)).encode())?
            .map(|bytes| {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes);
                u64::from_be_bytes(buf)
            }))
    }

    pub fn cid(&self, id: u64) -> Result<Option<Cid>> {
        Ok(self
            .tree
            .get(Key::Cid(id).encode())?
            .map(|bytes| Cid::try_from(&bytes[..]).unwrap()))
    }

    pub fn pin(&self, id: u64) -> Result<Option<u64>> {
        Ok(self.tree.get(Key::Pin(id).encode())?.map(|bytes| {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes);
            u64::from_be_bytes(buf)
        }))
    }

    pub fn blocks(&self) -> impl Iterator<Item = Result<u64>> {
        self.tree
            .scan_prefix(&Key::Cid(0).encode()[..1])
            .map(|entry| {
                let (_, v) = entry?;
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&v);
                Ok(u64::from_be_bytes(buf))
            })
    }

    pub fn references(&self, id: u64) -> impl Iterator<Item = Result<u64>> {
        self.tree
            .scan_prefix(&Key::To(id, 0).encode()[..9])
            .map(|entry| {
                let (_, v) = entry?;
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&v);
                Ok(u64::from_be_bytes(buf))
            })
    }

    pub fn referrers(&self, id: u64) -> impl Iterator<Item = Result<u64>> {
        self.tree
            .scan_prefix(&Key::From(id, 0).encode()[..9])
            .map(|entry| {
                let (_, v) = entry?;
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&v);
                Ok(u64::from_be_bytes(buf))
            })
    }

    pub fn subscribe(&self) -> Subscription {
        // cid => id prefix
        let subscriber = self.tree.watch_prefix([0]);
        let keys = self.tree.scan_prefix([0]);
        Subscription {
            keys: Some(keys),
            subscriber,
        }
    }

    pub fn flush(&self) -> Result<()> {
        self.tree.flush()?;
        Ok(())
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
