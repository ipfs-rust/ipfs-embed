use crate::storage::key::Key;
use crate::storage::Storage;
use async_std::prelude::*;
use async_std::task::{Context, Poll};
use core::convert::TryFrom;
use core::pin::Pin;
use libipld::cid::Cid;
use sled::{Event, Subscriber};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NetworkEvent {
    Want(Cid),
    Cancel(Cid),
    Provide(Cid),
    Unprovide(Cid),
}

pub struct NetworkSubscriber {
    public: Subscriber,
    want: Subscriber,
}

impl Storage {
    pub fn watch_network(&self) -> NetworkSubscriber {
        log::trace!("watching public() with prefix {:?}", Key::Public.prefix());
        log::trace!("watching want() with prefix {:?}", Key::Want.prefix());
        NetworkSubscriber {
            public: self.tree.watch_prefix(Key::Public.prefix()),
            want: self.tree.watch_prefix(Key::Want.prefix()),
        }
    }
}

impl Stream for NetworkSubscriber {
    type Item = NetworkEvent;

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
                        NetworkEvent::Want(cid)
                    }
                    Event::Remove { .. } => {
                        log::trace!("emit cancel event {}", cid.to_string());
                        NetworkEvent::Cancel(cid)
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
                        NetworkEvent::Provide(cid)
                    }
                    Event::Remove { .. } => {
                        log::trace!("emit unprovide event {}", cid.to_string());
                        NetworkEvent::Unprovide(cid)
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
