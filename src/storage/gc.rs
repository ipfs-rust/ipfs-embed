use crate::storage::Storage;
use crate::storage::key::Key;
use core::convert::TryFrom;
use core::pin::Pin;
use async_std::prelude::*;
use async_std::task::{Context, Poll};
use libipld_core::cid::Cid;
use sled::{Event, Subscriber};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GcEvent {
    Pin(Cid),
    Unpin(Cid),
}

pub struct GcSubscriber {
    pin: Subscriber,
}

impl Storage {
    pub fn watch_gc(&self) -> GcSubscriber {
        log::trace!("watching pin() with prefix {:?}", Key::Pin.prefix());
        GcSubscriber {
            pin: self.tree.watch_prefix(Key::Pin.prefix()),
        }
    }
}

impl Stream for GcSubscriber {
    type Item = GcEvent;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.pin).poll(ctx) {
            Poll::Ready(Some(event)) => {
                let key = match &event {
                    Event::Insert { key, .. } => key,
                    Event::Remove { key } => key,
                };
                let cid = Cid::try_from(&key[1..]).expect("valid cid");
                let event = match event {
                    Event::Insert { .. } => {
                        log::trace!("emit pin event {}", cid.to_string());
                        GcEvent::Pin(cid)
                    }
                    Event::Remove { .. } => {
                        log::trace!("emit unpin event {}", cid.to_string());
                        GcEvent::Unpin(cid)
                    }
                };
                Poll::Ready(Some(event))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
