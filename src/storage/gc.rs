use crate::storage::key::Key;
use crate::storage::Storage;
use async_std::prelude::*;
use async_std::task::{Context, Poll};
use core::convert::TryFrom;
use core::pin::Pin;
use libipld::cid::Cid;
use sled::{Event, Subscriber};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GcEvent {
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
        loop {
            match Pin::new(&mut self.pin).poll(ctx) {
                Poll::Ready(Some(Event::Remove { key })) => {
                    let cid = Cid::try_from(&key[1..]).expect("valid cid");
                    log::trace!("emit unpin event {}", cid.to_string());
                    return Poll::Ready(Some(GcEvent::Unpin(cid)));
                }
                Poll::Ready(Some(_)) => continue,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => break,
            }
        }
        Poll::Pending
    }
}
