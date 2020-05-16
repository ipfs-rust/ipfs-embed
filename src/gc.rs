use crate::storage::{GcEvent, GcSubscriber, Storage};
use async_std::stream::Stream;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

pub struct GarbageCollector {
    storage: Storage,
    subscriber: GcSubscriber,
}

impl GarbageCollector {
    pub fn new(storage: Storage) -> Self {
        let subscriber = storage.watch_gc();
        Self {
            storage,
            subscriber,
        }
    }
}

impl Future for GarbageCollector {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        loop {
            match Pin::new(&mut self.subscriber).poll_next(ctx) {
                Poll::Ready(Some(GcEvent::Unpin(cid))) => {
                    if let Err(e) = self.storage.remove(&cid) {
                        log::error!("gc error: {}", e);
                    }
                }
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            }
        }
        Poll::Pending
    }
}
