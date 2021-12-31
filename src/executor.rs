//! ipfs-embed supports different configuration of the used async executor to
//! spawn its background tasks (network, garbage collection, etc.). Those can be
//! configured with the following feature flags:
//! * `async_global`: Uses the `async-global-executor` crate (with async-std).
//!   This is the default.
//! * `tokio`: Uses a user provided tokio >= 1.0 runtime to spawn its background
//!   tasks.  Note, that
//! for this to work `ipfs-embed` needs to be executed within the context of a
//! tokio runtime. ipfs-embed won't spawn any on its own.

use futures::{Future, FutureExt};
use pin_project::pin_project;
use std::{pin::Pin, task::Poll};

#[derive(Clone)]
pub enum Executor {
    #[cfg(feature = "tokio")]
    Tokio,
    #[cfg(feature = "async_global")]
    AsyncGlobal,
}
impl Executor {
    #[allow(unreachable_code)]
    pub fn new() -> Self {
        #[cfg(feature = "async_global")]
        return Self::AsyncGlobal;

        #[cfg(feature = "tokio")]
        return Self::Tokio;
    }
    pub fn spawn<F: Future<Output = T> + Send + 'static, T: Send + 'static>(
        &self,
        future: F,
    ) -> JoinHandle<T> {
        match self {
            #[cfg(feature = "async_global")]
            Self::AsyncGlobal => {
                let task = async_global_executor::spawn(future);
                JoinHandle::AsyncGlobal(Some(task))
            }
            #[cfg(feature = "tokio")]
            Self::Tokio => {
                let task = tokio_crate::spawn(future);
                JoinHandle::Tokio(task)
            }
        }
    }

    pub fn spawn_blocking<Fun: FnOnce() -> T + Send + 'static, T: Send + 'static>(
        &self,
        f: Fun,
    ) -> JoinHandle<T> {
        match self {
            #[cfg(feature = "async_global")]
            Self::AsyncGlobal => {
                let task = async_global_executor::spawn(async_global_executor::spawn_blocking(f));
                JoinHandle::AsyncGlobal(Some(task))
            }
            #[cfg(feature = "tokio")]
            Self::Tokio => {
                let task = tokio_crate::task::spawn_blocking(f);
                JoinHandle::Tokio(task)
            }
        }
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

#[pin_project(project = EnumProj)]
pub enum JoinHandle<T> {
    #[cfg(feature = "tokio")]
    Tokio(tokio_crate::task::JoinHandle<T>),
    #[cfg(feature = "async_global")]
    AsyncGlobal(Option<async_global_executor::Task<T>>),
}

impl<T> JoinHandle<T> {
    #[allow(unused_mut)]
    pub fn detach(mut self) {
        #[cfg(feature = "async_global")]
        if let Self::AsyncGlobal(Some(t)) = self {
            t.detach()
        }
        // tokio task detaches when dropped
    }
    #[allow(unused_mut)]
    pub fn abort(mut self) {
        #[cfg(feature = "tokio")]
        if let Self::Tokio(t) = self {
            t.abort()
        }
        // async-global-executor task cancels when drop
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = anyhow::Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this {
            #[cfg(feature = "tokio")]
            EnumProj::Tokio(j) => match j.poll_unpin(cx) {
                Poll::Ready(t) => {
                    use anyhow::Context;
                    Poll::Ready(t.context("tokio::task::JoinHandle Error"))
                }
                Poll::Pending => Poll::Pending,
            },
            #[cfg(feature = "async_global")]
            EnumProj::AsyncGlobal(h) => {
                if let Some(handle) = h {
                    match handle.poll_unpin(cx) {
                        Poll::Ready(r) => Poll::Ready(Ok(r)),
                        _ => Poll::Pending,
                    }
                } else {
                    Poll::Ready(Err(anyhow::anyhow!("Future detached")))
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{generate_keypair, Config, DefaultParams, Ipfs};
    use tempdir::TempDir;

    #[test]
    fn should_work_with_async_global_per_default() {
        use futures::executor::block_on;
        let tmp = TempDir::new("ipfs-embed").unwrap();
        block_on(Ipfs::<DefaultParams>::new(Config::new(
            tmp.path(),
            generate_keypair(),
        )))
        .unwrap();
    }

    #[cfg(feature = "tokio")]
    #[test]
    #[should_panic(
        expected = "here is no reactor running, must be called from the context of a Tokio 1.x runtime"
    )]
    fn should_panic_without_a_tokio_runtime() {
        use futures::executor::block_on;
        let tmp = TempDir::new("ipfs-embed").unwrap();
        let _ = block_on(Ipfs::<DefaultParams>::new0(
            Config::new(tmp.path(), generate_keypair()),
            crate::Executor::Tokio,
        ));
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn should_not_panic_with_a_tokio_runtime() {
        let tmp = TempDir::new("ipfs-embed").unwrap();
        let rt = tokio_crate::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(Ipfs::<DefaultParams>::new0(
            Config::new(tmp.path(), generate_keypair()),
            crate::Executor::Tokio,
        ))
        .unwrap();
    }
}
