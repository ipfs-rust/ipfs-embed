use crate::behaviour::NetworkBackendBehaviour;
use async_mutex::Mutex;
use futures::channel::{mpsc, oneshot};
use futures::{future, pin_mut, FutureExt};
use libipld::store::StoreParams;
use libipld::{Cid, Result};
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::Transport;
use libp2p::dns::DnsConfig;
use libp2p::mplex::MplexConfig;
use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
use libp2p::swarm::{AddressScore, Swarm, SwarmBuilder};
use libp2p::tcp::TcpConfig;
use libp2p_bitswap::Channel;
//use libp2p::yamux::Config as YamuxConfig;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

mod behaviour;
mod config;

pub use crate::behaviour::{NetworkEvent, NetworkStats, QueryId};
pub use crate::config::NetworkConfig;
pub use libp2p::swarm::AddressRecord;
pub use libp2p::{Multiaddr, PeerId};

#[derive(Clone)]
pub struct NetworkService<P: StoreParams> {
    swarm: Arc<Mutex<Swarm<NetworkBackendBehaviour<P>>>>,
}

impl<P: StoreParams> NetworkService<P> {
    pub async fn new(
        config: NetworkConfig,
        tx: mpsc::UnboundedSender<NetworkEvent<P>>,
    ) -> Result<Self> {
        let dh_key = Keypair::<X25519Spec>::new()
            .into_authentic(&config.node_key)
            .unwrap();
        let transport = DnsConfig::new(
            TcpConfig::new()
                .nodelay(true)
                .upgrade(Version::V1)
                .authenticate(NoiseConfig::xx(dh_key).into_authenticated())
                .multiplex(MplexConfig::new())
                .timeout(Duration::from_secs(5))
                .boxed(),
        )?;

        let peer_id = config.peer_id();
        let behaviour = NetworkBackendBehaviour::<P>::new(config.clone()).await?;
        let mut swarm = SwarmBuilder::new(transport.boxed(), behaviour, peer_id)
            .executor(Box::new(|fut| {
                async_global_executor::spawn(fut).detach();
            }))
            .build();
        for addr in config.listen_addresses {
            Swarm::listen_on(&mut swarm, addr)?;
        }
        for addr in config.public_addresses {
            Swarm::add_external_address(&mut swarm, addr, AddressScore::Infinite);
        }

        while swarm.next_event().now_or_never().is_some() {}

        let swarm = Arc::new(Mutex::new(swarm));
        let swarm2 = swarm.clone();
        async_global_executor::spawn(future::poll_fn(move |cx| {
            let lock = swarm.lock();
            pin_mut!(lock);
            if let Poll::Ready(mut swarm) = lock.poll(cx) {
                while let Poll::Ready(ev) = {
                    let fut = swarm.next();
                    pin_mut!(fut);
                    fut.poll(cx)
                } {
                    if tx.unbounded_send(ev).is_err() {
                        tracing::trace!("exiting swarm");
                        return Poll::Ready(());
                    }
                }
            }
            Poll::Pending
        }))
        .detach();

        Ok(Self { swarm: swarm2 })
    }

    pub async fn local_peer_id(&self) -> PeerId {
        let swarm = self.swarm.lock().await;
        *Swarm::local_peer_id(&swarm)
    }

    pub async fn listeners(&self) -> Vec<Multiaddr> {
        let swarm = self.swarm.lock().await;
        Swarm::listeners(&swarm).cloned().collect()
    }

    pub async fn external_addresses(&self) -> Vec<AddressRecord> {
        let swarm = self.swarm.lock().await;
        Swarm::external_addresses(&swarm).cloned().collect()
    }

    pub async fn get(&self, cid: Cid) -> Result<()> {
        let mut swarm = self.swarm.lock().await;
        let (rx, id) = swarm.get(cid);
        QueryFuture {
            swarm: Some(self.swarm.clone()),
            id,
            rx,
        }
        .await
    }

    pub async fn sync(&self, cid: Cid, missing: impl Iterator<Item = Cid>) -> Result<()> {
        let mut swarm = self.swarm.lock().await;
        let (rx, id) = swarm.sync(cid, missing);
        QueryFuture {
            swarm: Some(self.swarm.clone()),
            id,
            rx,
        }
        .await
    }

    pub async fn bootstrap(&self) -> Result<()> {
        tracing::trace!("starting bootstrap");
        let mut swarm = self.swarm.lock().await;
        let rx = swarm.bootstrap();
        drop(swarm);
        rx.await??;
        tracing::trace!("boostrap complete");
        Ok(())
    }

    pub async fn provide(&self, cid: Cid) -> oneshot::Receiver<Result<()>> {
        let mut swarm = self.swarm.lock().await;
        swarm.provide(cid)
    }

    pub async fn unprovide(&self, cid: Cid) {
        let mut swarm = self.swarm.lock().await;
        swarm.unprovide(cid)
    }

    pub async fn inject_have(&self, ch: Channel, have: bool) {
        let mut swarm = self.swarm.lock().await;
        swarm.inject_have(ch, have);
    }

    pub async fn inject_block(&self, ch: Channel, block: Option<Vec<u8>>) {
        let mut swarm = self.swarm.lock().await;
        swarm.inject_block(ch, block);
    }

    pub async fn inject_missing_blocks(&self, id: QueryId, missing: Vec<Cid>) {
        let mut swarm = self.swarm.lock().await;
        swarm.inject_missing_blocks(id, missing);
    }

    pub async fn stats(&self) -> NetworkStats {
        let swarm = self.swarm.lock().await;
        swarm.stats()
    }
}

pub struct QueryFuture<P: StoreParams> {
    swarm: Option<Arc<Mutex<Swarm<NetworkBackendBehaviour<P>>>>>,
    id: QueryId,
    rx: oneshot::Receiver<Result<()>>,
}

impl<P: StoreParams> Future for QueryFuture<P> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(res)) => Poll::Ready(res),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<P: StoreParams> Drop for QueryFuture<P> {
    fn drop(&mut self) {
        let swarm = self.swarm.take().unwrap();
        let id = self.id;
        async_global_executor::spawn(async move {
            let mut swarm = swarm.lock().await;
            swarm.cancel(id);
        })
        .detach();
    }
}
