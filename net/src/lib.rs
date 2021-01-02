use crate::behaviour::NetworkBackendBehaviour;
use futures::channel::{mpsc, oneshot};
use futures::stream::Stream;
use futures::{future, pin_mut};
use libipld::store::StoreParams;
use libipld::{Cid, Result};
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::Transport;
use libp2p::dns::DnsConfig;
use libp2p::mplex::MplexConfig;
use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
use libp2p::swarm::{AddressScore, Swarm, SwarmBuilder, SwarmEvent};
use libp2p::tcp::TcpConfig;
use libp2p_bitswap::Channel;
use prometheus::Registry;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

mod behaviour;
mod config;

pub use crate::behaviour::{NetworkEvent, QueryId, QueryOk};
pub use crate::config::NetworkConfig;
pub use libp2p::kad::record::{Key, Record};
pub use libp2p::kad::{PeerRecord, Quorum};
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
        let behaviour = NetworkBackendBehaviour::<P>::new(config.clone(), tx).await?;
        let swarm = SwarmBuilder::new(transport.boxed(), behaviour, peer_id)
            .executor(Box::new(|fut| {
                async_global_executor::spawn(fut).detach();
            }))
            .build();

        let swarm = Arc::new(Mutex::new(swarm));
        let swarm2 = swarm.clone();
        async_global_executor::spawn::<_, ()>(future::poll_fn(move |cx| {
            let mut guard = swarm.lock().unwrap();
            while let Poll::Ready(_) = {
                let swarm = &mut *guard;
                pin_mut!(swarm);
                swarm.poll_next(cx)
            } {}
            Poll::Pending
        }))
        .detach();

        Ok(Self { swarm: swarm2 })
    }

    pub fn local_peer_id(&self) -> PeerId {
        let swarm = self.swarm.lock().unwrap();
        *Swarm::local_peer_id(&swarm)
    }

    pub async fn listen_on(&self, addr: Multiaddr) -> Result<Multiaddr> {
        let mut swarm = self.swarm.lock().unwrap();
        Swarm::listen_on(&mut swarm, addr)?;
        loop {
            match swarm.next_event().await {
                SwarmEvent::NewListenAddr(addr) => {
                    tracing::info!("listening on {}", addr);
                    return Ok(addr);
                }
                SwarmEvent::ListenerClosed {
                    reason: Err(err), ..
                } => return Err(err.into()),
                _ => continue,
            }
        }
    }

    pub fn listeners(&self) -> Vec<Multiaddr> {
        let swarm = self.swarm.lock().unwrap();
        Swarm::listeners(&swarm).cloned().collect()
    }

    pub fn add_external_address(&self, addr: Multiaddr) {
        let mut swarm = self.swarm.lock().unwrap();
        Swarm::add_external_address(&mut swarm, addr, AddressScore::Infinite);
    }

    pub fn external_addresses(&self) -> Vec<AddressRecord> {
        let swarm = self.swarm.lock().unwrap();
        Swarm::external_addresses(&swarm).cloned().collect()
    }

    pub fn add_address(&self, peer: &PeerId, addr: Multiaddr) {
        let mut swarm = self.swarm.lock().unwrap();
        swarm.add_address(peer, addr);
    }

    pub fn remove_address(&self, peer: &PeerId, addr: &Multiaddr) {
        let mut swarm = self.swarm.lock().unwrap();
        swarm.remove_address(peer, addr);
    }

    pub fn dial(&self, peer: &PeerId) -> Result<()> {
        let mut swarm = self.swarm.lock().unwrap();
        Ok(Swarm::dial(&mut swarm, peer)?)
    }

    pub fn ban(&self, peer: PeerId) {
        let mut swarm = self.swarm.lock().unwrap();
        Swarm::ban_peer_id(&mut swarm, peer)
    }

    pub fn unban(&self, peer: PeerId) {
        let mut swarm = self.swarm.lock().unwrap();
        Swarm::unban_peer_id(&mut swarm, peer)
    }

    pub async fn bootstrap(&self, peers: &[(PeerId, Multiaddr)]) -> Result<()> {
        for (peer, addr) in peers {
            self.add_address(peer, addr.clone());
            self.dial(peer)?;
        }
        let rx = {
            let mut swarm = self.swarm.lock().unwrap();
            swarm.bootstrap()
        };
        tracing::trace!("started bootstrap");
        rx.await??;
        tracing::trace!("boostrap complete");
        Ok(())
    }

    pub async fn get_record(&self, key: &Key, quorum: Quorum) -> Result<Vec<PeerRecord>> {
        let rx = {
            let mut swarm = self.swarm.lock().unwrap();
            swarm.get_record(key, quorum)
        };
        if let QueryOk::Records(records) = rx.await?? {
            Ok(records)
        } else {
            unreachable!()
        }
    }

    pub async fn put_record(&self, record: Record, quorum: Quorum) -> Result<()> {
        let rx = {
            let mut swarm = self.swarm.lock().unwrap();
            swarm.put_record(record, quorum)
        };
        rx.await??;
        Ok(())
    }

    pub fn remove_record(&self, key: &Key) {
        let mut swarm = self.swarm.lock().unwrap();
        swarm.remove_record(key)
    }

    pub async fn get(&self, cid: Cid) -> Result<()> {
        let query = {
            let mut swarm = self.swarm.lock().unwrap();
            let (rx, id) = swarm.get(cid);
            QueryFuture {
                swarm: Some(self.swarm.clone()),
                id,
                rx,
            }
        };
        query.await
    }

    pub async fn sync(&self, cid: Cid, missing: impl Iterator<Item = Cid>) -> Result<()> {
        let query = {
            let mut swarm = self.swarm.lock().unwrap();
            let (rx, id) = swarm.sync(cid, missing);
            QueryFuture {
                swarm: Some(self.swarm.clone()),
                id,
                rx,
            }
        };
        query.await
    }

    pub async fn provide(&self, cid: Cid) -> Result<()> {
        let rx = {
            let mut swarm = self.swarm.lock().unwrap();
            swarm.provide(cid)
        };
        rx.await??;
        Ok(())
    }

    pub fn unprovide(&self, cid: Cid) {
        let mut swarm = self.swarm.lock().unwrap();
        swarm.unprovide(cid)
    }

    pub fn inject_have(&self, ch: Channel, have: bool) {
        let mut swarm = self.swarm.lock().unwrap();
        swarm.inject_have(ch, have);
    }

    pub fn inject_block(&self, ch: Channel, block: Option<Vec<u8>>) {
        let mut swarm = self.swarm.lock().unwrap();
        swarm.inject_block(ch, block);
    }

    pub fn inject_missing_blocks(&self, id: QueryId, missing: Vec<Cid>) {
        let mut swarm = self.swarm.lock().unwrap();
        swarm.inject_missing_blocks(id, missing);
    }

    pub fn register_metrics(&self, registry: &Registry) -> Result<()> {
        let swarm = self.swarm.lock().unwrap();
        swarm.register_metrics(registry)
    }
}

pub struct QueryFuture<P: StoreParams> {
    swarm: Option<Arc<Mutex<Swarm<NetworkBackendBehaviour<P>>>>>,
    id: QueryId,
    rx: oneshot::Receiver<Result<QueryOk>>,
}

impl<P: StoreParams> Future for QueryFuture<P> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<P: StoreParams> Drop for QueryFuture<P> {
    fn drop(&mut self) {
        let swarm = self.swarm.take().unwrap();
        let mut swarm = swarm.lock().unwrap();
        swarm.cancel(self.id);
    }
}
