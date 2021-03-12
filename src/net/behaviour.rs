use crate::net::config::NetworkConfig;
use crate::net::peers::{AddressBook, AddressSource, PeerInfo};
use fnv::FnvHashMap;
use futures::channel::{mpsc, oneshot};
use futures::stream::Stream;
use ip_network::IpNetwork;
use libipld::store::StoreParams;
use libipld::{Cid, Result};
use libp2p::gossipsub::{
    Gossipsub, GossipsubEvent, GossipsubMessage, IdentTopic, MessageAuthenticity,
};
use libp2p::identify::{Identify, IdentifyEvent};
use libp2p::kad::record::store::MemoryStore;
use libp2p::kad::record::{Key, Record};
use libp2p::kad::{
    AddProviderOk, BootstrapOk, GetProvidersOk, GetRecordOk, Kademlia, KademliaEvent, PeerRecord,
    PutRecordOk, QueryResult, Quorum,
};
use libp2p::mdns::{Mdns, MdnsEvent};
use libp2p::multiaddr::Protocol;
use libp2p::ping::{Ping, PingEvent, PingFailure, PingSuccess};
use libp2p::swarm::toggle::Toggle;
use libp2p::swarm::NetworkBehaviourEventProcess;
use libp2p::NetworkBehaviour;
use libp2p::{Multiaddr, PeerId};
use libp2p_bitswap::{Bitswap, BitswapEvent, BitswapStore};
use libp2p_broadcast::{BroadcastBehaviour as Broadcast, BroadcastEvent, Topic};
use prometheus::Registry;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct QueryId(InnerQueryId);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum InnerQueryId {
    Bitswap(libp2p_bitswap::QueryId),
    Kad(libp2p::kad::QueryId),
}

impl From<libp2p_bitswap::QueryId> for QueryId {
    fn from(id: libp2p_bitswap::QueryId) -> Self {
        Self(InnerQueryId::Bitswap(id))
    }
}

impl From<libp2p::kad::QueryId> for QueryId {
    fn from(id: libp2p::kad::QueryId) -> Self {
        Self(InnerQueryId::Kad(id))
    }
}

/// An event of a sync query.
#[derive(Debug)]
pub enum SyncEvent {
    /// Signals that the sync query made progress and counts the amount of subtrees to
    /// sync. If it is syncing a linked list, it will always be 1.
    Progress { missing: usize },
    /// Signals completion of the sync query and if it was completed successfully.
    Complete(Result<()>),
}

pub type GetChannel = oneshot::Receiver<Result<()>>;
pub type SyncChannel = mpsc::UnboundedReceiver<SyncEvent>;
pub type BootstrapChannel = oneshot::Receiver<Result<()>>;
pub type StartProvidingChannel = oneshot::Receiver<Result<()>>;
pub type GetRecordChannel = oneshot::Receiver<Result<Vec<PeerRecord>>>;
pub type PutRecordChannel = oneshot::Receiver<Result<()>>;

enum QueryChannel {
    Get(oneshot::Sender<Result<()>>),
    Sync(mpsc::UnboundedSender<SyncEvent>),
    Bootstrap(oneshot::Sender<Result<()>>),
    StartProviding(oneshot::Sender<Result<()>>),
    GetRecord(oneshot::Sender<Result<Vec<PeerRecord>>>),
    PutRecord(oneshot::Sender<Result<()>>),
}

/// Behaviour type.
#[derive(NetworkBehaviour)]
pub struct NetworkBackendBehaviour<P: StoreParams> {
    #[behaviour(ignore)]
    allow_non_globals_in_dht: bool,
    #[behaviour(ignore)]
    bootstrap_complete: bool,

    peers: AddressBook,
    kad: Toggle<Kademlia<MemoryStore>>,
    mdns: Toggle<Mdns>,
    ping: Ping,
    identify: Identify,
    bitswap: Bitswap<P>,
    gossipsub: Gossipsub,
    broadcast: Broadcast,

    #[behaviour(ignore)]
    provider_queries: FnvHashMap<libp2p::kad::QueryId, libp2p_bitswap::QueryId>,
    #[behaviour(ignore)]
    queries: FnvHashMap<QueryId, QueryChannel>,
    #[behaviour(ignore)]
    subscriptions: FnvHashMap<String, Vec<mpsc::UnboundedSender<Vec<u8>>>>,
}

impl<P: StoreParams> NetworkBehaviourEventProcess<MdnsEvent> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, event: MdnsEvent) {
        match event {
            MdnsEvent::Discovered(list) => {
                for (peer_id, addr) in list {
                    self.add_address(&peer_id, addr, AddressSource::Mdns);
                }
            }
            MdnsEvent::Expired(_) => {
                // Ignore expired addresses
            }
        }
    }
}

#[derive(Debug, Error)]
#[error("Trying to use kad before bootstrap completed successfully.")]
pub struct NotBootstrapped;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadStoreError(pub libp2p::kad::record::store::Error);

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadAddProviderError(pub libp2p::kad::AddProviderError);

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadGetRecordError(pub libp2p::kad::GetRecordError);

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadPutRecordError(pub libp2p::kad::PutRecordError);

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadBootstrapError(pub libp2p::kad::BootstrapError);

impl<P: StoreParams> NetworkBehaviourEventProcess<KademliaEvent> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, event: KademliaEvent) {
        tracing::trace!("kademlia event {:?}", event);
        if let KademliaEvent::QueryResult { id, result, .. } = event {
            match result {
                QueryResult::GetProviders(Ok(GetProvidersOk { providers, .. })) => {
                    if let Some(id) = self.provider_queries.remove(&id) {
                        self.bitswap
                            .inject_providers(id, providers.into_iter().collect());
                    }
                }
                QueryResult::GetProviders(Err(err)) => {
                    tracing::trace!("{:?}", err);
                    if let Some(id) = self.provider_queries.remove(&id) {
                        self.bitswap.inject_providers(id, vec![]);
                    }
                }
                QueryResult::Bootstrap(Ok(BootstrapOk { num_remaining, .. })) => {
                    tracing::trace!("remaining {}", num_remaining);
                    if num_remaining == 0 {
                        self.bootstrap_complete = true;
                        if let Some(QueryChannel::Bootstrap(ch)) = self.queries.remove(&id.into()) {
                            ch.send(Ok(())).ok();
                        }
                    }
                }
                QueryResult::Bootstrap(Err(err)) => {
                    tracing::trace!("{:?}", err);
                    if let Some(QueryChannel::Bootstrap(ch)) = self.queries.remove(&id.into()) {
                        ch.send(Err(KadBootstrapError(err).into())).ok();
                    }
                }
                QueryResult::StartProviding(Ok(AddProviderOk { .. })) => {
                    if let Some(QueryChannel::StartProviding(ch)) = self.queries.remove(&id.into())
                    {
                        ch.send(Ok(())).ok();
                    }
                }
                QueryResult::StartProviding(Err(err)) => {
                    tracing::trace!("{:?}", err);
                    if let Some(QueryChannel::StartProviding(ch)) = self.queries.remove(&id.into())
                    {
                        ch.send(Err(KadAddProviderError(err).into())).ok();
                    }
                }
                QueryResult::RepublishProvider(Ok(_)) => {}
                QueryResult::RepublishProvider(Err(err)) => {
                    tracing::trace!("{:?}", err);
                }
                QueryResult::GetRecord(Ok(GetRecordOk { records })) => {
                    if let Some(QueryChannel::GetRecord(ch)) = self.queries.remove(&id.into()) {
                        ch.send(Ok(records)).ok();
                    }
                }
                QueryResult::GetRecord(Err(err)) => {
                    tracing::trace!("{:?}", err);
                    if let Some(QueryChannel::GetRecord(ch)) = self.queries.remove(&id.into()) {
                        ch.send(Err(KadGetRecordError(err).into())).ok();
                    }
                }
                QueryResult::PutRecord(Ok(PutRecordOk { .. })) => {
                    if let Some(QueryChannel::PutRecord(ch)) = self.queries.remove(&id.into()) {
                        ch.send(Ok(())).ok();
                    }
                }
                QueryResult::PutRecord(Err(err)) => {
                    tracing::trace!("{:?}", err);
                    if let Some(QueryChannel::PutRecord(ch)) = self.queries.remove(&id.into()) {
                        ch.send(Err(KadPutRecordError(err).into())).ok();
                    }
                }
                QueryResult::RepublishRecord(Ok(_)) => {}
                QueryResult::RepublishRecord(Err(err)) => {
                    tracing::trace!("{:?}", err);
                }
                QueryResult::GetClosestPeers(Ok(_)) => {}
                QueryResult::GetClosestPeers(Err(err)) => {
                    tracing::trace!("{:?}", err);
                }
            }
        }
    }
}

impl<P: StoreParams> NetworkBehaviourEventProcess<BitswapEvent> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, event: BitswapEvent) {
        match event {
            BitswapEvent::Providers(id, cid) => {
                if self.bootstrap_complete {
                    let key = Key::new(&cid.to_bytes());
                    let kad_id = self.kad.as_mut().unwrap().get_providers(key);
                    self.provider_queries.insert(kad_id, id);
                } else {
                    let providers = self.peers().copied().collect();
                    self.bitswap.inject_providers(id, providers);
                }
            }
            BitswapEvent::Progress(id, missing) => {
                if let Some(QueryChannel::Sync(ch)) = self.queries.get(&id.into()) {
                    ch.unbounded_send(SyncEvent::Progress { missing }).ok();
                }
            }
            BitswapEvent::Complete(id, result) => match self.queries.remove(&id.into()) {
                Some(QueryChannel::Get(ch)) => {
                    ch.send(result).ok();
                }
                Some(QueryChannel::Sync(ch)) => {
                    ch.unbounded_send(SyncEvent::Complete(result)).ok();
                }
                _ => {}
            },
        }
    }
}

impl<P: StoreParams> NetworkBehaviourEventProcess<PingEvent> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, event: PingEvent) {
        // Don't really need to do anything here as ping handles disconnecting automatically.
        match event {
            PingEvent {
                peer,
                result: Result::Ok(PingSuccess::Ping { rtt }),
            } => {
                tracing::trace!("ping: rtt to {} is {} ms", peer, rtt.as_millis());
                self.peers.set_rtt(&peer, Some(rtt));
            }
            PingEvent {
                peer,
                result: Result::Ok(PingSuccess::Pong),
            } => {
                tracing::trace!("ping: pong from {}", peer);
            }
            PingEvent {
                peer,
                result: Result::Err(PingFailure::Timeout),
            } => {
                tracing::trace!("ping: timeout to {}", peer);
                self.peers.set_rtt(&peer, None);
            }
            PingEvent {
                peer,
                result: Result::Err(PingFailure::Other { error }),
            } => {
                tracing::trace!("ping: failure with {}: {}", peer, error);
                self.peers.set_rtt(&peer, None);
            }
        }
    }
}

impl<P: StoreParams> NetworkBehaviourEventProcess<IdentifyEvent> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, event: IdentifyEvent) {
        // When a peer opens a connection we only have it's outgoing address. The identify
        // protocol sends the listening address which needs to be registered with kademlia.
        if let IdentifyEvent::Received {
            peer_id,
            info,
            observed_addr,
        } = event
        {
            self.peers.set_info(&peer_id, info);
            tracing::debug!("has external address {}", observed_addr);
            let local_peer_id = *self.peers.local_peer_id();
            // source doesn't matter as it won't be added to address book.
            self.add_address(&local_peer_id, observed_addr, AddressSource::User);
        }
    }
}

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct GossipsubPublishError(pub libp2p::gossipsub::error::PublishError);

impl<P: StoreParams> NetworkBehaviourEventProcess<GossipsubEvent> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, event: GossipsubEvent) {
        match event {
            GossipsubEvent::Message {
                message: GossipsubMessage { data, topic, .. },
                ..
            } => {
                if let Some(subscribers) = self.subscriptions.get_mut(topic.as_str()) {
                    subscribers
                        .retain(|subscriber| subscriber.unbounded_send(data.clone()).is_ok());
                    if subscribers.is_empty() {
                        self.unsubscribe(topic.as_str());
                        self.subscriptions.remove(topic.as_str());
                    }
                }
            }
            GossipsubEvent::Subscribed { .. } => {}
            GossipsubEvent::Unsubscribed { .. } => {}
        }
    }
}

impl<P: StoreParams> NetworkBehaviourEventProcess<BroadcastEvent> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, event: BroadcastEvent) {
        match event {
            BroadcastEvent::Received(_peer_id, topic, data) => {
                let topic = std::str::from_utf8(&topic).unwrap();
                if let Some(subscribers) = self.subscriptions.get_mut(topic) {
                    subscribers
                        .retain(|subscriber| subscriber.unbounded_send(data.to_vec()).is_ok());
                    if subscribers.is_empty() {
                        self.unsubscribe(topic);
                        self.subscriptions.remove(topic);
                    }
                }
            }
            BroadcastEvent::Subscribed(_, _) => {}
            BroadcastEvent::Unsubscribed(_, _) => {}
        }
    }
}

impl<P: StoreParams> NetworkBehaviourEventProcess<void::Void> for NetworkBackendBehaviour<P> {
    fn inject_event(&mut self, _event: void::Void) {}
}

impl<P: StoreParams> NetworkBackendBehaviour<P> {
    /// Create a Kademlia behaviour with the IPFS bootstrap nodes.
    pub async fn new<S: BitswapStore<Params = P>>(config: NetworkConfig, store: S) -> Result<Self> {
        let peer_id = config.peer_id();
        let mdns = if config.enable_mdns {
            Some(Mdns::new().await?)
        } else {
            None
        }
        .into();
        let kad = if config.enable_kad {
            let kad_store = MemoryStore::new(peer_id);
            Some(Kademlia::new(peer_id, kad_store))
        } else {
            None
        }
        .into();
        let public = config.public();
        let ping = Ping::new(config.ping);
        let identify = Identify::new("/ipfs-embed/1.0".into(), config.node_name.clone(), public);
        let bitswap = Bitswap::new(config.bitswap, store);
        let gossipsub = Gossipsub::new(
            MessageAuthenticity::Signed(config.node_key.clone()),
            config.gossipsub,
        )
        .map_err(|err| anyhow::anyhow!("{}", err))?;

        Ok(Self {
            allow_non_globals_in_dht: config.allow_non_globals_in_dht,
            bootstrap_complete: false,
            peers: AddressBook::new(peer_id),
            mdns,
            kad,
            ping,
            identify,
            bitswap,
            gossipsub,
            broadcast: Broadcast::default(),
            provider_queries: Default::default(),
            queries: Default::default(),
            subscriptions: Default::default(),
        })
    }

    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr, source: AddressSource) {
        if let Some(kad) = self.kad.as_mut() {
            let is_global = match addr.iter().next() {
                Some(Protocol::Ip4(ip)) => IpNetwork::from(ip).is_global(),
                Some(Protocol::Ip6(ip)) => IpNetwork::from(ip).is_global(),
                Some(Protocol::Dns(_)) => true,
                Some(Protocol::Dns4(_)) => true,
                Some(Protocol::Dns6(_)) => true,
                _ => false,
            };
            if self.allow_non_globals_in_dht || is_global {
                kad.add_address(&peer_id, addr.clone());
            } else {
                tracing::trace!("not adding local address {}", addr);
            }
        }
        self.peers.add_address(peer_id, addr, source);
    }

    pub fn remove_address(&mut self, peer_id: &PeerId, addr: &Multiaddr) {
        self.peers.remove_address(peer_id, addr);
        if let Some(kad) = self.kad.as_mut() {
            kad.remove_address(peer_id, addr);
        }
    }

    pub fn peers(&self) -> impl Iterator<Item = &PeerId> + '_ {
        self.peers.peers()
    }

    pub fn info(&self, peer_id: &PeerId) -> Option<&PeerInfo> {
        self.peers.info(peer_id)
    }

    pub fn connections(&self) -> impl Iterator<Item = (&PeerId, &Multiaddr)> + '_ {
        self.peers.connections()
    }

    pub fn bootstrap(&mut self) -> BootstrapChannel {
        let (tx, rx) = oneshot::channel();
        if let Some(kad) = self.kad.as_mut() {
            match kad.bootstrap() {
                Ok(id) => {
                    self.queries.insert(id.into(), QueryChannel::Bootstrap(tx));
                }
                Err(err) => {
                    tx.send(Err(err.into())).ok();
                }
            }
        } else {
            tx.send(Err(NotBootstrapped.into())).ok();
        }
        rx
    }

    pub fn provide(&mut self, cid: Cid) -> StartProvidingChannel {
        let (tx, rx) = oneshot::channel();
        if self.bootstrap_complete {
            if let Some(kad) = self.kad.as_mut() {
                let key = Key::new(&cid.to_bytes());
                match kad.start_providing(key) {
                    Ok(id) => {
                        self.queries
                            .insert(id.into(), QueryChannel::StartProviding(tx));
                    }
                    Err(err) => {
                        tx.send(Err(KadStoreError(err).into())).ok();
                    }
                }
            }
        } else {
            tx.send(Err(NotBootstrapped.into())).ok();
        }
        rx
    }

    pub fn unprovide(&mut self, cid: Cid) {
        if let Some(kad) = self.kad.as_mut() {
            let key = Key::new(&cid.to_bytes());
            kad.stop_providing(&key);
        }
    }

    pub fn get_record(&mut self, key: &Key, quorum: Quorum) -> GetRecordChannel {
        let (tx, rx) = oneshot::channel();
        if self.bootstrap_complete {
            if let Some(kad) = self.kad.as_mut() {
                let id = kad.get_record(key, quorum);
                self.queries.insert(id.into(), QueryChannel::GetRecord(tx));
            }
        } else {
            tx.send(Err(NotBootstrapped.into())).ok();
        }
        rx
    }

    pub fn put_record(&mut self, record: Record, quorum: Quorum) -> PutRecordChannel {
        let (tx, rx) = oneshot::channel();
        if self.bootstrap_complete {
            if let Some(kad) = self.kad.as_mut() {
                match kad.put_record(record, quorum) {
                    Ok(id) => {
                        self.queries.insert(id.into(), QueryChannel::PutRecord(tx));
                    }
                    Err(err) => {
                        tx.send(Err(KadStoreError(err).into())).ok();
                    }
                }
            }
        } else {
            tx.send(Err(NotBootstrapped.into())).ok();
        }
        rx
    }

    pub fn remove_record(&mut self, key: &Key) {
        if let Some(kad) = self.kad.as_mut() {
            kad.remove_record(key);
        }
    }

    pub fn subscribe(&mut self, topic: &str) -> Result<impl Stream<Item = Vec<u8>>> {
        let (tx, rx) = mpsc::unbounded();
        if let Some(subscribers) = self.subscriptions.get_mut(topic) {
            subscribers.push(tx);
        } else {
            let gossip_topic = IdentTopic::new(topic);
            let broadcast_topic = Topic::new(gossip_topic.hash().as_str().as_ref());
            self.subscriptions
                .insert(gossip_topic.hash().as_str().to_string(), vec![tx]);
            self.gossipsub
                .subscribe(&gossip_topic)
                .map_err(|err| anyhow::anyhow!("{:?}", err))?;
            self.broadcast.subscribe(broadcast_topic);
        }
        Ok(rx)
    }

    fn unsubscribe(&mut self, topic: &str) {
        let gossip_topic = IdentTopic::new(topic);
        let broadcast_topic = Topic::new(gossip_topic.hash().as_str().as_ref());
        if let Err(err) = self.gossipsub.unsubscribe(&gossip_topic) {
            tracing::trace!("unsubscribing from topic {} failed with {:?}", topic, err);
        }
        self.broadcast.unsubscribe(&broadcast_topic);
    }

    pub fn publish(&mut self, topic: &str, msg: Vec<u8>) -> Result<()> {
        let gossip_topic = IdentTopic::new(topic);
        self.gossipsub
            .publish(gossip_topic, msg)
            .map_err(GossipsubPublishError)?;
        Ok(())
    }

    pub fn broadcast(&mut self, topic: &str, msg: Vec<u8>) -> Result<()> {
        let gossip_topic = IdentTopic::new(topic);
        let broadcast_topic = Topic::new(gossip_topic.hash().as_str().as_ref());
        self.broadcast.broadcast(&broadcast_topic, msg.into());
        Ok(())
    }

    pub fn get(&mut self, cid: Cid) -> (GetChannel, QueryId) {
        let (tx, rx) = oneshot::channel();
        let id = self.bitswap.get(cid, std::iter::empty());
        self.queries.insert(id.into(), QueryChannel::Get(tx));
        (rx, id.into())
    }

    pub fn sync(&mut self, cid: Cid, missing: impl Iterator<Item = Cid>) -> (SyncChannel, QueryId) {
        let (tx, rx) = mpsc::unbounded();
        let id = self.bitswap.sync(cid, missing);
        self.queries.insert(id.into(), QueryChannel::Sync(tx));
        (rx, id.into())
    }

    pub fn cancel(&mut self, id: QueryId) {
        self.queries.remove(&id);
        if let QueryId(InnerQueryId::Bitswap(id)) = id {
            self.bitswap.cancel(id);
        }
    }

    pub fn register_metrics(&self, registry: &Registry) -> Result<()> {
        self.bitswap.register_metrics(registry)?;
        Ok(())
    }
}
