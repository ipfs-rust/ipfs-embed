use crate::{
    net::{
        config::NetworkConfig,
        peers::{AddressBook, Event},
    },
    variable::Writer,
    AddressSource, PeerInfo,
};
use fnv::{FnvHashMap, FnvHashSet};
use futures::channel::{
    mpsc::{self, UnboundedSender},
    oneshot,
};
use libipld::{store::StoreParams, Cid, DefaultParams, Result};
use libp2p::{
    core::ConnectedPoint,
    gossipsub::{Gossipsub, GossipsubEvent, GossipsubMessage, IdentTopic, MessageAuthenticity},
    identify::{Identify, IdentifyEvent},
    kad::{
        record::{store::MemoryStore, Key, Record},
        AddProviderOk, BootstrapOk, GetClosestPeersOk, GetProvidersOk, GetRecordOk, Kademlia,
        KademliaEvent, PeerRecord, PutRecordOk, QueryResult, Quorum,
    },
    mdns::{Mdns, MdnsEvent},
    ping::{Ping, PingEvent, PingFailure, PingSuccess},
    swarm::{
        behaviour::toggle::Toggle, AddressRecord, ConnectionError, ConnectionHandler,
        IntoConnectionHandler, NetworkBehaviour,
    },
    Multiaddr, NetworkBehaviour, PeerId,
};
use libp2p_bitswap::{Bitswap, BitswapEvent, BitswapStore};
use libp2p_broadcast::{Broadcast, BroadcastEvent, Topic};
use std::{collections::HashSet, sync::Arc, time::Duration};
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
    /// Signals that the sync query made progress and counts the amount of
    /// subtrees to sync. If it is syncing a linked list, it will always be
    /// 1.
    Progress { missing: usize },
    /// Signals completion of the sync query and if it was completed
    /// successfully.
    Complete(Result<()>),
}

pub type GetChannel = oneshot::Receiver<Result<()>>;
pub type SyncChannel = mpsc::UnboundedReceiver<SyncEvent>;

pub enum QueryChannel {
    Get(oneshot::Sender<Result<()>>),
    Sync(mpsc::UnboundedSender<SyncEvent>),
    Bootstrap(oneshot::Sender<Result<()>>),
    #[allow(dead_code)]
    GetClosestPeers(oneshot::Sender<Result<Vec<PeerId>>>),
    GetProviders(oneshot::Sender<Result<HashSet<PeerId>>>),
    StartProviding(oneshot::Sender<Result<()>>),
    GetRecord(oneshot::Sender<Result<Vec<PeerRecord>>>),
    PutRecord(oneshot::Sender<Result<()>>),
}
#[derive(Clone, Debug, PartialEq)]
pub enum GossipEvent {
    Subscribed(PeerId),
    Message(PeerId, Arc<[u8]>),
    Unsubscribed(PeerId),
}

pub(crate) type MyHandlerError = <<<NetworkBackendBehaviour<DefaultParams> as NetworkBehaviour>
    ::ConnectionHandler as IntoConnectionHandler>::Handler as ConnectionHandler>::Error;

#[derive(NetworkBehaviour)]
pub struct NetworkBackendBehaviour<P: StoreParams> {
    peers: AddressBook,
    kad: Toggle<Kademlia<MemoryStore>>,
    mdns: Toggle<Mdns>,
    ping: Toggle<Ping>,
    identify: Toggle<Identify>,
    bitswap: Toggle<Bitswap<P>>,
    gossipsub: Toggle<Gossipsub>,
    broadcast: Toggle<Broadcast>,
}

impl<P: StoreParams> NetworkBackendBehaviour<P> {
    pub fn inject_mdns_event(&mut self, event: MdnsEvent) {
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
#[error("Protocol `{0}` was disabled in `NetworkConfig`.")]
pub struct DisabledProtocol(&'static str);

#[derive(Debug, Error)]
#[error("Trying to use kad before bootstrap completed successfully.")]
pub struct NotBootstrapped;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadStoreError(pub libp2p::kad::record::store::Error);

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadBootstrapError(pub libp2p::kad::BootstrapError);

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadGetClosestPeersError(pub libp2p::kad::GetClosestPeersError);

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadGetProvidersError(pub libp2p::kad::GetProvidersError);

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadAddProviderError(pub libp2p::kad::AddProviderError);

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadGetRecordError(pub libp2p::kad::GetRecordError);

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct KadPutRecordError(pub libp2p::kad::PutRecordError);

impl<P: StoreParams> NetworkBackendBehaviour<P> {
    pub fn inject_kad_event(
        &mut self,
        event: KademliaEvent,
        bootstrap_complete: &mut bool,
        queries: &mut FnvHashMap<QueryId, QueryChannel>,
    ) {
        tracing::trace!("kademlia event {:?}", event);
        if let KademliaEvent::OutboundQueryCompleted { id, result, .. } = event {
            match result {
                QueryResult::Bootstrap(Ok(BootstrapOk { num_remaining, .. })) => {
                    tracing::trace!("remaining {}", num_remaining);
                    if num_remaining == 0 {
                        *bootstrap_complete = true;
                        if let Some(QueryChannel::Bootstrap(ch)) = queries.remove(&id.into()) {
                            ch.send(Ok(())).ok();
                        }
                        self.peers.notify(Event::Bootstrapped);
                    }
                }
                QueryResult::Bootstrap(Err(err)) => {
                    tracing::trace!("{:?}", err);
                    if let Some(QueryChannel::Bootstrap(ch)) = queries.remove(&id.into()) {
                        ch.send(Err(KadBootstrapError(err).into())).ok();
                    }
                }
                QueryResult::GetClosestPeers(Ok(GetClosestPeersOk { peers, .. })) => {
                    if let Some(QueryChannel::GetClosestPeers(ch)) = queries.remove(&id.into()) {
                        ch.send(Ok(peers)).ok();
                    }
                }
                QueryResult::GetClosestPeers(Err(err)) => {
                    tracing::trace!("{:?}", err);
                    if let Some(QueryChannel::GetClosestPeers(ch)) = queries.remove(&id.into()) {
                        ch.send(Err(KadGetClosestPeersError(err).into())).ok();
                    }
                }
                QueryResult::GetProviders(Ok(GetProvidersOk { providers, .. })) => {
                    if let Some(QueryChannel::GetProviders(ch)) = queries.remove(&id.into()) {
                        ch.send(Ok(providers)).ok();
                    }
                }
                QueryResult::GetProviders(Err(err)) => {
                    tracing::trace!("{:?}", err);
                    if let Some(QueryChannel::GetProviders(ch)) = queries.remove(&id.into()) {
                        ch.send(Err(KadGetProvidersError(err).into())).ok();
                    }
                }
                QueryResult::StartProviding(Ok(AddProviderOk { .. })) => {
                    if let Some(QueryChannel::StartProviding(ch)) = queries.remove(&id.into()) {
                        ch.send(Ok(())).ok();
                    }
                }
                QueryResult::StartProviding(Err(err)) => {
                    tracing::trace!("{:?}", err);
                    if let Some(QueryChannel::StartProviding(ch)) = queries.remove(&id.into()) {
                        ch.send(Err(KadAddProviderError(err).into())).ok();
                    }
                }
                QueryResult::RepublishProvider(Ok(_)) => {}
                QueryResult::RepublishProvider(Err(err)) => {
                    tracing::trace!("{:?}", err);
                }
                QueryResult::GetRecord(Ok(GetRecordOk { records, .. })) => {
                    if let Some(QueryChannel::GetRecord(ch)) = queries.remove(&id.into()) {
                        ch.send(Ok(records)).ok();
                    }
                }
                QueryResult::GetRecord(Err(err)) => {
                    tracing::trace!("{:?}", err);
                    if let Some(QueryChannel::GetRecord(ch)) = queries.remove(&id.into()) {
                        ch.send(Err(KadGetRecordError(err).into())).ok();
                    }
                }
                QueryResult::PutRecord(Ok(PutRecordOk { .. })) => {
                    if let Some(QueryChannel::PutRecord(ch)) = queries.remove(&id.into()) {
                        ch.send(Ok(())).ok();
                    }
                }
                QueryResult::PutRecord(Err(err)) => {
                    tracing::trace!("{:?}", err);
                    if let Some(QueryChannel::PutRecord(ch)) = queries.remove(&id.into()) {
                        ch.send(Err(KadPutRecordError(err).into())).ok();
                    }
                }
                QueryResult::RepublishRecord(Ok(_)) => {}
                QueryResult::RepublishRecord(Err(err)) => {
                    tracing::trace!("{:?}", err);
                }
            }
        }
    }
}

impl<P: StoreParams> NetworkBackendBehaviour<P> {
    pub fn inject_bitswap_event(
        &mut self,
        event: BitswapEvent,
        queries: &mut FnvHashMap<QueryId, QueryChannel>,
    ) {
        match event {
            BitswapEvent::Progress(id, missing) => {
                if let Some(QueryChannel::Sync(ch)) = queries.get(&id.into()) {
                    ch.unbounded_send(SyncEvent::Progress { missing }).ok();
                }
            }
            BitswapEvent::Complete(id, result) => match queries.remove(&id.into()) {
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

impl<P: StoreParams> NetworkBackendBehaviour<P> {
    pub fn inject_ping_event(&mut self, event: PingEvent) {
        // Don't really need to do anything here as ping handles disconnecting
        // automatically.
        let peer = event.peer;
        match event.result {
            Ok(PingSuccess::Ping { rtt }) => {
                //tracing::trace!("ping: rtt to {} is {} ms", peer, rtt.as_millis());
                self.peers.set_rtt(&peer, Some(rtt));
            }
            Ok(PingSuccess::Pong) => {
                //tracing::trace!("ping: pong from {}", peer);
            }
            Err(PingFailure::Timeout) => {
                tracing::debug!("ping: timeout to {}", peer);
                self.peers.set_rtt(&peer, None);
            }
            Err(PingFailure::Other { error }) => {
                tracing::info!("ping: failure with {}: {}", peer, error);
                self.peers.set_rtt(&peer, None);
            }
            Err(PingFailure::Unsupported) => {
                tracing::warn!("ping: {} does not support the ping protocol", peer);
            }
        }
    }
}

impl<P: StoreParams> NetworkBackendBehaviour<P> {
    pub fn inject_id_event(&mut self, event: IdentifyEvent) {
        // When a peer opens a connection we only have it's outgoing address. The
        // identify protocol sends the listening address which needs to be
        // registered with kademlia.
        if let IdentifyEvent::Received { peer_id, info } = event {
            self.peers.set_info(&peer_id, info);
        }
    }
}

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct GossipsubPublishError(pub libp2p::gossipsub::error::PublishError);

impl<P: StoreParams> NetworkBackendBehaviour<P> {
    pub fn inject_gossip_event(
        &mut self,
        event: GossipsubEvent,
        subscriptions: &mut FnvHashMap<String, Vec<mpsc::UnboundedSender<GossipEvent>>>,
    ) {
        match event {
            GossipsubEvent::Message {
                message:
                    GossipsubMessage {
                        data,
                        topic,
                        source,
                        ..
                    },
                propagation_source,
                ..
            } => {
                self.notify_subscribers(
                    &*topic.to_string(),
                    GossipEvent::Message(source.unwrap_or(propagation_source), data.into()),
                    subscriptions,
                );
            }
            GossipsubEvent::Subscribed { peer_id, topic, .. } => {
                self.peers
                    .notify(Event::Subscribed(peer_id, topic.to_string()));

                self.notify_subscribers(
                    &*topic.to_string(),
                    GossipEvent::Subscribed(peer_id),
                    subscriptions,
                );
            }
            GossipsubEvent::Unsubscribed { peer_id, topic, .. } => {
                self.peers
                    .notify(Event::Unsubscribed(peer_id, topic.to_string()));
                self.notify_subscribers(
                    &*topic.to_string(),
                    GossipEvent::Unsubscribed(peer_id),
                    subscriptions,
                );
            }
            GossipsubEvent::GossipsubNotSupported { .. } => {}
        }
    }
}

impl<P: StoreParams> NetworkBackendBehaviour<P> {
    pub fn inject_broadcast_event(
        &mut self,
        event: BroadcastEvent,
        subscriptions: &mut FnvHashMap<String, Vec<mpsc::UnboundedSender<GossipEvent>>>,
    ) {
        match event {
            BroadcastEvent::Received(peer_id, topic, data) => {
                let topic = std::str::from_utf8(&topic).unwrap();
                self.notify_subscribers(topic, GossipEvent::Message(peer_id, data), subscriptions);
            }
            BroadcastEvent::Subscribed(peer_id, topic) => {
                if let Ok(topic) = std::str::from_utf8(&topic) {
                    self.peers.notify(Event::Subscribed(peer_id, topic.into()));
                    self.notify_subscribers(topic, GossipEvent::Subscribed(peer_id), subscriptions);
                }
            }
            BroadcastEvent::Unsubscribed(peer_id, topic) => {
                if let Ok(topic) = std::str::from_utf8(&topic) {
                    self.peers
                        .notify(Event::Unsubscribed(peer_id, topic.into()));
                    self.notify_subscribers(
                        topic,
                        GossipEvent::Unsubscribed(peer_id),
                        subscriptions,
                    );
                }
            }
        }
    }
}

impl<P: StoreParams> NetworkBackendBehaviour<P> {
    /// Create a Kademlia behaviour with the IPFS bootstrap nodes.
    pub async fn new<S: BitswapStore<Params = P>>(
        config: &mut NetworkConfig,
        store: S,
        listeners: Writer<FnvHashSet<Multiaddr>>,
        peers: Writer<FnvHashMap<PeerId, PeerInfo>>,
        external: Writer<Vec<AddressRecord>>,
    ) -> Result<Self> {
        let node_key = libp2p::identity::Keypair::Ed25519(config.node_key.clone());
        let node_name = config.node_name.clone();
        let peer_id = node_key.public().to_peer_id();
        let mdns = if let Some(config) = config.mdns.take() {
            Some(Mdns::new(config).await?)
        } else {
            None
        };
        let kad = if let Some(config) = config.kad.take() {
            let kad_store = MemoryStore::with_config(peer_id, config);
            Some(Kademlia::new(peer_id, kad_store))
        } else {
            None
        };
        let ping = config.ping.take().map(Ping::new);
        let identify = if let Some(mut config) = config.identify.take() {
            config.local_public_key = node_key.public();
            config.agent_version = node_name.clone();
            Some(Identify::new(config))
        } else {
            None
        };
        let gossipsub = if let Some(config) = config.gossipsub.take() {
            let gossipsub = Gossipsub::new(MessageAuthenticity::Signed(node_key), config)
                .map_err(|err| anyhow::anyhow!("{}", err))?;
            Some(gossipsub)
        } else {
            None
        };
        let broadcast = config.broadcast.take().map(Broadcast::new);
        let bitswap = config
            .bitswap
            .take()
            .map(|config| Bitswap::new(config, store));
        Ok(Self {
            peers: AddressBook::new(
                peer_id,
                config.port_reuse,
                config.enable_loopback,
                listeners,
                peers,
                external,
            ),
            mdns: mdns.into(),
            kad: kad.into(),
            ping: ping.into(),
            identify: identify.into(),
            bitswap: bitswap.into(),
            gossipsub: gossipsub.into(),
            broadcast: broadcast.into(),
        })
    }

    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr, source: AddressSource) {
        if let Some(kad) = self.kad.as_mut() {
            kad.add_address(peer_id, addr.clone());
        }
        self.peers.add_address(peer_id, addr, source);
    }

    pub fn remove_address(&mut self, peer_id: &PeerId, addr: &Multiaddr) {
        self.peers.remove_address(peer_id, addr);
        if let Some(kad) = self.kad.as_mut() {
            kad.remove_address(peer_id, addr);
        }
    }

    pub fn prune_peers(&mut self, min_age: Duration) {
        self.peers.prune_peers(min_age);
    }

    pub fn dial(&mut self, peer_id: &PeerId) {
        self.peers.dial(peer_id);
    }

    pub fn dial_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.peers.dial_address(peer_id, addr);
    }

    pub(crate) fn connection_closed(
        &mut self,
        peer: PeerId,
        cp: ConnectedPoint,
        num_established: u32,
        error: Option<ConnectionError<MyHandlerError>>,
    ) {
        self.peers
            .connection_closed(peer, cp, num_established, error);
    }

    pub fn bootstrap(
        &mut self,
        queries: &mut FnvHashMap<QueryId, QueryChannel>,
        tx: oneshot::Sender<Result<()>>,
    ) {
        if let Some(kad) = self.kad.as_mut() {
            match kad.bootstrap() {
                Ok(id) => {
                    queries.insert(id.into(), QueryChannel::Bootstrap(tx));
                }
                Err(err) => {
                    tx.send(Err(err.into())).ok();
                }
            }
        } else {
            tx.send(Err(NotBootstrapped.into())).ok();
        }
    }

    // pub fn get_closest_peers<K>(
    //     &mut self,
    //     key: K,
    //     bootstrap_complete: bool,
    //     queries: &mut FnvHashMap<QueryId, QueryChannel>,
    // ) -> GetClosestPeersChannel
    // where
    //     K: Into<BucketKey<K>> + Into<Vec<u8>> + Clone,
    // {
    //     let (tx, rx) = oneshot::channel();
    //     if bootstrap_complete {
    //         if let Some(kad) = self.kad.as_mut() {
    //             let id = kad.get_closest_peers(key);
    //             queries.insert(id.into(), QueryChannel::GetClosestPeers(tx));
    //         }
    //     } else {
    //         tx.send(Err(NotBootstrapped.into())).ok();
    //     }
    //     rx
    // }

    pub fn provide(
        &mut self,
        key: Key,
        bootstrap_complete: bool,
        queries: &mut FnvHashMap<QueryId, QueryChannel>,
        tx: oneshot::Sender<Result<()>>,
    ) {
        if bootstrap_complete {
            if let Some(kad) = self.kad.as_mut() {
                match kad.start_providing(key) {
                    Ok(id) => {
                        queries.insert(id.into(), QueryChannel::StartProviding(tx));
                    }
                    Err(err) => {
                        tx.send(Err(KadStoreError(err).into())).ok();
                    }
                }
            }
        } else {
            tx.send(Err(NotBootstrapped.into())).ok();
        }
    }

    pub fn unprovide(&mut self, key: &Key) {
        if let Some(kad) = self.kad.as_mut() {
            kad.stop_providing(key);
        }
    }

    pub fn providers(
        &mut self,
        key: Key,
        bootstrap_complete: bool,
        queries: &mut FnvHashMap<QueryId, QueryChannel>,
        tx: oneshot::Sender<Result<HashSet<PeerId>>>,
    ) {
        if bootstrap_complete {
            if let Some(kad) = self.kad.as_mut() {
                let id = kad.get_providers(key);
                queries.insert(id.into(), QueryChannel::GetProviders(tx));
            }
        } else {
            tx.send(Err(NotBootstrapped.into())).ok();
        }
    }

    pub fn get_record(
        &mut self,
        key: Key,
        quorum: Quorum,
        bootstrap_complete: bool,
        queries: &mut FnvHashMap<QueryId, QueryChannel>,
        tx: oneshot::Sender<Result<Vec<PeerRecord>>>,
    ) {
        if bootstrap_complete {
            if let Some(kad) = self.kad.as_mut() {
                let id = kad.get_record(key, quorum);
                queries.insert(id.into(), QueryChannel::GetRecord(tx));
            }
        } else {
            tx.send(Err(NotBootstrapped.into())).ok();
        }
    }

    pub fn put_record(
        &mut self,
        record: Record,
        quorum: Quorum,
        bootstrap_complete: bool,
        queries: &mut FnvHashMap<QueryId, QueryChannel>,
        tx: oneshot::Sender<Result<()>>,
    ) {
        if bootstrap_complete {
            if let Some(kad) = self.kad.as_mut() {
                match kad.put_record(record, quorum) {
                    Ok(id) => {
                        queries.insert(id.into(), QueryChannel::PutRecord(tx));
                    }
                    Err(err) => {
                        tx.send(Err(KadStoreError(err).into())).ok();
                    }
                }
            }
        } else {
            tx.send(Err(NotBootstrapped.into())).ok();
        }
    }

    pub fn remove_record(&mut self, key: &Key) {
        if let Some(kad) = self.kad.as_mut() {
            kad.remove_record(key);
        }
    }

    pub fn subscribe(
        &mut self,
        topic: &str,
        subscriptions: &mut FnvHashMap<String, Vec<mpsc::UnboundedSender<GossipEvent>>>,
    ) -> Result<mpsc::UnboundedReceiver<GossipEvent>> {
        if self.gossipsub.as_ref().is_none() && self.broadcast.as_ref().is_none() {
            return Err(DisabledProtocol("gossipsub and broadcast").into());
        }
        let (tx, rx) = mpsc::unbounded();
        if let Some(subscribers) = subscriptions.get_mut(topic) {
            subscribers.push(tx);
        } else {
            let gossip_topic = IdentTopic::new(topic);
            let broadcast_topic = Topic::new(gossip_topic.hash().as_str().as_ref());
            subscriptions.insert(gossip_topic.hash().as_str().to_string(), vec![tx]);
            if let Some(gossipsub) = self.gossipsub.as_mut() {
                gossipsub
                    .subscribe(&gossip_topic)
                    .map_err(|err| anyhow::anyhow!("{:?}", err))?;
            }
            if let Some(broadcast) = self.broadcast.as_mut() {
                broadcast.subscribe(broadcast_topic);
            }
        }
        Ok(rx)
    }

    fn unsubscribe(&mut self, topic: &str) {
        let gossip_topic = IdentTopic::new(topic);
        let broadcast_topic = Topic::new(gossip_topic.hash().as_str().as_ref());
        if let Some(gossipsub) = self.gossipsub.as_mut() {
            if let Err(err) = gossipsub.unsubscribe(&gossip_topic) {
                tracing::trace!("unsubscribing from topic {} failed with {:?}", topic, err);
            }
        }
        if let Some(broadcast) = self.broadcast.as_mut() {
            broadcast.unsubscribe(&broadcast_topic);
        }
    }

    fn notify_subscribers(
        &mut self,
        topic: &str,
        event: GossipEvent,
        subscriptions: &mut FnvHashMap<String, Vec<mpsc::UnboundedSender<GossipEvent>>>,
    ) {
        if let Some(subscribers) = subscriptions.get_mut(topic) {
            subscribers.retain(|subscriber| subscriber.unbounded_send(event.clone()).is_ok());
            if subscribers.is_empty() {
                self.unsubscribe(topic);
                subscriptions.remove(topic);
            }
        }
    }

    pub fn publish(&mut self, topic: &str, msg: Vec<u8>) -> Result<()> {
        use libp2p::gossipsub::error::PublishError;
        if let Some(gossipsub) = self.gossipsub.as_mut() {
            let gossip_topic = IdentTopic::new(topic);
            match gossipsub.publish(gossip_topic, msg) {
                Ok(_) => Ok(()),
                Err(PublishError::InsufficientPeers) => {
                    tracing::trace!("publish: insufficient peers.");
                    Ok(())
                }
                Err(err) => Err(GossipsubPublishError(err).into()),
            }
        } else {
            Err(DisabledProtocol("gossipsub").into())
        }
    }

    pub fn broadcast(&mut self, topic: &str, msg: Vec<u8>) -> Result<()> {
        if let Some(broadcast) = self.broadcast.as_mut() {
            let gossip_topic = IdentTopic::new(topic);
            let broadcast_topic = Topic::new(gossip_topic.hash().as_str().as_ref());
            broadcast.broadcast(&broadcast_topic, msg.into());
            Ok(())
        } else {
            Err(DisabledProtocol("broadcast").into())
        }
    }

    pub fn get(
        &mut self,
        cid: Cid,
        providers: impl Iterator<Item = PeerId>,
        queries: &mut FnvHashMap<QueryId, QueryChannel>,
    ) -> (GetChannel, QueryId) {
        let bitswap = self.bitswap.as_mut().expect("bitswap enabled");
        let (tx, rx) = oneshot::channel();
        let id = bitswap.get(cid, providers);
        queries.insert(id.into(), QueryChannel::Get(tx));
        (rx, id.into())
    }

    pub fn sync(
        &mut self,
        cid: Cid,
        providers: Vec<PeerId>,
        missing: impl Iterator<Item = Cid>,
        queries: &mut FnvHashMap<QueryId, QueryChannel>,
    ) -> (SyncChannel, QueryId) {
        let bitswap = self.bitswap.as_mut().expect("bitswap enabled");
        let (tx, rx) = mpsc::unbounded();
        let id = bitswap.sync(cid, providers, missing);
        queries.insert(id.into(), QueryChannel::Sync(tx));
        (rx, id.into())
    }

    pub fn cancel(&mut self, id: QueryId, queries: &mut FnvHashMap<QueryId, QueryChannel>) {
        queries.remove(&id);
        if let QueryId(InnerQueryId::Bitswap(id)) = id {
            self.bitswap.as_mut().unwrap().cancel(id);
        }
    }

    pub fn swarm_events(&mut self, tx: UnboundedSender<Event>) {
        self.peers.swarm_events(tx)
    }
}
