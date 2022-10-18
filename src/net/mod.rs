mod address_handler;
mod behaviour;
mod config;
mod peer_info;
mod peers;
#[cfg(test)]
mod tests;

pub use self::{
    behaviour::{GossipEvent, QueryId, SyncEvent},
    config::{DnsConfig, NetworkConfig},
    peer_info::{AddressSource, ConnectionFailure, Direction, PeerInfo, Rtt},
    peers::{register_metrics, Event, SwarmEvents},
};

use self::behaviour::{GetChannel, NetworkBackendBehaviour, QueryChannel, SyncChannel};
use crate::{
    executor::{Executor, JoinHandle},
    variable::{Reader, Writer},
};
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use fnv::{FnvHashMap, FnvHashSet};
use futures::{
    channel::{
        mpsc::{self, Receiver, Sender, TrySendError, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    future::{self, Either},
    stream::{Stream, StreamExt},
    FutureExt,
};
use libipld::{error::BlockNotFound, store::StoreParams, Cid, Result};
#[cfg(feature = "async_global")]
use libp2p::dns::DnsConfig as Dns;
#[cfg(all(feature = "tokio", not(feature = "async_global")))]
use libp2p::dns::TokioDnsConfig as Dns;
#[cfg(feature = "async_global")]
use libp2p::tcp::GenTcpConfig as TcpConfig;
#[cfg(all(feature = "tokio", not(feature = "async_global")))]
use libp2p::tcp::TokioTcpConfig as TcpConfig;
use libp2p::{
    core::{
        either::EitherTransport,
        transport::{ListenerId, Transport},
        upgrade::{SelectUpgrade, Version},
    },
    identity::ed25519::PublicKey,
    kad::{record::Key, PeerRecord, Quorum, Record},
    mplex::MplexConfig,
    noise::{self, NoiseConfig, X25519Spec},
    pnet::{PnetConfig, PreSharedKey},
    swarm::{AddressRecord, AddressScore, Swarm, SwarmBuilder, SwarmEvent},
    tcp::TcpTransport,
    yamux::YamuxConfig,
    Multiaddr, PeerId,
};
use libp2p_bitswap::BitswapStore;
use std::{
    collections::HashSet,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use void::unreachable;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListenerEvent {
    NewListenAddr(Multiaddr),
    ExpiredListenAddr(Multiaddr),
    ListenFailed(Multiaddr, String),
}

#[derive(Debug)]
pub enum NetworkCommand {
    ListenOn(Multiaddr, UnboundedSender<ListenerEvent>),
    AddExternalAddress(Multiaddr),
    AddAddress(PeerId, Multiaddr),
    RemoveAddress(PeerId, Multiaddr),
    PrunePeers(Duration),
    Dial(PeerId),
    DialAddress(PeerId, Multiaddr),
    Ban(PeerId),
    Unban(PeerId),
    Bootstrap(
        Vec<(PeerId, Multiaddr)>,
        oneshot::Sender<anyhow::Result<()>>,
    ),
    Providers(Key, oneshot::Sender<anyhow::Result<HashSet<PeerId>>>),
    Provide(Key, oneshot::Sender<anyhow::Result<()>>),
    Unprovide(Key),
    GetRecord(
        Key,
        Quorum,
        oneshot::Sender<anyhow::Result<Vec<PeerRecord>>>,
    ),
    PutRecord(Record, Quorum, oneshot::Sender<anyhow::Result<()>>),
    RemoveRecord(Key),
    Subscribe(
        String,
        oneshot::Sender<anyhow::Result<UnboundedReceiver<GossipEvent>>>,
    ),
    Publish(String, Vec<u8>, oneshot::Sender<anyhow::Result<()>>),
    Broadcast(String, Vec<u8>, oneshot::Sender<anyhow::Result<()>>),
    Get(Cid, Vec<PeerId>, oneshot::Sender<GetQuery>),
    Sync(Cid, Vec<PeerId>, Vec<Cid>, oneshot::Sender<SyncQuery>),
    SwarmEvents(oneshot::Sender<SwarmEvents>),
    CancelQuery(QueryId),
}

#[derive(Clone)]
pub struct NetworkService {
    bootstrapped: Reader<bool>,
    peers: Reader<FnvHashMap<PeerId, PeerInfo>>,
    listeners: Reader<FnvHashSet<Multiaddr>>,
    external: Reader<Vec<AddressRecord>>,
    public_key: PublicKey,
    peer_id: PeerId,
    node_name: String,
    cmd: Sender<NetworkCommand>,
    _swarm_task: Arc<JoinHandle<()>>,
}

impl NetworkService {
    pub async fn new<S: BitswapStore>(
        mut config: NetworkConfig,
        store: S,
        executor: Executor,
    ) -> Result<Self> {
        let public_key = config.node_key.public();
        let peer_id =
            PeerId::from_public_key(&libp2p::core::PublicKey::Ed25519(public_key.clone()));
        let node_name = config.node_name.clone();

        let peers = Writer::new(FnvHashMap::default());
        let peers2 = peers.reader();
        let listeners = Writer::new(FnvHashSet::default());
        let listeners2 = listeners.reader();
        let external = Writer::new(vec![]);
        let external2 = external.reader();
        let behaviour =
            NetworkBackendBehaviour::new(&mut config, store, listeners, peers, external).await?;

        let tcp = {
            let transport =
                TcpTransport::new(TcpConfig::new().nodelay(true).port_reuse(config.port_reuse));
            let transport = if let Some(psk) = config.psk {
                let psk = PreSharedKey::new(psk);
                EitherTransport::Left(
                    transport.and_then(move |socket, _| PnetConfig::new(psk).handshake(socket)),
                )
            } else {
                EitherTransport::Right(transport)
            };
            let dh_key = noise::Keypair::<X25519Spec>::new()
                .into_authentic(&libp2p::core::identity::Keypair::Ed25519(
                    config.node_key.clone(),
                ))
                .unwrap();
            transport
                .upgrade(Version::V1)
                .authenticate(NoiseConfig::xx(dh_key).into_authenticated())
                .multiplex(SelectUpgrade::new(
                    YamuxConfig::default(),
                    MplexConfig::new(),
                ))
                .timeout(Duration::from_secs(5))
                .boxed()
        };
        /*let quic = {
            QuicConfig {
                keypair: config.node_key,
                transport: config.quic,
                ..Default::default()
            }
            .listen_on("/ip4/0.0.0.0/udp/0/quic".parse().unwrap())
            .await?
            .boxed()
        };
        let quic_or_tcp = quic.or_transport(tcp).map(|either, _| match either {
            EitherOutput::First(first) => first,
            EitherOutput::Second(second) => second,
        });*/
        let quic_or_tcp = tcp;
        #[cfg(feature = "async_global")]
        let transport = if let Some(config) = config.dns {
            match config {
                DnsConfig::Custom { config, opts } => {
                    Dns::custom(quic_or_tcp, config, opts).await?.boxed()
                }
                DnsConfig::SystemWithFallback { config, opts } => {
                    match trust_dns_resolver::system_conf::read_system_conf() {
                        Ok((config, opts)) => Dns::custom(quic_or_tcp, config, opts).await?.boxed(),
                        Err(e) => {
                            tracing::warn!("falling back to custom DNS config, system default yielded error `${:#}`", e);
                            Dns::custom(quic_or_tcp, config, opts).await?.boxed()
                        }
                    }
                }
            }
        } else {
            Dns::system(quic_or_tcp).await?.boxed()
        };
        #[cfg(all(feature = "tokio", not(feature = "async_global")))]
        let transport = if let Some(config) = config.dns {
            match config {
                DnsConfig::Custom { config, opts } => {
                    Dns::custom(quic_or_tcp, config, opts)?.boxed()
                }
                DnsConfig::SystemWithFallback { config, opts } => {
                    match trust_dns_resolver::system_conf::read_system_conf() {
                        Ok((config, opts)) => Dns::custom(quic_or_tcp, config, opts)?.boxed(),
                        Err(e) => {
                            tracing::warn!("falling back to custom DNS config, system default yielded error `${:#}`", e);
                            Dns::custom(quic_or_tcp, config, opts)?.boxed()
                        }
                    }
                }
            }
        } else {
            Dns::system(quic_or_tcp)?.boxed()
        };

        let exec = executor.clone();
        let swarm = SwarmBuilder::new(transport, behaviour, peer_id)
            .executor(Box::new(move |fut| {
                exec.spawn(fut).detach();
            }))
            .build();
        /*
        // Required for swarm book keeping.
        swarm
            .listen_on("/ip4/0.0.0.0/udp/0/quic".parse().unwrap())
            .unwrap();
        */

        let bootstrapped = Writer::new(false);
        let bootstrapped2 = bootstrapped.reader();
        let (cmd_tx, cmd_rx) = mpsc::channel(100);
        let swarm_task = executor.spawn(poll_swarm(
            cmd_rx,
            cmd_tx.clone(),
            swarm,
            executor.clone(),
            bootstrapped,
        ));

        Ok(Self {
            bootstrapped: bootstrapped2,
            peers: peers2,
            listeners: listeners2,
            external: external2,
            public_key,
            peer_id,
            node_name,
            cmd: cmd_tx,
            _swarm_task: Arc::new(swarm_task),
        })
    }

    pub fn local_public_key(&self) -> PublicKey {
        self.public_key.clone()
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.peer_id
    }

    pub fn local_node_name(&self) -> String {
        self.node_name.clone()
    }

    fn cmd(&mut self, msg: NetworkCommand) -> Option<(NetworkCommand, &'static str)> {
        Self::handle_send_result(self.cmd.try_send(msg))
    }

    fn cmd_shared(&self, msg: NetworkCommand) -> Option<(NetworkCommand, &'static str)> {
        Self::handle_send_result(self.cmd.clone().try_send(msg))
    }

    fn handle_send_result(
        res: Result<(), TrySendError<NetworkCommand>>,
    ) -> Option<(NetworkCommand, &'static str)> {
        match res {
            Ok(_) => None,
            Err(err) => {
                let reason = if err.is_disconnected() {
                    "receiver went away"
                } else {
                    "channel is full"
                };
                let val = err.into_inner();
                tracing::warn!("failed IPFS swarm command {:?}: {}", val, reason);
                Some((val, reason))
            }
        }
    }

    pub fn listen_on(&mut self, addr: Multiaddr) -> impl Stream<Item = ListenerEvent> {
        let (tx, rx) = mpsc::unbounded();
        if let Some((NetworkCommand::ListenOn(addr, tx), reason)) =
            self.cmd(NetworkCommand::ListenOn(addr, tx))
        {
            tx.unbounded_send(ListenerEvent::ListenFailed(
                addr,
                format!("cannot send to Swarm: {}", reason),
            ))
            .ok();
        }
        rx
    }

    pub fn listeners(&self) -> Vec<Multiaddr> {
        self.listeners.project(|l| l.iter().cloned().collect())
    }

    pub fn add_external_address(&mut self, mut addr: Multiaddr) {
        peers::normalize_addr(&mut addr, &self.local_peer_id());
        self.cmd(NetworkCommand::AddExternalAddress(addr));
    }

    pub fn external_addresses(&self) -> Vec<AddressRecord> {
        self.external.get_cloned()
    }

    pub fn add_address(&mut self, peer: PeerId, addr: Multiaddr) {
        self.cmd(NetworkCommand::AddAddress(peer, addr));
    }

    pub fn remove_address(&mut self, peer: PeerId, addr: Multiaddr) {
        self.cmd(NetworkCommand::RemoveAddress(peer, addr));
    }

    pub fn prune_peers(&mut self, min_age: Duration) {
        self.cmd(NetworkCommand::PrunePeers(min_age));
    }

    pub fn dial(&mut self, peer: PeerId) {
        self.cmd(NetworkCommand::Dial(peer));
    }

    pub fn dial_address(&mut self, peer: PeerId, addr: Multiaddr) {
        self.cmd(NetworkCommand::DialAddress(peer, addr));
    }

    pub fn ban(&mut self, peer: PeerId) {
        self.cmd(NetworkCommand::Ban(peer));
    }

    pub fn unban(&mut self, peer: PeerId) {
        self.cmd(NetworkCommand::Unban(peer));
    }

    pub fn peers(&self) -> Vec<PeerId> {
        self.peers.project(|peers| peers.keys().copied().collect())
    }

    pub fn connections(&self) -> Vec<(PeerId, Multiaddr, DateTime<Utc>, Direction)> {
        self.peers.project(|peers| {
            peers
                .iter()
                .flat_map(|(peer, info)| {
                    info.connections
                        .iter()
                        .map(move |(a, t)| (*peer, a.clone(), t.0, t.1))
                })
                .collect()
        })
    }

    pub fn is_connected(&self, peer: &PeerId) -> bool {
        *peer == self.local_peer_id()
            || self.peers.project(|peers| {
                peers
                    .get(peer)
                    .map(|info| !info.connections.is_empty())
                    .unwrap_or(false)
            })
    }

    pub fn peer_info(&self, peer: &PeerId) -> Option<PeerInfo> {
        self.peers.project(|peers| peers.get(peer).cloned())
    }

    pub fn bootstrap(
        &mut self,
        peers: Vec<(PeerId, Multiaddr)>,
    ) -> impl Future<Output = Result<()>> {
        let (tx, rx) = oneshot::channel();
        if let Some((_, err)) = self.cmd(NetworkCommand::Bootstrap(peers, tx)) {
            return future::ready(Err(anyhow!("{}", err))).left_future();
        }
        tracing::debug!("started bootstrap");
        async {
            rx.await??;
            tracing::debug!("boostrap complete");
            Ok(())
        }
        .right_future()
    }

    pub fn is_bootstrapped(&self) -> bool {
        self.bootstrapped.get()
    }

    // This weird function signature seems impossible to support. WTF.
    // pub async fn get_closest_peers<K>(&self, key: K) -> Result<Vec<PeerId>>
    // where
    //     K: Into<BucketKey<K>> + Into<Vec<u8>> + Clone,
    // {
    //     let rx = {
    //         let mut swarm = self.swarm.lock();
    //         swarm.behaviour_mut().get_closest_peers(key)
    //     };
    //     Ok(rx.await??)
    // }

    pub fn providers(&mut self, key: Key) -> impl Future<Output = Result<HashSet<PeerId>>> {
        let (tx, rx) = oneshot::channel();
        if let Some((_, err)) = self.cmd(NetworkCommand::Providers(key, tx)) {
            return future::ready(Err(anyhow!("{}", err))).left_future();
        }
        async { rx.await? }.right_future()
    }

    pub fn provide(&mut self, key: Key) -> impl Future<Output = Result<()>> {
        let (tx, rx) = oneshot::channel();
        if let Some((_, err)) = self.cmd(NetworkCommand::Provide(key, tx)) {
            return future::ready(Err(anyhow!("{}", err))).left_future();
        }
        async { rx.await? }.right_future()
    }

    pub fn unprovide(&mut self, key: Key) -> Result<()> {
        if let Some((_, err)) = self.cmd(NetworkCommand::Unprovide(key)) {
            return Err(anyhow!("{}", err));
        }
        Ok(())
    }

    pub fn get_record(
        &mut self,
        key: Key,
        quorum: Quorum,
    ) -> impl Future<Output = Result<Vec<PeerRecord>>> {
        let (tx, rx) = oneshot::channel();
        if let Some((_, err)) = self.cmd(NetworkCommand::GetRecord(key, quorum, tx)) {
            return future::ready(Err(anyhow!("{}", err))).left_future();
        }
        async { rx.await? }.right_future()
    }

    pub fn put_record(
        &mut self,
        record: Record,
        quorum: Quorum,
    ) -> impl Future<Output = Result<()>> {
        let (tx, rx) = oneshot::channel();
        if let Some((_, err)) = self.cmd(NetworkCommand::PutRecord(record, quorum, tx)) {
            return future::ready(Err(anyhow!("{}", err))).left_future();
        }
        async { rx.await? }.right_future()
    }

    pub fn remove_record(&mut self, key: Key) -> Result<()> {
        if let Some((_, err)) = self.cmd(NetworkCommand::RemoveRecord(key)) {
            return Err(anyhow!("{}", err));
        }
        Ok(())
    }

    pub fn subscribe(
        &mut self,
        topic: String,
    ) -> impl Future<Output = Result<impl Stream<Item = GossipEvent>>> {
        let (tx, rx) = oneshot::channel();
        if let Some((_, err)) = self.cmd(NetworkCommand::Subscribe(topic, tx)) {
            return future::ready(Err(anyhow!("{}", err))).left_future();
        }
        async { rx.await? }.right_future()
    }

    pub fn publish(&mut self, topic: String, msg: Vec<u8>) -> impl Future<Output = Result<()>> {
        let (tx, rx) = oneshot::channel();
        if let Some((_, err)) = self.cmd(NetworkCommand::Publish(topic, msg, tx)) {
            return future::ready(Err(anyhow!("{}", err))).left_future();
        }
        async { rx.await? }.right_future()
    }

    pub fn broadcast(&mut self, topic: String, msg: Vec<u8>) -> impl Future<Output = Result<()>> {
        let (tx, rx) = oneshot::channel();
        if let Some((_, err)) = self.cmd(NetworkCommand::Broadcast(topic, msg, tx)) {
            return future::ready(Err(anyhow!("{}", err))).left_future();
        }
        async { rx.await? }.right_future()
    }

    // This cannot take `&mut self` due to trait constraints, so it needs to use the less efficient cmd_shared.
    pub fn get(&self, cid: Cid, providers: Vec<PeerId>) -> impl Future<Output = Result<GetQuery>> {
        let (tx, rx) = oneshot::channel();
        if let Some((_, err)) = self.cmd_shared(NetworkCommand::Get(cid, providers, tx)) {
            return future::ready(Err(anyhow!("{}", err))).left_future();
        }
        async { Ok(rx.await?) }.right_future()
    }

    // This cannot take `&mut self` due to trait constraints, so it needs to use the less efficient cmd_shared.
    pub fn sync(
        &self,
        cid: Cid,
        providers: Vec<PeerId>,
        missing: Vec<Cid>,
    ) -> impl Future<Output = Result<SyncQuery>> {
        if missing.is_empty() {
            return future::ready(Ok(SyncQuery::ready(Ok(())))).left_future();
        }
        if providers.is_empty() {
            return future::ready(Ok(SyncQuery::ready(Err(BlockNotFound(missing[0]).into()))))
                .left_future();
        }
        let (tx, rx) = oneshot::channel();
        if let Some((_, err)) = self.cmd_shared(NetworkCommand::Sync(cid, providers, missing, tx)) {
            return future::ready(Err(anyhow!("{}", err))).left_future();
        }
        async { Ok(rx.await?) }.right_future()
    }

    pub fn swarm_events(&mut self) -> impl Future<Output = Result<SwarmEvents>> {
        let (tx, rx) = oneshot::channel();
        if let Some((_, err)) = self.cmd(NetworkCommand::SwarmEvents(tx)) {
            return future::ready(Err(anyhow!("{}", err))).left_future();
        }
        async { Ok(rx.await?) }.right_future()
    }
}

async fn poll_swarm<P: StoreParams>(
    mut cmd_rx: Receiver<NetworkCommand>,
    cmd_tx: Sender<NetworkCommand>,
    mut swarm: Swarm<NetworkBackendBehaviour<P>>,
    executor: Executor,
    bootstrapped: Writer<bool>,
) {
    let mut subscriptions =
        FnvHashMap::<String, Vec<mpsc::UnboundedSender<GossipEvent>>>::default();
    let mut queries = FnvHashMap::<QueryId, QueryChannel>::default();
    loop {
        match future::select(
            future::poll_fn(|cx| swarm.poll_next_unpin(cx)),
            cmd_rx.next(),
        )
        .await
        {
            Either::Left((None, _)) => {
                tracing::debug!("poll_swarm: swarm stream ended, terminating");
                return;
            }
            Either::Left((Some(cmd), _)) => match cmd {
                SwarmEvent::ConnectionClosed {
                    peer_id,
                    endpoint,
                    num_established,
                    cause,
                } => swarm.behaviour_mut().connection_closed(
                    peer_id,
                    endpoint,
                    num_established,
                    cause,
                ),
                SwarmEvent::Behaviour(event) => {
                    let swarm = swarm.behaviour_mut();
                    match event {
                        behaviour::NetworkBackendBehaviourEvent::Peers(e) => unreachable(e),
                        behaviour::NetworkBackendBehaviourEvent::Kad(e) => {
                            let mut bootstrap_complete = *bootstrapped.read();
                            let bootstrap_old = bootstrap_complete;
                            // DO NOT HOLD bootstrapped LOCK ACROSS ARBITRARY CODE
                            swarm.inject_kad_event(e, &mut bootstrap_complete, &mut queries);
                            if bootstrap_complete != bootstrap_old {
                                *bootstrapped.write() = bootstrap_complete;
                            }
                        }
                        behaviour::NetworkBackendBehaviourEvent::Mdns(e) => {
                            swarm.inject_mdns_event(e);
                        }
                        behaviour::NetworkBackendBehaviourEvent::Ping(e) => {
                            swarm.inject_ping_event(e);
                        }
                        behaviour::NetworkBackendBehaviourEvent::Identify(e) => {
                            swarm.inject_id_event(e);
                        }
                        behaviour::NetworkBackendBehaviourEvent::Bitswap(e) => {
                            swarm.inject_bitswap_event(e, &mut queries);
                        }
                        behaviour::NetworkBackendBehaviourEvent::Gossipsub(e) => {
                            swarm.inject_gossip_event(e, &mut subscriptions);
                        }
                        behaviour::NetworkBackendBehaviourEvent::Broadcast(e) => {
                            swarm.inject_broadcast_event(e, &mut subscriptions);
                        }
                    }
                }
                _ => {}
            },
            Either::Right((None, _)) => {
                tracing::debug!("poll_swarm: command sender dropped, terminating");
                return;
            }
            Either::Right((Some(cmd), _)) => match cmd {
                NetworkCommand::ListenOn(addr, response) => {
                    let (tx, rx) = mpsc::unbounded();
                    swarm.behaviour_mut().swarm_events(tx);
                    match swarm.listen_on(addr.clone()) {
                        Ok(listener) => executor
                            .spawn(forward_listener_events(listener, response, rx))
                            .detach(),
                        Err(error) => {
                            response
                                .unbounded_send(ListenerEvent::ListenFailed(
                                    addr,
                                    error.to_string(),
                                ))
                                .ok();
                        }
                    };
                }
                NetworkCommand::AddExternalAddress(addr) => {
                    swarm.add_external_address(addr, AddressScore::Infinite);
                }
                NetworkCommand::AddAddress(peer, addr) => {
                    swarm
                        .behaviour_mut()
                        .add_address(&peer, addr, AddressSource::User);
                }
                NetworkCommand::RemoveAddress(peer, addr) => {
                    swarm.behaviour_mut().remove_address(&peer, &addr);
                }
                NetworkCommand::PrunePeers(min_age) => {
                    swarm.behaviour_mut().prune_peers(min_age);
                }
                NetworkCommand::Dial(peer) => {
                    swarm.behaviour_mut().dial(&peer);
                }
                NetworkCommand::DialAddress(peer, addr) => {
                    swarm.behaviour_mut().dial_address(&peer, addr);
                }
                NetworkCommand::Ban(peer) => {
                    swarm.ban_peer_id(peer);
                }
                NetworkCommand::Unban(peer) => {
                    swarm.unban_peer_id(peer);
                }
                NetworkCommand::Bootstrap(initial, tx) => {
                    let swarm = swarm.behaviour_mut();
                    for (peer, addr) in initial {
                        swarm.add_address(&peer, addr.clone(), AddressSource::User);
                        swarm.dial(&peer);
                    }
                    swarm.bootstrap(&mut queries, tx);
                }
                NetworkCommand::Providers(key, tx) => {
                    let bootstrap_complete = *bootstrapped.read();
                    swarm
                        .behaviour_mut()
                        .providers(key, bootstrap_complete, &mut queries, tx);
                }
                NetworkCommand::Provide(key, tx) => {
                    let bootstrap_complete = *bootstrapped.read();
                    swarm
                        .behaviour_mut()
                        .provide(key, bootstrap_complete, &mut queries, tx);
                }
                NetworkCommand::Unprovide(key) => {
                    swarm.behaviour_mut().unprovide(&key);
                }
                NetworkCommand::GetRecord(key, quorum, tx) => {
                    let bootstrap_complete = *bootstrapped.read();
                    swarm.behaviour_mut().get_record(
                        key,
                        quorum,
                        bootstrap_complete,
                        &mut queries,
                        tx,
                    );
                }
                NetworkCommand::PutRecord(record, quorum, tx) => {
                    let bootstrap_complete = *bootstrapped.read();
                    swarm.behaviour_mut().put_record(
                        record,
                        quorum,
                        bootstrap_complete,
                        &mut queries,
                        tx,
                    );
                }
                NetworkCommand::RemoveRecord(key) => {
                    swarm.behaviour_mut().remove_record(&key);
                }
                NetworkCommand::Subscribe(topic, tx) => {
                    tx.send(swarm.behaviour_mut().subscribe(&*topic, &mut subscriptions))
                        .ok();
                }
                NetworkCommand::Publish(topic, msg, tx) => {
                    tx.send(swarm.behaviour_mut().publish(&*topic, msg)).ok();
                }
                NetworkCommand::Broadcast(topic, msg, tx) => {
                    tx.send(swarm.behaviour_mut().broadcast(&*topic, msg)).ok();
                }
                NetworkCommand::Get(cid, providers, tx) => {
                    let (rx, id) =
                        swarm
                            .behaviour_mut()
                            .get(cid, providers.into_iter(), &mut queries);
                    tx.send(GetQuery {
                        swarm: cmd_tx.clone(),
                        id,
                        rx,
                    })
                    .ok();
                }
                NetworkCommand::Sync(cid, providers, missing, tx) => {
                    let (rx, id) = swarm.behaviour_mut().sync(
                        cid,
                        providers,
                        missing.into_iter(),
                        &mut queries,
                    );
                    tx.send(SyncQuery {
                        swarm: Some(cmd_tx.clone()),
                        id: Some(id),
                        rx,
                    })
                    .ok();
                }
                NetworkCommand::SwarmEvents(result) => {
                    let (tx, rx) = mpsc::unbounded();
                    swarm.behaviour_mut().swarm_events(tx);
                    result.send(SwarmEvents::new(rx)).ok();
                }
                NetworkCommand::CancelQuery(id) => {
                    swarm.behaviour_mut().cancel(id, &mut queries);
                }
            },
        }
    }
}

fn forward_listener_events(
    listener: ListenerId,
    response: UnboundedSender<ListenerEvent>,
    rx: UnboundedReceiver<Event>,
) -> impl Future<Output = ()> {
    rx.take_while(move |event| match event {
        Event::ListenerClosed(id) if *id == listener => future::ready(false),
        Event::NewListenAddr(id, addr) if *id == listener => future::ready(
            response
                .unbounded_send(ListenerEvent::NewListenAddr(addr.clone()))
                .is_ok(),
        ),
        Event::ExpiredListenAddr(id, addr) if *id == listener => future::ready(
            response
                .unbounded_send(ListenerEvent::ExpiredListenAddr(addr.clone()))
                .is_ok(),
        ),
        _ => future::ready(true),
    })
    .for_each(|_| future::ready(()))
}

#[derive(Debug)]
pub struct GetQuery {
    swarm: Sender<NetworkCommand>,
    id: QueryId,
    rx: GetChannel,
}

impl Future for GetQuery {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for GetQuery {
    fn drop(&mut self) {
        if let Err(err) = self.swarm.try_send(NetworkCommand::CancelQuery(self.id)) {
            if !err.is_disconnected() {
                tracing::warn!("cannot cancel dropped GetQuery: {}", err.into_send_error());
            }
        }
    }
}

/// A `bitswap` sync query.
#[derive(Debug)]
pub struct SyncQuery {
    swarm: Option<Sender<NetworkCommand>>,
    id: Option<QueryId>,
    rx: SyncChannel,
}

impl SyncQuery {
    fn ready(res: Result<()>) -> Self {
        let (tx, rx) = mpsc::unbounded();
        tx.unbounded_send(SyncEvent::Complete(res)).unwrap();
        Self {
            swarm: None,
            id: None,
            rx,
        }
    }
}

impl Future for SyncQuery {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        loop {
            let poll = Pin::new(&mut self.rx).poll_next(cx);
            tracing::trace!("sync progress: {:?}", poll);
            match poll {
                Poll::Ready(Some(SyncEvent::Complete(result))) => return Poll::Ready(result),
                Poll::Ready(_) => continue,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Stream for SyncQuery {
    type Item = SyncEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let poll = Pin::new(&mut self.rx).poll_next(cx);
        tracing::trace!("sync progress: {:?}", poll);
        poll
    }
}

impl Drop for SyncQuery {
    fn drop(&mut self) {
        if let (Some(id), Some(mut swarm)) = (self.id.take(), self.swarm.take()) {
            if let Err(err) = swarm.try_send(NetworkCommand::CancelQuery(id)) {
                if !err.is_disconnected() {
                    tracing::warn!("cannot cancel dropped SyncQuery: {}", err.into_send_error());
                }
            }
        }
    }
}
