use fnv::FnvHashMap;
use libipld::DagCbor;
use libp2p::core::connection::{ConnectedPoint, ConnectionId};
use libp2p::identify::IdentifyInfo;
use libp2p::swarm::protocols_handler::DummyProtocolsHandler;
use libp2p::swarm::{DialPeerCondition, NetworkBehaviour, NetworkBehaviourAction, PollParameters};
use libp2p::{Multiaddr, PeerId};
use std::collections::VecDeque;
use std::task::{Context, Poll};
use std::time::Duration;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerInfo {
    protocol_version: Option<String>,
    agent_version: Option<String>,
    protocols: Vec<String>,
    addresses: FnvHashMap<Multiaddr, AddressSource>,
    rtt: Option<Duration>,
}

impl PeerInfo {
    pub fn protocol_version(&self) -> Option<&str> {
        self.protocol_version.as_deref()
    }

    pub fn agent_version(&self) -> Option<&str> {
        self.agent_version.as_deref()
    }

    pub fn protocols(&self) -> impl Iterator<Item = &str> + '_ {
        self.protocols.iter().map(|s| &**s)
    }

    pub fn addresses(&self) -> impl Iterator<Item = (&Multiaddr, AddressSource)> + '_ {
        self.addresses.iter().map(|(addr, source)| (addr, *source))
    }

    pub fn rtt(&self) -> Option<Duration> {
        self.rtt
    }
}

#[derive(Clone, Copy, DagCbor, Debug, Eq, PartialEq)]
#[ipld(repr = "int")]
pub enum AddressSource {
    Mdns,
    Kad,
    Peer,
    User,
}

#[derive(Debug)]
pub struct AddressBook {
    local_peer_id: PeerId,
    peers: FnvHashMap<PeerId, PeerInfo>,
    connections: FnvHashMap<PeerId, Multiaddr>,
    events: VecDeque<PeerId>,
}

impl AddressBook {
    pub fn new(local_peer_id: PeerId) -> Self {
        Self {
            local_peer_id,
            peers: Default::default(),
            connections: Default::default(),
            events: Default::default(),
        }
    }

    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    pub fn add_address(&mut self, peer: &PeerId, address: Multiaddr, source: AddressSource) {
        if peer == self.local_peer_id() {
            return;
        }
        tracing::trace!(
            "adding address {} for peer {} from {:?}",
            address,
            peer,
            source
        );
        let info = self.peers.entry(*peer).or_default();
        info.addresses.insert(address.clone(), source);
        if !self.is_connected(peer) {
            self.events.push_back(*peer);
        }
    }

    pub fn remove_address(&mut self, peer: &PeerId, address: &Multiaddr) {
        if let Some(info) = self.peers.get_mut(peer) {
            tracing::trace!("removing address {} for peer {}", address, peer);
            info.addresses.remove(address);
        }
    }

    pub fn peers(&self) -> impl Iterator<Item = &PeerId> + '_ {
        self.peers.keys()
    }

    pub fn connections(&self) -> impl Iterator<Item = (&PeerId, &Multiaddr)> + '_ {
        self.connections.iter().map(|(peer, addr)| (peer, addr))
    }

    pub fn is_connected(&self, peer: &PeerId) -> bool {
        self.connections.contains_key(peer) || peer == self.local_peer_id()
    }

    pub fn info(&self, peer_id: &PeerId) -> Option<&PeerInfo> {
        self.peers.get(peer_id)
    }

    pub fn set_rtt(&mut self, peer_id: &PeerId, rtt: Option<Duration>) {
        if let Some(info) = self.peers.get_mut(peer_id) {
            info.rtt = rtt;
        }
    }

    pub fn set_info(&mut self, peer_id: &PeerId, identify: IdentifyInfo) {
        if let Some(info) = self.peers.get_mut(peer_id) {
            info.protocol_version = Some(identify.protocol_version);
            info.agent_version = Some(identify.agent_version);
            info.protocols = identify.protocols;
        }
    }
}

impl NetworkBehaviour for AddressBook {
    type ProtocolsHandler = DummyProtocolsHandler;
    type OutEvent = void::Void;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        Default::default()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        if let Some(info) = self.peers.get(peer_id) {
            info.addresses().map(|(addr, _)| addr.clone()).collect()
        } else {
            vec![]
        }
    }

    fn inject_connected(&mut self, peer_id: &PeerId) {
        tracing::trace!("connected to {}", peer_id);
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId) {
        tracing::trace!("disconnected from {}", peer_id);
    }

    fn inject_event(&mut self, _peer_id: PeerId, _connection: ConnectionId, _event: void::Void) {}

    fn poll(
        &mut self,
        _cx: &mut Context,
        _params: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<void::Void, void::Void>> {
        if let Some(peer_id) = self.events.pop_front() {
            Poll::Ready(NetworkBehaviourAction::DialPeer {
                peer_id,
                condition: DialPeerCondition::Disconnected,
            })
        } else {
            Poll::Pending
        }
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        _: &ConnectionId,
        conn: &ConnectedPoint,
    ) {
        self.connections
            .insert(*peer_id, conn.get_remote_address().clone());
    }

    fn inject_address_change(
        &mut self,
        peer_id: &PeerId,
        _: &ConnectionId,
        _old: &ConnectedPoint,
        new: &ConnectedPoint,
    ) {
        self.connections
            .insert(*peer_id, new.get_remote_address().clone());
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        _: &ConnectionId,
        _conn: &ConnectedPoint,
    ) {
        self.connections.remove(&peer_id);
    }

    fn inject_addr_reach_failure(
        &mut self,
        peer_id: Option<&PeerId>,
        addr: &Multiaddr,
        error: &dyn std::error::Error,
    ) {
        if let Some(peer_id) = peer_id {
            tracing::trace!("inject_addr_reach_failure {}", error);
            self.remove_address(peer_id, addr);
        }
    }

    fn inject_dial_failure(&mut self, peer_id: &PeerId) {
        tracing::trace!("dial failure {}", peer_id);
        self.peers.remove(peer_id);
    }
}
