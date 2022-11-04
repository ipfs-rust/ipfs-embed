use libp2p::{
    core::{upgrade::DeniedUpgrade, ConnectedPoint},
    multiaddr::Protocol,
    swarm::{
        handler::{InboundUpgradeSend, OutboundUpgradeSend},
        ConnectionHandler, IntoConnectionHandler, KeepAlive, SubstreamProtocol,
    },
    Multiaddr, PeerId,
};
use std::{
    convert::TryInto,
    task::{Context, Poll},
};
use void::Void;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntoAddressHandler(pub Option<(Multiaddr, u8)>, pub bool);

impl IntoAddressHandler {
    pub fn peer_id(&self) -> Option<PeerId> {
        let (addr, _retries) = self.0.as_ref()?;
        match addr.iter().last() {
            Some(Protocol::P2p(p)) => p.try_into().ok(),
            _ => None,
        }
    }
}

impl IntoConnectionHandler for IntoAddressHandler {
    type Handler = AddressHandler;

    fn into_handler(
        self,
        remote_peer_id: &libp2p::PeerId,
        connected_point: &libp2p::core::ConnectedPoint,
    ) -> Self::Handler {
        AddressHandler {
            own_dial: self.0,
            remote_peer_id: *remote_peer_id,
            connected_point: connected_point.clone(),
            keep_alive: self.1,
        }
    }

    fn inbound_protocol(&self) -> DeniedUpgrade {
        DeniedUpgrade
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressHandler {
    pub own_dial: Option<(Multiaddr, u8)>,
    pub remote_peer_id: PeerId,
    pub connected_point: ConnectedPoint,
    pub keep_alive: bool,
}

impl ConnectionHandler for AddressHandler {
    type InEvent = Void;
    type OutEvent = Void;
    type Error = Void;
    type InboundProtocol = DeniedUpgrade;
    type OutboundProtocol = DeniedUpgrade;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = Void;

    fn listen_protocol(
        &self,
    ) -> libp2p::swarm::SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(DeniedUpgrade, ())
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        _protocol: <Self::InboundProtocol as InboundUpgradeSend>::Output,
        _info: Self::InboundOpenInfo,
    ) {
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        _protocol: <Self::OutboundProtocol as OutboundUpgradeSend>::Output,
        _info: Self::OutboundOpenInfo,
    ) {
    }

    fn inject_event(&mut self, _event: Self::InEvent) {}

    fn inject_dial_upgrade_error(
        &mut self,
        _info: Self::OutboundOpenInfo,
        _error: libp2p::swarm::ConnectionHandlerUpgrErr<
            <Self::OutboundProtocol as OutboundUpgradeSend>::Error,
        >,
    ) {
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        if self.keep_alive {
            KeepAlive::Yes
        } else {
            KeepAlive::No
        }
    }

    #[allow(clippy::type_complexity)]
    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<
        libp2p::swarm::ConnectionHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::OutEvent,
            Self::Error,
        >,
    > {
        Poll::Pending
    }
}
