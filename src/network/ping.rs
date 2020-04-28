use crate::network::behaviour::NetworkBackendBehaviour;
use libp2p::ping::PingEvent;
use libp2p::swarm::NetworkBehaviourEventProcess;

impl NetworkBehaviourEventProcess<PingEvent> for NetworkBackendBehaviour {
    fn inject_event(&mut self, event: PingEvent) {
        // Don't really need to do anything here as ping handles disconnecting automatically.
        if let Err(err) = &event.result {
            log::debug!("ping: {} {:?}", event.peer.to_base58(), err);
        }
    }
}
