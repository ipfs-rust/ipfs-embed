use crate::network::behaviour::NetworkBackendBehaviour;
use libp2p::mdns::MdnsEvent;
use libp2p::swarm::NetworkBehaviourEventProcess;

impl NetworkBehaviourEventProcess<MdnsEvent> for NetworkBackendBehaviour {
    fn inject_event(&mut self, event: MdnsEvent) {
        match event {
            MdnsEvent::Discovered(list) => {
                for (peer, _) in list {
                    //log::trace!("mdns: Discovered peer {}", peer.to_base58());
                    self.connect(peer);
                }
            }
            MdnsEvent::Expired(_) => {
                // Don't need to do anything here as ping should handle disconnecting from nodes
            }
        }
    }
}
