use crate::network::behaviour::NetworkBackendBehaviour;
use libp2p::kad::KademliaEvent;
use libp2p::kad::{GetProvidersError, GetProvidersOk};
use libp2p::swarm::NetworkBehaviourEventProcess;

impl NetworkBehaviourEventProcess<KademliaEvent> for NetworkBackendBehaviour {
    fn inject_event(&mut self, event: KademliaEvent) {
        match event {
            KademliaEvent::GetProvidersResult(Ok(GetProvidersOk {
                key,
                providers,
                closest_peers,
            })) => {
                if providers.is_empty() {
                    log::debug!("no provider for {:?}", key);
                } else {
                    log::debug!("found provider for {:?}", key);
                    for peer in closest_peers {
                        self.connect(peer);
                    }
                }
            }
            KademliaEvent::GetProvidersResult(Err(err)) => match err {
                GetProvidersError::Timeout { key, .. } => {
                    log::debug!("get provider timed out for {:?}", key);
                }
            },
            event => log::trace!("{:?}", event),
        }
    }
}
