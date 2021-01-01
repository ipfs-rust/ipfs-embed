use libp2p::core::PeerId;
use libp2p::identity::{Keypair, PublicKey};
use std::num::NonZeroU16;
use std::time::Duration;

/// Network configuration.
#[derive(Clone)]
pub struct NetworkConfig {
    /// Node identity keypair.
    pub node_key: Keypair,
    /// Name of the node. Sent over the wire for debugging purposes.
    pub node_name: String,
    /// Enable mdns.
    pub enable_mdns: bool,
    /// Should we insert non-global addresses into the DHT?
    pub allow_non_globals_in_dht: bool,
    /// Bitswap request timeout.
    pub bitswap_request_timeout: Duration,
    /// Bitswap connection keep alive.
    pub bitswap_connection_keepalive: Duration,
    /// Bitswap inbound requests per peer limit.
    pub bitswap_receive_limit: NonZeroU16,
}

impl NetworkConfig {
    /// Creates a new network configuration.
    pub fn new() -> Self {
        Self {
            enable_mdns: true,
            allow_non_globals_in_dht: false,
            node_key: Keypair::generate_ed25519(),
            node_name: names::Generator::with_naming(names::Name::Numbered)
                .next()
                .unwrap(),
            bitswap_request_timeout: Duration::from_secs(10),
            bitswap_connection_keepalive: Duration::from_secs(10),
            bitswap_receive_limit: NonZeroU16::new(20).expect("20 > 0"),
        }
    }

    /// The public node key.
    pub fn public(&self) -> PublicKey {
        self.node_key.public()
    }

    /// The peer id of the node.
    pub fn peer_id(&self) -> PeerId {
        self.node_key.public().into_peer_id()
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self::new()
    }
}
