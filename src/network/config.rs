use libp2p::core::{Multiaddr, PeerId};
use libp2p::identity::{Keypair, PublicKey};
use std::time::Duration;

/// Network configuration.
#[derive(Clone)]
pub struct NetworkConfig {
    /// Multiaddresses to listen for incoming connections.
    pub listen_addresses: Vec<Multiaddr>,
    /// Multiaddresses to advertise. Detected automatically if empty.
    pub public_addresses: Vec<Multiaddr>,
    /// List of initial node addresses.
    pub boot_nodes: Vec<(Multiaddr, PeerId)>,
    /// Node identity keypair.
    pub node_key: Keypair,
    /// Name of the node. Sent over the wire for debugging purposes.
    pub node_name: String,
    /// Enable mdns.
    pub enable_mdns: bool,
    /// Enable ping.
    pub enable_ping: bool,
    /// Should we insert non-global addresses into the DHT?
    pub allow_non_globals_in_dht: bool,
    /// Block fetch timeout.
    pub timeout: Duration,
}

impl NetworkConfig {
    /// Creates a new network configuration.
    pub fn new() -> Self {
        Self {
            listen_addresses: vec!["/ip4/0.0.0.0/tcp/0".parse().unwrap()],
            public_addresses: vec![],
            boot_nodes: vec![],
            enable_mdns: true,
            enable_ping: true,
            allow_non_globals_in_dht: false,
            node_key: Keypair::generate_ed25519(),
            node_name: names::Generator::with_naming(names::Name::Numbered)
                .next()
                .unwrap(),
            timeout: Duration::from_secs(20),
        }
    }

    /// Creates a new local network configuration.
    pub fn new_local() -> Self {
        let mut config = Self::new();
        config.listen_addresses = vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()];
        config.allow_non_globals_in_dht = true;
        config
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
