use libp2p::core::PeerId;
use libp2p::gossipsub::GossipsubConfig;
use libp2p::identity::{Keypair, PublicKey};
use libp2p::mdns::MdnsConfig;
use libp2p::ping::PingConfig;
use libp2p::pnet::PreSharedKey;
use libp2p_bitswap::BitswapConfig;

/// Network configuration.
#[derive(Clone)]
pub struct NetworkConfig {
    /// Node identity keypair.
    pub node_key: Keypair,
    /// Name of the node. Sent over the wire for debugging purposes.
    pub node_name: String,
    /// Enable mdns.
    pub enable_mdns: bool,
    /// Enable kad.
    pub enable_kad: bool,
    /// Should we insert non-global addresses into the DHT?
    pub allow_non_globals_in_dht: bool,
    /// Pre shared key for pnet.
    pub psk: Option<PreSharedKey>,
    /// Mdns config.
    pub mdns: MdnsConfig,
    /// Ping config.
    pub ping: PingConfig,
    /// Gossipsub config.
    pub gossipsub: GossipsubConfig,
    /// Bitswap config.
    pub bitswap: BitswapConfig,
}

impl NetworkConfig {
    /// Creates a new network configuration.
    pub fn new() -> Self {
        Self {
            enable_mdns: true,
            enable_kad: true,
            allow_non_globals_in_dht: false,
            node_key: Keypair::generate_ed25519(),
            node_name: names::Generator::with_naming(names::Name::Numbered)
                .next()
                .unwrap(),
            psk: None,
            mdns: MdnsConfig::default(),
            ping: PingConfig::new().with_keep_alive(true),
            gossipsub: GossipsubConfig::default(),
            bitswap: BitswapConfig::default(),
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

impl std::fmt::Debug for NetworkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("NetworkConfig")
            .field("node_key", &self.peer_id().to_string())
            .field("node_name", &self.node_name)
            .field("enable_mdns", &self.enable_mdns)
            .field("enable_kad", &self.enable_kad)
            .field("allow_non_globals_in_dht", &self.allow_non_globals_in_dht)
            .field("psk", &self.psk.is_some())
            .field("mdns", &self.mdns)
            .field("ping", &self.ping)
            .field("gossipsub", &self.gossipsub)
            .field("bitswap", &self.bitswap)
            .finish()
    }
}
