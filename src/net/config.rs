use crate::config::*;
use libp2p::identity::ed25519::Keypair;

/// Network configuration.
#[derive(Debug)]
pub struct NetworkConfig {
    /// Enable adding loopback addresses to the address book. Should be
    /// enabled during testing and disabled in production.
    pub enable_loopback: bool,
    /// Enable binding to the listen port number when dialling peers
    /// instead of using a random outgoing port. While this may allow
    /// stricter firewall confiuration or shorter peer lists when interacting
    /// with other IPFS implementations, it also opens up the possibility of
    /// TCP simultaneous open, which leads to spurious dial errors.
    pub port_reuse: bool,
    /// Node name.
    pub node_name: String,
    /// Node key.
    pub node_key: Keypair,
    /// Pre shared key.
    pub psk: Option<[u8; 32]>,
    /// Dns config. If no dns config is provided the system
    /// defaults will be used.
    pub dns: Option<DnsConfig>,
    /// Mdns config.
    pub mdns: Option<MdnsConfig>,
    /// Kad config.
    pub kad: Option<KadConfig>,
    /// Ping config.
    pub ping: Option<PingConfig>,
    /// Identify config. Note that the `node_name` and
    /// `node_key` will overwrite the `local_public_key` and
    /// the `agent_version`.
    pub identify: Option<IdentifyConfig>,
    /// Gossipsub config.
    pub gossipsub: Option<GossipsubConfig>,
    /// Broadcast config.
    pub broadcast: Option<BroadcastConfig>,
    /// Bitswap config.
    pub bitswap: Option<BitswapConfig>,
    /// Keep explicitly dialed and incoming connections open indefinitely
    pub keep_alive: bool,
}

/// `DNS` configuration.
#[derive(Debug)]
pub enum DnsConfig {
    Custom {
        /// Configures the nameservers to use.
        config: ResolverConfig,
        /// Configuration for the resolver.
        opts: ResolverOpts,
    },
    SystemWithFallback {
        /// Configures the nameservers to use.
        config: ResolverConfig,
        /// Configuration for the resolver.
        opts: ResolverOpts,
    },
}

impl NetworkConfig {
    /// Creates a new network configuration.
    pub fn new(node_key: Keypair) -> Self {
        let node_name = names::Generator::with_naming(names::Name::Numbered)
            .next()
            .unwrap();
        let identify = IdentifyConfig::new(
            "/ipfs-embed/1.0".into(),
            libp2p::identity::PublicKey::Ed25519(node_key.public()),
        );
        Self {
            enable_loopback: true,
            port_reuse: true,
            node_name,
            node_key,
            psk: None,
            dns: None,
            mdns: Some(MdnsConfig::default()),
            kad: Some(KadConfig::default()),
            ping: None,
            identify: Some(identify),
            gossipsub: Some(GossipsubConfig::default()),
            broadcast: Some(BroadcastConfig::default()),
            bitswap: Some(BitswapConfig::default()),
            keep_alive: false,
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self::new(Keypair::generate())
    }
}
