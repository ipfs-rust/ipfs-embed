use libipld::store::{DefaultParams, StoreParams};
pub use libp2p::dns::{ResolverConfig, ResolverOpts};
pub use libp2p::gossipsub::GossipsubConfig;
pub use libp2p::identify::IdentifyConfig;
pub use libp2p::kad::record::store::MemoryStoreConfig;
pub use libp2p::mdns::MdnsConfig;
pub use libp2p::ping::PingConfig;
pub use libp2p_bitswap::BitswapConfig;
pub use libp2p_broadcast::BroadcastConfig;
pub use libp2p_quic::{Keypair, ToLibp2p, TransportConfig};
pub use secrecy::Secret;
use std::time::Duration;

/// Network configuration.
#[derive(Debug)]
pub struct NetworkConfig {
    /// Node name.
    pub node_name: String,
    /// Node key.
    pub node_key: Keypair,
    /// Pre shared key.
    pub psk: Option<Secret<[u8; 32]>>,
    /// Quic config.
    pub quic: TransportConfig,
    /// Dns config. If no dns config is provided the system
    /// defaults will be used.
    pub dns: Option<DnsConfig>,
    /// Mdns config.
    pub mdns: Option<MdnsConfig>,
    /// Kad config.
    pub kad: Option<MemoryStoreConfig>,
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
}

/// `DNS` configuration.
#[derive(Debug)]
pub struct DnsConfig {
    /// Configures the nameservers to use.
    pub config: ResolverConfig,
    /// Configuration for the resolver.
    pub opts: ResolverOpts,
}

impl NetworkConfig {
    /// Creates a new network configuration.
    pub fn new(node_key: Keypair) -> Self {
        let node_name = names::Generator::with_naming(names::Name::Numbered)
            .next()
            .unwrap();
        let mut identify = IdentifyConfig::new("/ipfs-embed/1.0".into(), node_key.to_public());
        identify.agent_version = node_name.clone();
        let mut quic = TransportConfig::default();
        quic.keep_alive_interval(Some(Duration::from_millis(100)));
        quic.max_concurrent_bidi_streams(1024).unwrap();
        quic.max_idle_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        quic.stream_receive_window(DefaultParams::MAX_BLOCK_SIZE as _)
            .unwrap();
        quic.receive_window(4_000_000).unwrap();
        quic.send_window(4_000_000);
        Self {
            node_name,
            node_key,
            psk: None,
            quic,
            dns: None,
            mdns: Some(MdnsConfig::default()),
            kad: Some(MemoryStoreConfig::default()),
            ping: None,
            identify: Some(identify),
            gossipsub: Some(GossipsubConfig::default()),
            broadcast: Some(BroadcastConfig::default()),
            bitswap: Some(BitswapConfig::default()),
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self::new(Keypair::generate())
    }
}
