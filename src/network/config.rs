use libp2p::core::{Multiaddr, PeerId};
use libp2p::identity::Keypair;
use libp2p::multiaddr::Protocol;

const IPFS_BOOTSTRAP_NODES: &[&str] = &[
    "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
    "/ip4/104.236.179.241/tcp/4001/p2p/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
    "/ip4/104.236.76.40/tcp/4001/p2p/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
    "/ip4/128.199.219.111/tcp/4001/p2p/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
    "/ip4/178.62.158.247/tcp/4001/p2p/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",
    "/ip6/2400:6180:0:d0::151:6001/tcp/4001/p2p/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
    "/ip6/2604:a880:1:20::203:d001/tcp/4001/p2p/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
    "/ip6/2604:a880:800:10::4a:5001/tcp/4001/p2p/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
    "/ip6/2a03:b0c0:0:1010::23:1001/tcp/4001/p2p/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",
];

fn ipfs_bootstrap_nodes() -> Vec<(Multiaddr, PeerId)> {
    let mut bootstrap = vec![];
    for addr in IPFS_BOOTSTRAP_NODES {
        let mut addr: Multiaddr = addr.parse().unwrap();
        let peer_id = match addr.pop() {
            Some(Protocol::P2p(hash)) => PeerId::from_multihash(hash).unwrap(),
            _ => unreachable!(),
        };
        bootstrap.push((addr, peer_id));
    }
    bootstrap
}

pub struct NetworkConfig {
    pub enable_mdns: bool,
    pub enable_ping: bool,
    pub bootstrap_nodes: Vec<(Multiaddr, PeerId)>,
    pub keypair: Keypair,
}

impl NetworkConfig {
    pub fn new(keypair: Keypair) -> Self {
        Self {
            enable_mdns: true,
            enable_ping: true,
            bootstrap_nodes: ipfs_bootstrap_nodes(),
            keypair,
        }
    }

    pub fn peer_id(&self) -> PeerId {
        self.keypair.public().into_peer_id()
    }
}
