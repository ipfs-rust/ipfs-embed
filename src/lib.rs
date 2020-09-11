//! IpfsEmbed is an embeddable ipfs implementation.
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use ipfs_embed::{Config, Store, Multicodec, Multihash};
//! let config = Config::from_path_local("/tmp/db")?;
//! let store = Store::<Multicodec, Multihash>::new(config)?;
//! # Ok(()) }
//! ```
mod network;
mod store;

pub use libipld::store::Store;
pub use libipld::{Cid, Multicodec, Multihash};
pub use libp2p::core::{Multiaddr, PeerId};
pub use network::NetworkConfig;
pub use store::Ipfs;
