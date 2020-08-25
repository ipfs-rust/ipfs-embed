//! IpfsEmbed is an embeddable ipfs implementation.
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use ipfs_embed::{Config, Store, Multicodec, Multihash};
//! let config = Config::from_path_local("/tmp/db")?;
//! let store = Store::<Multicodec, Multihash>::new(config)?;
//! # Ok(()) }
//! ```
mod config;
mod gc;
mod network;
mod storage;
mod store;

pub use config::{Config, TREE};
pub use libipld::store::{AliasStore, ReadonlyStore, Store as WritableStore};
pub use libipld::{Cid, Multicodec, Multihash};
pub use libp2p::core::{Multiaddr, PeerId};
pub use network::NetworkConfig;
pub use sled::IVec;
pub use storage::Metadata;
pub use store::Store;

/// The maximum block size is 1MiB.
pub const MAX_BLOCK_SIZE: usize = 1_048_576;
