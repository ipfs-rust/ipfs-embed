//! IpfsEmbed is an embeddable ipfs implementation.
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use ipfs_embed::{Config, Store};
//! let config = Config::from_path("/tmp/db")?;
//! let store = Store::new(config)?;
//! # Ok(()) }
//! ```
mod config;
mod error;
mod gc;
mod network;
mod storage;
mod store;

pub use config::{Config, TREE};
pub use error::Error;
pub use libipld::cid::Cid;
pub use libipld::store::{AliasStore, ReadonlyStore, Store as WritableStore};
pub use libp2p::core::{Multiaddr, PeerId};
pub use network::NetworkConfig;
pub use sled::IVec;
pub use storage::Metadata;
pub use store::Store;
