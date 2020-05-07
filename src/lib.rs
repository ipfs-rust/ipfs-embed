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
mod network;
mod storage;
mod store;

pub use config::Config;
pub use store::Store;
