use crate::network::NetworkConfig;
use sled::{Error, Tree};
use std::path::Path;
use std::time::Duration;

pub const TREE: &str = "ipfs_tree";

pub struct Config {
    pub tree: Tree,
    pub timeout: Duration,
    pub network: NetworkConfig,
}

impl Config {
    /// Creates a new config.
    pub fn new(tree: Tree, network: NetworkConfig) -> Self {
        Self {
            tree,
            timeout: Duration::from_millis(20000),
            network,
        }
    }

    /// Creates a default configuration.
    pub fn from_path<T: AsRef<Path>>(path: T) -> Result<Self, Error> {
        let db = sled::open(path)?;
        let tree = db.open_tree(TREE)?;
        let network = NetworkConfig::new();
        Ok(Self::new(tree, network))
    }

    /// Creates a default local network configuration.
    pub fn from_path_local<T: AsRef<Path>>(path: T) -> Result<Self, Error> {
        let db = sled::open(path)?;
        let tree = db.open_tree(TREE)?;
        let network = NetworkConfig::new_local();
        Ok(Self::new(tree, network))
    }
}
