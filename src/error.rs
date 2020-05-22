use libipld::cid::Error as CidError;
use libipld::error::{Error as IpldError, StoreError};
use libp2p::core::transport::TransportError;
use sled::transaction::TransactionError;
use sled::Error as SledError;
use std::io::Error as IoError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Sled(#[from] SledError),
    #[error(transparent)]
    Transaction(#[from] TransactionError),
    #[error(transparent)]
    Cid(#[from] CidError),
    #[error(transparent)]
    Transport(#[from] TransportError<IoError>),
    #[error(transparent)]
    Ipld(#[from] IpldError),
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("empty batch")]
    EmptyBatch,
}

impl From<Error> for StoreError {
    fn from(error: Error) -> Self {
        if let Error::EmptyBatch = error {
            return Self::EmptyBatch;
        }
        Self::Other(Box::new(error))
    }
}
