use libipld::cid::{Cid, Error as CidError};
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
    Sled2(#[from] TransactionError),
    #[error(transparent)]
    Cid(#[from] CidError),
    #[error(transparent)]
    Net(#[from] TransportError<IoError>),
    #[error(transparent)]
    Ipld(#[from] IpldError),
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("failed to retrieve block {0}")]
    BlockNotFound(Cid),
}

impl From<Error> for StoreError {
    fn from(error: Error) -> Self {
        match error {
            Error::BlockNotFound(cid) => Self::BlockNotFound(cid),
            _ => Self::Other(Box::new(error)),
        }
    }
}
