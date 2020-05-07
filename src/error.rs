use libipld::cid::{Cid, Error as CidError};
use libipld::error::StoreError;
use libp2p::core::transport::TransportError;
use sled::transaction::TransactionError;
use sled::Error as SledError;
use std::io::{Error as IoError, ErrorKind};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Sled(#[from] SledError),
    #[error("{0}")]
    Sled2(#[from] TransactionError),
    #[error("{0}")]
    Cid(#[from] CidError),
    #[error("{0}")]
    Net(#[from] TransportError<IoError>),
    #[error("failed to retrieve block {0}")]
    BlockNotFound(Cid),
}

impl From<Error> for StoreError {
    fn from(error: Error) -> Self {
        match error {
            Error::BlockNotFound(cid) => Self::BlockNotFound(cid),
            Error::Net(TransportError::Other(io)) => Self::Io(io),
            _ => Self::Io(IoError::new(ErrorKind::Other, error)),
        }
    }
}
