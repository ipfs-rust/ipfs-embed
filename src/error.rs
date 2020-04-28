use libipld_core::cid::Error as CidError;
use libipld_core::error::BlockError;
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
}

impl From<Error> for BlockError {
    fn from(error: Error) -> Self {
        match error {
            Error::Cid(cid) => BlockError::Cid(cid),
            err => BlockError::Io(err.into()),
        }
    }
}

impl From<Error> for IoError {
    fn from(error: Error) -> Self {
        IoError::new(ErrorKind::Other, error)
    }
}
