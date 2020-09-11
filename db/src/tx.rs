use crate::metadata::Metadata;
use anyhow::Result;
use libipld::cid::Cid;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum InvalidTransaction {
    #[error("block not found")]
    BlockNotFound,
    #[error("unpin underflow: {0}")]
    UnpinUnderflow(u64),
}

pub struct StateBuilder {
    metadata: Metadata,
    next_id: u64,
    lookup: HashMap<Cid, u64>,
    pins: HashMap<u64, u64>,
    rrefs: HashSet<(u64, u64)>,
}

impl StateBuilder {
    pub fn new(metadata: Metadata, next_id: u64) -> Self {
        Self {
            metadata,
            next_id,
            lookup: Default::default(),
            pins: Default::default(),
            rrefs: Default::default(),
        }
    }

    fn _get(&mut self, cid: &Cid) -> Result<Option<u64>> {
        let id = if let Some(id) = self.lookup.get(&cid) {
            Some(*id)
        } else {
            let id = self.metadata.id(cid)?;
            if let Some(id) = id {
                self.lookup.insert(cid.clone(), id);
            }
            id
        };
        Ok(id)
    }

    pub fn get(&mut self, cid: &Cid) -> Result<u64> {
        Ok(self._get(cid)?.ok_or(InvalidTransaction::BlockNotFound)?)
    }

    pub fn insert(&mut self, cid: &Cid) -> Result<u64> {
        let id = if let Some(id) = self._get(cid)? {
            id
        } else {
            let id = self.next_id;
            self.next_id += 1;
            self.lookup.insert(cid.clone(), id);
            id
        };
        Ok(id)
    }

    pub fn pin(&mut self, cid: &Cid) -> Result<()> {
        let id = self.get(cid)?;
        let pins = if let Some(pins) = self.pins.get(&id) {
            *pins
        } else {
            self.metadata.pin(id)?.unwrap_or_default()
        };
        self.pins.insert(id, pins + 1);
        Ok(())
    }

    pub fn unpin(&mut self, cid: &Cid) -> Result<()> {
        let id = self.get(cid)?;
        let pins = if let Some(pins) = self.pins.get(&id) {
            *pins
        } else {
            self.metadata
                .pin(id)?
                .ok_or(InvalidTransaction::BlockNotFound)?
        };
        if pins < 1 {
            return Err(InvalidTransaction::UnpinUnderflow(id).into());
        }
        self.pins.insert(id, pins - 1);
        Ok(())
    }

    pub fn pins<'a>(&'a self) -> impl Iterator<Item = (u64, u64)> + 'a {
        self.pins.iter().map(|(k, v)| (*k, *v))
    }

    pub fn remove_reference(&mut self, edge: (u64, u64)) {
        self.rrefs.insert(edge);
    }

    pub fn is_live(&self, id: u64) -> Result<bool> {
        if let Some(pin) = self.pins.get(&id) {
            if *pin != 0 {
                return Ok(true);
            }
        } else if self.metadata.pin(id)?.unwrap_or_default() != 0 {
            return Ok(true);
        }
        log::trace!("not pinned: {}", id);

        for id2 in self.metadata.referrers(id) {
            let id2 = id2?;
            if !self.rrefs.contains(&(id2, id)) {
                log::trace!("{} referenced by {}", id, id2);
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn begin(self) -> u64 {
        self.next_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libipld::cid::RAW;
    use libipld::multihash::{Multihash, MultihashDigest, SHA2_256};
    use tempdir::TempDir;

    fn setup() -> (TempDir, Metadata) {
        let tmp = TempDir::new("state").unwrap();
        let db = sled::open(tmp.path()).unwrap();
        let tree = db.open_tree("metadata").unwrap();
        let metadata = Metadata::new(tree);
        (tmp, metadata)
    }

    fn cid(id: u64) -> Cid {
        let mh = Multihash::new(SHA2_256, &id.to_be_bytes()[..]).unwrap();
        Cid::new_v1(RAW, mh.to_raw().unwrap())
    }

    #[test]
    fn test_state() {
        let (_tmp, metadata) = setup();

        let cid = [cid(0), cid(1)];

        metadata.insert(&cid[0], 0).unwrap();
        metadata.set_pin(0, 3).unwrap();

        let mut state = StateBuilder::new(metadata.clone(), 1);
        assert_eq!(state.get(&cid[0]).unwrap(), 0);
        assert_eq!(state.insert(&cid[0]).unwrap(), 0);
        assert_eq!(state.insert(&cid[1]).unwrap(), 1);
        assert_eq!(state.get(&cid[1]).unwrap(), 1);

        state.pin(&cid[0]).unwrap();
        state.pin(&cid[0]).unwrap();
        state.unpin(&cid[0]).unwrap();

        state.pin(&cid[1]).unwrap();
        state.pin(&cid[1]).unwrap();
        state.unpin(&cid[1]).unwrap();

        assert_eq!(state.pins.len(), 2);
        assert_eq!(state.pins.get(&0), Some(&4));
        assert_eq!(state.pins.get(&1), Some(&1));
        assert_eq!(state.begin(), 2);
    }
}
