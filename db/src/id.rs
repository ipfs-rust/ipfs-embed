use fnv::FnvHashMap;
use sled::IVec;

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Id(IVec);

impl From<IVec> for Id {
    fn from(id: IVec) -> Self {
        Self(id)
    }
}

impl From<Id> for IVec {
    fn from(id: Id) -> Self {
        id.0
    }
}

impl From<u64> for Id {
    fn from(id: u64) -> Self {
        Self(IVec::from(&id.to_be_bytes()[..]))
    }
}

impl<'a> From<&'a Id> for u64 {
    fn from(id: &'a Id) -> Self {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&id.0[..8]);
        u64::from_be_bytes(buf)
    }
}

impl<'a> From<&'a Id> for IVec {
    fn from(id: &'a Id) -> Self {
        id.0.clone()
    }
}

impl AsRef<[u8]> for Id {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let id: u64 = self.into();
        id.fmt(f)
    }
}

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let id: u64 = self.into();
        id.fmt(f)
    }
}

#[derive(Clone, Default, Eq, Hash, PartialEq)]
pub struct Ids(IVec);

impl Ids {
    pub fn concat(idss: &[&Self]) -> Self {
        let cap = idss.iter().map(|ids| ids.as_ref().len()).sum();
        let mut buf = Vec::with_capacity(cap);
        unsafe { buf.set_len(cap) };
        let mut start = 0;
        for ids in idss {
            let end = start + ids.as_ref().len();
            buf[start..end].copy_from_slice(ids.as_ref());
            start = end;
        }
        Self(IVec::from(buf))
    }

    pub fn iter(&self) -> IdsIter<'_> {
        IdsIter { ids: self, pos: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.0.len() / 8
    }

    pub fn from_iter<'a, I: Iterator<Item = &'a Id> + 'a>(iter: I, len: usize) -> Self {
        let mut buf = Vec::with_capacity(len * 8);
        for id in iter {
            buf.extend_from_slice(id.as_ref());
        }
        Self(IVec::from(buf))
    }
}

impl From<IVec> for Ids {
    fn from(ids: IVec) -> Self {
        Self(ids)
    }
}

impl<'a> From<&'a Ids> for IVec {
    fn from(ids: &'a Ids) -> Self {
        ids.0.clone()
    }
}

impl From<Ids> for IVec {
    fn from(ids: Ids) -> Self {
        ids.0
    }
}

impl AsRef<[u8]> for Ids {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

pub struct IdsIter<'a> {
    ids: &'a Ids,
    pos: usize,
}

impl<'a> Iterator for IdsIter<'a> {
    type Item = Id;

    fn next(&mut self) -> Option<Id> {
        let start = self.pos;
        let end = self.pos + 8;
        if end > self.ids.0.len() {
            return None;
        }
        let inner = IVec::from(&self.ids.0[start..end]);
        self.pos = end;
        Some(Id::from(inner))
    }
}

#[derive(Debug)]
pub struct LiveSet {
    filter: FnvHashMap<Id, u64>,
}

impl LiveSet {
    pub fn new() -> Self {
        Self {
            filter: Default::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.filter.len()
    }

    pub fn contains(&self, id: &Id) -> bool {
        self.filter.contains_key(id)
    }

    pub fn add(&mut self, id: &Id) {
        let count = self.filter.entry(id.clone()).or_default();
        *count += 1;
    }

    pub fn delete(&mut self, id: &Id) {
        if let Some((id, count)) = self.filter.remove_entry(id) {
            if count > 1 {
                self.filter.insert(id, count - 1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_live_set() {
        let mut live = LiveSet::new();
        let id = Id::from(0);
        live.add(&id);
        live.add(&id);
        live.delete(&id);
        assert_eq!(live.contains(&id), true);
        live.delete(&id);
        assert_eq!(live.contains(&id), false);
    }
}
