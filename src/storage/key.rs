use core::convert::TryFrom;
use libipld::cid::Cid;
use sled::IVec;
use std::collections::HashSet;

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub enum Key {
    Block,
    Pin,
    Alias,
    Public,
    Want,
    Refs,
    Refer,
}

impl Key {
    pub fn prefix(self) -> IVec {
        (&[self as u8]).into()
    }

    fn byte_key(self, bytes: &[u8]) -> IVec {
        let mut key = Vec::with_capacity(bytes.len() + 1);
        key.push(self as u8);
        key.extend_from_slice(bytes);
        key.into()
    }

    fn cid_key(self, cid: &Cid) -> IVec {
        self.byte_key(&cid.to_bytes())
    }

    pub fn block(cid: &Cid) -> IVec {
        Self::Block.cid_key(cid)
    }

    pub fn pin(cid: &Cid) -> IVec {
        Self::Pin.cid_key(cid)
    }

    pub fn alias(bytes: &[u8]) -> IVec {
        Self::Alias.byte_key(bytes)
    }

    pub fn public(cid: &Cid) -> IVec {
        Self::Public.cid_key(cid)
    }

    pub fn want(cid: &Cid) -> IVec {
        Self::Want.cid_key(cid)
    }

    pub fn refs(cid: &Cid) -> IVec {
        Self::Refs.cid_key(cid)
    }

    pub fn refer(cid: &Cid) -> IVec {
        Self::Refer.cid_key(cid)
    }
}

#[derive(Clone, Debug)]
pub struct Value(IVec);

impl From<Value> for IVec {
    fn from(value: Value) -> Self {
        value.0
    }
}

impl From<IVec> for Value {
    fn from(ivec: IVec) -> Self {
        Self(ivec)
    }
}

impl From<bool> for Value {
    fn from(flag: bool) -> Self {
        if flag {
            Self(IVec::from(&[]))
        } else {
            panic!("remove the key instead");
        }
    }
}

impl From<Value> for bool {
    fn from(_: Value) -> bool {
        true
    }
}

impl From<u32> for Value {
    fn from(n: u32) -> Self {
        let bytes = n.to_le_bytes();
        Self(IVec::from(&bytes[..]))
    }
}

impl From<Value> for u32 {
    fn from(value: Value) -> Self {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&value.0);
        u32::from_le_bytes(buf)
    }
}

impl From<Cid> for Value {
    fn from(cid: Cid) -> Self {
        Self(cid.to_bytes().into())
    }
}

impl From<Value> for Cid {
    fn from(value: Value) -> Self {
        Cid::try_from(value.0.as_ref()).expect("valid cid")
    }
}

impl<'a> From<&'a HashSet<Cid>> for Value {
    fn from(refs: &'a HashSet<Cid>) -> Self {
        let mut buf = vec![];
        buf.extend(&(refs.len() as u64).to_le_bytes());
        for cid in refs.iter() {
            let bytes = cid.to_bytes();
            buf.extend(&[bytes.len() as u8]);
            buf.extend(bytes);
        }
        Self(buf.into())
    }
}

impl From<Value> for HashSet<Cid> {
    fn from(value: Value) -> Self {
        let buf = value.0.as_ref();
        let mut len_buf = [0u8; 8];
        len_buf.copy_from_slice(&buf[..8]);
        let len = u64::from_le_bytes(len_buf);
        let mut refs = HashSet::with_capacity(len as _);
        let mut pos = 8;
        for _ in 0..len {
            let len = buf[pos] as usize;
            pos += 1;
            let cid = Cid::try_from(&buf[pos..(pos + len)]).expect("valid refs");
            pos += len;
            refs.insert(cid);
        }
        refs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libipld::cid::Codec;
    use libipld::multihash::Sha2_256;

    fn create_block(bytes: &[u8]) -> (Cid, IVec) {
        let hash = Sha2_256::digest(&bytes);
        let cid = Cid::new_v1(Codec::Raw, hash);
        (cid, bytes.into())
    }

    #[test]
    fn test_key_prefix() {
        assert_eq!(&Key::Block.prefix(), &[0]);
        assert_eq!(&Key::Want.prefix(), &[4]);
    }

    #[test]
    fn test_refs() {
        let (cid1, _) = create_block(b"a");
        let (cid2, _) = create_block(b"b");
        let (cid3, _) = create_block(b"c");
        let mut refs = HashSet::new();
        refs.insert(cid1);
        refs.insert(cid2);
        refs.insert(cid3);
        let value = Value::from(&refs);
        let refs2 = HashSet::from(value);
        assert_eq!(refs, refs2);
    }
}
