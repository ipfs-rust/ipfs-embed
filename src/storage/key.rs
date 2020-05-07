use libipld::cid::Cid;
use sled::IVec;

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub enum Key {
    Block,
    Pin,
    Alias,
    Public,
    Want,
}

impl Key {
    pub fn prefix(&self) -> IVec {
        (&[*self as u8]).into()
    }

    fn byte_key(&self, bytes: &[u8]) -> IVec {
        let mut key = Vec::with_capacity(bytes.len() + 1);
        key.push(*self as u8);
        key.extend_from_slice(bytes);
        key.into()
    }

    fn cid_key(&self, cid: &Cid) -> IVec {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_prefix() {
        assert_eq!(&Key::Block.prefix(), &[0]);
        assert_eq!(&Key::Want.prefix(), &[4]);
    }
}
