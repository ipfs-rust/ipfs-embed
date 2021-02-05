use ipfs_embed::{Config, Ipfs};
use libipld::cbor::DagCborCodec;
use libipld::multihash::Code;
use libipld::{alias, Cid, DagCbor, DefaultParams, Result};
use std::convert::TryFrom;
use std::path::Path;

const ROOT: &str = alias!(root);

#[derive(Debug, Default, DagCbor)]
pub struct Block {
    prev: Option<Cid>,
    id: u32,
    loopback: Option<Cid>,
    payload: Vec<u8>,
}

fn loopback(block: u32) -> Option<u32> {
    let x = block.trailing_zeros();
    if x > 1 && block > 0 {
        Some(block - (1 << (x - 1)))
    } else {
        None
    }
}

/*pub struct ChainSyncer<S: StoreParams, T: Storage<S>> {
    index: sled::Db,
    storage: BitswapStorage<S, T>,
}

impl<S: StoreParams, T: Storage<S>> ChainSyncer<S, T> {
    pub fn new(index: sled::Db, storage: BitswapStorage<S, T>) -> Arc<Self> {
        Arc::new(Self { index, storage })
    }

    fn lookup_cid(&self, id: u32) -> Result<Option<Cid>> {
        if let Some(cid) = self.index.get(id.to_be_bytes())? {
            Ok(Some(Cid::try_from(&cid[..]).unwrap()))
        } else {
            Ok(None)
        }
    }

    fn loopback_cid(&self, id: u32) -> Result<Option<Cid>> {
        if let Some(id) = loopback(id) {
            Ok(self.lookup_cid(id)?)
        } else {
            Ok(None)
        }
    }
}

impl<S: StoreParams, T: Storage<S>> BitswapSync for ChainSyncer<S, T>
where
    S::Codecs: Into<DagCborCodec>,
{
    fn references(&self, cid: &Cid) -> Box<dyn Iterator<Item = Cid>> {
        if let Some(data) = self.storage.get(cid) {
            let ipld_block = libipld::Block::<S>::new_unchecked(*cid, data);
            if let Ok(block) = ipld_block.decode::<DagCborCodec, Block>() {
                let loopback = self.loopback_cid(block.id);
                let block_loopback = block.loopback;
                let valid_loopback = match (loopback, block_loopback) {
                    (Ok(Some(cid1)), Some(cid2)) => cid1 == cid2,
                    (Ok(None), _) => true,
                    (Ok(Some(_)), None) => false,
                    (Err(err), _) => {
                        log::error!("{:?}", err);
                        true
                    }
                };
                if valid_loopback {
                    match (block.prev, block.loopback) {
                        (Some(cid1), Some(cid2)) => {
                            let cid1 = std::iter::once(cid1);
                            let cid2 = std::iter::once(cid2);
                            return Box::new(cid1.chain(cid2));
                        }
                        (Some(cid), None) => return Box::new(std::iter::once(cid)),
                        _ => {}
                    }
                } else {
                    log::error!("rejecting block due to invalid loopback");
                }
            }
        }
        Box::new(std::iter::empty())
    }

    fn contains(&self, cid: &Cid) -> bool {
        self.storage.contains(cid)
    }
}*/

pub struct BlockChain {
    index: sled::Db,
    ipfs: Ipfs<DefaultParams>,
    root_cid: Option<Cid>,
    root_id: u32,
}

impl BlockChain {
    pub async fn open<P: AsRef<Path>>(path: P, cache_size: u64) -> Result<Self> {
        let index = sled::open(path.as_ref().join("index"))?;
        let config = Config::new(Some(path.as_ref().join("blocks")), cache_size);
        //let config = Config::new(None, cache_size);
        let ipfs = Ipfs::new(config).await?;
        ipfs.listen_on("/ip4/127.0.0.1/tcp/0".parse()?).await?;
        let root_cid = ipfs.resolve(ROOT)?;
        let mut chain = Self {
            index,
            ipfs,
            root_cid,
            root_id: 0,
        };
        if root_cid.is_none() {
            // insert the genesis block
            chain.push(vec![], false)?;
        }
        chain.root_id = chain.get_by_cid(chain.root_cid.as_ref().unwrap())?.id;
        Ok(chain)
    }

    pub fn lookup_cid(&self, id: u32) -> Result<Option<Cid>> {
        if let Some(cid) = self.index.get(id.to_be_bytes())? {
            Ok(Some(Cid::try_from(&cid[..]).unwrap()))
        } else {
            Ok(None)
        }
    }

    pub fn get_by_cid(&self, cid: &Cid) -> Result<Block> {
        let block = self.ipfs.get(cid)?;
        let block = block.decode::<DagCborCodec, Block>()?;
        Ok(block)
    }

    pub fn get_by_id(&self, id: u32) -> Result<Option<Block>> {
        if let Some(cid) = self.lookup_cid(id)? {
            Ok(Some(self.get_by_cid(&cid)?))
        } else {
            Ok(None)
        }
    }

    pub fn loopback_cid(&self, id: u32) -> Result<Option<Cid>> {
        if let Some(id) = loopback(id) {
            Ok(Some(self.lookup_cid(id)?.unwrap()))
        } else {
            Ok(None)
        }
    }

    fn index_block(&self, id: u32, cid: &Cid) -> Result<()> {
        self.index.insert(id.to_be_bytes(), cid.to_bytes())?;
        Ok(())
    }

    pub fn push(&mut self, payload: Vec<u8>, import: bool) -> Result<Cid> {
        if import {
            tracing::trace!("chain: import");
        } else {
            tracing::trace!("chain: push");
        }
        let id = if self.root_cid.is_none() {
            0
        } else {
            self.root_id + 1
        };
        let loopback = self.loopback_cid(id)?;
        //let loopback = None;
        let block = Block {
            prev: self.root_cid,
            id,
            loopback,
            payload,
        };
        let ipld_block = libipld::Block::encode(DagCborCodec, Code::Blake3_256, &block)?;
        let cid = *ipld_block.cid();
        let _ = self.ipfs.insert(&ipld_block)?;
        self.index_block(id, &cid)?;
        if !import {
            self.ipfs.alias(ROOT, Some(&cid))?;
        }
        self.root_id = id;
        self.root_cid = Some(cid);
        Ok(cid)
    }

    pub async fn sync(&mut self, root: Cid) -> Result<()> {
        tracing::trace!("chain: sync");
        //let syncer = ChainSyncer::new(self.index.clone(), self.ipfs.bitswap_storage());
        let tmp = self.ipfs.create_temp_pin()?;
        self.ipfs.temp_pin(&tmp, &root)?;
        self.ipfs.sync(&root).await?;

        let mut cid = root;
        let mut block = self.get_by_cid(&root)?;
        let prev_root_id = self.root_id;
        let new_root_id = block.id;

        for _ in prev_root_id..new_root_id {
            self.index_block(block.id, &cid)?;
            cid = block.prev.unwrap();
            block = self.get_by_cid(&cid)?;
        }

        self.ipfs.alias(ROOT, Some(&root))?;
        self.ipfs.flush().await?;
        self.root_id = block.id;
        self.root_cid = Some(cid);

        Ok(())
    }

    pub fn root(&self) -> &Cid {
        self.root_cid.as_ref().unwrap()
    }
}

#[async_std::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let mut local1 = BlockChain::open("/tmp/local1", 1000).await?;
    let mut local2 = BlockChain::open("/tmp/local2", 1000).await?;
    ipfs_embed::telemetry("127.0.0.1:8080".parse()?, &local1.ipfs)?;

    for i in 0..10 {
        local1.push(vec![i + 1 as u8], true)?;
    }

    let root = *local1.root();
    local1.sync(root).await?;

    println!("starting sync");
    let start = std::time::Instant::now();
    local2.sync(root).await?;
    let end = std::time::Instant::now();
    println!("time to sync {}ms", end.duration_since(start).as_millis());

    /*let root = local1.push(b"hello world".to_vec()).await?;
    local2.sync(&root).await?;
    let block = local2.get_by_id(1).await?;
    assert_eq!(block.unwrap().payload, b"hello world".to_vec());

    let root = local2.push(b"another block".to_vec()).await?;
    local1.sync(&root).await?;
    let block = local1.get_by_id(1).await?;
    assert_eq!(block.unwrap().payload, b"hello world".to_vec());
    let block = local1.get_by_id(2).await?;
    assert_eq!(block.unwrap().payload, b"another block".to_vec());*/
    Ok(())
}
