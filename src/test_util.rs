use anyhow::Result;
use libipld::cbor::DagCborCodec;
use libipld::multihash::Code;
use libipld::{Block, Cid, DagCbor, DefaultParams};
use rand::RngCore;

#[derive(Debug, DagCbor)]
pub struct Node {
    pub links: Vec<Cid>,
    pub depth: u64,
    pub payload: Box<[u8]>,
}

fn create_block(links: Vec<Cid>, depth: u64) -> Result<Block<DefaultParams>> {
    let payload = if links.is_empty() {
        let mut payload = [0u8; 1024 * 16];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut payload);
        payload.to_vec().into_boxed_slice()
    } else {
        let mut payload = [0u8; 512];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut payload);
        payload.to_vec().into_boxed_slice()
    };
    let node = Node {
        links,
        depth,
        payload,
    };
    Block::encode(DagCborCodec, Code::Blake3_256, &node)
}

fn build_tree_0(width: u64, depth: u64, blocks: &mut Vec<Block<DefaultParams>>) -> Result<Cid> {
    let links = if depth == 0 {
        vec![]
    } else {
        let mut links = Vec::with_capacity(width as usize);
        for _ in 0..width {
            let cid = build_tree_0(width, depth - 1, blocks)?;
            links.push(cid);
        }
        links
    };
    let block = create_block(links, depth)?;
    let cid = *block.cid();
    blocks.push(block);
    Ok(cid)
}

pub fn build_tree(width: u64, depth: u64) -> Result<(Cid, Vec<Block<DefaultParams>>)> {
    let mut blocks = vec![];
    let cid = build_tree_0(width, depth, &mut blocks)?;
    Ok((cid, blocks))
}
