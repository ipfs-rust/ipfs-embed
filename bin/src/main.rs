use clap::Clap;
use ipfs_embed_db::StorageService;
use libipld::block::Block;
use libipld::cid::Cid;
use libipld::codec::Codec;
use libipld::json::DagJsonCodec;
use libipld::store::DefaultStoreParams;
use std::path::PathBuf;

#[derive(Clone, Debug, Clap)]
pub struct Opts {
    #[clap(subcommand)]
    pub cmd: SubCommand,
    #[clap(short = "p", long = "path")]
    pub path: PathBuf,
    #[clap(short = "t", long = "tree")]
    pub tree: Option<String>,
}

#[derive(Clone, Debug, Clap)]
pub enum SubCommand {
    Ls,
    Cat(CidCommand),
    References(IdCommand),
    Referrers(IdCommand),
}

#[derive(Clone, Debug, Clap)]
pub struct CidCommand {
    cid: Cid,
}

#[derive(Clone, Debug, Clap)]
pub struct IdCommand {
    id: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opts = Opts::parse();
    let store = StorageService::<DefaultStoreParams>::open(&opts.path)?;
    match opts.cmd {
        SubCommand::Ls => {
            for (id, cid, pin) in store.blocks()? {
                println!("{} {} {}", id, cid.to_string(), pin);
            }
        }
        SubCommand::Cat(CidCommand { cid }) => {
            if let Some(data) = store.get(&cid)? {
                let block = Block::<DefaultStoreParams>::new_unchecked(cid, data);
                let json = DagJsonCodec.encode(&block.ipld()?)?;
                println!("{}", std::str::from_utf8(&json)?);
            }
        }
        SubCommand::References(IdCommand { id }) => {
            for (id, cid) in store.references(id)? {
                println!("{} {}", id, cid.to_string());
            }
        }
        SubCommand::Referrers(IdCommand { id }) => {
            for (id, cid) in store.referrers(id)? {
                println!("{} {}", id, cid.to_string());
            }
        }
    }
    Ok(())
}
