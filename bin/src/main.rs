use crate::command::*;
use clap::Clap;
use exitfailure::ExitDisplay;
use ipfs_embed::{Cid, Config, Metadata, Store, WritableStore};
use libipld::block::decode_ipld;
use libipld::codec::Codec;
use libipld::json::DagJsonCodec;

mod command;

fn main() -> Result<(), ExitDisplay<Box<dyn std::error::Error>>> {
    Ok(run()?)
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opts = Opts::parse();
    let db = sled::open(opts.path)?;
    let tree_name = opts.tree.unwrap_or_else(|| ipfs_embed::TREE.to_string());
    let tree = db.open_tree(tree_name)?;
    let config = Config::new(tree, Default::default());
    let store = Store::new(config)?;
    match opts.cmd {
        SubCommand::Tree => {
            for name in db.tree_names() {
                println!("{}", std::str::from_utf8(&name).unwrap());
            }
        }
        SubCommand::Ls(LsCommand {
            pinned,
            live,
            dead,
            all,
        }) => {
            println!(
                "{:10} {:10} {:10} {:10} cid",
                "pins", "parents", "children", "public"
            );
            for res in store.blocks() {
                let cid = res?;
                let metadata = store.metadata(&cid)?;
                let is_pinned = metadata.pins > 0;
                let is_live = metadata.referers > 0 || metadata.pins > 0;
                let all = all || (!pinned && !live && !dead);
                let print = all || pinned && is_pinned || live && is_live || dead && !is_live;
                if print {
                    print_metadata(&cid, &metadata);
                }
            }
        }
        SubCommand::Cat(CatCommand { cid }) => {
            if let Some(bytes) = store.get_local(&cid)? {
                let ipld = decode_ipld(&cid, &bytes)?;
                let json = DagJsonCodec::encode(&ipld)?;
                println!("{}", std::str::from_utf8(&json)?);
            }
        }
        SubCommand::Refs(RefsCommand { cid }) => {
            let metadata = store.metadata(&cid)?;
            for cid in metadata.refs {
                println!("{}", cid.to_string());
            }
        }
        SubCommand::Unpin(UnpinCommand { cid }) => {
            async_std::task::block_on(store.unpin(&cid))?;
        }
    }
    Ok(())
}

fn print_metadata(cid: &Cid, metadata: &Metadata) {
    println!(
        "{:10} {:10} {:10} {:10} {}",
        metadata.pins.to_string(),
        metadata.referers.to_string(),
        metadata.refs.len().to_string(),
        metadata.public,
        cid.to_string()
    );
}
