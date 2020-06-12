use clap::Clap;
use libipld::cid::Cid;
use std::path::PathBuf;

#[derive(Clone, Debug, Clap)]
pub struct Opts {
    #[clap(subcommand)]
    pub cmd: SubCommand,
    #[clap(short = "p", long = "path")]
    pub path: PathBuf,
}

#[derive(Clone, Debug, Clap)]
pub enum SubCommand {
    Ls(LsCommand),
    Cat(CatCommand),
    Unpin(UnpinCommand),
}

#[derive(Clone, Debug, Clap)]
pub struct LsCommand {
    #[clap(long = "pinned", conflicts_with_all(&["live", "dead", "all"]))]
    pub pinned: bool,
    #[clap(long = "live", conflicts_with_all(&["pinned", "dead", "all"]))]
    pub live: bool,
    #[clap(long = "dead", conflicts_with_all(&["pinned", "live", "all"]))]
    pub dead: bool,
    #[clap(long = "all", conflicts_with_all(&["pinned", "live", "dead"]))]
    pub all: bool,
}

#[derive(Clone, Debug, Clap)]
pub struct CatCommand {
    pub cid: Cid,
}

#[derive(Clone, Debug, Clap)]
pub struct UnpinCommand {
    pub cid: Cid,
}
