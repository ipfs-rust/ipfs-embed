use assert_cmd::prelude::OutputAssertExt;
use escargot::CargoBuild;
use predicates::{
    boolean::PredicateBooleanExt,
    str::{contains, is_empty},
};
use std::io::Write;

fn run(bin: &str, args: impl IntoIterator<Item = &'static str>) -> anyhow::Result<()> {
    let cmd = CargoBuild::new()
        .manifest_path(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .bin(bin)
        .run()?
        .command()
        .args(args)
        .env(
            "RUST_LOG",
            "netsim_embed_machine=debug,ipfs_embed=trace,multi=debug,info",
        )
        .assert();
    let out = cmd.get_output().stdout.clone();
    let err = cmd.get_output().stderr.clone();
    (|| {
        cmd.try_stderr(is_empty())?
            .try_stdout(contains("ERROR").not())?
            .try_success()
    })()
    .map(|_| ())
    .map_err(|e| {
        eprintln!("--- stdout");
        std::io::stderr().write_all(out.as_slice()).ok();
        eprintln!("--- stderr");
        std::io::stderr().write_all(err.as_slice()).ok();
        eprintln!("---");
        e.into()
    })
}

#[cfg(target_os = "linux")]
#[test]
fn netsim_bitswap() -> anyhow::Result<()> {
    run(
        "bitswap",
        ["--enable-mdns", "--tree-depth=2", "--tree-width=10"],
    )
}

#[cfg(target_os = "linux")]
#[test]
fn netsim_bitswap_no_reuse() -> anyhow::Result<()> {
    run(
        "bitswap",
        [
            "--enable-mdns",
            "--tree-depth=2",
            "--disable-port-reuse",
            "--tree-width=10",
        ],
    )
}

#[cfg(target_os = "linux")]
#[test]
fn netsim_discover_nat_forward() -> anyhow::Result<()> {
    run("discover_nat_forward", [])
}

#[cfg(target_os = "linux")]
#[test]
fn netsim_discover_nat_forward_no_reuse() -> anyhow::Result<()> {
    run("discover_nat_forward", ["--disable-port-reuse"])
}

#[cfg(target_os = "linux")]
#[test]
fn netsim_discover_nat() -> anyhow::Result<()> {
    run("discover_nat", [])
}

#[cfg(target_os = "linux")]
#[test]
fn netsim_discover_nat_no_reuse() -> anyhow::Result<()> {
    run("discover_nat", ["--disable-port-reuse"])
}

#[cfg(target_os = "linux")]
#[test]
fn netsim_discover_plain() -> anyhow::Result<()> {
    run("discover_plain", [])
}

#[cfg(target_os = "linux")]
#[test]
fn netsim_discover_plain_no_reuse() -> anyhow::Result<()> {
    run("discover_plain", ["--disable-port-reuse"])
}

#[cfg(target_os = "linux")]
#[test]
fn netsim_sim_open() -> anyhow::Result<()> {
    run("sim_open", [])
}
