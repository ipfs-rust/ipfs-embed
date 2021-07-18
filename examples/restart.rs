use anyhow::Result;
use ipfs_embed::{Config, DefaultParams, Ipfs};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Duration;

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    println!("executor threads: {}", threads()?);
    println!("{}", comms()?.join(""));

    let config = Config::default();
    let ipfs = Ipfs::<DefaultParams>::new(config).await?;
    async_std::task::sleep(Duration::from_millis(10)).await;
    println!("ipfs-embed threads: {}", threads()?);
    println!("{}", comms()?.join(""));

    drop(ipfs);
    async_std::task::sleep(Duration::from_millis(10)).await;
    println!("after ipfs-embed drop threads: {}", threads()?);
    println!("{}", comms()?.join(""));

    let config = Config::default();
    let ipfs = Ipfs::<DefaultParams>::new(config).await?;
    async_std::task::sleep(Duration::from_millis(10)).await;
    println!("threads after 2nd ipfs: {}", threads()?);
    println!("{}", comms()?.join(""));

    drop(ipfs);
    println!("after 2nd ipfs-embed drop threads: {}", threads()?);
    async_std::task::sleep(Duration::from_millis(10)).await;
    println!("{}", comms()?.join(""));

    let config = Config::default();
    let ipfs = Ipfs::<DefaultParams>::new(config).await?;
    async_std::task::sleep(Duration::from_millis(10)).await;
    println!("threads after 3nd ipfs: {}", threads()?);
    println!("{}", comms()?.join(""));

    drop(ipfs);
    async_std::task::sleep(Duration::from_millis(10)).await;
    println!("after 3nd ipfs-embed drop threads: {}", threads()?);
    println!("{}", comms()?.join(""));

    for _ in 0..128 {
        let config = Config::default();
        let ipfs = Ipfs::<DefaultParams>::new(config).await?;
        drop(ipfs);
    }

    async_std::task::sleep(Duration::from_secs(1)).await;
    println!("threads: {}", threads()?);
    println!("{}", comms()?.join(""));

    Ok(())
}

fn threads() -> Result<u64> {
    let f = File::open("/proc/self/status")?;
    let reader = BufReader::new(f);
    for line in reader.lines() {
        let line = line?;
        if !line.starts_with("Threads:") {
            continue;
        }
        let mut iter = line.split_whitespace();
        iter.next().unwrap();
        let n = iter.next().unwrap();
        return Ok(n.parse().unwrap());
    }
    unimplemented!();
}

fn comms() -> Result<Vec<String>> {
    let mut tasks = vec![];
    let dir = std::fs::read_dir("/proc/self/task")?;
    for entry in dir {
        let entry = entry?;
        let comm = entry.path().join("comm");
        if comm.exists() {
            tasks.push(std::fs::read_to_string(comm)?);
        }
    }
    Ok(tasks)
}
