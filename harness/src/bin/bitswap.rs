#[cfg(target_os = "linux")]
fn main() -> anyhow::Result<()> {
    use anyhow::Context;
    use harness::{MachineExt, MultiaddrExt};
    use ipfs_embed_cli::{Command, Event};
    use libipld::alias;
    use std::time::Instant;

    harness::build_bin()?;

    harness::run_netsim(|mut network, opts| async move {
        let providers = 0..opts.n_providers;
        let consumers = opts.n_providers..(opts.n_providers + opts.n_consumers);

        let mut peers = Vec::with_capacity(network.machines().len());
        for machine in network.machines_mut() {
            let peer = machine.peer_id();
            loop {
                if let Some(Event::NewListenAddr(addr)) = machine.recv().await {
                    if !addr.is_loopback() {
                        peers.push((peer, addr));
                        break;
                    }
                }
            }
        }

        for machine in network.machines_mut() {
            for (peer, addr) in &peers {
                machine.send(Command::AddAddress(*peer, addr.clone()));
            }
        }

        // create some blocks in each node that will not participate in the sync
        if opts.n_spam > 0 {
            println!("creating spam data");
        }
        for i in 0..opts.n_spam {
            let alias = format!("passive-{}", i);
            let (cid, blocks) = harness::build_tree(opts.tree_width, opts.tree_depth)?;
            for machine in network.machines_mut() {
                machine.send(Command::Alias(alias.clone(), Some(cid)));
                for block in blocks.iter().rev() {
                    machine.send(Command::Insert(block.clone()));
                }
            }
        }

        // create the blocks to be synced in n_providers nodes
        println!("creating test data");
        let root = alias!(root);
        let (cid, blocks) = harness::build_tree(opts.tree_width, opts.tree_depth)?;
        for machine in &mut network.machines_mut()[providers] {
            machine.send(Command::Alias(root.to_string(), Some(cid)));
            for block in blocks.iter().rev() {
                machine.send(Command::Insert(block.clone()));
            }
        }

        // flush test and spam data
        for machine in network.machines_mut() {
            machine.send(Command::Flush);
        }
        for machine in network.machines_mut() {
            loop {
                if let Some(Event::Flushed) = machine.recv().await {
                    break;
                }
            }
        }

        // compute total size of data to be synced
        let size: usize = blocks.iter().map(|block| block.data().len()).sum();
        println!("test data built {} blocks, {} bytes", blocks.len(), size);

        let t0 = Instant::now();

        for machine in &mut network.machines_mut()[consumers.clone()] {
            machine.send(Command::Alias(root.to_string(), Some(cid)));
            machine.send(Command::Sync(cid));
        }

        for machine in &mut network.machines_mut()[consumers.clone()] {
            loop {
                if let Some(Event::Synced) = machine.recv().await {
                    break;
                }
            }
            machine.send(Command::Flush);
            loop {
                if let Some(Event::Flushed) = machine.recv().await {
                    break;
                }
            }
        }

        println!(
            "tree sync complete in {} ms {} blocks {} bytes {} providers {} consumers",
            t0.elapsed().as_millis(),
            blocks.len(),
            size,
            opts.n_providers,
            opts.n_consumers,
        );

        for machine in &mut network.machines_mut()[consumers] {
            // check that data is indeed synced
            for block in &blocks {
                machine.send(Command::Get(*block.cid()));
                loop {
                    if let Some(Event::Block(data)) = machine.recv().await {
                        assert_eq!(&data, block);
                        break;
                    }
                }
            }
        }
        Ok(())
    })
    .context("netsim")
}

#[cfg(not(target_os = "linux"))]
fn main() {}
