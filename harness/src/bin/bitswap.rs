#[cfg(target_os = "linux")]
fn main() -> anyhow::Result<()> {
    use ipfs_embed_cli::{Command, Event};
    use libipld::alias;
    use std::time::Instant;

    ipfs_embed_harness::run_netsim(|mut network, opts| async move {
        let providers = 0..opts.n_providers;
        let consumers = opts.n_providers..(opts.n_providers + opts.n_consumers);

        let mut peers = Vec::with_capacity(network.machines().len());
        for machine in network.machines_mut() {
            machine
                .send(Command::ListenOn("/ip4/0.0.0.0/tcp/0".parse().unwrap()))
                .await;
            if let Some(Event::ListeningOn(peer, addr)) = machine.recv().await {
                peers.push((peer, addr));
            } else {
                unreachable!();
            }
        }

        for machine in network.machines_mut() {
            for (peer, addr) in &peers {
                machine.send(Command::AddAddress(*peer, addr.clone())).await;
            }
        }

        // create some blocks in each node that will not participate in the sync
        if opts.n_spam > 0 {
            println!("creating spam data");
        }
        for i in 0..opts.n_spam {
            let alias = format!("passive-{}", i);
            let (cid, blocks) =
                ipfs_embed_harness::build_tree(opts.tree_width, opts.tree_depth).unwrap();
            for machine in network.machines_mut() {
                machine.send(Command::Alias(alias.clone(), Some(cid))).await;
                for block in blocks.iter().rev() {
                    machine.send(Command::Insert(block.clone())).await;
                }
            }
        }

        // create the blocks to be synced in n_providers nodes
        println!("creating test data");
        let root = alias!(root);
        let (cid, blocks) =
            ipfs_embed_harness::build_tree(opts.tree_width, opts.tree_depth).unwrap();
        for machine in &mut network.machines_mut()[providers] {
            machine
                .send(Command::Alias(root.to_string(), Some(cid)))
                .await;
            for block in blocks.iter().rev() {
                machine.send(Command::Insert(block.clone())).await;
            }
        }

        // flush test and spam data
        for machine in network.machines_mut() {
            machine.send(Command::Flush).await;
        }
        for machine in network.machines_mut() {
            assert_eq!(machine.recv().await, Some(Event::Flushed));
        }

        // compute total size of data to be synced
        let size: usize = blocks.iter().map(|block| block.data().len()).sum();
        println!("test data built {} blocks, {} bytes", blocks.len(), size);

        let t0 = Instant::now();

        for machine in &mut network.machines_mut()[consumers.clone()] {
            machine
                .send(Command::Alias(root.to_string(), Some(cid)))
                .await;
            machine.send(Command::Sync(cid)).await;
        }

        for machine in &mut network.machines_mut()[consumers.clone()] {
            assert_eq!(machine.recv().await, Some(Event::Synced));
            machine.send(Command::Flush).await;
            assert_eq!(machine.recv().await, Some(Event::Flushed));
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
                machine.send(Command::Get(*block.cid())).await;
                let data = if let Some(Event::Block(data)) = machine.recv().await {
                    data
                } else {
                    unreachable!();
                };
                assert_eq!(&data, block);
            }
        }

        network
    })
}

#[cfg(not(target_os = "linux"))]
fn main() {}
