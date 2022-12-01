# Changelog

This changelog was begun after 0.22.

## Release 0.26

- update to libp2p 0.50
- mitigate issue with sim-open redials happening too soon after the error, leading to `EADDRNOTAVAIL`
- various CI-related fixes to get github actions green again

## Release 0.25.1

- use patch libp2p-yamux 0.41.1
- fix display of error messages

## Release 0.25

- offer `keep_alive` config option to keep all explicitly dialled or incoming connections open indefinitely

## Release 0.24

- update to libp2p 0.49
- restructure network behaviour to hold all state in the polling task
- send all external commands to that task, i.e. everything is async now

## Release 0.23

- update to libp2p 0.43
- make PortReuse configurable (and recommend to turn it off)
- update to ipfs-sqlite-block-store 0.10 and thereby rusqlite 0.26
- rewrite address book logic to validate addresses and retain only confirmed ones:
  - every successful outgoing connection counts as confirmation
  - every outgoing dial failure removes unconfirmed addresses (or confirmed, if PeerId changed)
  - all discovered addresses from MDNS and Kademlia are validated by dialling
  - all incoming connections’ remote addresses are translated to likely listen addresses using IdentifyInfo
- offer detailed peer information and error history in PeerInfo
- forward all connection-related swarm events
- allow DNS fallback configuration in case system config parsing fails
