use ipfs_embed_core::{Cid, Multiaddr, Network, NetworkEvent, PeerId, StoreParams, Stream};
use sc_network::{BitswapEvent, DhtEvent, Event, ExHashT, Key, NetworkService, NetworkStateInfo};
use sp_runtime::traits::Block;
use std::convert::TryFrom;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct SubstrateNetwork<B: Block + 'static, H: ExHashT, S: StoreParams + 'static> {
    _marker: PhantomData<S>,
    net: Arc<NetworkService<B, H, S::Hashes>>,
}

impl<B: Block + 'static, H: ExHashT, S: StoreParams + 'static> SubstrateNetwork<B, H, S> {
    pub fn new(net: Arc<NetworkService<B, H, S::Hashes>>) -> Self {
        Self {
            _marker: PhantomData,
            net,
        }
    }
}

impl<B: Block + 'static, H: ExHashT, S: StoreParams + Unpin + 'static> Network<S>
    for SubstrateNetwork<B, H, S>
{
    type Subscription = Subscription;

    fn local_peer_id(&self) -> &PeerId {
        self.net.local_peer_id()
    }

    fn external_addresses(&self) -> Vec<Multiaddr> {
        self.net.external_addresses()
    }

    fn provide(&self, cid: &Cid) {
        let key = Key::new(&cid.to_bytes());
        self.net.provide(key);
    }

    fn unprovide(&self, cid: &Cid) {
        let key = Key::new(&cid.to_bytes());
        self.net.unprovide(key);
    }

    fn providers(&self, cid: &Cid) {
        let key = Key::new(&cid.to_bytes());
        self.net.providers(key);
    }

    fn connect(&self, _peer_id: PeerId) {
        // TODO
    }

    fn want(&self, cid: Cid, priority: i32) {
        self.net.bitswap_want_block(cid, priority)
    }

    fn cancel(&self, cid: Cid) {
        self.net.bitswap_cancel_block(cid)
    }

    fn send_to(&self, peer_id: PeerId, cid: Cid, data: Vec<u8>) {
        self.net
            .bitswap_send_block(peer_id, cid, data.into_boxed_slice())
    }

    fn send(&self, cid: Cid, data: Vec<u8>) {
        self.net
            .bitswap_send_block_all(cid, data.into_boxed_slice())
    }

    fn subscribe(&self) -> Self::Subscription {
        Subscription {
            events: Box::new(self.net.event_stream("ipfs-embed")),
        }
    }
}

pub struct Subscription {
    events: Box<dyn Stream<Item = Event> + Send + Unpin>,
}

impl Stream for Subscription {
    type Item = NetworkEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            let ev = match Pin::new(&mut self.events).poll_next(cx) {
                Poll::Ready(Some(ev)) => ev,
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
            };
            let ev = match ev {
                Event::Dht(DhtEvent::Providers(key, providers)) => {
                    if let Ok(cid) = Cid::try_from(key.as_ref()) {
                        NetworkEvent::Providers(cid, providers)
                    } else {
                        continue;
                    }
                }
                Event::Dht(DhtEvent::GetProvidersFailed(key)) => {
                    if let Ok(cid) = Cid::try_from(key.as_ref()) {
                        NetworkEvent::GetProvidersFailed(cid)
                    } else {
                        continue;
                    }
                }
                Event::Dht(DhtEvent::Providing(key)) => {
                    if let Ok(cid) = Cid::try_from(key.as_ref()) {
                        NetworkEvent::Providing(cid)
                    } else {
                        continue;
                    }
                }
                Event::Dht(DhtEvent::StartProvidingFailed(key)) => {
                    if let Ok(cid) = Cid::try_from(key.as_ref()) {
                        NetworkEvent::StartProvidingFailed(cid)
                    } else {
                        continue;
                    }
                }
                Event::Bitswap(BitswapEvent::ReceivedBlock(peer_id, cid, data)) => {
                    NetworkEvent::ReceivedBlock(peer_id, cid, data.to_vec())
                }
                Event::Bitswap(BitswapEvent::ReceivedWant(peer_id, cid, priority)) => {
                    NetworkEvent::ReceivedWant(peer_id, cid, priority)
                }
                _ => continue,
            };
            return Poll::Ready(Some(ev));
        }
    }
}
