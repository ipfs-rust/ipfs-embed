use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use libp2p::{
    core::{
        multiaddr::{Multiaddr, Protocol},
        muxing::StreamMuxerBox,
        transport::{ListenerEvent, Transport, TransportError},
    },
    PeerId,
};

#[derive(Clone)]
pub struct P2pWrapper<T: Transport>(pub T);

impl<T: Transport<Output = (PeerId, StreamMuxerBox)>> Transport for P2pWrapper<T>
where
    T::Listener: Send + 'static,
    T::ListenerUpgrade: Send + 'static,
    T::Error: Send + 'static,
{
    type Output = T::Output;
    type Error = T::Error;
    #[allow(clippy::type_complexity)]
    type Listener =
        BoxStream<'static, Result<ListenerEvent<Self::ListenerUpgrade, Self::Error>, Self::Error>>;
    type ListenerUpgrade = futures::future::Ready<Result<T::Output, T::Error>>;
    type Dial = T::Dial;

    fn listen_on(self, addr: Multiaddr) -> Result<Self::Listener, TransportError<Self::Error>> {
        Ok(self
            .0
            .listen_on(addr)?
            .and_then(|event| async move {
                Ok(match event {
                    ListenerEvent::Upgrade {
                        local_addr,
                        mut remote_addr,
                        upgrade,
                    } => {
                        let upgrade = match upgrade.await {
                            Ok((peer, muxer)) => {
                                remote_addr.push(Protocol::P2p(peer.into()));
                                futures::future::ok((peer, muxer))
                            }
                            Err(err) => futures::future::err(err),
                        };
                        ListenerEvent::Upgrade {
                            local_addr,
                            remote_addr,
                            upgrade,
                        }
                    }
                    ListenerEvent::NewAddress(addr) => ListenerEvent::NewAddress(addr),
                    ListenerEvent::AddressExpired(addr) => ListenerEvent::AddressExpired(addr),
                    ListenerEvent::Error(err) => ListenerEvent::Error(err),
                })
            })
            .boxed())
    }

    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>> {
        self.0.dial(addr)
    }

    fn address_translation(&self, listen: &Multiaddr, observed: &Multiaddr) -> Option<Multiaddr> {
        self.0.address_translation(listen, observed)
    }
}
