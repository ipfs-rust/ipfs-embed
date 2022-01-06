use crate::Ipfs;
use anyhow::Result;
use libipld::{codec::References, store::StoreParams, Ipld};
use prometheus::Encoder;

/// Telemetry server
pub fn telemetry<P: StoreParams>(addr: std::net::SocketAddr, ipfs: &Ipfs<P>) -> Result<()>
where
    Ipld: References<P::Codecs>,
{
    let registry = prometheus::default_registry();
    ipfs.register_metrics(registry)?;
    let mut s = tide::new();
    s.at("/metrics").get(get_metric);
    async_global_executor::spawn(async move { s.listen(addr).await }).detach();
    Ok(())
}

/// Return metrics to prometheus
async fn get_metric(_: tide::Request<()>) -> tide::Result {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];

    encoder.encode(&metric_families, &mut buffer).unwrap();

    let response = tide::Response::builder(200)
        .content_type("text/plain; version=0.0.4")
        .body(tide::Body::from(buffer))
        .build();

    Ok(response)
}
