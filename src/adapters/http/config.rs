use std::net::SocketAddr;

/// Default limit for a request body: 1 MiB.
const DEFAULT_MAX_BODY_BYTES: usize = 1024 * 1024;

/// When the server answers a request that became a delivery.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ResponseTiming {
    /// Answer when the delivery is acknowledged or dropped, so a client can
    /// retry every request that was not processed.
    #[default]
    Ack,
    /// Answer `202 Accepted` as soon as the subscription receives the request.
    /// A request is lost if the process stops before its delivery completes,
    /// and the client does not learn about processing failures.
    Receive,
}

#[derive(Clone, Debug)]
pub struct HttpSourceConfig {
    /// Address the server listens on. Port 0 selects a free port, which
    /// [`HttpSource::local_addr`](super::HttpSource::local_addr) reports.
    pub bind: SocketAddr,
    /// Larger request bodies are answered with `413 Payload Too Large` without
    /// becoming deliveries.
    pub max_body_bytes: usize,
    pub response: ResponseTiming,
}

impl HttpSourceConfig {
    pub fn new(bind: SocketAddr) -> Self {
        Self {
            bind,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            response: ResponseTiming::default(),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.max_body_bytes > 0,
            "HTTP maximum body size must be greater than zero"
        );
        Ok(())
    }
}
