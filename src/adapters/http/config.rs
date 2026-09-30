use std::net::SocketAddr;

/// Default limit for a request body: 1 MiB.
const DEFAULT_MAX_BODY_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct HttpSourceConfig {
    /// Address the server listens on. Port 0 selects a free port, which
    /// [`HttpSource::local_addr`](super::HttpSource::local_addr) reports.
    pub bind: SocketAddr,
    /// Larger request bodies are answered with `413 Payload Too Large` without
    /// becoming deliveries.
    pub max_body_bytes: usize,
}

impl HttpSourceConfig {
    pub fn new(bind: SocketAddr) -> Self {
        Self {
            bind,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
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
