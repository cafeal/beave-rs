use anyhow::Context as _;
use reqwest::{
    Method, Url,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use std::{net::SocketAddr, time::Duration};

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

/// Default bound on one request, from connecting until the response body is read.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Default bound on establishing a connection, including the TLS handshake.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug)]
pub struct HttpSinkConfig {
    /// Endpoint of every request, with an `http` or `https` scheme. An
    /// [`HttpPublish::path`](super::HttpPublish::path) replaces its path and
    /// query.
    pub url: String,
    /// Request method. Defaults to `POST`.
    pub method: String,
    /// Headers sent with every request, before the headers of each
    /// [`HttpPublish`](super::HttpPublish), such as
    /// `content-type: application/json` or an authorization header.
    pub headers: Vec<(String, String)>,
    /// Bound on one request attempt, from connecting until the response is read.
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
}

impl HttpSinkConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            method: "POST".to_owned(),
            headers: Vec::new(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        }
    }

    /// Adds a header sent with every request.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        self.endpoint()?;
        self.request_method()?;
        self.default_headers()?;
        anyhow::ensure!(
            !self.request_timeout.is_zero() && !self.connect_timeout.is_zero(),
            "HTTP sink timeouts must be greater than zero"
        );
        Ok(())
    }

    pub(super) fn endpoint(&self) -> anyhow::Result<Url> {
        let url = Url::parse(&self.url)
            .with_context(|| format!("invalid HTTP sink URL {:?}", self.url))?;
        anyhow::ensure!(
            matches!(url.scheme(), "http" | "https"),
            "HTTP sink URL must use http or https, not {}",
            url.scheme()
        );
        Ok(url)
    }

    pub(super) fn request_method(&self) -> anyhow::Result<Method> {
        Method::from_bytes(self.method.as_bytes())
            .with_context(|| format!("invalid HTTP method {:?}", self.method))
    }

    pub(super) fn default_headers(&self) -> anyhow::Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        for (name, value) in &self.headers {
            let (name, value) = header(name, value.as_bytes())?;
            headers.append(name, value);
        }
        Ok(headers)
    }
}

/// Parses a header name and value.
pub(super) fn header(name: &str, value: &[u8]) -> anyhow::Result<(HeaderName, HeaderValue)> {
    let name = HeaderName::from_bytes(name.as_bytes())
        .with_context(|| format!("invalid HTTP header name {name:?}"))?;
    let value = HeaderValue::from_bytes(value)
        .with_context(|| format!("invalid value of HTTP header {name}"))?;
    Ok((name, value))
}
