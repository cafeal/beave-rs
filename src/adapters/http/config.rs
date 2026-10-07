use crate::error::{Context, Error, ensure};
use hyper::{
    Method, Uri,
    header::{HeaderName, HeaderValue},
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

/// Configuration for an [`HttpSource`](super::HttpSource).
#[derive(Clone, Debug)]
pub struct HttpSourceConfig {
    /// Address the server listens on. Port 0 selects a free port, which
    /// [`HttpSource::local_addr`](super::HttpSource::local_addr) reports.
    pub bind: SocketAddr,
    /// Larger request bodies are answered with `413 Payload Too Large` without
    /// becoming deliveries.
    pub max_body_bytes: usize,
    /// When a received request is answered. Defaults to [`ResponseTiming::Ack`].
    pub response: ResponseTiming,
}

impl HttpSourceConfig {
    /// Creates a configuration for `bind` with a 1 MiB body limit and
    /// [`ResponseTiming::Ack`].
    pub fn new(bind: SocketAddr) -> Self {
        Self {
            bind,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            response: ResponseTiming::default(),
        }
    }

    /// Checks the configuration without binding the address; fails when
    /// `max_body_bytes` is zero.
    pub fn validate(&self) -> Result<(), Error> {
        ensure!(
            self.max_body_bytes > 0,
            Error::config,
            "HTTP maximum body size must be greater than zero"
        );
        Ok(())
    }
}

/// Default time allowed for one request, from connecting to reading the response.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Request method of an [`HttpSink`](super::HttpSink).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HttpMethod {
    /// `POST`, the default.
    #[default]
    Post,
    /// `PUT`.
    Put,
    /// `PATCH`.
    Patch,
}

impl HttpMethod {
    pub(super) fn as_hyper(self) -> Method {
        match self {
            Self::Post => Method::POST,
            Self::Put => Method::PUT,
            Self::Patch => Method::PATCH,
        }
    }
}

/// Configuration for an [`HttpSink`](super::HttpSink).
#[derive(Clone, Debug)]
pub struct HttpSinkConfig {
    /// Absolute `http` or `https` URL every output is sent to. Credentials
    /// belong in `headers`, not in the URL.
    pub url: String,
    /// Request method. Defaults to [`HttpMethod::Post`].
    pub method: HttpMethod,
    /// Headers sent with every request, such as `content-type` or
    /// `authorization`. A header name set on an output replaces every
    /// configured header of that name.
    pub headers: Vec<(String, String)>,
    /// Maximum time for one attempt, from connecting until the response body
    /// is read. An attempt that times out is retried.
    pub timeout: Duration,
}

impl HttpSinkConfig {
    /// Creates a configuration for `url` with [`HttpMethod::Post`], no headers,
    /// and a 30 second timeout.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            method: HttpMethod::default(),
            headers: Vec::new(),
            timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    /// Checks the configuration without connecting.
    ///
    /// Fails for a URL without a host, a scheme other than `http` or `https`,
    /// credentials in the URL, an invalid header name or value, or a zero
    /// timeout.
    pub fn validate(&self) -> Result<(), Error> {
        self.uri()?;
        for (name, value) in &self.headers {
            HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| Error::config(format!("invalid HTTP header name {name:?}")))?;
            HeaderValue::from_str(value)
                .map_err(|_| Error::config(format!("invalid value for HTTP header {name:?}")))?;
        }
        ensure!(
            !self.timeout.is_zero(),
            Error::config,
            "HTTP request timeout must be greater than zero"
        );
        Ok(())
    }

    pub(super) fn uri(&self) -> Result<Uri, Error> {
        let uri: Uri = self.url.parse().map_err(|error| {
            Error::config(format!("invalid HTTP sink URL {:?}: {error}", self.url))
        })?;
        ensure!(
            matches!(uri.scheme_str(), Some("http" | "https")),
            Error::config,
            "HTTP sink URL must use the http or https scheme"
        );
        let authority = uri
            .authority()
            .context("HTTP sink URL must include a host")?;
        ensure!(
            !authority.as_str().contains('@'),
            Error::config,
            "HTTP sink URL must not contain credentials; send them in a header"
        );
        Ok(uri)
    }
}
