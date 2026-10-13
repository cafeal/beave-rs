use crate::error::{Context, Error, ensure};
use hyper::{
    HeaderMap, Method, Uri,
    header::{HeaderName, HeaderValue},
};
use std::time::Duration;

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

/// Settings of an [`HttpSource`](super::HttpSource).
#[derive(Clone, Debug)]
pub(super) struct SourceConfig {
    pub(super) max_body_bytes: usize,
    pub(super) response: ResponseTiming,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            response: ResponseTiming::default(),
        }
    }
}

impl SourceConfig {
    pub(super) fn validate(&self) -> Result<(), Error> {
        ensure!(
            self.max_body_bytes > 0,
            Error::config,
            "HTTP maximum body size must be greater than zero"
        );
        Ok(())
    }
}

/// Default time allowed for one request, from connecting to reading the response.
pub(super) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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

/// Settings of an [`HttpSink`](super::HttpSink).
#[derive(Clone, Debug)]
pub(super) struct SinkConfig {
    pub(super) url: String,
    pub(super) method: HttpMethod,
    /// Headers that replace the handle's headers of the same name.
    pub(super) headers: Vec<(String, String)>,
    /// Replaces the handle's timeout when set.
    pub(super) timeout: Option<Duration>,
}

impl SinkConfig {
    pub(super) fn new(url: String) -> Self {
        Self {
            url,
            method: HttpMethod::default(),
            headers: Vec::new(),
            timeout: None,
        }
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        self.uri()?;
        headers(&self.headers)?;
        if let Some(timeout) = self.timeout {
            validate_timeout(timeout)?;
        }
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

/// Parses configured headers into a header map.
pub(super) fn headers(headers: &[(String, String)]) -> Result<HeaderMap, Error> {
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| Error::config(format!("invalid HTTP header name {name:?}")))?;
        let value = HeaderValue::from_str(value)
            .map_err(|_| Error::config(format!("invalid value for HTTP header {name:?}")))?;
        map.append(name, value);
    }
    Ok(map)
}

pub(super) fn validate_timeout(timeout: Duration) -> Result<(), Error> {
    ensure!(
        !timeout.is_zero(),
        Error::config,
        "HTTP request timeout must be greater than zero"
    );
    Ok(())
}
