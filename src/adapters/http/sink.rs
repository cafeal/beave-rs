use super::{
    client::{HttpClient, Pool},
    config::{self, HttpMethod, SinkConfig},
    metrics::ClientMetrics,
    record::HttpPublish,
};
use crate::{
    codec::Encoder,
    error::{BoxError, Error, ensure},
    sink::{PublishRejected, Sink},
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{
    HeaderMap, Method, Request, StatusCode, Uri,
    body::Incoming,
    header::{HeaderName, HeaderValue},
};
use std::{
    collections::HashSet,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

/// Bytes of an unsuccessful response body kept for the error message.
const ERROR_BODY_EXCERPT: usize = 512;

/// An encoded request body with its complete, validated header set.
/// Preparing an output performs all codec work once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpPrepared {
    headers: HeaderMap,
    body: Bytes,
}

impl HttpPrepared {
    /// Encoded request body.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Request headers with lowercase names, configured ones included.
    pub fn headers(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes()))
    }
}

/// The validated request target shared by every output.
struct Target {
    uri: Uri,
    method: Method,
    headers: HeaderMap,
    timeout: Duration,
    /// `METHOD scheme://host/path` for error messages, without the query.
    display: String,
    metrics: ClientMetrics,
}

struct State {
    target: OnceLock<Target>,
    closed: AtomicBool,
}

impl State {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            target: OnceLock::new(),
            closed: AtomicBool::new(false),
        })
    }
}

/// Sends each output as one HTTP request and succeeds when the destination
/// answers with a `2xx` status.
///
/// Built with [`HttpClient::sink`]. It validates its settings when it
/// prepares its first output and sends through the handle's connection pool.
/// Clones share the closure state.
///
/// Failed connections, timeouts, `408 Request Timeout`,
/// `429 Too Many Requests`, and `5xx` responses are retried under the
/// subscription's `publish_retry` policy. Every other response, including
/// redirects, is a [`PublishRejected`] error that the error policy routes as
/// [`FailureKind::PublishRejected`](crate::FailureKind::PublishRejected).
pub struct HttpSink<C> {
    http: HttpClient,
    config: SinkConfig,
    codec: Arc<C>,
    state: Arc<State>,
}

impl<C> Clone for HttpSink<C> {
    fn clone(&self) -> Self {
        Self {
            http: self.http.clone(),
            config: self.config.clone(),
            codec: self.codec.clone(),
            state: self.state.clone(),
        }
    }
}

impl<C> HttpSink<C> {
    pub(super) fn new(http: HttpClient, config: SinkConfig, codec: C) -> Self {
        Self {
            http,
            config,
            codec: Arc::new(codec),
            state: State::new(),
        }
    }

    /// The request method. Defaults to [`HttpMethod::Post`].
    pub fn method(mut self, method: HttpMethod) -> Self {
        self.config.method = method;
        self.state = State::new();
        self
    }

    /// Adds a header sent with every request, such as `content-type`. It
    /// replaces the handle's headers of the same name, and a header name set
    /// on an output replaces every configured header of that name.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.headers.push((name.into(), value.into()));
        self.state = State::new();
        self
    }

    /// Maximum time for one attempt, from connecting until the response body
    /// is read, replacing the handle's timeout. An attempt that times out is
    /// retried.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = Some(timeout);
        self.state = State::new();
        self
    }

    /// Checks the handle and sink settings without connecting.
    ///
    /// Fails for a URL without a host, a scheme other than `http` or `https`,
    /// credentials in the URL, an invalid header name or value, or a zero
    /// timeout.
    pub fn validate(&self) -> Result<(), Error> {
        self.http.validate()?;
        self.config.validate()
    }

    /// The request target, validated and built on first use.
    fn target(&self) -> Result<&Target, Error> {
        if let Some(target) = self.state.target.get() {
            return Ok(target);
        }
        self.validate()?;
        let uri = self.config.uri()?;
        let mut headers = self.http.headers()?;
        let own = config::headers(&self.config.headers)?;
        for name in own.keys() {
            headers.remove(name);
        }
        headers.extend(own);
        let location = format!(
            "{}://{}{}",
            uri.scheme_str().unwrap_or_default(),
            uri.authority().map(|a| a.as_str()).unwrap_or_default(),
            uri.path()
        );
        let method = self.config.method.as_hyper();
        let target = Target {
            metrics: ClientMetrics::new(&location),
            display: format!("{method} {location}"),
            timeout: self.config.timeout.unwrap_or(self.http.default_timeout()),
            uri,
            method,
            headers,
        };
        let _ = self.state.target.set(target);
        Ok(self.state.target.get().expect("target was set"))
    }

    fn client(&self) -> Result<(Pool, &Target), BoxError> {
        ensure!(
            !self.state.closed.load(Ordering::Acquire),
            Error::closed,
            "HTTP sink is closed"
        );
        let target = self.target()?;
        let pool = self.http.pool(target.uri.scheme_str() == Some("https"))?;
        Ok((pool, target))
    }

    async fn send(
        &self,
        client: Pool,
        target: &Target,
        output: &HttpPrepared,
    ) -> Result<(), BoxError> {
        let mut request = Request::new(Full::new(output.body.clone()));
        *request.method_mut() = target.method.clone();
        *request.uri_mut() = target.uri.clone();
        *request.headers_mut() = output.headers.clone();
        let started = Instant::now();
        let exchange = async {
            let response = client.request(request).await?;
            let (parts, body) = response.into_parts();
            let keep = if parts.status.is_success() {
                0
            } else {
                ERROR_BODY_EXCERPT
            };
            Ok::<_, BoxError>((parts.status, read_body(body, keep).await))
        };
        let (status, body) = match tokio::time::timeout(target.timeout, exchange).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                target.metrics.attempt("error", started);
                return Err(Error::wrap(format!("HTTP {} failed", target.display), error).into());
            }
            Err(_) => {
                target.metrics.attempt("timeout", started);
                return Err(Error::msg(format!(
                    "HTTP {} timed out after {:?}",
                    target.display, target.timeout
                ))
                .into());
            }
        };
        target.metrics.attempt(status.as_str(), started);
        if status.is_success() {
            return Ok(());
        }
        let excerpt = String::from_utf8_lossy(&body);
        let excerpt = excerpt.trim();
        let error = if excerpt.is_empty() {
            Error::msg(format!("HTTP {} answered {status}", target.display))
        } else {
            Error::msg(format!(
                "HTTP {} answered {status}: {excerpt}",
                target.display
            ))
        };
        if retryable(status) {
            Err(error.into())
        } else {
            Err(PublishRejected::wrap(error))
        }
    }
}

impl<C> Sink<HttpPublish<C::Value>> for HttpSink<C>
where
    C: Encoder<Value: Send + Sync + 'static>,
{
    type Prepared = HttpPrepared;

    fn prepare(&self, record: HttpPublish<C::Value>) -> Result<Self::Prepared, BoxError> {
        let body = Bytes::from(self.codec.encode(&record.body)?);
        let mut headers = self.target()?.headers.clone();
        let mut replaced = HashSet::new();
        for (name, value) in record.headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| Error::invalid_record(format!("invalid HTTP header name {name:?}")))?;
            let value = HeaderValue::from_bytes(&value).map_err(|_| {
                Error::invalid_record(format!("invalid value for HTTP header {name:?}"))
            })?;
            if replaced.insert(name.clone()) {
                headers.remove(&name);
            }
            headers.append(name, value);
        }
        Ok(HttpPrepared { headers, body })
    }

    /// Succeeds once the destination answers with a `2xx` status. Dropping the
    /// future abandons the request; the destination may still have processed
    /// it, so a retried output can arrive twice.
    async fn publish(&self, output: &Self::Prepared) -> Result<(), BoxError> {
        let (client, target) = self.client()?;
        self.send(client, target, output).await
    }

    /// Refuses further publications. The handle's connection pool stays open
    /// for its other sinks.
    async fn close(&self) -> Result<(), BoxError> {
        self.state.closed.store(true, Ordering::Release);
        Ok(())
    }
}

/// Statuses that can succeed when the same request is sent again.
fn retryable(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
}

/// Reads a response body to its end so the connection can be reused, keeping
/// at most `keep` bytes. A body that fails midway ends the read early.
async fn read_body(mut body: Incoming, keep: usize) -> Vec<u8> {
    let mut kept = Vec::new();
    while let Some(Ok(frame)) = body.frame().await {
        if let Ok(data) = frame.into_data() {
            let take = keep.saturating_sub(kept.len()).min(data.len());
            kept.extend_from_slice(&data[..take]);
        }
    }
    kept
}
