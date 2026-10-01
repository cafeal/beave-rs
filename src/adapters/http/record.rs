use crate::propagation::PropagationCarrier;
use serde::Serialize;
use std::net::SocketAddr;

/// Read-only connection details of a received request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HttpMetadata {
    /// Peer address of the TCP connection. Behind a reverse proxy this is the
    /// proxy; forwarding headers such as `X-Forwarded-For` stay in `headers`.
    pub remote_addr: SocketAddr,
}

/// A decoded HTTP request: its target, headers, and body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HttpRecord<T> {
    /// Request path, such as `/orders`.
    pub path: String,
    /// Query string without the leading `?`.
    pub query: Option<String>,
    /// Header names are lowercase. Repeated headers keep one entry per value,
    /// in received order.
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: T,
    pub metadata: HttpMetadata,
}

impl<T> HttpRecord<T> {
    pub fn metadata(&self) -> &HttpMetadata {
        &self.metadata
    }

    /// The first value of a header, matched case-insensitively.
    pub fn header(&self, name: &str) -> Option<&[u8]> {
        self.headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_slice())
    }
}

/// A request published by [`HttpSink`](super::HttpSink): the body and the
/// request-specific target and headers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpPublish<T> {
    /// Path and optional query, such as `/orders/7?notify=1`, replacing those
    /// of the configured URL. `None` sends the request to the configured URL.
    pub path: Option<String>,
    /// Sent after the configured headers. Repeated names send one header line
    /// per value.
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: T,
}

impl<T> HttpPublish<T> {
    pub fn new(body: T) -> Self {
        Self {
            path: None,
            headers: Vec::new(),
            body,
        }
    }

    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn header(mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// Fields are UTF-8 header values. Setting a field removes every header with
/// that name, matched case-insensitively.
impl<T> PropagationCarrier for HttpPublish<T> {
    fn set_propagation_field(&mut self, name: &str, value: String) {
        self.headers
            .retain(|(header, _)| !header.eq_ignore_ascii_case(name));
        self.headers.push((name.to_owned(), value.into_bytes()));
    }
}
