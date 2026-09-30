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
