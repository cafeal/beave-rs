//! Server and client metrics recorded through the `metrics` facade. Server
//! metrics are labeled by listener address and client metrics by destination
//! URL, so several sources and sinks in one process stay distinguishable.
use hyper::StatusCode;
use metrics::{Gauge, Histogram, counter, gauge, histogram};
use std::{net::SocketAddr, time::Instant};

pub(super) struct ServerMetrics {
    listener: String,
    connections: Gauge,
    in_flight: Gauge,
    body_bytes: Histogram,
}

impl ServerMetrics {
    pub(super) fn new(listener: SocketAddr) -> Self {
        let listener = listener.to_string();
        Self {
            connections: gauge!("beavers_http_connections_open", "listener" => listener.clone()),
            in_flight: gauge!("beavers_http_requests_in_flight", "listener" => listener.clone()),
            body_bytes: histogram!("beavers_http_request_body_bytes", "listener" => listener.clone()),
            listener,
        }
    }

    /// Counts an open connection until the returned guard is dropped.
    pub(super) fn connection(&self) -> Open {
        Open::new(&self.connections)
    }

    /// Counts a request in progress until the returned guard is dropped,
    /// including when the client disconnects before the response.
    pub(super) fn request(&self) -> Open {
        Open::new(&self.in_flight)
    }

    pub(super) fn body(&self, bytes: usize) {
        self.body_bytes.record(bytes as f64);
    }

    pub(super) fn response(&self, status: StatusCode, started: Instant) {
        let labels = [
            ("listener", self.listener.clone()),
            ("status", status.as_u16().to_string()),
        ];
        counter!("beavers_http_requests_total", &labels).increment(1);
        histogram!("beavers_http_request_duration_seconds", &labels)
            .record(started.elapsed().as_secs_f64());
    }
}

pub(super) struct Open(Gauge);

impl Open {
    fn new(gauge: &Gauge) -> Self {
        gauge.increment(1.0);
        Self(gauge.clone())
    }
}

impl Drop for Open {
    fn drop(&mut self) {
        self.0.decrement(1.0);
    }
}

/// Request metrics of one HTTP sink.
pub(super) struct ClientMetrics {
    url: String,
}

impl ClientMetrics {
    pub(super) fn new(url: &str) -> Self {
        Self {
            url: url.to_owned(),
        }
    }

    /// Records one attempt; `outcome` is the response status, `error`, or `timeout`.
    pub(super) fn attempt(&self, outcome: &str, started: Instant) {
        let labels = [("url", self.url.clone()), ("outcome", outcome.to_owned())];
        counter!("beavers_http_client_requests_total", &labels).increment(1);
        histogram!("beavers_http_client_request_duration_seconds", &labels)
            .record(started.elapsed().as_secs_f64());
    }
}
