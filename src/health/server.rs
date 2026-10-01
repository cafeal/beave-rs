//! HTTP/1.1 server answering Kubernetes-style liveness and readiness probes.
use super::state::{Health, HealthReport};
use crate::shutdown::CancellationToken;
use bytes::Bytes;
use http_body_util::Full;
use hyper::{
    Method, Request, Response, StatusCode, body::Incoming, header, server::conn::http1,
    service::service_fn,
};
use hyper_util::{rt::TokioIo, server::graceful::GracefulShutdown};
use std::{
    convert::Infallible,
    net::{self, SocketAddr},
    time::Duration,
};
use tokio::net::TcpListener;
use tracing::{debug, warn};

/// Pause after a failed accept, such as when file descriptors are exhausted.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Serves `GET /livez` and `GET /readyz` for an [`App`](crate::App) registered
/// with [`App::health_server`](crate::App::health_server).
///
/// Each endpoint answers `200 OK` when the [`HealthReport`] is live or ready
/// respectively and `503 Service Unavailable` otherwise, with the report as a
/// JSON body. Other paths are answered with `404 Not Found` and other methods
/// with `405 Method Not Allowed`.
///
/// The listener is bound on construction, so address errors surface
/// immediately and [`local_addr`](Self::local_addr) is known before the
/// application runs. The server starts with the application and stops after
/// every subscription has finished, so readiness turns `503` as soon as
/// shutdown starts while liveness keeps answering during draining.
pub struct HealthServer {
    listener: net::TcpListener,
    local_addr: SocketAddr,
}

impl HealthServer {
    /// Binds the listener. Port 0 selects a free port.
    pub fn bind(addr: SocketAddr) -> anyhow::Result<Self> {
        let listener = net::TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            local_addr: listener.local_addr()?,
            listener,
        })
    }

    /// The bound address, including the port chosen for port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Serves probes until `stop` is cancelled, then waits for open
    /// connections to finish their current request.
    pub(crate) async fn serve(self, health: Health, stop: CancellationToken) -> anyhow::Result<()> {
        let listener = TcpListener::from_std(self.listener)?;
        let graceful = GracefulShutdown::new();
        loop {
            let stream = tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => stream,
                    Err(error) => {
                        warn!(error = %error, "health accept failed");
                        tokio::time::sleep(ACCEPT_BACKOFF).await;
                        continue;
                    }
                },
            };
            let health = health.clone();
            let service = service_fn(move |request| {
                let response = respond(&request, &health);
                async move { Ok::<_, Infallible>(response) }
            });
            let connection = graceful
                .watch(http1::Builder::new().serve_connection(TokioIo::new(stream), service));
            tokio::spawn(async move {
                if let Err(error) = connection.await {
                    debug!(error = %error, "health connection closed with an error");
                }
            });
        }
        drop(listener);
        graceful.shutdown().await;
        Ok(())
    }
}

fn respond(request: &Request<Incoming>, health: &Health) -> Response<Full<Bytes>> {
    let probe: fn(&HealthReport) -> bool = match request.uri().path() {
        "/livez" => |report| report.live,
        "/readyz" => |report| report.ready,
        _ => return status(StatusCode::NOT_FOUND),
    };
    if request.method() != Method::GET {
        let mut response = status(StatusCode::METHOD_NOT_ALLOWED);
        response
            .headers_mut()
            .insert(header::ALLOW, header::HeaderValue::from_static("GET"));
        return response;
    }
    let report = health.report();
    let mut response = match serde_json::to_vec(&report) {
        Ok(body) => Response::new(Full::from(body)),
        Err(_) => return status(StatusCode::INTERNAL_SERVER_ERROR),
    };
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    if !probe(&report) {
        *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    }
    response
}

fn status(status: StatusCode) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::default());
    *response.status_mut() = status;
    response
}
