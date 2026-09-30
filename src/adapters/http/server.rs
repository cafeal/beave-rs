//! Accept loop and per-request handling. A request waits for the outcome of
//! its delivery before the server responds.
use super::record::{HttpMetadata, HttpRecord};
use crate::shutdown::CancellationToken;
use bytes::Bytes;
use http_body_util::{BodyExt, Empty, LengthLimitError, Limited};
use hyper::{
    Method, Request, Response, StatusCode, body::Incoming, header, server::conn::http1,
    service::service_fn,
};
use hyper_util::{rt::TokioIo, server::graceful::GracefulShutdown};
use std::{convert::Infallible, net::SocketAddr, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};
use tracing::{debug, warn};

/// Pause after a failed accept, such as when file descriptors are exhausted.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// A received request waiting for the response status its delivery decides.
/// Dropping `respond` answers the request with `503 Service Unavailable`.
pub(super) struct Pending {
    pub record: HttpRecord<Vec<u8>>,
    pub respond: oneshot::Sender<StatusCode>,
}

/// Serves connections until `shutdown` is cancelled, then waits for open
/// connections to finish their current request.
pub(super) async fn serve(
    listener: TcpListener,
    requests: mpsc::Sender<Pending>,
    max_body_bytes: usize,
    shutdown: CancellationToken,
) {
    let graceful = GracefulShutdown::new();
    loop {
        let (stream, remote_addr) = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    warn!(error = %error, "HTTP accept failed");
                    tokio::time::sleep(ACCEPT_BACKOFF).await;
                    continue;
                }
            },
        };
        let requests = requests.clone();
        let service = service_fn(move |request| {
            handle(request, remote_addr, requests.clone(), max_body_bytes)
        });
        let connection =
            graceful.watch(http1::Builder::new().serve_connection(TokioIo::new(stream), service));
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                debug!(error = %error, "HTTP connection closed with an error");
            }
        });
    }
    drop(listener);
    graceful.shutdown().await;
}

async fn handle(
    request: Request<Incoming>,
    remote_addr: SocketAddr,
    requests: mpsc::Sender<Pending>,
    max_body_bytes: usize,
) -> Result<Response<Empty<Bytes>>, Infallible> {
    let mut response = Response::new(Empty::new());
    if request.method() != Method::POST {
        *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
        response
            .headers_mut()
            .insert(header::ALLOW, header::HeaderValue::from_static("POST"));
        return Ok(response);
    }
    *response.status_mut() = deliver(request, remote_addr, requests, max_body_bytes).await;
    Ok(response)
}

async fn deliver(
    request: Request<Incoming>,
    remote_addr: SocketAddr,
    requests: mpsc::Sender<Pending>,
    max_body_bytes: usize,
) -> StatusCode {
    let (parts, body) = request.into_parts();
    let body = match Limited::new(body, max_body_bytes).collect().await {
        Ok(collected) => collected.to_bytes().to_vec(),
        Err(error) if error.is::<LengthLimitError>() => return StatusCode::PAYLOAD_TOO_LARGE,
        Err(_) => return StatusCode::BAD_REQUEST,
    };
    let record = HttpRecord {
        path: parts.uri.path().to_owned(),
        query: parts.uri.query().map(str::to_owned),
        headers: parts
            .headers
            .iter()
            .map(|(name, value)| (name.as_str().to_owned(), value.as_bytes().to_vec()))
            .collect(),
        body,
        metadata: HttpMetadata { remote_addr },
    };
    let (respond, outcome) = oneshot::channel();
    // Waiting for queue capacity applies backpressure to the client.
    if requests.send(Pending { record, respond }).await.is_err() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    outcome.await.unwrap_or(StatusCode::SERVICE_UNAVAILABLE)
}
