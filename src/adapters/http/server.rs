//! Accept loop and per-request handling. A request whose body decodes waits
//! for the outcome of its delivery before the server responds.
use super::record::{HttpMetadata, HttpRecord};
use crate::{codec::Decoder, shutdown::CancellationToken};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::{
    Method, Request, Response, StatusCode, body::Incoming, header, server::conn::http1,
    service::service_fn,
};
use hyper_util::{rt::TokioIo, server::graceful::GracefulShutdown};
use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};
use tracing::{debug, warn};

/// Pause after a failed accept, such as when file descriptors are exhausted.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// A decoded request waiting for the response status its delivery decides.
/// Dropping `respond` answers the request with `503 Service Unavailable`.
pub(super) struct Pending<T> {
    /// The request with its undecoded body, kept for dead letters.
    pub raw: HttpRecord<Vec<u8>>,
    pub body: T,
    pub respond: oneshot::Sender<StatusCode>,
}

/// What a request handler needs, shared by every connection.
pub(super) struct Intake<C, T> {
    pub requests: mpsc::Sender<Pending<T>>,
    pub codec: Arc<C>,
    pub max_body_bytes: usize,
}

impl<C, T> Clone for Intake<C, T> {
    fn clone(&self) -> Self {
        Self {
            requests: self.requests.clone(),
            codec: self.codec.clone(),
            max_body_bytes: self.max_body_bytes,
        }
    }
}

/// Serves connections until `shutdown` is cancelled, then waits for open
/// connections to finish their current request.
pub(super) async fn serve<C, T>(
    listener: TcpListener,
    intake: Intake<C, T>,
    shutdown: CancellationToken,
) where
    C: Decoder<T>,
    T: Send + 'static,
{
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
        let intake = intake.clone();
        let service = service_fn(move |request| handle(request, remote_addr, intake.clone()));
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

async fn handle<C: Decoder<T>, T>(
    request: Request<Incoming>,
    remote_addr: SocketAddr,
    intake: Intake<C, T>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if request.method() != Method::POST {
        let mut response = status(StatusCode::METHOD_NOT_ALLOWED);
        response
            .headers_mut()
            .insert(header::ALLOW, header::HeaderValue::from_static("POST"));
        return Ok(response);
    }
    Ok(deliver(request, remote_addr, intake).await)
}

fn status(status: StatusCode) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::default());
    *response.status_mut() = status;
    response
}

async fn deliver<C: Decoder<T>, T>(
    request: Request<Incoming>,
    remote_addr: SocketAddr,
    intake: Intake<C, T>,
) -> Response<Full<Bytes>> {
    let (parts, body) = request.into_parts();
    let body = match Limited::new(body, intake.max_body_bytes).collect().await {
        Ok(collected) => collected.to_bytes().to_vec(),
        Err(error) if error.is::<LengthLimitError>() => {
            return status(StatusCode::PAYLOAD_TOO_LARGE);
        }
        Err(_) => return status(StatusCode::BAD_REQUEST),
    };
    let decoded = match intake.codec.decode(&body) {
        Ok(decoded) => decoded,
        Err(error) => {
            debug!(
                error = format!("{error:#}"),
                "HTTP request body failed to decode"
            );
            let mut response = Response::new(Full::from(format!("{error:#}\n")));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            return response;
        }
    };
    let raw = HttpRecord {
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
    let pending = Pending {
        raw,
        body: decoded,
        respond,
    };
    // Waiting for queue capacity applies backpressure to the client.
    if intake.requests.send(pending).await.is_err() {
        return status(StatusCode::SERVICE_UNAVAILABLE);
    }
    status(outcome.await.unwrap_or(StatusCode::SERVICE_UNAVAILABLE))
}
