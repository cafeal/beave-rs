# HTTP adapter

The HTTP adapter runs an HTTP/1.1 server and turns each `POST` request into a
delivery. The response waits for the delivery's outcome, so a client learns
whether its request was processed and can retry when it was not. Enable it with
the `http` feature:

```toml
[dependencies]
beavers = { version = "0.1", features = ["http"] }
```

```rust,no_run
use beavers::{
    App, ErrorPolicy, FailureAction, Json, Result, StdoutSink, Subscription,
    adapters::http::{HttpRecord, HttpSource, HttpSourceConfig},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize)]
struct Order {
    id: u64,
}

async fn accept(record: HttpRecord<Order>) -> Result<Order> {
    Ok(record.body)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let source = HttpSource::<Json, Order>::new(HttpSourceConfig::new(
        "0.0.0.0:8080".parse()?,
    ))?;
    App::new()
        .subscription(
            Subscription::new("orders", source, StdoutSink::<Json>::new(), accept)
                .error_policy(ErrorPolicy {
                    decode: FailureAction::Discard,
                    ..ErrorPolicy::default()
                }),
        )
        .run()
        .await
}
```

```sh
curl -i -X POST localhost:8080/orders -d '{"id":7}'
```

## Configuration

| Field | Default | Meaning |
|---|---|---|
| `bind` | required | Listen address. Port 0 selects a free port. |
| `max_body_bytes` | 1 MiB | Larger bodies are answered with `413` and never become deliveries. |

`HttpSource::new(config)` validates the configuration and binds the listener
immediately, so an address in use is reported by the constructor and
`local_addr()` returns the bound address, including a port chosen for port 0.
The server starts accepting connections on the first `receive`.
`with_codec(config, codec)` accepts a configured codec.

## Records

Handlers receive `HttpRecord<T>`:

| Field | Content |
|---|---|
| `path` | Request path, such as `/orders` |
| `query` | Query string without `?`, or `None` |
| `headers` | Lowercase names and byte values, one entry per received value |
| `body` | Body decoded by the source codec |
| `metadata.remote_addr` | Peer address of the TCP connection |

`header(name)` returns the first value of a header, matched case-insensitively.
Behind a reverse proxy, `remote_addr` is the proxy; forwarding headers such as
`X-Forwarded-For` remain in `headers`. The source accepts every path; a handler
or middleware that serves only some paths rejects the others.

The raw form carried by dead letters is `HttpRecord<Vec<u8>>` with the
undecoded body. Header values that are valid UTF-8, including `traceparent`,
are the delivery's propagation fields, so with the `opentelemetry` feature the
delivery span continues the client's trace.

## Responses and delivery

The server answers each request once its delivery has an outcome:

| Outcome | Status |
|---|---|
| ACK after processing, dead-lettering, or discarding | `204 No Content` |
| ACK after a decode failure was discarded or dead-lettered | `400 Bad Request` |
| Delivery dropped without ACK: subscription failure, shutdown, or drain timeout | `503 Service Unavailable` |
| Request not yet received when the source closes | `503 Service Unavailable` |
| Method other than `POST` | `405 Method Not Allowed` |
| Body larger than `max_body_bytes` | `413 Payload Too Large` |
| Body not readable, such as an aborted upload | `400 Bad Request` |

A success status therefore means the subscription took responsibility for the
request: its outputs were published, or the error policy dead-lettered or
intentionally discarded it. A client that retries on `503`, on a timeout, and on
a closed connection gets at-least-once processing. Its retry can duplicate a
request whose response was lost, so outputs should tolerate duplicates.

A client that disconnects while its request is processed does not cancel the
delivery. The delivery is acknowledged normally and the response is dropped.

With the default [error policy](../runtime.md#error-policy), one undecodable
body stops the subscription and the server. Because HTTP input is not trusted,
set `decode` to `FailureAction::Discard`, or to `FailureAction::DeadLetter` with
a dead-letter sink, so malformed requests are answered with `400` and the
server keeps running. Handler rejections are dead-lettered by default and then
answered with `204`; without a dead-letter sink they stop the subscription.

## Backpressure and ordering

Requests wait in a queue of one until the subscription receives them, so a
subscription at its `max_in_flight` limit makes new requests wait instead of
failing. Clients should apply a timeout. Requests have no ordering key; with
`concurrency` above one they are processed in parallel and complete in any
order.

## Closing

`close` stops accepting connections, answers queued requests that were not
received with `503`, and waits until open connections have answered their
current request. Idle keep-alive connections are closed. The subscription
runtime closes the source after draining, within its drain timeout. Dropping
the source also stops the listener.

## Limitations

- HTTP/1.1 only, without TLS. Terminate TLS and HTTP/2 at a reverse proxy.
- The response has no body. Returning handler output to the client, as a
  request-reply endpoint, is not supported.
- No request timeout is enforced by the server; the drain timeout bounds how
  long shutdown waits for open requests.
