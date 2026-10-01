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
    App, Json, Result, StdoutSink,
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
        .subscribe("orders", source, StdoutSink::<Json>::new(), accept)
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
| `response` | `ResponseTiming::Ack` | When a received request is answered; see [response timing](#response-timing). |

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

## Decoding

The server decodes each body with the source codec as soon as the request is
read, before the request becomes a delivery. A body that fails to decode is
answered with `400 Bad Request` and the codec error as a plain-text body. It
never reaches the subscription, so the [error policy](../runtime.md#error-policy)
and dead-letter sink do not see it, and one malformed request cannot stop the
subscription. `SourceMessage::decode` of an HTTP delivery therefore always
succeeds.

## Responses and delivery

With the default `ResponseTiming::Ack`, the server answers each decoded request
once its delivery has an outcome:

| Outcome | Status |
|---|---|
| ACK after processing, dead-lettering, or discarding | `200 OK` |
| Delivery dropped without ACK: subscription failure, shutdown, or drain timeout | `503 Service Unavailable` |
| Request not yet received when the subscription stops receiving | `503 Service Unavailable` |
| Body that fails to decode | `400 Bad Request` with the codec error |
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

Handler rejections are dead-lettered by default and then answered with `200`;
without a dead-letter sink they stop the subscription, and the server with it.

## Response timing

`ResponseTiming::Receive` answers `202 Accepted` as soon as the subscription
receives the decoded request, before processing. Clients get a response
without waiting for the pipeline, but a success status no longer means the
request was processed:

- A request is lost if the process stops before its delivery completes.
- Handler and publish failures are not reported to the client, so configure the
  error policy and a dead-letter sink to keep them.
- Requests not yet received when the subscription stops receiving are still
  answered with `503`, and invalid or undecodable requests still receive `405`,
  `413`, or `400` before they become deliveries.

Use it when the producer cannot wait for processing and occasional loss on a
crash is acceptable. Keep the default when every accepted request must be
processed at least once.

## Backpressure and ordering

Requests wait in a queue of one until the subscription receives them, so a
subscription at its `max_in_flight` limit makes new requests wait instead of
failing. Clients should apply a timeout. Requests have no ordering key; with
`concurrency` above one they are processed in parallel and complete in any
order.

## Closing

When the subscription stops receiving, on application shutdown or a failure,
the runtime calls `stop_receiving` before draining. The source then stops
accepting connections, closes idle keep-alive connections, and answers queued
requests that were not received with `503`. New connections are refused while
received requests drain; their connections stay open until each delivery
completes and its response is sent.

`close` runs after draining, within the drain timeout. It does the same and
then waits until open connections have answered their current request.
Dropping the source also stops the listener.

Behind a load balancer, serve [health checks](../runtime.md#health-checks)
with the `health` feature. Readiness fails as soon as shutdown starts, so a
Kubernetes Service stops routing new requests to an instance before its source
refuses them.

## Metrics

The server records these metrics through the `metrics` facade, alongside the
[subscription metrics](../runtime.md#observability). Each carries a `listener`
label with the bound address.

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `beavers_http_requests_total` | counter | `status` | Answered requests by response status |
| `beavers_http_request_duration_seconds` | histogram | `status` | Time from receiving the request head to the response |
| `beavers_http_requests_in_flight` | gauge | | Requests being read, decoded, or awaiting their delivery |
| `beavers_http_connections_open` | gauge | | Open client connections |
| `beavers_http_request_body_bytes` | histogram | | Size of bodies read within `max_body_bytes` |

A request whose client disconnects before the response is not counted in
`beavers_http_requests_total`. Decode failures appear as `status="400"` and
never reach the subscription's `beavers_delivery_failures_total`.

## Limitations

- HTTP/1.1 only, without TLS. Terminate TLS and HTTP/2 at a reverse proxy.
- Responses have no body. Returning handler output to the client, as a
  request-reply endpoint, is not supported.
- No request timeout is enforced by the server; the drain timeout bounds how
  long shutdown waits for open requests.
