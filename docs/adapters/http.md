# HTTP adapter

The HTTP adapter connects pipelines to services whose interface is HTTP. Enable
it with the `http` feature:

```toml
[dependencies]
beavers = { version = "0.1", features = ["http"] }
```

- [`HttpSource`](#source) runs an HTTP/1.1 server and turns each `POST` request
  into a delivery. The response waits for the delivery's outcome, so a client
  learns whether its request was processed and can retry when it was not.
- [`HttpSink`](#sink) sends each output as an HTTP request and succeeds on a
  `2xx` response, so the input is acknowledged only after the destination
  accepted every output.

## Source

```rust,no_run
use beavers::{
    App, Json, StdoutSink,
    adapters::http::{HttpRecord, HttpSource, HttpSourceConfig},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize)]
struct Order {
    id: u64,
}

async fn accept(record: HttpRecord<Order>) -> beavers::Result<Order> {
    Ok(record.body)
}

#[tokio::main]
async fn main() -> Result<(), beavers::BoxError> {
    let source = HttpSource::<Json, Order>::new(HttpSourceConfig::new(
        "0.0.0.0:8080".parse()?,
    ))?;
    App::new()
        .subscribe("orders", source, StdoutSink::<Json>::new(), accept)
        .run()
        .await?;
    Ok(())
}
```

```sh
curl -i -X POST localhost:8080/orders -d '{"id":7}'
```

### Server configuration

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

### Records

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

### Decoding

The server decodes each body with the source codec as soon as the request is
read, before the request becomes a delivery. A body that fails to decode is
answered with `400 Bad Request` and the codec error as a plain-text body. It
never reaches the subscription, so the [error policy](../runtime.md#error-policy)
and dead-letter sink do not see it, and one malformed request cannot stop the
subscription. `SourceMessage::decode` of an HTTP delivery therefore always
succeeds.

### Responses and delivery

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

### Response timing

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

### Backpressure and ordering

Requests wait in a queue of one until the subscription receives them, so a
subscription at its `max_in_flight` limit makes new requests wait instead of
failing. Clients should apply a timeout. Requests have no ordering key; with
`concurrency` above one they are processed in parallel and complete in any
order.

### Closing

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

### Server metrics

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

### Server limitations

- HTTP/1.1 only, without TLS. Terminate TLS and HTTP/2 at a reverse proxy.
- Responses have no body. Returning handler output to the client, as a
  request-reply endpoint, is not supported.
- No request timeout is enforced by the server; the drain timeout bounds how
  long shutdown waits for open requests.

## Sink

`HttpSink<C, T>` accepts `HttpPublish<T>` outputs. It encodes each body with
its codec and sends one request per output to the configured URL. A typical use
is handing events consumed from a broker to a service with an HTTP API:

```rust,ignore
use beavers::{
    App, Json, Result,
    adapters::{
        http::{HttpPublish, HttpSink, HttpSinkConfig},
        kafka::{KafkaRecord, KafkaSource, KafkaSourceConfig},
    },
};

async fn to_request(record: KafkaRecord<Order>) -> Result<HttpPublish<Order>> {
    let metadata = record.metadata().clone();
    let order = record.value.ok_or("tombstone")?;
    // A stable key lets the service recognize a redelivered event.
    let key = format!("{}-{}-{}", metadata.topic, metadata.partition, metadata.offset);
    Ok(HttpPublish::new(order).with_header("idempotency-key", key))
}

let source = KafkaSource::<Json, Order>::new(KafkaSourceConfig::new(
    "localhost:9092",
    "orders-to-billing",
    ["orders"],
));
let mut config = HttpSinkConfig::new("https://billing.internal/orders");
config
    .headers
    .push(("content-type".into(), "application/json".into()));
let sink = HttpSink::<Json, Order>::new(config)?;
App::new().subscribe("orders", source, sink, to_request).run().await?;
```

### Sink configuration

| Field | Default | Meaning |
|---|---|---|
| `url` | required | Absolute `http` or `https` URL, including any path and query |
| `method` | `HttpMethod::Post` | `Post`, `Put`, or `Patch` |
| `headers` | none | Headers sent with every request, such as `content-type` or `authorization` |
| `timeout` | 30 s | Limit for one attempt, from connecting until the response body is read |

`HttpSink::new(config)` validates the configuration and returns an error for a
URL without a host, a scheme other than `http` or `https`, credentials in the
URL, invalid header names or values, or a zero timeout. The client is created
on the first publication. `with_codec(config, codec)` accepts a configured
codec. Clones share one connection pool and closing state.

`https` servers are verified against the platform's root certificates, loaded
with the client; a platform without any fails every publication. The codec does
not set `content-type`, so configure it in `headers`.

### Requests

`HttpPublish<T>` carries the body and optional `headers`. A header name set on
an output replaces every configured header of that name; repeating a name on
the output sends each value. Header names are case-insensitive, and invalid
names or values fail `prepare`, which the error policy routes as `Encode`. The
prepared request keeps the encoded body and complete header set, so a retry
sends the same bytes without encoding again.

`HttpPublish` implements `PropagationCarrier`, so with the `opentelemetry`
feature the outgoing request carries the delivery's `traceparent` header.
HTTP records have no tombstones, so `HttpPublish` does not implement
`TombstonePublish`, and `Tombstones` middleware for an HTTP sink does not
compile.

### Responses and retries

Each output waits for its response before the next output of the delivery is
sent, and the delivery is acknowledged after every output succeeded:

| Result | Outcome |
|---|---|
| `2xx` | Published |
| `408`, `429`, or `5xx` | Retried under `publish_retry` |
| Connection failure or `timeout` | Retried under `publish_retry` |
| Any other status, including `3xx` redirects | [`PublishRejected`](../runtime.md#error-policy) without retry |

An exhausted retry stops the subscription without ACK, so a destination that is
down leaves the input unacknowledged for redelivery after restart. A rejected
output is routed by the error policy as `FailureKind::PublishRejected`, which
dead-letters the input by default and stops the subscription when no
dead-letter sink is configured. The error text names the method, URL without
its query, status, and the start of the response body.

Delivery is at-least-once. A request whose response is lost or times out is
sent again, the whole delivery is processed again after a crash or a failure of
a later output, and outputs published before a rejection stay published. The
destination should treat requests as idempotent, for example by keying them on
the source record's position, as the example does. Redirects are not followed
and `Retry-After` is not honored; retry delays come from `publish_retry`.

Outputs of one delivery are sent in order. Deliveries processed concurrently
send their requests concurrently over pooled HTTP/1.1 connections, so the
subscription's `concurrency` bounds the requests in flight to one destination.

### Closing the sink

`close` refuses further publications from the sink and all its clones. Requests
already in progress finish, and idle connections close when the last of them
releases the pool.

### Client metrics

Each attempt is recorded through the `metrics` facade with a `url` label
holding the scheme, host, and path, without the query, and an `outcome` label
holding the response status, `error` for a failed connection or response, or
`timeout`.

| Metric | Type | Meaning |
|---|---|---|
| `beavers_http_client_requests_total` | counter | Request attempts, including retried ones |
| `beavers_http_client_request_duration_seconds` | histogram | Time from sending the request to reading the response body |

### Sink limitations

- HTTP/1.1 only. Client certificates and custom root certificates are not
  configurable.
- The URL and method are fixed per sink; an output cannot choose its path.
- Response bodies are discarded after success and truncated in error messages.
