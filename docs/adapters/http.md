# HTTP adapter

The HTTP adapter provides a source and a sink. `HttpSource` runs an HTTP/1.1
server and turns each `POST` request into a delivery. The response waits for
the delivery's outcome, so a client learns whether its request was processed and
can retry when it was not. [`HttpSink`](#sink) sends each output as a request to
an HTTP endpoint, for example to hand Kafka events to a service with an HTTP
interface. Enable both with the `http` feature:

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

## Source limitations

- HTTP/1.1 only, without TLS. Terminate TLS and HTTP/2 at a reverse proxy.
- Responses have no body. Returning handler output to the client, as a
  request-reply endpoint, is not supported.
- No request timeout is enforced by the server; the drain timeout bounds how
  long shutdown waits for open requests.

## Sink

`HttpSink<C, T>` publishes `HttpPublish<T>` outputs. Each output becomes one
request whose body is encoded by the sink codec:

```rust,no_run
use beavers::{
    App, Json, Result,
    adapters::{
        http::{HttpPublish, HttpSink, HttpSinkConfig},
        kafka::{KafkaRecord, KafkaSource, KafkaSourceConfig},
    },
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize)]
struct Order {
    id: u64,
}

async fn deliver(record: KafkaRecord<Order>) -> Result<HttpPublish<Order>> {
    let key = format!("{}-{}", record.metadata.partition, record.metadata.offset);
    let order = record.value.ok_or_else(|| anyhow::anyhow!("tombstone"))?;
    Ok(HttpPublish::new(order).header("idempotency-key", key))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let source = KafkaSource::<Json, Order>::new(KafkaSourceConfig::new(
        "localhost:9092",
        "order-forwarder",
        ["orders"],
    ));
    let sink = HttpSink::<Json, Order>::new(
        HttpSinkConfig::new("https://orders.example.com/v1/orders")
            .header("content-type", "application/json"),
    )?;
    App::new().subscribe("orders", source, sink, deliver).run().await
}
```

### Sink configuration

| Field | Default | Meaning |
|---|---|---|
| `url` | required | Endpoint with an `http` or `https` scheme |
| `method` | `POST` | Request method |
| `headers` | none | Headers sent with every request, such as `content-type` or `authorization` |
| `request_timeout` | 30 s | Bound on one request attempt, from connecting until the response is read |
| `connect_timeout` | 10 s | Bound on establishing a connection, including the TLS handshake |

`HttpSink::new(config)` validates the configuration: the URL, the method, the
header names and values, and non-zero timeouts. It performs no network access;
the client is created on the first publish. `with_codec(config, codec)` accepts
a configured codec. The sink sets no `content-type` of its own, so configure
the one matching the codec.

`https` endpoints are verified against the platform's trust store with
`rustls`. Requests use HTTP/1.1 over pooled keep-alive connections, and the
client honors the `HTTPS_PROXY`, `HTTP_PROXY`, and `NO_PROXY` environment
variables.

### Publish records

| `HttpPublish<T>` field | Content |
|---|---|
| `path` | Path and optional query, such as `/orders/7?notify=1`, replacing those of the configured URL; `None` keeps them |
| `headers` | Names and byte values sent after the configured headers; repeated names send one line per value |
| `body` | Value encoded by the sink codec |

`HttpPublish::new(body)` creates a record without path or headers, and
`path(...)` and `header(...)` add them. `prepare` encodes the body and checks
the path and headers once, so an invalid path or header is an encode failure
routed by the [error policy](../runtime.md#error-policy), and a publish retry
sends the same request. With `TraceContext`, the trace context is injected as
request headers such as `traceparent`.

### Sink delivery

A publication succeeds when the endpoint answers with a `2xx` status, and the
input is acknowledged only after every output of the delivery succeeded.
Any other status, a connection failure, and a timeout fail the attempt; the
error names the status and the first 512 characters of the response body.
Redirects are not followed, so a `3xx` status fails as well. Failed attempts are retried under the subscription's `publish_retry` policy,
and an exhausted retry stops the subscription without acknowledging the input.

The sink is at-least-once. A request whose response is lost, or that times out
after the endpoint processed it, is sent again by the retry or after the input
is redelivered. Give the endpoint a way to recognize duplicates, such as an
`idempotency-key` header built from the input's Kafka partition and offset or
Pulsar message ID.

Each publication waits for its response, so a subscription sends at most
`concurrency` requests at a time and outputs of one ordering scope arrive in
order. Every status other than `2xx` is retried the same way, including
permanent failures such as `400`; see the [design plan](../plan.md#http-sink)
for the remaining decisions.
