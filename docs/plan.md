# beave.rs — Design plan

This is the project's design direction and roadmap, not a claim that every
capability below is implemented. See the [runtime guide](runtime.md) for current
behavior and [architecture](architecture.md) for implemented trait boundaries.

## Project and motivation

The project is named **Beavers**, with **beave.rs** as its brand. The name treats
the final `rs` in “beavers” as Rust's `.rs` extension.

Beavers build dams that control water flow. Similarly, this framework receives
message and event flows and manages processing, buffering, backpressure, and
routing before forwarding output. It shares Watermill's water-flow inspiration
while developing an identity based on Rust's type system.

## Goals

Build a lightweight Rust message processing framework that works across Kafka,
NATS JetStream, SQS, and other brokers:

```text
Source → Subscription → Handler → Sink
```

Users primarily write typed handlers:

```rust,ignore
async fn handler(input: Input) -> Result<Output>
```

The framework handles connectivity, serialization, consumer groups, ACKs, offset
commits, concurrency, ordering, retries, DLQs, transactions, shutdown, and
observability. It is closer to Watermill or a broker-independent AWS Lambda event
source mapping than to a stateful stream-processing engine. Delivery guarantees
must reflect actual broker capabilities.

## Non-goals

The initial scope excludes stateful processing, windows, joins, event time,
watermarks, state stores, DAG execution, distributed scheduling, SQL, and atomic
fan-out to multiple sinks. This is not a Flink replacement.

## Core model and subscriptions

The application is a bipartite graph of sources and sinks. Each edge is an
independent subscription with exactly one source, one handler, and one sink.
A subscription also owns codec selection, concurrency, ordering, retry/error
policy, DLQ configuration, delivery semantics, and observability context.

```text
Source A ── Subscription A ── Sink X
    └────── Subscription B ── Sink Y
Source B ── Subscription C ── Sink Z
```

Do not directly fan out one runtime delivery to several handlers. Independent
subscriptions provide independent progress, ACKs, retries, failures, concurrency,
and observability. In Kafka, separate consumer groups can express this model.

Multiple output sinks within one subscription are excluded initially: if one
publish succeeds and another fails, retry and ACK semantics become ambiguous and
can duplicate output. Use separate subscriptions or another broker hop instead.

## Handler API

Plain input/output is the default. Raw broker messages and bytes should not leak
into ordinary handlers. Metadata and explicit error classification are available
when needed.

Output cardinality is explicit:

```rust
pub enum Emit<T> {
    None,
    One(T),
    Many(Vec<T>),
}
```

A `Vec<T>` returned as an ordinary output is one payload, not implicit fan-out.
The prototype uses `Subscription::new_emitting` for `Result<Emit<T>>` handlers.

### Handler execution model

**Decision recorded; blocking handlers are not implemented yet.**

Register async handlers directly and mark synchronous handlers with a wrapper:

```rust,ignore
app.subscribe(source, sink, async_handler);
app.subscribe(source, sink, blocking(sync_handler)); // Future API
```

Use a distinct wrapper type rather than overlapping blanket implementations to
automatically distinguish synchronous and asynchronous functions.

The wrapper will implement the existing `Handler<Input>` trait, submit synchronous
work to a framework-managed worker pool, and return a future waiting for the
result. The runtime can retain the same handler contract. Source and Sink do not
need synchronous APIs; publish and ACK still follow handler completion.

Reuse worker threads instead of creating one per message. Bound both worker count
and queued jobs, consistently with subscription concurrency and max_in_flight.
Tokio's shared `spawn_blocking` pool is not equivalent to a dedicated framework
pool. Pool ownership, startup, and shutdown must be designed before implementation.

Async handlers must not perform blocking work directly. Moving synchronous
handlers to a separate pool does not protect communication and control from an
async handler that blocks. Whether to isolate the handler async executor is a
separate, unresolved decision; current execution provides no such isolation.

Cancellation or timeout of a future cannot be treated as forcibly stopping an
already running synchronous handler. Define disposal of queued jobs, treatment of
running results, panic handling, and pool shutdown deadlines before implementing
the pool. Never treat unfinished handler work as successful or ACK it.

## Message models and metadata

Let handler input/output types express whether they use just a value or an entire
broker-specific record. Do not force unrelated metadata into a universal struct.

| Platform | Body | Broker-specific information |
|---|---|---|
| Kafka | value | key, headers, timestamp, partition, offset |
| NATS | payload | subject, headers, reply subject |
| SQS | string body | message attributes, message ID, receipt handle, receive count |

Conceptual APIs, not yet implemented:

```rust,ignore
async fn handler(input: Order) -> Result<Event>

async fn handler(input: KafkaRecord<String, Order>) -> Result<Event>

async fn handler(input: KafkaRecord<String, Order>)
    -> Result<KafkaPublishRecord<String, Event>>
```

`KafkaPublishRecord` is a provisional name. Distinguish received metadata from
publish metadata so receive-only fields such as offset cannot be copied blindly.
Kafka keys should be typed separately from values and other metadata:

```rust,ignore
struct KafkaRecord<K, V> {
    key: Option<K>,
    value: Option<V>,
    metadata: KafkaMetadata,
}
```

Preserve the difference between a null value and empty bytes. The policy for
null values with plain-value handlers is still open.

### Plain output and inheritance

Allow `value → value` when the subscription policy and sink configuration can
determine output metadata. Using the same platform alone is not sufficient.

Default Kafka-to-Kafka inheritance is intended to be:

| Field | Policy |
|---|---|
| key | Inherit |
| headers | Inherit |
| partition | Let the sink determine it |
| offset | Never inherit |
| timestamp | Let the sink determine it for the new output |

Plain output uses configured inheritance/mapping and sink settings. When a handler
explicitly returns publish metadata, use it without implicitly merging input
metadata. Cross-platform value-only forwarding must explicitly define what
happens to metadata; do not silently discard it. Reject missing mappings at
compile time where practical, otherwise at startup. Local components without
broker metadata can directly support value-to-value processing.

### Subscription middleware

Express simple inheritance and conversion as typed subscription middleware. For
example, explicitly map a Kafka key to an SQS MessageGroupId rather than treating
the two fields as interchangeable.

```rust,ignore
Subscription::new(kafka_source, sqs_sink, handler)
    .middleware(MapMetadata::new(map_kafka_to_sqs))
```

This is a future API sketch. Validate input metadata and sink publish metadata
types during registration. Resolve output metadata after the handler and before
encoding/publication. Apply mapping to plain outputs; explicit handler metadata
takes precedence. With `Emit::Many`, apply it per output. Publish retries reuse
already mapped outputs. Mapping failures must not ACK input; detailed error
classification remains open.

Use the same mechanism for default same-platform inheritance. Middleware must
not own ACK ordering, retry sequencing, or delivery semantics.

## Serialization and codecs

For byte-based transports, the lifecycle is:

```text
Broker bytes → Source → Decoder → Input → Handler
    → Output → Encoder → Sink → Broker bytes
```

Keep thin framework-owned `Decoder<T>` and `Encoder<T>` traits; Serde itself is
not the framework's codec abstraction. JSON uses serde_json. Planned codecs
include Protobuf through prost, MessagePack through rmp-serde, and potentially
Avro. Kafka with Protobuf is a first-class target.

Select codecs through source/sink type parameters, inferring payload types from
the handler. Planned Kafka syntax:

```rust,ignore
KafkaSource::<Json>::new(source_config)
KafkaSink::<Protobuf>::new(sink_config)
KafkaSource::<Json, Utf8>::new(source_config)
```

The abbreviated Kafka codec applies to the value. The proposed parameter order
is `ValueCodec, KeyCodec = RawBytes`. Validate decode/encode compatibility at
registration. Do not add `with_codec` now; reconsider when a concrete stateful or
configured codec requires it.

Typed sources such as IterSource skip byte deserialization. Core contracts must
support typed and byte-based input without forcing an unnecessary conversion.

## Source and sink contracts

Use explicit receive states:

```rust
pub enum Receive<M> {
    Message(M),
    End,
}
```

End means no future messages exist and the runtime must not receive again. An
idle source waits; a poll timeout is not End. Do not overload `Option::None` with
stream lifecycle semantics. Receive failure and external shutdown are distinct
from normal input completion.

`Source::Message` implements `SourceMessage`, which owns decode and consuming
ACK. Do not require every source to expose `payload() -> &[u8]`.
Broker ACK behavior belongs to the adapter: Kafka completion/offset management,
SQS DeleteMessage, or NATS JetStream ACK.

`Sink<T>::prepare` produces its associated `Prepared` representation before
`publish`. Preserve broker metadata and null values rather than reducing every
sink to `publish(&[u8])`. Runtime retries reuse prepared output. The implemented
contracts are detailed in [architecture](architecture.md).

## Processing lifecycle and delivery semantics

```text
receive → decode → handler → resolve metadata → encode/prepare → publish → ACK
```

Never ACK input before all required output publishes succeed. Default processing
follows at-least-once ordering. Durable redelivery depends on source capabilities;
local debugging sources do not promise it. A crash after publish but before ACK
can duplicate output, and that behavior must be documented.

Exactly-once is supported only when a platform can atomically consume, transform,
and produce. Kafka-to-Kafka transactions are the first target:

```text
consume → handler → begin transaction → produce output
    → send consumed offsets to transaction → commit transaction
```

Pulsar may be considered later. Exactly-once is a processing capability, not
ordinary middleware. Unsupported source/sink pairs must be rejected at compile
time when practical or otherwise at startup.

## Error model

Handlers classify failures as Retry, Reject, or Fatal.

- **Retry:** invoke the handler again. The framework manages bounded attempts,
  exponential backoff, planned jitter, and observability. The handler decides
  whether retry is meaningful.
- **Reject:** a permanent, message-specific failure such as invalid input,
  unsupported payload, or a domain constraint. Publish to DLQ, then ACK and
  continue. Never silently drop the input.
- **Fatal:** processing cannot safely continue, for example invalid application
  configuration, a broken invariant, or a critical dependency failure. Do not
  ACK the failed input; report the failure and stop the subscription.

Do not infer that an unexpected ordinary error is transient. The current `?`
conversion defaults to Fatal; further error API refinement remains possible.

### Receive failures

The source adapter explicitly returns Retry or Fatal. Runtime does not guess.
Receive retry has its own policy and resets its consecutive failure count after
a message is received. SDK-internal reconnection belongs inside the adapter.

On Fatal or retry exhaustion, stop receiving, drain outstanding work with a
bounded timeout, finish publish/ACK only for successful work, and exit with an
error. Failed ACKs must not be reported as success. Receive failures are not
message rejections and must not go to DLQ.

### Publisher and DLQ failures

If a handler succeeds but publish fails, retry publication without rerunning the
handler. Exhaustion stops the subscription without ACK. Infrastructure publish
failures are not normally routed to DLQ.

For Reject, ACK only after DLQ publication succeeds. Retry failed DLQ publication;
on exhaustion, stop without ACK. Detailed policies for handler retry exhaustion,
decode/encode errors, and rejection without a DLQ are distinguished from current
prototype behavior in the [runtime guide](runtime.md).

## Concurrency, ordering, and backpressure

Concurrency belongs to SubscriptionConfig. Kafka should preserve partition
ordering by default: sequential processing within a partition, parallel processing
across partitions. Explicit unordered mode may process several messages from one
partition concurrently and exceed partition-count parallelism.

```rust,ignore
Ordering::Partition
Ordering::Unordered
```

In unordered mode, Kafka must only commit contiguous completed progress. If
10 and 12 are complete but 11 is still processing, do not commit past 11.

Bound in-flight messages with max_in_flight. A slow handler or sink must not cause
unbounded source consumption. Use adapter pause/resume capabilities when available.
Partition scheduling and commit management remain future broker work.

## Kafka rebalance

A partition can be revoked while messages are in flight. The adapter/runtime must
coordinate revocation, in-flight processing, cancellation, and safe offset commits.
Keep rebalance details out of ordinary handler APIs and inside the broker adapter
where possible. The exact cancellation strategy remains open.

## Completion and graceful shutdown

Stop new receives, drain or cancel outstanding work according to policy, publish
completed outputs, ACK only safely completed inputs, flush/close resources, and
exit. Treat SIGTERM as a first-class deployment concern for Kubernetes and ECS.

On Receive::End, drain the subscription and close resources before reporting
normal completion. End of input does not imply success if processing or cleanup
fails afterward.

One normal subscription completion leaves others running. App returns success
when all finish normally. By default, any subscription error requests graceful
shutdown of the others and App returns an error. Receive waits and retry backoff
must respond to shutdown. Detailed timeout and cancellation policies remain
subject to refinement, especially for future blocking handlers.

## Configuration

Keep settings with their owning components:

- KafkaSourceConfig: brokers, topic, and consumer-specific settings.
- KafkaSinkConfig: brokers, topic, and producer-specific settings.
- SubscriptionConfig: concurrency, ordering, max_in_flight, independent retry
  policies, shutdown/drain policy, and delivery semantics. Attach typed middleware
  and DLQ components to the subscription builder.

Avoid a giant framework configuration containing every broker's settings.

## Observability

Provide standard signals rather than a notification system. Slack or PagerDuty
integration belongs to external monitoring. Consider `tracing` spans shaped as:

```text
subscription
  └── message
       ├── decode
       ├── handler
       ├── encode
       ├── publish
       └── ack
```

Candidate metrics include messages_received_total, messages_processed_total,
messages_rejected_total, handler_errors_total, retries_total, publish_errors_total,
dlq_total, dlq_errors_total, processing_duration, handler_duration,
publish_duration, and in_flight.

Plan OpenTelemetry integration for connections to Prometheus, Datadog, Grafana,
CloudWatch, and similar systems. Cross-platform trace-context propagation still
needs design. These integrations are not currently implemented.

## Application API

Keep normal registration short:

```rust,ignore
let app = App::new()
    .subscribe(source_a, sink_a, handler_a)
    .subscription(
        Subscription::new(source_b, sink_b, handler_b)
            .name("orders-to-events")
            .concurrency(16)
            .max_in_flight(64)
            .retry(retry_policy)
            .middleware(metadata_mapping),
    );
app.run().await?;
```

`subscribe(source, sink, handler)` abbreviates registration of
`Subscription::new(source, sink, handler)`. Registration only constructs the app;
connection and reception begin in run. `retry` configures handler retry, while
receive retry is separate. Multiple subscriptions run in the same process.

Local development uses IterSource first, then StdinSource with JSON, and
StdoutSink with one JSON message per line. InMemorySink supports assertions.
Keep diagnostics on stderr. IterSource ends after all elements and StdinSource
at EOF; broker sources generally wait until shutdown. Local ACK is only a
completion marker and offers no durable redelivery guarantee.

## Design principles

- Keep simple handlers simple; reveal metadata, retry classification, and custom
  codecs only when needed.
- Support progression from plain values to broker records and explicit
  `Emit<Record>` results.
- Hide broker differences where their meanings align, without pretending distinct
  capabilities are equivalent.
- Prefer duplicates over silent data loss. Do not ACK ambiguous failures.
- Make capabilities explicit through types or startup validation.

## Module and crate structure

Use the [current module layout](architecture.md#module-layout) within one crate.
Source, message, sink, handler, and codec contracts are independent of adapter
implementations. Keep per-message processing and scheduling private. Broker
commit/ACK details stay in adapters. Export public APIs explicitly from lib.rs.

Future Kafka modules will cover source, sink, metadata, and transactions. NATS
and SQS will have their own adapters. Separate adapter and codec crates when SDK
dependencies or distribution require it, rather than splitting prematurely.

## Implementation order

Start with an end-to-end vertical slice, not Kafka transactions.

| Phase | Scope |
|---|---|
| 1 — Core runtime | Local source → typed handler → local sink; codecs, Emit, errors, concurrency, backpressure, shutdown |
| 2 — Kafka at-least-once | Kafka source → handler → Kafka sink with safe publish-before-ACK semantics |
| 3 — Kafka production features | Partition ordering, unordered processing, offsets, rebalance, DLQ, observability |
| 4 — Kafka exactly-once | Atomic consume-transform-produce using transactions |
| 5 — NATS JetStream | NATS source and sink |
| 6 — AWS SQS | SQS source and sink |

Phase 1 includes App registration, finite-input End and drain, independent receive
retry, application-wide error shutdown, and the foundation for typed metadata
middleware. Use local/in-memory components to verify processing semantics and
allow typed sources to skip decoding bytes. IterSource and StdoutSink come before
StdinSource. The first milestone is receive → decode → handler → encode → publish
→ ACK, with correct lifecycle behavior.

Kafka is the reference broker for consumer groups, ordering, concurrent
processing, offsets, rebalance, keys/headers, transactions, exactly-once, and
Protobuf. Blocking handler support is deferred.

## Open questions

1. Further refinement of Source, SourceMessage, and Sink generics, lifetimes, and error types.
2. Handler generic/trait ergonomics, including implicit versus explicit Emit registration.
3. Coexistence of explicit HandlerError classification and ordinary Rust errors through `?`.
4. Codec interfaces for additional formats and configured implementations.
5. Concrete input/output metadata types and record names.
6. Kafka rebalance cancellation strategy for in-flight work.
7. Compile-time versus startup validation of exactly-once capabilities.
8. Trace-context propagation across Kafka headers, SQS attributes, and NATS headers.
9. Typed metadata middleware composition and mapping error classification.
10. Shutdown deadlines and cancellation policy.
11. Adapter and codec crate boundaries.
12. Handling Kafka null values in plain-value handlers.
13. Decode/encode error classification and final handler retry exhaustion policy.
14. Blocking pool ownership (App or Subscription), worker/queue limits, startup, shutdown, panic handling, and cancellation.
15. Whether to isolate handler async execution from communication/control execution.

## Core philosophy

Application code should focus on `Input → Output`. The framework owns the
surrounding message-flow infrastructure without promising capabilities that its
underlying platform cannot provide.

**Let application code process events. Let beave.rs manage the flow.**
