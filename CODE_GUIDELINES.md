# Code Guidelines

These guidelines define the repository's implementation standards. They apply
to contributors and automated coding agents.

## Module design

The project uses one crate. Modules separate public contracts from transport
implementations and runtime internals. Split crates when SDK dependencies or
distribution requirements justify it; do not create empty modules for
unimplemented brokers.

```text
src/
├── lib.rs                 # Public modules and explicit re-exports
├── app.rs                 # Registration, supervision, and application shutdown
├── source.rs              # Source, Receive, ReceiveError, and SourceItem
├── message.rs             # SourceMessage and Delivery ownership
├── sink.rs                # Prepare, publish, and close contracts
├── handler.rs             # Handler, Emit, HandlerError, and Result
├── forward.rs             # Value-only handlers for same-platform sources and sinks
├── middleware.rs          # Middleware, Flow, and MapMetadata around the handler
├── tombstone.rs           # Tombstones middleware for null-value records
├── transaction.rs         # TransactionalSink for atomic publish and acknowledge
├── error.rs               # Error reported by the crate and BoxError
├── blocking.rs            # Synchronous handlers on a bounded worker pool
├── error_policy.rs        # FailureKind, FailureAction, and ErrorPolicy
├── dead_letter.rs         # DeadLetter envelope
├── distribution.rs        # Distribution, SourceOrder, and Parallel
├── retry.rs               # Retry policy and jitter
├── propagation.rs         # Trace-context carrier contract for outputs
├── telemetry.rs           # OpenTelemetry propagation (`opentelemetry` feature)
├── shutdown.rs            # Cancellation and process signals
├── state/
│   ├── mod.rs             # Module declarations and public re-exports
│   ├── store.rs           # StateStore, Begin, and StateContext
│   ├── handler.rs         # StatefulHandler and stateful()
│   ├── key.rs             # StateKey and MessageKey
│   ├── value.rs           # State<T>, one stored value per key
│   └── memory.rs          # InMemoryState
├── health/
│   ├── mod.rs             # Module declarations and public re-exports
│   ├── state.rs           # Health handle, reports, and per-subscription state
│   └── server.rs          # Liveness and readiness probes (`health` feature)
├── testing/               # Test sources for fabricated records (`testing` feature)
├── codec/
│   ├── mod.rs             # Codec, Decoder, and Encoder contracts
│   ├── json.rs            # JSON codec
│   ├── raw_bytes.rs       # RawBytes codec
│   ├── utf8.rs            # Utf8 codec
│   ├── avro.rs            # Avro codec (`avro` feature)
│   ├── protobuf.rs        # Protobuf codec (`protobuf` feature)
│   ├── msgpack.rs         # MessagePack codec (`msgpack` feature)
│   ├── cbor.rs            # CBOR codec (`cbor` feature)
│   ├── base64.rs          # Base64 wrapper (`base64` feature)
│   ├── gzip.rs            # Gzip wrapper (`gzip` feature)
│   ├── zstd.rs            # Zstandard wrapper (`zstd` feature)
│   ├── bounded.rs         # Bounded decompression shared by the wrappers
│   └── typed.rs           # Shared impls for codecs generic over their value type
├── subscription/
│   ├── mod.rs             # Module declarations and public re-exports
│   ├── builder.rs         # Subscription type and builder API
│   ├── handler.rs         # Handler shapes: IntoHandler, ByRecord, ByValue, and state markers
│   ├── config.rs          # Runtime configuration and validation
│   ├── runtime.rs         # Private receive loop, draining, and cleanup
│   ├── decoding.rs        # Private parallel decoding, finished in receive order
│   ├── scheduler.rs       # Private distribution lanes and per-key flow control
│   ├── instruments.rs     # Private per-subscription metric handles
│   ├── hooks.rs           # Private object-safe form of stored middleware
│   ├── processing.rs      # Private per-message processing lifecycle
│   ├── completion.rs      # Private prepare, publish, and acknowledge stage
│   └── transaction.rs     # Private batching of deliveries into sink transactions
└── adapters/
    ├── mod.rs
    ├── channel/           # Chained subscriptions with deferred upstream ACK
    ├── kafka/, pulsar/,   # Broker adapters (optional features), each with
    │   rabbitmq/, sqs/,   #   config, record, source, and sink modules
    │   pubsub/, servicebus/,
    │   http/
    ├── pending.rs         # Private bound on unconfirmed broker publications
    ├── iter.rs
    ├── memory.rs
    ├── stdin.rs
    └── stdout.rs
```

Core contracts do not depend on concrete adapters. Internal code imports the
module that owns a responsibility; root re-exports shorten application imports.
The scheduler and per-message processing implementation remain private.

- Keep `mod.rs` focused on module declarations and public re-exports.
- Put source, sink, configuration, record, and protocol-specific state in
  separate files once an adapter contains more than one of those concerns.
- Prefer small private modules with explicit public exports over a large public
  module containing unrelated implementation details.
- Do not extract abstractions merely because two broker SDKs use similar words.
  Share a component only when its semantics and lifecycle are actually the same.

Broker adapters should normally use this layout:

```text
src/adapters/<broker>/
  mod.rs
  config.rs
  record.rs
  source.rs
  sink.rs
```

Additional private modules such as `progress.rs` or `consumer_task.rs` are
appropriate when they isolate a substantial state machine or lifecycle.

## Public adapter API

- Expose broker adapters below `beavers::adapters::<broker>`.
- Use `Source`, `SourceMessage`, and `Sink` as the common runtime boundary.
- Represent a received broker record separately from a record prepared for
  publication. Received records may contain immutable delivery metadata;
  publish records contain only user-controlled output fields.
- Define `SourceMessage::Raw` as the adapter's received record with the payload
  left undecoded, such as `KafkaRecord<Vec<u8>>`, so dead letters keep broker
  metadata even when decoding fails.
- Make broker data models explicit in handler types. Do not silently translate
  keys, headers, properties, partitions, timestamps, or other metadata between
  platforms.
- Use `SourceName<C>` and `SinkName<C>` consistently. The codec names the
  payload type through `Codec::Value`, as in `KafkaSink<Json<Order>>`.
- Provide `new(config)` when `C: Default` and
  `with_codec(config, codec)` for configured or stateful codecs.
- Keep codec work in `decode` and `prepare`. A publish retry must reuse prepared
  bytes rather than encode the value again.

## Configuration and lifecycle

- Configuration types provide `new` for required values and `validate` for
  local validation that performs no network access.
- Construct broker clients lazily on the first `receive` or `publish`.
- Treat invalid configuration and malformed broker metadata as fatal errors.
  Report transient receive failures as retryable errors.
- A successful sink publication means the broker or local transport has
  accepted the prepared output according to that adapter's documented contract.
- ACK only after all required output publication succeeds. Dropping a delivery
  must not acknowledge it.
- Make `close` safe to call more than once and ensure clones share closure state
  where the sink itself is cloneable.
- Preserve cancellation safety at every async boundary. Document any SDK
  operation whose cancellation behavior affects delivery guarantees.

## Broker semantics

- Keep offset tracking, rebalances, acknowledgements, transactions, routing,
  and reconnect behavior inside the owning adapter.
- Preserve nullable payloads when the broker supports them; do not assign
  sentinel meanings to empty bytes, empty strings, or `None`.
- Do not claim exactly-once processing unless publication and acknowledgement
  are part of one broker transaction implemented by the adapter.
- Avoid implicit metadata inheritance. A middleware or explicit conversion may
  map metadata when the application requests it.

## Readability and imports

- Import commonly used standard-library and dependency types at the top of the
  module instead of repeating inline paths such as `std::future::Future` or
  `std::collections::HashMap` throughout declarations and implementations.
- Use the unqualified prelude `Result` when it is unambiguous. When a domain
  result alias and the standard result type are both needed, give one a clear
  local alias such as `StdResult` rather than repeating its full path.
- Keep an explicit path when it communicates ownership or avoids ambiguity
  better than an import. Do not introduce imports solely to shorten a
  one-off path whose namespace is useful context.
- Prefer focused imports over wildcard imports in implementation and test code.

## Quality and documentation

- Add focused unit tests for state machines and contract tests for public API
  behavior. Keep live broker tests ignored by default and runnable explicitly.
- Split a large integration-test target into feature-oriented modules with a
  small entry file. Put reusable mocks and helpers in a dedicated `fixtures`
  module instead of duplicating them or mixing them with test cases.
- Run formatting, the default test suite, all-feature tests, Clippy with
  warnings denied, and documentation generation before considering a change
  complete.
- Keep public documentation in English and update it in the same change as an
  API or behavior change.
- Documentation and comments describe current contracts, rationale that remains
  relevant, and real limitations. Do not leave review history, temporary notes,
  agent activity, or implementation worklogs in the repository.
- Give each broker adapter guide in `docs/adapters/` these sections, in this
  order, omitting those that do not apply: Configuration, Records,
  Acknowledgements and ordering, Publication, Metadata inheritance, Tombstones,
  Dead letters, and Delivery guarantees. Adapter-specific sections, such as
  Kafka transactions, follow them.
- Keep `docs/plan.md` focused on future work and unresolved decisions. Move
  completed designs into the durable documentation for the implemented API.
