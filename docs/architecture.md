# Architecture

The project currently uses one crate. Modules separate public contracts from
transport implementations and runtime internals. Split crates when SDK
dependencies or distribution requirements justify it; do not create empty
modules for unimplemented brokers.

## Module layout

```text
src/
├── lib.rs                 # Public modules and explicit re-exports
├── app.rs                 # Registration, supervision, and application shutdown
├── source.rs              # Source, Receive, ReceiveError, and SourceItem
├── message.rs             # SourceMessage and Delivery ownership
├── sink.rs                # Prepare, publish, and close contracts
├── handler.rs             # Handler, Emit, HandlerError, and Result
├── blocking.rs            # Synchronous handlers on a bounded worker pool
├── error_policy.rs        # FailureKind, FailureAction, and ErrorPolicy
├── dead_letter.rs         # DeadLetter envelope
├── retry.rs               # Retry policy and jitter
├── propagation.rs         # Trace-context carrier contract for outputs
├── telemetry.rs           # OpenTelemetry propagation (`opentelemetry` feature)
├── shutdown.rs            # Cancellation and process signals
├── codec/
│   ├── mod.rs             # Decoder and Encoder contracts
│   └── json.rs            # JSON implementation
├── subscription/
│   ├── mod.rs             # Module declarations and public re-exports
│   ├── builder.rs         # Subscription type and builder API
│   ├── config.rs          # Runtime configuration and validation
│   ├── runtime.rs         # Private receive loop, draining, and cleanup
│   ├── scheduler.rs       # Private ordering-key queues
│   ├── instruments.rs     # Private per-subscription metric handles
│   └── processing.rs      # Private per-message processing lifecycle
└── adapters/
    ├── mod.rs
    ├── channel.rs         # Chained subscriptions with deferred upstream ACK
    ├── iter.rs
    ├── memory.rs
    ├── stdin.rs
    └── stdout.rs
```

See the [adapter guide](adapters.md) for the built-in implementations.

Core contracts do not depend on concrete adapters. Internal code imports the
module that owns a responsibility; root re-exports shorten application imports.
The scheduler and per-message processing implementation remain private.

## Trait boundaries

| Contract | Responsibility |
|---|---|
| `Source` | Receive an associated `Message: SourceMessage`; report end of input or receive failure; declare whether application shutdown stops receiving |
| `SourceMessage` | Own a delivery, decode its input, expose its undecoded form, acknowledge completion, and report its ordering key, revocation, and propagation fields |
| `Handler<Input>` | Transform typed input asynchronously; also implemented for async functions and closures |
| `Decoder<T>` / `Encoder<T>` | Convert serialization formats without broker operations |
| `Sink<T>` | Prepare an associated output representation, submit or publish it, report its completion, and close resources |

### Source and message ownership

`Source::Message` can be an adapter-specific type. Adapters are not required to
use a shared byte buffer or boxed ACK callback. `Delivery<T>` is a convenience
implementation for already typed local input.

A message can retain raw bytes and broker-specific information until processing
finishes. The runtime calls `decode` once and passes the decoded value to the
handler. `decode` must not publish or acknowledge. Dropping a message must never
acknowledge it. `raw` returns the undecoded delivery, including broker metadata,
as the adapter's `Raw` type. The runtime calls it only when a failure is routed
to a dead-letter sink.

ACK consumes the message. Its adapter owns safe broker completion behavior,
including offset ordering and assignment validity where applicable. A message
can expose an `OrderingKey` for its ordered delivery scope and a revocation
`CancellationToken` that the adapter cancels when it loses ownership. The
runtime uses these for [scheduling and revocation](runtime.md#ordering-and-backpressure)
without exposing broker rebalances to handlers. A custom
`Source::receive` must be cancellation-safe: dropping its future must not silently
lose a delivery.

### Handler

`Handler<Input>` exposes an associated output and returns a future. The runtime
normalizes results into `Emit` internally. Retry and middleware currently require
inputs to implement `Clone + Send + Sync`.

`blocking(...)` wraps a synchronous function in a `Handler` that submits each
call to a `BlockingPool` and asynchronously waits for its result, so Source and
Sink stay asynchronous. See [blocking handlers](runtime.md#blocking-handlers).

See the [codec guide](codecs.md) for serialization implementations and payload bounds.

### Sink preparation and publication

`Sink<T>::prepare(T)` validates and encodes output without publishing it.
`Sink<T>::Prepared` is not restricted to bytes: a broker implementation can retain
keys, headers, and other publish fields in its own representation.

The runtime maps and prepares all outputs before submitting any of them.
`Sink<T>::submit` returns once the sink accepts an output, with a `Completion`
that resolves at the acknowledgement boundary; the default publishes and is
already complete. The runtime frees the job's concurrency slot after submission
and acknowledges the input once every completion succeeds. Publish
retries reuse the same prepared value, without rerunning the handler, middleware,
or encoder. A successful `publish` means the sink's acknowledgement boundary has
been reached. It must not report success while required output confirmation is
still outstanding.

### Configuration ownership

`SubscriptionConfig` holds processing policy. Source and Sink connection settings
belong to their adapters. Codecs own serialization behavior. Broker-specific
metadata must not become a universal configuration or message struct.

Metadata mapping is typed subscription middleware rather than a shared
structure: each adapter may provide same-platform inheritance for its own record
and publish types, and other mappings are explicit application functions. See
the [runtime guide](runtime.md#middleware).

Future broker adapters implement these boundaries and can move into separate
crates when SDK dependencies require it. Transaction support and ordering
capabilities still require the work described in the
[design plan](plan.md).
