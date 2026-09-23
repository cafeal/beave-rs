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
├── retry.rs               # Retry policy
├── shutdown.rs            # Cancellation and process signals
├── codec/
│   ├── mod.rs             # Decoder and Encoder contracts
│   └── json.rs            # JSON implementation
├── subscription/
│   ├── mod.rs             # Module declarations and public re-exports
│   ├── builder.rs         # Subscription type and builder API
│   ├── config.rs          # Runtime configuration and validation
│   ├── runtime.rs         # Private scheduling, draining, and cleanup
│   └── processing.rs      # Private per-message processing lifecycle
└── adapters/
    ├── mod.rs
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
| `Source` | Receive an associated `Message: SourceMessage`; report end of input or receive failure |
| `SourceMessage` | Own a delivery, decode its input, and acknowledge completion |
| `Handler<Input>` | Transform typed input asynchronously; also implemented for async functions and closures |
| `Decoder<T>` / `Encoder<T>` | Convert serialization formats without broker operations |
| `Sink<T>` | Prepare an associated output representation, publish it, and close resources |

### Source and message ownership

`Source::Message` can be an adapter-specific type. Adapters are not required to
use a shared byte buffer or boxed ACK callback. `Delivery<T>` is a convenience
implementation for already typed local input.

A message can retain raw bytes and broker-specific information until processing
finishes. The runtime calls `decode` once and passes the decoded value to the
handler. `decode` must not publish or acknowledge. Dropping a message must never
acknowledge it.

ACK consumes the message. Its adapter owns safe broker completion behavior,
including offset ordering and assignment validity where applicable. A custom
`Source::receive` must be cancellation-safe: dropping its future must not silently
lose a delivery.

### Handler

`Handler<Input>` exposes an associated output and returns a future. The runtime
normalizes results into `Emit` internally. Retry and middleware currently require
inputs to implement `Clone + Send + Sync`.

The future-returning contract can later accommodate a `blocking(...)` wrapper
that submits synchronous work to a worker pool and asynchronously waits for its
result. This does not require changing Source or Sink to synchronous APIs. The
wrapper and pool are [planned, not implemented](plan.md#handler-execution-model).

See the [codec guide](codecs.md) for serialization implementations and payload bounds.

### Sink preparation and publication

`Sink<T>::prepare(T)` validates and encodes output without publishing it.
`Sink<T>::Prepared` is not restricted to bytes: a broker implementation can retain
keys, headers, and other publish fields in its own representation.

The runtime maps and prepares all outputs before publishing any of them. Publish
retries reuse the same prepared value, without rerunning the handler, middleware,
or encoder. A successful `publish` means the sink's acknowledgement boundary has
been reached. It must not report success while required output confirmation is
still outstanding.

### Configuration ownership

`SubscriptionConfig` holds processing policy. Source and Sink connection settings
belong to their adapters. Codecs own serialization behavior. Broker-specific
metadata must not become a universal configuration or message struct.

Future broker adapters implement these boundaries and can move into separate
crates when SDK dependencies require it. Concrete metadata routing, transaction
support, and ordering capabilities still require the work described in the
[design plan](plan.md).
