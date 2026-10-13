# Architecture

An application registers subscriptions on an `App`. Each subscription connects
one `Source`, one handler, and one `Sink`: the source receives broker messages,
the handler turns each decoded input into outputs, and the sink publishes them.
The source message is acknowledged only after its outputs are published. Codecs
convert between bytes and typed values, and middleware runs around the handler.
The [runtime guide](runtime.md) describes processing behavior, and the
[adapter guide](adapters.md) the built-in sources and sinks.

## Trait boundaries

| Contract | Responsibility |
|---|---|
| `Source` | Receive an associated `Message: SourceMessage`; report end of input or receive failure; declare whether application shutdown stops receiving |
| `SourceMessage` | Own a delivery, decode its input, expose its undecoded form, acknowledge completion, and report its ordering key, revocation, and propagation fields |
| `Handler<Input>` | Transform typed input asynchronously; also implemented for async functions and closures |
| `Decoder<T>` / `Encoder<T>` | Convert serialization formats without broker operations |
| `Sink<T>` | Prepare an associated output representation, submit or publish it, report its completion, and close resources |

### Errors

Operations the crate performs return `beavers::Error`: `App::run` and
`run_until`, configuration `validate` methods, adapter and codec constructors
such as `HttpSource::new` and `Avro::new`, `ChannelSender::send`, and
dead-letter parsing such as `KafkaDeadLetter::from_record`. Its variants
distinguish invalid configuration (`Config`), malformed records
(`InvalidRecord`), closed handles (`Closed`), drain and close deadlines
(`Timeout`), other failed operations (`Failed`), and a stopped subscription
(`Subscription`), which carries the subscription's name and the cause.
`Display` describes one level; `{:#}` and `Debug` include every cause.

Traits an application implements, `Decoder`, `Encoder`, `Source`,
`SourceMessage`, `Sink`, and `TransactionalSink`, return `BoxError`, a boxed
`std::error::Error + Send + Sync`. An implementation propagates any error type
with `?`, including `beavers::Error`, `String`, and `&str`, and the runtime adds
context as the error moves up. Handlers return `HandlerError`, whose variants
hold a `BoxError`; see [errors and retries](runtime/errors.md).

### Source and message ownership

`Source::Message` can be an adapter-specific type. Adapters are not required to
use a shared byte buffer or boxed ACK callback. `Delivery<T>` is a convenience
implementation for already typed local input; `Delivery::with_raw` gives it a
raw form other than `()`.

A message can retain raw bytes and broker-specific information until processing
finishes. The runtime calls `decode` once and passes the decoded value to the
handler. `decode` must not publish or acknowledge. Dropping a message must never
acknowledge it. `raw` returns the undecoded delivery, including broker metadata,
as the adapter's `Raw` type, which implements `Debug` and `Serialize`. The
runtime calls it when a failure is routed to a dead-letter sink, and a sink
that keeps it for a later stage, such as a channel, calls it while preparing
output.

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
Sink stay asynchronous. See [blocking handlers](runtime/handlers.md#blocking-handlers).

See the [codec guide](codecs.md) for serialization implementations and payload bounds.

### Sink preparation and publication

`Sink<T>::prepare(T)` validates and encodes output without publishing it.
`Sink<T>::Prepared` is not restricted to bytes: a broker implementation can retain
keys, headers, and other publish fields in its own representation. The runtime
prepares a delivery's outputs with `Sink<T>::prepare_from(T, &delivery)`, which
calls `prepare` by default. A sink overrides it when an output keeps facts of
the delivery that the handler does not see, as a channel keeps the ordering key
and raw form for the next subscription.

The runtime maps and prepares all outputs before submitting any of them.
`Sink<T>::submit` returns once the sink accepts an output, with a `Completion`
that resolves at the acknowledgement boundary; the default publishes and is
already complete. The runtime frees the job's concurrency slot after submission
and acknowledges the input once every completion succeeds. Publish
retries reuse the same prepared value, without rerunning the handler, middleware,
or encoder. A successful `publish` means the sink's acknowledgement boundary has
been reached. It must not report success while required output confirmation is
still outstanding.

A sink can also implement `TransactionalSink<M, T>` for a source message type
`M` whose acknowledgement can join the sink's transactions. Its `commit`
publishes a delivery's prepared outputs and acknowledges the delivery
atomically, and must finish or abort the transaction even when its future is
dropped. `Subscription::transactional()` requires this implementation, so an
unsupported source and sink pair is rejected at compile time. Its
`verify_source` checks what types cannot express, such as whether the source
and the sink connect to the same cluster, and the runtime calls it with the
first delivery before processing it. See the
[runtime guide](runtime/transactions.md).

### Configuration ownership

`SubscriptionConfig` holds processing policy. Source and Sink connection settings
belong to their adapters. Codecs own serialization behavior. Broker-specific
metadata must not become a universal configuration or message struct.

Metadata mapping is typed subscription middleware rather than a shared
structure: each adapter may provide same-platform inheritance for its own record
and publish types, and other mappings are explicit application functions. See
the [runtime guide](runtime/middleware.md).

Future broker adapters implement these boundaries and can move into separate
crates when SDK dependencies require it. Further transaction support and
ordering capabilities still require the work described in the
[design plan](plan.md).
