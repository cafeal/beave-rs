# Errors and retries

`beavers::Result<T>` uses `HandlerError`, whose variants hold a `BoxError`.
Ordinary errors propagated with `?`, from any `std::error::Error + Send + Sync`
type, `String`, `&str`, or `anyhow::Error`, become `Reject`: the handler is not
retried, and the input is dead-lettered by default. The runtime cannot tell a
transient failure from a deterministic one, so the handler requests other
outcomes at the call site with the `Classify` extension trait. `.reject()?`
states the default explicitly, `.retry()?` reruns the handler under its retry
policy and dead-letters the input once that is
exhausted; `.fatal()?` stops the subscription:

```rust,ignore
use beavers::{Classify, Result};

async fn handle(order: Order) -> Result<Output> {
    validate(&order).reject()?; // invalid input: dead-letter now (same as `?`)
    let user = db.find_user(order.user_id).await.retry()?; // transient: retry, then dead-letter
    Ok(build_output(order, user))
}
```

## Error policy

`ErrorPolicy` decides what happens to a delivery after one of five routable
failures, identified by `FailureKind`:

| `FailureKind` | Cause | Default action |
|---|---|---|
| `Decode` | `SourceMessage::decode` failed | `Stop` |
| `Rejected` | The handler or a middleware returned `Reject`, including errors propagated with `?` | `DeadLetter` |
| `RetryExhausted` | The handler returned `Retry` on its final permitted attempt | `DeadLetter` |
| `Encode` | `Sink::prepare` failed for an emitted output | `Stop` |
| `PublishRejected` | The sink's destination refused an output permanently | `DeadLetter` |

Each failure maps to one `FailureAction`:

- `Stop` returns an error and leaves the delivery unacknowledged, so a durable
  broker can redeliver it after restart.
- `DeadLetter` publishes a `DeadLetter` envelope to the dead-letter sink and
  then acknowledges the delivery.
- `Discard` acknowledges the delivery without publishing anything. It is an
  explicit choice to lose that delivery.

Rejections, exhausted handler retries, and rejected publications without a
configured dead-letter sink stop without ACK: the input could not be processed
and nothing can receive it. `DeadLetter` for decode or encode failures requires a dead-letter
sink and fails validation otherwise. `ErrorPolicy::dead_letter_all()` routes
every kind to the dead-letter sink.

Encoding happens for all emitted outputs before any publication, so an `Encode`
failure never follows a partial publish; dead-lettering the input after it does
not duplicate outputs.

A sink reports a permanent refusal, such as an HTTP `4xx` response, by marking
its publish, submit, or commit error with `PublishRejected::wrap`. The runtime
does not retry a rejected output and submits no later outputs of the delivery.
Outputs submitted before it stay published, and their completions are awaited
before the delivery is routed, so a `DeadLetter` or `Discard` action
acknowledges it only after they reached the sink's acknowledgement boundary.
Errors without the marker are retried under `publish_retry`, and an exhausted
retry stops the subscription. A failed completion is never routed. A
[transactional subscription](transactions.md#batches) cannot route a rejected commit either.

## Dead letters

`Subscription::dlq(sink)` accepts a `Sink<DeadLetter<Input, Raw>>`, where `Raw`
is the source message's undecoded form (`SourceRaw<S>`).
`Subscription::dlq_with(sink, convert)` converts each envelope into the sink's
own output type first, for example to forward the original payload to a broker
topic with failure details as headers. Handlers never see dead letters.

| Field | Meaning |
|---|---|
| `subscription` | Name of the failing subscription |
| `failure` | The `FailureKind` |
| `error` | Error message including its context chain |
| `attempts` | Handler attempts made; zero for decode failures |
| `input` | Decoded handler input; `None` after a decode failure |
| `raw` | The delivery as received, with its broker metadata |

`SourceMessage::Raw` defines `raw` per source, so broker metadata keeps its
native shape instead of passing through a universal structure:

| Source | `Raw` |
|---|---|
| Kafka | `KafkaRecord<Vec<u8>>`: value bytes, key, headers, and delivery metadata |
| Pulsar | `PulsarRecord<Vec<u8>>`: payload bytes, key, properties, event time, and delivery metadata |
| RabbitMQ | `RabbitMqRecord<Vec<u8>>`: body bytes, headers, properties, and delivery metadata |
| SQS | `SqsRecord<Vec<u8>>`: body bytes, message attributes, and message metadata |
| Pub/Sub | `PubSubRecord<Vec<u8>>`: data bytes, attributes, ordering key, and message metadata |
| Azure Service Bus | `ServiceBusRecord<Vec<u8>>`: body bytes, application properties, message properties, and message metadata |
| Stdin | `Vec<u8>`: the received line |
| `Delivery` (`IterSource`) | `()`: input is already typed |
| Channel | `ChannelRaw`: the raw form of the upstream delivery that produced the value |

`SourceMessage::Raw` implements `Serialize`, so `DeadLetter` implements it
whenever its input does and a JSON sink can publish it directly.

Conversion and preparation run once. Publication then retries under the
`dlq_retry` policy. A conversion, preparation, or exhausted publication failure
stops without ACK; dead-letter failures are never routed again.

## Forwarding dead letters to a broker topic

`KafkaPublish::from_dead_letter`, `PulsarPublish::from_dead_letter`,
`RabbitMqPublish::from_dead_letter`, `SqsPublish::from_dead_letter`,
`PubSubPublish::from_dead_letter`, and `ServiceBusPublish::from_dead_letter`
convert a dead letter into a record for a
dead-letter topic or queue of the same platform. The record keeps the original
key, value bytes, and headers or properties (and the Pulsar event time), so the
payload can be inspected with ordinary broker tools and decoded by the same
codec as the original topic. Failure details are added as text headers (Kafka),
properties (Pulsar), string headers (RabbitMQ), string application properties
(Azure Service Bus), or attributes (Pub/Sub, whose 1024-byte value limit cuts a
longer error message). SQS accepts at most 10
message attributes, so the SQS adapter writes the same names and values as one
JSON object into the `beavers-dlq-details` string attribute:

| Name | Value |
|---|---|
| `beavers-dlq-subscription` | Subscription that dead-lettered the payload |
| `beavers-dlq-failure` | `FailureKind` name, such as `rejected` |
| `beavers-dlq-error` | Error message including its context chain |
| `beavers-dlq-attempts` | Handler attempts made |
| `beavers-dlq-count` | Times the payload has been dead-lettered, including this one |
| `beavers-dlq-origin-topic` | Kafka or Pulsar topic the payload was first received from |
| `beavers-dlq-origin-partition`, `-offset`, `-timestamp` | Kafka location and record timestamp of the first receipt |
| `beavers-dlq-origin-message-id`, `-publish-time` | Pulsar message ID (`ledger:entry:partition:batch`) and publish time of the first receipt |
| `beavers-dlq-origin-queue`, `-exchange`, `-routing-key` | RabbitMQ queue of the first receipt, and the exchange and routing key it was published with |
| `beavers-dlq-origin-queue-url`, `-message-id` | SQS queue URL and message ID of the first receipt |
| `beavers-dlq-origin-subscription`, `-message-id` | Pub/Sub subscription and message ID of the first receipt |
| `beavers-dlq-origin-entity-path`, `-sequence-number` | Azure Service Bus entity path and sequence number of the first receipt |

The sink must publish raw bytes:

```rust,ignore
let dead_letters = kafka.sink::<RawBytes>("orders-dlq");

Subscription::new("orders", source, sink, handle)
    .dlq_with(dead_letters, |dead_letter| Ok(KafkaPublish::from_dead_letter(dead_letter)));
```

When the dead-lettered record was itself received from a dead-letter topic, the
origin headers are kept and the count is incremented, while the other details
describe the latest failure. Previous dead-letter headers that are malformed
are replaced as if the payload had never been dead-lettered.
`KafkaDeadLetter::from_record`, `PulsarDeadLetter::from_record`,
`RabbitMqDeadLetter::from_record`, `SqsDeadLetter::from_record`,
`PubSubDeadLetter::from_record`, and `ServiceBusDeadLetter::from_record` read
the details back from a received record
and return `None` for a record without them. The inheritance middleware of each
adapter never copies names starting with
`DEAD_LETTER_HEADER_PREFIX` into outputs.

The dead-letter record's own timestamp or publish time is when it was
dead-lettered. A subscription downstream of a [channel](../adapters/channel.md)
receives a `ChannelRaw`; `DeadLetter::try_map_raw` reads it as the upstream
record before conversion.

## Reprocessing dead letters

beavers has no dedicated redrive command. A dead-letter topic is an ordinary
topic, so dead letters are reprocessed by a subscription whose source reads the
dead-letter topic and whose handler is the original one, or a wrapper around
it that inspects `KafkaDeadLetter::from_record` first, for example to stop
after a limit on `details.count`. The
[dead-letter handling guide](../dead-letter-handling.md) describes how to find
the cause of dead letters, how to reprocess, park, or drop them on log and
queue brokers, and complete examples.

## Other failures

| Failure | Behavior |
|---|---|
| Handler `Retry` | Retry the handler with cloned input, then apply `RetryExhausted` routing |
| Handler `Fatal` | Stop without ACK |
| Receive `Retry` | Back off and retry receive; reset the failure count after receiving a message |
| Receive `Fatal` or exhausted retry | Stop receiving, drain outstanding work, return an error |
| Value-only input unavailable (tombstone) | `Rejected` routing without invoking the handler |
| Middleware `Reject` | `Rejected` routing with the decoded input; publish no outputs |
| Middleware `Retry` or `Fatal` | Stop without ACK; never rerun the handler or middleware |
| Publish failure | Retry the prepared output; never rerun handler, middleware, or encoding |
| Publish rejected by the destination | `PublishRejected` routing without retry |
| Exhausted publish | Stop without ACK; infrastructure failures are never dead-lettered |
| ACK failure | Return an error; do not claim successful completion |

Publication, dead-letter publication, and ACK results are never assumed. An
interrupted or failed attempt is not treated as success.

Sources and sinks built from one broker handle share its connection. A lost
shared connection therefore affects every subscription that uses it at once:
each one sees its own receive, publish, or ACK failure and handles it as
above, so a subscription still stops on its own fatal or exhausted failure.
