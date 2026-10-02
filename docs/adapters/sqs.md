# SQS adapter

The optional `sqs` feature provides typed Amazon SQS source and sink adapters
backed by the AWS SDK for Rust (`aws-sdk-sqs`).

```toml
beavers = { version = "0.1", features = ["sqs"] }
```

## Configuration

`SqsSourceConfig` and `SqsSinkConfig` take the URL of one queue. A URL ending in
`.fifo` names a FIFO queue. `region`, `endpoint_url`, and `credentials` are
optional: unset, the SDK's default chains resolve the region and credentials
from the environment, shared configuration files, web identity, or container
and instance metadata. Set `endpoint_url` to use an SQS-compatible emulator
such as ElasticMQ or LocalStack. `validate` checks the values without contacting
AWS, and both adapters create their SDK client on first use. The `Debug` output
of `SqsCredentials` hides the secret key and session token.

```rust
use beavers::{Utf8, adapters::sqs::{SqsPublish, SqsSink, SqsSinkConfig, SqsSource, SqsSourceConfig}};

let source = SqsSource::<Utf8, String>::new(SqsSourceConfig::new(
    "https://sqs.eu-west-1.amazonaws.com/123456789012/orders",
));
let sink = SqsSink::<Utf8, String>::new(SqsSinkConfig::new(
    "https://sqs.eu-west-1.amazonaws.com/123456789012/processed-orders.fifo",
));

let output = SqsPublish::new("created".to_owned()).with_message_group_id("customer-7");
```

The adapters create no queues and set no queue attributes such as a redrive
policy. Create them with infrastructure tooling before the subscription starts.

The source receives with long polling. `SqsSourceConfig::max_messages` (1 to
10, default 10) is the batch size of each `ReceiveMessage` call and `wait_time`
(at most 20 seconds, the default) how long a call waits for messages.
`visibility_timeout`, 30 seconds by default, is requested with every receive and
overrides the queue's own default.

## Records

`SqsRecord<T>` is the decoded input and `SqsPublish<T>` the output. The codec
applies to the message body. SQS has no null body, so the value is always
present and SQS records support no tombstones. SQS bodies are text: `prepare`
rejects an encoded value that is not UTF-8, so binary codecs such as Protobuf
and Avro need a text representation before they can be sent.

`attributes` holds the message attributes as `SqsAttributeValue::String`,
`Number` in its decimal text form, or `Binary`. A custom type label such as
`Number.float` is read as its base type and not published again. SQS accepts at
most 10 attributes per message.

`SqsMetadata` contains read-only facts about a received message: the queue URL,
message ID, receive count, sent and first-receive timestamps, and for FIFO
queues the message group, deduplication ID, and sequence number. `SqsPublish`
has none of them except the FIFO message group and deduplication ID, which the
output sets itself, and an optional delivery `delay`.

The raw form of an `SqsMessage` is an `SqsRecord<Vec<u8>>` with the undecoded
body. Dead letters carry it, so the original body, attributes, and metadata
survive even when decoding fails.

## Acknowledgements and ordering

A received message stays invisible to other consumers for the visibility
timeout. The source keeps every message it has received and not yet deleted
invisible: a background task extends a message's visibility with
`ChangeMessageVisibilityBatch` once a third of its timeout has passed, until it
is acknowledged or released. SQS limits visibility to 12 hours from the receipt;
a message held longer becomes visible again.

A successful ACK deletes the message with `DeleteMessage`, retried under
`SqsSourceConfig::ack_retry`. Dropping an `SqsMessage` without acknowledging it,
or closing the source, changes the visibility of its remaining messages to zero,
so another receive takes them at once with an incremented receive count.

A receipt handle is valid only for the receipt that returned it. When an
extension fails or arrives too late, SQS may already have handed the message to
another consumer, so the adapter cancels that delivery's revocation token. The
runtime abandons a revoked delivery without ACK or subscription failure. An
acknowledgement that finds its visibility expired, or that SQS rejects as an
invalid receipt handle, also revokes the delivery and fails.

A standard queue delivers messages in no particular order, so its deliveries
have no ordering key and run in parallel up to the subscription's
`concurrency`. A FIFO queue delivers each message group in order; each delivery's
ordering key is its message group, so `ProcessingOrder::PerKey` processes one
message per group at a time while groups run in parallel.

`ReceiveMessage` runs as a task the source keeps: dropping a pending `receive`
leaves the call running, and the next `receive` takes its messages.

## Publication

`prepare` encodes the value and converts the attributes once, so publish
retries reuse them. It requires a message group ID for a FIFO queue, rejects a
per-message delay there because FIFO queues accept only a queue-wide delay, and
limits the delay to 15 minutes elsewhere. A FIFO queue without content-based
deduplication also needs `deduplication_id`.

`SqsSink::publish` sends the message with `SendMessage` and succeeds once SQS
has stored it. The SDK retries throttling and transient errors itself.
Parameter errors such as an invalid attribute fail as `PublishRejected`, which
the subscription's error policy can dead-letter instead of retrying. The sink
keeps no pending messages, so `close` only stops new publications.

## Metadata inheritance

Register `SqsInherit` on an SQS-to-SQS subscription to forward the received
message attributes:

```rust,ignore
use beavers::adapters::sqs::SqsInherit;

Subscription::new("orders", sqs_source, sqs_sink, handler)
    .middleware(SqsInherit::new().with_message_group())
```

A received attribute is added only when the output does not set that name, and
dead-letter attributes starting with `beavers-dlq-` are never inherited.
`with_message_group()` also forwards the FIFO message group when the output
leaves it `None`, for pipelines between FIFO queues. The message ID,
deduplication ID, and delay are never inherited. `without_attributes()`
disables attribute inheritance.

`Subscription::forward` applies `SqsInherit::new()` automatically for a
value-only handler between an SQS source and sink; register
`SqsInherit::new().with_message_group()` explicitly to forward groups. See the
[runtime guide](../runtime.md#same-platform-forwarding). Register
`TraceContext` after `SqsInherit` to replace inherited trace-context
attributes; the source reads trace context from string attributes.

## Dead letters

`SqsPublish::from_dead_letter` forwards a dead letter with its original body,
attributes, and FIFO message group. Because SQS accepts at most 10 attributes,
the failure details and the URL and message ID of the first receipt are written
as one JSON object into the `beavers-dlq-details` string attribute; a message
that already has 10 attributes is rejected by SQS. `SqsDeadLetter::from_record`
reads them back; see
[forwarding dead letters](../runtime.md#forwarding-dead-letters-to-a-broker-topic).

A queue's redrive policy moves a message to its dead-letter queue after
`maxReceiveCount` receives without deletion. The subscription's error policy
decides what happens to a failed delivery first, so the redrive policy applies
to messages that are received again and again without being acknowledged, for
example because they crash the process every time. Keep `maxReceiveCount` high
enough that the releases of a source's shutdowns do not exhaust it.

## Delivery guarantees

A source ACK and a sink publication are separate requests, so a failure between
them, or a visibility timeout that expires before the deletion, can produce a
duplicate. Standard queues may also deliver a message more than once. SQS
pipelines are at-least-once; FIFO deduplication applies only to messages sent
within its five-minute window.
