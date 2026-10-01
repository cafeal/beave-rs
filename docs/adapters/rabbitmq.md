# RabbitMQ adapter

The optional `rabbitmq` feature provides typed RabbitMQ source and sink adapters
for AMQP 0-9-1, backed by the `lapin` 4 client.

```toml
beavers = { version = "0.1", features = ["rabbitmq"] }
```

## Configuration

`RabbitMqSourceConfig` takes a broker URI and the queue to consume;
`RabbitMqSinkConfig` takes a broker URI, the exchange every output is published
to, and the routing key of outputs that do not set their own. The empty
exchange name is RabbitMQ's default exchange, which routes a message to the
queue named by its routing key. `validate` checks the values without contacting
the broker, and both adapters connect on first use. The `Debug` output of both
configurations hides the URI's password.

```rust
use beavers::{Utf8, adapters::rabbitmq::{
    RabbitMqPublish, RabbitMqSink, RabbitMqSinkConfig, RabbitMqSource, RabbitMqSourceConfig,
}};

let source = RabbitMqSource::<Utf8, String>::new(RabbitMqSourceConfig::new(
    "amqp://guest:guest@localhost:5672/%2f",
    "orders",
));
let sink = RabbitMqSink::<Utf8, String>::new(RabbitMqSinkConfig::new(
    "amqp://guest:guest@localhost:5672/%2f",
    "",
    "processed-orders",
));

let output = RabbitMqPublish::new("created".to_owned()).with_routing_key("audit");
```

The adapters declare no exchanges, queues, or bindings. Create the topology
with broker definitions, the management tools, or application code before the
subscription starts; a source whose queue does not exist fails its receive and
retries under the subscription's `receive_retry`.

`RabbitMqSourceConfig::prefetch` is the `basic.qos` prefetch count, 100 by
default: the number of unacknowledged messages the broker sends ahead. It bounds
the source's buffered and in-flight deliveries together, so keep it at least as
large as the subscription's `max_in_flight`. Each connection is named
`beavers source <queue>` or `beavers sink <exchange>` in the management tools.

## Records

`RabbitMqRecord<T>` is the decoded input and `RabbitMqPublish<T>` the output.
The codec applies to the message body. AMQP has no null body, so the value is
always present and RabbitMQ records support no tombstones.

`headers` is the AMQP `headers` table as a map of `RabbitMqValue`, which keeps
each field's AMQP type: integers of each width, floats, decimals, timestamps,
booleans, nested arrays and tables, strings, and byte arrays. Short strings and
UTF-8 long strings are read as `String`, and a long string that is not UTF-8 as
`Bytes`. `RabbitMqProperties` holds the basic properties applications set:
content type and encoding, priority, correlation ID, reply-to, expiration,
message ID, timestamp, type (`kind`), user ID, and app ID. The protocol limits
text properties, header names, and routing keys to 255 bytes; `prepare` rejects
longer values.

`RabbitMqMetadata` contains read-only delivery facts: the consumed queue, the
exchange and routing key the message was published with, the redelivered flag,
whether the message is persistent, and its delivery tag. `RabbitMqPublish` has
none of them, so the sink publishes to its configured exchange with the output's
routing key or the configured one, and input metadata is never inherited
implicitly.

The raw form of a `RabbitMqMessage` is a `RabbitMqRecord<Vec<u8>>` with the
undecoded body. Dead letters carry it, so the original body, headers,
properties, and delivery metadata survive even when decoding fails.

## Acknowledgements and ordering

The source consumes with manual acknowledgements. A successful ACK sends
`basic.ack` for the delivery's tag. AMQP does not confirm an acknowledgement,
so a connection lost right after it can still requeue the message, which is
then processed again. Dropping a `RabbitMqMessage` leaves its message
unacknowledged until its channel closes, when the broker requeues it with the
redelivered flag set; this happens when the source closes or loses its
connection.

A delivery tag is valid only on the channel that delivered it. When the
connection or channel is lost, or the broker cancels the consumer, `receive`
cancels the revocation token of every delivery of that channel and reports a
retryable error, and the next `receive` connects again. The runtime abandons
revoked deliveries without ACK or subscription failure, because the broker has
already requeued them. An acknowledgement that finds its channel closed also
revokes its channel's deliveries and fails. Broker heartbeats detect a silent
connection loss.

RabbitMQ delivers a queue's messages to one consumer in order, but distributes
them among several consumers and requeues unacknowledged messages, so
deliveries have no ordering key by default and run in parallel up to the
subscription's `concurrency`. Set `RabbitMqSourceConfig::ordered` to give every
delivery the queue as its ordering key, so that `ProcessingOrder::PerKey`
processes one message at a time in delivery order. Combine it with a queue that
has a single active consumer (`x-single-active-consumer`) for order across
application instances; a requeued message is still delivered again after later
ones.

The consumer is a stream fed by the connection's I/O task: a delivery leaves it
only when a receive completes, so dropping a pending receive does not consume a
delivery.

## Publication

`prepare` encodes the value and converts the routing key, headers, and
properties to their AMQP form once, so publish retries reuse them. The sink
opens its connection and a channel in publisher-confirm mode on first use, and
again after the previous channel was lost.

`RabbitMqSink::submit` returns once the message is written to the channel, with
a completion that resolves on the broker's publisher confirmation. The runtime
frees the job's concurrency slot at submission and acknowledges the input when
the confirmation arrives. `publish` writes the message and waits for its
confirmation. A broker `nack`, a channel lost before the confirmation, or a
message returned as unroutable fails the completion, which stops the
subscription without acknowledging the input, and the input is redelivered.

`RabbitMqSinkConfig::mandatory`, enabled by default, asks the broker to return
a message no queue receives, so a missing binding fails the subscription
instead of losing outputs. Disable it to let the broker discard such messages.
`persistent`, also enabled by default, publishes with the persistent delivery
mode, so durable queues keep the messages across a broker restart; the
confirmation then arrives after the broker has written them.

`RabbitMqSinkConfig::max_pending` bounds the messages submitted whose
confirmation has not arrived; the default is 1000. `submit` waits while that
many are outstanding. Messages on one channel are confirmed in publication
order. `close` stops new submissions, waits for outstanding confirmations, and
closes the connection.

## Metadata inheritance

Register `RabbitMqInherit` on a RabbitMQ-to-RabbitMQ subscription to forward the
received headers and the properties that describe the payload or its
conversation: content type, content encoding, type, correlation ID, app ID, and
priority.

```rust,ignore
use beavers::adapters::rabbitmq::RabbitMqInherit;

Subscription::new("orders", rabbitmq_source, rabbitmq_sink, handler)
    .middleware(RabbitMqInherit::new())
```

Explicit output fields take precedence: a received header is added only when the
output does not set that name, and a property only when the output leaves it
`None`. Dead-letter headers starting with `beavers-dlq-` are never inherited.
The message ID, timestamp, expiration, reply-to address, and user ID identify or
address one message and are never inherited, and neither is the routing key, so
an output cannot route back to the queue it came from unless the sink is
configured to. `without_headers()` and `without_properties()` disable either
part.

`Subscription::forward` applies `RabbitMqInherit::new()` automatically for a
value-only handler between a RabbitMQ source and sink. See the
[runtime guide](../runtime.md#same-platform-forwarding). Register
`TraceContext` after `RabbitMqInherit` to replace inherited trace-context
headers; the source reads trace context from string headers.

## Dead letters

`RabbitMqPublish::from_dead_letter` forwards a dead letter with its original
body, headers, and properties, adding string headers with the failure details
and the queue, exchange, and routing key of its first receipt.
`RabbitMqDeadLetter::from_record` reads them back; see
[forwarding dead letters](../runtime.md#forwarding-dead-letters-to-a-broker-topic).
The sink's routing key applies, so a sink on the default exchange whose routing
key names a dead-letter queue collects them there.

RabbitMQ's own dead-lettering, through a queue's dead-letter exchange, applies
to messages that are rejected, expire, or exceed a queue's delivery limit. The
adapter never rejects a message, so the subscription's error policy decides
what happens to a failed delivery. On a quorum queue, a delivery limit
(`x-delivery-limit`) with a dead-letter exchange still moves a message that
keeps being requeued, for example because it crashes the process every time,
out of the queue.

## Delivery guarantees

A source ACK and a sink publication are separate operations, and AMQP
acknowledgements are not confirmed, so a failure between them, or a connection
loss right after an acknowledgement, can produce a duplicate on redelivery. The
adapter does not use AMQP transactions; RabbitMQ pipelines are at-least-once.
