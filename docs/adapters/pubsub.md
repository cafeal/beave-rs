# Pub/Sub adapter

The optional `pubsub` feature provides typed Google Cloud Pub/Sub source and
sink adapters backed by the official Google Cloud client library for Rust
(`google-cloud-pubsub`).

```toml
beavers = { version = "0.1", features = ["pubsub"] }
```

## Configuration

A `PubSub` handle holds the connection settings that its sources and sinks
share. It resolves its credentials once and keeps one subscriber client for all
of its sources. Each sink has a publisher client of its own, because a Pub/Sub
publisher batches messages for one topic, and uses the handle's credentials.
Credentials and clients are created on the first `receive` or publication that
needs them, which also validates the settings.

The handle uses Application Default Credentials unless `anonymous()` is set:
the `GOOGLE_APPLICATION_CREDENTIALS` key file, the gcloud user credentials, or
the metadata server of the Google Cloud environment the application runs in.
`endpoint` replaces `https://pubsub.googleapis.com`, for example with a
regional endpoint. Each handle builder method returns a handle with
credentials and clients of its own, so configure the handle before building
sources and sinks from it.

`source` takes the full name of one subscription,
`projects/{project}/subscriptions/{id}`, and `sink` the full name of one topic,
`projects/{project}/topics/{id}`. `source_with_codec` and `sink_with_codec`
take a codec value first. The handle, source, and sink each have `validate`,
which checks the values without contacting Google Cloud.

```rust
use beavers::{Utf8, adapters::pubsub::{PubSub, PubSubPublish}};

let pubsub = PubSub::new();
let source = pubsub.source::<Utf8, String>("projects/my-project/subscriptions/orders");
let sink = pubsub.sink::<Utf8, String>("projects/my-project/topics/processed-orders");

let output = PubSubPublish::new("created".to_owned()).with_ordering_key("customer-7");
```

For the [Pub/Sub emulator](../development.md#broker-configuration), set the
endpoint to its address and send no credentials:
`PubSub::new().endpoint("http://localhost:8085").anonymous()`.

The adapters create no topics or subscriptions and set no subscription
properties such as message ordering, exactly-once delivery, or a dead-letter
policy. Create them with infrastructure tooling before the subscription starts.

The source receives through one streaming pull. Pub/Sub stops sending once
`max_outstanding_messages` (1000 by default) messages or
`max_outstanding_bytes` (100 MiB) of data are received and not yet
acknowledged; zero or less sets no limit. The runtime's `max_in_flight` bounds
the deliveries it holds; the rest of the outstanding messages wait in the
client's buffer with their leases extended.

The sink's `batch_delay`, 10 milliseconds by default, is how long the client
collects outputs into one `Publish` request.

```rust
use beavers::{Utf8, adapters::pubsub::PubSub};
use std::time::Duration;

let pubsub = PubSub::new();
let source = pubsub
    .source::<Utf8, String>("projects/my-project/subscriptions/orders")
    .max_outstanding_messages(200)
    .ack_deadline(Duration::from_secs(120));
let sink = pubsub
    .sink::<Utf8, String>("projects/my-project/topics/processed-orders")
    .batch_delay(Duration::from_millis(50));
```

## Records

`PubSubRecord<T>` is the decoded input and `PubSubPublish<T>` the output. The
codec applies to the message data. Pub/Sub has no null message, so the value
is always present and Pub/Sub records support no tombstones. The data is bytes,
so every codec works.

`attributes` holds the message attributes as strings. `ordering_key` is the key
the message was published with, or `None`. `PubSubMetadata` contains read-only
facts about a received message: the subscription, message ID, publish time in
milliseconds since the Unix epoch, and the delivery attempt, which Pub/Sub
counts only on a subscription with a dead-letter policy. `PubSubPublish` has
none of them; it sets the value, attributes, and ordering key.

The raw form of a `PubSubMessage` is a `PubSubRecord<Vec<u8>>` with the
undecoded data. Dead letters carry it, so the original data, attributes,
ordering key, and metadata survive even when decoding fails.

## Acknowledgements and ordering

Pub/Sub redelivers a message whose acknowledgement deadline passes. The client
extends the lease of every message the source has received and not yet
acknowledged, by `ack_deadline` (60 seconds by default, 10 seconds to 10
minutes) at a time, for at most `max_lease` (one hour by default). When a
delivery is still held at `max_lease`, the source cancels its revocation
token, because the client stops extending it and Pub/Sub will redeliver it.
The runtime abandons a revoked delivery without ACK or subscription failure.

An ACK hands the acknowledgement to the client, which sends acknowledgements in
batches without confirmation, so an acknowledged message can still be
redelivered. On a subscription with exactly-once delivery, the ACK waits for
Pub/Sub to confirm it, fails when the client cannot confirm it, and revokes the
delivery when its lease expired first. Dropping a `PubSubMessage` without
acknowledging it nacks it, so Pub/Sub redelivers it at once.

`close` stops the stream, waits until the deliveries the source handed out are
acknowledged or dropped, sends the pending acknowledgements, and nacks the
messages the client received but the source never handed out. A stream that
fails with an error the client does not retry itself is replaced on the next
`receive`; deliveries from the failed stream keep their leases until they
finish.

A message published with an ordering key has the subscription and that key as
its ordering key, so `ProcessingOrder::PerKey` processes one message per key at
a time while keys run in parallel. Pub/Sub delivers the messages of a key in
order only on a subscription with message ordering enabled. Messages without an
ordering key have no ordering key and run in parallel up to the subscription's
`concurrency`.

The client buffers what the stream received, so a cancelled `receive` loses no
message.

## Publication

`prepare` encodes the value and checks Pub/Sub's limits once, so publish
retries reuse the message: at most 100 attributes, names of 1 to 256 bytes that
do not start with `goog`, values and the ordering key of at most 1024 bytes,
and data or at least one attribute.

`PubSubSink::publish` succeeds once Pub/Sub has stored the message and returned
its ID. The client batches publications and retries transient errors itself.
An invalid argument fails as `PublishRejected`, which the subscription's error
policy can dead-letter instead of retrying. After a publication with an
ordering key fails, the client pauses that key; the sink resumes it, so the
runtime's retry or a later output of the key can be published. `close` sends
the batched messages and stops new publications.

## Metadata inheritance

Register `PubSubInherit` on a Pub/Sub-to-Pub/Sub subscription to forward the
received message attributes:

```rust,ignore
use beavers::adapters::pubsub::PubSubInherit;

Subscription::new("orders", pubsub_source, pubsub_sink, handler)
    .middleware(PubSubInherit::new().with_ordering_key())
```

A received attribute is added only when the output does not set that name, and
dead-letter attributes starting with `beavers-dlq-` are never inherited.
`with_ordering_key()` also forwards the ordering key when the output leaves it
`None`. The message ID, publish time, and delivery attempt are never inherited.
`without_attributes()` disables attribute inheritance.

A handler that returns a plain value between a Pub/Sub source and sink gets
`PubSubInherit::new()` automatically; register
`PubSubInherit::new().with_ordering_key()` explicitly to forward ordering keys.
See the [runtime guide](../runtime.md#handler-shapes). Register `TraceContext`
after `PubSubInherit` to replace inherited trace-context attributes; the source
reads trace context from attributes.

## Dead letters

`PubSubPublish::from_dead_letter` forwards a dead letter with its original
data, attributes, and ordering key, and adds the failure details and the
subscription and message ID of the first receipt as `beavers-dlq-*`
attributes. Pub/Sub limits attribute values to 1024 bytes, so a longer error
message is cut at a character boundary. `PubSubDeadLetter::from_record` reads
them back; see
[forwarding dead letters](../runtime.md#forwarding-dead-letters-to-a-broker-topic).

A subscription's dead-letter policy moves a message to its dead-letter topic
after `maxDeliveryAttempts` deliveries without acknowledgement. The
subscription's error policy decides what happens to a failed delivery first,
so the dead-letter policy applies to messages that are delivered again and
again without being acknowledged, for example because they crash the process
every time. Keep `maxDeliveryAttempts` high enough that the nacks of a
source's shutdowns do not exhaust it.

## Delivery guarantees

A source ACK and a sink publication are separate requests, so a failure between
them, a lease that expires before the acknowledgement, or an acknowledgement
the client loses can produce a duplicate. Pub/Sub pipelines are at-least-once.
Exactly-once delivery on the input subscription confirms each acknowledgement
but does not make publication and acknowledgement atomic.
