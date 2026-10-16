# Handling dead letters

A dead letter is a delivery that a subscription could not process and that its
[error policy](runtime/errors.md#error-policy) sent to a dead-letter sink instead of
stopping. This guide describes what to do with dead letters once they are
there. It assumes dead letters are forwarded to a topic or queue of the same
platform with `from_dead_letter`, so each one is the original payload with
`beavers-dlq-*` failure details beside it, as described in
[forwarding dead letters](runtime/errors.md#forwarding-dead-letters-to-a-broker-topic).

The workflow is the same on every platform: notice the dead letters, find out
why they failed, and choose one of three outcomes. What differs is how a
dead-letter destination can be read. Log brokers keep messages after they are
read, so dead letters can be examined first and reprocessed later. Queue
brokers remove a message once it is acknowledged, so reading a dead letter
means deciding what happens to it.

## Workflow

### Notice

beavers sends no notifications. Alert on the number of dead-lettered deliveries
and on the backlog of the dead-letter destination, as described in the
[monitoring and alerting guide](monitoring-and-alerting/README.md), rather than
on individual messages: a broken dependency can dead-letter thousands of
deliveries within minutes.

### Find the cause

Three sources tell dead letters apart without reading every message:

- `beavers_delivery_failures_total{action="dead_letter"}` breaks the count
  down by subscription and `FailureKind`, such as `rejected` or
  `retry_exhausted`.
- The `dead-lettered delivery` log event carries the error message of every
  dead letter, so a log search grouped by `error` shows whether one cause
  dominates.
- The `beavers-dlq-error`, `beavers-dlq-failure`, `beavers-dlq-count`, and
  origin details stored with each dead letter identify the failure and where
  the payload was first received.

### Choose an outcome

| Cause | Example | Outcome |
|---|---|---|
| A transient failure | A payment API was unavailable for ten minutes and every order it saw failed | Reprocess the dead letters unchanged, after a delay |
| A bug | An upstream service started sending a currency code the handler cannot parse | Fix and deploy the handler, then reprocess the dead letters |
| Bad data | Load-test orders reached production, or an order has a negative amount | Drop the dead letters, keeping a record, or correct a few by hand and publish them again |

Reprocessing means running the original handler on the dead letters. A
subscription whose source reads the dead-letter destination and whose handler
wraps the original one does this without extra tooling; beavers has no
separate redrive command. Reading the dead-letter destination, rather than
publishing dead letters back to the original topic or queue, keeps other
consumers of the original from receiving them a second time.

## Reprocessing safely

- **Order.** Reprocessed payloads arrive after newer payloads with the same
  key. A handler that overwrites state by key can replace newer state with
  older state unless it compares a version or timestamp first.
- **Loops.** A payload that fails again is dead-lettered again, and
  `beavers-dlq-count` records how many times. Stop at a limit and send such
  payloads to a final destination that nothing reads automatically, called a
  parking topic or queue below.
- **Decoding.** A dead letter that failed to decode in the original
  subscription fails again in the reprocessing one. The default error policy
  stops on decode failures, so reprocessing subscriptions use
  `ErrorPolicy::dead_letter_all()` to park them instead.
- **Load.** Reprocessing a large backlog sends a burst to the handler's
  dependencies. Lower `max_in_flight` on the reprocessing subscription to
  limit it.
- **Retention.** Dead letters are only useful while they exist. Retain the
  dead-letter destination for longer than it takes to investigate a failure,
  which is usually longer than the original. SQS keeps a message for at most
  14 days, and a Pub/Sub subscription for at most 7.

## Log brokers: Kafka and Pulsar

Kafka consumer groups and Pulsar subscriptions each track their own position,
and reading a dead-letter topic removes nothing from it. Dead letters can be
examined by one subscription and reprocessed later by another.

A Pulsar topic keeps messages for its subscriptions; without a retention
policy, messages published while the topic has no subscription are not kept.
Configure retention on the dead-letter topic, or create the subscription that
will reprocess it before dead letters arrive.

### Examine

A subscription with its own consumer group can read the whole dead-letter
topic and print a summary of each dead letter as one JSON line:

```rust,ignore
use beavers::{
    Json, RawBytes, StdoutSink, Subscription,
    adapters::kafka::{Kafka, KafkaDeadLetter, KafkaRecord},
};

#[derive(serde::Serialize)]
struct Summary {
    failure: Option<String>,
    error: Option<String>,
    count: u32,
    origin: Option<String>,
}

let kafka = Kafka::new("localhost:9092");
let source = kafka
    .source::<RawBytes>("orders-dlq-examine", ["orders-dlq"])
    .property("auto.offset.reset", "earliest");

Subscription::new("orders-dlq-examine", source, StdoutSink::<Json<_>>::new(), |record: KafkaRecord<Vec<u8>>| async move {
    let dead = KafkaDeadLetter::from_record(&record).ok().flatten();
    Ok(Summary {
        failure: dead.as_ref().map(|dead| dead.details.failure.as_str().to_owned()),
        error: dead.as_ref().map(|dead| dead.details.error.clone()),
        count: dead.as_ref().map_or(0, |dead| dead.details.count),
        origin: dead.map(|dead| format!("{}/{}@{}", dead.origin.topic, dead.origin.partition, dead.origin.offset)),
    })
})
```

The source reads raw bytes, so payloads that failed to decode are summarized
too, and a record without readable failure details is summarized with empty
fields instead of stopping the subscription. Group the output by error with a tool such as `jq`:

```text
jq -s 'group_by(.error) | map({error: .[0].error, dead_letters: length})' summaries.jsonl
```

The subscription keeps waiting for new dead letters; stop it once its consumer
lag reaches zero, and delete the consumer group if it is not needed again.
Ordinary broker tools show the same details, for example
`kafka-console-consumer --property print.headers=true`. For Pulsar, use
`PulsarDeadLetter::from_record` with a new subscription. A new Pulsar
subscription starts at the end of the topic, so create it at the earliest
position first:

```text
pulsar-admin topics create-subscription --subscription orders-dlq-examine --messageId earliest persistent://public/default/orders-dlq
```

### Reprocess

The reprocessing subscription reads the dead-letter topic with the original
codec and calls the original handler, parking payloads that have already been
dead-lettered three times:

```rust,ignore
use beavers::{
    Classify, ErrorPolicy, HandlerError, Json, RawBytes, Subscription,
    adapters::kafka::{KafkaDeadLetter, KafkaPublish, KafkaRecord},
};

let source = kafka.source::<Json<Order>>("orders-redrive", ["orders-dlq"]);
let parked = kafka.sink::<RawBytes>("orders-parked");

Subscription::new("orders-redrive", source, invoices, |record: KafkaRecord<Order>| async move {
    if let Some(dead) = KafkaDeadLetter::from_record(&record).reject()?.filter(|dead| dead.details.count >= 3) {
        let error = format!("gave up after {} dead-letterings: {}", dead.details.count, dead.details.error);
        return Err(HandlerError::Reject(error.into()));
    }
    handle(record).await
})
.dead_letter_with(parked, |dead_letter| Ok(KafkaPublish::from_dead_letter(dead_letter)))
.error_policy(ErrorPolicy::dead_letter_all())
```

A payload that fails on reprocessing goes to `orders-parked` with its origin
details kept and its count incremented. Publishing failures back to
`orders-dlq` instead, so that a later run retries them, also works when the
count limit is in place.

Two operating styles are common:

- **After a fix.** An operator starts the subscription once the fix is
  deployed and stops it once its consumer lag reaches zero.
- **Continuously, for transient failures.** The subscription runs alongside
  the original one and waits before reprocessing: the handler sleeps until the
  record's timestamp plus a delay, then calls the original handler. Later
  records of a partition are newer, so waiting at the head of the partition
  delays them by no more than the same delay.

A Kafka handler that sleeps longer than `max.poll.interval.ms` (5 minutes by
default) while `max_in_flight` deliveries are waiting makes the consumer leave
its group; raise that property for longer delays. Shutdown does not wait for a
sleeping handler beyond `drain_timeout`; its delivery stays unacknowledged and
is received again.

## Queue brokers: RabbitMQ, SQS, Pub/Sub, and Azure Service Bus

An acknowledged message is removed from a queue or subscription, and even
looking at a message without acknowledging it has side effects:

- **RabbitMQ.** Fetching messages in the management UI and requeuing them
  marks them redelivered, and on a quorum queue each requeue counts toward
  the delivery limit.
- **SQS.** Receiving a message, including from the console, hides it for the
  visibility timeout and increments its receive count. Messages cannot be
  listed without receiving them.
- **Pub/Sub.** A pulled message that is not acknowledged is delivered again
  after its acknowledgement deadline. Seeking back to a time or snapshot
  replays messages only on a subscription that retains acknowledged messages.
- **Azure Service Bus.** Peeking reads messages without locking them, for
  example in Service Bus Explorer, so dead letters can be browsed. Reading
  them through a source still locks and completes them.

Instead of examining dead letters in place, a subscription either decides each
dead letter's outcome as it reads it, or copies dead letters to storage that
can be read repeatedly. The metrics and log events above provide the
breakdown by cause without reading the queue.

### Decide on read

One subscription reads the dead-letter queue and applies the outcomes as
rules: it parks payloads that keep failing, drops payloads known to be bad,
and reprocesses everything else with the original handler. The examples use
SQS; the other platforms have the same shape with `RabbitMqDeadLetter`,
`PubSubDeadLetter`, or `ServiceBusDeadLetter`.

```rust,ignore
use beavers::{
    Classify, Emit, ErrorPolicy, HandlerError, Json, RawBytes, Subscription,
    adapters::sqs::{Sqs, SqsDeadLetter, SqsPublish, SqsRecord},
};

let sqs = Sqs::new();
let source = sqs.source::<Json<Order>>(orders_dlq_url);
let parked = sqs.sink::<RawBytes>(orders_parked_url);

Subscription::new_emitting("orders-dlq-triage", source, invoices, |record: SqsRecord<Order>| async move {
    if let Some(dead) = SqsDeadLetter::from_record(&record).reject()?.filter(|dead| dead.details.count >= 3) {
        let error = format!("gave up after {} dead-letterings: {}", dead.details.count, dead.details.error);
        return Err(HandlerError::Reject(error.into()));
    }
    if record.value.customer_id.starts_with("load-test-") {
        tracing::warn!(message_id = %record.metadata.message_id, "dropping a load-test order");
        return Ok(Emit::None);
    }
    handle(record).await.map(Emit::One)
})
.dead_letter_with(parked, |dead_letter| Ok(SqsPublish::from_dead_letter(dead_letter)))
.error_policy(ErrorPolicy::dead_letter_all())
```

`Emit::None` acknowledges the dead letter without publishing anything, so the
handler logs what it drops; nothing else records it. Run the subscription
after a fix and stop it once the queue is empty, or keep it running to retry
transient failures.

To retry transient failures after a delay, delay the dead letters themselves.
An SQS message can be delayed by up to 15 minutes when it is sent to a
standard queue, so the original subscription can delay its dead letters and
the triage subscription receives them only after that:

```rust,ignore
Subscription::new("orders", source, invoices, handle)
    .dead_letter_with(dead_letters, |dead_letter| {
        let mut publish = SqsPublish::from_dead_letter(dead_letter);
        publish.delay = Some(Duration::from_secs(300));
        Ok(publish)
    })
```

FIFO queues accept only a queue-wide delay. On the other platforms, the
triage handler can sleep before calling the original handler, as in the
continuous style above. The message stays locked or leased to the source
while the handler sleeps; see the [Pub/Sub](adapters/pubsub.md) and
[Azure Service Bus](adapters/servicebus.md) adapter pages for how long a
source holds a message.

### Copy out

When dead letters need investigation before anyone decides what to do, copy
them to storage that can be read repeatedly. This subscription writes each
dead letter as one JSON line to standard output, so a one-off run can be
redirected to a file:

```rust,ignore
use beavers::{
    Json, RawBytes, StdoutSink, Subscription,
    adapters::sqs::{SqsDeadLetter, SqsRecord},
};

#[derive(serde::Serialize)]
struct Archived {
    message_id: String,
    failure: Option<String>,
    error: Option<String>,
    count: u32,
    body: String,
}

let source = sqs.source::<RawBytes>(orders_dlq_url);

Subscription::new("orders-dlq-archive", source, StdoutSink::<Json<_>>::new(), |record: SqsRecord<Vec<u8>>| async move {
    let dead = SqsDeadLetter::from_record(&record).ok().flatten();
    Ok(Archived {
        message_id: record.metadata.message_id.clone(),
        failure: dead.as_ref().map(|dead| dead.details.failure.as_str().to_owned()),
        error: dead.as_ref().map(|dead| dead.details.error.clone()),
        count: dead.map_or(0, |dead| dead.details.count),
        body: String::from_utf8_lossy(&record.value).into_owned(),
    })
})
```

Each line is written before its message is deleted from the queue, so a
crash can repeat a line but not lose one. Stop the run once the queue is
empty. A long-term archive uses a durable sink instead of standard output,
such as an [HTTP sink](adapters/http.md) in front of object storage or a
Kafka topic. The archived payloads can be published again later by a
subscription that reads the file with `StdinSource` and publishes to the
original queue.

## Native broker dead-lettering

The adapters never ask the broker to dead-letter a message: they acknowledge a
delivery after the error policy has published its dead letter. A native
dead-letter mechanism, such as a RabbitMQ dead-letter exchange with a delivery
limit, an SQS redrive policy, a Pub/Sub dead-letter topic, or the Azure
Service Bus dead-letter subqueue, therefore receives only messages that were
delivered again and again without being acknowledged, typically because they
crash the process every time. Keep one configured as a safety net for such
messages, as each adapter page describes.

Messages moved there carry no `beavers-dlq-*` details, so `from_record`
returns `None` for them; the examples above treat that as a count of zero.
Handle them with the same subscriptions, reading the native destination as a
source, such as the Service Bus path `orders/$DeadLetterQueue`. Because such a
message crashed the process before, reprocess it only after the cause is
fixed.
