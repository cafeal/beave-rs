# Azure Service Bus adapter

The optional `servicebus` feature provides typed Azure Service Bus source and
sink adapters that speak AMQP 1.0 to a namespace through `fe2o3-amqp`.

```toml
beavers = { version = "0.1", features = ["servicebus"] }
```

## Configuration

A `ServiceBus` handle names a namespace, such as
`contoso.servicebus.windows.net`, and a credential:

- `shared_access_key(name, key)` signs a shared access signature for each
  entity the adapter uses and renews it before it expires.
- `shared_access_signature(token)` uses a token signed elsewhere. It cannot be
  renewed, so the connection fails once the token expires.

`ServiceBus::from_connection_string` reads a connection string as the Azure
portal and the Service Bus emulator show it. A connection string with
`EntityPath` is refused; pass the entity to the source or sink instead. The
`Debug` output of the handle hides the key and the signature.

The adapters connect with AMQP over TLS on port 5671 and authenticate with
claims-based security. `UseDevelopmentEmulator=true` in a connection string, or
`emulator()`, selects plain AMQP on port 5672 for the emulator; a port in the
namespace overrides either default. Microsoft Entra ID credentials are not
supported yet.

The handle opens one AMQP connection with one session, and every source and
sink built from it attaches its links to that session. Each entity is
authorized once on the connection through claims-based security, and tokens
signed with a shared access key are renewed before they expire. The connection
opens on the first use that needs it, which also validates the settings, and is
opened again after it is lost. Closing a source or sink detaches its links
only; the connection closes once the handle and every source and sink built
from it have been dropped. Each handle builder method returns a handle with a
connection of its own, so configure the handle before building sources and
sinks from it.

```rust
use beavers::{Utf8, adapters::servicebus::{ServiceBus, ServiceBusPublish}};

let bus = ServiceBus::from_connection_string(
    "Endpoint=sb://contoso.servicebus.windows.net/;SharedAccessKeyName=app;SharedAccessKey=...",
)?;
let source = bus.source::<Utf8>("orders/Subscriptions/billing");
let sink = bus.sink::<Utf8>("processed-orders");

let output = ServiceBusPublish::new("created".to_owned()).with_message_id("order-7");
# Ok::<(), beavers::Error>(())
```

`source` receives from a queue, a topic subscription written as
`{topic}/Subscriptions/{subscription}`, or a dead-letter subqueue such as
`orders/$DeadLetterQueue`. Session-enabled entities are not supported. The
source's `prefetch`, 10 by default, is how many messages Service Bus may send
ahead of `receive`. Each is locked from the moment it is sent, so keep
`prefetch` small enough that prefetched messages reach a handler well within
the lock duration.

`sink` sends to one queue or topic. The sink's `max_pending`, 1000 by default,
limits the messages sent and not yet accepted.

```rust
use beavers::{Utf8, adapters::servicebus::ServiceBus};

let bus = ServiceBus::new("contoso.servicebus.windows.net")
    .shared_access_key("app", "...");
let source = bus.source::<Utf8>("orders").prefetch(20);
let sink = bus.sink::<Utf8>("processed-orders").max_pending(100);
```

`source_with_codec` and `sink_with_codec` take a codec value first. The handle,
source, and sink each have `validate`, which checks the values without
contacting Service Bus. The adapters create no entities; create queues, topics,
subscriptions, and their rules with infrastructure tooling before the
subscription starts.

## Records

`ServiceBusRecord<T>` is the decoded input and `ServiceBusPublish<T>` the
output. The codec applies to the message body, which the adapters send as one
AMQP data section. A received message whose body is an AMQP value or sequence,
as some AMQP clients send, fails to decode; its raw form carries the AMQP
encoding of the body.

`application_properties` holds typed `ServiceBusValue`s: strings, booleans,
integers, floats, characters, timestamps, and UUIDs. Service Bus refuses binary
application properties and stores symbols as strings, so the adapter has
neither; received binary, decimal, and described values are skipped.

`ServiceBusProperties` holds the properties the sender sets: message ID,
correlation ID, session ID, reply-to address and session, subject, content type,
`to`, time to live, and partition key. `prepare` limits the identifiers to 128
characters, requires the partition key to equal a session ID when both are set,
and accepts a time to live from one millisecond to 49 days.

`ServiceBusMetadata` contains read-only facts about a received message: the
entity path, sequence number, enqueued and scheduled enqueue times in
milliseconds since the Unix epoch, delivery count, lock token, and the entity a
dead-lettered message came from. `ServiceBusPublish` has none of them except an
optional `scheduled_enqueue_time`, which keeps the message invisible until that
time.

The raw form of a `ServiceBusMessage` is a `ServiceBusRecord<Vec<u8>>` with the
undecoded body. Dead letters carry it, so the original body, properties, and
metadata survive even when decoding fails.

## Acknowledgements and ordering

The source receives in peek-lock mode. A received message is locked to the
source for the entity's lock duration. A background task renews the locks of
the messages the source has received and not yet settled with
`com.microsoft:renew-lock` once a third of the lock duration has passed, so a
handler may run longer than the lock duration.

A successful ACK completes the message with an `accepted` disposition. Service
Bus does not confirm a completion, so a completion sent just before the lock is
lost leaves the message to be delivered again. Dropping a `ServiceBusMessage`
without acknowledging it abandons the message, which Service Bus delivers again
at once with an incremented delivery count. Closing the source releases the
messages Service Bus sent ahead and the application never received, without
counting a delivery, and detaches its links; the handle's connection stays
open.

A lock that renewal reports lost, or that ends before it is renewed, means
Service Bus may already have handed the message to another receiver, so the
adapter cancels that delivery's revocation token. The runtime abandons a
revoked delivery without ACK or subscription failure. An ACK whose lock has
already ended also revokes the delivery and fails. When the connection or link
is lost, every unsettled delivery is revoked and the next `receive` attaches
again, reopening the handle's connection when it was lost.

Without sessions, Service Bus delivers messages in no guaranteed order, so the
deliveries have no ordering key and run in parallel up to the subscription's
`concurrency`. `ServiceBusDistribution::Session` processes the deliveries of
one session ID one at a time, in the order this source received them, while
deliveries without a session ID run in parallel:

```rust,ignore
use beavers::adapters::servicebus::ServiceBusDistribution;

Subscription::new("orders", source, sink, handler)
    .concurrency(16)
    .distribution(ServiceBusDistribution::Session);
```

Service Bus neither orders a session's messages for a receiver without
sessions nor keeps them on one receiver, so this orders only what one process
receives.

`receive` is cancel-safe: a message that arrives for a dropped `receive` is
kept for the next one.

## Publication

`prepare` encodes the value and converts the properties once, so publish
retries reuse the message. `ServiceBusSink::submit` sends the message on the
link and returns a completion that resolves when Service Bus accepts it;
`publish` waits for that completion. A message larger than the link's maximum
message size fails as `PublishRejected`, which the subscription's error policy
can dead-letter instead of retrying. A lost connection fails the publication,
and the next one attaches again, reopening the handle's connection. `close`
stops new publications and detaches the send link; the handle's connection
stays open.

A scheduled message is stored at once, so a completed publication of a
scheduled message is as durable as any other.

## Metadata inheritance

Register `ServiceBusInherit` on a Service Bus to Service Bus subscription to
forward the received application properties:

```rust,ignore
use beavers::adapters::servicebus::ServiceBusInherit;

Subscription::new("orders", servicebus_source, servicebus_sink, handler)
    .middleware(ServiceBusInherit::new())
```

An application property is added only when the output does not set that name,
and dead-letter properties starting with `beavers-dlq-` are never inherited.
`with_session()` also forwards the session ID and partition key when the output
leaves the session ID `None`. The message ID, correlation ID, subject, content
type, time to live, and schedule are never inherited.
`without_application_properties()` disables property inheritance.

A handler that returns a plain value between a Service Bus source and sink gets
`ServiceBusInherit::new()` automatically. See the
[runtime guide](../runtime/handlers.md#handler-shapes). Register `TraceContext` after
`ServiceBusInherit` to replace inherited trace-context properties; the source
reads trace context from string application properties.

## Dead letters

`ServiceBusPublish::from_dead_letter` forwards a dead letter with its original
body, application properties, and message properties, and adds the failure
details and the entity path and sequence number of the first receipt as
`beavers-dlq-*` string application properties. It clears the message ID, so
duplicate detection on the dead-letter entity does not drop a payload
dead-lettered again, and the time to live, so the dead letter does not expire
with the original. `ServiceBusDeadLetter::from_record` reads the details back;
see [forwarding dead letters](../runtime/errors.md#forwarding-dead-letters-to-a-broker-topic).

Every queue and subscription also has a dead-letter subqueue, to which Service
Bus moves a message once its delivery count exceeds the entity's
`MaxDeliveryCount`. The subscription's error policy decides what happens to a
failed delivery first, so the subqueue receives messages that are delivered
again and again without being settled, for example because they crash the
process every time. Keep `MaxDeliveryCount` high enough for that. A source can
receive from the subqueue by its path, such as `orders/$DeadLetterQueue`.

## Delivery guarantees

A source ACK and a sink publication are separate operations, so a failure
between them, or a lock lost before the completion arrives, can produce a
duplicate. Service Bus pipelines are at-least-once; duplicate detection on the
target entity drops a repeated message ID only within its detection window.
