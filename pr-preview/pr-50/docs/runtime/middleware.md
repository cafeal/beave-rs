# Middleware

`Subscription::middleware` registers a `Middleware<Input, Output>` with two
asynchronous hooks, both of which default to doing nothing:

```rust,ignore
pub trait Middleware<I, O> {
    fn pre_handler(&self, input: I) -> impl Future<Output = Result<Flow<I, O>>> + Send;
    fn post_handler(&self, input: &I, output: O) -> impl Future<Output = Result<O>> + Send;
}

pub enum Flow<I, O> {
    Continue(I),
    Intercept(Emit<O>),
}
```

Implement the hooks with `async fn`. A hook can await external lookups, such
as a cache, a schema registry, or a metadata service, before the handler runs
or before the outputs are prepared:

```rust,ignore
struct Customers(CustomerClient);

impl Middleware<KafkaRecord<Order>, KafkaPublish<Order>> for Customers {
    async fn pre_handler(
        &self,
        mut input: KafkaRecord<Order>,
    ) -> beavers::Result<Flow<KafkaRecord<Order>, KafkaPublish<Order>>> {
        if let Some(order) = input.value.as_mut() {
            order.customer = self.0.fetch(order.customer_id).await.map_err(HandlerError::Retry)?;
        }
        Ok(Flow::Continue(input))
    }
}
```

The runtime awaits each hook before starting the next, so the hooks of one
delivery never run concurrently. Other deliveries keep processing while a hook
waits, within the subscription's concurrency and ordering limits. The input and
output types must be `Send`, and the input type also `Sync`, because the hook
futures run on the multi-threaded Tokio executor. Like handlers, hooks must
not block executor threads, and a hook future is dropped at its current await
point when the delivery is revoked or a drain timeout cancels it; the delivery
is then not acknowledged.

`pre_handler` runs once per delivery, before the handler, in registration
order. Each middleware receives the input returned by the previous one.
`Flow::Continue(input)` passes a possibly transformed input on, and the handler
receives the final one. `Flow::Intercept(emit)` skips the remaining
`pre_handler` hooks and the handler: `Emit::None` acknowledges the delivery
without output, and other values become its outputs. Handler retries reuse the
transformed input and never rerun `pre_handler`.

`post_handler` runs in registration order for every output, including outputs
from an intercepting middleware, and every invocation receives the input as
decoded from the source, before any `pre_handler` transformation. All outputs
pass through `post_handler` and are prepared before the first publication, so
publish retries never rerun a middleware. Handler outputs carry explicit
publish fields; an inheriting middleware only fills fields that the output
leaves unset.

The source item and sink output types are checked at compile time, so a
middleware written for one broker's record and publish types cannot be
registered on a subscription whose source or sink uses different types. Wrap a
synchronous function or closure in `MapMetadata::new` to use it as a
`post_handler`,
including for conversions between different platforms:

```rust,ignore
Subscription::new("orders", kafka_source, pulsar_sink, handler)
    .middleware(MapMetadata::new(|input: &KafkaRecord<Order>, mut output: PulsarPublish<Order>| {
        output.key = input.key.clone();
        Ok(output)
    }))
```

A middleware error never reruns the handler or the middleware. `Reject` is a
`Rejected` failure routed by the [error policy](errors.md#error-policy) with the input as
decoded from the source, and none of the delivery's outputs are published.
`Retry` and `Fatal` stop processing without ACK.

Adapters provide same-platform inheritance middleware: `KafkaInherit` for Kafka
to Kafka, `PulsarInherit` for Pulsar to Pulsar, `RabbitMqInherit` for
RabbitMQ to RabbitMQ, `SqsInherit` for SQS to SQS, `PubSubInherit` for
Pub/Sub to Pub/Sub, and `ServiceBusInherit` for Azure Service Bus to Azure
Service Bus. They copy user-controlled metadata and never copy delivery
facts such as offsets, partitions, message IDs, routing keys, or broker
timestamps. See the [Kafka](../adapters/kafka.md#metadata-inheritance),
[Pulsar](../adapters/pulsar.md#metadata-inheritance),
[RabbitMQ](../adapters/rabbitmq.md#metadata-inheritance),
[SQS](../adapters/sqs.md#metadata-inheritance),
[Pub/Sub](../adapters/pubsub.md#metadata-inheritance), and
[Azure Service Bus](../adapters/servicebus.md#metadata-inheritance) guides. A handler that
returns the sink's publish record carries no received metadata into outputs
unless middleware maps it; a handler that returns a plain value inherits the
platform's defaults, as described in [handler shapes](handlers.md#handler-shapes).
