#![cfg(feature = "servicebus")]

use beavers::{
    App, BoxError, CancellationToken, Receive, ReceiveError, Sink, Source, SourceMessage,
    Subscription, Utf8,
    adapters::servicebus::{
        ServiceBus, ServiceBusMessage, ServiceBusPublish, ServiceBusSink, ServiceBusSource,
        ServiceBusValue,
    },
};
use std::{
    env,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[test]
fn prepared_messages_are_checked_against_service_bus_limits() {
    let bus = ServiceBus::from_connection_string(
        "Endpoint=sb://contoso.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=a2V5",
    )
    .unwrap();
    let sink = bus.sink::<Utf8>("orders");
    let mut mismatched = ServiceBusPublish::new("order".to_owned()).with_session_id("a");
    mismatched.properties.partition_key = Some("b".into());
    assert!(sink.prepare(mismatched).is_err());
    let long_id = ServiceBusPublish::new("order".to_owned()).with_message_id("x".repeat(129));
    assert!(sink.prepare(long_id).is_err());
    assert_eq!(
        sink.prepare(ServiceBusPublish::new("order".to_owned()))
            .unwrap()
            .body(),
        b"order"
    );
}

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos}-{sequence}")
}

#[test]
fn handles_sources_and_sinks_validate_locally() {
    let bus = ServiceBus::new("contoso.servicebus.windows.net");
    assert!(bus.validate().is_err());
    let bus = bus.shared_access_key("send", "a2V5");
    bus.validate().unwrap();
    bus.source::<Utf8>("events/Subscriptions/billing")
        .validate()
        .unwrap();
    assert!(bus.source::<Utf8>("orders").prefetch(0).validate().is_err());
    assert!(bus.sink::<Utf8>("/orders").validate().is_err());
    assert!(
        bus.sink::<Utf8>("orders")
            .max_pending(0)
            .validate()
            .is_err()
    );
    assert!(
        ServiceBus::new("contoso.servicebus.windows.net")
            .shared_access_signature("sig")
            .validate()
            .is_err()
    );
    let debug = format!("{bus:?}");
    assert!(debug.contains("send") && !debug.contains("a2V5"));
}

/// The Service Bus emulator from `compose.yaml` by default.
fn connection() -> ServiceBus {
    let connection_string = env::var("SERVICEBUS_CONNECTION_STRING").unwrap_or_else(|_| {
        "Endpoint=sb://127.0.0.1:5673;SharedAccessKeyName=RootManageSharedAccessKey;\
         SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true"
            .into()
    });
    ServiceBus::from_connection_string(&connection_string)
        .expect("valid SERVICEBUS_CONNECTION_STRING")
}

/// The emulator's administration endpoint.
fn admin_address() -> String {
    env::var("SERVICEBUS_ADMIN_ADDR").unwrap_or_else(|_| "127.0.0.1:5300".into())
}

/// Sends one request to the administration endpoint and returns the status
/// code and body.
async fn admin(method: &str, path: &str, body: &str) -> Result<(u16, String), BoxError> {
    let address = admin_address();
    let mut stream = TcpStream::connect(&address).await?;
    let request = format!(
        "{method} {path}?api-version=2021-05 HTTP/1.1\r\nHost: {address}\r\n\
         Content-Type: application/atom+xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    let status = response
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| BoxError::from(format!("invalid administration response: {response}")))?;
    Ok((status, response))
}

/// A queue that the test deletes when it ends.
struct Queue(String);

impl Queue {
    /// Creates a queue with the given lock duration and maximum delivery count.
    async fn create(prefix: &str, lock: &str, max_delivery_count: u32) -> Result<Self, BoxError> {
        let name = unique_name(prefix);
        let description = format!(
            "<entry xmlns=\"http://www.w3.org/2005/Atom\"><content type=\"application/xml\">\
             <QueueDescription xmlns=\"http://schemas.microsoft.com/netservices/2010/10/servicebus/connect\">\
             <LockDuration>{lock}</LockDuration><MaxDeliveryCount>{max_delivery_count}</MaxDeliveryCount>\
             </QueueDescription></content></entry>"
        );
        let (status, response) = admin("PUT", &format!("/{name}"), &description).await?;
        if !(status == 201) {
            return Err(format!("creating queue {name} failed: {response}").into());
        };
        Ok(Self(name))
    }

    async fn default(prefix: &str) -> Result<Self, BoxError> {
        Self::create(prefix, "PT1M", 10).await
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        let path = format!("/{}", self.0);
        // The emulator holds a limited number of entities.
        let _ = std::thread::spawn(move || {
            tokio::runtime::Runtime::new()
                .expect("runtime")
                .block_on(admin("DELETE", &path, ""))
        })
        .join();
    }
}

fn source(entity: &str) -> ServiceBusSource<Utf8> {
    connection().source(entity)
}

fn sink(entity: &str) -> ServiceBusSink<Utf8> {
    connection().sink(entity)
}

async fn send(
    sink: &ServiceBusSink<Utf8>,
    output: ServiceBusPublish<String>,
) -> Result<(), BoxError> {
    let prepared = sink.prepare(output)?;
    tokio::time::timeout(Duration::from_secs(30), sink.publish(&prepared))
        .await
        .expect("timed out sending to Service Bus")
}

/// Receives the next message, retrying receive errors, or `None` after `timeout`.
async fn next_message(
    source: &mut ServiceBusSource<Utf8>,
    timeout: Duration,
) -> Result<Option<ServiceBusMessage<Utf8>>, BoxError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, source.receive()).await {
            Err(_) => return Ok(None),
            Ok(Ok(Receive::Message(message))) => return Ok(Some(message)),
            Ok(Ok(Receive::End)) => return Err("Service Bus source ended".into()),
            Ok(Err(ReceiveError::Retry(error))) => {
                eprintln!("retrying Service Bus receive: {error:#}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Ok(Err(ReceiveError::Fatal(error))) => {
                return Err(format!("Service Bus receive failed: {error:#}").into());
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires the Service Bus emulator; run with cargo test --features servicebus -- --ignored"]
async fn send_receive_and_complete_against_service_bus() -> Result<(), BoxError> {
    let queue = Queue::default("beavers-sb-test").await?;
    // One handle: the sink and both sources share its connection, and closing
    // the sink and the first source leaves it usable.
    let bus = connection();
    let sink = bus.sink::<Utf8>(&queue.0);
    let mut output = ServiceBusPublish::new("hello service bus".to_owned()).with_message_id("m-1");
    output
        .application_properties
        .insert("kind".into(), ServiceBusValue::from("test"));
    output
        .application_properties
        .insert("count".into(), ServiceBusValue::I64(3));
    output
        .application_properties
        .insert("id".into(), ServiceBusValue::Uuid([7; 16]));
    output.properties.correlation_id = Some("c-1".into());
    output.properties.subject = Some("created".into());
    output.properties.content_type = Some("text/plain".into());
    output.properties.time_to_live = Some(Duration::from_secs(600));
    send(&sink, output).await?;
    sink.close().await?;

    let mut source = bus.source::<Utf8>(&queue.0);
    let message = next_message(&mut source, Duration::from_secs(30))
        .await?
        .expect("no Service Bus message");
    let record = message.decode()?;
    assert_eq!(record.value, "hello service bus");
    assert_eq!(
        record.application_properties["kind"],
        ServiceBusValue::from("test")
    );
    assert_eq!(
        record.application_properties["count"],
        ServiceBusValue::I64(3)
    );
    assert_eq!(
        record.application_properties["id"],
        ServiceBusValue::Uuid([7; 16])
    );
    assert_eq!(record.properties.message_id.as_deref(), Some("m-1"));
    assert_eq!(record.properties.correlation_id.as_deref(), Some("c-1"));
    assert_eq!(record.properties.subject.as_deref(), Some("created"));
    assert_eq!(
        record.properties.content_type.as_deref(),
        Some("text/plain")
    );
    assert_eq!(
        record.properties.time_to_live,
        Some(Duration::from_secs(600))
    );
    assert_eq!(record.metadata.entity_path, queue.0);
    assert!(record.metadata.sequence_number > 0);
    assert!(record.metadata.enqueued_time.is_some());
    assert_eq!(record.metadata.delivery_count, 1);
    assert_eq!(record.metadata.lock_token.len(), 36);
    assert_eq!(message.propagation_fields(), [("kind", "test")]);
    message.ack().await?;
    source.close().await?;

    let mut again = bus.source::<Utf8>(&queue.0);
    assert!(
        next_message(&mut again, Duration::from_secs(3))
            .await?
            .is_none(),
        "a completed message was received again"
    );
    again.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Service Bus emulator; run with cargo test --features servicebus -- --ignored"]
async fn locks_are_renewed_while_a_delivery_is_held() -> Result<(), BoxError> {
    let queue = Queue::create("beavers-sb-lock", "PT5S", 10).await?;
    send(&sink(&queue.0), ServiceBusPublish::new("slow".to_owned())).await?;

    let mut holder = source(&queue.0);
    let message = next_message(&mut holder, Duration::from_secs(30))
        .await?
        .expect("no Service Bus message");

    let mut competitor = source(&queue.0);
    assert!(
        next_message(&mut competitor, Duration::from_secs(15))
            .await?
            .is_none(),
        "the held message was delivered to another receiver"
    );
    assert!(!message.revocation().unwrap().is_cancelled());
    message.ack().await?;
    holder.close().await?;
    competitor.close().await?;

    let mut again = source(&queue.0);
    assert!(
        next_message(&mut again, Duration::from_secs(8))
            .await?
            .is_none(),
        "a message completed after renewals was received again"
    );
    again.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Service Bus emulator; run with cargo test --features servicebus -- --ignored"]
async fn dropped_deliveries_are_abandoned() -> Result<(), BoxError> {
    let queue = Queue::default("beavers-sb-abandon").await?;
    send(&sink(&queue.0), ServiceBusPublish::new("order".to_owned())).await?;

    let mut source = source(&queue.0);
    let message = next_message(&mut source, Duration::from_secs(30))
        .await?
        .expect("no Service Bus message");
    drop(message);
    // The abandoned message returns to the same link, which still has credit.
    let message = next_message(&mut source, Duration::from_secs(15))
        .await?
        .expect("a dropped message was not abandoned");
    assert_eq!(message.decode()?.metadata.delivery_count, 2);
    message.ack().await?;
    source.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Service Bus emulator; run with cargo test --features servicebus -- --ignored"]
async fn closing_returns_prefetched_messages() -> Result<(), BoxError> {
    let queue = Queue::default("beavers-sb-close").await?;
    let sink = sink(&queue.0);
    for order in ["a", "b", "c"] {
        send(&sink, ServiceBusPublish::new(order.to_owned())).await?;
    }

    let mut first = source(&queue.0);
    let message = next_message(&mut first, Duration::from_secs(30))
        .await?
        .expect("no Service Bus message");
    let taken = message.decode()?.value;
    message.ack().await?;
    // Give Service Bus time to send the other messages ahead.
    tokio::time::sleep(Duration::from_secs(1)).await;
    first.close().await?;

    // The one-minute lock of the prefetched messages would outlast this wait.
    let mut second = source(&queue.0);
    let mut values = Vec::new();
    while values.len() < 2 {
        let message = next_message(&mut second, Duration::from_secs(15))
            .await?
            .expect("a closed source kept its prefetched messages locked");
        let record = message.decode()?;
        // Releasing a message the application never received counts no delivery.
        assert_eq!(record.metadata.delivery_count, 1, "{}", record.value);
        values.push(record.value);
        message.ack().await?;
    }
    values.push(taken);
    values.sort();
    assert_eq!(values, ["a", "b", "c"]);
    second.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Service Bus emulator; run with cargo test --features servicebus -- --ignored"]
async fn exhausted_deliveries_move_to_the_dead_letter_subqueue() -> Result<(), BoxError> {
    let queue = Queue::create("beavers-sb-exhausted", "PT1M", 1).await?;
    send(&sink(&queue.0), ServiceBusPublish::new("poison".to_owned())).await?;

    let mut consumer = source(&queue.0);
    let message = next_message(&mut consumer, Duration::from_secs(30))
        .await?
        .expect("no Service Bus message");
    drop(message);
    consumer.close().await?;

    let mut dead_letters = source(&format!("{}/$DeadLetterQueue", queue.0));
    let message = next_message(&mut dead_letters, Duration::from_secs(30))
        .await?
        .expect("the abandoned message did not reach the dead-letter subqueue");
    let record = message.decode()?;
    assert_eq!(record.value, "poison");
    assert_eq!(
        record.application_properties.get("DeadLetterReason"),
        Some(&ServiceBusValue::from("MaxDeliveryCountExceeded"))
    );
    message.ack().await?;
    dead_letters.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Service Bus emulator; run with cargo test --features servicebus -- --ignored"]
async fn topic_subscriptions_receive_published_messages() -> Result<(), BoxError> {
    // `compose.yaml` configures this topic and subscription, because the
    // emulator cannot create subscriptions through its administration API.
    let marker = unique_name("event");
    send(
        &sink("beavers-events"),
        ServiceBusPublish::new(marker.clone()),
    )
    .await?;

    let mut source = source("beavers-events/Subscriptions/beavers-test");
    loop {
        let message = next_message(&mut source, Duration::from_secs(30))
            .await?
            .expect("the subscription did not receive the message");
        let record = message.decode()?;
        message.ack().await?;
        if record.value == marker {
            assert_eq!(
                record.metadata.entity_path,
                "beavers-events/Subscriptions/beavers-test"
            );
            break;
        }
    }
    source.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Service Bus emulator; run with cargo test --features servicebus -- --ignored"]
async fn scheduled_messages_wait_until_their_time() -> Result<(), BoxError> {
    let queue = Queue::default("beavers-sb-scheduled").await?;
    let mut output = ServiceBusPublish::new("later".to_owned());
    let due = SystemTime::now() + Duration::from_secs(6);
    output.scheduled_enqueue_time = Some(due);
    send(&sink(&queue.0), output).await?;

    let mut source = source(&queue.0);
    assert!(
        next_message(&mut source, Duration::from_secs(3))
            .await?
            .is_none(),
        "a scheduled message arrived early"
    );
    let message = next_message(&mut source, Duration::from_secs(30))
        .await?
        .expect("the scheduled message never arrived");
    let record = message.decode()?;
    assert_eq!(record.value, "later");
    assert!(record.metadata.scheduled_enqueue_time.is_some());
    message.ack().await?;
    source.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Service Bus emulator; run with cargo test --features servicebus -- --ignored"]
async fn pipeline_forwards_application_properties_and_completes_every_input() -> Result<(), BoxError>
{
    let input = Queue::default("beavers-sb-input").await?;
    let output = Queue::default("beavers-sb-output").await?;
    let producer = sink(&input.0);
    for order in ["a", "b", "c"] {
        let mut message = ServiceBusPublish::new(order.to_owned());
        message
            .application_properties
            .insert("order".into(), ServiceBusValue::from(order));
        send(&producer, message).await?;
    }

    let shutdown = CancellationToken::new();
    let app = App::new().subscription(Subscription::new(
        "servicebus-pipeline",
        source(&input.0),
        sink(&output.0),
        |order: String| async move { Ok(order.to_uppercase()) },
    ));
    let running = tokio::spawn(app.run_until(shutdown.clone()));

    let mut reader = source(&output.0);
    let mut received = Vec::new();
    while received.len() < 3 {
        let message = next_message(&mut reader, Duration::from_secs(30))
            .await?
            .expect("the pipeline did not send every output");
        received.push(message.decode()?);
        message.ack().await?;
    }
    reader.close().await?;
    shutdown.cancel();
    running.await??;

    received.sort_by(|a, b| a.value.cmp(&b.value));
    for (record, order) in received.iter().zip(["a", "b", "c"]) {
        assert_eq!(record.value, order.to_uppercase());
        assert_eq!(
            record.application_properties["order"],
            ServiceBusValue::from(order)
        );
    }
    let mut leftover = source(&input.0);
    assert!(
        next_message(&mut leftover, Duration::from_secs(3))
            .await?
            .is_none(),
        "an input was left uncompleted"
    );
    leftover.close().await?;
    Ok(())
}
