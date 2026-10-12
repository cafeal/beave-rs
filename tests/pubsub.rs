#![cfg(feature = "pubsub")]

use beavers::{
    App, CancellationToken, RawBytes, Receive, ReceiveError, Sink, Source, SourceMessage,
    Subscription, Utf8,
    adapters::pubsub::{
        PubSubCredentials, PubSubMessage, PubSubPublish, PubSubSink, PubSubSinkConfig,
        PubSubSource, PubSubSourceConfig,
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
fn prepared_messages_respect_attribute_limits() {
    let sink =
        PubSubSink::<RawBytes, Vec<u8>>::new(PubSubSinkConfig::new("projects/p/topics/orders"));
    assert!(sink.prepare(PubSubPublish::new(Vec::new())).is_err());
    let mut reserved = PubSubPublish::new(b"order".to_vec());
    reserved.attributes.insert("googclient".into(), "x".into());
    assert!(sink.prepare(reserved).is_err());
    let mut long = PubSubPublish::new(b"order".to_vec());
    long.attributes.insert("note".into(), "x".repeat(1025));
    assert!(sink.prepare(long).is_err());
    let mut only_attributes = PubSubPublish::new(Vec::new());
    only_attributes
        .attributes
        .insert("kind".into(), "ping".into());
    assert!(sink.prepare(only_attributes).is_ok());
    let prepared = sink
        .prepare(PubSubPublish::new(b"order".to_vec()).with_ordering_key("customer-7"))
        .unwrap();
    assert_eq!(prepared.data(), b"order");
}

const PROJECT: &str = "beavers";

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos}-{sequence}")
}

/// The emulator's `host:port`, the one from `compose.yaml` by default.
fn emulator_host() -> String {
    env::var("PUBSUB_EMULATOR_HOST").unwrap_or_else(|_| "127.0.0.1:8085".into())
}

/// Sends a `PUT` with a JSON body to the emulator's REST API and reads the
/// response, which the emulator sends without closing the connection.
async fn put(path: &str, body: &str) -> anyhow::Result<()> {
    let address = emulator_host();
    let mut stream = TcpStream::connect(&address).await?;
    let request = format!(
        "PUT {path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        let read = stream.read(&mut buffer).await?;
        anyhow::ensure!(read > 0, "the emulator closed the connection");
        response.extend_from_slice(&buffer[..read]);
        let text = String::from_utf8_lossy(&response);
        let Some((head, body)) = text.split_once("\r\n\r\n") else {
            continue;
        };
        let length = head
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")?
                    .trim()
                    .parse()
                    .ok()
            })
            .unwrap_or(0);
        if body.len() < length {
            continue;
        }
        anyhow::ensure!(
            head.starts_with("HTTP/1.1 200"),
            "PUT {path} failed: {text}"
        );
        return Ok(());
    }
}

/// A topic and one subscription to it.
struct Topic {
    topic: String,
    subscription: String,
}

/// Creates a topic and a subscription with a 10-second acknowledgement deadline.
async fn create_topic(prefix: &str, ordered: bool) -> anyhow::Result<Topic> {
    let name = unique_name(prefix);
    let topic = format!("projects/{PROJECT}/topics/{name}");
    let subscription = format!("projects/{PROJECT}/subscriptions/{name}");
    put(&format!("/v1/{topic}"), "{}").await?;
    put(
        &format!("/v1/{subscription}"),
        &format!(
            r#"{{"topic":"{topic}","ackDeadlineSeconds":10,"enableMessageOrdering":{ordered}}}"#
        ),
    )
    .await?;
    Ok(Topic {
        topic,
        subscription,
    })
}

fn source_config(subscription: &str) -> PubSubSourceConfig {
    let mut config = PubSubSourceConfig::new(subscription);
    config.endpoint = Some(format!("http://{}", emulator_host()));
    config.credentials = PubSubCredentials::Anonymous;
    config.ack_deadline = Duration::from_secs(10);
    config
}

fn source(subscription: &str) -> PubSubSource<Utf8, String> {
    PubSubSource::new(source_config(subscription))
}

fn sink(topic: &str) -> PubSubSink<Utf8, String> {
    let mut config = PubSubSinkConfig::new(topic);
    config.endpoint = Some(format!("http://{}", emulator_host()));
    config.credentials = PubSubCredentials::Anonymous;
    PubSubSink::new(config)
}

async fn send(
    sink: &PubSubSink<Utf8, String>,
    output: PubSubPublish<String>,
) -> anyhow::Result<()> {
    let prepared = sink.prepare(output)?;
    tokio::time::timeout(Duration::from_secs(20), sink.publish(&prepared))
        .await
        .expect("timed out publishing to Pub/Sub")
}

/// Receives the next message, retrying receive errors, or `None` after `timeout`.
async fn next_message(
    source: &mut PubSubSource<Utf8, String>,
    timeout: Duration,
) -> anyhow::Result<Option<PubSubMessage<Utf8, String>>> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, source.receive()).await {
            Err(_) => return Ok(None),
            Ok(Ok(Receive::Message(message))) => return Ok(Some(message)),
            Ok(Ok(Receive::End)) => anyhow::bail!("Pub/Sub source ended"),
            Ok(Err(ReceiveError::Retry(error))) => {
                eprintln!("retrying Pub/Sub receive: {error:#}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(Err(ReceiveError::Fatal(error))) => {
                anyhow::bail!("Pub/Sub receive failed: {error:#}")
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires the Pub/Sub emulator; run with cargo test --features pubsub -- --ignored"]
async fn publish_receive_and_ack_against_pubsub() -> anyhow::Result<()> {
    let Topic {
        topic,
        subscription,
    } = create_topic("beavers-pubsub-test", false).await?;
    let sink = sink(&topic);
    let mut output = PubSubPublish::new("hello pubsub".to_owned());
    output.attributes.insert("kind".into(), "test".into());
    send(&sink, output).await?;
    sink.close().await?;
    sink.close().await?;

    let mut source = source(&subscription);
    let message = next_message(&mut source, Duration::from_secs(20))
        .await?
        .expect("no Pub/Sub message");
    let record = message.decode()?;
    assert_eq!(record.value, "hello pubsub");
    assert_eq!(record.attributes["kind"], "test");
    assert_eq!(record.ordering_key, None);
    assert_eq!(record.metadata.subscription, subscription);
    assert!(!record.metadata.message_id.is_empty());
    assert!(record.metadata.publish_time.is_some());
    assert_eq!(message.ordering_key(), None);
    assert_eq!(message.propagation_fields(), [("kind", "test")]);
    message.ack().await?;
    source.close().await?;

    let mut again = self::source(&subscription);
    assert!(
        next_message(&mut again, Duration::from_secs(12))
            .await?
            .is_none(),
        "an acknowledged message was received again"
    );
    again.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Pub/Sub emulator; run with cargo test --features pubsub -- --ignored"]
async fn leases_are_extended_while_a_delivery_is_held() -> anyhow::Result<()> {
    let Topic {
        topic,
        subscription,
    } = create_topic("beavers-pubsub-lease", false).await?;
    send(&sink(&topic), PubSubPublish::new("slow".to_owned())).await?;

    let mut holder = source(&subscription);
    let message = next_message(&mut holder, Duration::from_secs(20))
        .await?
        .expect("no Pub/Sub message");

    let mut competitor = source(&subscription);
    assert!(
        next_message(&mut competitor, Duration::from_secs(15))
            .await?
            .is_none(),
        "the held message was redelivered to another consumer"
    );
    assert!(!message.revocation().unwrap().is_cancelled());
    message.ack().await?;
    holder.close().await?;
    competitor.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Pub/Sub emulator; run with cargo test --features pubsub -- --ignored"]
async fn deliveries_held_past_the_max_lease_are_revoked() -> anyhow::Result<()> {
    let Topic {
        topic,
        subscription,
    } = create_topic("beavers-pubsub-max-lease", false).await?;
    send(&sink(&topic), PubSubPublish::new("stuck".to_owned())).await?;

    let mut config = source_config(&subscription);
    config.max_lease = Duration::from_secs(10);
    let mut holder = PubSubSource::<Utf8, String>::new(config);
    let message = next_message(&mut holder, Duration::from_secs(20))
        .await?
        .expect("no Pub/Sub message");
    let revocation = message.revocation().unwrap();
    tokio::time::timeout(Duration::from_secs(15), revocation.cancelled())
        .await
        .expect("the delivery was not revoked at its max lease");
    drop(message);
    holder.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Pub/Sub emulator; run with cargo test --features pubsub -- --ignored"]
async fn dropped_and_closed_deliveries_are_redelivered() -> anyhow::Result<()> {
    let Topic {
        topic,
        subscription,
    } = create_topic("beavers-pubsub-nack", false).await?;
    let sink = sink(&topic);
    for order in ["first", "second"] {
        send(&sink, PubSubPublish::new(order.to_owned())).await?;
    }

    let mut first = source(&subscription);
    let message = next_message(&mut first, Duration::from_secs(20))
        .await?
        .expect("no Pub/Sub message");
    let message_id = message.decode()?.metadata.message_id;
    drop(message);
    let mut redelivered = None;
    while redelivered.is_none() {
        let message = next_message(&mut first, Duration::from_secs(8))
            .await?
            .expect("a dropped message was not redelivered");
        if message.decode()?.metadata.message_id == message_id {
            redelivered = Some(message);
        } else {
            message.ack().await?;
        }
    }
    redelivered.unwrap().ack().await?;
    first.close().await?;

    // Closing nacks a message the source received but has not handed out,
    // well before its 10-second lease would expire.
    send(&sink, PubSubPublish::new("third".to_owned())).await?;
    send(&sink, PubSubPublish::new("fourth".to_owned())).await?;
    let mut holder = source(&subscription);
    let held = next_message(&mut holder, Duration::from_secs(20))
        .await?
        .expect("no Pub/Sub message");
    held.ack().await?;
    holder.close().await?;
    let mut last = source(&subscription);
    let message = next_message(&mut last, Duration::from_secs(8))
        .await?
        .expect("a closed source did not release its message");
    message.ack().await?;
    last.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Pub/Sub emulator; run with cargo test --features pubsub -- --ignored"]
async fn messages_are_ordered_by_their_ordering_key() -> anyhow::Result<()> {
    let Topic {
        topic,
        subscription,
    } = create_topic("beavers-pubsub-ordered", true).await?;
    let sink = sink(&topic);
    for value in ["a1", "a2", "a3"] {
        send(
            &sink,
            PubSubPublish::new(value.to_owned()).with_ordering_key("a"),
        )
        .await?;
    }

    let mut source = source(&subscription);
    for expected in ["a1", "a2", "a3"] {
        let message = next_message(&mut source, Duration::from_secs(20))
            .await?
            .expect("no ordered Pub/Sub message");
        let record = message.decode()?;
        assert_eq!(record.value, expected);
        assert_eq!(record.ordering_key.as_deref(), Some("a"));
        let key = message.ordering_key().expect("keyed messages are ordered");
        assert_eq!(key.key(), Some(b"a".as_slice()));
        message.ack().await?;
    }
    source.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Pub/Sub emulator; run with cargo test --features pubsub -- --ignored"]
async fn pipeline_forwards_attributes_and_acknowledges_every_input() -> anyhow::Result<()> {
    let input = create_topic("beavers-pubsub-input", false).await?;
    let output = create_topic("beavers-pubsub-output", false).await?;
    let producer = sink(&input.topic);
    for order in ["a", "b", "c"] {
        let mut message = PubSubPublish::new(order.to_owned());
        message.attributes.insert("order".into(), order.into());
        send(&producer, message).await?;
    }

    let shutdown = CancellationToken::new();
    let app = App::new().subscription(Subscription::new(
        "pubsub-pipeline",
        source(&input.subscription),
        sink(&output.topic),
        |order: String| async move { Ok(order.to_uppercase()) },
    ));
    let running = tokio::spawn(app.run_until(shutdown.clone()));

    let mut reader = source(&output.subscription);
    let mut received = Vec::new();
    while received.len() < 3 {
        let message = next_message(&mut reader, Duration::from_secs(20))
            .await?
            .expect("the pipeline did not publish every output");
        received.push(message.decode()?);
        message.ack().await?;
    }
    reader.close().await?;
    shutdown.cancel();
    running.await??;

    received.sort_by(|a, b| a.value.cmp(&b.value));
    for (record, order) in received.iter().zip(["a", "b", "c"]) {
        assert_eq!(record.value, order.to_uppercase());
        assert_eq!(record.attributes["order"], order);
    }
    let mut leftover = source(&input.subscription);
    assert!(
        next_message(&mut leftover, Duration::from_secs(12))
            .await?
            .is_none(),
        "an input was left unacknowledged"
    );
    leftover.close().await?;
    Ok(())
}
