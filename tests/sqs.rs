#![cfg(feature = "sqs")]

use beavers::{
    App, CancellationToken, Receive, ReceiveError, Sink, Source, SourceMessage, Subscription, Utf8,
    adapters::sqs::{
        SqsAttributeValue, SqsCredentials, SqsMessage, SqsPublish, SqsSink, SqsSinkConfig,
        SqsSource, SqsSourceConfig,
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
fn prepared_messages_need_text_bodies_and_fifo_groups() {
    let sink = SqsSink::<beavers::RawBytes, Vec<u8>>::new(SqsSinkConfig::new(
        "https://sqs.eu-west-1.amazonaws.com/1/orders",
    ));
    assert!(sink.prepare(SqsPublish::new(vec![0xff])).is_err());
    let mut delayed = SqsPublish::new(b"order".to_vec());
    delayed.delay = Some(Duration::from_secs(901));
    assert!(sink.prepare(delayed).is_err());
    assert_eq!(
        sink.prepare(SqsPublish::new(b"order".to_vec()))
            .unwrap()
            .body(),
        "order"
    );

    let fifo = SqsSink::<Utf8, String>::new(SqsSinkConfig::new(
        "https://sqs.eu-west-1.amazonaws.com/1/orders.fifo",
    ));
    assert!(fifo.prepare(SqsPublish::new("order".to_owned())).is_err());
    let mut delayed = SqsPublish::new("order".to_owned()).with_message_group_id("g");
    delayed.delay = Some(Duration::from_secs(1));
    assert!(fifo.prepare(delayed).is_err());
    assert!(
        fifo.prepare(SqsPublish::new("order".to_owned()).with_message_group_id("g"))
            .is_ok()
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

/// The SQS-compatible endpoint, ElasticMQ from `compose.yaml` by default.
fn endpoint() -> String {
    env::var("SQS_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9324".into())
}

/// Creates a queue through the query API and returns its URL.
async fn create_queue(prefix: &str, fifo: bool) -> anyhow::Result<String> {
    let mut path = format!("/?Action=CreateQueue&QueueName={}", unique_name(prefix));
    if fifo {
        path.push_str(".fifo&Attribute.1.Name=FifoQueue&Attribute.1.Value=true");
        path.push_str("&Attribute.2.Name=ContentBasedDeduplication&Attribute.2.Value=true");
    }
    let address = endpoint().trim_start_matches("http://").to_owned();
    let mut stream = TcpStream::connect(&address).await?;
    stream
        .write_all(format!("GET {path} HTTP/1.0\r\nHost: {address}\r\n\r\n").as_bytes())
        .await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    let url = response
        .split_once("<QueueUrl>")
        .and_then(|(_, rest)| rest.split_once("</QueueUrl>"))
        .map(|(url, _)| url.to_owned())
        .ok_or_else(|| anyhow::anyhow!("CreateQueue failed: {response}"))?;
    Ok(url)
}

fn credentials() -> SqsCredentials {
    SqsCredentials::new("beavers", "beavers")
}

fn source_config(queue_url: &str) -> SqsSourceConfig {
    let mut config = SqsSourceConfig::new(queue_url);
    config.region = Some("elasticmq".into());
    config.endpoint_url = Some(endpoint());
    config.credentials = Some(credentials());
    config.wait_time = Duration::from_secs(1);
    config
}

fn source(queue_url: &str) -> SqsSource<Utf8, String> {
    SqsSource::new(source_config(queue_url))
}

fn sink(queue_url: &str) -> SqsSink<Utf8, String> {
    let mut config = SqsSinkConfig::new(queue_url);
    config.region = Some("elasticmq".into());
    config.endpoint_url = Some(endpoint());
    config.credentials = Some(credentials());
    SqsSink::new(config)
}

async fn send(sink: &SqsSink<Utf8, String>, output: SqsPublish<String>) -> anyhow::Result<()> {
    let prepared = sink.prepare(output)?;
    tokio::time::timeout(Duration::from_secs(20), sink.publish(&prepared))
        .await
        .expect("timed out sending to SQS")
}

/// Receives the next message, retrying receive errors, or `None` after `timeout`.
async fn next_message(
    source: &mut SqsSource<Utf8, String>,
    timeout: Duration,
) -> anyhow::Result<Option<SqsMessage<Utf8, String>>> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, source.receive()).await {
            Err(_) => return Ok(None),
            Ok(Ok(Receive::Message(message))) => return Ok(Some(message)),
            Ok(Ok(Receive::End)) => anyhow::bail!("SQS source ended"),
            Ok(Err(ReceiveError::Retry(error))) => {
                eprintln!("retrying SQS receive: {error:#}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(Err(ReceiveError::Fatal(error))) => {
                anyhow::bail!("SQS receive failed: {error:#}")
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires an SQS endpoint; run with cargo test --features sqs -- --ignored"]
async fn send_receive_and_delete_against_sqs() -> anyhow::Result<()> {
    let queue = create_queue("beavers-sqs-test", false).await?;
    let sink = sink(&queue);
    let mut output = SqsPublish::new("hello sqs".to_owned());
    output
        .attributes
        .insert("kind".into(), SqsAttributeValue::from("test"));
    output
        .attributes
        .insert("count".into(), SqsAttributeValue::Number("3".into()));
    output
        .attributes
        .insert("bytes".into(), SqsAttributeValue::Binary(vec![0, 0xff]));
    send(&sink, output).await?;
    sink.close().await?;

    let mut source = source(&queue);
    let message = next_message(&mut source, Duration::from_secs(20))
        .await?
        .expect("no SQS message");
    let record = message.decode()?;
    assert_eq!(record.value, "hello sqs");
    assert_eq!(record.attributes["kind"], SqsAttributeValue::from("test"));
    assert_eq!(
        record.attributes["count"],
        SqsAttributeValue::Number("3".into())
    );
    assert_eq!(
        record.attributes["bytes"],
        SqsAttributeValue::Binary(vec![0, 0xff])
    );
    assert_eq!(record.metadata.queue_url, queue);
    assert!(!record.metadata.message_id.is_empty());
    assert_eq!(record.metadata.receive_count, 1);
    assert!(record.metadata.sent_timestamp.is_some());
    assert_eq!(message.propagation_fields(), [("kind", "test")]);
    message.ack().await?;
    source.close().await?;

    let mut again = self::source(&queue);
    assert!(
        next_message(&mut again, Duration::from_secs(2))
            .await?
            .is_none(),
        "a deleted message was received again"
    );
    again.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an SQS endpoint; run with cargo test --features sqs -- --ignored"]
async fn visibility_is_extended_while_a_delivery_is_held() -> anyhow::Result<()> {
    let queue = create_queue("beavers-sqs-visibility", false).await?;
    send(&sink(&queue), SqsPublish::new("slow".to_owned())).await?;

    let mut config = source_config(&queue);
    config.visibility_timeout = Duration::from_secs(2);
    let mut holder = SqsSource::<Utf8, String>::new(config);
    let message = next_message(&mut holder, Duration::from_secs(20))
        .await?
        .expect("no SQS message");

    let mut competitor = source(&queue);
    assert!(
        next_message(&mut competitor, Duration::from_secs(7))
            .await?
            .is_none(),
        "the held message became visible to another consumer"
    );
    assert!(!message.revocation().unwrap().is_cancelled());
    message.ack().await?;
    holder.close().await?;
    competitor.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an SQS endpoint; run with cargo test --features sqs -- --ignored"]
async fn dropped_and_closed_deliveries_become_visible_again() -> anyhow::Result<()> {
    let queue = create_queue("beavers-sqs-release", false).await?;
    send(&sink(&queue), SqsPublish::new("order".to_owned())).await?;

    let mut first = source(&queue);
    let message = next_message(&mut first, Duration::from_secs(20))
        .await?
        .expect("no SQS message");
    drop(message);
    let mut second = source(&queue);
    let message = next_message(&mut second, Duration::from_secs(10))
        .await?
        .expect("a dropped message was not released");
    assert_eq!(message.decode()?.metadata.receive_count, 2);
    first.close().await?;

    // Closing releases a delivery the source received but nobody acknowledged.
    drop(message);
    second.close().await?;
    let mut third = source(&queue);
    let message = next_message(&mut third, Duration::from_secs(10))
        .await?
        .expect("a closed source did not release its message");
    message.ack().await?;
    third.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an SQS endpoint; run with cargo test --features sqs -- --ignored"]
async fn fifo_messages_are_ordered_by_their_group() -> anyhow::Result<()> {
    let queue = create_queue("beavers-sqs-fifo", true).await?;
    let sink = sink(&queue);
    for value in ["a1", "a2"] {
        send(
            &sink,
            SqsPublish::new(value.to_owned()).with_message_group_id("a"),
        )
        .await?;
    }

    let mut source = source(&queue);
    for expected in ["a1", "a2"] {
        let message = next_message(&mut source, Duration::from_secs(20))
            .await?
            .expect("no SQS FIFO message");
        let record = message.decode()?;
        assert_eq!(record.value, expected);
        assert_eq!(record.metadata.message_group_id.as_deref(), Some("a"));
        assert!(record.metadata.sequence_number.is_some());
        let key = message.ordering_key().expect("FIFO messages are ordered");
        assert_eq!(key.key(), Some(b"a".as_slice()));
        message.ack().await?;
    }
    source.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an SQS endpoint; run with cargo test --features sqs -- --ignored"]
async fn pipeline_forwards_attributes_and_deletes_every_input() -> anyhow::Result<()> {
    let input = create_queue("beavers-sqs-input", false).await?;
    let output = create_queue("beavers-sqs-output", false).await?;
    let producer = sink(&input);
    for order in ["a", "b", "c"] {
        let mut message = SqsPublish::new(order.to_owned());
        message
            .attributes
            .insert("order".into(), SqsAttributeValue::from(order));
        send(&producer, message).await?;
    }

    let shutdown = CancellationToken::new();
    let app = App::new().subscription(Subscription::forward(
        "sqs-pipeline",
        source(&input),
        sink(&output),
        |order: String| async move { Ok(order.to_uppercase()) },
    ));
    let running = tokio::spawn(app.run_until(shutdown.clone()));

    let mut reader = source(&output);
    let mut received = Vec::new();
    while received.len() < 3 {
        let message = next_message(&mut reader, Duration::from_secs(20))
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
        assert_eq!(record.attributes["order"], SqsAttributeValue::from(order));
    }
    let mut leftover = source(&input);
    assert!(
        next_message(&mut leftover, Duration::from_secs(2))
            .await?
            .is_none(),
        "an input was left undeleted"
    );
    leftover.close().await?;
    Ok(())
}
