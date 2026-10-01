#![cfg(feature = "rabbitmq")]

use beavers::{
    App, CancellationToken, Receive, ReceiveError, Sink, Source, SourceMessage, Subscription, Utf8,
    adapters::rabbitmq::{
        RabbitMqMessage, RabbitMqPublish, RabbitMqSink, RabbitMqSinkConfig, RabbitMqSource,
        RabbitMqSourceConfig, RabbitMqValue,
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
fn prepared_messages_use_the_configured_routing_key_unless_set() {
    let sink = RabbitMqSink::<Utf8, String>::new(RabbitMqSinkConfig::new(
        "amqp://localhost",
        "shop",
        "orders",
    ));
    let prepared = sink
        .prepare(RabbitMqPublish::new("order".to_owned()))
        .unwrap();
    assert_eq!(prepared.routing_key(), "orders");
    assert_eq!(prepared.payload(), b"order");
    let prepared = sink
        .prepare(RabbitMqPublish::new("order".to_owned()).with_routing_key("audit"))
        .unwrap();
    assert_eq!(prepared.routing_key(), "audit");
    let too_long = RabbitMqPublish::new("order".to_owned()).with_routing_key("k".repeat(256));
    assert!(sink.prepare(too_long).is_err());
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

fn uri() -> String {
    env::var("RABBITMQ_URL").unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672/%2f".into())
}

/// Sends a request to the management HTTP API at `RABBITMQ_MANAGEMENT_ADDR`
/// (default: `127.0.0.1:15672`) as `guest` and returns the response body.
async fn management(method: &str, path: &str, body: &str) -> anyhow::Result<String> {
    let address = env::var("RABBITMQ_MANAGEMENT_ADDR").unwrap_or_else(|_| "127.0.0.1:15672".into());
    let request = format!(
        "{method} {path} HTTP/1.0\r\nHost: {address}\r\nAuthorization: Basic Z3Vlc3Q6Z3Vlc3Q=\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut stream = TcpStream::connect(&address).await?;
    stream.write_all(request.as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    let status = response.split(' ').nth(1).unwrap_or_default();
    anyhow::ensure!(
        status.starts_with('2'),
        "{method} {path} failed: {response}"
    );
    let body = response.split_once("\r\n\r\n").map_or("", |(_, body)| body);
    Ok(body.to_owned())
}

async fn declare_queue(prefix: &str) -> anyhow::Result<String> {
    let queue = unique_name(prefix);
    management(
        "PUT",
        &format!("/api/queues/%2F/{queue}"),
        r#"{"durable":true,"auto_delete":false}"#,
    )
    .await?;
    Ok(queue)
}

/// Closes every broker connection whose name is `name`.
async fn close_connections(name: &str) -> anyhow::Result<usize> {
    let connections: serde_json::Value = serde_json::from_str(
        &management(
            "GET",
            "/api/connections?columns=name,user_provided_name",
            "",
        )
        .await?,
    )?;
    let mut closed = 0;
    for connection in connections.as_array().into_iter().flatten() {
        if connection["user_provided_name"] == name {
            let id = connection["name"].as_str().unwrap_or_default();
            let encoded: String = id
                .bytes()
                .map(|byte| match byte {
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' => {
                        (byte as char).to_string()
                    }
                    _ => format!("%{byte:02X}"),
                })
                .collect();
            management("DELETE", &format!("/api/connections/{encoded}"), "").await?;
            closed += 1;
        }
    }
    Ok(closed)
}

fn source(queue: &str) -> RabbitMqSource<Utf8, String> {
    RabbitMqSource::new(RabbitMqSourceConfig::new(uri(), queue))
}

fn queue_sink(queue: &str) -> RabbitMqSink<Utf8, String> {
    RabbitMqSink::new(RabbitMqSinkConfig::new(uri(), "", queue))
}

/// Receives the next message, retrying receive errors, or `None` after `timeout`.
async fn next_message(
    source: &mut RabbitMqSource<Utf8, String>,
    timeout: Duration,
) -> anyhow::Result<Option<RabbitMqMessage<Utf8, String>>> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, source.receive()).await {
            Err(_) => return Ok(None),
            Ok(Ok(Receive::Message(message))) => return Ok(Some(message)),
            Ok(Ok(Receive::End)) => anyhow::bail!("RabbitMQ source ended"),
            Ok(Err(ReceiveError::Retry(_))) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(Err(ReceiveError::Fatal(error))) => {
                anyhow::bail!("RabbitMQ receive failed: {error:#}")
            }
        }
    }
}

async fn publish(sink: &RabbitMqSink<Utf8, String>, value: &str) -> anyhow::Result<()> {
    let prepared = sink.prepare(RabbitMqPublish::new(value.to_owned()))?;
    tokio::time::timeout(Duration::from_secs(20), sink.publish(&prepared))
        .await
        .expect("timed out publishing to RabbitMQ")
}

#[tokio::test]
#[ignore = "requires a RabbitMQ broker; run with cargo test --features rabbitmq -- --ignored"]
async fn publish_receive_and_ack_against_rabbitmq() -> anyhow::Result<()> {
    let queue = declare_queue("beavers-rabbitmq-test").await?;
    let sink = queue_sink(&queue);
    let mut output = RabbitMqPublish::new("hello rabbitmq".to_owned());
    output
        .headers
        .insert("kind".into(), RabbitMqValue::from("test"));
    output.headers.insert("count".into(), RabbitMqValue::I32(3));
    output.properties.message_id = Some("m-1".into());
    output.properties.content_type = Some("text/plain".into());
    let prepared = sink.prepare(output)?;
    tokio::time::timeout(Duration::from_secs(20), sink.publish(&prepared))
        .await
        .expect("timed out publishing to RabbitMQ")?;
    sink.close().await?;

    let mut source = source(&queue);
    let message = next_message(&mut source, Duration::from_secs(20))
        .await?
        .expect("no RabbitMQ delivery");
    let record = message.decode()?;
    assert_eq!(record.value, "hello rabbitmq");
    assert_eq!(record.headers["kind"], RabbitMqValue::from("test"));
    assert_eq!(record.headers["count"], RabbitMqValue::I32(3));
    assert_eq!(record.properties.message_id.as_deref(), Some("m-1"));
    assert_eq!(
        record.properties.content_type.as_deref(),
        Some("text/plain")
    );
    assert_eq!(record.metadata.queue, queue);
    assert_eq!(record.metadata.exchange, "");
    assert_eq!(record.metadata.routing_key, queue);
    assert!(record.metadata.persistent);
    assert!(!record.metadata.redelivered);
    message.ack().await?;
    source.close().await?;

    let mut again = self::source(&queue);
    assert!(
        next_message(&mut again, Duration::from_secs(1))
            .await?
            .is_none(),
        "an acknowledged message was delivered again"
    );
    again.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a RabbitMQ broker; run with cargo test --features rabbitmq -- --ignored"]
async fn unacknowledged_messages_are_redelivered_after_close() -> anyhow::Result<()> {
    let queue = declare_queue("beavers-rabbitmq-redeliver").await?;
    let sink = queue_sink(&queue);
    publish(&sink, "order").await?;
    sink.close().await?;

    let mut first = source(&queue);
    let message = next_message(&mut first, Duration::from_secs(20))
        .await?
        .expect("no RabbitMQ delivery");
    drop(message);
    first.close().await?;

    let mut second = source(&queue);
    let message = next_message(&mut second, Duration::from_secs(20))
        .await?
        .expect("the unacknowledged message was not redelivered");
    let record = message.decode()?;
    assert_eq!(record.value, "order");
    assert!(record.metadata.redelivered);
    message.ack().await?;
    second.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a RabbitMQ broker; run with cargo test --features rabbitmq -- --ignored"]
async fn a_lost_connection_revokes_its_deliveries() -> anyhow::Result<()> {
    let queue = declare_queue("beavers-rabbitmq-revoke").await?;
    let sink = queue_sink(&queue);
    publish(&sink, "order").await?;
    sink.close().await?;

    let mut source = source(&queue);
    let message = next_message(&mut source, Duration::from_secs(20))
        .await?
        .expect("no RabbitMQ delivery");
    let revoked = message
        .revocation()
        .expect("RabbitMQ deliveries are revocable");
    assert!(!revoked.is_cancelled());

    // The management API lists a new connection after its statistics interval.
    let name = format!("beavers source {queue}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while close_connections(&name).await? == 0 {
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the source's connection was not listed"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let redelivered = next_message(&mut source, Duration::from_secs(30))
        .await?
        .expect("the message was not redelivered after reconnecting");
    assert!(revoked.is_cancelled());
    assert!(message.ack().await.is_err());
    let record = redelivered.decode()?;
    assert_eq!(record.value, "order");
    assert!(record.metadata.redelivered);
    redelivered.ack().await?;
    source.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a RabbitMQ broker; run with cargo test --features rabbitmq -- --ignored"]
async fn unroutable_messages_fail_their_completion() -> anyhow::Result<()> {
    let sink = RabbitMqSink::<Utf8, String>::new(RabbitMqSinkConfig::new(
        uri(),
        "amq.direct",
        unique_name("beavers-unbound"),
    ));
    let error = publish(&sink, "lost").await.unwrap_err();
    assert!(format!("{error:#}").contains("unroutable"), "{error:#}");

    let mut config = RabbitMqSinkConfig::new(uri(), "amq.direct", unique_name("beavers-unbound"));
    config.mandatory = false;
    let discarding = RabbitMqSink::<Utf8, String>::new(config);
    publish(&discarding, "discarded").await?;
    sink.close().await?;
    discarding.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a RabbitMQ broker; run with cargo test --features rabbitmq -- --ignored"]
async fn submitted_messages_complete_on_their_confirmations() -> anyhow::Result<()> {
    let queue = declare_queue("beavers-rabbitmq-submit").await?;
    let mut config = RabbitMqSinkConfig::new(uri(), "", &queue);
    config.max_pending = 2;
    let sink = RabbitMqSink::<Utf8, String>::new(config);
    let prepared = sink.prepare(RabbitMqPublish::new("a".to_owned()))?;

    let first = tokio::time::timeout(Duration::from_secs(20), sink.submit(&prepared))
        .await
        .expect("timed out connecting")?;
    let second = sink.submit(&prepared).await?;
    assert!(!first.is_done() && !second.is_done());
    let blocked = tokio::time::timeout(Duration::from_millis(200), sink.submit(&prepared)).await;
    assert!(
        blocked.is_err(),
        "a third message was accepted beyond max_pending"
    );
    for completion in [first, second] {
        tokio::time::timeout(Duration::from_secs(20), completion.wait())
            .await
            .expect("timed out waiting for a publisher confirmation")?;
    }
    tokio::time::timeout(Duration::from_secs(20), sink.publish(&prepared))
        .await
        .expect("timed out publishing after the completions freed their slots")?;
    sink.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a RabbitMQ broker; run with cargo test --features rabbitmq -- --ignored"]
async fn pipeline_forwards_headers_and_acknowledges_every_input() -> anyhow::Result<()> {
    let input = declare_queue("beavers-rabbitmq-input").await?;
    let output = declare_queue("beavers-rabbitmq-output").await?;
    let producer = queue_sink(&input);
    for order in ["a", "b", "c"] {
        let mut message = RabbitMqPublish::new(order.to_owned());
        message
            .headers
            .insert("order".into(), RabbitMqValue::from(order));
        message.properties.correlation_id = Some(format!("c-{order}"));
        let prepared = producer.prepare(message)?;
        producer.publish(&prepared).await?;
    }
    producer.close().await?;

    let shutdown = CancellationToken::new();
    let app = App::new().subscription(Subscription::forward(
        "rabbitmq-pipeline",
        source(&input),
        queue_sink(&output),
        |order: String| async move { Ok(order.to_uppercase()) },
    ));
    let running = tokio::spawn(app.run_until(shutdown.clone()));

    let mut reader = source(&output);
    let mut received = Vec::new();
    while received.len() < 3 {
        let message = next_message(&mut reader, Duration::from_secs(20))
            .await?
            .expect("the pipeline did not publish every output");
        let record = message.decode()?;
        message.ack().await?;
        received.push(record);
    }
    reader.close().await?;
    shutdown.cancel();
    running.await??;

    received.sort_by(|a, b| a.value.cmp(&b.value));
    for (record, order) in received.iter().zip(["a", "b", "c"]) {
        assert_eq!(record.value, order.to_uppercase());
        assert_eq!(record.headers["order"], RabbitMqValue::from(order));
        assert_eq!(
            record.properties.correlation_id.as_deref(),
            Some(format!("c-{order}").as_str())
        );
    }
    let mut leftover = source(&input);
    assert!(
        next_message(&mut leftover, Duration::from_secs(1))
            .await?
            .is_none(),
        "an input was left unacknowledged"
    );
    leftover.close().await?;
    Ok(())
}
