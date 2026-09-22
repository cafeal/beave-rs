//! Contract tests use custom implementations rather than the built-in adapters.
use beavers::{
    App, Handler, Receive, ReceiveError, Result, RetryPolicy, Sink, Source, SourceMessage,
    Subscription,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

struct RawMessage {
    fail_decode: bool,
    decoded: Arc<AtomicUsize>,
    acked: Arc<AtomicUsize>,
}
impl SourceMessage for RawMessage {
    type Item = i32;
    fn decode(&self) -> anyhow::Result<i32> {
        self.decoded.fetch_add(1, Ordering::SeqCst);
        anyhow::ensure!(!self.fail_decode, "decode failed");
        Ok(21)
    }
    async fn ack(self) -> anyhow::Result<()> {
        self.acked.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct RawSource(Option<RawMessage>);
impl Source for RawSource {
    type Message = RawMessage;
    async fn receive(&mut self) -> std::result::Result<Receive<RawMessage>, ReceiveError> {
        Ok(match self.0.take() {
            Some(message) => Receive::Message(message),
            None => Receive::End,
        })
    }
}
struct Double;
impl Handler<i32> for Double {
    type Output = i32;
    async fn handle(&self, input: i32) -> Result<i32> {
        Ok(input * 2)
    }
}
struct PreparedSink {
    prepared: Arc<AtomicUsize>,
    published: Arc<AtomicUsize>,
    fail_prepare: bool,
}
impl Sink<i32> for PreparedSink {
    type Prepared = String;
    fn prepare(&self, output: i32) -> anyhow::Result<String> {
        self.prepared.fetch_add(1, Ordering::SeqCst);
        anyhow::ensure!(!self.fail_prepare, "encode failed");
        Ok(output.to_string())
    }
    async fn publish(&self, output: &String) -> anyhow::Result<()> {
        assert_eq!(output, "42");
        let attempt = self.published.fetch_add(1, Ordering::SeqCst);
        anyhow::ensure!(attempt >= 1, "transient transport failure");
        Ok(())
    }
}
#[tokio::test]
async fn custom_message_handler_and_prepared_output_work_through_public_contracts() {
    let decoded = Arc::default();
    let acked = Arc::new(AtomicUsize::new(0));
    let prepared = Arc::new(AtomicUsize::new(0));
    let published = Arc::new(AtomicUsize::new(0));
    let source = RawSource(Some(RawMessage {
        fail_decode: false,
        decoded,
        acked: acked.clone(),
    }));
    let sink = PreparedSink {
        prepared: prepared.clone(),
        published: published.clone(),
        fail_prepare: false,
    };
    App::new()
        .subscription(
            Subscription::new(source, sink, Double).publish_retry(RetryPolicy {
                max_attempts: 2,
                initial_delay: Duration::ZERO,
                max_delay: Duration::ZERO,
            }),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(prepared.load(Ordering::SeqCst), 1);
    assert_eq!(published.load(Ordering::SeqCst), 2);
    assert_eq!(acked.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn decode_and_prepare_failures_do_not_publish_or_ack() {
    for fail_decode in [false, true] {
        let acked = Arc::new(AtomicUsize::new(0));
        let published = Arc::new(AtomicUsize::new(0));
        let source = RawSource(Some(RawMessage {
            fail_decode,
            decoded: Arc::default(),
            acked: acked.clone(),
        }));
        let sink = PreparedSink {
            prepared: Arc::default(),
            published: published.clone(),
            fail_prepare: true,
        };
        assert!(
            App::new()
                .subscribe(source, sink, Double)
                .run()
                .await
                .is_err()
        );
        assert_eq!(published.load(Ordering::SeqCst), 0);
        assert_eq!(acked.load(Ordering::SeqCst), 0);
    }
}
