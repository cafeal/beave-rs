//! Contract tests use custom implementations rather than the built-in adapters.
use beavers::{
    App, BoxError, Classify, Error, Handler, InMemorySink, IterSource, Receive, ReceiveError,
    Result, RetryPolicy, Sink, Source, SourceMessage, Subscription,
};
use std::{
    result::Result as StdResult,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct RawMessage {
    fail_decode: bool,
    decoded: Arc<AtomicUsize>,
    acked: Arc<AtomicUsize>,
}
impl SourceMessage for RawMessage {
    type Item = i32;
    type Raw = ();
    fn decode(&self) -> StdResult<i32, BoxError> {
        self.decoded.fetch_add(1, Ordering::SeqCst);
        if !(!self.fail_decode) {
            return Err("decode failed".into());
        };
        Ok(21)
    }
    fn raw(&self) {}
    async fn ack(self) -> StdResult<(), BoxError> {
        self.acked.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct RawSource(Option<RawMessage>);
impl Source for RawSource {
    type Message = RawMessage;
    async fn receive(&mut self) -> StdResult<Receive<RawMessage>, ReceiveError> {
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
    fn prepare(&self, output: i32) -> StdResult<String, BoxError> {
        self.prepared.fetch_add(1, Ordering::SeqCst);
        if !(!self.fail_prepare) {
            return Err("encode failed".into());
        };
        Ok(output.to_string())
    }
    async fn publish(&self, output: &String) -> StdResult<(), BoxError> {
        assert_eq!(output, "42");
        let attempt = self.published.fetch_add(1, Ordering::SeqCst);
        if !(attempt >= 1) {
            return Err("transient transport failure".into());
        };
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
            Subscription::new(
                "custom_message_handler_and_prepared_output_work_through_public_contracts",
                source,
                sink,
                Double,
            )
            .publish_retry(RetryPolicy {
                max_attempts: 2,
                initial_delay: Duration::ZERO,
                max_delay: Duration::ZERO,
                ..RetryPolicy::default()
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
                .subscribe(
                    "decode_and_prepare_failures_do_not_publish_or_ack",
                    source,
                    sink,
                    Double
                )
                .run()
                .await
                .is_err()
        );
        assert_eq!(published.load(Ordering::SeqCst), 0);
        assert_eq!(acked.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn app_errors_report_invalid_configuration_and_the_failed_subscription() {
    let error = App::new()
        .subscribe(
            " ",
            IterSource::new([1]),
            InMemorySink::default(),
            |value: i32| async move { Ok(value) },
        )
        .run()
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Config(_)), "{error:?}");

    let error = App::new()
        .subscribe(
            "parse",
            IterSource::new(["x".to_owned()]),
            InMemorySink::<i32>::default(),
            |text: String| async move { text.parse::<i32>().fatal() },
        )
        .run()
        .await
        .unwrap_err();
    let Error::Subscription { name, source } = &error else {
        panic!("expected a subscription failure: {error:?}");
    };
    assert_eq!(name, "parse");
    assert!(
        source.to_string().contains("fatal handler error"),
        "{error:#}"
    );
    assert!(
        format!("{error:#}").contains("invalid digit found in string"),
        "{error:#}"
    );
}

#[tokio::test]
async fn handler_errors_accept_std_errors_strings_box_errors_and_anyhow() {
    async fn handle(text: String) -> Result<i32> {
        if text.is_empty() {
            return Err("empty input".into());
        }
        let boxed: StdResult<(), BoxError> = Ok(());
        boxed?;
        let checked: anyhow::Result<()> = Ok(());
        checked?;
        Ok(text.parse::<i32>()?)
    }
    let sink = InMemorySink::default();
    App::new()
        .subscribe(
            "parse",
            IterSource::new(["7".to_owned()]),
            sink.clone(),
            handle,
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), [7]);
}
