use super::fixtures::{Flaky, RejectNegative, TextSource, fast};
use beavers::{
    App, Classify, DeadLetter, Emit, ErrorPolicy, FailureAction, FailureKind, HandlerError,
    InMemorySink, IterSource, RetryPolicy, Subscription,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn text_source(payloads: &[&'static str]) -> (TextSource, Arc<AtomicUsize>) {
    let acks = Arc::new(AtomicUsize::new(0));
    let source = TextSource {
        payloads: payloads.iter().copied().collect(),
        acks: acks.clone(),
    };
    (source, acks)
}

#[tokio::test]
async fn decode_failure_dead_letters_raw_delivery_and_continues() {
    let (source, acks) = text_source(&["1", "oops", "3"]);
    let sink = InMemorySink::default();
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(source, sink.clone(), |n| async move { Ok(n) })
                .dlq(dlq.clone())
                .error_policy(ErrorPolicy {
                    decode: FailureAction::DeadLetter,
                    ..ErrorPolicy::default()
                }),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![1, 3]);
    let [dead_letter]: [DeadLetter<i32, Vec<u8>>; 1] = dlq.values().try_into().unwrap();
    assert_eq!(dead_letter.failure, FailureKind::Decode);
    assert_eq!(dead_letter.attempts, 0);
    assert_eq!(dead_letter.input, None);
    assert_eq!(dead_letter.raw, b"oops");
    assert!(dead_letter.error.contains("invalid digit"));
    assert_eq!(acks.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn discard_acknowledges_without_output() {
    let (source, acks) = text_source(&["oops", "2"]);
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(source, sink.clone(), |n| async move { Ok(n) }).error_policy(
                ErrorPolicy {
                    decode: FailureAction::Discard,
                    ..ErrorPolicy::default()
                },
            ),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![2]);
    assert_eq!(acks.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn propagated_errors_are_dead_lettered_without_retry_by_default() {
    for with_dlq in [true, false] {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let (source, acks) = text_source(&["5"]);
        let dlq = InMemorySink::default();
        let mut subscription = Subscription::new(source, InMemorySink::default(), move |_: i32| {
            counter.fetch_add(1, Ordering::SeqCst);
            async {
                let value: i32 = "invalid".parse()?;
                Ok(value)
            }
        })
        .retry(fast());
        if with_dlq {
            subscription = subscription.dlq(dlq.clone());
        }
        let result = App::new().subscription(subscription).run().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        if with_dlq {
            result.unwrap();
            let [letter]: [DeadLetter<i32, Vec<u8>>; 1] = dlq.values().try_into().unwrap();
            assert_eq!(letter.failure, FailureKind::Rejected);
            assert_eq!(letter.attempts, 1);
            assert_eq!(letter.input, Some(5));
            assert_eq!(letter.raw, b"5");
            assert_eq!(acks.load(Ordering::SeqCst), 1);
        } else {
            let error = format!("{:#}", result.unwrap_err());
            assert!(error.contains("no dead-letter sink configured"), "{error}");
            assert!(error.contains("handler rejected input"), "{error}");
            assert_eq!(acks.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn retry_exhaustion_can_stop() {
    let (source, acks) = text_source(&["5"]);
    let dlq = InMemorySink::<DeadLetter<i32, Vec<u8>>>::default();
    let error = App::new()
        .subscription(
            Subscription::new(source, InMemorySink::default(), |_: i32| async {
                Err::<i32, _>(HandlerError::Retry(anyhow::anyhow!("busy")))
            })
            .retry(fast())
            .dlq(dlq.clone())
            .error_policy(ErrorPolicy {
                retry_exhausted: FailureAction::Stop,
                ..ErrorPolicy::default()
            }),
        )
        .run()
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("handler retry exhausted"));
    assert!(dlq.values().is_empty());
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn encode_failure_dead_letters_input_without_partial_publish() {
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    let sink = RejectNegative::default();
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new_emitting(source, sink.clone(), |n: i32| async move {
                Ok(Emit::Many(vec![n, -n]))
            })
            .dlq(dlq.clone())
            .error_policy(ErrorPolicy::dead_letter_all()),
        )
        .run()
        .await
        .unwrap();
    assert!(sink.0.lock().unwrap().is_empty());
    let [letter]: [DeadLetter<i32, ()>; 1] = dlq.values().try_into().unwrap();
    assert_eq!(letter.failure, FailureKind::Encode);
    assert_eq!(letter.input, Some(1));
    assert!(letter.error.contains("negative output"));
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dead_letter_policy_without_sink_fails_validation() {
    let (source, acks) = text_source(&["1"]);
    let sink = InMemorySink::<i32>::default();
    let error = App::new()
        .subscription(
            Subscription::new(source, sink.clone(), |n| async move { Ok(n) })
                .error_policy(ErrorPolicy::dead_letter_all()),
        )
        .run()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no dead-letter sink"), "{error}");
    assert!(sink.values().is_empty());
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dead_letter_retry_is_independent_of_publish_retry() {
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    let calls = Arc::new(AtomicUsize::new(0));
    let dlq = Flaky {
        calls: calls.clone(),
        acks: acks.clone(),
        fail_always: false,
    };
    App::new()
        .subscription(
            Subscription::new(source, InMemorySink::<i32>::default(), |_| async {
                Err(HandlerError::Reject(anyhow::anyhow!("reject")))
            })
            .dlq_with(dlq, |letter| Ok(letter.input.unwrap()))
            .publish_retry(RetryPolicy {
                max_attempts: 1,
                ..fast()
            })
            .dlq_retry(fast()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dead_letter_conversion_failure_does_not_ack() {
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    let dlq = InMemorySink::<i32>::default();
    let error = App::new()
        .subscription(
            Subscription::new(source, InMemorySink::<i32>::default(), |_| async {
                Err(HandlerError::Reject(anyhow::anyhow!("reject")))
            })
            .dlq_with(dlq.clone(), |_| anyhow::bail!("unsupported")),
        )
        .run()
        .await
        .unwrap_err();
    let error = format!("{error:#}");
    assert!(error.contains("prepare dead letter failed"), "{error}");
    assert!(error.contains("handler rejected input: reject"), "{error}");
    assert!(dlq.values().is_empty());
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dead_letter_serializes_failure_context() {
    let (source, _) = text_source(&["x"]);
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                source,
                InMemorySink::default(),
                |n: i32| async move { Ok(n) },
            )
            .name("numbers")
            .dlq(dlq.clone())
            .error_policy(ErrorPolicy::dead_letter_all()),
        )
        .run()
        .await
        .unwrap();
    let [letter]: [DeadLetter<i32, Vec<u8>>; 1] = dlq.values().try_into().unwrap();
    let json = serde_json::to_value(&letter).unwrap();
    assert_eq!(json["subscription"], "numbers");
    assert_eq!(json["failure"], "decode");
    assert_eq!(json["input"], serde_json::Value::Null);
    assert_eq!(json["raw"], serde_json::json!([120]));
}

#[tokio::test]
async fn retry_classified_errors_are_retried_then_dead_lettered() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let (source, acks) = text_source(&["5"]);
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(source, InMemorySink::default(), move |_: i32| {
                counter.fetch_add(1, Ordering::SeqCst);
                async {
                    let value: i32 = "busy".parse().retry()?;
                    Ok(value)
                }
            })
            .retry(fast())
            .dlq(dlq.clone()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let [letter]: [DeadLetter<i32, Vec<u8>>; 1] = dlq.values().try_into().unwrap();
    assert_eq!(letter.failure, FailureKind::RetryExhausted);
    assert_eq!(letter.attempts, 3);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fatal_errors_stop_without_retry_or_dead_letter() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let (source, acks) = text_source(&["5"]);
    let dlq = InMemorySink::<DeadLetter<i32, Vec<u8>>>::default();
    let result = App::new()
        .subscription(
            Subscription::new(source, InMemorySink::default(), move |_: i32| {
                counter.fetch_add(1, Ordering::SeqCst);
                async {
                    let value: i32 = "broken".parse().fatal()?;
                    Ok(value)
                }
            })
            .retry(fast())
            .dlq(dlq.clone()),
        )
        .run()
        .await;
    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(dlq.values().is_empty());
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn reject_classified_errors_match_propagated_errors() {
    let (source, acks) = text_source(&["5"]);
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(source, InMemorySink::default(), |_: i32| async {
                let value: i32 = "invalid".parse().reject()?;
                Ok(value)
            })
            .dlq(dlq.clone()),
        )
        .run()
        .await
        .unwrap();
    let [letter]: [DeadLetter<i32, Vec<u8>>; 1] = dlq.values().try_into().unwrap();
    assert_eq!(letter.failure, FailureKind::Rejected);
    assert_eq!(letter.attempts, 1);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}
