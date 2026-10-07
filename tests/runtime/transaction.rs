use super::fixtures::{TransactionalSource, Transactions, fast};
use beavers::{
    App, BoxError, CancellationToken, Emit, ErrorPolicy, FailureAction, HandlerError, InMemorySink,
    ProcessingOrder, RetryPolicy, Subscription, TransactionBatch,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[tokio::test]
async fn outputs_and_acknowledgement_commit_in_one_transaction() {
    let source = TransactionalSource::new([1, 2]);
    let acks = source.acks.clone();
    let sink = Transactions::default();
    App::new()
        .subscription(
            Subscription::new_emitting("transactions", source, sink.clone(), |n: i32| async move {
                Ok(Emit::Many(vec![n * 10, n * 10 + 1]))
            })
            .transactional(),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(
        *sink.committed.lock().unwrap(),
        vec![(1, vec![10, 11]), (2, vec![20, 21])]
    );
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn deliveries_without_output_commit_empty_transactions() {
    let source = TransactionalSource::new([0, 1, 2, 3]);
    let acks = source.acks.clone();
    let sink = Transactions::default();
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new_emitting("transactions", source, sink.clone(), |n: i32| async move {
                match n {
                    1 => Ok(Emit::None),
                    2 => Err(HandlerError::Reject(BoxError::from("rejected"))),
                    _ => Ok(Emit::One(-n)),
                }
            })
            .transactional()
            .dlq(dlq.clone())
            .error_policy(ErrorPolicy {
                decode: FailureAction::Discard,
                encode: FailureAction::Discard,
                ..ErrorPolicy::default()
            }),
        )
        .run()
        .await
        .unwrap();
    // Zero fails to decode, one emits nothing, two is dead-lettered outside the
    // transaction, and three fails to encode; each commits without outputs.
    assert_eq!(
        *sink.committed.lock().unwrap(),
        vec![(0, vec![]), (1, vec![]), (2, vec![]), (3, vec![])]
    );
    assert_eq!(dlq.values().len(), 1);
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn failed_commits_are_retried_with_the_prepared_outputs() {
    let sink = Transactions {
        failures: 2,
        ..Transactions::default()
    };
    App::new()
        .subscription(
            Subscription::new(
                "transactions",
                TransactionalSource::new([4]),
                sink.clone(),
                |n: i32| async move { Ok(n) },
            )
            .transactional()
            .publish_retry(fast()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 3);
    assert_eq!(*sink.committed.lock().unwrap(), vec![(4, vec![4])]);
}

#[tokio::test]
async fn exhausted_commit_retries_stop_without_acknowledgement() {
    let source = TransactionalSource::new([4]);
    let acks = source.acks.clone();
    let sink = Transactions {
        failures: usize::MAX,
        ..Transactions::default()
    };
    let error = App::new()
        .subscription(
            Subscription::new("transactions", source, sink.clone(), |n: i32| async move {
                Ok(n)
            })
            .transactional()
            .publish_retry(fast()),
        )
        .run()
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("transaction aborted"));
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 3);
    assert!(sink.committed.lock().unwrap().is_empty());
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn transactional_subscriptions_require_per_key_ordering() {
    let error = App::new()
        .subscription(
            Subscription::new(
                "transactions",
                TransactionalSource::new([1]),
                Transactions::default(),
                |n: i32| async move { Ok(n) },
            )
            .transactional()
            .ordering(ProcessingOrder::Unordered),
        )
        .run()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("ProcessingOrder::PerKey"));
}

#[tokio::test]
async fn the_source_is_verified_once_before_processing() {
    let sink = Transactions {
        verify_failures: 2,
        ..Transactions::default()
    };
    App::new()
        .subscription(
            Subscription::new(
                "transactions",
                TransactionalSource::new([1, 2]),
                sink.clone(),
                |n: i32| async move { Ok(n) },
            )
            .transactional()
            .publish_retry(fast()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.verifications.load(Ordering::SeqCst), 3);
    assert_eq!(
        *sink.committed.lock().unwrap(),
        vec![(1, vec![1]), (2, vec![2])]
    );
}

#[tokio::test]
async fn an_incompatible_source_stops_the_subscription_before_processing() {
    let source = TransactionalSource::new([1]);
    let acks = source.acks.clone();
    let handled = Arc::new(AtomicUsize::new(0));
    let calls = handled.clone();
    let sink = Transactions {
        verify_failures: usize::MAX,
        ..Transactions::default()
    };
    let error = App::new()
        .subscription(
            Subscription::new("transactions", source, sink.clone(), move |n: i32| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok(n) }
            })
            .transactional()
            .publish_retry(fast()),
        )
        .run()
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("source on another cluster"));
    assert_eq!(handled.load(Ordering::SeqCst), 0);
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 0);
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn deliveries_commit_in_batches_bounded_by_size() {
    let sink = Transactions::default();
    App::new()
        .subscription(
            Subscription::new(
                "transactions",
                TransactionalSource::new(1..=10),
                sink.clone(),
                |n: i32| async move { Ok(n) },
            )
            .transactional()
            .transaction_batch(TransactionBatch::new(4, Duration::from_millis(200))),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(*sink.batches.lock().unwrap(), vec![4, 4, 2]);
    let committed = sink.committed.lock().unwrap();
    assert_eq!(
        committed.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
        (1..=10).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_single_delivery_batch_commits_each_delivery_alone() {
    let sink = Transactions::default();
    App::new()
        .subscription(
            Subscription::new(
                "transactions",
                TransactionalSource::new(1..=3),
                sink.clone(),
                |n: i32| async move { Ok(n) },
            )
            .transactional()
            .transaction_batch(TransactionBatch::single()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(*sink.batches.lock().unwrap(), vec![1, 1, 1]);
}

#[tokio::test]
async fn a_failed_batch_is_retried_as_a_whole() {
    let sink = Transactions {
        failures: 1,
        ..Transactions::default()
    };
    App::new()
        .subscription(
            Subscription::new(
                "transactions",
                TransactionalSource::new(1..=3),
                sink.clone(),
                |n: i32| async move { Ok(n) },
            )
            .transactional()
            .transaction_batch(TransactionBatch::new(3, Duration::from_secs(5)))
            .publish_retry(fast()),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 2);
    assert_eq!(*sink.batches.lock().unwrap(), vec![3]);
}

#[tokio::test]
async fn a_revoked_delivery_leaves_the_batch_without_spending_a_retry() {
    let revoked = CancellationToken::new();
    let mut source = TransactionalSource::new(1..=3);
    source.revocable = Some((2, revoked.clone()));
    let sink = Transactions {
        failures: 1,
        revoke_on_failure: Some(revoked),
        ..Transactions::default()
    };
    App::new()
        .subscription(
            Subscription::new("transactions", source, sink.clone(), |n: i32| async move {
                Ok(n)
            })
            .transactional()
            .transaction_batch(TransactionBatch::new(3, Duration::from_secs(5)))
            .publish_retry(RetryPolicy {
                max_attempts: 1,
                ..fast()
            }),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(
        *sink.committed.lock().unwrap(),
        vec![(1, vec![1]), (3, vec![3])]
    );
}

#[tokio::test]
async fn batches_after_a_failed_batch_do_not_commit() {
    let sink = Transactions {
        failures: usize::MAX,
        ..Transactions::default()
    };
    let error = App::new()
        .subscription(
            Subscription::new(
                "transactions",
                TransactionalSource::new(1..=4),
                sink.clone(),
                |n: i32| async move { Ok(n) },
            )
            .transactional()
            .transaction_batch(TransactionBatch::new(2, Duration::from_secs(5)))
            .publish_retry(fast()),
        )
        .run()
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("transaction aborted"));
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 3);
    assert!(sink.committed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_zero_transaction_batch_size_fails_validation() {
    let error = App::new()
        .subscription(
            Subscription::new(
                "transactions",
                TransactionalSource::new([1]),
                Transactions::default(),
                |n: i32| async move { Ok(n) },
            )
            .transactional()
            .transaction_batch(TransactionBatch::new(0, Duration::ZERO)),
        )
        .run()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("transaction batch size"));
}

#[tokio::test]
async fn a_rejected_batch_commit_stops_without_retrying() {
    let source = TransactionalSource::new(1..=2);
    let sink = Transactions {
        failures: usize::MAX,
        reject: true,
        ..Transactions::default()
    };
    let error = App::new()
        .subscription(
            Subscription::new("transactions", source, sink.clone(), |n: i32| async move {
                Ok(n)
            })
            .transactional()
            .publish_retry(fast()),
        )
        .run()
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("cannot route a rejected output"));
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 1);
    assert!(sink.committed.lock().unwrap().is_empty());
}
