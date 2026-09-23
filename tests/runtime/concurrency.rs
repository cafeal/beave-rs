use beavers::{App, CancellationToken, InMemorySink, IterSource, Result, Subscription};
use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[tokio::test]
async fn shutdown_aborts_timed_out_processing_without_ack() {
    let token = CancellationToken::new();
    let stop = token.clone();
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    let app = App::new().subscription(
        Subscription::new(source, InMemorySink::<i32>::default(), move |_| {
            stop.cancel();
            async { pending::<Result<i32>>().await }
        })
        .drain_timeout(Duration::from_millis(10)),
    );
    assert!(app.run_until(token).await.is_err());
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn backpressure_limits_receiving_and_allows_parallel_work() {
    let source = IterSource::new(0..6);
    let acks = source.acknowledgements();
    let entered = Arc::new(AtomicUsize::new(0));
    let count = entered.clone();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let handler_gate = gate.clone();
    let app = App::new().subscription(
        Subscription::new(source, InMemorySink::default(), move |n| {
            let gate = handler_gate.clone();
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                gate.acquire().await.unwrap().forget();
                Ok(n)
            }
        })
        .concurrency(4)
        .max_in_flight(2),
    );
    let run = tokio::spawn(app.run());
    tokio::time::timeout(Duration::from_secs(1), async {
        while entered.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(entered.load(Ordering::SeqCst), 2);
    assert_eq!(acks.load(Ordering::SeqCst), 0);
    gate.add_permits(6);
    run.await.unwrap().unwrap();
    assert_eq!(acks.load(Ordering::SeqCst), 6);
}
