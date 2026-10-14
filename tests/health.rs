use beavers::{
    App, BoxError, CancellationToken, Delivery, Error, Health, Receive, ReceiveError, RetryPolicy,
    Sink, Source, SourceProbe, Subscription, health::SubscriptionStatus,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, mpsc},
    task::JoinHandle,
    time::{sleep, timeout},
};

/// Receives what the test sends: a value, or `None` for a retryable failure.
/// The source ends when the sender is dropped.
struct Scripted(mpsc::UnboundedReceiver<Option<i32>>);

impl Source for Scripted {
    type Message = Delivery<i32>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        match self.0.recv().await {
            Some(Some(value)) => Ok(Receive::Message(Delivery::untracked(value))),
            Some(None) => Err(ReceiveError::Retry(BoxError::from("broker unavailable"))),
            None => Ok(Receive::End),
        }
    }
}

/// Fails every publication while `down` is set.
#[derive(Clone, Default)]
struct Switch {
    down: Arc<AtomicBool>,
}

impl Sink<i32> for Switch {
    type Prepared = i32;

    fn prepare(&self, value: i32) -> Result<i32, BoxError> {
        Ok(value)
    }

    async fn publish(&self, _: &i32) -> Result<(), BoxError> {
        if !(!self.down.load(Ordering::SeqCst)) {
            return Err("sink unavailable".into());
        };
        Ok(())
    }
}

fn patient(delay: Duration) -> RetryPolicy {
    RetryPolicy {
        max_attempts: 1000,
        initial_delay: delay,
        max_delay: delay,
        ..RetryPolicy::default()
    }
}

async fn eventually(health: &Health, condition: impl Fn(&Health) -> bool) {
    timeout(Duration::from_secs(5), async {
        while !condition(health) {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("health never matched: {:?}", health.report()));
}

fn scripted(
    name: &str,
    sink: Switch,
) -> (
    Subscription<Scripted, Switch, i32>,
    mpsc::UnboundedSender<Option<i32>>,
) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let subscription = Subscription::new(name, Scripted(receiver), sink, |value: i32| async move {
        Ok(value)
    });
    (subscription, sender)
}

fn run(app: App, shutdown: &CancellationToken) -> JoinHandle<Result<(), Error>> {
    tokio::spawn(app.run_until(shutdown.clone()))
}

#[tokio::test]
async fn receive_backoff_makes_the_application_unready_until_the_retry() {
    let (subscription, input) = scripted("orders", Switch::default());
    let app =
        App::new().subscription(subscription.receive_retry(patient(Duration::from_millis(200))));
    let health = app.health();
    assert!(!health.is_ready(), "not ready before the application runs");
    let shutdown = CancellationToken::new();
    let running = run(app, &shutdown);
    eventually(&health, Health::is_ready).await;

    input.send(None).unwrap();
    eventually(&health, |health| !health.is_ready()).await;
    let report = health.report();
    assert!(report.live);
    assert_eq!(report.subscriptions[0].receive_failures, 1);
    assert!(report.subscriptions[0].receive_backoff);

    // Receiving again after the delay is ready, though no receive succeeded yet.
    eventually(&health, Health::is_ready).await;
    assert_eq!(health.report().subscriptions[0].receive_failures, 1);
    input.send(Some(1)).unwrap();
    eventually(&health, |health| {
        health.report().subscriptions[0].receive_failures == 0
    })
    .await;

    shutdown.cancel();
    running.await.unwrap().unwrap();
    let report = health.report();
    assert!(report.shutting_down && !report.ready && report.live);
    assert_eq!(report.subscriptions[0].status, SubscriptionStatus::Stopped);
}

#[tokio::test]
async fn publish_retry_makes_the_application_unready_until_it_succeeds() {
    let sink = Switch::default();
    sink.down.store(true, Ordering::SeqCst);
    let (subscription, input) = scripted("orders", sink.clone());
    let app =
        App::new().subscription(subscription.publish_retry(patient(Duration::from_millis(10))));
    let health = app.health();
    let shutdown = CancellationToken::new();
    let running = run(app, &shutdown);

    input.send(Some(1)).unwrap();
    eventually(&health, |health| {
        health.report().subscriptions[0].publish_retries == 1
    })
    .await;
    assert!(!health.is_ready());

    sink.down.store(false, Ordering::SeqCst);
    eventually(&health, Health::is_ready).await;
    assert_eq!(health.report().subscriptions[0].publish_retries, 0);

    shutdown.cancel();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn ended_source_keeps_the_application_ready() {
    let (seed, seed_input) = scripted("seed", Switch::default());
    let (orders, _orders_input) = scripted("orders", Switch::default());
    let app = App::new().subscription(seed).subscription(orders);
    let health = app.health();
    let shutdown = CancellationToken::new();
    let running = run(app, &shutdown);

    drop(seed_input);
    eventually(&health, |health| {
        health.report().subscriptions[0].status == SubscriptionStatus::Stopped
    })
    .await;
    assert!(health.is_ready());

    shutdown.cancel();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn failed_subscription_is_not_live() {
    let (subscription, input) = scripted("orders", Switch::default());
    let app = App::new().subscription(subscription.receive_retry(RetryPolicy {
        max_attempts: 1,
        ..RetryPolicy::default()
    }));
    let health = app.health();
    let running = run(app, &CancellationToken::new());

    input.send(None).unwrap();
    assert!(running.await.unwrap().is_err());
    let report = health.report();
    assert!(!report.live && !report.ready && report.shutting_down);
    assert_eq!(report.subscriptions[0].status, SubscriptionStatus::Failed);
}

#[tokio::test]
async fn draining_subscription_is_unready_but_live() {
    let release = Arc::new(Notify::new());
    let started = Arc::new(Notify::new());
    let (sender, receiver) = mpsc::unbounded_channel();
    let subscription = Subscription::new("orders", Scripted(receiver), Switch::default(), {
        let release = release.clone();
        let started = started.clone();
        move |value: i32| {
            let release = release.clone();
            let started = started.clone();
            async move {
                started.notify_one();
                release.notified().await;
                Ok(value)
            }
        }
    });
    let app = App::new().subscription(subscription);
    let health = app.health();
    let shutdown = CancellationToken::new();
    let running = run(app, &shutdown);

    sender.send(Some(1)).unwrap();
    started.notified().await;
    shutdown.cancel();
    eventually(&health, |health| {
        health.report().subscriptions[0].status == SubscriptionStatus::Stopping
    })
    .await;
    let report = health.report();
    assert!(report.live && !report.ready);

    release.notify_one();
    running.await.unwrap().unwrap();
}

/// Connection state and lag the test sets.
#[derive(Default)]
struct Reading(Mutex<(Option<bool>, Option<u64>)>);

impl SourceProbe for Reading {
    fn connected(&self) -> Option<bool> {
        self.0.lock().unwrap().0
    }

    fn lag(&self) -> Option<u64> {
        self.0.lock().unwrap().1
    }
}

/// A scripted source that reports a probe.
struct Probed(Scripted, Arc<Reading>);

impl Source for Probed {
    type Message = Delivery<i32>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        self.0.receive().await
    }

    fn probe(&self) -> Option<Arc<dyn SourceProbe>> {
        Some(self.1.clone())
    }
}

#[tokio::test]
async fn disconnected_source_makes_the_application_unready_and_lag_does_not() {
    let reading = Arc::new(Reading::default());
    let (_input, receiver) = mpsc::unbounded_channel();
    let source = Probed(Scripted(receiver), reading.clone());
    let app = App::new().subscription(Subscription::new(
        "orders",
        source,
        Switch::default(),
        |value: i32| async move { Ok(value) },
    ));
    let health = app.health();
    let shutdown = CancellationToken::new();
    let running = run(app, &shutdown);
    eventually(&health, Health::is_ready).await;
    let report = health.report();
    assert_eq!(report.subscriptions[0].connected, None);
    assert_eq!(report.subscriptions[0].lag, None);

    *reading.0.lock().unwrap() = (Some(true), Some(1_000_000));
    let report = health.report();
    assert!(report.ready, "lag does not affect readiness");
    assert_eq!(report.subscriptions[0].connected, Some(true));
    assert_eq!(report.subscriptions[0].lag, Some(1_000_000));

    *reading.0.lock().unwrap() = (Some(false), Some(0));
    let report = health.report();
    assert!(!report.ready && report.live);

    *reading.0.lock().unwrap() = (Some(true), Some(0));
    assert!(health.is_ready());

    shutdown.cancel();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn stuck_handler_is_not_live_after_the_stall_timeout() {
    let release = Arc::new(Notify::new());
    let (sender, receiver) = mpsc::unbounded_channel();
    let subscription = Subscription::new("orders", Scripted(receiver), Switch::default(), {
        let release = release.clone();
        move |value: i32| {
            let release = release.clone();
            async move {
                if value == 1 {
                    release.notified().await;
                }
                Ok(value)
            }
        }
    })
    .stall_timeout(Duration::from_millis(100));
    let app = App::new().subscription(subscription);
    let health = app.health();
    let shutdown = CancellationToken::new();
    let running = run(app, &shutdown);
    eventually(&health, Health::is_ready).await;

    // An idle subscription is never stalled.
    sleep(Duration::from_millis(200)).await;
    assert!(health.is_live());

    sender.send(Some(1)).unwrap();
    eventually(&health, |health| !health.is_live()).await;
    let report = health.report();
    assert!(report.subscriptions[0].stalled);
    assert_eq!(report.subscriptions[0].status, SubscriptionStatus::Running);

    release.notify_one();
    eventually(&health, Health::is_live).await;
    assert!(!health.report().subscriptions[0].stalled);

    shutdown.cancel();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn zero_stall_timeout_is_rejected() {
    let (subscription, _input) = scripted("orders", Switch::default());
    let result = App::new()
        .subscription(subscription.stall_timeout(Duration::ZERO))
        .run()
        .await;
    assert!(matches!(result, Err(Error::Config(_))), "{result:?}");
}

#[cfg(feature = "health")]
mod server {
    use super::*;
    use beavers::health::HealthServer;
    use std::net::SocketAddr;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    /// Sends one request over its own connection and returns the status and body.
    async fn request(addr: SocketAddr, method: &str, path: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let head =
            format!("{method} {path} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n");
        stream.write_all(head.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        let status = response.split(' ').nth(1).unwrap().parse().unwrap();
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_owned())
            .unwrap_or_default();
        (status, body)
    }

    fn server() -> HealthServer {
        HealthServer::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap()
    }

    #[tokio::test]
    async fn probes_answer_with_the_report() {
        let server = server();
        let addr = server.local_addr();
        assert_ne!(addr.port(), 0);
        let (subscription, _input) = scripted("orders", Switch::default());
        let app = App::new().subscription(subscription).health_server(server);
        let health = app.health();
        let shutdown = CancellationToken::new();
        let running = run(app, &shutdown);
        eventually(&health, Health::is_ready).await;

        let (status, body) = request(addr, "GET", "/readyz").await;
        assert_eq!(status, 200);
        let report: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(report["ready"], true);
        assert_eq!(report["subscriptions"][0]["name"], "orders");
        assert_eq!(report["subscriptions"][0]["status"], "running");
        assert_eq!(request(addr, "GET", "/livez").await.0, 200);
        assert_eq!(request(addr, "GET", "/metrics").await.0, 404);
        assert_eq!(request(addr, "POST", "/readyz").await.0, 405);

        shutdown.cancel();
        running.await.unwrap().unwrap();
        assert!(
            TcpStream::connect(addr).await.is_err(),
            "the server stops with the application"
        );
    }

    #[tokio::test]
    async fn readiness_fails_while_draining_and_liveness_holds() {
        let server = server();
        let addr = server.local_addr();
        let release = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        let (sender, receiver) = mpsc::unbounded_channel();
        let subscription = Subscription::new("orders", Scripted(receiver), Switch::default(), {
            let release = release.clone();
            let started = started.clone();
            move |value: i32| {
                let release = release.clone();
                let started = started.clone();
                async move {
                    started.notify_one();
                    release.notified().await;
                    Ok(value)
                }
            }
        });
        let shutdown = CancellationToken::new();
        let running = run(
            App::new().subscription(subscription).health_server(server),
            &shutdown,
        );

        sender.send(Some(1)).unwrap();
        started.notified().await;
        assert_eq!(request(addr, "GET", "/readyz").await.0, 200);
        shutdown.cancel();
        let (status, body) = request(addr, "GET", "/readyz").await;
        assert_eq!(status, 503);
        assert!(body.contains(r#""shutting_down":true"#), "{body}");
        assert_eq!(request(addr, "GET", "/livez").await.0, 200);

        release.notify_one();
        running.await.unwrap().unwrap();
    }
}
