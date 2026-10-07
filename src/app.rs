//! Application registration, subscription supervision, and shutdown coordination.
#[cfg(feature = "health")]
use crate::health::HealthServer;
use crate::{
    health::Health,
    shutdown::{CancellationToken, termination_signal},
    sink::Sink,
    source::Source,
    subscription::{IntoHandler, One, Subscription},
};
use std::{collections::HashSet, future::Future, pin::Pin, time::Duration};
use tokio::task::JoinSet;
use tracing::warn;

type Runner = Box<
    dyn FnOnce(CancellationToken) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
        + Send,
>;
/// A set of subscriptions that run, fail, and shut down together.
///
/// ```no_run
/// use beavers::{App, IterSource, Json, Result, StdoutSink};
///
/// async fn double(value: u64) -> Result<u64> {
///     Ok(value * 2)
/// }
///
/// # async fn run() -> anyhow::Result<()> {
/// App::new()
///     .subscribe("double", IterSource::new([1, 2, 3]), StdoutSink::<Json>::new(), double)
///     .run()
///     .await
/// # }
/// ```
#[derive(Default)]
pub struct App {
    subscriptions: Vec<Runner>,
    names: HashSet<String>,
    validation: Vec<String>,
    health: Health,
    exit_delay: Duration,
    #[cfg(feature = "health")]
    health_server: Option<HealthServer>,
}
impl App {
    /// An application without subscriptions.
    pub fn new() -> Self {
        Self::default()
    }
    /// Add a subscription with the default [`SubscriptionConfig`](crate::SubscriptionConfig).
    ///
    /// Use [`subscription`](Self::subscription) to register a configured [`Subscription`].
    ///
    /// The handler takes the received record or its value and returns the sink's
    /// output type or a plain value; see [`IntoHandler`] for the accepted shapes.
    pub fn subscribe<S, K, H, In, Out>(
        self,
        name: impl Into<String>,
        source: S,
        sink: K,
        handler: H,
    ) -> Self
    where
        S: Source,
        H: IntoHandler<S, K, (In, Out, One)>,
        K: Sink<H::Output>,
    {
        self.subscription(Subscription::new(name, source, sink, handler))
    }
    /// Add a configured subscription.
    ///
    /// Subscription names must be unique. Invalid configuration is reported when the
    /// application starts running.
    pub fn subscription<S, K, O>(mut self, subscription: Subscription<S, K, O>) -> Self
    where
        S: Source,
        K: Sink<O>,
        O: Send + Sync + 'static,
    {
        if let Err(error) = subscription.validate() {
            self.validation.push(error.to_string());
        }
        // Names label logs, spans, and metrics, so two subscriptions must not share one.
        if !self.names.insert(subscription.name().to_owned()) {
            self.validation.push(format!(
                "duplicate subscription name {:?}",
                subscription.name()
            ));
        }
        let tracker = self.health.register(subscription.name());
        self.subscriptions
            .push(Box::new(|token| Box::pin(subscription.run(token, tracker))));
        self
    }
    /// A handle reporting the liveness and readiness of the subscriptions
    /// registered so far and of those registered later.
    pub fn health(&self) -> Health {
        self.health.clone()
    }
    /// Serves liveness and readiness probes while the application runs.
    #[cfg(feature = "health")]
    pub fn health_server(mut self, server: HealthServer) -> Self {
        self.health_server = Some(server);
        self
    }
    /// How long to wait after a subscription failure, once every subscription has
    /// finished, before returning the failure.
    ///
    /// The health server keeps answering during the delay, with `/livez` reporting
    /// `503`, and the process keeps serving whatever metrics exporter it installed,
    /// so probes and a final scrape observe the failure before the process exits.
    /// [`run`](Self::run) ends the delay early when the process receives SIGINT or
    /// SIGTERM. Defaults to zero, which returns the failure immediately. The delay
    /// does not apply when every subscription finishes without an error.
    pub fn exit_delay(mut self, delay: Duration) -> Self {
        self.exit_delay = delay;
        self
    }
    /// Run every subscription until all of them finish or `shutdown` is cancelled.
    ///
    /// A subscription failure cancels `shutdown`, so the others drain and stop; the first
    /// failure is returned after every subscription has finished and the
    /// [exit delay](Self::exit_delay) has elapsed.
    pub async fn run_until(self, shutdown: CancellationToken) -> anyhow::Result<()> {
        self.run_with(shutdown, CancellationToken::new()).await
    }
    /// Runs like [`run_until`](Self::run_until); cancelling `interrupt` skips or ends
    /// the exit delay.
    async fn run_with(
        self,
        shutdown: CancellationToken,
        interrupt: CancellationToken,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.validation.is_empty(),
            "invalid subscription configuration: {}",
            self.validation.join("; ")
        );
        self.health.start(&shutdown);
        #[cfg(feature = "health")]
        let health_server = self.health_server.map(|server| {
            let stop = CancellationToken::new();
            let task = tokio::spawn(server.serve(self.health.clone(), stop.clone()));
            (stop, task)
        });
        let mut subscriptions = JoinSet::new();
        for run in self.subscriptions {
            subscriptions.spawn(run(shutdown.clone()));
        }
        let mut failure = None;
        while let Some(result) = subscriptions.join_next().await {
            if let Err(error) = result.unwrap_or_else(|error| Err(error.into())) {
                shutdown.cancel();
                failure.get_or_insert(error);
            }
        }
        if let Some(error) = &failure
            && !self.exit_delay.is_zero()
            && !interrupt.is_cancelled()
        {
            warn!(
                error = format!("{error:#}"),
                delay = ?self.exit_delay,
                "subscription failed; delaying exit"
            );
            tokio::select! {
                _ = tokio::time::sleep(self.exit_delay) => {}
                _ = interrupt.cancelled() => {}
            }
        }
        #[cfg(feature = "health")]
        if let Some((stop, task)) = health_server {
            stop.cancel();
            if let Err(error) = task.await.unwrap_or_else(|error| Err(error.into())) {
                failure.get_or_insert(error.context("health server failed"));
            }
        }
        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
    /// Run every subscription until all of them finish, one fails, or the process receives
    /// SIGINT or SIGTERM.
    ///
    /// A signal skips the [exit delay](Self::exit_delay), or ends it if it has started.
    pub async fn run(self) -> anyhow::Result<()> {
        let token = CancellationToken::new();
        let interrupt = CancellationToken::new();
        let run = self.run_with(token.clone(), interrupt.clone());
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => result,
            signal = termination_signal() => {
                token.cancel();
                interrupt.cancel();
                let result = run.await;
                signal?;
                result
            }
        }
    }
}
