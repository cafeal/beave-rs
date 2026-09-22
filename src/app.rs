//! Application registration, subscription supervision, and shutdown coordination.
use crate::{
    handler::Handler,
    shutdown::{CancellationToken, termination_signal},
    sink::Sink,
    source::{Source, SourceItem},
    subscription::Subscription,
};
use std::{future::Future, pin::Pin};
use tokio::task::JoinSet;

type Runner = Box<
    dyn FnOnce(CancellationToken) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
        + Send,
>;
#[derive(Default)]
pub struct App {
    subscriptions: Vec<Runner>,
    validation: Vec<String>,
}
impl App {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn subscribe<S, K, H, O>(self, source: S, sink: K, handler: H) -> Self
    where
        S: Source,
        K: Sink<O>,
        H: Handler<SourceItem<S>, Output = O>,
        O: Send + Sync + 'static,
    {
        self.subscription(Subscription::new(source, sink, handler))
    }
    pub fn subscription<S, K, O>(mut self, subscription: Subscription<S, K, O>) -> Self
    where
        S: Source,
        K: Sink<O>,
        O: Send + Sync + 'static,
    {
        if let Err(error) = subscription.validate() {
            self.validation.push(error.to_string());
        }
        self.subscriptions
            .push(Box::new(|token| Box::pin(subscription.run(token))));
        self
    }
    pub async fn run_until(self, shutdown: CancellationToken) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.validation.is_empty(),
            "invalid subscription configuration: {}",
            self.validation.join("; ")
        );
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
        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
    pub async fn run(self) -> anyhow::Result<()> {
        let token = CancellationToken::new();
        let run = self.run_until(token.clone());
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => result,
            signal = termination_signal() => { token.cancel(); let result = run.await; signal?; result }
        }
    }
}
