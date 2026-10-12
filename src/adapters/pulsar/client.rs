use super::config::PulsarAuthentication;
use crate::error::BoxError;
use magnetar::{PulsarClient, proto::SupervisorConfig};
use std::sync::Arc;
use tokio::runtime::Handle;

/// Connects a client that reconnects its producers and consumers after the
/// broker connection drops.
pub(super) async fn connect(
    service_url: &str,
    authentication: Option<&PulsarAuthentication>,
) -> Result<PulsarClient, BoxError> {
    let mut builder = PulsarClient::builder()
        .service_url(service_url)
        .enable_reconnect(SupervisorConfig::default());
    if let Some(authentication) = authentication {
        builder = builder.auth(authentication.provider());
    }
    Ok(builder.build().await?)
}

/// The topics that hold `topic`'s messages: each partition of a partitioned
/// topic, or the topic itself.
pub(super) async fn partition_topics(
    client: &PulsarClient,
    topic: &str,
) -> Result<Vec<String>, BoxError> {
    let partitions = client.partitions_for_topic(topic).await?;
    Ok(if partitions == 0 {
        vec![topic.to_owned()]
    } else {
        (0..partitions)
            .map(|partition| format!("{topic}-partition-{partition}"))
            .collect()
    })
}

/// A client shared by a source and its deliveries. Acknowledging through a
/// closed client never completes, so the client closes only after the last
/// delivery that can acknowledge through it is dropped.
pub(super) struct SharedClient(Option<PulsarClient>);

impl SharedClient {
    pub(super) fn new(client: PulsarClient) -> Arc<Self> {
        Arc::new(Self(Some(client)))
    }
}

impl Drop for SharedClient {
    fn drop(&mut self) {
        if let (Some(client), Ok(runtime)) = (self.0.take(), Handle::try_current()) {
            runtime.spawn(client.close());
        }
    }
}
