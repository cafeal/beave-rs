//! Pub/Sub clients for a configured endpoint.
use super::config::{Endpoint, PubSubCredentials, PubSubSinkConfig};
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_pubsub::client::{Publisher, Subscriber};

pub(super) async fn subscriber(endpoint: &Endpoint<'_>) -> anyhow::Result<Subscriber> {
    let mut builder = Subscriber::builder();
    if let Some(url) = endpoint.endpoint {
        builder = builder.with_endpoint(url);
    }
    if *endpoint.credentials == PubSubCredentials::Anonymous {
        builder = builder.with_credentials(Anonymous::new().build());
    }
    Ok(builder.build().await?)
}

pub(super) async fn publisher(config: &PubSubSinkConfig) -> anyhow::Result<Publisher> {
    let endpoint = config.endpoint();
    let mut builder = Publisher::builder(&config.topic).set_delay_threshold(config.batch_delay);
    if let Some(url) = endpoint.endpoint {
        builder = builder.with_endpoint(url);
    }
    if *endpoint.credentials == PubSubCredentials::Anonymous {
        builder = builder.with_credentials(Anonymous::new().build());
    }
    Ok(builder.build().await?)
}
