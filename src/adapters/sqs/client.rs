//! SQS clients for a configured endpoint.
use super::config::Endpoint;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_sqs::{Client, config::Credentials};

pub(super) async fn client(endpoint: &Endpoint<'_>) -> Client {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(region) = endpoint.region {
        loader = loader.region(Region::new(region.to_owned()));
    }
    if let Some(url) = endpoint.endpoint_url {
        loader = loader.endpoint_url(url);
    }
    if let Some(credentials) = endpoint.credentials {
        loader = loader.credentials_provider(Credentials::new(
            &credentials.access_key_id,
            &credentials.secret_access_key,
            credentials.session_token.clone(),
            None,
            "beavers",
        ));
    }
    Client::new(&loader.load().await)
}
