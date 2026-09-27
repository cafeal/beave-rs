use pulsar::SubType;

#[derive(Clone, Debug)]
pub struct PulsarAuthentication {
    pub name: String,
    pub data: Vec<u8>,
}

impl PulsarAuthentication {
    pub fn token(token: impl Into<Vec<u8>>) -> Self {
        Self {
            name: "token".into(),
            data: token.into(),
        }
    }

    pub(super) fn provider(&self) -> pulsar::Authentication {
        pulsar::Authentication {
            name: self.name.clone(),
            data: self.data.clone(),
        }
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.name.trim().is_empty(),
            "Pulsar auth method is required"
        );
        anyhow::ensure!(!self.data.is_empty(), "Pulsar auth data is required");
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PulsarSourceConfig {
    pub service_url: String,
    pub topic: String,
    pub subscription: String,
    pub subscription_type: SubType,
    pub authentication: Option<PulsarAuthentication>,
    pub buffer_size: usize,
}

impl PulsarSourceConfig {
    pub fn new(
        service_url: impl Into<String>,
        topic: impl Into<String>,
        subscription: impl Into<String>,
    ) -> Self {
        Self {
            service_url: service_url.into(),
            topic: topic.into(),
            subscription: subscription.into(),
            subscription_type: SubType::Shared,
            authentication: None,
            buffer_size: 100,
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        validate_endpoint(&self.service_url, &self.topic)?;
        anyhow::ensure!(
            !self.subscription.trim().is_empty(),
            "Pulsar subscription is required"
        );
        anyhow::ensure!(
            self.buffer_size > 0,
            "Pulsar buffer size must be greater than zero"
        );
        if let Some(authentication) = &self.authentication {
            authentication.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PulsarSinkConfig {
    pub service_url: String,
    pub topic: String,
    pub producer_name: Option<String>,
    pub authentication: Option<PulsarAuthentication>,
}

impl PulsarSinkConfig {
    pub fn new(service_url: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            service_url: service_url.into(),
            topic: topic.into(),
            producer_name: None,
            authentication: None,
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        validate_endpoint(&self.service_url, &self.topic)?;
        if let Some(authentication) = &self.authentication {
            authentication.validate()?;
        }
        if let Some(name) = &self.producer_name {
            anyhow::ensure!(
                !name.trim().is_empty(),
                "Pulsar producer name must not be empty"
            );
        }
        Ok(())
    }
}

fn validate_endpoint(service_url: &str, topic: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !service_url.trim().is_empty(),
        "Pulsar service URL is required"
    );
    anyhow::ensure!(
        !service_url.chars().any(char::is_whitespace),
        "Pulsar service URL must not contain whitespace"
    );
    anyhow::ensure!(
        service_url.starts_with("pulsar://") || service_url.starts_with("pulsar+ssl://"),
        "Pulsar service URL must use pulsar:// or pulsar+ssl://"
    );
    anyhow::ensure!(!topic.trim().is_empty(), "Pulsar topic is required");
    Ok(())
}
