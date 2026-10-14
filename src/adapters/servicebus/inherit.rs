use super::record::{ServiceBusPublish, ServiceBusRecord};
use crate::{
    dead_letter::DEAD_LETTER_HEADER_PREFIX,
    handler::Result,
    middleware::Middleware,
    record::{SamePlatform, ValueRecord},
};

/// Service Bus to Service Bus middleware that copies the received application
/// properties, and optionally the session, into each output message.
///
/// An application property is added only when the output does not set that
/// name. [Dead-letter properties](crate::DEAD_LETTER_HEADER_PREFIX) are not
/// inherited. The session ID and partition key are inherited only when enabled
/// with [`with_session`](Self::with_session) and the output leaves the session
/// ID `None`, for forwarding between session-enabled entities. The message ID,
/// correlation ID, subject, content type, time to live, and schedule are never
/// inherited.
#[derive(Clone, Copy, Debug)]
pub struct ServiceBusInherit {
    application_properties: bool,
    session: bool,
}

impl ServiceBusInherit {
    /// Inherits application properties.
    pub fn new() -> Self {
        Self {
            application_properties: true,
            session: false,
        }
    }

    /// Does not inherit application properties.
    pub fn without_application_properties(mut self) -> Self {
        self.application_properties = false;
        self
    }

    /// Also inherits the session ID and partition key of the received message.
    pub fn with_session(mut self) -> Self {
        self.session = true;
        self
    }
}

impl Default for ServiceBusInherit {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, O> Middleware<ServiceBusRecord<I>, ServiceBusPublish<O>> for ServiceBusInherit
where
    I: Send + Sync + 'static,
    O: Send + 'static,
{
    async fn post_handler(
        &self,
        input: &ServiceBusRecord<I>,
        mut output: ServiceBusPublish<O>,
    ) -> Result<ServiceBusPublish<O>> {
        if self.application_properties {
            let inherited = input
                .application_properties
                .iter()
                .filter(|(name, _)| !name.starts_with(DEAD_LETTER_HEADER_PREFIX));
            for (name, value) in inherited {
                output
                    .application_properties
                    .entry(name.clone())
                    .or_insert_with(|| value.clone());
            }
        }
        if self.session && output.properties.session_id.is_none() {
            output
                .properties
                .session_id
                .clone_from(&input.properties.session_id);
            if output.properties.partition_key.is_none() {
                output
                    .properties
                    .partition_key
                    .clone_from(&input.properties.partition_key);
            }
        }
        Ok(output)
    }
}

impl<T: Clone + Send + Sync + 'static> ValueRecord for ServiceBusRecord<T> {
    type Value = T;

    fn value(&self) -> Result<T> {
        Ok(self.value.clone())
    }
}

impl<T: Clone + Send + Sync + 'static, U: Send + Sync + 'static> SamePlatform<U>
    for ServiceBusRecord<T>
{
    type Publish = ServiceBusPublish<U>;
    type Inherit = ServiceBusInherit;

    fn publish(value: U) -> ServiceBusPublish<U> {
        ServiceBusPublish::new(value)
    }
}
