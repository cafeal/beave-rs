//! Connection state from librdkafka statistics.
use crate::source::SourceProbe;
use rdkafka::statistics::Statistics;
use std::sync::Mutex;

/// Interval of the statistics the probe reads, unless the application sets
/// `statistics.interval.ms` itself.
pub(super) const STATISTICS_INTERVAL_MS: &str = "5000";

/// Whether the consumer was connected at its latest statistics.
#[derive(Default)]
pub(super) struct KafkaProbe(Mutex<Option<bool>>);

impl KafkaProbe {
    /// The consumer is connected while any broker connection is up, which is
    /// the opposite of librdkafka's "all brokers down" condition.
    pub(super) fn update(&self, statistics: &Statistics) {
        let connected = statistics
            .brokers
            .values()
            .any(|broker| broker.source != "internal" && broker.state == "UP");
        *self.0.lock().unwrap() = Some(connected);
    }

    /// Forgets the last reading once the consumer is gone.
    pub(super) fn clear(&self) {
        *self.0.lock().unwrap() = None;
    }
}

impl SourceProbe for KafkaProbe {
    fn connected(&self) -> Option<bool> {
        *self.0.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::statistics::Broker;

    fn statistics(brokers: &[(&str, &str)]) -> Statistics {
        Statistics {
            brokers: brokers
                .iter()
                .enumerate()
                .map(|(index, (source, state))| {
                    let broker = Broker {
                        source: (*source).to_owned(),
                        state: (*state).to_owned(),
                        ..Broker::default()
                    };
                    (index.to_string(), broker)
                })
                .collect(),
            ..Statistics::default()
        }
    }

    #[test]
    fn reads_connection_state() {
        let probe = KafkaProbe::default();
        assert_eq!(probe.connected(), None);

        probe.update(&statistics(&[
            ("internal", "UP"),
            ("learned", "UP"),
            ("learned", "DOWN"),
        ]));
        assert_eq!(probe.connected(), Some(true));

        probe.update(&statistics(&[
            ("internal", "UP"),
            ("learned", "TRY_CONNECT"),
        ]));
        assert_eq!(probe.connected(), Some(false));

        probe.clear();
        assert_eq!(probe.connected(), None);
    }
}
