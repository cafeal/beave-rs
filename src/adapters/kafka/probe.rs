//! Connection state and consumer lag from librdkafka statistics.
use super::progress::Progress;
use crate::source::SourceProbe;
use rdkafka::statistics::{Partition, Statistics};
use std::sync::Mutex;

/// Interval of the statistics the probe reads, unless the application sets
/// `statistics.interval.ms` itself.
pub(super) const STATISTICS_INTERVAL_MS: &str = "5000";

/// The latest reading from the consumer's statistics callback.
#[derive(Default)]
pub(super) struct KafkaProbe(Mutex<Option<Reading>>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Reading {
    connected: bool,
    lag: u64,
}

impl KafkaProbe {
    pub(super) fn update(&self, statistics: &Statistics, progress: &Progress) {
        *self.0.lock().unwrap() = Some(read(statistics, progress));
    }

    /// Forgets the last reading once the consumer is gone.
    pub(super) fn clear(&self) {
        *self.0.lock().unwrap() = None;
    }
}

impl SourceProbe for KafkaProbe {
    fn connected(&self) -> Option<bool> {
        self.0.lock().unwrap().map(|reading| reading.connected)
    }

    fn lag(&self) -> Option<u64> {
        self.0.lock().unwrap().map(|reading| reading.lag)
    }
}

/// The consumer is connected while any broker connection is up, which is the
/// opposite of librdkafka's "all brokers down" condition.
fn read(statistics: &Statistics, progress: &Progress) -> Reading {
    let connected = statistics
        .brokers
        .values()
        .any(|broker| broker.source != "internal" && broker.state == "UP");
    let lag = statistics
        .topics
        .iter()
        .flat_map(|(topic, stats)| {
            stats
                .partitions
                .values()
                .map(move |partition| (topic, partition))
        })
        .filter_map(|(topic, partition)| partition_lag(topic, partition, progress))
        .sum();
    Reading { connected, lag }
}

/// The distance from the first offset this consumer has not committed to the
/// end of a partition it fetches. librdkafka's own `consumer_lag` does not
/// follow commits made with an explicit offset list, so the start comes from
/// the source's progress, then from the committed offset in the statistics,
/// and before either is known from the consumer's fetch position. The end is
/// the last stable offset, which equals the high watermark under
/// `read_uncommitted`.
fn partition_lag(topic: &str, partition: &Partition, progress: &Progress) -> Option<u64> {
    if partition.partition < 0 || matches!(partition.fetch_state.as_str(), "none" | "stopped") {
        return None;
    }
    let end = [partition.ls_offset, partition.hi_offset]
        .into_iter()
        .find(|offset| *offset >= 0)?;
    let start = progress
        .position(&(topic.to_owned(), partition.partition))
        .into_iter()
        .chain([partition.committed_offset, partition.app_offset])
        .find(|offset| *offset >= 0)?;
    u64::try_from(end - start).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::statistics::{Broker, Topic};

    /// Partitions as `(id, fetch state, committed offset, fetch position, end)`.
    fn statistics(
        brokers: &[(&str, &str)],
        partitions: &[(i32, &str, i64, i64, i64)],
    ) -> Statistics {
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
            topics: [(
                "orders".to_owned(),
                Topic {
                    partitions: partitions
                        .iter()
                        .map(|&(partition, state, committed, app, end)| {
                            let partition = Partition {
                                partition,
                                fetch_state: state.to_owned(),
                                committed_offset: committed,
                                app_offset: app,
                                ls_offset: end,
                                hi_offset: end,
                                ..Partition::default()
                            };
                            (partition.partition, partition)
                        })
                        .collect(),
                    ..Topic::default()
                },
            )]
            .into(),
            ..Statistics::default()
        }
    }

    #[test]
    fn reads_connection_and_lag() {
        const INVALID: i64 = -1001;
        let probe = KafkaProbe::default();
        assert_eq!((probe.connected(), probe.lag()), (None, None));

        let mut progress = Progress::default();
        progress.activate(("orders".to_owned(), 4));
        progress.register(&("orders".to_owned(), 4), 10);
        probe.update(
            &statistics(
                &[("internal", "UP"), ("learned", "UP"), ("learned", "DOWN")],
                &[
                    // The unassigned-partition placeholder and a partition not fetched.
                    (-1, "none", INVALID, INVALID, INVALID),
                    (3, "none", 1, 1, 9),
                    // Committed up to 4 of 7, and not committed yet with 2 of 5 fetched.
                    (0, "active", 4, 6, 7),
                    (1, "active", INVALID, 2, 5),
                    // No offsets known yet.
                    (2, "offset-query", INVALID, INVALID, INVALID),
                    // Received from offset 10 and not committed, fetched up to 12 of 12.
                    (4, "active", INVALID, 12, 12),
                ],
            ),
            &progress,
        );
        assert_eq!((probe.connected(), probe.lag()), (Some(true), Some(8)));

        probe.update(
            &statistics(&[("internal", "UP"), ("learned", "TRY_CONNECT")], &[]),
            &progress,
        );
        assert_eq!((probe.connected(), probe.lag()), (Some(false), Some(0)));

        probe.clear();
        assert_eq!(probe.connected(), None);
    }
}
