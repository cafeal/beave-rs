//! Metric handles registered once per subscription run.
use crate::{
    error_policy::{ErrorPolicy, FailureAction, FailureKind},
    health::Tracker,
};
use metrics::{Counter, Gauge, Histogram, counter, gauge, histogram};
use std::{sync::Arc, time::Instant};

const KINDS: [FailureKind; 5] = [
    FailureKind::Decode,
    FailureKind::Rejected,
    FailureKind::RetryExhausted,
    FailureKind::Encode,
    FailureKind::PublishRejected,
];

/// A timed part of one delivery's processing.
#[derive(Clone, Copy)]
pub(super) enum Stage {
    Decode,
    Handler,
    Encode,
    Publish,
    Complete,
    DeadLetter,
    Ack,
    /// A transaction that publishes outputs and acknowledges together.
    Commit,
}

const STAGES: [Stage; 8] = [
    Stage::Decode,
    Stage::Handler,
    Stage::Encode,
    Stage::Publish,
    Stage::Complete,
    Stage::DeadLetter,
    Stage::Ack,
    Stage::Commit,
];

impl Stage {
    fn label(self) -> &'static str {
        match self {
            Self::Decode => "decode",
            Self::Handler => "handler",
            Self::Encode => "encode",
            Self::Publish => "publish",
            Self::Complete => "complete",
            Self::DeadLetter => "dead_letter",
            Self::Ack => "ack",
            Self::Commit => "commit",
        }
    }
}

#[derive(Clone)]
pub(super) struct Instruments {
    pub(super) received: Counter,
    pub(super) acknowledged: Counter,
    pub(super) revoked: Counter,
    pub(super) receive_errors: Counter,
    pub(super) handler_retries: Counter,
    pub(super) publish_failures: Counter,
    pub(super) dead_letter_publish_failures: Counter,
    pub(super) in_flight: Gauge,
    /// Deliveries committed by each transaction of a transactional subscription.
    pub(super) transaction_deliveries: Histogram,
    /// Health state the runtime reports while it receives and publishes.
    pub(super) health: Arc<Tracker>,
    failures: [Counter; 5],
    stages: [Histogram; 8],
}

impl Instruments {
    /// Handles bind to the recorder installed when the subscription starts.
    pub(super) fn new(
        subscription: &str,
        policy: &ErrorPolicy,
        has_dead_letter_sink: bool,
        health: Arc<Tracker>,
    ) -> Self {
        let name = subscription.to_owned();
        let publish_failures = |sink: &'static str| counter!("beavers_publish_failures_total", "subscription" => name.clone(), "sink" => sink);
        Self {
            received: counter!("beavers_deliveries_received_total", "subscription" => name.clone()),
            acknowledged: counter!("beavers_deliveries_acknowledged_total", "subscription" => name.clone()),
            revoked: counter!("beavers_deliveries_revoked_total", "subscription" => name.clone()),
            receive_errors: counter!("beavers_receive_errors_total", "subscription" => name.clone()),
            handler_retries: counter!("beavers_handler_retries_total", "subscription" => name.clone()),
            publish_failures: publish_failures("output"),
            dead_letter_publish_failures: publish_failures("dead_letter"),
            in_flight: gauge!("beavers_deliveries_in_flight", "subscription" => name.clone()),
            transaction_deliveries: histogram!("beavers_transaction_deliveries", "subscription" => name.clone()),
            health,
            failures: KINDS.map(|kind| {
                let action = effective_action(policy, kind, has_dead_letter_sink);
                counter!(
                    "beavers_delivery_failures_total",
                    "subscription" => name.clone(),
                    "failure" => kind_label(kind),
                    "action" => action_label(action),
                )
            }),
            stages: STAGES.map(|stage| {
                histogram!(
                    "beavers_stage_duration_seconds",
                    "subscription" => name.clone(),
                    "stage" => stage.label(),
                )
            }),
        }
    }

    pub(super) fn failure(&self, kind: FailureKind) -> &Counter {
        &self.failures[KINDS.iter().position(|known| *known == kind).unwrap()]
    }

    pub(super) fn record(&self, stage: Stage, started: Instant) {
        self.stages[stage as usize].record(started.elapsed());
    }
}

/// A dead-letter action without a dead-letter sink stops the subscription.
pub(super) fn effective_action(
    policy: &ErrorPolicy,
    kind: FailureKind,
    has_dead_letter_sink: bool,
) -> FailureAction {
    match policy.action(kind) {
        FailureAction::DeadLetter if !has_dead_letter_sink => FailureAction::Stop,
        action => action,
    }
}

fn kind_label(kind: FailureKind) -> &'static str {
    match kind {
        FailureKind::Decode => "decode",
        FailureKind::Rejected => "rejected",
        FailureKind::RetryExhausted => "retry_exhausted",
        FailureKind::Encode => "encode",
        FailureKind::PublishRejected => "publish_rejected",
    }
}

fn action_label(action: FailureAction) -> &'static str {
    match action {
        FailureAction::Stop => "stop",
        FailureAction::DeadLetter => "dead_letter",
        FailureAction::Discard => "discard",
    }
}
