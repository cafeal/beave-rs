use beavers::{Receive, ReceiveError, Source, SourceMessage};
use metrics_util::{
    CompositeKey,
    debugging::{DebugValue, DebuggingRecorder, Snapshotter},
};
#[cfg(feature = "opentelemetry")]
use opentelemetry::{global, trace::TracerProvider as _};
#[cfg(feature = "opentelemetry")]
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    trace::{InMemorySpanExporter, SdkTracerProvider, SpanData},
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, OnceLock},
};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id},
};
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt},
    registry,
    registry::{LookupSpan, SpanRef},
};

/// A text delivery that decodes as `i32` and carries text-map fields.
pub(crate) struct FieldMessage {
    payload: &'static str,
    fields: Vec<(String, String)>,
}

impl SourceMessage for FieldMessage {
    type Item = i32;
    type Raw = Vec<u8>;

    fn decode(&self) -> anyhow::Result<i32> {
        Ok(self.payload.parse()?)
    }

    async fn ack(self) -> anyhow::Result<()> {
        Ok(())
    }

    fn raw(&self) -> Vec<u8> {
        self.payload.as_bytes().to_vec()
    }

    fn propagation_fields(&self) -> Vec<(&str, &str)> {
        self.fields
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect()
    }
}

pub(crate) struct FieldSource(VecDeque<FieldMessage>);

impl FieldSource {
    pub(crate) fn text(payloads: &[&'static str]) -> Self {
        Self::with_fields(payloads, &[])
    }

    pub(crate) fn with_fields(payloads: &[&'static str], fields: &[(&str, &str)]) -> Self {
        let fields: Vec<_> = fields
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        Self(
            payloads
                .iter()
                .map(|payload| FieldMessage {
                    payload,
                    fields: fields.clone(),
                })
                .collect(),
        )
    }
}

impl Source for FieldSource {
    type Message = FieldMessage;

    async fn receive(&mut self) -> Result<Receive<FieldMessage>, ReceiveError> {
        Ok(match self.0.pop_front() {
            Some(message) => Receive::Message(message),
            None => Receive::End,
        })
    }
}

/// Counters and histogram samples accumulated across snapshots, because taking a
/// snapshot resets them and tests running in parallel share one recorder.
struct Metrics {
    snapshotter: Snapshotter,
    recorded: Mutex<HashMap<CompositeKey, Value>>,
}

/// Installs the process-wide metrics recorder shared by every test in this binary.
/// Tests use distinct subscription names to keep their series apart.
pub(crate) fn install_metrics() {
    metrics();
}

fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().unwrap();
        Metrics {
            snapshotter,
            recorded: Mutex::default(),
        }
    })
}

/// A counter total or the histogram samples recorded so far.
#[derive(Clone, Debug)]
pub(crate) enum Value {
    Counter(u64),
    Histogram(Vec<f64>),
}

/// Every counter or histogram series of `name` whose labels include all of `labels`.
pub(crate) fn series(name: &str, labels: &[(&str, &str)]) -> Vec<Value> {
    let metrics = metrics();
    let mut recorded = metrics.recorded.lock().unwrap();
    for (key, _, _, value) in metrics.snapshotter.snapshot().into_vec() {
        match (recorded.entry(key).or_insert(Value::Counter(0)), value) {
            (Value::Counter(total), DebugValue::Counter(count)) => *total += count,
            (entry @ Value::Counter(0), DebugValue::Histogram(samples)) => {
                *entry = Value::Histogram(samples.into_iter().map(|s| s.into_inner()).collect());
            }
            (Value::Histogram(all), DebugValue::Histogram(samples)) => {
                all.extend(samples.into_iter().map(|sample| sample.into_inner()));
            }
            // Gauges are reset by every snapshot, so they cannot be compared across tests.
            _ => {}
        }
    }
    recorded
        .iter()
        .filter(|(key, _)| {
            let key = key.key();
            key.name() == name
                && labels.iter().all(|(label, value)| {
                    key.labels()
                        .any(|found| found.key() == *label && found.value() == *value)
                })
        })
        .map(|(_, value)| value.clone())
        .collect()
}

pub(crate) fn counter(name: &str, labels: &[(&str, &str)]) -> u64 {
    series(name, labels)
        .into_iter()
        .map(|value| match value {
            Value::Counter(count) => count,
            other => panic!("{name} is not a counter: {other:?}"),
        })
        .sum()
}

/// A recorded span or event with the names of its enclosing spans, innermost first.
#[derive(Clone, Debug)]
pub(crate) struct Record {
    /// The `subscription` field of the enclosing subscription span.
    pub(crate) subscription: Option<String>,
    pub(crate) name: String,
    pub(crate) level: tracing::Level,
    pub(crate) scope: Vec<String>,
    pub(crate) fields: Vec<(String, String)>,
}

impl Record {
    pub(crate) fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.as_str())
    }
}

/// Captures spans and events for assertions.
#[derive(Clone, Default)]
pub(crate) struct Capture {
    spans: Arc<Mutex<Vec<Record>>>,
    events: Arc<Mutex<Vec<Record>>>,
}

impl Capture {
    /// The process-wide capturing subscriber shared by every test in this binary.
    /// A thread-local subscriber would race with other tests over callsite interest.
    pub(crate) fn global() -> &'static Capture {
        &telemetry().capture
    }

    /// Spans recorded within the named subscription.
    pub(crate) fn spans(&self, subscription: &str) -> Vec<Record> {
        Self::within(&self.spans, subscription)
    }

    /// Events recorded within the named subscription.
    pub(crate) fn events(&self, subscription: &str) -> Vec<Record> {
        Self::within(&self.events, subscription)
    }

    fn within(records: &Mutex<Vec<Record>>, subscription: &str) -> Vec<Record> {
        records
            .lock()
            .unwrap()
            .iter()
            .filter(|record| record.subscription.as_deref() == Some(subscription))
            .cloned()
            .collect()
    }
}

struct Telemetry {
    capture: Capture,
    #[cfg(feature = "opentelemetry")]
    provider: SdkTracerProvider,
    #[cfg(feature = "opentelemetry")]
    exporter: InMemorySpanExporter,
}

fn telemetry() -> &'static Telemetry {
    static TELEMETRY: OnceLock<Telemetry> = OnceLock::new();
    TELEMETRY.get_or_init(|| {
        let capture = Capture::default();
        let subscriber = registry().with(capture.clone());
        #[cfg(feature = "opentelemetry")]
        {
            global::set_text_map_propagator(TraceContextPropagator::new());
            let exporter = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder()
                .with_simple_exporter(exporter.clone())
                .build();
            let tracer = provider.tracer("beavers-test");
            let subscriber = subscriber.with(tracing_opentelemetry::layer().with_tracer(tracer));
            tracing::subscriber::set_global_default(subscriber).unwrap();
            Telemetry {
                capture,
                provider,
                exporter,
            }
        }
        #[cfg(not(feature = "opentelemetry"))]
        {
            tracing::subscriber::set_global_default(subscriber).unwrap();
            Telemetry { capture }
        }
    })
}

/// Spans exported through OpenTelemetry by every test in this binary.
#[cfg(feature = "opentelemetry")]
pub(crate) fn exported_spans() -> Vec<SpanData> {
    let telemetry = telemetry();
    telemetry.provider.force_flush().unwrap();
    telemetry.exporter.get_finished_spans().unwrap()
}

#[derive(Clone, Default)]
struct Fields(Vec<(String, String)>);

impl Fields {
    fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.as_str())
    }
}

/// The `subscription` field of the outermost span that records one.
fn subscription<'a, S: LookupSpan<'a> + 'a>(scope: &[SpanRef<'a, S>]) -> Option<String> {
    scope
        .iter()
        .filter_map(|span| {
            let extensions = span.extensions();
            extensions
                .get::<Fields>()?
                .get("subscription")
                .map(str::to_owned)
        })
        .last()
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.push((field.name().to_owned(), format!("{value:?}")));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push((field.name().to_owned(), value.to_owned()));
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        let mut fields = Fields::default();
        attributes.record(&mut fields);
        let span = context.span(id).unwrap();
        span.extensions_mut().insert(fields.clone());
        self.spans.lock().unwrap().push(Record {
            subscription: subscription(&span.scope().collect::<Vec<_>>()),
            name: span.name().to_owned(),
            level: *span.metadata().level(),
            scope: span
                .scope()
                .skip(1)
                .map(|span| span.name().to_owned())
                .collect(),
            fields: fields.0,
        });
    }

    fn on_event(&self, event: &Event<'_>, context: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        let scope: Vec<_> = context
            .event_scope(event)
            .map(|scope| scope.collect())
            .unwrap_or_default();
        self.events.lock().unwrap().push(Record {
            subscription: subscription(&scope),
            name: fields
                .0
                .iter()
                .find(|(name, _)| name == "message")
                .map(|(_, message)| message.clone())
                .unwrap_or_default(),
            level: *event.metadata().level(),
            scope: scope.iter().map(|span| span.name().to_owned()).collect(),
            fields: fields.0,
        });
    }
}
