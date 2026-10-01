//! OpenTelemetry trace-context propagation between deliveries and outputs.
//!
//! Requires an application-installed `tracing-opentelemetry` layer and a global
//! text-map propagator such as `TraceContextPropagator`. Without them, no
//! context is extracted or injected.
use crate::{handler::Result, middleware::Middleware, propagation::PropagationCarrier};
use opentelemetry::{
    Context, global,
    propagation::{Extractor, Injector},
};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Middleware that writes the current delivery's trace context into each output.
///
/// It runs as a `post_handler` hook inside the delivery's `message` span, whose
/// parent is the context extracted from the received delivery. Downstream
/// consumers therefore continue the trace as children of this processing step.
/// Fields the output already carries under the propagator's names, including
/// fields copied by inheritance middleware registered earlier, are replaced.
#[derive(Clone, Copy, Debug, Default)]
pub struct TraceContext;

impl TraceContext {
    pub fn new() -> Self {
        Self
    }
}

impl<I, O> Middleware<I, O> for TraceContext
where
    I: Send + Sync + 'static,
    O: PropagationCarrier + Send + 'static,
{
    async fn post_handler(&self, _input: &I, mut output: O) -> Result<O> {
        let context = Span::current().context();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&context, &mut CarrierInjector(&mut output));
        });
        Ok(output)
    }
}

struct CarrierInjector<'a, O>(&'a mut O);

impl<O: PropagationCarrier> Injector for CarrierInjector<'_, O> {
    fn set(&mut self, key: &str, value: String) {
        self.0.set_propagation_field(key, value);
    }
}

struct FieldExtractor<'a>(&'a [(&'a str, &'a str)]);

impl Extractor for FieldExtractor<'_> {
    /// The last field wins when a delivery carries a name more than once.
    fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .rev()
            .find(|(name, _)| name.eq_ignore_ascii_case(key))
            .map(|(_, value)| *value)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|(name, _)| *name).collect()
    }
}

/// Sets the remote parent of a delivery's span from its propagation fields.
pub(crate) fn set_remote_parent(span: &Span, fields: &[(&str, &str)]) {
    if fields.is_empty() {
        return;
    }
    let parent: Context =
        global::get_text_map_propagator(|propagator| propagator.extract(&FieldExtractor(fields)));
    // Setting a parent can only fail when the span is disabled or already started.
    let _ = span.set_parent(parent);
}

/// The current span's trace context as text-map fields, for in-process hand-offs
/// such as channels that have no broker metadata to carry it.
pub(crate) fn current_fields() -> Vec<(String, String)> {
    let context = Span::current().context();
    let mut fields = FieldInjector(Vec::new());
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut fields);
    });
    fields.0
}

struct FieldInjector(Vec<(String, String)>);

impl Injector for FieldInjector {
    fn set(&mut self, key: &str, value: String) {
        self.0.push((key.to_owned(), value));
    }
}
