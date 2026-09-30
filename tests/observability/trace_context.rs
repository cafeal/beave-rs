use super::fixtures::{Capture, FieldSource, exported_spans};
use beavers::{App, InMemorySink, MapMetadata, PropagationCarrier, Subscription, TraceContext};

const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const PARENT_SPAN_ID: &str = "00f067aa0ba902b7";

/// Output record whose propagation fields replace earlier values of the same name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Carried {
    value: i32,
    fields: Vec<(String, String)>,
}

impl Carried {
    fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.as_str())
    }
}

impl PropagationCarrier for Carried {
    fn set_propagation_field(&mut self, name: &str, value: String) {
        self.fields.retain(|(field, _)| field != name);
        self.fields.push((name.to_owned(), value));
    }
}

#[tokio::test]
async fn outputs_continue_the_received_trace() {
    Capture::global();
    let traceparent = format!("00-{TRACE_ID}-{PARENT_SPAN_ID}-01");
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "propagating",
                FieldSource::with_fields(&["5"], &[("traceparent", &traceparent)]),
                sink.clone(),
                |value: i32| async move {
                    Ok(Carried {
                        value,
                        fields: Vec::new(),
                    })
                },
            )
            // Stands in for inheritance middleware that copies the received context.
            .middleware(MapMetadata::new(|_: &i32, mut output: Carried| {
                output.set_propagation_field("traceparent", "stale".into());
                Ok(output)
            }))
            .middleware(TraceContext::new()),
        )
        .run()
        .await
        .unwrap();

    let [output]: [Carried; 1] = sink.values().try_into().unwrap();
    assert_eq!(output.value, 5);
    let injected = output.field("traceparent").unwrap();
    let parts: Vec<_> = injected.split('-').collect();
    assert_eq!(parts.len(), 4, "{injected}");
    assert_eq!(parts[1], TRACE_ID);
    assert_ne!(parts[2], PARENT_SPAN_ID);

    let spans = exported_spans();
    let message = spans
        .iter()
        .find(|span| span.name == "message" && span.span_context.trace_id().to_string() == TRACE_ID)
        .unwrap();
    assert_eq!(message.parent_span_id.to_string(), PARENT_SPAN_ID);
    assert_eq!(message.span_context.span_id().to_string(), parts[2]);
}
