# Observability

The runtime reports through the [`tracing`](https://docs.rs/tracing) and
[`metrics`](https://docs.rs/metrics) facades. Both do nothing until the
application installs a subscriber or recorder, such as `tracing-subscriber`
or a Prometheus exporter. Metric handles are registered when a subscription
starts, so install the recorder before `App::run`.

## Spans

```text
subscription                 subscription = <name>
  └── message                one per delivery
       ├── decode
       ├── handler           one per attempt, with `attempt`
       ├── encode            `Sink::prepare` for every output
       ├── publish           every output, including publish retries
       ├── dead_letter       conversion and dead-letter publication
       └── ack
  └── commit                 transactional subscriptions: every attempt to commit
                             a batch, with `deliveries` and `outputs`
```

A [transactional subscription](transactions.md) records no `publish` or `ack`
span. Its `commit` spans belong to the subscription, not to a delivery,
because one transaction commits several deliveries.

`pre_handler` and `post_handler` middleware run in the `message` span.

## Events

| Event | Level | Fields |
|---|---|---|
| `subscription started` / `subscription stopped` | INFO | |
| `subscription failed` | ERROR | `error` |
| `discarding delivery` | WARN | `failure`, `attempts`, `error` |
| `dead-lettered delivery` | WARN | `failure`, `attempts`, `error` |
| `retrying receive` | WARN | `attempt`, `delay`, `error` |
| `retrying handler` / `retrying publish` | DEBUG | `attempt`, `error` |
| `abandoned revoked delivery` | DEBUG | |

A discarded delivery is always logged and counted, because
`FailureAction::Discard` leaves no other trace.

## Metrics

Every metric carries a `subscription` label with the subscription name.

| Metric | Type | Additional labels | Meaning |
|---|---|---|---|
| `beavers_deliveries_received_total` | counter | | Deliveries received from the source |
| `beavers_deliveries_acknowledged_total` | counter | | Successful ACKs or transaction commits, including after discard and dead-lettering |
| `beavers_delivery_failures_total` | counter | `failure`, `action` | Routable failures by `FailureKind` and the action taken |
| `beavers_handler_retries_total` | counter | | Handler attempts that returned `Retry` and were retried |
| `beavers_publish_failures_total` | counter | `sink` (`output`, `dead_letter`) | Failed publish or transaction commit attempts, including retried ones |
| `beavers_receive_errors_total` | counter | | Failed receive attempts |
| `beavers_deliveries_revoked_total` | counter | | Deliveries abandoned after revocation |
| `beavers_deliveries_in_flight` | gauge | | Received deliveries that have not finished |
| `beavers_stage_duration_seconds` | histogram | `stage` | Duration of `decode`, `handler`, `encode`, `publish`, `complete`, `dead_letter`, `ack`, and `commit` |
| `beavers_transaction_deliveries` | histogram | | Deliveries in each committed transaction |

`failure` is `decode`, `rejected`, `retry_exhausted`, `encode`, or
`publish_rejected`. `action` is
`stop`, `dead_letter`, or `discard`; a `DeadLetter` action without a
dead-letter sink is reported as `stop`, which is what it does. A failure is
counted when it is routed, before the action runs. `handler` durations are
per attempt and exclude retry backoff; `publish` durations cover all outputs
of a delivery, including backoff. `commit` durations are per successful
transaction and cover the whole batch.

## Alerting

beavers sends no notifications itself. Alert on the metrics with a monitoring
system and let it route alerts, since per-message notifications flood a channel
when many deliveries fail at once. The
[monitoring and alerting guide](../monitoring-and-alerting/README.md) lists the
signals worth alerting on, PromQL alert rules, and configurations for
Kubernetes, AWS, Google Cloud, and Azure. A failed subscription also makes the
[liveness probe](health.md) fail.

## Exporting metrics to OpenTelemetry

beavers does not bridge the `metrics` facade to OpenTelemetry metrics. An
application that sends metrics through an OpenTelemetry Collector exposes them
in the Prometheus format with
[`metrics-exporter-prometheus`](https://docs.rs/metrics-exporter-prometheus)
and lets the Collector scrape them.

Install the exporter before `App::run`:

```rust,ignore
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};

PrometheusBuilder::new()
    .with_http_listener(([0, 0, 0, 0], 9000))
    .set_buckets_for_metric(
        Matcher::Full("beavers_stage_duration_seconds".to_owned()),
        &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
    )?
    .install()?;

app.run().await?;
```

The listener serves `/metrics` over HTTP. Without configured buckets, the
exporter writes histograms as Prometheus summaries, which arrive as
OpenTelemetry summaries that many backends do not support; configure buckets for
`beavers_stage_duration_seconds` and any histograms the application records
itself.

Scrape the endpoint with the Collector's Prometheus receiver, which is part of
the `otelcol-contrib` distribution, and forward the metrics to any exporter:

```yaml
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: beavers
          scrape_interval: 15s
          static_configs:
            - targets: ["my-app:9000"]

exporters:
  otlp:
    endpoint: my-backend:4317

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      exporters: [otlp]
```

Counters arrive as cumulative OpenTelemetry sums, the in-flight gauge as a
gauge, and bucketed histograms as histograms. Labels become data-point
attributes, and the receiver maps the scrape's `job` and `instance` to the
`service.name` and `service.instance.id` resource attributes.

## Trace-context propagation

`SourceMessage::propagation_fields` exposes text-map fields received with a
delivery, and `PropagationCarrier` lets an output record accept them. Each
adapter maps the fields to its own metadata:

| Adapter | Received fields | Output fields |
|---|---|---|
| Kafka | Headers with UTF-8 values | Headers; every header of the same name is replaced |
| Pulsar | Properties | Properties |
| RabbitMQ | String headers | String headers |
| SQS | String message attributes | String message attributes |
| Pub/Sub | Attributes | Attributes |
| Azure Service Bus | String application properties | String application properties |
| Channel | The sender's trace context, captured when a value is enqueued | Captured automatically; no carrier needed |
| `Delivery` | Fields set with `Delivery::with_propagation_fields` | Not supported |
| Other local adapters | None | Not supported |

With the `opentelemetry` feature, the runtime extracts each `message` span's
remote parent from those fields through the global text-map propagator, and
the `TraceContext` middleware injects the `message` span's context into every
output:

A [channel](../adapters/channel.md) needs no middleware. With the
`opentelemetry` feature, `ChannelSink` and `ChannelSender` capture the current
span's context when they enqueue a value, so the receiving subscription's
`message` span continues the trace as a child of the sending stage.

```rust,ignore
use beavers::TraceContext;

opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
// Install a tracing subscriber with a tracing-opentelemetry layer.

Subscription::new("orders", kafka_source, kafka_sink, handler)
    .middleware(TraceContext::new())
```

Inheritance middleware such as `KafkaInherit` copies every header, including
`traceparent`, unchanged. Without `TraceContext`, an output therefore carries
the received context, and the processing step is invisible to the trace. With
`TraceContext` registered after the inheritance middleware, the injected
context replaces the inherited one, so downstream consumers continue the trace
as children of the processing step. Middleware runs in registration order, and
a handler returning a plain value registers the inheritance middleware first.
