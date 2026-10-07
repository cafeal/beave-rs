# Monitoring and alerting

beavers sends no notifications itself. It reports metrics through the
[`metrics`](https://docs.rs/metrics) facade, structured events through
[`tracing`](https://docs.rs/tracing), and health through the `/livez` and
`/readyz` endpoints of the [health server](../runtime.md#health-checks). This
guide explains which of those signals deserve an alert and how to wire them to
a platform that pages people, for teams that do not already run Prometheus and
Alertmanager.

| Platform | Guide |
|---|---|
| Kubernetes with a self-managed Prometheus Operator | [Kubernetes](kubernetes.md) |
| Amazon EKS, ECS, or EC2 | [AWS](aws.md): Amazon Managed Service for Prometheus or CloudWatch |
| GKE or Cloud Run | [Google Cloud](gcp.md): Managed Service for Prometheus and Cloud Monitoring |
| AKS or Azure Container Apps | [Azure](azure.md): Azure Monitor managed service for Prometheus and Azure Monitor alerts |

The managed Prometheus services of all three clouds evaluate the
[alert rules](#alert-rules) below unchanged, so most of each platform guide is
about collecting the metrics and routing the alerts.

## What to expose

Every deployment needs the same three outputs from the application.

**Metrics.** Install a Prometheus exporter before `App::run`, as described in
[Exporting metrics](../runtime.md#exporting-metrics-to-opentelemetry), and
configure histogram buckets for `beavers_stage_duration_seconds`. The examples
in this guide assume the exporter listens on port `9000` and serves `/metrics`.

**Health.** Enable the `health` feature and register a `HealthServer`. The
examples assume port `8080`. Readiness turns `503` during shutdown, receive
backoff, and output publish retries; liveness turns `503` after a subscription
fails.

**JSON logs.** Emit events as JSON so that cloud log services can match the
`subscription failed` event. With `tracing-subscriber` and its `json`
feature:

```rust,ignore
tracing_subscriber::fmt().json().init();
```

Each event becomes one line on standard output. The message is in
`fields.message`, so a failed subscription appears as:

```json
{"timestamp":"…","level":"ERROR","fields":{"message":"subscription failed","error":"…"},"target":"beavers::app"}
```

## What to alert on

A failed subscription stops the whole application: `App::run` returns the
error after the other subscriptions drain, and the process usually exits. The
last increment of `beavers_delivery_failures_total{action="stop"}` may
therefore never be scraped. Cover that case with at least one signal that
outlives the process: a restart alert from the orchestrator or a log-based
alert on `subscription failed`. Each platform guide shows both.

| Situation | Signal | Severity | Why |
|---|---|---|---|
| A subscription stopped on an unroutable failure | `beavers_delivery_failures_total{action="stop"}`, container restarts, the `subscription failed` log event | critical | Consumption stopped; the instance restarts and may fail again on the same record |
| Dead letters cannot be published | `beavers_publish_failures_total{sink="dead_letter"}` | critical | Failed deliveries cannot leave the subscription, so it stops making progress |
| Outputs cannot be published | `beavers_publish_failures_total{sink="output"}` for several minutes | critical | Deliveries wait for publish retries; readiness is `503` meanwhile |
| The source keeps failing | `beavers_receive_errors_total` for several minutes | critical | The broker is unreachable or rejects the client; readiness is `503` during backoff |
| Work is stuck | `beavers_deliveries_in_flight` above zero with no acknowledgement | critical | A handler or acknowledgement never completes; liveness does not detect this |
| Metrics disappeared | The scrape target is down | critical | Nothing else on this list can fire |
| Deliveries were dead-lettered | `beavers_delivery_failures_total{action="dead_letter"}` | warning | Someone needs to inspect and redrive them; see [reprocessing dead letters](../runtime.md#reprocessing-dead-letters) |
| Producers send undecodable records | Ratio of `failure="decode"` to received deliveries | warning | Usually a schema or producer change |
| Handlers are slow | The 99th percentile of `beavers_stage_duration_seconds{stage="handler"}` | warning | The backlog grows when processing falls behind the input rate |
| The backlog grows | Consumer lag or queue depth from the broker | warning | beavers does not report lag; see [Backlog](#backlog) |

Send `warning` alerts to the owning team's channel and `critical` alerts to
on-call paging. A dead letter needs investigation during working hours, while a
stopped subscription or a failing dead-letter sink stops consumption until
someone acts.

Alert on counts over a window, never on individual failures. A burst of bad
records produces one alert per subscription instead of one notification per
record.

## Alert rules

These rules use PromQL. Prometheus, Amazon Managed Service for Prometheus,
Google Cloud Managed Service for Prometheus, and Azure Monitor managed service
for Prometheus evaluate them as written; the platform guides show where each
expects them. Thresholds and windows are starting points; tune them to the
input rate and latency of each subscription.

```yaml
groups:
  - name: beavers
    rules:
      - alert: BeaversStoppedOnFailure
        expr: sum by (subscription) (increase(beavers_delivery_failures_total{action="stop"}[15m])) > 0
        labels:
          severity: critical
        annotations:
          summary: "{{ $labels.subscription }} stopped on an unroutable failure"
      - alert: BeaversDeadLetterPublishFailing
        expr: sum by (subscription) (increase(beavers_publish_failures_total{sink="dead_letter"}[5m])) > 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "{{ $labels.subscription }} cannot publish dead letters"
      - alert: BeaversOutputPublishFailing
        expr: sum by (subscription) (rate(beavers_publish_failures_total{sink="output"}[5m])) > 0
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "{{ $labels.subscription }} has failed to publish outputs for 10 minutes"
      - alert: BeaversReceiveFailing
        expr: sum by (subscription) (rate(beavers_receive_errors_total[5m])) > 0
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "{{ $labels.subscription }} has failed to receive from its source for 10 minutes"
      - alert: BeaversStalled
        expr: |
          max by (subscription) (beavers_deliveries_in_flight) > 0
            and sum by (subscription) (increase(beavers_deliveries_acknowledged_total[15m])) == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "{{ $labels.subscription }} has unfinished deliveries and acknowledged none in 15 minutes"
      - alert: BeaversMetricsDown
        expr: up{job="beavers"} == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "{{ $labels.instance }} does not serve metrics"
      - alert: BeaversDeadLetters
        expr: sum by (subscription) (increase(beavers_delivery_failures_total{action="dead_letter"}[10m])) > 0
        labels:
          severity: warning
        annotations:
          summary: "{{ $labels.subscription }} dead-lettered {{ $value }} deliveries in 10 minutes"
      - alert: BeaversDecodeFailureRatio
        expr: |
          sum by (subscription) (rate(beavers_delivery_failures_total{failure="decode"}[5m]))
            / sum by (subscription) (rate(beavers_deliveries_received_total[5m])) > 0.01
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "{{ $labels.subscription }} cannot decode over 1% of its input; check producers and schemas"
      - alert: BeaversHandlerSlow
        expr: |
          histogram_quantile(0.99,
            sum by (subscription, le) (rate(beavers_stage_duration_seconds_bucket{stage="handler"}[10m]))
          ) > 5
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "{{ $labels.subscription }} handler p99 is above 5 seconds"
```

`BeaversMetricsDown` matches the `job` label that the scrape configuration
assigns; replace `beavers` with that value. `BeaversHandlerSlow` needs
histogram buckets, and its threshold must lie within them. `BeaversStalled`
also fires for a subscription whose handler legitimately runs longer than 15
minutes; raise the window above the longest expected handler duration.

## Backlog

beavers does not report consumer lag yet; see the
[design plan](../plan.md#observability). Alert on the broker's own measurement
of the backlog, which also covers an application that is not running at all:

| Broker | Metric |
|---|---|
| Kafka with [kafka-exporter](https://github.com/danielqsj/kafka_exporter) | `kafka_consumergroup_lag` by `consumergroup` and `topic` |
| Amazon MSK | `MaxOffsetLag`, `SumOffsetLag`, or `EstimatedMaxTimeLag` by consumer group and topic |
| Pulsar | `pulsar_subscription_back_log` from the broker's `/metrics` |
| RabbitMQ with the `rabbitmq_prometheus` plugin | `rabbitmq_queue_messages_ready` from the per-object endpoint |
| Amazon SQS | `ApproximateAgeOfOldestMessage` and `ApproximateNumberOfMessagesVisible` |

Alert when the backlog keeps growing rather than when it is above zero, for
example `deriv(sum by (consumergroup) (kafka_consumergroup_lag)[15m:]) > 0`
for 30 minutes, or when the age of the oldest message exceeds what the
consumers of the output can tolerate. Apply the same alerts to a dead-letter
topic consumed by a [reprocessing subscription](../runtime.md#reprocessing-dead-letters) to see
how many dead letters remain unhandled.
