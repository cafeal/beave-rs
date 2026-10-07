# Google Cloud

Google Cloud Managed Service for Prometheus stores the metrics in Cloud
Monitoring, and Cloud Monitoring alerting policies evaluate the
[shared PromQL rules](README.md#alert-rules) against them. Notifications go to
Cloud Monitoring notification channels, such as email, Slack, PagerDuty, SMS,
webhooks, or Pub/Sub, so no Alertmanager is needed.

## Collect on GKE

Managed collection is enabled by default on recent GKE Autopilot and Standard
clusters. Expose the ports and probes as in the
[Kubernetes Deployment](kubernetes.md#deployment), then select the pods with a
`PodMonitoring` resource:

```yaml
apiVersion: monitoring.googleapis.com/v1
kind: PodMonitoring
metadata:
  name: beavers
  namespace: orders
spec:
  selector:
    matchLabels:
      app: orders-consumer
  endpoints:
    - port: metrics
      interval: 30s
```

The `job` label of the collected series is the `PodMonitoring` name,
`beavers`, which the `BeaversMetricsDown` rule expects.

## Collect on Cloud Run

A Cloud Run service that consumes from a broker must keep running between
requests: use instance-based billing and at least one minimum instance. Run
the OpenTelemetry Collector as a sidecar container that scrapes the exporter
and writes to Managed Service for Prometheus:

```yaml
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: beavers
          scrape_interval: 30s
          static_configs:
            - targets: ["localhost:9000"]

processors:
  resourcedetection:
    detectors: [env, gcp]
  batch: {}

exporters:
  googlemanagedprometheus: {}

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      processors: [resourcedetection, batch]
      exporters: [googlemanagedprometheus]
```

The `googlemanagedprometheus` exporter is part of the `otelcol-contrib`
distribution and of Google's built OpenTelemetry Collector image. The service
account needs the `roles/monitoring.metricWriter` role. Add a liveness probe on
`/livez` to the application container; Cloud Run then replaces an instance
whose subscription failed.

## Alerting policies

Create one alerting policy per rule. Each condition holds the PromQL
expression of a [shared rule](README.md#alert-rules) unchanged:

```yaml
# dead-letters.yaml
displayName: "beavers: dead letters"
severity: WARNING
combiner: OR
conditions:
  - displayName: Deliveries were dead-lettered
    conditionPrometheusQueryLanguage:
      query: sum by (subscription) (increase(beavers_delivery_failures_total{action="dead_letter"}[10m])) > 0
      duration: 0s
      evaluationInterval: 60s
alertStrategy:
  autoClose: 1800s
notificationChannels:
  - projects/my-project/notificationChannels/1234567890
documentation:
  mimeType: text/markdown
  content: |
    Inspect the dead-letter topic and redrive the deliveries once the cause is fixed.
```

```yaml
# stalled.yaml
displayName: "beavers: stalled subscription"
severity: CRITICAL
combiner: OR
conditions:
  - displayName: Unfinished deliveries without acknowledgements
    conditionPrometheusQueryLanguage:
      query: |
        max by (subscription) (beavers_deliveries_in_flight) > 0
          and sum by (subscription) (increase(beavers_deliveries_acknowledged_total[15m])) == 0
      duration: 300s
      evaluationInterval: 60s
alertStrategy:
  autoClose: 1800s
notificationChannels:
  - projects/my-project/notificationChannels/9876543210
```

```sh
gcloud monitoring policies create --policy-from-file=dead-letters.yaml
gcloud monitoring policies create --policy-from-file=stalled.yaml
```

The rule's `for` becomes `duration`, and the policy's `severity` takes the
value of the rule's `severity` label. Give `critical` policies the on-call channel and
`warning` policies the team's channel. Each time series that matches the
query opens its own incident, so a policy notifies once per failing
subscription.

On GKE, restarts are available as a Cloud Monitoring system metric, which
PromQL reads under its converted name:

```text
increase(kubernetes_io:container_restart_count{monitored_resource="k8s_container", namespace_name="orders", container_name="app"}[15m]) > 0
```

## Log-based alert

GKE and Cloud Run parse [JSON logs](README.md#what-to-expose) on standard
output into `jsonPayload`, so a log-based alerting policy can fire on the
`subscription failed` event even when the process exits before its last
scrape:

```yaml
displayName: "beavers: subscription failed"
severity: CRITICAL
combiner: OR
conditions:
  - displayName: subscription failed was logged
    conditionMatchedLog:
      filter: jsonPayload.fields.message="subscription failed"
      labelExtractors:
        error: EXTRACT(jsonPayload.fields.error)
alertStrategy:
  notificationRateLimit:
    period: 300s
  autoClose: 1800s
notificationChannels:
  - projects/my-project/notificationChannels/9876543210
```

## Backlog

Scrape kafka-exporter or the Pulsar broker with a `PodMonitoring` resource, or
a Collector sidecar outside GKE, and alert on the metrics listed in
[Backlog](README.md#backlog) with the same kind of PromQL policy.
