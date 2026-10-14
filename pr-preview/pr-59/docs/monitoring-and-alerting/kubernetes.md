# Kubernetes

This guide runs Prometheus and Alertmanager in the cluster with the
[Prometheus Operator](https://prometheus-operator.dev/). On EKS, GKE, or AKS,
the cloud's managed Prometheus avoids operating them yourself; see
[AWS](aws.md), [Google Cloud](gcp.md), or [Azure](azure.md). The
[Deployment](#deployment) below applies to every cluster.

## Deployment

Expose the metrics and health ports, probe the health server, and give
draining enough time before Kubernetes kills the container:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders-consumer
  namespace: orders
spec:
  replicas: 2
  selector:
    matchLabels:
      app: orders-consumer
  template:
    metadata:
      labels:
        app: orders-consumer
    spec:
      terminationGracePeriodSeconds: 60
      containers:
        - name: app
          image: registry.example.com/orders-consumer:1.0.0
          ports:
            - name: health
              containerPort: 8080
            - name: metrics
              containerPort: 9000
          livenessProbe:
            httpGet: { path: /livez, port: health }
            periodSeconds: 10
            failureThreshold: 3
          readinessProbe:
            httpGet: { path: /readyz, port: health }
            periodSeconds: 5
            failureThreshold: 2
```

Readiness removes an instance that serves an [HTTP source](../adapters/http.md)
from its Service while it drains or retries. For broker sources, readiness only
affects rollouts: a rolling update waits until new pods are ready. Liveness
restarts a container whose subscription failed and that has not exited yet.

`terminationGracePeriodSeconds` must cover the drain of in-flight deliveries,
including publish retries, after `SIGTERM`. A container killed before it
finishes draining leaves unacknowledged deliveries that the broker redelivers.

## Install the monitoring stack

The `kube-prometheus-stack` Helm chart installs the operator, Prometheus,
Alertmanager, kube-state-metrics, and default Kubernetes alerts:

```sh
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring --create-namespace \
  --values monitoring-values.yaml
```

By default the chart's Prometheus selects only `PodMonitor` and
`PrometheusRule` resources labelled `release: monitoring`, the Helm release
name. The examples below carry that label.

## Scrape the metrics

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: beavers
  namespace: orders
  labels:
    release: monitoring
spec:
  selector:
    matchLabels:
      app: orders-consumer
  podMetricsEndpoints:
    - port: metrics
      interval: 30s
```

The operator sets the `job` label of a `PodMonitor` target to
`<namespace>/<name>`, here `orders/beavers`. Use that value in the
`BeaversMetricsDown` rule.

## Alert rules

Wrap the [shared rules](README.md#alert-rules) in a `PrometheusRule` and add a
restart alert for the application, which also fires when the failed
subscription's last metrics were never scraped:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: beavers
  namespace: orders
  labels:
    release: monitoring
spec:
  groups:
    - name: beavers
      rules:
        # The shared rules from README.md#alert-rules, with
        # up{job="orders/beavers"} in BeaversMetricsDown.
        - alert: BeaversRestarted
          expr: increase(kube_pod_container_status_restarts_total{namespace="orders", container="app"}[15m]) > 0
          labels:
            severity: critical
          annotations:
            summary: "{{ $labels.pod }} restarted; check its previous logs for 'subscription failed'"
```

`kube-prometheus-stack` already alerts on crash loops with
`KubePodCrashLooping`, but only after repeated restarts. `BeaversRestarted`
fires on the first one, because a subscription that stops on a poison record
restarts and fails again on the same record until someone dead-letters or
skips it.

## Route alerts

Configure Alertmanager in `monitoring-values.yaml`. This example sends every
alert to a Slack channel and pages on `critical` alerts:

```yaml
alertmanager:
  config:
    route:
      receiver: team-slack
      group_by: [alertname, subscription]
      routes:
        - receiver: "null"
          matchers: ['alertname="Watchdog"']
        - receiver: on-call
          matchers: ['severity="critical"']
          continue: true
        - receiver: team-slack
    receivers:
      - name: "null"
      - name: team-slack
        slack_configs:
          - api_url_file: /etc/alertmanager/secrets/alertmanager-slack/url
            channel: "#orders-alerts"
            send_resolved: true
            title: "{{ .CommonLabels.alertname }}"
            text: "{{ range .Alerts }}{{ .Annotations.summary }}\n{{ end }}"
      - name: on-call
        pagerduty_configs:
          - routing_key_file: /etc/alertmanager/secrets/alertmanager-pagerduty/key
  alertmanagerSpec:
    secrets: [alertmanager-slack, alertmanager-pagerduty]
```

Store the Slack webhook URL and the PagerDuty integration key in the
`alertmanager-slack` and `alertmanager-pagerduty` Secrets of the `monitoring`
namespace. Grouping by `alertname` and `subscription` sends one notification
per failing subscription, however many deliveries failed.

## Logs

Kubernetes keeps only the logs of the current and the previous container. Ship
the [JSON logs](README.md#what-to-expose) to a log store, such as Loki or the
cloud's log service, to read the `subscription failed` event after a restart.
With Loki, a ruler rule alerts on the event directly:

```yaml
groups:
  - name: beavers-logs
    rules:
      - alert: BeaversSubscriptionFailed
        expr: |
          sum by (namespace, pod) (
            count_over_time({namespace="orders"} | json | fields_message="subscription failed" [10m])
          ) > 0
        labels:
          severity: critical
        annotations:
          summary: "{{ $labels.pod }} logged 'subscription failed'"
```
