# Azure

Choose by where the application runs:

- **AKS:** collect metrics with
  [Azure Monitor managed service for Prometheus](#aks) and evaluate the
  [shared PromQL rules](README.md#alert-rules) as Prometheus rule groups.
- **Azure Container Apps:** send metrics to Application Insights with an
  OpenTelemetry Collector sidecar and alert with
  [log search alerts](#azure-container-apps).

Both deliver alerts through an Azure Monitor action group, which notifies by
email, SMS, push, voice, or webhook and integrates with ITSM tools. Create one
action group for on-call paging and one for the team:

```bicep
resource onCall 'Microsoft.Insights/actionGroups@2023-01-01' = {
  name: 'beavers-critical'
  location: 'global'
  properties: {
    groupShortName: 'bvcrit'
    enabled: true
    emailReceivers: [
      { name: 'on-call', emailAddress: 'on-call@example.com', useCommonAlertSchema: true }
    ]
    webhookReceivers: [
      { name: 'pager', serviceUri: 'https://events.example.com/azure', useCommonAlertSchema: true }
    ]
  }
}
```

Define a `team` action group the same way with the team's receivers.

## AKS

### Collect

Enable managed Prometheus on the cluster with an Azure Monitor workspace:

```sh
az aks update --resource-group orders-rg --name orders-aks \
  --enable-azure-monitor-metrics \
  --azure-monitor-workspace-resource-id "$WORKSPACE_ID"
```

Expose the ports and probes as in the
[Kubernetes Deployment](kubernetes.md#deployment), then select the pods with a
`PodMonitor`. The managed add-on reads its own API group,
`azmonitoring.coreos.com`, not the Prometheus Operator's:

```yaml
apiVersion: azmonitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: beavers
  namespace: orders
spec:
  selector:
    matchLabels:
      app: orders-consumer
  podMetricsEndpoints:
    - port: metrics
      interval: 30s
```

Check the `job` label of the collected `up` series in the workspace's query
editor and use it in the `BeaversMetricsDown` rule.

### Alert rules

Prometheus rule groups are Azure resources. Each rule takes the expression of a
[shared rule](README.md#alert-rules) unchanged, a numeric severity from `0`
(critical) to `4` (verbose), and the action group to notify:

```bicep
param workspaceId string
param clusterName string
param location string = resourceGroup().location

resource beavers 'Microsoft.AlertsManagement/prometheusRuleGroups@2023-03-01' = {
  name: 'beavers'
  location: location
  properties: {
    scopes: [workspaceId]
    clusterName: clusterName
    interval: 'PT1M'
    rules: [
      {
        alert: 'BeaversStalled'
        expression: 'max by (subscription) (beavers_deliveries_in_flight) > 0 and sum by (subscription) (increase(beavers_deliveries_acknowledged_total[15m])) == 0'
        for: 'PT5M'
        severity: 0
        annotations: {
          summary: 'A subscription has unfinished deliveries and acknowledged none in 15 minutes'
        }
        actions: [{ actionGroupId: onCall.id }]
        resolveConfiguration: { autoResolved: true, timeToResolve: 'PT10M' }
      }
      {
        alert: 'BeaversDeadLetters'
        expression: 'sum by (subscription) (increase(beavers_delivery_failures_total{action="dead_letter"}[10m])) > 0'
        severity: 2
        actions: [{ actionGroupId: team.id }]
        resolveConfiguration: { autoResolved: true, timeToResolve: 'PT10M' }
      }
      {
        alert: 'BeaversRestarted'
        expression: 'increase(kube_pod_container_status_restarts_total{namespace="orders", container="app"}[15m]) > 0'
        severity: 0
        actions: [{ actionGroupId: onCall.id }]
        resolveConfiguration: { autoResolved: true, timeToResolve: 'PT10M' }
      }
      // The remaining shared rules follow the same shape.
    ]
  }
}
```

`onCall` and `team` are the action groups defined above. A rule's `for` is an
ISO 8601 duration, so `10m` becomes `PT10M`. The managed add-on scrapes
kube-state-metrics by default, which provides
`kube_pod_container_status_restarts_total` for the restart rule.

### Logs

With Container insights enabled, [JSON logs](README.md#what-to-expose) arrive
in the `ContainerLogV2` table of the Log Analytics workspace. A log search
alert rule with this query, evaluated every 5 minutes with a threshold of
`Count > 0`, fires on the `subscription failed` event:

```kusto
ContainerLogV2
| where PodNamespace == "orders" and ContainerName == "app"
| where tostring(LogMessage.fields.message) == "subscription failed"
| project TimeGenerated, PodName, Error = tostring(LogMessage.fields.error)
```

## Azure Container Apps

### Health probes

Container Apps supports HTTP liveness and readiness probes on the application
container:

```yaml
probes:
  - type: Liveness
    httpGet: { path: /livez, port: 8080 }
    periodSeconds: 10
  - type: Readiness
    httpGet: { path: /readyz, port: 8080 }
    periodSeconds: 5
```

A broker consumer must keep at least one replica running: set the minimum
replica count to `1` or more.

### Collect

Run the OpenTelemetry Collector `otelcol-contrib` as a sidecar container. It
scrapes the exporter, converts counters to per-interval increases, and sends
them to Application Insights:

```yaml
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: beavers
          scrape_interval: 60s
          static_configs:
            - targets: ["localhost:9000"]

processors:
  cumulativetodelta: {}
  batch: {}

exporters:
  azuremonitor:
    connection_string: ${env:APPLICATIONINSIGHTS_CONNECTION_STRING}

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      processors: [cumulativetodelta, batch]
      exporters: [azuremonitor]
```

Metrics arrive in the `customMetrics` table with their labels in
`customDimensions`.

### Alerts

Create log search alert rules on the Application Insights resource. Each
query returns one row per subscription that should alert; set the threshold to
`Count > 0` and split by the `subscription` column so that each subscription
alerts separately:

```kusto
// Dead letters in the last 10 minutes; severity 2, team action group.
customMetrics
| where timestamp > ago(10m)
| where name == "beavers_delivery_failures_total"
| where tostring(customDimensions.action) == "dead_letter"
| summarize Count = sum(valueSum) by subscription = tostring(customDimensions.subscription)
| where Count > 0
```

```kusto
// Failed dead-letter publication in the last 10 minutes; severity 0, on-call.
customMetrics
| where timestamp > ago(10m)
| where name == "beavers_publish_failures_total"
| where tostring(customDimensions.sink) == "dead_letter"
| summarize Count = sum(valueSum) by subscription = tostring(customDimensions.subscription)
| where Count > 0
```

Other counter alerts follow the same shape. The application's console output,
including the `subscription failed` event, is in the
`ContainerAppConsoleLogs_CL` table of the environment's Log Analytics
workspace:

```kusto
ContainerAppConsoleLogs_CL
| where ContainerAppName_s == "orders-consumer"
| where Log_s has "\"subscription failed\""
```

The `RestartCount` platform metric of the container app supports a metric
alert rule that fires on restarts without any collector.

## Backlog

Scrape kafka-exporter or the Pulsar broker with a `PodMonitor` on AKS, or with
the Collector sidecar on Container Apps, and alert on the metrics listed in
[Backlog](README.md#backlog).
