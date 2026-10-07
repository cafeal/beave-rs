# AWS

Choose by where the application runs:

- **Amazon EKS:** collect metrics into
  [Amazon Managed Service for Prometheus](#amazon-managed-service-for-prometheus)
  and evaluate the [shared PromQL rules](README.md#alert-rules) there.
- **Amazon ECS or EC2:** send metrics to [CloudWatch](#cloudwatch) with the
  AWS Distro for OpenTelemetry (ADOT) Collector and alert with CloudWatch
  alarms.

Both paths deliver alerts through an Amazon SNS topic. Subscribe an email
address, an HTTPS endpoint, or an incident tool that accepts SNS, such as
PagerDuty or Opsgenie. CloudWatch alarms can also reach Slack or Microsoft
Teams through Amazon Q Developer in chat applications.

## Amazon Managed Service for Prometheus

### Collect

Create a workspace and an Amazon Managed Service for Prometheus collector, the
agentless scraper, for the EKS cluster. Give the scraper a configuration that
discovers the application's pods:

```yaml
global:
  scrape_interval: 30s
scrape_configs:
  - job_name: beavers
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app]
        regex: orders-consumer
        action: keep
      - source_labels: [__meta_kubernetes_pod_container_port_name]
        regex: metrics
        action: keep
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
```

The scraper needs the cluster role and access entry described in the
Amazon Managed Service for Prometheus documentation. A cluster that already
runs ADOT or Prometheus in agent mode can remote-write to the workspace
instead. Expose the ports and probes as in the
[Kubernetes Deployment](kubernetes.md#deployment).

### Alert rules

Save the [shared rules](README.md#alert-rules) as `beavers-rules.yaml`, add the
[restart rule](kubernetes.md#alert-rules) if the cluster runs
kube-state-metrics, and upload them as a rule groups namespace:

```sh
aws amp create-rule-groups-namespace \
  --workspace-id ws-0123456789abcdef \
  --name beavers \
  --data fileb://beavers-rules.yaml
```

Use `aws amp put-rule-groups-namespace` with the same arguments to update it.

### Route alerts

The workspace's alert manager accepts the Alertmanager configuration format,
wrapped in `alertmanager_config`, and delivers only to SNS. Route by severity
to two topics:

```yaml
alertmanager_config: |
  route:
    receiver: team
    group_by: [alertname, subscription]
    routes:
      - receiver: on-call
        matchers: ['severity="critical"']
  receivers:
    - name: team
      sns_configs:
        - topic_arn: arn:aws:sns:ap-northeast-1:123456789012:beavers-warning
          sigv4:
            region: ap-northeast-1
          subject: "{{ .CommonLabels.alertname }}"
          message: "{{ range .Alerts }}{{ .Annotations.summary }}\n{{ end }}"
    - name: on-call
      sns_configs:
        - topic_arn: arn:aws:sns:ap-northeast-1:123456789012:beavers-critical
          sigv4:
            region: ap-northeast-1
          subject: "{{ .CommonLabels.alertname }}"
          message: "{{ range .Alerts }}{{ .Annotations.summary }}\n{{ end }}"
```

```sh
aws amp create-alert-manager-definition \
  --workspace-id ws-0123456789abcdef \
  --data fileb://alertmanager.yaml
```

Each topic's access policy must let the service publish:

```json
{
  "Effect": "Allow",
  "Principal": { "Service": "aps.amazonaws.com" },
  "Action": ["sns:Publish", "sns:GetTopicAttributes"],
  "Resource": "arn:aws:sns:ap-northeast-1:123456789012:beavers-critical",
  "Condition": {
    "ArnEquals": { "aws:SourceArn": "arn:aws:aps:ap-northeast-1:123456789012:workspace/ws-0123456789abcdef" },
    "StringEquals": { "AWS:SourceAccount": "123456789012" }
  }
}
```

## CloudWatch

### Collect

Run the ADOT Collector as a sidecar container in the ECS task, or as a service
on the EC2 instance. It scrapes the exporter and writes CloudWatch metrics
through the embedded metric format:

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
  batch: {}

exporters:
  awsemf:
    namespace: Beavers
    log_group_name: /beavers/metrics
    dimension_rollup_option: NoDimensionRollup
    metric_declarations:
      - dimensions: [[subscription]]
        metric_name_selectors:
          - "^beavers_deliveries_.*"
          - "^beavers_receive_errors_total$"
      - dimensions: [[subscription, action], [subscription, failure]]
        metric_name_selectors:
          - "^beavers_delivery_failures_total$"
      - dimensions: [[subscription, sink]]
        metric_name_selectors:
          - "^beavers_publish_failures_total$"

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      processors: [batch]
      exporters: [awsemf]
```

`awsemf` converts cumulative counters to the increase since the previous
scrape, so the `Sum` statistic of a counter over an alarm period is the number
of events in that period. Only metrics matched by `metric_declarations` become
CloudWatch metrics, and each dimension set is published separately: declaring
`[subscription, action]` lets an alarm count dead letters of every failure kind
together. The task role needs `logs:PutLogEvents`, `logs:CreateLogStream`, and
`logs:CreateLogGroup` on the log group.

The handler latency histogram is left out because CloudWatch cannot derive a
99th percentile from Prometheus buckets; time the handler with a CloudWatch
metric of its own if latency needs an alarm.

### Alarms

This CloudFormation template creates the alarms for one subscription. Repeat
the resources, or generate them, for each subscription:

```yaml
Parameters:
  Subscription:
    Type: String
  CriticalTopicArn:
    Type: String
  WarningTopicArn:
    Type: String

Resources:
  StoppedOnFailure:
    Type: AWS::CloudWatch::Alarm
    Properties:
      AlarmName: !Sub beavers-${Subscription}-stopped
      Namespace: Beavers
      MetricName: beavers_delivery_failures_total
      Dimensions:
        - { Name: subscription, Value: !Ref Subscription }
        - { Name: action, Value: stop }
      Statistic: Sum
      Period: 300
      EvaluationPeriods: 1
      Threshold: 0
      ComparisonOperator: GreaterThanThreshold
      TreatMissingData: notBreaching
      AlarmActions: [!Ref CriticalTopicArn]

  DeadLetterPublishFailing:
    Type: AWS::CloudWatch::Alarm
    Properties:
      AlarmName: !Sub beavers-${Subscription}-dead-letter-publish
      Namespace: Beavers
      MetricName: beavers_publish_failures_total
      Dimensions:
        - { Name: subscription, Value: !Ref Subscription }
        - { Name: sink, Value: dead_letter }
      Statistic: Sum
      Period: 300
      EvaluationPeriods: 2
      Threshold: 0
      ComparisonOperator: GreaterThanThreshold
      TreatMissingData: notBreaching
      AlarmActions: [!Ref CriticalTopicArn]
      OKActions: [!Ref CriticalTopicArn]

  OutputPublishFailing:
    Type: AWS::CloudWatch::Alarm
    Properties:
      AlarmName: !Sub beavers-${Subscription}-output-publish
      Namespace: Beavers
      MetricName: beavers_publish_failures_total
      Dimensions:
        - { Name: subscription, Value: !Ref Subscription }
        - { Name: sink, Value: output }
      Statistic: Sum
      Period: 300
      EvaluationPeriods: 2
      Threshold: 0
      ComparisonOperator: GreaterThanThreshold
      TreatMissingData: notBreaching
      AlarmActions: [!Ref CriticalTopicArn]
      OKActions: [!Ref CriticalTopicArn]

  ReceiveFailing:
    Type: AWS::CloudWatch::Alarm
    Properties:
      AlarmName: !Sub beavers-${Subscription}-receive
      Namespace: Beavers
      MetricName: beavers_receive_errors_total
      Dimensions:
        - { Name: subscription, Value: !Ref Subscription }
      Statistic: Sum
      Period: 300
      EvaluationPeriods: 2
      Threshold: 0
      ComparisonOperator: GreaterThanThreshold
      TreatMissingData: notBreaching
      AlarmActions: [!Ref CriticalTopicArn]
      OKActions: [!Ref CriticalTopicArn]

  Stalled:
    Type: AWS::CloudWatch::Alarm
    Properties:
      AlarmName: !Sub beavers-${Subscription}-stalled
      Metrics:
        - Id: inflight
          ReturnData: false
          MetricStat:
            Metric:
              Namespace: Beavers
              MetricName: beavers_deliveries_in_flight
              Dimensions:
                - { Name: subscription, Value: !Ref Subscription }
            Period: 900
            Stat: Minimum
        - Id: acked
          ReturnData: false
          MetricStat:
            Metric:
              Namespace: Beavers
              MetricName: beavers_deliveries_acknowledged_total
              Dimensions:
                - { Name: subscription, Value: !Ref Subscription }
            Period: 900
            Stat: Sum
        - Id: stalled
          Expression: IF(inflight > 0 AND acked == 0, 1, 0)
          Label: stalled
      EvaluationPeriods: 1
      Threshold: 0
      ComparisonOperator: GreaterThanThreshold
      TreatMissingData: notBreaching
      AlarmActions: [!Ref CriticalTopicArn]

  DeadLetters:
    Type: AWS::CloudWatch::Alarm
    Properties:
      AlarmName: !Sub beavers-${Subscription}-dead-letters
      Namespace: Beavers
      MetricName: beavers_delivery_failures_total
      Dimensions:
        - { Name: subscription, Value: !Ref Subscription }
        - { Name: action, Value: dead_letter }
      Statistic: Sum
      Period: 600
      EvaluationPeriods: 1
      Threshold: 0
      ComparisonOperator: GreaterThanThreshold
      TreatMissingData: notBreaching
      AlarmActions: [!Ref WarningTopicArn]

  DecodeFailureRatio:
    Type: AWS::CloudWatch::Alarm
    Properties:
      AlarmName: !Sub beavers-${Subscription}-decode-ratio
      Metrics:
        - Id: decode
          ReturnData: false
          MetricStat:
            Metric:
              Namespace: Beavers
              MetricName: beavers_delivery_failures_total
              Dimensions:
                - { Name: subscription, Value: !Ref Subscription }
                - { Name: failure, Value: decode }
            Period: 300
            Stat: Sum
        - Id: received
          ReturnData: false
          MetricStat:
            Metric:
              Namespace: Beavers
              MetricName: beavers_deliveries_received_total
              Dimensions:
                - { Name: subscription, Value: !Ref Subscription }
            Period: 300
            Stat: Sum
        - Id: ratio
          Expression: FILL(decode, 0) / received
          Label: decode failure ratio
      EvaluationPeriods: 2
      Threshold: 0.01
      ComparisonOperator: GreaterThanThreshold
      TreatMissingData: notBreaching
      AlarmActions: [!Ref WarningTopicArn]
```

`TreatMissingData: notBreaching` keeps idle subscriptions quiet, but it also
keeps the alarms quiet when the collector stops sending metrics. Cover that
with the [task and log alerts](#task-and-log-alerts) below.

### Task and log alerts

A failed subscription makes the process exit, usually before its last metrics
are scraped. ECS reports the stopped task to Amazon EventBridge; route it to
the critical topic:

```json
{
  "source": ["aws.ecs"],
  "detail-type": ["ECS Task State Change"],
  "detail": {
    "clusterArn": ["arn:aws:ecs:ap-northeast-1:123456789012:cluster/orders"],
    "group": ["service:orders-consumer"],
    "lastStatus": ["STOPPED"],
    "stopCode": ["EssentialContainerExited"]
  }
}
```

With the `awslogs` log driver and [JSON logs](README.md#what-to-expose), a
metric filter on the application's log group counts `subscription failed`
events. Alarm on `Sum > 0` of the resulting metric, as for the other counters:

```yaml
SubscriptionFailedFilter:
  Type: AWS::Logs::MetricFilter
  Properties:
    LogGroupName: /ecs/orders-consumer
    FilterPattern: '{ $.fields.message = "subscription failed" }'
    MetricTransformations:
      - MetricNamespace: Beavers
        MetricName: subscription_failed
        MetricValue: "1"
        DefaultValue: 0
```

### Health checks

ECS has no HTTP probe of its own. Behind an Application Load Balancer, which
an [HTTP source](../adapters/http.md) needs, set the target group health check
path to `/readyz` so that a draining or retrying task stops receiving
requests. A container health check that calls `/livez` needs an HTTP client in
the image; a minimal image without one can rely on the process exiting after a
subscription fails, which ECS replaces according to the service's desired
count.

## Backlog

Amazon MSK publishes `MaxOffsetLag`, `SumOffsetLag`, and `EstimatedMaxTimeLag`
per consumer group and topic, and Amazon SQS publishes
`ApproximateAgeOfOldestMessage`. Alarm on them as described in
[Backlog](README.md#backlog); with Amazon Managed Service for Prometheus, a
self-managed Kafka cluster can be scraped with kafka-exporter instead.
