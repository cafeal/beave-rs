# Health checks

`App::health()` returns a cloneable `Health` handle. Obtain it before
`App::run`; it reports every subscription registered with the application.
`Health::report()` returns a `HealthReport` for the current moment:

| Field | Meaning |
|---|---|
| `live` | `false` once a subscription has failed, which stops the application, or while a subscription is stalled |
| `ready` | The application is running, shutdown has not started, and every subscription is ready |
| `shutting_down` | Shutdown was requested by a signal, the `run_until` token, or a failure |
| `subscriptions` | One `SubscriptionReport` per subscription, in registration order |

Each `SubscriptionReport` carries the subscription `name`, its `status`
(`Pending` before the application runs, then `Running`, `Stopping` while it
drains, and finally `Stopped` or `Failed`), `receive_failures`, the
consecutive failed receives since the last successful one,
`receive_backoff`, whether the runtime is waiting for a receive retry delay,
`publish_retries`, the deliveries whose output publication or transaction
commit failed and is waiting to be retried, `connected` and `lag` from the
source, described below, and `stalled`. A subscription is ready while it is
`Running` with no receive backoff, no publish retry, and a source that does not
report itself disconnected. A subscription whose source ended and that stopped
without an error stays ready, so a finite source next to a long-running one
does not make the application unready.

A retried receive that waits for input counts as ready again, because the
runtime cannot tell an idle broker from an unreachable one while a receive is
pending; the source's connection state covers that case where the source
reports it. Dead-letter publish retries do not affect readiness.

## Connection state and lag

A source can report its connection state and consumer lag through
`Source::probe`, which the health report reads on every request:

| Adapter | `connected` | `lag` |
|---|---|---|
| Kafka | Any broker connection is up, from librdkafka statistics | Over the partitions the consumer fetches, the distance from the first offset the source has not committed to the end of the partition |
| Pulsar | Every partition's consumer is connected | Not reported |
| Other adapters | Not reported | Not reported |

Both values are `null` until the source has connected, and for Kafka until the
first statistics arrive. Kafka lag includes records that were received but not
acknowledged yet. The Kafka source sets `statistics.interval.ms` to 5000 unless
the handle or source sets it; `0` disables statistics and leaves both values
unreported. librdkafka delivers statistics while the source polls its consumer,
which the runtime does whenever it has room to receive, so while every
delivery slot stays busy the Kafka values keep their last reading. A source reporting `connected: false` makes its
subscription unready. The Kafka and Pulsar clients reconnect by themselves, and
readiness returns when they do.

Lag never affects readiness: taking every instance out of rotation does not
reduce a backlog. Alert on it instead, with `beavers_consumer_lag` or the
broker-side measurements in the
[monitoring guide](../monitoring-and-alerting/README.md#backlog), which also
cover the other adapters and an application that is not running.

## Stall detection

Liveness reports that the runtime answers and that no subscription failed. With
`stall_timeout`, it also fails while a subscription is `stalled`: deliveries are
in flight, but no handler attempt, publish or commit attempt, acknowledgement,
or newly received delivery has occurred for that duration.

```rust,ignore
Subscription::new("orders", source, sink, handler)
    .stall_timeout(Duration::from_secs(300))
```

An idle subscription with nothing in flight is never stalled, and each failed
retry attempt counts as progress, so backoff does not trigger it. Stall
detection is disabled by default; choose a duration longer than the slowest
expected handler attempt, because a restart abandons the deliveries in
progress. It detects a subscription in which nothing progresses. One stuck
delivery while others still complete, such as one ordering key blocked under
`ProcessingOrder::PerKey`, keeps the subscription live; its partition's commit
stops advancing, which shows as growing lag.

With the `health` feature, `HealthServer` answers probes over HTTP/1.1 while
the application runs:

```rust,ignore
let server = HealthServer::bind("0.0.0.0:8080".parse()?)?;
App::new()
    .subscription(subscription)
    .health_server(server)
    .run()
    .await?;
```

`GET /livez` answers `200 OK` when the report is live and `GET /readyz` when it
is ready; otherwise each answers `503 Service Unavailable`. Both return the
report as a JSON body. Other paths answer `404 Not Found` and other methods
`405 Method Not Allowed`. `HealthServer::bind` binds the listener immediately,
so address errors surface before the application runs and `local_addr`
reports the port chosen for port 0. The server starts with the application and
stops after every subscription has finished. Readiness therefore turns `503`
as soon as shutdown starts, which removes an instance serving an
[HTTP source](../adapters/http.md) from a Kubernetes Service while it drains,
and liveness keeps answering until draining completes. Use the readiness probe
for traffic routing and the liveness probe for restarts:

```yaml
livenessProbe:
  httpGet: { path: /livez, port: 8080 }
readinessProbe:
  httpGet: { path: /readyz, port: 8080 }
```

The health server does not export metrics; [metrics](observability.md#metrics) go through the
`metrics` facade and the recorder the application installs.
