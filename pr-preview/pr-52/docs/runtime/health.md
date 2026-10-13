# Health checks

`App::health()` returns a cloneable `Health` handle. Obtain it before
`App::run`; it reports every subscription registered with the application.
`Health::report()` returns a `HealthReport` for the current moment:

| Field | Meaning |
|---|---|
| `live` | `false` once a subscription has failed, which stops the application |
| `ready` | The application is running, shutdown has not started, and every subscription is ready |
| `shutting_down` | Shutdown was requested by a signal, the `run_until` token, or a failure |
| `subscriptions` | One `SubscriptionReport` per subscription, in registration order |

Each `SubscriptionReport` carries the subscription `name`, its `status`
(`Pending` before the application runs, then `Running`, `Stopping` while it
drains, and finally `Stopped` or `Failed`), `receive_failures`, the
consecutive failed receives since the last successful one,
`receive_backoff`, whether the runtime is waiting for a receive retry delay,
and `publish_retries`, the deliveries whose output publication or transaction
commit failed and is waiting to be retried. A subscription is ready while it is
`Running` with no receive backoff and no publish retry. A subscription whose
source ended and that stopped without an error stays ready, so a finite source
next to a long-running one does not make the application unready.

Readiness reflects only what the runtime observes. A retried receive that waits
for input counts as ready again, because a source cannot tell an idle broker
from an unreachable one while its receive is pending. Dead-letter publish
retries do not affect readiness. Liveness reports that the runtime answers and
that no subscription failed; it does not detect a handler that never returns.

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
