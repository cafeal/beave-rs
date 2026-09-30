# Local development brokers

`compose.yaml` at the repository root starts single-node Kafka and Apache Pulsar
brokers with web consoles for both. Use it to run the broker examples, the
ignored live tests, and manual experiments. It requires Docker with Compose v2.

```sh
make up          # or: docker compose up -d --wait
make down        # stops and removes the containers and their data
```

`make reset` recreates the environment from empty brokers. The brokers keep no
named volumes, so removing the containers discards every topic, offset, and
subscription.

| Service | Address | Purpose |
|---|---|---|
| Kafka | `localhost:9092` | Kafka 4.1 in KRaft mode |
| Kafka UI | <http://localhost:8080> | Topics, messages, and consumer groups (kafbat kafka-ui) |
| Pulsar | `pulsar://localhost:6650` | Pulsar 4.0 standalone |
| Pulsar admin API | <http://localhost:8081> | `pulsar-admin --admin-url` and REST |
| Pulsar Manager | <http://localhost:9527> | Tenants, topics, and subscriptions; log in as `admin` / `apachepulsar` |

These are the defaults of the live tests and examples, which also read
`KAFKA_BROKERS` and `PULSAR_URL` to target other brokers.

## Broker configuration

Kafka auto-creates topics with three partitions, so per-partition ordering and
parallelism are visible with a single broker. Replication factors are 1 and the
transaction state log accepts a single replica, which allows transactional
producers against the one broker. Applications on the host use the listener
advertised as `localhost:9092`; containers use `kafka:29092`.

Pulsar runs in standalone mode without the functions worker or stream storage
and with the transaction coordinator enabled. The coordinator is loaded when
the first transactional client connects. Other `standalone.conf` settings can
be added to the `pulsar` service environment, which `apply-config-from-env.py`
applies before the broker starts.
The one-shot `pulsar-init` service creates the `beavers-examples` subscription
on `persistent://public/default/orders` at the earliest position and the Pulsar
Manager login. Pulsar Manager is preconfigured with a `local` environment
pointing at the broker. The credentials are for local use only.

## Examples

Each broker example has a `produce` command that publishes sample JSON orders
keyed by customer, and a `process` command that runs a subscription until
Ctrl-C. Both commands are ordinary beavers applications: `produce` publishes an
`IterSource` through a broker sink, and `process` uses `Subscription::forward`.

| Command | Example | Flow |
|---|---|---|
| `make kafka-produce` | `kafka_orders` | 10 orders to Kafka `orders` |
| `make kafka-process` | `kafka_orders` | Kafka `orders` → `order-events` |
| `make pulsar-produce` | `pulsar_orders` | 10 orders to Pulsar `orders` |
| `make pulsar-process` | `pulsar_orders` | Pulsar `orders` → `order-events` with a `Key_Shared` subscription |
| `make kafka-to-pulsar` | `kafka_to_pulsar` | Kafka `orders` → Pulsar `orders-from-kafka` |

Set `COUNT` to change the number of orders, as in `make kafka-produce COUNT=100`.
The targets wrap `cargo run --example <name> --features <features> -- <command>`.

`kafka_to_pulsar` maps metadata explicitly with `MapMetadata`: the Kafka key
becomes the Pulsar key and Kafka headers with UTF-8 values become Pulsar
properties. It skips Kafka tombstones with `Tombstones::skip()`.

The Kafka examples read from the earliest offset when their consumer group has
no committed offset, and the Pulsar example uses the subscription created at
startup, so orders produced before a first `process` run are consumed. Run a
`process` command again to see that committed or acknowledged deliveries are
not processed twice.

Inspect the results in Kafka UI and Pulsar Manager, or from the command line:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 --topic order-events --from-beginning \
  --property print.key=true
docker compose exec pulsar bin/pulsar-admin topics stats \
  persistent://public/default/order-events
```

## Live tests

The Kafka and Pulsar integration tests are ignored by default. With the
environment running:

```sh
make test-live   # cargo test --features kafka,pulsar --test kafka --test pulsar -- --ignored
```

Each live test uses unique topic, group, and subscription names, so the tests
can run repeatedly without resetting the brokers.
