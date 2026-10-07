# Local development brokers

`compose.yaml` at the repository root starts single-node Kafka, Apache Pulsar,
and RabbitMQ brokers and an ElasticMQ SQS emulator, with web consoles for each. Use it to run the broker examples, the
ignored live tests, and manual experiments. It requires Docker with Compose v2.

## Quick start

```sh
docker compose up -d --wait   # start the brokers and wait until they are healthy
cargo kafka-produce           # publish sample orders to Kafka
cargo kafka-process           # run the Kafka pipeline until Ctrl-C
cargo test-live               # run every ignored live test
docker compose down           # stop the brokers and discard their data
```

The `cargo` commands are aliases defined in `.cargo/config.toml`; they only
run `cargo run --example` and `cargo test` with the required features. Cargo
aliases cannot start Docker, so the brokers are always started and stopped with
`docker compose` directly.

## Managing the environment

| Command | Effect |
|---|---|
| `docker compose up -d --wait` | Starts every service and returns when the brokers pass their health checks |
| `docker compose up -d --wait kafka pulsar rabbitmq elasticmq` | Starts only the brokers, without the separate web consoles |
| `docker compose ps --all` | Shows the state of each service, including the finished `pulsar-init` |
| `docker compose logs -f pulsar` | Follows the logs of one service |
| `docker compose down` | Stops and removes the containers |

The brokers keep no named volumes, so `docker compose down` discards every
topic, offset, and subscription; the next `up` starts from empty brokers.
`docker compose stop` keeps the containers and their data for a later
`docker compose start`.

## Services

| Service | Address | Purpose |
|---|---|---|
| Kafka | `localhost:9092` | Kafka 4.1 in KRaft mode |
| Kafka UI | <http://localhost:8081> | Topics, messages, and consumer groups (kafbat kafka-ui) |
| Pulsar | `pulsar://localhost:6650` | Pulsar 4.0 standalone |
| Pulsar admin API | <http://localhost:8080> | `pulsar-admin --admin-url` and REST |
| Pulsar Manager | <http://localhost:9527> | Tenants, topics, and subscriptions; log in as `admin` / `apachepulsar` |
| RabbitMQ | `amqp://guest:guest@localhost:5672/%2f` | RabbitMQ 4.1 |
| RabbitMQ management | <http://localhost:15672> | Queues, exchanges, connections, and the management HTTP API; log in as `guest` / `guest` |
| ElasticMQ | <http://localhost:9324> | SQS-compatible API; queue URLs look like `http://localhost:9324/000000000000/orders` |
| ElasticMQ UI | <http://localhost:9325> | Queues and their message counts |

The examples and live tests use these addresses by default. Set these
variables to target other brokers:

| Variable | Default | Used by |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Kafka examples and live tests |
| `PULSAR_URL` | `pulsar://localhost:6650` in examples, `pulsar://127.0.0.1:6650` in tests | Pulsar examples and live tests |
| `HTTP_SINK_URL` | `http://127.0.0.1:8090/orders` | `kafka_to_http` example |
| `PULSAR_ADMIN_ADDR` | `127.0.0.1:8080` | Live tests that create Pulsar topics or subscriptions |
| `RABBITMQ_URL` | `amqp://guest:guest@127.0.0.1:5672/%2f` | RabbitMQ live tests |
| `RABBITMQ_MANAGEMENT_ADDR` | `127.0.0.1:15672` | RabbitMQ live tests that declare queues or close connections |
| `SQS_ENDPOINT` | `http://127.0.0.1:9324` | SQS live tests |

## Broker configuration

Kafka auto-creates topics with three partitions, so per-partition ordering and
parallelism are visible with a single broker. Replication factors are 1 and the
transaction state log accepts a single replica, which allows transactional
producers against the one broker. Applications on the host use the listener
advertised as `localhost:9092`; containers use `kafka:29092`.

Pulsar runs in standalone mode without the functions worker or stream storage.
`standalone.conf` settings can be added to the `pulsar` service environment, which `apply-config-from-env.py`
applies before the broker starts.

The one-shot `pulsar-init` service runs after the broker and Pulsar Manager
start. It creates the `beavers-examples` subscription on
`persistent://public/default/orders` at the earliest position, because a new
Pulsar subscription otherwise starts at the latest message, and it creates the
Pulsar Manager login. Pulsar Manager is preconfigured with a `local`
environment pointing at the broker. The credentials are for local use only.

RabbitMQ runs with its management plugin and the default `guest` user, which
the broker accepts from the host through the published port. It starts without
queues; the live tests declare their own through the management API.

ElasticMQ implements the SQS API in memory and accepts any credentials and
region. Point `SqsSourceConfig::endpoint_url` and `SqsSinkConfig::endpoint_url`
at it. It starts without queues; create one with the AWS CLI
(`aws --endpoint-url http://localhost:9324 sqs create-queue --queue-name orders`)
or the query API, as the live tests do. It does not reproduce every AWS limit
and behavior, so verify production settings against SQS itself.

## Examples

| Command | Example | Flow |
|---|---|---|
| `cargo kafka-produce [count]` | `kafka_orders` | Publishes orders, 10 by default, to Kafka `orders` |
| `cargo kafka-process` | `kafka_orders` | Kafka `orders` → `order-events` |
| `cargo pulsar-produce [count]` | `pulsar_orders` | Publishes orders, 10 by default, to Pulsar `orders` |
| `cargo pulsar-process` | `pulsar_orders` | Pulsar `orders` → `order-events` with a `Key_Shared` subscription |
| `cargo kafka-to-pulsar` | `kafka_to_pulsar` | Kafka `orders` → Pulsar `orders-from-kafka` |
| `cargo http-receive` | `kafka_to_http` | HTTP server on `127.0.0.1:8090` that prints the orders it receives |
| `cargo kafka-to-http` | `kafka_to_http` | Kafka `orders` → HTTP `POST` to `HTTP_SINK_URL` |

Each alias expands to `cargo run --example <name> --features <features> --
<command>`, and arguments after the alias are appended, as in
`cargo kafka-produce 100`.

The `produce` commands are ordinary beavers applications that publish an
`IterSource` of JSON orders keyed by customer through a broker sink, and end
when the source is exhausted. The `process` commands and `kafka-to-pulsar` run
a subscription until Ctrl-C. `process` registers a value handler, so each
output inherits the input's key and headers or properties.

`kafka_to_http` sends each order to `HTTP_SINK_URL`, by default the
`http-receive` server, with an `idempotency-key` header built from the record's
topic, partition, and offset. Stop `http-receive` while `kafka-to-http` runs to
see the sink retry, stop after its publish retries, and resume from the
uncommitted offset on the next run.

`kafka_to_pulsar` maps metadata explicitly with `MapMetadata`: the Kafka key
becomes the Pulsar key and Kafka headers with UTF-8 values become Pulsar
properties. It skips Kafka tombstones with `Tombstones::skip()`.

The Kafka examples read from the earliest offset when their consumer group has
no committed offset, and the Pulsar example uses the subscription created by
`pulsar-init`, so orders produced before a first `process` run are consumed.
Run a `process` command again to see that committed or acknowledged deliveries
are not processed twice.

GitHub Actions runs each example flow in its own `Example` job, which starts
the services the flow needs and runs `.github/scripts/example.sh`. The script
runs the commands above and checks that 10 outputs arrive on the output topic
or at the HTTP receiver. Run the script locally with the same scenario name, such
as `.github/scripts/example.sh kafka-to-http`, after starting the brokers.

## Inspecting broker state

Kafka UI shows topics, partitions, messages, and consumer group offsets. Pulsar
Manager shows tenants, namespaces, topics, and subscription backlogs. The same
information is available from the command line:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 --topic order-events --from-beginning \
  --property print.key=true
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:29092 --describe --group beavers-examples
docker compose exec pulsar bin/pulsar-admin topics stats \
  persistent://public/default/order-events
```

## Live tests

The Kafka, Pulsar, RabbitMQ, and SQS adapter tests in `tests/kafka.rs`,
`tests/pulsar.rs`, `tests/rabbitmq.rs`, and `tests/sqs.rs` and the end-to-end pipeline tests in `tests/pipelines.rs` are ignored by
default. With the brokers running:

```sh
cargo test-live             # cargo test --features kafka,pulsar,rabbitmq,sqs -- --ignored
cargo test-live pipeline    # only tests whose name contains "pipeline"
```

The pipeline tests run the same flows as the examples as complete
applications: a Kafka pipeline that inherits keys and headers and commits every
consumed offset, a Pulsar pipeline that inherits keys and properties and leaves
no subscription backlog, and a Kafka-to-Pulsar pipeline with an explicit
metadata mapping. They create Pulsar subscriptions through the admin API before
producing input.

Every live test uses unique topic, group, subscription, and queue names, so the
tests can run repeatedly without resetting the brokers. GitHub Actions runs them
in the `Live broker tests` job, which starts the `kafka`, `pulsar`,
`rabbitmq`, and `elasticmq` services from
`compose.yaml` and runs `cargo test --all-features -- --ignored`.

## Troubleshooting

- **A port is already in use.** The environment publishes ports 9092, 6650,
  8080, 8081, 9527, 5672, 15672, 9324, and 9325. Stop the conflicting process, or change the host side
  of the mapping in `compose.yaml`; the Kafka port must stay 9092 unless the
  advertised `HOST` listener changes with it.
- **An example or test waits without output.** The brokers are probably not
  running. Check `docker compose ps`. A Kafka producer keeps retrying an
  unreachable broker until its delivery timeout expires.
- **A Pulsar consumer misses messages produced before it started.** New
  subscriptions start at the latest message. Create the subscription first, as
  `pulsar-init` and the pipeline tests do.
- **Stale state from an earlier session.** Run `docker compose down` and start
  again.
