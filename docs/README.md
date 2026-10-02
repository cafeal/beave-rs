# Documentation

| Document | Purpose |
|---|---|
| [Architecture](architecture.md) | Module layout, trait contracts, and extension boundaries |
| [Adapters](adapters.md) | Local, channel, Kafka, Pulsar, RabbitMQ, SQS, and HTTP adapters, usage, and extension contracts |
| [Codecs](codecs.md) | Serialization contracts and JSON, raw bytes, UTF-8, Protobuf, and Avro codecs |
| [Testing](testing.md) | Testing handlers, middleware, and error policies with fabricated broker records |
| [Runtime](runtime.md) | Implemented APIs, processing behavior, configuration, and limitations |
| [Design plan](plan.md) | Product goals, agreed design direction, future work, and open questions |
| [Local development brokers](development.md) | Docker Compose Kafka, Pulsar, RabbitMQ, and ElasticMQ brokers, web consoles, broker examples, and live tests |

The runtime guide describes what works today. The design plan includes future
APIs and capabilities; its examples are conceptual unless explicitly identified
as implemented.

Adapter references: [Channel](adapters/channel.md), [Kafka](adapters/kafka.md),
[Pulsar](adapters/pulsar.md), [RabbitMQ](adapters/rabbitmq.md),
[SQS](adapters/sqs.md), and [HTTP](adapters/http.md).

Codec references: [Raw bytes and UTF-8](codecs/raw-utf8.md),
[Protobuf](codecs/protobuf.md), and [Avro](codecs/avro.md).

Keep repository documentation, code comments, and examples in English. Keep the
root README focused on the project overview and quick start. Put detailed user
and contributor explanations here, and document public API contracts alongside
their Rust definitions. When behavior changes, update the relevant guide and
keep planned capabilities distinct from implemented ones.

## Documentation site

The guides in this directory are published with [mdBook](https://rust-lang.github.io/mdBook/)
at <https://cafeal.github.io/beave-rs/>, together with the
[API reference](https://cafeal.github.io/beave-rs/api/beavers/) built by
rustdoc with every feature enabled. `SUMMARY.md` defines the site navigation, so
add new guides there. Links between guides are relative `.md` links; link to
files outside `docs/`, such as examples, by their GitHub URL.

Preview the site locally:

```sh
cargo install mdbook
mdbook serve --open
```

The Pages workflow builds the site on every pull request and deploys it when
`main` changes.
