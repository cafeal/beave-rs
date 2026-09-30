# Shortcuts for the local brokers in compose.yaml. See docs/development.md.

COUNT ?= 10
CARGO_RUN := cargo run --example

.PHONY: up down reset ps logs test-live \
	kafka-produce kafka-process pulsar-produce pulsar-process kafka-to-pulsar

up:
	docker compose up -d --wait

down:
	docker compose down

reset:
	docker compose down --volumes --remove-orphans
	docker compose up -d --wait

ps:
	docker compose ps --all

logs:
	docker compose logs --follow

test-live:
	cargo test --features kafka,pulsar -- --ignored

kafka-produce:
	$(CARGO_RUN) kafka_orders --features kafka -- produce $(COUNT)

kafka-process:
	$(CARGO_RUN) kafka_orders --features kafka -- process

pulsar-produce:
	$(CARGO_RUN) pulsar_orders --features pulsar -- produce $(COUNT)

pulsar-process:
	$(CARGO_RUN) pulsar_orders --features pulsar -- process

kafka-to-pulsar:
	$(CARGO_RUN) kafka_to_pulsar --features kafka,pulsar
