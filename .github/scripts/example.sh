#!/usr/bin/env bash
# Runs one example scenario from docs/development.md against the brokers of
# compose.yaml and checks that its outputs arrive. Long-running examples are
# started in the background and stopped when the check finishes.
#
#   .github/scripts/example.sh <kafka-orders|pulsar-orders|kafka-to-pulsar|kafka-to-http|transform>
set -euo pipefail

COUNT=10
TIMEOUT=120
logs=$(mktemp -d)
pids=()

cleanup() {
  status=$?
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if [ "$status" -ne 0 ]; then
    for log in "$logs"/*.log; do
      [ -e "$log" ] || continue
      echo "::group::$(basename "$log")"
      cat "$log"
      echo "::endgroup::"
    done
  fi
}
trap cleanup EXIT

# Builds an alias's example first so that compilation does not count toward
# the timeouts below.
build() {
  cargo build --locked --example "$1" --features "$2"
}

# Runs a Cargo alias in the background, logging to $logs/<alias>.log.
start() {
  cargo "$@" >"$logs/$1.log" 2>&1 &
  pids+=($!)
}

# Waits until `pattern` occurs at least `count` times in an alias's log.
wait_for() {
  local log=$logs/$1.log pattern=$2 count=$3 deadline=$((SECONDS + TIMEOUT))
  until [ "$(grep -c -- "$pattern" "$log" 2>/dev/null || true)" -ge "$count" ]; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "timed out waiting for $count lines matching '$pattern' in $1" >&2
      return 1
    fi
    sleep 1
  done
}

# Consumes `count` records of a Kafka topic from its beginning.
expect_kafka() {
  local topic=$1 received
  received=$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:29092 --topic "$topic" --from-beginning \
    --max-messages "$COUNT" --timeout-ms $((TIMEOUT * 1000)) 2>/dev/null | grep -c . || true)
  echo "$received records on Kafka $topic"
  [ "$received" -ge "$COUNT" ]
}

# Consumes `count` messages of a Pulsar topic from its earliest position.
expect_pulsar() {
  local topic=$1
  timeout "$TIMEOUT" docker compose exec -T pulsar bin/pulsar-client consume \
    --subscription-name ci-check --subscription-position Earliest \
    --num-messages "$COUNT" "persistent://public/default/$topic" >"$logs/check.log" 2>&1
  echo "$(grep -c "got message" "$logs/check.log") messages on Pulsar $topic"
}

case "${1:-}" in
  kafka-orders)
    build kafka_orders kafka
    cargo kafka-produce "$COUNT"
    start kafka-process
    expect_kafka order-events
    ;;
  pulsar-orders)
    build pulsar_orders pulsar
    # The compose pulsar-init service creates this subscription at the
    # earliest position; it also needs Pulsar Manager, which CI does not start.
    docker compose exec -T pulsar bin/pulsar-admin topics create-subscription \
      --subscription beavers-examples --messageId earliest \
      persistent://public/default/orders
    cargo pulsar-produce "$COUNT"
    start pulsar-process
    expect_pulsar order-events
    ;;
  kafka-to-pulsar)
    build kafka_orders kafka
    build kafka_to_pulsar kafka,pulsar
    cargo kafka-produce "$COUNT"
    start kafka-to-pulsar
    expect_pulsar orders-from-kafka
    ;;
  kafka-to-http)
    build kafka_orders kafka
    build kafka_to_http kafka,http
    start http-receive
    wait_for http-receive "listening on" 1
    cargo kafka-produce "$COUNT"
    start kafka-to-http
    wait_for http-receive "^received " "$COUNT"
    echo "$COUNT orders received over HTTP"
    ;;
  transform)
    output=$(cargo run --locked --quiet --example transform)
    echo "$output"
    [ "$output" = $'{"order_id":1}\n{"order_id":2}' ]
    output=$(printf '{"id":3}\n' | cargo run --locked --quiet --example transform -- --stdin)
    echo "$output"
    [ "$output" = '{"order_id":3}' ]
    ;;
  *)
    echo "usage: $0 <kafka-orders|pulsar-orders|kafka-to-pulsar|kafka-to-http|transform>" >&2
    exit 2
    ;;
esac
