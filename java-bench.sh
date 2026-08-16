#!/usr/bin/env bash
# The Apache Kafka **Java** client, measured with Apache's own perf tools.
#
# The Java client is the reference implementation and its record accumulator is
# the thing this client does not have, so it is the comparison that matters most
# — and `kafka-producer-perf-test` is the number anyone would quote back at us,
# which is why this uses it rather than benchmark code of my own.
#
# Runs inside a container on the cluster's network (`./cluster.sh up` first), so
# it reaches the brokers by their internal listeners and pays no more for the
# network than `kestrel-bench` does over loopback.
#
#   ./java-bench.sh            against the Kafka cluster
#   ./java-bench.sh redpanda   against the Redpanda cluster
#   ./java-bench.sh kafka cores    N concurrent producers, aggregate rate
#
# **Fairness**: the Java producer is given its best configuration, not its
# defaults — `linger.ms=0`, a large `batch.size`, five in flight — for the same
# reason `kestrel-bench` gives librdkafka its best. PERF.md records what
# happened the one time that was not done. It is also given far more records
# than our own cells use, so the JIT is warm before the number is taken; a cold
# JVM measures the JVM, not the client.
set -euo pipefail

IMAGE="${KAFKA_IMAGE:-docker.io/apache/kafka:3.9.0}"
NET=kestrel-net
FLAVOUR="${1:-kafka}"
PARTITIONS=8

case "$FLAVOUR" in
  kafka)    BOOTSTRAP="kafka1:9092,kafka2:9092,kafka3:9092" ;;
  redpanda) BOOTSTRAP="redpanda1:9092,redpanda2:9092,redpanda3:9092" ;;
  *) echo "usage: $0 [kafka|redpanda]" >&2; exit 2 ;;
esac

run() { podman run --rm --network "$NET" "$IMAGE" "$@"; }

topic_for() {
  local name="java-bench-$1-$$"
  run /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --topic "$name" --partitions "$PARTITIONS" --replication-factor 1 \
    >/dev/null 2>&1 || true
  echo "$name"
}

# One producer cell. `batch` is `batch.size` in bytes; 0 disables batching
# entirely, which is the Java equivalent of sending one record at a time.
produce_cell() {
  local label="$1" records="$2" size="$3" batch="$4"
  local topic
  topic=$(topic_for "p$size-$batch")
  echo -n "$label"
  run /opt/kafka/bin/kafka-producer-perf-test.sh \
    --topic "$topic" \
    --num-records "$records" \
    --record-size "$size" \
    --throughput -1 \
    --producer-props \
      bootstrap.servers="$BOOTSTRAP" \
      acks=all \
      enable.idempotence=true \
      linger.ms=0 \
      batch.size="$batch" \
      max.in.flight.requests.per.connection=5 \
      compression.type=none \
    2>/dev/null | tail -1
}

consume_cell() {
  local records="$1" size="$2"
  local topic
  topic=$(topic_for "c$size")
  # Fill it first, with the Java producer.
  run /opt/kafka/bin/kafka-producer-perf-test.sh \
    --topic "$topic" --num-records "$records" --record-size "$size" --throughput -1 \
    --producer-props bootstrap.servers="$BOOTSTRAP" acks=all linger.ms=0 \
      batch.size=1048576 compression.type=none \
    >/dev/null 2>&1

  echo -n "consume $size B          "
  # Header first, then the data row: nMsg.sec is what we want.
  run /opt/kafka/bin/kafka-consumer-perf-test.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$topic" \
    --messages "$records" \
    --group "java-bench-$$" \
    --timeout 60000 \
    2>/dev/null | tail -1
}

# N concurrent producers against one topic, rates summed.
#
# The counterpart to `kestrel-bench`'s many-core cell. One producer per process
# is what `kafka-producer-perf-test` gives, which is also what N application
# instances would look like; each JVM has its own sender thread, and the rate
# each reports covers only its send loop, so JVM startup is not counted.
cores_cell() {
  local topic
  topic=$(topic_for "cores")
  echo "producers   aggregate rec/s"
  for n in 1 2 4 8; do
    local per=$((2000000 / n))
    local pids=() out
    out=$(mktemp -d)
    for i in $(seq 1 "$n"); do
      (
        run /opt/kafka/bin/kafka-producer-perf-test.sh \
          --topic "$topic" \
          --num-records "$per" \
          --record-size 128 \
          --throughput -1 \
          --producer-props \
            bootstrap.servers="$BOOTSTRAP" \
            acks=all \
            enable.idempotence=true \
            linger.ms=0 \
            batch.size=131072 \
            max.in.flight.requests.per.connection=5 \
            compression.type=none \
          2>/dev/null | tail -1 > "$out/$i"
      ) &
      pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid"; done
    # Sum the per-process rates. They ran concurrently, so the sum is the
    # aggregate the cluster actually saw.
    awk -v n="$n" '{ for (i = 1; i <= NF; i++) if ($i == "records/sec") total += $(i-1) }
         END { printf "%9d   %15.0f\n", n, total }' "$out"/*
    rm -rf "$out"
  done
}

if [ "${2:-}" = "cores" ]; then
  echo "Java client, N concurrent producers against $FLAVOUR"
  cores_cell
  exit 0
fi

echo "Java client (kafka-producer-perf-test) against $FLAVOUR"
echo "records sent, rate, MB/s, avg latency, max latency, 50th, 95th, 99th, 99.9th"
echo

produce_cell "no batching,  128 B   " 500000  128  0
produce_cell "batched,      128 B   " 2000000 128  131072
produce_cell "batched,      1 KiB   " 500000  1024 1048576
produce_cell "batched,      8 KiB   " 100000  8192 1048576

echo
consume_cell 1000000 128
