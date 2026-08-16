#!/usr/bin/env bash
# A three-broker Kafka cluster on podman, for the measurements a single broker
# cannot answer.
#
# The many-core cell in PERF.md is unusable against one broker: past roughly
# 4M records/s the broker is the constraint, and what the numbers describe is
# its behaviour under pressure rather than the client's. Three brokers do not
# make this a real deployment — still one machine, still loopback — but they do
# spread partition leadership, which is the specific thing that was missing.
#
#   ./cluster.sh up      3 brokers, KRaft, no ZooKeeper
#   ./cluster.sh down    remove them and the network
#   ./cluster.sh ports   print the bootstrap string
#
# Replication factor stays 1 for topics the bench creates (see kestrel-bench):
# the question is whether the client scales across cores, and RF=3 would add
# replication cost to every cell and answer a different question.
set -euo pipefail

IMAGE="${KAFKA_IMAGE:-docker.io/apache/kafka:3.9.0}"
NET=kestrel-net
# Fixed, so a rerun is reproducible and a stale volume is obviously stale.
CLUSTER_ID="${KAFKA_CLUSTER_ID:-5L6g3nShT-eMCtK--X86sw}"
NODES=3

# Host port per broker. The advertised listener must carry the *host* port, not
# the container's, or a client connects to the bootstrap and is then handed an
# address it cannot reach.
host_port() { echo $((9092 + ($1 - 1) * 100)); }

voters() {
  local out=""
  for i in $(seq 1 $NODES); do
    [ -n "$out" ] && out="$out,"
    out="$out$i@kafka$i:9093"
  done
  echo "$out"
}

up() {
  podman network exists "$NET" || podman network create "$NET" >/dev/null
  for i in $(seq 1 $NODES); do
    local port
    port=$(host_port "$i")
    podman rm -f "kafka$i" >/dev/null 2>&1 || true
    podman run -d --name "kafka$i" --network "$NET" \
      -p "$port:19092" \
      -e KAFKA_NODE_ID="$i" \
      -e KAFKA_PROCESS_ROLES=broker,controller \
      -e KAFKA_CONTROLLER_QUORUM_VOTERS="$(voters)" \
      -e KAFKA_LISTENERS="INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:19092" \
      -e KAFKA_ADVERTISED_LISTENERS="INTERNAL://kafka$i:9092,EXTERNAL://127.0.0.1:$port" \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT" \
      -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
      -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      -e KAFKA_CLUSTER_ID="$CLUSTER_ID" \
      -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 \
      -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3 \
      -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2 \
      -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
      -e KAFKA_NUM_PARTITIONS=8 \
      "$IMAGE" >/dev/null
    echo "kafka$i on 127.0.0.1:$port"
  done

  # Wait for **all three** brokers to be registered, not just for the first one
  # to answer. A cluster where only one broker is up answers Metadata perfectly
  # well — with every partition led by that broker — so a test or a benchmark
  # started too early measures a single-broker cluster and a transaction
  # coordinator that does not yet know the topics it is being asked about.
  echo -n "waiting for quorum"
  for _ in $(seq 1 90); do
    if [ "$(podman exec kafka1 /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server kafka1:9092 2>/dev/null | grep -c 'id:')" = "$NODES" ]; then
      echo " ok"
      ports
      return 0
    fi
    echo -n .
    sleep 1
  done
  echo " timed out"
  podman logs --tail 40 kafka1
  return 1
}

down() {
  for i in $(seq 1 $NODES); do podman rm -f "kafka$i" >/dev/null 2>&1 || true; done
  podman network rm "$NET" >/dev/null 2>&1 || true
  echo "removed"
}

ports() {
  local out=""
  for i in $(seq 1 $NODES); do
    [ -n "$out" ] && out="$out,"
    out="${out}127.0.0.1:$(host_port "$i")"
  done
  echo "KAFKA_BOOTSTRAP=$out"
}

case "${1:-up}" in
  up) up ;;
  down) down ;;
  ports) ports ;;
  *) echo "usage: $0 {up|down|ports}" >&2; exit 2 ;;
esac
