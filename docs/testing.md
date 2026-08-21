# Testing

[← barnabas](../README.md)

```sh
cargo test                                        # 151 tests, no broker, no runtime
cargo test -p barnabas-glommio -- --ignored --test-threads=1   # needs a broker
cargo test -p barnabas-tokio   -- --ignored --test-threads=1   # the same suite, other runtime
```

The broker tests need Kafka; the invocation is at the top of
`barnabas-glommio/tests/real_broker.rs`, and `./cluster.sh up` (repo root) brings up a suitable one. Note that
`__transaction_state` defaults to replication factor 3, so a single-node broker needs it overridden
or every transaction fails with error 15.

The broker suite is in four files: `real_broker.rs` (consuming, seeking, filtering, pausing,
offsets), `real_broker_producer.rs` (idempotence, transactions, EOS with groups, the accumulator),
`real_broker_group.rs` (membership, rebalancing, commit and resume) and `real_broker_admin.rs`
(topics, configs, expansion).

`./cluster.sh up` (repo root) brings up three Apache Kafka brokers (KRaft, no ZooKeeper);
`./cluster.sh up redpanda` brings up three Redpanda brokers on the same ports,
so `KAFKA_BOOTSTRAP` does not change. `./cluster.sh down` removes either.

**Redpanda is worth running.** It is an independent implementation of the same
wire protocol, so it catches assumptions about *Apache Kafka's behaviour* that a
second Kafka never would. It found one immediately: this client performed the
`ApiVersions` handshake at connect and then ignored the answer, sending every
request at a hardcoded version. That works against Kafka, whose versions those
are. Redpanda answers a version it does not know by **closing the connection**,
so the symptom was an unexplained EOF at connect rather than an error code.

Requests now go out at the newest version the broker admits to supporting,
clamped to what this client knows how to read. The full suite passes against
both.
