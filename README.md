# kestrel

A native Rust Kafka client with a sans-io core, so it runs on any async runtime —
including thread-per-core ones, which is what it was built for.

**Working name, and early.** Nothing here is published, the API will change, and only the
assign-only consumer exists so far. Design and sequencing:
`../docs/superpowers/specs/2026-08-15-native-kafka-client-design.md`.

## Why

`rdkafka` wraps librdkafka, which owns its own OS threads and a poll-based C API. It works, but it
will never cooperate with io_uring: every call from a thread-per-core runtime is a hop onto threads
the runtime did not schedule. `rskafka` is native but documents no transactions and no offset
tracking. So the gap in the ecosystem is a **native transactional producer**, and that is the piece
worth building.

## Shape

```
kafka-protocol          generated from Kafka's JSON schemas — the wire codec, not ours
      ↓
kestrel-core            sans-io: framing, correlation, filtering. No sockets, no clock, no Send.
      ↓
kestrel-glommio         sockets and timers. !Send handle, one connection per broker.
kestrel-tokio           (P3)
```

The core names no runtime, so there is nothing to abstract over and no `Send` bound forced by a
lowest-common-denominator trait. The *binding* decides `Send`-ness, which is how one state machine
serves both a per-core handle and a work-stealing one.

The second reason for sans-io is testing, and for a Kafka client it is the bigger one: exactly-once
bugs are silent — a wrong retry duplicates records while every status code stays green — so they
have to be caught by driving the state machine adversarially rather than by watching a broker
behave. Every `kestrel-core` test does that, with no broker and no executor.

## Status

| | |
|---|---|
| **Consumer, assign-only** | works — `ApiVersions`, `Metadata`, `ListOffsets`, `Fetch`, seek, READ_COMMITTED filtering |
| **Producer** | idempotent and transactional — sequencing, coordinator routing, zombie fencing. **Partitioning is the caller's**: `send(topic, partition, ..)` takes an explicit partition, there is no default partitioner yet |
| **Leader routing, metadata cache** | works — fetches go to the leader from the cluster map, `NOT_LEADER_OR_FOLLOWER` invalidates that partition and retries |
| **TLS, SASL** | not started (P3). The `futures-io` seam means `futures-rustls` drops in |
| **Consumer groups** | **out of scope** — callers assign partitions themselves |

## Tests

```sh
cargo test                                        # 52 unit tests, no broker, no runtime
cargo test -p kestrel-glommio -- --ignored --test-threads=1   # needs a broker
```

The broker tests need a single-node Kafka; the invocation is at the top of
`kestrel-glommio/tests/real_broker.rs`. Note that `__transaction_state` defaults to replication
factor 3, so a single-node broker needs it overridden or every transaction fails with error 15.

## What using a real broker taught us

These are in the code as comments and tests, and are the reason the filtering rules look the way
they do:

- **READ_COMMITTED is client-side.** The broker does not withhold aborted records under
  `isolation_level = 1`. It returns them with an aborted-transactions list and a last-stable-offset,
  and the consumer filters. Skip it and aborted data is reported as committed, with no error.
- **The abort marker closes the range.** One producer can interleave an aborted and a committed
  transaction in a single fetch, so "drop everything from an aborted producer" silently discards
  committed records.
- **Batches come back whole.** A fetch from the middle of a batch returns the records before it too;
  dropping them is the client's job, or a restore re-delivers data the caller has already emitted.
- **A fully-filtered fetch must still advance.** Otherwise a partition of only-aborted records
  stalls forever while looking perfectly healthy.
- **Sequence numbers continue across transactions.** Restarting them makes the broker dedupe: `Ok`,
  the original base offset echoed back, nothing written, transaction committed empty.
- **Invalidate per partition, not wholesale.** One moved partition does not make the rest of the
  map wrong, and throwing it all away turns a single failover into a reconnect storm.
- **Error codes are states, not failures.** A cold cluster answers 15, then 14, then 16 — the last
  *after* a successful `FindCoordinator`. `CONCURRENT_TRANSACTIONS` (51) appears whenever a
  transaction starts right after one ends. See `ErrorCode::disposition`.
