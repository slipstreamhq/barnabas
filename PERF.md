# Measurements

Numbers from `cargo run -p kestrel-bench --release`, 2026-08-16. Reproduce them before believing
them; the harness is 350 lines and prints its own parameters.

**Machine:** AMD Ryzen Threadripper PRO 9975WX (32 cores / 64 threads), Linux 7.1.8, one local
`apache/kafka:3.9.0` broker over loopback.
**Workload:** 200 000 records × 256 B, batches of 1 000, 8 partitions, `acks=all`, idempotent
producer, no compression.

| | records/s | MiB/s |
|---|---:|---:|
| kestrel produce (glommio) | 2 190 000 – 2 350 000 | 536 – 573 |
| kestrel produce (tokio) | 2 170 000 – 2 280 000 | 529 – 556 |
| rdkafka produce (tokio) | 341 000 – 352 000 | 83 – 86 |
| kestrel consume (glommio) | 5 330 000 – 5 650 000 | 1 301 – 1 379 |
| rdkafka consume (tokio) | 311 000 – 349 000 | 76 – 85 |

Three consecutive runs; the ranges are the spread. One rdkafka consume run came in at 124 000
rec/s — a 3× outlier with no obvious cause, which is itself worth knowing.

## How the comparison was made fair

**librdkafka is configured for its best case, not its defaults.** With defaults it produced
116 000 rec/s, and the first draft of this file would have claimed 16×. That number was its 5 ms
linger interacting with the harness awaiting each batch, not its speed. It now runs with
`linger.ms=0`, a 1 MiB batch size, 10 000-message batches and a large output queue, **and every
record is enqueued before anything is awaited**, so its accumulator chooses its own batch
boundaries rather than being flushed by us. That tripled it.

If there is a further configuration that closes the gap, this file is wrong and should be corrected rather
than defended.

## What these numbers do not say

- **The consume comparison is partly an API difference.** `kestrel`'s consumer returns a whole
  batch per `fetch()`; `rdkafka`'s `StreamConsumer` returns one message per `await`, and that
  per-message wakeup is a real cost of that API rather than of librdkafka's decoding. Read the
  consume row as "batch-at-a-time beats message-at-a-time", which is a design claim, not a
  decoder claim.
- **One process, one broker, loopback.** No network, no replication beyond one node, no
  contention. This excludes the case a per-core client is built for — many cores each owning
  partitions — so it is a floor rather than a ceiling.
- **No latency numbers at all.** Throughput with `acks=all` at batch 1 000 says nothing about
  p99 for a single record, which is the number a request/response service cares about.
- **The consumer is still one connection and one `Fetch` per partition.** Multi-partition fetch
  and fetch sessions (KIP-227) are unbuilt, so the consume figure should improve — and the
  connection count should fall — when they land.

## Why the produce numbers are what they are

Both clients do the same protocol work; the difference is round trips and copies.

- **One `Produce` per broker, not per partition.** Eight partitions on one broker travel in one
  request. This landed in `perf(kestrel): one request per broker, not per partition` and is the
  single largest structural difference.
- **Batches are encoded once** and re-sent byte-identically on retry, so a retry costs a write,
  not a re-encode.
- **Zero-copy on the read path**: record values are `Bytes` slices into the fetch buffer rather
  than per-record allocations.
- No C library, no thread hop: on the glommio arm every request is io_uring on the core that owns
  the partition.
