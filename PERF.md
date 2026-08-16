# Measurements

`cargo run -p kestrel-bench --release`, 2026-08-16. Reproduce before believing; the harness prints
its own parameters and asserts that every run handled every record before dividing.

**Machine:** AMD Ryzen Threadripper PRO 9975WX (32 cores / 64 threads), Linux 7.1.8, one local
`apache/kafka:3.9.0` broker over loopback.
**Common to every cell:** 8 partitions, `acks=all`, idempotent producer, no compression.
**Ranges are across 3–5 runs. Every ratio uses `rdkafka`'s *best* observed number**, so the
comparison errs against us.

## Produce throughput

| batch | record | kestrel rec/s | rdkafka rec/s | ratio |
|---:|---:|---:|---:|---:|
| 1 | 128 B | 10 400 – 32 900 | 9 830 – 9 860 | 1.1× – 3.3× |
| 10 | 128 B | 98 700 – 156 000 | 39 100 – 39 200 | 2.5× – 4.0× |
| 100 | 128 B | 968 000 – 1 251 000 | 176 000 – 183 000 | 5.3× – 6.8× |
| 1 000 | 128 B | 3 343 000 – 3 806 000 | 313 000 – 338 000 | 9.9× – 11.3× |
| 1 000 | 1 KiB | 1 056 000 – 1 150 000 | 314 000 – 329 000 | 3.2× – 3.5× |
| 1 000 | 8 KiB | 168 000 – 245 000 | 62 000 – 81 000 | 2.1× – 3.0× |

The batch-1 row has the widest spread and the first run of a process is always its slowest — that
cell is dominated by warm-up, not by steady state.

## Produce latency, one record at a time

| | kestrel | rdkafka |
|---|---:|---:|
| p50 | 0.041 – 0.045 ms | 0.051 – 0.053 ms |
| p99 | 0.051 – 0.113 ms | 0.087 – 0.098 ms |
| max | 1.7 – 2.0 ms | **502 – 504 ms** |

**p99 is a tie, not a win.** Across runs ours lands anywhere from 0.051 to 0.113 ms against
librdkafka's steadier 0.087–0.098, so some runs are faster and some slower. Earlier revisions of
this file claimed p99 outright; that was three runs' luck, and the wider sample does not support
it.

The maximum is the interesting column. A half-second worst case in librdkafka is reproducible
across every run; ours is under 2 ms. For anything user-facing, the tail is the number that
matters, and this is a three-orders-of-magnitude difference in it.

## Consume throughput

| record | kestrel rec/s | rdkafka rec/s | ratio |
|---:|---:|---:|---:|
| 128 B | 7 589 000 – 8 065 000 | 123 000 – 327 000 | **~24×** (against its best) |

Measured with **one consumer holding all eight partitions** — one connection, one `Fetch` per
poll. The per-partition shape it replaced managed 5.9–7.1 M rec/s over eight connections, so
multi-partition fetch bought roughly 13% and cut the connection count by 8×. The connection saving
is the more important half: it is what makes a per-core client affordable to a broker.

**`rdkafka`'s consumer is bimodal here** — roughly 125k or roughly 330k, with no configuration
change between runs. The ratio above uses the fast mode; against the slow mode it would read 55×,
which is why the range is printed rather than a single figure.

Part of this gap is an API difference and should be read as such: `kestrel` returns a whole batch
per `fetch()`, `rdkafka`'s `StreamConsumer` returns one message per `await`. That per-message
wakeup is a real cost of that API, not evidence about librdkafka's decoder.

## Many cores: not measurable on this setup

The design's central claim is that N cores each own partitions and never coordinate. The harness
has a cell for it — N glommio executors pinned to N cores, each with its own client and partitions
— and **its output is not usable**:

| cores | run 1 | run 2 | run 3 |
|---:|---:|---:|---:|
| 1 | 3.89 M | 3.83 M | 4.30 M |
| 2 | 6.76 M | 7.08 M | 0.73 M |
| 4 | 7.98 M | 0.38 M | 0.50 M |
| 8 | 8.46 M | 0.50 M | 8.09 M |

A twenty-fold swing between runs of the same configuration is not a client measurement. One broker
on one node, eight partitions on one disk, everything over loopback: past roughly 4 M records/s the
broker is the constraint and its behaviour under that pressure dominates everything the client
does.

Single-core numbers are stable (3.8–4.3 M) and the 1→2 step is repeatable at ~1.8×. Beyond that,
**this setup cannot answer the question**, and publishing a scaling curve from it would be
inventing one. Answering it properly needs a multi-broker cluster on real hardware, which is
unbuilt.

## Two fairness mistakes, both mine, both corrected

Recorded because a benchmark's credibility is mostly its author's willingness to write these down.

1. **librdkafka's producer ran on defaults.** Its 5 ms linger interacted with the harness awaiting
   each batch and produced 116 000 rec/s — a number that would have supported a "16×" headline.
   Configured properly (`linger.ms=0`, 1 MiB batches, large queue) and with every record enqueued
   before anything is awaited, it tripled to ~340 000.
2. **Then I "tuned" its consumer and made it worse.** Setting `fetch.min.bytes=1 MiB` and
   `fetch.wait.max.ms=100` cut it from ~330k to ~125k — a 2.6× handicap I had introduced while
   trying to help it. It now runs on librdkafka's own defaults.

If a further configuration closes any gap here, this file is wrong and should be corrected rather
than defended.

## What is still unmeasured

- **No multi-broker cluster**, no replication beyond one node, no real network. Loopback removes
  the variable that usually dominates.
- **No many-core run.** Everything above is one process; the case a per-core client is built for —
  each core owning partitions — is not represented at all.
- **No consume latency**, only throughput.
- **Fetch sessions are implemented** (KIP-227, incremental fetch, on by default) but their benefit
  is **not measured**: it shows up when most partitions are idle, and every cell here keeps every
  partition busy. A cell with mostly-idle partitions is the missing measurement.
- **No compression cells**, though all four codecs round-trip correctly in the test suite.
- **No sustained run.** These are seconds-long bursts; nothing here says what happens after an hour,
  or how memory behaves under backpressure.

## Why produce is fast

- **One `Produce` per broker, not per partition** — eight partitions on one broker travel in one
  request.
- **Batches are encoded once** and re-sent byte-identically on retry, so a retry costs a write, not
  a re-encode.
- **Zero-copy reads**: record values are `Bytes` slices into the fetch buffer.
- No C library and no thread hop; on glommio every request is io_uring on the core that owns the
  partition.
