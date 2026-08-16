# Measurements

`cargo run -p kestrel-bench --release`, 2026-08-16. Reproduce before believing; the harness prints
its own parameters and asserts that every run handled every record before dividing.

**Machine:** AMD Ryzen Threadripper PRO 9975WX (32 cores / 64 threads), Linux 7.1.8.
**Cluster:** three `apache/kafka:3.9.0` brokers in KRaft mode over loopback — `./cluster.sh up`.
Still one machine and still no network, but partition leadership is spread over three brokers,
which is what the single-broker rig could not do.
**Common to every cell:** 8 partitions, replication factor 1, `acks=all`, idempotent producer, no
compression.
**Ranges are across 3–5 runs. Every ratio uses `rdkafka`'s *best* observed number**, so the
comparison errs against us.

## Produce throughput

| batch | record | kestrel rec/s | rdkafka rec/s | ratio |
|---:|---:|---:|---:|---:|
| 1 | 128 B | 7 000 – 24 500 | 9 780 – 9 870 | 0.7× – 2.5× |
| 10 | 128 B | 69 700 – 153 000 | 38 700 – 39 000 | 1.8× – 3.9× |
| 100 | 128 B | 272 000 – 1 126 000 | 181 000 – 185 000 | 1.5× – 6.2× |
| 1 000 | 128 B | 3 861 000 – 4 347 000 | 319 000 – 345 000 | 11.3× – 13.6× |
| 1 000 | 1 KiB | 1 315 000 – 1 936 000 | 325 000 – 334 000 | 4.0× – 5.9× |
| 1 000 | 8 KiB | 216 000 – 261 000 | 86 300 – 88 700 | 2.4× – 3.0× |

The small-batch rows have very wide spreads and the first run of a process is always its slowest;
those cells are dominated by warm-up, not steady state. **The batch-1 row is sometimes a loss** —
a single record per call is where librdkafka's accumulator earns its keep and where an API that
sends when told to has nothing to offer.

## Produce latency, one record at a time

| | kestrel | rdkafka |
|---|---:|---:|
| p50 | 0.046 – 0.052 ms | 0.052 – 0.064 ms |
| p99 | 0.102 – 0.127 ms | 0.107 – 0.136 ms |
| max | 1.5 – 2.1 ms | **504 – 506 ms** |

**p99 is a tie.** An earlier revision of this file claimed it outright on three runs; wider sampling
does not support that, and it is left recorded here rather than quietly deleted.

The maximum is the interesting column. A half-second worst case in librdkafka is reproducible in
every run; ours is around 2 ms. For anything user-facing the tail is the number that matters, and
that is a three-orders-of-magnitude difference.

## Consume throughput

| records consumed | kestrel rec/s | rdkafka rec/s |
|---:|---:|---:|
| 200 000 | 1 585 000 | 128 000 |
| 1 000 000 | 3 005 000 | 132 000 |
| 2 000 000 | 3 200 000 | 145 000 |

**The 200 000-record cell was not measuring throughput** — it was measuring the fixed cost at either
end of a fetch loop, chiefly the last poll of each broker waiting out `max_wait` with nothing left
to return. Consuming five times as much data nearly doubles the rate, and ten times as much barely
moves it again: steady state on this cluster is roughly **3.2 M records/s**, and the old figure
understated it by half. The default cell is now 1 M records; `KESTREL_CONSUME_RECORDS` overrides it,
and varying it is the check that a rate is steady state rather than overhead.

Measured with **one consumer holding all eight partitions**, so a poll is one `Fetch` per broker
rather than one per partition. That connection saving is what makes a per-core client affordable to
a broker.

Part of the gap to librdkafka is an API difference and should be read as such: `kestrel` returns a
whole batch per `fetch()`, `rdkafka`'s `StreamConsumer` returns one message per `await`. That
per-message wakeup is a real cost of that API, not evidence about librdkafka's decoder.

## Many cores

| cores | rec/s | per core | scaling |
|---:|---:|---:|---:|
| 1 | 3.27 – 3.39 M | 3.3 M | 1.00× |
| 2 | 5.97 – 6.23 M | 3.1 M | 1.80 – 1.90× |
| 4 | 8.04 – 8.36 M | 2.1 M | 2.42 – 2.56× |
| 8 | 7.57 – 7.92 M | 1.0 M | 2.28 – 2.33× |

Against a single broker this cell swung twentyfold between identical runs and was not usable. On
three brokers it repeats to within about 10% — with one exception: **one run in three or four
collapses at 8 cores** (a 1.34 M outlier against a 7.6–7.9 M norm). That is not yet explained, and
the row should be read as "8 cores does not beat 4 here", not as a number.

**Scaling is real but sublinear, and it stops at four cores.** Doubling to 2 gives 1.8×; 4 gives
2.4×; 8 gives nothing further. Three brokers on one machine over loopback is still a shared
bottleneck — one page cache, one disk, one kernel — so the plateau is at least as likely to be the
cluster as the client. What can be said is that the first doubling is nearly free, and that the
design's claim of N non-coordinating cores has not been contradicted; it has also not been shown
past four.

An earlier version of this cell was worse than noisy, it was **wrong**: each core wrote to only the
first partition it owned, so the 1-core baseline drove a single log while the 8-core run drove
eight, and part of what it reported as client scaling was the broker's partition parallelism. Every
core count now writes to all eight partitions.

## What concurrent per-broker requests changed

Requests to different brokers used to be issued one after another. Against a single broker that is
invisible — there is only ever one request — and it cost most of a factor of two as soon as there
were three:

| cell | sequential | concurrent |
|---|---:|---:|
| produce, batch 1 000, 128 B | 2.15 M rec/s | 3.86 – 4.35 M rec/s |
| produce, batch 1 000, 1 KiB | 1.16 M rec/s | 1.32 – 1.94 M rec/s |
| consume, 200 k records | 1.54 M rec/s | 1.58 M rec/s |

**Consume did not improve, and the reason given for expecting it to was wrong.** The consume cell at
200 k records is dominated by fixed cost, not by round trips, so there was nothing there for
concurrency to remove — which is what the record-count sweep above found.

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

- **No real network and no replication.** Three brokers on one machine share a page cache and a
  disk; RF is 1. Loopback removes the variable that usually dominates, and a cluster on separate
  hosts could move every row here.
- **The 8-core collapse is unexplained.** Roughly one run in three or four.
- **No consume latency**, only throughput.
- **Fetch sessions are implemented** (KIP-227, incremental fetch, on by default) but their benefit is
  **not measured**: it shows up when most partitions are idle, and every cell here keeps every
  partition busy. A cell with mostly-idle partitions is the missing measurement.
- **No compression cells**, though all four codecs round-trip correctly in the test suite.
- **No sustained run.** These are seconds-long bursts; nothing here says what happens after an hour,
  or how memory behaves under backpressure.
- **In-flight is still one request per connection.** Concurrency across brokers is done; pipelining
  several requests onto one connection is not, and `kestrel_core::Connection` already correlates
  them, so it is a change in `Broker::call`, not in the core.

## Why produce is fast

- **One `Produce` per broker, not per partition** — eight partitions on one broker travel in one
  request — and **every broker's request is in flight at once**.
- **Batches are encoded once** and re-sent byte-identically on retry, so a retry costs a write, not
  a re-encode.
- **Zero-copy reads**: record values are `Bytes` slices into the fetch buffer.
- No C library and no thread hop; on glommio every request is io_uring on the core that owns the
  partition.
