# Measurements

`cargo run -p kestrel-bench --release`, 2026-08-16. Reproduce before believing; the harness prints
its own parameters and asserts that every run handled every record before dividing.
`KESTREL_CELLS=cores` (or `produce`, `latency`, `consume`, `compression`, `e2e`, `idle`, `pipeline`) runs one
section, because re-measuring one cell should not cost the others.

**Machine:** AMD Ryzen Threadripper PRO 9975WX (32 cores / 64 threads), Linux 7.1.8.
**Cluster:** three `apache/kafka:3.9.0` brokers in KRaft mode over loopback — `./cluster.sh up`.
`./cluster.sh up redpanda` runs the same cells against three Redpanda brokers instead; see below.
Still one machine and still no network, but partition leadership is spread across three brokers.
**Common to every cell:** 8 partitions, replication factor 1, `acks=all`, idempotent producer, no
compression unless the cell says otherwise.
**Ranges are across 2–6 runs. Every ratio uses `rdkafka`'s *best* observed number**, so the
comparison errs against us.

## Many cores — the claim the design rests on

| cores | rec/s | per core |
|---:|---:|---:|
| 1 | 3.69 – 4.01 M | 3.7 – 4.0 M |
| 2 | 3.80 – 9.53 M | 1.9 – 4.8 M |
| 4 | 7.32 – 17.96 M | 1.8 – 4.5 M |
| 8 | **25.0 – 33.1 M** | 3.1 – 4.1 M |

**Per-core throughput is flat from one core to eight** — about 3.1–4.1 M records/s each, with
eight cores reaching 25–33 M records/s in aggregate. Six consecutive runs, no collapses. That is
what "N cores that own partitions and never coordinate" is supposed to look like, and it is the
first version of this cell that shows it.

The 2- and 4-core rows still have occasional dips (one run in six landed at 3.80 M on 2 cores, two
at ~7–8 M on 4). They are roughly 2× dips, not the 20× swings this cell used to produce, and the
8-core row did not dip at all in six runs.

### Three bugs this cell had, all mine

It took three corrections before this row meant anything, and the first two flattered or destroyed
the result for reasons that had nothing to do with the client:

1. **Each core wrote to only the first partition it owned.** The 1-core baseline drove a single log
   while the 8-core run drove eight, so part of the reported "scaling" was the broker's partition
   parallelism. Every core count now writes to all eight partitions.
2. **Producer setup was inside the timed region.** The clock started before the threads spawned, so
   N concurrent `InitProducerId` round trips counted as throughput. Cores now connect, wait on a
   barrier, and the clock starts when all are ready. This roughly doubled every number.
3. **A flat 250 ms first retry** — see below. This was the "8-core collapse".

## The retry backoff that was the collapse

Earlier revisions of this file recorded an unexplained 8-core collapse: two runs in three fell to
~1.35 M rec/s against a ~28 M norm. It was not the broker and not contention.

`Producer::backoff(attempt)` took the attempt number **and ignored it**, sleeping a flat 250 ms.
A single retriable answer — a partition mid-election, a broker briefly behind — cost a quarter
second. Against a 400 000-record run that is 0.27 s of pure sleep, which is exactly the 6× drop
observed. The same 250 ms shows up in the old produce-latency `max` column as a 253 ms outlier.

It is now exponential from 5 ms, capped at 250 ms. The collapse is gone in six runs, and the
latency `max` column went from an occasional 253 ms to a steady ~1.1 ms.

## Produce throughput

| batch | record | kestrel rec/s | rdkafka rec/s | ratio |
|---:|---:|---:|---:|---:|
| 1 | 128 B | 20 000 – 25 300 | 9 830 – 9 870 | 2.0× – 2.6× |
| 10 | 128 B | 129 000 – 159 000 | 38 900 – 39 300 | 3.3× – 4.1× |
| 100 | 128 B | 1 059 000 – 1 299 000 | 175 000 – 186 000 | 5.9× – 7.4× |
| 1 000 | 128 B | 3 745 000 – 4 505 000 | 316 000 – 343 000 | 11.0× – 14.3× |
| 1 000 | 1 KiB | 1 415 000 – 1 946 000 | 316 000 – 333 000 | 4.4× – 5.9× |
| 1 000 | 8 KiB | 189 000 – 232 000 | 83 400 – 90 700 | 2.2× – 2.7× |

**`rdkafka`'s small-batch rows are bimodal, and the fast mode beats us.** One run put its batch-1 at
47 795 rec/s against its usual ~9 850; another put its batch-10 at 177 947 against its usual
~39 000 — faster than our 158 983 in the same run. Neither repeated, but it has now happened twice
on two different rows, so **treat the batch-1 and batch-10 ratios as unsettled**: whatever puts
librdkafka into its fast mode is not understood, and if it is the normal state on some machine then
those two rows are losses rather than 2–4× wins. The batched rows have never shown this.

The first run of a fresh cluster is also always slow for both clients — one showed us at 11 152 and
librdkafka at 4 888, roughly half their steady figures — so a cold run is not a data point.

## Pipelining: requests in flight per connection

Batch 100, 128 B records, spread over 8 partitions. The caller enqueues a window and flushes it, so
several `Produce` requests are on one connection at once.

| in flight | rec/s | vs 1 |
|---:|---:|---:|
| 1 | 594 000 – 946 000 | 1.00× |
| 2 | 1 390 000 – 1 681 000 | 1.8× – 2.3× |
| 5 | 2 410 000 – 2 992 000 | **3.1× – 4.1×** |
| 10 | 1 616 000 – 2 840 000 | 1.7× – 3.2× |

**Five is the default**, which is what the Java client allows with idempotence enabled. Ten buys
nothing here and one run of three was markedly worse, so the cap is doing its job rather than
holding anything back.

### Why a window needs a limit

The broker processes one connection's requests in order, so a partition's batches stay in sequence
even with several outstanding. The cost is recovery: if round 2 of a five-round window fails, the
broker never wrote it, so rounds 3 and 4 for that partition were rejected as out of sequence
whatever they contain. All three must be re-sent, in order. That work is proportional to the window,
which is the reason to bound it.

`flush` therefore retires **only the contiguous leading run of successes** per partition. A batch
whose own round succeeded but which sits behind a failure stays queued. Getting this wrong leaves a
gap in the log, and it returns `Ok`.

`OUT_OF_ORDER_SEQUENCE_NUMBER` is still fatal when it arrives with nothing failed before it — that
means the producer's stream is genuinely wrong and retrying is how duplicates get written. It is
treated as a consequence only when this client already knows an earlier batch for that partition
failed in the same window.

The simulator now enforces the sequence rule a real broker enforces. Before that it accepted any
sequence, so a pipelined producer that gapped or reordered its batches would have passed.

## Produce latency, one record at a time

| | kestrel | rdkafka |
|---|---:|---:|
| p50 | 0.040 – 0.047 ms | 0.046 – 0.063 ms |
| p99 | 0.087 – 0.095 ms | 0.105 – 0.131 ms |
| max | 1.00 – 1.15 ms | **503 – 504 ms** |

A half-second worst case in librdkafka is reproducible in every run; ours is around 1 ms. For
anything user-facing the tail is the number that matters.

An earlier revision claimed p99 outright on three runs, then retracted it as a tie. With the retry
backoff fixed it is a win again — recorded with that history attached, because the claim has now
been wrong in both directions once.

## End-to-end latency, produce to consume

500 samples, one record at a time, producer and consumer sharing one executor.

| | kestrel | kestrel, no prefetch | rdkafka |
|---|---:|---:|---:|
| p50 | 0.064 – 0.106 ms | 0.099 – 0.115 ms | 0.062 – 0.088 ms |
| p99 | 0.129 – 0.332 ms | 0.204 – 0.252 ms | 0.139 – 0.224 ms |
| max | 1.19 – 1.91 ms | 1.50 – 4.72 ms | 503 – 504 ms |

**The consumer now keeps a fetch permanently in flight** (`set_prefetch`, on by
default): the request for the next poll is sent as soon as the current response is decoded, so the
broker is already working while the caller processes records. Exactly one fetch per broker is
outstanding — the fetch-session epoch advances per accepted response, so a second would carry an
epoch the broker has not reached.

**It bought less than expected.** Both configurations run in the same process precisely because the
cluster moves enough between runs to swamp the difference, and even then the p50 improvement is
small and the p99 goes both ways. What can be said is that end-to-end is now **roughly parity with
librdkafka rather than a clear loss** — one run had us at 0.064 ms against its 0.062 — where the
previous revision of this file recorded a consistent loss and named the missing prefetch as the
cause. That diagnosis was right about the mechanism and wrong about the size of it.

An earlier measurement of this cell without the in-process comparison showed both clients degrading
together between runs, which is what prompted running them side by side.

## Consume throughput

Measured at 1 M records with **one consumer holding all eight partitions**, so a poll is one
`Fetch` per broker rather than one per partition.

| | kestrel rec/s | rdkafka rec/s | ratio |
|---|---:|---:|---:|
| 128 B | 2 944 000 – 3 080 000 | 132 000 – 152 000 | **~20×** (against its best) |

**The old 200 000-record cell was not measuring throughput.** It was measuring the fixed cost at
either end of a fetch loop: 200 k gives 1.58 M rec/s, 1 M gives 3.00 M, 2 M gives 3.20 M. The old
figure understated steady state by half. Varying the record count (`KESTREL_CONSUME_RECORDS`) is
the check that a rate is steady state rather than overhead.

Part of the gap to librdkafka is an API difference and should be read as such: `kestrel` returns a
whole batch per `fetch()`, `rdkafka`'s `StreamConsumer` returns one message per `await`.

## Compression

1 KiB records, batch 1 000. The payload is **half structured text and half incompressible noise** —
`payload` elsewhere in the harness is a run of one byte, which every codec shrinks to nothing and
would make compression look free.

| codec | kestrel rec/s | rdkafka rec/s | ratio |
|---|---:|---:|---:|
| none | 1 372 000 – 1 943 000 | 331 000 – 339 000 | 4.1× – 5.7× |
| gzip | 573 000 – 701 000 | 325 000 – 352 000 | 1.7× – 2.2× |
| snappy | 1 928 000 – 2 265 000 | 316 000 – 338 000 | 5.9× – 7.1× |
| lz4 | 668 000 – 695 000 | 316 000 – 333 000 | 2.0× – 2.2× |
| zstd | 2 033 000 – 2 149 000 | 319 000 – 335 000 | 6.0× – 6.6× |

Two things worth noting, neither of them flattering by default:

- **snappy and zstd beat no compression at all.** Over loopback the bytes saved outweigh the cost
  of saving them. On a real network the effect would be larger, which is an argument for turning
  compression on rather than a claim about our encoder.
- **lz4 is as slow as gzip here**, which it should not be — lz4 and snappy are similar designs and
  snappy is 3× faster. That points at `kafka-protocol`'s lz4 path rather than anything in this
  crate, and it is unexplained. **Prefer snappy or zstd** until it is.

## Idle partitions, and what fetch sessions buy

64 partitions, none of them receiving data, `max_wait` zero.

| | polls/s | vs full fetch |
|---|---:|---:|
| full fetch | 13 300 – 14 200 | 1.00× |
| incremental (KIP-227) | 15 000 – 16 500 | **1.13× – 1.19×** |

A full fetch restates all 64 partitions and their offsets every poll; an incremental one sends
almost nothing. This is the workload the feature exists for, and every other cell here keeps every
partition busy — so until now it shipped unmeasured.

**The first version of this cell could not have shown a difference.** It used `max_wait` of 1 ms,
which floors every poll at ~1.2 ms, and duly reported 839 vs 851 polls/s — a 1% "result" that was
entirely the broker's wait timer.

## Against Redpanda

Same client, same cells, three `redpandadata/redpanda:v24.2.7` brokers instead of Kafka
(`./cluster.sh up redpanda`).

| cell | Kafka | Redpanda |
|---|---:|---:|
| batch 1, 128 B | 20 000 – 25 300 | 30 600 – 33 500 |
| batch 10, 128 B | 129 000 – 159 000 | 161 000 – 176 000 |
| batch 100, 128 B | 1 059 000 – 1 299 000 | 1 148 000 – 1 207 000 |
| batch 1 000, 128 B | 3 745 000 – 4 505 000 | 3 243 000 – 3 710 000 |
| batch 1 000, 1 KiB | 1 415 000 – 1 946 000 | 1 205 000 – 1 505 000 |
| batch 1 000, 8 KiB | 189 000 – 259 000 | 128 000 – 159 000 |
| consume, 128 B | 2 944 000 – 3 080 000 | 2 643 000 – 2 893 000 |

**Small batches are faster against Redpanda, large ones slower** — the crossover is somewhere around
batch 100. Both are single-machine loopback clusters and neither is tuned, so this says more about
the two brokers' defaults under a benchmark than about either product; it is recorded because the
client is the same in both columns and the shape of the difference is consistent across runs.

`rdkafka`'s consumer is notably steadier here (~178 000 rec/s against Kafka's bimodal
132 000 – 152 000), which narrows our consume ratio to ~15–16× from ~20×.

**Redpanda found a real bug on its first run**, which is the actual reason to keep it: this client
did the `ApiVersions` handshake and then ignored the answer, sending hardcoded versions that only
Apache Kafka happens to support. Redpanda closes the connection on a version it does not know, so it
failed at connect. Versions are now negotiated per broker.

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
  disk; RF is 1. A cluster on separate hosts could move every row here.
- **The occasional 2-core and 4-core dips** are unexplained, though far milder than what the retry
  backoff was causing.
- **lz4's slowness** is unexplained.
- **No sustained run in this file.** `KESTREL_SOAK_SECONDS` produces continuously and prints per-10s
  rates and RSS, but no long run has been recorded here yet.
- **The produce-throughput table above still uses `send_keyed`**, which awaits each call and so has
  one request in flight. **A caller that does not switch to `enqueue`/`flush` sees no pipelining
  benefit at all**; the default path is exactly as fast as it was. Those rows understate what the client can do; the pipelining table is
  measured separately rather than folded in, because changing the table's shape would make it
  incomparable with every earlier revision of this file.

## Why produce is fast

- **One `Produce` per broker, not per partition** — eight partitions on one broker travel in one
  request — and **every broker's request is in flight at once**.
- **Up to five requests in flight per connection**, with the ordered-window retry that makes that
  safe for an idempotent producer.
- **Batches are encoded once** and re-sent byte-identically on retry, so a retry costs a write, not
  a re-encode.
- **Zero-copy reads**: record values are `Bytes` slices into the fetch buffer.
- No C library and no thread hop; on glommio every request is io_uring on the core that owns the
  partition.
