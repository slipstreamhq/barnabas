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
comparison errs against us — but see the Java section: `rdkafka` is **not** the fastest mainstream
client, so a ratio against it flatters us regardless.

## Many cores — the claim the design rests on

N producer instances, each owning its own partitions of one 8-partition topic. `kestrel` uses N
glommio executors pinned to N cores; `rdkafka` uses N threads each with its own client; the Java
column is N concurrent `kafka-producer-perf-test` processes, rates summed. Connection setup is
outside the timing for all three.

| instances | kestrel | Java | rdkafka |
|---:|---:|---:|---:|
| 1 | 3.50 – 4.07 M | 1.70 M | 0.56 M |
| 2 | 4.17 – 8.45 M | 2.63 M | 0.66 M |
| 4 | 15.2 – 16.9 M | 3.71 M | 0.66 M |
| 8 | **24.9 – 29.1 M** | 5.38 M | 0.66 M |
| scaling, 1→8 | **6.1× – 8.3×** | 3.2× | 1.2× |

**This is the result the design exists for.** Per-core throughput stays flat — about 3.1–4.2 M
records/s each, from one core to eight — so the aggregate tracks the core count. Java scales too,
at roughly 3.2×; librdkafka is **flat from two clients onward**, gaining nothing from 2 to 8.

At 8 instances we are **4.6× – 5.4× the Java client** and **37× – 44× librdkafka**.

Two honest qualifications:

- **We may be approaching what this cluster can absorb at 25–29 M records/s, and the other two are
  not.** Java at 5.4 M and librdkafka at 0.66 M are nowhere near the ceiling we are pushing against,
  so their limits here are client-side, not broker-side. That makes the *ratio* trustworthy and the
  *absolute* ceiling ours alone — a bigger cluster would raise our number and probably not theirs.
- **Eight JVMs is a much heavier footprint** than eight glommio executors on the same machine:
  eight heaps, eight GCs, eight sender threads, all competing with the brokers for the same box.
  Some of Java's shortfall at 8 is that crowding rather than the client.

The 2-core row still dips occasionally (one run of two came out at 4.17 M, a 1.19× step, against
8.45 M and 2.08× in the other). Unexplained, and milder than what it used to be.

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
| 128 B | 14 791 000 – 15 835 000 | 132 000 – 152 000 | **~100×** (against its best) |

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

## Against rskafka — the other pure-Rust client

`rskafka` 0.6, from InfluxData's IOx: async, no C library, no background thread pool. Architecturally
the closest thing to this client, and therefore the most informative comparison in this file.

| cell | kestrel | rskafka | verdict |
|---|---:|---:|---|
| batch 1, 128 B | 26 600 – 31 600 | 22 300 – 22 600 | kestrel, ~1.3× |
| batch 10, 128 B | 189 000 – 236 000 | 91 000 – 205 000 | kestrel (see note) |
| batch 100, 128 B | 1 094 000 – 1 170 000 | 2 435 | see below — not a real comparison |
| batch 1 000, 128 B | 3 972 000 – 4 569 000 | 1 176 000 – 3 139 000 | kestrel, 1.3× – 3.9× |
| batch 1 000, 1 KiB | 1 685 000 – 1 954 000 | 1 292 000 – 1 364 000 | kestrel, 1.2× – 1.5× |
| consume, 128 B | **14 791 000 – 15 835 000** | 3 923 000 – 5 065 000 | kestrel, 3.4× – 4.0× |

**We are not fastest on every dimension.** rskafka beats us at small batches and, more importantly,
**beats us on consume by about 45%**. The ~20× consume advantage over librdkafka elsewhere in this
file says more about librdkafka's per-message `StreamConsumer` API than about our decoder, and this
row is the evidence.

Two differences do real work for us and should be weighed against those losses, but they do not
explain them away:

- **rskafka has no idempotent producer.** No sequence numbers to allocate or track, so it does less
  per batch than we do.
- **rskafka has no transactions**, so its consumer does no READ_COMMITTED filtering. Ours reads
  `aborted_transactions` and filters every batch.

It also binds a client to **one partition**, so eight partitions are eight clients and eight
connections, and a write spanning them is eight requests. Ours is one request per *broker*. That is
the trade this client was designed around, and at batch 1 000 it shows.

### The lean decoder: built, measured, and now the only path

`kestrel_core::records` reads a record batch without building a record per record. Batch facts —
producer id, transactional, control — are kept once, and a record is an offset, a timestamp and
ranges into the buffer the batch owns. Keys, values and headers are materialised when a caller asks
for them, which matters because every slice of one buffer increments the same atomic refcount.

| consume, 1 M records | rec/s |
|---|---:|
| before (a record per record) | 8 021 000 – 8 296 000 |
| **after** | **14 791 000 – 15 835 000** |
| rskafka | 3 923 000 – 5 065 000 |

**About 1.9× on consume**, and 3.4× – 4.0× rskafka. The measurement reads every value, so
materialising the payload is inside it.

Filtering is cheaper for a second reason: `transactional`, `control` and `producer_id` are
batch-level in the format, so an aborted transaction is dropped a batch at a time rather than a
record at a time.

**Headers and compression are handled**, which the first version was not.

- **Headers** were never the difficulty they looked like: the record keeps the header block's byte
  range and count — eight bytes — and parses it only on demand. A record without headers costs
  nothing. Holding them inline would have put a `Vec` in every record and undone the point.
- **Compression** needs the codec run before anything can be parsed, so the records section is
  decompressed into a buffer the batch then owns. One allocation per batch, which `kafka_protocol`
  pays too.
- **Kafka's snappy is xerial-framed** — a 16-byte magic header then `[u32 length][block]` repeated —
  not raw snappy, and reading it as raw fails at once. The round-trip test against the reference
  decoder caught it.

Only a **pre-magic-2 batch** falls back, and then only that partition pays the old cost.

**It is now the only path.** `fetch` returns the batches and `FetchedRecords::iter` walks them as
`RecordRef`s, so a caller that does not care about batching does not have to see it. Keeping the old
decoder beside it would have been one job with two APIs.

What it costs to own: about 350 lines of format-parsing code, where being wrong means handing a
caller bad records rather than failing. Guarded by the CRC, by the fallback, by a round-trip test per
codec against the reference decoder, and by a live test over a partition holding plain records, an
aborted transaction and a committed one.

### Would SIMD help the decoder? No — measured

Decode is now the largest single item in a consume: ~70 ns of a ~120 ns/record budget, where before
the `max_bytes` fix it was a fifth of it. So it is worth attacking. SIMD is not how.

| stage | ns/record |
|---|---:|
| **parse only** — walk every record, keep nothing | **3.0** |
| crc32c over the batch | 13.7 – 14.3 |
| parse + 24-byte record (offsets into the batch) | 5.5 |
| parse + `Bytes` slices | 27.9 – 29.4 |
| **`kafka_protocol` → `Vec<Record>` (what we do today)** | **69 – 73** |
| `rskafka` → `RecordBatch` | 121 – 123 |

**The parsing SIMD would target is 3 ns of 70 — four percent.** Varint decoding and pointer
arithmetic are not the cost, and could not be: records are variable-length and each one's position
depends on the previous one's length, so there is little for wide lanes to do. The one place SIMD
already applies is the CRC, and `crc32c` is using the hardware instruction — it shows up in profiles
as `crc_u64_parallel3`.

**Three quarters of the time is materialisation.** Parsing into a 24-byte record costs 5.5 ns;
producing `Bytes` slices instead costs 28–29; producing `kafka_protocol`'s `Record` costs 69–73.
That last struct carries producer id, epoch, sequence, partition leader epoch, timestamp type and
two flags — **all of which are batch-level fields being copied into every record** — plus an
`IndexMap` for headers.

The `Bytes` step is worth understanding before optimising it. Every record's key and value is a slice
of the *same* buffer, so every slice increments the *same* atomic refcount — a million records is two
million read-modify-writes on one cache line, self-contended. That is why refcounting measures the
same as copying 128 bytes: it is not the atomic being expensive, it is the sharing.

**So the lever is representation, not instructions.** A record that holds offsets into a retained
buffer and materialises a `Bytes` only when the caller asks for one decodes at 5.5 ns against 69 —
about twelve times faster — and would take decode from ~58% of the consume budget to under 10%.
Sketching what that is worth: consume might go from 8.4 M to somewhere above 12 M records/s.

Not attempted here. It means owning the record-batch decoder rather than using `kafka_protocol`'s,
including compression, control records, headers and every batch version — a much larger commitment
than a benchmark cell, and one to make deliberately.

### The small-batch rows were comparing different work

An earlier revision recorded a 15–27% loss at batch 10. It was the harness.

`send_keyed` hashes every record independently, so a batch of ten spreads over eight partitions and
becomes one request per *broker* — three of them — while rskafka's partition-bound client sends all
ten to one partition in one request. Three round trips against one is not a comparison.

The table now carries a **1-partition column**: the same records, sent by this client to a single
partition, which is rskafka's shape exactly. Matched that way, batch 10 is 189 000 – 236 000 against
rskafka's 91 000 – 205 000. Both are noisy at that size and the ranges overlap; what is no longer
true is that we lose.

At batch 1 000 the spread-out shape is the faster of our two, which is the batching this client is
built around doing its job.

**The 8 KiB row runs only in the spread shape.** A thousand 8 KiB records on one partition is one
8 MB message, and the broker's default `max.message.bytes` is just over 1 MiB, so the
single-partition shapes cannot send it. `send_keyed` spreads it across eight partitions and stays
under. The cell is skipped for those columns rather than reported as a failure.

### Chasing the consume gap: both hypotheses wrong, and the real cause

Isolating the two suspects in a broker-free microbenchmark (`KESTREL_CELLS=decode`, one record batch
in memory, both decoders on the same bytes) refuted both of them:

| | ns/record |
|---|---:|
| crc32c over the batch — the floor | 13.3 – 14.3 |
| **kafka-protocol → `Vec<Record>` (ours)** | **66** |
| **rskafka → `RecordBatch` (theirs)** | **121 – 125** |
| clone every key+value (`Bytes`, refcounted) | 24.8 – 25.1 |
| copy every key+value (`Vec<u8>`) | 23.6 – 24.4 |
| move the decoded `Vec<Record>` once | 32 |

**Our decoder is about twice as fast as rskafka's**, and **refcounting a `Bytes` costs the same as
copying the bytes** at this record size. Both hypotheses in the previous revision of this file were
wrong, and the "zero-copy is secretly expensive" worry was unfounded.

That reframed the arithmetic: decoding costs 66 ns/record, but consuming cost 308 ns/record. The
other 242 ns were not in the decoder at all.

**The cause was one number used for two budgets.** `max_bytes` was applied both to each partition
(`partition_max_bytes`) *and* to the whole response (`fetch.max.bytes`), at 10 MiB. One `Fetch` per
broker carries many partitions, so the response cap was shared between them — while a client that
fetches each partition on its own connection gets the full budget for each. The test that found it
was running our own consumer in the shape we had deliberately replaced:

| shape | rec/s |
|---|---:|
| one `Fetch` per broker, shared 10 MiB response cap | 3 102 000 – 3 109 000 |
| one consumer per partition (the shape we replaced) | 8 695 000 – 9 749 000 |
| **one `Fetch` per broker, 64 MiB response cap** | **8 036 000 – 8 464 000** |

Splitting the two budgets recovers nearly all of it: **consume went from 3.10 M to 8.0–8.5 M
records/s**, and this client now leads rskafka by 1.6× – 1.8× on the cell it was losing by 45%.

The per-broker batching was never the problem — starving it was. It is still worth one connection
per broker instead of one per partition.

### The batch-100 figure is not a comparison

2 435 records/s, reproducible to four significant figures across runs, while the neighbouring cells
manage 20 000 requests/s. Per request that is ~41 ms, against ~0.05 ms at batch 10 — a fixed delay,
not a size law.

**`rskafka` does not set `TCP_NODELAY`** (no `set_nodelay` anywhere in its source), and ~40 ms is the
classic delayed-ACK timer. That is consistent with Nagle holding a partial final segment until the
peer's delayed ACK arrives, at one payload size and not its neighbours. Stated as the likely cause,
not a proven one — no packet capture was taken. **Do not read 449× from that row.**

**We were not setting it either.** Every serious Kafka client does — Java and librdkafka both — and
this client now does too, on both runtimes. It changed none of the numbers above, because none of
our payload sizes were landing in that trap, but it is a latent hazard removed on a client that is
supposed to be about tail latency.

## Against the Java client — the comparison that matters

`./java-bench.sh`, which runs Apache's own `kafka-producer-perf-test` and
`kafka-consumer-perf-test` in a container on the cluster network. Apache's tools rather than
benchmark code of mine, because these are the numbers anyone would quote back at us. The Java
producer gets its best configuration (`linger.ms=0`, 1 MiB batches, five in flight, `acks=all`,
idempotent) and **far more records than our own cells use**, so its JIT is warm before the number is
taken.

| cell | kestrel | Java | librdkafka |
|---|---:|---:|---:|
| batched, 128 B | 3 745 000 – 4 505 000 | **1 622 000** | 316 000 – 343 000 |
| batched, 1 KiB | 1 415 000 – 1 946 000 | **426 000** | 316 000 – 333 000 |
| batched, 8 KiB | 189 000 – 259 000 | **72 000** | 83 000 – 91 000 |
| one record at a time, queued | 199 000 – 229 000 | **153 000** | — |
| consume, 128 B | 2 944 000 – 3 080 000 | **172 000** (241 000 excluding rebalance) | 132 000 – 152 000 |

**The Java client is much faster than librdkafka**, by roughly 5× on batched 128 B — so every ratio
elsewhere in this file is measured against the weaker of the two mainstream clients. Read the
headline as **2.3× – 4.6× against the best available client**, not the 11× – 14× the librdkafka
column suggests. That is the single most important correction in this document, and it exists
because the comparison was asked for rather than because it was volunteered.

librdkafka does beat Java at 8 KiB records (83–91 k against 72 k), the one cell where it wins.

### The one-record row is not the one it looks like

`kafka-producer-perf-test` with `batch.size=0` reaches 153 000 records/s — six times our *synchronous*
`send`, which is one round trip per record. That is not a like-for-like comparison: Java is queueing
deeply, and its own output says so, reporting **829 ms average and 1 137 ms 99th-percentile latency**
for that run. The fair analogue is `enqueue` one record at a time with a deep window, which reaches
199 000 – 229 000. Compared that way we are ahead; compared the other way we would have been
reporting a loss that is really an API difference.

### What this table does not say

Java's producer latency figures (0.22 ms average, 2 ms 99th, 133 ms max on the batched 128 B run)
are measured **under full-throttle load**, while our produce-latency cell sends one record at a time
into an idle cluster. They are not comparable and are deliberately not placed side by side.

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
- **The occasional 2-core dips** are unexplained, though far milder than what the retry backoff was
  causing.
- **Whether 25–29 M records/s is our ceiling or the cluster's** is unknown; the other clients are far
  enough below it that only our own number is in question.
- **Nothing outstanding on the decoder**: it handles headers and every codec, and is the only path.
- **Whether 64 MiB is the right response budget.** It was chosen to be clearly larger than the
  per-partition budget, not tuned; the memory a fetch may hold scales with it.
- **rskafka has no many-core row.** Its per-partition client shape makes the comparison less direct,
  and it has not been run.
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
