# kestrel

A native Rust Kafka client with a sans-io core, so it runs on any async runtime —
including thread-per-core ones, which is what it was built for.

**Working name, and early.** Nothing here is published and the API will change.

> **Consumer groups work**: `subscribe`, rebalancing, committed offsets, auto-commit and
> rebalance callbacks. Two gaps to know about before choosing this client:
> **heartbeats only go out when you `poll`** (see below), and **only eager rebalancing** —
> `cooperative-sticky` is not implemented yet. Remaining work is scoped in
> [`docs/completing-the-client.md`](docs/completing-the-client.md).

Design and sequencing:
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
kestrel-core            sans-io: framing, correlation, filtering, sequencing. No sockets,
      ↓                 no clock, no Send.
kestrel-client          the client: pooling, leader routing, metadata, fetch and transaction
      ↓                 flows. Generic over a four-function `Transport`. Still no Send.
kestrel-glommio (64)    connect, read, write, sleep.
kestrel-tokio   (67)    connect, read, write, sleep.
```

Those line counts are the load-bearing part. A binding supplies four functions and a handful of
type aliases; **all protocol code is shared**, so the two runtimes cannot drift and a fix cannot
land in one and not the other. The same broker suite — 6 consumer tests, 8 producer tests — runs
against both.

The core names no runtime, so there is nothing to abstract over and no `Send` bound forced by a
lowest-common-denominator trait. The *binding* decides `Send`-ness, which is how one state machine
serves both a per-core handle and a work-stealing one.

The second reason for sans-io is testing, and for a Kafka client it is the bigger one: exactly-once
bugs are silent — a wrong retry duplicates records while every status code stays green — so they
have to be caught by driving the client adversarially rather than by watching a broker behave.

`kestrel-client/tests/adversarial.rs` does exactly that. The `Transport` seam takes a **simulated
broker** that speaks the real wire protocol and lies on cue: a leader that moves mid-produce, a
coordinator that migrates, a fenced epoch, an out-of-sequence error. No runtime, no sockets, and
`sleep` returns immediately, so a test that forces forty retries runs in microseconds and
reproduces exactly. The assertions are on what the broker **accepted** — a duplicate returns `Ok`
just like a success does.

Both properties were mutation-proved: resetting sequences per transaction fails
`sequences_continue_across_transactions`, and classifying `NOT_COORDINATOR` as a plain retry fails
`a_moved_coordinator_is_rediscovered`.

## Choosing a runtime

**By depending on a binding, not by setting a feature.** There is no `cfg` in
this workspace selecting a runtime, and no default one to override.

`kestrel-client` is generic over one trait:

```rust
pub trait Transport: 'static {
    type Stream: 'static;
    fn connect(&self, addr: &str) -> impl Future<Output = io::Result<Self::Stream>>;
    fn read(stream: &mut Self::Stream, buf: &mut [u8]) -> impl Future<Output = io::Result<usize>>;
    fn write_all(stream: &mut Self::Stream, buf: &[u8]) -> impl Future<Output = io::Result<()>>;
    fn sleep(dur: Duration) -> impl Future<Output = ()>;
}
```

A binding is that trait plus three type aliases. `kestrel-glommio` is 64 lines;
`kestrel-tokio` is 67. So the choice is which line you put in `Cargo.toml`:

```toml
# thread-per-core, io_uring
kestrel-glommio = "0.1"
```
```toml
# work-stealing
kestrel-tokio = "0.1"
```

and which name you import:

```rust
use kestrel_glommio::{Consumer, Glommio, EARLIEST};   // or
use kestrel_tokio::{Consumer, Tokio, EARLIEST};
```

Each binding re-exports everything shared — `Error`, `Result`, `RecordRef`,
`ConsumerRecords`, `ProducerRecord`, `IsolationLevel`, `Partitioner`,
`CompressionCodec`, `EARLIEST`, `LATEST` — so **only those two lines differ**
between the two programs below.

### Why not a feature flag

Cargo features are additive: if two crates in a build ask for different
runtimes, a feature-selected client gets both and has to pick one, and the
program that did not ask for the winner is the one that breaks. Types do not
unify that way — `Consumer<Glommio>` and `Consumer<Tokio>` are different types,
so a binary can hold both at once, on different threads, and the compiler keeps
them apart.

It also means the `Send`-ness is the *binding's* property. `glommio::net::TcpStream`
belongs to the core that opened it, so everything on `Glommio` is `!Send` and
stays on its executor; the tokio side is `Send` from the same client code
because `kestrel-client` places no `Send` bound anywhere. A trait that demanded
`Send` would have forbidden the per-core side outright.

Selecting per build is then the *caller's* job, and a `cfg` in one place does
it. Slipstream, for instance, does this in its Kafka crate:

```rust
#[cfg(feature = "glommio")]
use kestrel_glommio as kestrel;
#[cfg(not(feature = "glommio"))]
use kestrel_tokio as kestrel;
```

## Creating a client

There are two ways in. The **staged builder** is the guided one:

```rust
use kestrel_glommio::{Consumer, Glommio, StartOffset};

let mut consumer = Consumer::builder(Glommio)      // needs a bootstrap list
    .bootstrap(["localhost:9092"])                 // needs a client id
    .client_id("my-app")                           // now everything else appears
    .assign_range("events", 0..8, StartOffset::Earliest)
    .assign("late-events", 0, StartOffset::At(1_234))
    .max_wait(Duration::from_millis(100))
    .build()
    .await?;
```

```rust
let mut producer = Producer::builder(Glommio)
    .bootstrap(["localhost:9092"])
    .client_id("my-app")
    .transactional_id("my-app-sink-0")   // omit for a plain idempotent producer
    .compression(CompressionCodec::Snappy)
    .max_in_flight(5)
    .build()
    .await?;
```

**Each stage is its own type.** `bootstrap` is the only method on the first,
`client_id` the only one on the second, and the optional settings and `build`
exist only on the third. So a completion list is the set of legal next steps,
and a missing required argument is a compile error naming the stage rather than
a failure at connect. `tests/builder_stages.rs` pins that; the shapes that must
*not* compile are listed there too.

`StartOffset` is an enum — `Earliest`, `Latest`, `At(offset)` — rather than the
protocol's `i64`, where the two sentinels are negative numbers sharing a
parameter with real offsets.

The **constructors** are still there and are the shortest path when nothing
optional is wanted:

```rust
let mut consumer =
    Consumer::new(Glommio, &brokers, "my-app", IsolationLevel::ReadCommitted).await?;
consumer.assign("events", 0, EARLIEST).await?;
```

Worth knowing what you are choosing: `Consumer::assign` takes seven positional
arguments including an adjacent `partition: i32` and `offset: i64`, so
transposing them compiles. The builder exists because that is the kind of
mistake a type can prevent.

## Examples

### Who decides which partitions you read

**Either you or the group — both are supported, and they are different tools.**

`subscribe` joins a consumer group and lets it decide: partitions arrive from
the group's leader and move when membership changes. This is what most programs
want.

```rust
consumer.subscribe("my-group", vec!["events".into()], Box::new(RangeAssignor), EARLIEST).await?;
consumer.set_auto_commit(Some(Duration::from_secs(5)));

loop {
    for batch in consumer.poll().await? {      // also drives membership
        for record in batch.iter() { handle(record.value()); }
    }
}
```

`assign` is the other tool: **you** choose the partitions and store the offsets,
and no group protocol runs at all. That suits a system whose own control plane
places partitions and whose checkpoints hold offsets — a group would be a second
authority for both. It is what Slipstream uses.

What the broker *is* asked, on your behalf:

| question | request |
|---|---|
| which broker leads each partition | `Metadata` |
| how many partitions does this topic have | `Metadata` (used by `assign_all`) |
| what offset is `Earliest` / `Latest` right now | `ListOffsets` |

You say *which* partitions; the broker says *where they live* and *what offsets
bound them*.

```rust
// Every partition — the count comes from the broker, so expanding the topic
// does not silently leave the new partitions unread on the next restart.
.assign_all("events", StartOffset::Earliest)

// A share of them, decided by something above this client.
.assign_range("events", my_shard.partitions(), StartOffset::Earliest)

// Resuming from offsets you stored yourself.
.assign("events", 3, StartOffset::At(checkpoint.offset_for(3)))
```

**Two things to know about the group implementation:**

- **Heartbeats go out when you `poll`.** This client spawns nothing, so there is
  no background thread sending them. A caller that spends longer than
  `session.timeout.ms` between polls is removed from the group — Java avoids
  this with a heartbeat thread and a separate `max.poll.interval.ms`. Call
  [`Consumer::heartbeat`] from your own task if your processing can outlast the
  session timeout.
- **Rebalancing is eager**: on a rebalance every partition is given up and
  reassigned. `cooperative-sticky`, which moves only what has to move, is not
  implemented yet.

### Consume

Assignment is the caller's: no consumer group, no rebalance, no offset commit.
Offsets are yours to store, which is what makes this usable from a system that
checkpoints them itself.

```rust
use kestrel_glommio::{Consumer, Glommio, IsolationLevel, EARLIEST};

let mut consumer = Consumer::new(
    Glommio,
    &["localhost:9092".to_owned()],
    "my-app",
    IsolationLevel::ReadCommitted,
).await?;

for partition in 0..8 {
    consumer.assign("events", partition, EARLIEST).await?;
}

loop {
    // One `Fetch` per *broker*, carrying every partition it leads — not one
    // per partition. All of them are in flight at once.
    for group in consumer.poll().await? {
        for record in group.iter() {
            // Key, value and headers are built when you ask for them, not at
            // decode time. Skipping a record costs nothing.
            let value = record.value();
            println!("{}-{} @{}: {:?}", group.topic, group.partition, record.offset(), value);
        }
    }
}
```

To resume from stored offsets, seek instead of assigning at `EARLIEST`:

```rust
consumer.seek_to("events", 3, my_checkpoint.offset_for(3));
```

### Produce

```rust
use bytes::Bytes;
use kestrel_glommio::{Glommio, Producer, ProducerRecord};

let mut producer = Producer::idempotent(
    Glommio,
    &["localhost:9092".to_owned()],
    "my-app",
).await?;

let records = vec![
    ProducerRecord::new(Some(Bytes::from("user-1")), Some(Bytes::from("hello"))),
    ProducerRecord::new(Some(Bytes::from("user-2")), Some(Bytes::from("world"))),
];

// Keys are hashed the way librdkafka hashes them (CRC-32), so a program
// migrating off `rdkafka` keeps its key placement. `Partitioner::Murmur2`
// matches the Java client instead; the two disagree for most keys.
producer.send_keyed("events", &records).await?;
```

### Produce with several requests in flight

`send` and `send_keyed` await, so they have exactly one request outstanding.
To pipeline, enqueue and then flush:

```rust
for batch in batches {
    producer.enqueue("events", partition, &batch).await?;  // encodes, sends nothing
}
let written = producer.flush().await?;   // up to 5 in flight, then collected
```

Five is the default, as in the Java client. If a request fails partway through
the window, everything behind it is re-sent **in order** — see
[`spec/`](spec/) for the model check of that rule.

### Transactions

```rust
let mut producer = Producer::transactional(
    Glommio,
    &brokers,
    "my-app",
    "my-app-sink-0",   // stable per instance: this is what fences a zombie
).await?;

producer.begin_transaction()?;
producer.send_keyed("events", &records).await?;
producer.commit_transaction().await?;   // or abort_transaction()
```

`InitProducerId` fences any earlier producer holding that transactional id, so
give each parallel instance its own — and only one process may hold one at a
time. Two initialisations of the same id is always a bug, and its symptom
appears later and elsewhere, as a failed commit.

### The same program on tokio

```rust
use kestrel_tokio::{Consumer, IsolationLevel, Tokio, EARLIEST};

let mut consumer = Consumer::new(
    Tokio,
    &["localhost:9092".to_owned()],
    "my-app",
    IsolationLevel::ReadCommitted,
).await?;
```

The rest is character-for-character identical.

### TLS and SASL

TLS is a feature on the binding, since the socket is the binding's business:

```toml
kestrel-glommio = { version = "0.1", features = ["tls"] }
```

```rust
// The type aliases are bound to the plaintext transport, so a TLS client is
// spelled out in full — which also needs `kestrel-client` as a direct
// dependency.
let transport = kestrel_glommio::tls::GlommioTls::new();
let mut consumer =
    kestrel_client::Consumer::<_>::new(transport, &brokers, "my-app", isolation).await?;
```

SASL is on the client and works on either runtime — PLAIN, SCRAM-SHA-256 and
SCRAM-SHA-512, with the server signature verified:

```rust
let mut cluster = Cluster::connect(Glommio, &brokers, "my-app").await?;
cluster.set_credentials(Credentials::scram_sha256("user", "pass"));
```

### Writing another binding

Implement the four functions. `kestrel-tokio/src/lib.rs` is the shortest
complete example at 67 lines, and nothing above the transport needs to change.

## Status

| | |
|---|---|
| **Consumer, assign-only** | works — `ApiVersions`, `Metadata`, `ListOffsets`, `Fetch`, seek, READ_COMMITTED filtering |
| **Producer** | idempotent and transactional — sequencing, coordinator routing, zombie fencing, keyed partitioning. **One `Produce` per broker**, not per partition; enrollment likewise |
| **Compression** | gzip, snappy, lz4, zstd — round-tripped against a real broker, both directions |
| **Leader routing, metadata cache** | works — fetches go to the leader from the cluster map, `NOT_LEADER_OR_FOLLOWER` invalidates that partition and retries |
| **Runtimes** | glommio and tokio, from one client. Adding a third is a `Transport` impl |
| **Consumer** | any number of partitions, **one `Fetch` per broker** rather than per partition, with incremental fetch sessions (KIP-227) — one connection where there were eight |
| **Connection recovery** | a closed connection is evicted and redialled once; a request that outlives its deadline drops its connection, because the late response would desynchronise the stream |
| **TLS** | opt-in `tls` feature on either binding. A second `Transport`, so `Consumer<GlommioTls>` is the same client over a different socket — the shared code needed no change at all |
| **SASL** | PLAIN and SCRAM-SHA-256/512. Shared, since the handshake is protocol |
| **Consumer groups** | **out of scope** — callers assign partitions themselves |

## Performance

Measured across a matrix, not a headline — `PERF.md` has every cell, the machine, the variance, and
what is still unmeasured. Against `rdkafka` on one local broker, using **its best observed number**
for every ratio:

| | kestrel | rdkafka |
|---|---:|---:|
| produce, batch 1 000 × 128 B | 3.3–3.8 M rec/s | 313–338 k rec/s |
| produce, batch 1 × 128 B | 10–33 k rec/s | ~9.8 k rec/s |
| produce latency p50 | 0.041–0.048 ms | 0.051–0.055 ms |
| produce latency p99 | 0.051–0.113 ms | 0.087–0.098 ms (a tie) |
| produce latency **max** | **1.7–2.0 ms** | **502–504 ms** |
| consume, 128 B | 7.6–8.1 M rec/s | 123–327 k rec/s |

The consume gap is partly an API difference (batch-at-a-time versus message-at-a-time) and is
labelled as such in `PERF.md`, along with two fairness mistakes that were found and corrected while
writing it.

```sh
cargo run -p kestrel-bench --release        # needs a broker
```

## Tests

```sh
cargo test                                        # 89 tests, no broker, no runtime
cargo test -p kestrel-glommio -- --ignored --test-threads=1   # needs a broker
cargo test -p kestrel-tokio   -- --ignored --test-threads=1   # the same suite, other runtime
```

The broker tests need a single-node Kafka; the invocation is at the top of
`kestrel-glommio/tests/real_broker.rs`. Note that `__transaction_state` defaults to replication
factor 3, so a single-node broker needs it overridden or every transaction fails with error 15.

## Security defaults

- **Certificate verification is on and there is no switch to turn it off.** A private CA goes in
  through `with_roots`, a pinned certificate or client auth through `from_config`.
- **`Credentials`' `Debug` prints `<redacted>`.** A password in a log is a security bug wearing a
  convenience's clothes.
- **SCRAM verifies the server's final signature.** Skipping it is the classic implementation
  shortcut, and it means authenticating to anyone who can complete a handshake.
- `SaslMechanism::requires_encryption()` is true for PLAIN, which sends the password in the clear.

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
- **There is no single "Kafka default" partitioner.** librdkafka hashes keys with CRC-32; the Java
  client uses murmur2. Same key, same topic, different partition, no error. `kestrel` implements
  both and defaults to CRC-32, so a program migrating off `rdkafka` keeps its placement — checked
  against `rdkafka` itself, key for key, while it is still around to be the oracle.
- **Metadata requests ask for topic auto-creation**, as librdkafka and Java do, and
  `UNKNOWN_TOPIC_OR_PARTITION` refreshes rather than failing: a topic being created reports it for
  a moment.
- **Error codes are states, not failures.** A cold cluster answers 15, then 14, then 16 — the last
  *after* a successful `FindCoordinator`. `CONCURRENT_TRANSACTIONS` (51) appears whenever a
  transaction starts right after one ends. See `ErrorCode::disposition`.

## Brokers

`./cluster.sh up` brings up three Apache Kafka brokers (KRaft, no ZooKeeper);
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
