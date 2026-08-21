# Cookbook

[← barnabas](../README.md)

Every snippet here is a body: it assumes an executor is already running and
elides error handling. They are compiled by
`barnabas-glommio/tests/readme_examples.rs`, because documentation that does not
build is worse than none.

There are two ways in. The **staged builder** is the guided one:

```rust
use barnabas_glommio::{Consumer, Glommio, StartOffset};

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

The snippets below are bodies: they assume an executor is already running and
elide the error handling. Under glommio, the shortest complete program that
runs one of them is

```rust
#[glommio::main]
async fn main() -> barnabas_glommio::Result<()> {
    // any snippet below
    Ok(())
}
```

`#[glommio::main]` builds a `LocalExecutor` and runs the body on it; without it,
`LocalExecutorBuilder::default().make()?.run(async { .. })` is the same thing
written out. Under tokio it is `#[tokio::main]` and the snippets are otherwise
unchanged — that is the point of the binding split.

Every snippet in this file is compiled by `tests/readme_examples.rs`, because
documentation that does not build is worse than none.

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

**Four assignors**, and picking one picks the rebalancing protocol with it,
because they are the same choice:

| assignor | rebalancing | what it does |
|---|---|---|
| `RangeAssignor` | eager | Java's default: contiguous ranges per topic |
| `RoundRobinAssignor` | eager | spread across all topics |
| `StickyAssignor` | eager | balanced first, then minimal movement |
| `CooperativeStickyAssignor` | **cooperative** (KIP-429) | revokes only what must move |

The first two match Java 3.9.0 byte-for-byte across the oracle cases in
[`Oracle.java`](Oracle.java). Cooperative rebalancing means a member keeps reading the
partitions nobody is taking, instead of the whole group stopping while
everything is handed back and redealt.

**Heartbeats go out when you `poll`.** This client spawns nothing, so there is
no background thread sending them. A caller that spends longer than
`session.timeout.ms` between polls is removed from the group — Java avoids this
with a heartbeat thread and a separate `max.poll.interval.ms`. Call
`Consumer::heartbeat` from your own task if your processing can outlast the
session timeout.

Rebalance callbacks run where you would expect, and revocation is reported
before the partitions move:

```rust
consumer.set_rebalance_listener(Box::new(MyListener));   // on_assigned / on_revoked
consumer.set_group_timeouts(Duration::from_secs(30), Duration::from_secs(60));
```

### Consume

Assignment is the caller's: no consumer group, no rebalance, no offset commit.
Offsets are yours to store, which is what makes this usable from a system that
checkpoints them itself.

```rust
use barnabas_glommio::{Consumer, Glommio, IsolationLevel, EARLIEST};

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
use barnabas_glommio::{Glommio, Producer, ProducerRecord};

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

### Exactly-once across a consumer group

Consume, transform, produce — with the input offsets committing **inside** the
producer's transaction, so a crash between producing and committing replays the
input rather than losing the output.

```rust
let records = consumer.poll().await?;
producer.begin_transaction()?;
producer.send_keyed("output", &transformed).await?;

// After producing, before committing: these offsets account for that output.
producer
    .send_offsets_to_transaction(&consumer.positions(), &consumer.group_metadata().unwrap())
    .await?;
producer.commit_transaction().await?;
```

`group_metadata()` carries the member id and the fencing token the coordinator
checks (KIP-447), so a member that has already been replaced cannot commit. Its
fields are deliberately not readable: naming a *generation* above the protocol
seam is precisely what KIP-848 changes, so the only thing a caller does with one
is pass it along. It is `None` unless the member is stable, and it must be
re-read per transaction.

Java and librdkafka both take the offsets from the caller too — the producer
holds no reference to the consumer, and a transaction's offsets are not always
"everything read so far".

### Producing one record at a time

`send` and `send_keyed` send what you give them. `produce` accumulates:

```rust
producer.set_linger(Duration::from_millis(5));
producer.set_batch_size(32 * 1024);          // default 16 KiB, as in Java

for event in events {
    producer.produce("events", record(event)).await?;   // batches on your behalf
}
producer.flush().await?;
```

A batch goes out when it fills or its linger expires. **The clock is only read
when the producer is called**, because nothing runs in the background here — so
a steady stream sends itself, and an idle producer holds its last partial batch
until `flush` or `tick`. An event loop with nothing else to do sleeps until
`linger_deadline()` and then calls `tick()`; the timer is the caller's, because
the tasks are.

### Pausing, offsets and lag

```rust
let hot = [TopicPartition::new("events", 3), TopicPartition::new("events", 4)];
consumer.pause(&hot);                         // still assigned, still heartbeating
consumer.resume(&hot);                        // carries on from where it stopped

let ends  = consumer.end_offsets(&partitions).await?;
let start = consumer.beginning_offsets(&partitions).await?;
let at    = consumer.offsets_for_times(&[(TopicPartition::new("events", 0), when)]).await?;
let lag   = consumer.lag().await?;
let done  = consumer.committed(&partitions).await?;
```

All of them share one batched `ListOffsets`: **one request per leader**, not per
partition, so this on a 64-partition assignment is one or two round trips.

Two absences are deliberate, and match Java. A partition with no record at or
after its timestamp is **absent** from `offsets_for_times` rather than present
with a sentinel; a partition with no position is **absent** from `lag` rather
than zero. "Unknown" and "zero" are different answers, and an alert built on the
second one stays quiet through a consumer that never started.

### Administering a cluster

```rust
let mut admin = Admin::connect(Glommio, &brokers, "my-tool").await?;

admin.create_topics(&[NewTopic::new("events", 8, 3).with_config("retention.ms", "604800000")]).await?;
admin.create_partitions("events", 16).await?;    // the new *total*, not a delta
let brokers = admin.describe_cluster().await?;   // and which one is the controller
let config  = admin.describe_topic_config("events").await?;
admin.delete_records(&[(TopicPartition::new("events", 0), 1_000)]).await?;
admin.delete_topics(&["events".to_owned()]).await?;
```

Enough to write a test suite and an operational tool without a second client;
librdkafka's admin surface is much larger and this is not trying to match it.

`create_topics` and `create_partitions` return when the change is **visible**,
not when the controller accepted it. The two are seconds apart, and a producer
that writes in between gets "no leader" for a topic that certainly exists.

`TOPIC_ALREADY_EXISTS` is an error rather than a silent success — a caller who
wants create-if-absent can ignore that code, and a caller who does not want it
and never learns is the one producing into the wrong partition count.

### When a topic grows partitions

Kafka topics gain partitions at runtime and never lose them. A client holding a
stale count keeps hashing keys against a number that no longer exists, and a
consumer holding a fixed assignment never reads the new partitions at all —
silently, because nothing errors and `lag` only covers what is assigned.

```rust
consumer.set_metadata_max_age(Duration::from_secs(60));   // 5 minutes by default

for (topic, before, after) in consumer.take_expansions() {
    tracing::warn!(%topic, before, after, "topic grew; extend the assignment");
}
```

- A **subscribed** consumer rejoins its group, and the leader assigns the new
  partitions. Nothing to do.
- A **manually assigned** one is told, and never extended behind your back. Java
  does not extend a manual assignment either, and it should not — but a caller
  who is never *told* cannot decide.
- A **producer** re-reads the count on the same schedule, so its keyed placement
  stops disagreeing with every other producer.

Only growth is acted on. A count that dipped would be a broker mid-election, and
moving every key on it would be worse than the stale count.

### The same program on tokio

```rust
use barnabas_tokio::{Consumer, IsolationLevel, Tokio, EARLIEST};

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
barnabas-glommio = { version = "0.1", features = ["tls"] }
```

```rust
// The type aliases are bound to the plaintext transport, so a TLS client is
// spelled out in full — which also needs `barnabas-client` as a direct
// dependency.
let transport = barnabas_glommio::tls::GlommioTls::new();
let mut consumer =
    barnabas_client::Consumer::<_>::new(transport, &brokers, "my-app", isolation).await?;
```

SASL is on the client and works on either runtime — PLAIN, SCRAM-SHA-256 and
SCRAM-SHA-512, with the server signature verified:

```rust
let mut cluster = Cluster::connect(Glommio, &brokers, "my-app").await?;
cluster.set_credentials(Credentials::scram_sha256("user", "pass"));
```
