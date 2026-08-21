# barnabas

A native Rust Kafka client with a sans-io core, so it runs on **any** async
runtime — including thread-per-core ones, which is what it was built for.

```toml
barnabas-glommio = "0.1"   # thread-per-core, io_uring
barnabas-tokio   = "0.1"   # work-stealing
```

```rust
use barnabas_glommio::{Consumer, Glommio, IsolationLevel, EARLIEST};

let mut consumer =
    Consumer::new(Glommio, &["localhost:9092".into()], "my-app", IsolationLevel::ReadCommitted)
        .await?;
consumer.assign("events", 0, EARLIEST).await?;

for batch in consumer.poll().await? {
    for record in batch.iter() {
        handle(record.value());
    }
}
```

Swap the two names for `barnabas_tokio` / `Tokio` and that program runs on
tokio instead. Nothing else changes — see [Choosing a runtime](docs/runtimes.md).

## Stability

**0.1.x, and early.** The client is feature-complete and covered by a broker
suite on both runtimes, but it is new and the surface has had few users.

**Breaking changes can happen in 0.x, and will be avoided where they can be.**
Where one is unavoidable it gets a minor bump, a CHANGELOG entry, and a
migration note. The 1.0 line is where that stops being a judgement call. If you
need immovable API, pin an exact version.

## Why

`rdkafka` wraps librdkafka, which owns its own OS threads and a poll-based C
API. It works, but it will never cooperate with io_uring: every call from a
thread-per-core runtime is a hop onto threads the runtime did not schedule.
`rskafka` is native but documents no transactions and no offset tracking. So the
gap is a **native transactional producer**, and that is the piece worth
building.

## Shape

```
kafka-protocol          generated from Kafka's JSON schemas — the wire codec, not ours
      ↓
barnabas-core           sans-io: framing, correlation, filtering, sequencing. No sockets,
      ↓                 no clock, no Send.
barnabas-client         the client: pooling, leader routing, metadata, fetch and transaction
      ↓                 flows. Generic over a four-function `Transport`. Still no Send.
barnabas-glommio (64)   connect, read, write, sleep.
barnabas-tokio   (67)   connect, read, write, sleep.
```

Those line counts are the load-bearing part. A binding supplies four functions
and a handful of type aliases; **all protocol code is shared**, so the two
runtimes cannot drift and a fix cannot land in one and not the other.

The core names no runtime, so there is nothing to abstract over and no `Send`
bound forced by a lowest-common-denominator trait. The *binding* decides
`Send`-ness, which is how one state machine serves both a per-core handle and a
work-stealing one.

The second reason for sans-io is testing, and for a Kafka client it is the
bigger one: **exactly-once bugs are silent.** A wrong retry duplicates records
while every status code stays green. So they have to be caught by driving the
client adversarially rather than by watching a broker behave.
`barnabas-client/tests/adversarial.rs` does that against a simulated broker that
speaks the real wire protocol and lies on cue — a leader that moves mid-produce,
a coordinator that migrates, a fenced epoch. No sockets, and `sleep` returns
immediately, so a test that forces forty retries runs in microseconds.

## Documentation

| | |
|---|---|
| [Cookbook](docs/cookbook.md) | Consuming, producing, transactions, groups, admin, TLS/SASL — every example, compiled |
| [Choosing a runtime](docs/runtimes.md) | Why a binding and not a feature flag, and how to write a third |
| [What works](docs/status.md) | The full feature table |
| [Testing](docs/testing.md) | The suites, and bringing up a broker |
| [Performance](PERF.md) | The whole matrix, the machine, the variance, what is unmeasured |
| [Security defaults](docs/security.md) | What is on, and what has no switch to turn it off |
| [Broker lessons](docs/broker-lessons.md) | What a real broker taught us, and why the filtering rules look like that |

## Two things to know before choosing it

**Heartbeats only go out when you `poll`.** This client spawns nothing, so there
is no background thread sending them. A caller that spends longer than
`session.timeout.ms` between polls is removed from the group. Call
`Consumer::heartbeat` from your own task if processing can outlast the session
timeout. See [the cookbook](docs/cookbook.md#who-decides-which-partitions-you-read).

**Not implemented, deliberately:** KIP-848 server-side assignment (the seam is
there), static membership (KIP-345), Kerberos/GSSAPI, and a configuration-string
API.

## Performance

Against `rdkafka` on one local broker, using **its best observed number** for
every ratio. [`PERF.md`](PERF.md) has every cell and the caveats.

| | barnabas | rdkafka |
|---|---:|---:|
| produce, batch 1 000 × 128 B | 3.3–3.8 M rec/s | 313–338 k rec/s |
| consume, 128 B | 7.6–8.1 M rec/s | 123–327 k rec/s |
| produce latency **max** | **1.7–2.0 ms** | **502–504 ms** |

The consume gap is partly an API difference — batch-at-a-time versus
message-at-a-time — and is labelled as such in `PERF.md`, along with two
fairness mistakes found and corrected while writing it.

## License

MIT OR Apache-2.0.
