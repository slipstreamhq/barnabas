//! A matrix, not a headline.
//!
//! One number is a slogan. This walks the axes that actually change a Kafka
//! client's behaviour — batch size, record size, and the difference between
//! throughput and latency — and prints `kestrel` beside `rdkafka` for each.
//!
//! **It is built to find the cells where we lose.** The interesting output is
//! not the row where a native client beats a C library on batched throughput;
//! it is the row where librdkafka's internal accumulator beats an API that
//! sends when it is told to. Those rows are marked.
//!
//! # Fairness
//!
//! librdkafka runs with its best configuration rather than its defaults
//! (`linger.ms=0`, large batches and queue), and for throughput every record is
//! enqueued before anything is awaited, so its accumulator chooses its own
//! batch boundaries. `PERF.md` records what happened when it was not given
//! that: a 3× understatement, and nearly a published headline.
//!
//! ```sh
//! cargo run -p kestrel-bench --release
//! ```

use std::time::{Duration, Instant};

use bytes::Bytes;
use kestrel_client::{ConsumerRecords, ProducerRecord};
use kestrel_glommio::CompressionCodec;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

const PARTITIONS: i32 = 8;

/// Records in the consume cell. Overridable because a fetch loop pays a fixed
/// cost at the end — the last poll of each broker waits out `max_wait` with
/// nothing left to return — and only varying the record count says whether a
/// given rate is steady state or that tail.
fn consume_records() -> usize {
    std::env::var("KESTREL_CONSUME_RECORDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000)
}

fn broker() -> String {
    std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".to_owned())
}

/// The bootstrap list as kestrel wants it: one address per element.
///
/// `KAFKA_BOOTSTRAP` is a comma-separated string because that is what
/// librdkafka takes; handing the whole string to kestrel as a single address
/// would produce one unresolvable host.
fn brokers() -> Vec<String> {
    broker().split(',').map(|s| s.trim().to_owned()).collect()
}

/// Which sections to run, as a comma-separated list in `KESTREL_CELLS`
/// (`produce`, `latency`, `cores`, `consume`, `compression`, `e2e`, `idle`).
/// Everything by default. A single cell takes seconds; the whole matrix takes
/// minutes, and re-measuring one of them should not cost the others.
fn want(section: &str) -> bool {
    match std::env::var("KESTREL_CELLS") {
        Ok(list) => list.split(',').any(|s| s.trim() == section),
        Err(_) => true,
    }
}

fn topic(prefix: &str) -> String {
    unique_topic(prefix)
}

fn unique_topic(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("bench-{prefix}-{nanos}")
}

fn create_topic(name: &str) {
    create_topic_with(name, PARTITIONS);
}

fn create_topic_with(name: &str, partitions: i32) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", broker())
        .create()
        .expect("admin client");

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async {
            admin
                .create_topics(
                    &[NewTopic::new(name, partitions, TopicReplication::Fixed(1))],
                    &AdminOptions::new(),
                )
                .await
                .expect("create topic");
            tokio::time::sleep(Duration::from_millis(400)).await;
        });
}

/// One cell of the matrix.
struct Cell {
    batch: usize,
    value_bytes: usize,
    records: usize,
}

impl Cell {
    fn rate(&self, elapsed: Duration) -> f64 {
        self.records as f64 / elapsed.as_secs_f64()
    }
}

/// Percentiles, because an average hides exactly the tail that matters.
struct Latency {
    samples: Vec<Duration>,
}

impl Latency {
    fn percentile(&mut self, p: f64) -> Duration {
        self.samples.sort_unstable();
        let index = ((self.samples.len() as f64 - 1.0) * p).round() as usize;
        self.samples[index]
    }
}

fn payload(bytes: usize) -> Bytes {
    Bytes::from(vec![b'x'; bytes])
}

// ── kestrel ──────────────────────────────────────────────────────────────────

fn kestrel_produce(topic: &str, cell: &Cell) -> Duration {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let mut producer =
            kestrel_tokio::Producer::idempotent(kestrel_tokio::Tokio, &brokers(), "bench")
                .await
                .expect("producer");

        let value = payload(cell.value_bytes);
        let batch: Vec<ProducerRecord> = (0..cell.batch)
            .map(|i| ProducerRecord::new(Some(Bytes::from(format!("k{i}"))), Some(value.clone())))
            .collect();

        let start = Instant::now();
        for _ in 0..(cell.records / cell.batch) {
            producer.send_keyed(topic, &batch).await.expect("send");
        }
        start.elapsed()
    })
}

/// The same shape rskafka's cell uses: **a whole batch to one partition**,
/// round-robin across calls.
///
/// `send_keyed` hashes each record independently, so a batch of ten spreads
/// over eight partitions and becomes one request per *broker* — three, here —
/// where rskafka's per-partition client sends one. At batch 1 000 that costs
/// nothing and the batching wins; at batch 10 it is three round trips against
/// one, and comparing them is comparing different work.
fn kestrel_produce_one_partition(topic: &str, cell: &Cell) -> Duration {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let mut producer =
            kestrel_tokio::Producer::idempotent(kestrel_tokio::Tokio, &brokers(), "bench")
                .await
                .expect("producer");

        let value = payload(cell.value_bytes);
        let batch: Vec<ProducerRecord> = (0..cell.batch)
            .map(|i| ProducerRecord::new(Some(Bytes::from(format!("k{i}"))), Some(value.clone())))
            .collect();

        let start = Instant::now();
        for round in 0..(cell.records / cell.batch) {
            let partition = (round % PARTITIONS as usize) as i32;
            producer.send(topic, partition, &batch).await.expect("send");
        }
        start.elapsed()
    })
}

fn kestrel_latency(topic: &str, samples: usize, value_bytes: usize) -> Latency {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let mut producer =
            kestrel_tokio::Producer::idempotent(kestrel_tokio::Tokio, &brokers(), "bench")
                .await
                .expect("producer");

        let value = payload(value_bytes);
        let mut latencies = Vec::with_capacity(samples);
        for i in 0..samples {
            let record = vec![ProducerRecord::new(
                Some(Bytes::from(format!("k{i}"))),
                Some(value.clone()),
            )];
            let start = Instant::now();
            producer.send_keyed(topic, &record).await.expect("send");
            latencies.push(start.elapsed());
        }
        Latency { samples: latencies }
    })
}

fn kestrel_consume(topic: &str, expect: usize) -> Duration {
    let topic = topic.to_owned();
    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("executor")
        .run(async move {
            // **One consumer for every partition.** All eight travel in one
            // Fetch to the broker that leads them, over one connection.
            let mut consumer = kestrel_glommio::Consumer::new(
                kestrel_glommio::Glommio,
                &brokers(),
                "bench",
                kestrel_core::IsolationLevel::ReadCommitted,
            )
            .await
            .expect("consumer");
            for partition in 0..PARTITIONS {
                consumer
                    .assign(&topic, partition, kestrel_glommio::EARLIEST)
                    .await
                    .expect("assign");
            }
            consumer.set_max_wait(Duration::from_millis(100));

            let start = Instant::now();
            let mut seen = 0;
            let mut polls = 0u64;
            while seen < expect {
                let groups = consumer.poll().await.expect("fetch");
                let got: usize = groups.iter().map(ConsumerRecords::len).sum();
                if got == 0 {
                    break;
                }
                seen += got;
                polls += 1;
            }
            assert_eq!(seen, expect, "kestrel consumed {seen} of {expect}");
            if std::env::var("KESTREL_DEBUG").is_ok() {
                eprintln!(
                    "  kestrel: {polls} polls, {:.0} records/poll",
                    seen as f64 / polls as f64
                );
            }
            start.elapsed()
        })
}

// ── rdkafka ──────────────────────────────────────────────────────────────────

/// librdkafka at its best: no linger, big batches, big queue.
fn rdkafka_producer(linger_ms: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", broker())
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("linger.ms", linger_ms)
        .set("batch.num.messages", "10000")
        .set("batch.size", "1048576")
        .set("queue.buffering.max.messages", "2000000")
        .set("queue.buffering.max.kbytes", "2097152")
        .create()
        .expect("producer")
}

fn rdkafka_produce(topic: &str, cell: &Cell) -> Duration {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let producer = rdkafka_producer("0");
        let value = payload(cell.value_bytes);
        let keys: Vec<String> = (0..cell.batch).map(|i| format!("k{i}")).collect();

        let start = Instant::now();
        let mut inflight = Vec::with_capacity(cell.records);
        for _ in 0..(cell.records / cell.batch) {
            for key in &keys {
                inflight.push(
                    producer
                        .send_result(FutureRecord::to(topic).key(key).payload(&value[..]))
                        .expect("enqueue"),
                );
            }
        }
        for delivery in inflight {
            delivery.await.expect("delivery").expect("ack");
        }
        start.elapsed()
    })
}

fn rdkafka_latency(topic: &str, samples: usize, value_bytes: usize) -> Latency {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        // `linger.ms=0` here too: a per-record latency measurement with a
        // linger configured is a measurement of the linger.
        let producer = rdkafka_producer("0");
        let value = payload(value_bytes);
        let mut latencies = Vec::with_capacity(samples);
        for i in 0..samples {
            let key = format!("k{i}");
            let start = Instant::now();
            producer
                .send_result(FutureRecord::to(topic).key(&key).payload(&value[..]))
                .expect("enqueue")
                .await
                .expect("delivery")
                .expect("ack");
            latencies.push(start.elapsed());
        }
        Latency { samples: latencies }
    })
}

fn rdkafka_consume(topic: &str, expect: usize) -> Duration {
    use rdkafka::consumer::{Consumer as _, StreamConsumer};
    use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", broker())
            .set("group.id", "bench")
            .set("enable.auto.commit", "false")
            // librdkafka's own defaults. An earlier version of this harness
            // set `fetch.min.bytes=1MiB` and `fetch.wait.max.ms=100` intending
            // to help throughput; it cut librdkafka's consume rate from ~330k
            // to ~125k, which would have inflated the ratio below by 2.6x.
            // Tuning the alternative is how a benchmark lies without anyone
            // noticing.
            .create()
            .expect("consumer");

        let mut tpl = TopicPartitionList::new();
        for partition in 0..PARTITIONS {
            tpl.add_partition_offset(topic, partition, Offset::Beginning)
                .expect("tpl");
        }
        consumer.assign(&tpl).expect("assign");

        let start = Instant::now();
        let mut seen = 0;
        while seen < expect {
            match tokio::time::timeout(Duration::from_secs(10), consumer.recv()).await {
                Ok(Ok(_)) => seen += 1,
                _ => break,
            }
        }
        assert_eq!(seen, expect, "rdkafka consumed {seen} of {expect}");
        start.elapsed()
    })
}

/// **The case per-core is built for**: N cores, each owning a slice of the
/// partitions, each with its own executor, sockets and client.
///
/// Nothing is shared between them — no locks, no work stealing, no cross-core
/// wakeups — which is the claim the whole design rests on and which every
/// number above this point failed to exercise.
fn kestrel_produce_many_cores(topic: &str, cores: usize, cell: &Cell) -> Duration {
    assert!(
        cores <= PARTITIONS as usize,
        "a core with no partition of its own would sit idle and flatter the average"
    );
    let per_core = cell.records / cores;
    let partitions_per_core = (PARTITIONS as usize).div_ceil(cores);

    // **Producer setup is not part of the measurement.** The timer used to
    // start before the threads spawned, so N concurrent `InitProducerId` round
    // trips landed inside the timed region. Against ~50 ms of actual work a
    // single 250 ms coordinator backoff cut the reported rate by six, which is
    // what the unexplained 8-core collapses were. Every core now connects
    // first, waits, and the clock starts when they are all ready.
    let ready = std::sync::Arc::new(std::sync::Barrier::new(cores + 1));
    let handles: Vec<_> = (0..cores)
        .map(|core| {
            let ready = std::sync::Arc::clone(&ready);
            let topic = topic.to_owned();
            let batch_size = cell.batch;
            let value_bytes = cell.value_bytes;
            std::thread::spawn(move || {
                glommio::LocalExecutorBuilder::new(glommio::Placement::Fixed(core))
                    .make()
                    .expect("executor")
                    .run(async move {
                        let mut producer = kestrel_glommio::Producer::idempotent(
                            kestrel_glommio::Glommio,
                            &brokers(),
                            "bench",
                        )
                        .await
                        .expect("producer");

                        let value = payload(value_bytes);
                        let batch: Vec<ProducerRecord> = (0..batch_size)
                            .map(|i| {
                                ProducerRecord::new(
                                    Some(Bytes::from(format!("k{core}-{i}"))),
                                    Some(value.clone()),
                                )
                            })
                            .collect();

                        // Each core writes to its own partitions, so nothing
                        // contends — on the client or on the broker's log.
                        //
                        // **Every core count writes to all eight partitions**,
                        // just split differently. An earlier version had each
                        // core write to the first partition it owned and no
                        // others, which meant the 1-core baseline drove a
                        // single log while the 8-core run drove eight: part of
                        // what it called client scaling was the broker's
                        // partition parallelism.
                        // Batch built before the barrier too: it is setup, not
                        // work under test.
                        ready.wait();

                        let owned: Vec<i32> = (0..PARTITIONS)
                            .filter(|p| *p as usize / partitions_per_core == core)
                            .collect();
                        for round in 0..(per_core / batch_size) {
                            let partition = owned[round % owned.len()];
                            producer.send(&topic, partition, &batch).await.expect("send");
                        }
                    });
            })
        })
        .collect();
    ready.wait();
    let start = Instant::now();
    for handle in handles {
        handle.join().expect("core finished");
    }
    start.elapsed()
}


// ── compression ──────────────────────────────────────────────────────────────

/// A payload with something to compress, but not everything.
///
/// `payload` is a run of one byte, which every codec shrinks to nearly nothing
/// and which would make compression look free. This is roughly half structured
/// text and half incompressible noise, which is closer to the log lines and
/// serialised records people actually put in Kafka.
fn payload_mixed(bytes: usize) -> Bytes {
    let mut out = Vec::with_capacity(bytes);
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    while out.len() < bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        if out.len() % 64 < 32 {
            out.extend_from_slice(b"level=info msg=request path=/api/v1/items status=200 ");
        } else {
            out.extend_from_slice(&state.to_le_bytes());
        }
    }
    out.truncate(bytes);
    Bytes::from(out)
}

fn kestrel_produce_compressed(topic: &str, cell: &Cell, codec: CompressionCodec) -> Duration {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let mut producer =
            kestrel_tokio::Producer::idempotent(kestrel_tokio::Tokio, &brokers(), "bench")
                .await
                .expect("producer");
        producer.set_compression(codec);

        let value = payload_mixed(cell.value_bytes);
        let batch: Vec<ProducerRecord> = (0..cell.batch)
            .map(|i| ProducerRecord::new(Some(Bytes::from(format!("k{i}"))), Some(value.clone())))
            .collect();

        let start = Instant::now();
        for _ in 0..(cell.records / cell.batch) {
            producer.send_keyed(topic, &batch).await.expect("send");
        }
        start.elapsed()
    })
}

fn rdkafka_produce_compressed(topic: &str, cell: &Cell, codec: &str) -> Duration {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", broker())
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("linger.ms", "0")
            .set("compression.codec", codec)
            .set("batch.num.messages", "10000")
            .set("batch.size", "1048576")
            .set("queue.buffering.max.messages", "2000000")
            .set("queue.buffering.max.kbytes", "2097152")
            .create()
            .expect("producer");

        let value = payload_mixed(cell.value_bytes);
        let start = Instant::now();
        let mut waits = Vec::with_capacity(cell.records);
        for i in 0..cell.records {
            let key = format!("k{i}");
            waits.push(
                producer
                    .send_result(FutureRecord::to(topic).key(&key).payload(&value[..]))
                    .expect("enqueue"),
            );
        }
        for wait in waits {
            wait.await.expect("delivery").expect("delivered");
        }
        start.elapsed()
    })
}

// ── pipelining ───────────────────────────────────────────────────────────────

/// Produce with a bounded window of requests in flight per connection.
///
/// The caller enqueues `window` batches and flushes them together, which is the
/// only way to have more than one write outstanding — `send` awaits, so it can
/// never have two.
fn kestrel_produce_pipelined(topic: &str, cell: &Cell, in_flight: usize) -> Duration {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let mut producer =
            kestrel_tokio::Producer::idempotent(kestrel_tokio::Tokio, &brokers(), "bench")
                .await
                .expect("producer");
        producer.set_max_in_flight(in_flight);

        let value = payload(cell.value_bytes);
        let batch: Vec<ProducerRecord> = (0..cell.batch)
            .map(|i| ProducerRecord::new(Some(Bytes::from(format!("k{i}"))), Some(value.clone())))
            .collect();

        let rounds = cell.records / cell.batch;
        let start = Instant::now();
        let mut queued = 0;
        for round in 0..rounds {
            // Spread across partitions so this is not one partition's log.
            let partition = (round % PARTITIONS as usize) as i32;
            producer
                .enqueue(topic, partition, &batch)
                .await
                .expect("enqueue");
            queued += 1;
            if queued == in_flight {
                producer.flush().await.expect("flush");
                queued = 0;
            }
        }
        if queued > 0 {
            producer.flush().await.expect("flush");
        }
        start.elapsed()
    })
}

// ── end-to-end latency ───────────────────────────────────────────────────────

/// **Produce to consume**, which is the latency a pipeline actually feels.
///
/// The producer and consumer share one executor, so this is one core doing both
/// halves — the shape a per-core stream processor has.
fn kestrel_end_to_end(
    topic: &str,
    samples: usize,
    value_bytes: usize,
    prefetch: bool,
) -> Latency {
    let topic = topic.to_owned();
    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("executor")
        .run(async move {
            let mut producer = kestrel_glommio::Producer::idempotent(
                kestrel_glommio::Glommio,
                &brokers(),
                "bench-e2e-producer",
            )
            .await
            .expect("producer");

            let mut consumer = kestrel_glommio::Consumer::for_partition(
                kestrel_glommio::Glommio,
                &brokers(),
                "bench-e2e-consumer",
                &topic,
                0,
                kestrel_glommio::LATEST,
                kestrel_core::IsolationLevel::ReadCommitted,
            )
            .await
            .expect("consumer");
            consumer.set_max_wait(Duration::from_millis(50));
            consumer.set_prefetch(prefetch);

            let value = payload(value_bytes);
            let mut latencies = Vec::with_capacity(samples);
            for i in 0..samples {
                let record = vec![ProducerRecord::new(
                    Some(Bytes::from(format!("k{i}"))),
                    Some(value.clone()),
                )];
                let start = Instant::now();
                producer.send(&topic, 0, &record).await.expect("send");
                // Bounded, so a harness mistake shows up as a failure rather
                // than a process that sits there forever.
                let mut polls = 0;
                loop {
                    let groups = consumer.poll().await.expect("fetch");
                    if groups.iter().any(|g| !g.is_empty()) {
                        break;
                    }
                    polls += 1;
                    assert!(polls < 200, "record {i} never arrived");
                }
                latencies.push(start.elapsed());
            }
            Latency { samples: latencies }
        })
}

fn rdkafka_end_to_end(topic: &str, samples: usize, value_bytes: usize) -> Latency {
    use rdkafka::consumer::{Consumer as _, StreamConsumer};
    use rdkafka::{Offset, TopicPartitionList};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let producer = rdkafka_producer("0");
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", broker())
            .set("group.id", "bench-e2e")
            .set("enable.auto.commit", "false")
            .create()
            .expect("consumer");
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(topic, 0, Offset::End)
            .expect("assign");
        consumer.assign(&assignment).expect("assign");

        let value = payload(value_bytes);
        let mut latencies = Vec::with_capacity(samples);
        for i in 0..samples {
            let key = format!("k{i}");
            let start = Instant::now();
            // **Partition 0 explicitly.** Keyed records would be spread over
            // all eight partitions while the consumer holds only one, so most
            // would never arrive and the loop would block forever — which is
            // exactly what it did. The kestrel side pins partition 0 too, so
            // the two measure the same path.
            producer
                .send_result(
                    FutureRecord::to(topic)
                        .partition(0)
                        .key(&key)
                        .payload(&value[..]),
                )
                .expect("enqueue")
                .await
                .expect("delivery")
                .expect("delivered");
            tokio::time::timeout(Duration::from_secs(10), consumer.recv())
                .await
                .expect("record never arrived")
                .expect("message");
            latencies.push(start.elapsed());
        }
        Latency { samples: latencies }
    })
}

// ── fetch sessions ───────────────────────────────────────────────────────────

/// The cell KIP-227 exists for: **many partitions, almost all of them idle**.
///
/// A full fetch names every partition and its offset on every poll. An
/// incremental one names only what moved, which is nothing here. Every cell
/// elsewhere in this file keeps every partition busy, which is precisely the
/// workload where the feature does nothing — so without this it was shipped
/// unmeasured.
fn kestrel_idle_polls(topic: &str, partitions: i32, polls: usize, incremental: bool) -> Duration {
    let topic = topic.to_owned();
    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("executor")
        .run(async move {
            let mut consumer = kestrel_glommio::Consumer::new(
                kestrel_glommio::Glommio,
                &brokers(),
                "bench-idle",
                kestrel_core::IsolationLevel::ReadCommitted,
            )
            .await
            .expect("consumer");
            for partition in 0..partitions {
                consumer
                    .assign(&topic, partition, kestrel_glommio::EARLIEST)
                    .await
                    .expect("assign");
            }
            consumer.set_incremental_fetch(incremental);
            // **Zero**, so the cell measures request cost rather than the
            // broker's willingness to wait. At 1 ms every poll was floor-bound
            // at ~1.2 ms and the two configurations came out within 1% of each
            // other — a cell that could not have shown a difference if there
            // was one.
            consumer.set_max_wait(Duration::ZERO);

            // One poll to establish the session before timing starts.
            consumer.poll().await.expect("warm-up fetch");

            let start = Instant::now();
            for _ in 0..polls {
                consumer.poll().await.expect("fetch");
            }
            start.elapsed()
        })
}

// ── soak ─────────────────────────────────────────────────────────────────────

/// Resident set size in KiB, straight from `/proc/self/statm`.
fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok()))
        .map_or(0, |pages| pages * 4)
}

/// Produce continuously for `seconds`, reporting the rate and RSS each interval.
///
/// Everything else here is a burst of a few seconds, which says nothing about
/// what happens after an hour or whether memory settles. Opt-in via
/// `KESTREL_SOAK_SECONDS` because it is slow by construction.
fn kestrel_soak(topic: &str, seconds: u64) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let mut producer =
            kestrel_tokio::Producer::idempotent(kestrel_tokio::Tokio, &brokers(), "bench-soak")
                .await
                .expect("producer");

        let value = payload(128);
        let batch: Vec<ProducerRecord> = (0..1_000)
            .map(|i| ProducerRecord::new(Some(Bytes::from(format!("k{i}"))), Some(value.clone())))
            .collect();

        let started = Instant::now();
        let mut window_start = Instant::now();
        let mut window_records = 0u64;
        let mut rates = Vec::new();
        let baseline_rss = rss_kib();

        while started.elapsed().as_secs() < seconds {
            producer.send_keyed(topic, &batch).await.expect("send");
            window_records += batch.len() as u64;

            if window_start.elapsed() >= Duration::from_secs(10) {
                let rate = window_records as f64 / window_start.elapsed().as_secs_f64();
                println!(
                    "  t+{:>4}s          {:>12.0} rec/s     RSS {:>7} KiB",
                    started.elapsed().as_secs(),
                    rate,
                    rss_kib()
                );
                rates.push(rate);
                window_records = 0;
                window_start = Instant::now();
            }
        }

        if rates.len() >= 2 {
            let first = rates[0];
            let last = *rates.last().expect("non-empty");
            println!(
                "  drift             last window is {:>5.1}% of the first; RSS grew {} KiB",
                last / first * 100.0,
                rss_kib().saturating_sub(baseline_rss)
            );
        }
    });
}

/// The same shape as [`kestrel_produce_many_cores`], for librdkafka: **N
/// independent clients**, each owning its own partitions of one topic.
///
/// Threads rather than pinned executors, deliberately. librdkafka runs its own
/// background sender threads and pinning the calling thread would not pin
/// those, so pinning would constrain the API thread and nothing else. This
/// gives it the shape it is built for: N clients, each accumulating and
/// sending on its own schedule.
fn rdkafka_produce_many_clients(topic: &str, clients: usize, cell: &Cell) -> Duration {
    assert!(clients <= PARTITIONS as usize);
    let per_client = cell.records / clients;
    let partitions_per_client = (PARTITIONS as usize).div_ceil(clients);

    let ready = std::sync::Arc::new(std::sync::Barrier::new(clients + 1));
    let handles: Vec<_> = (0..clients)
        .map(|client| {
            let ready = std::sync::Arc::clone(&ready);
            let topic = topic.to_owned();
            let value_bytes = cell.value_bytes;
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("runtime");
                runtime.block_on(async move {
                    let producer = rdkafka_producer("0");
                    let value = payload(value_bytes);
                    let owned: Vec<i32> = (0..PARTITIONS)
                        .filter(|p| *p as usize / partitions_per_client == client)
                        .collect();

                    // Connection setup excluded from the timing, as it is for
                    // kestrel.
                    ready.wait();

                    let mut waits = Vec::with_capacity(per_client);
                    for i in 0..per_client {
                        let partition = owned[i % owned.len()];
                        let key = format!("k{client}-{i}");
                        waits.push(
                            producer
                                .send_result(
                                    FutureRecord::to(&topic)
                                        .partition(partition)
                                        .key(&key)
                                        .payload(&value[..]),
                                )
                                .expect("enqueue"),
                        );
                    }
                    for wait in waits {
                        wait.await.expect("delivery").expect("delivered");
                    }
                });
            })
        })
        .collect();

    ready.wait();
    let start = Instant::now();
    for handle in handles {
        handle.join().expect("client finished");
    }
    start.elapsed()
}

/// The same consume loop on **tokio**, to separate the runtime from the code.
///
/// If this matches the glommio number, the gap to rskafka is in what this
/// client does per record, not in which executor it does it on.
fn kestrel_consume_tokio(topic: &str, expect: usize) -> Duration {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let mut consumer = kestrel_tokio::Consumer::new(
            kestrel_tokio::Tokio,
            &brokers(),
            "bench",
            kestrel_core::IsolationLevel::ReadCommitted,
        )
        .await
        .expect("consumer");
        for partition in 0..PARTITIONS {
            consumer
                .assign(topic, partition, kestrel_tokio::EARLIEST)
                .await
                .expect("assign");
        }
        consumer.set_max_wait(Duration::from_millis(100));

        let start = Instant::now();
        let mut seen = 0;
        while seen < expect {
            let groups = consumer.poll().await.expect("fetch");
            let got: usize = groups.iter().map(ConsumerRecords::len).sum();
            if got == 0 {
                break;
            }
            seen += got;
        }
        assert_eq!(seen, expect, "kestrel/tokio consumed {seen} of {expect}");
        start.elapsed()
    })
}

/// **One consumer per partition**, the shape this client deliberately replaced.
///
/// Eight consumers means eight connections and eight `Fetch` requests per
/// round, against one request per broker for the batched shape. If this is
/// faster, the consume gap is connection parallelism rather than anything in
/// the decode path — and the per-broker batching that makes this client cheap
/// for a broker to serve costs throughput on a bulk read.
fn kestrel_consume_per_partition(topic: &str, expect: usize) -> Duration {
    let topic = topic.to_owned();
    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("executor")
        .run(async move {
            let mut consumers = Vec::new();
            for partition in 0..PARTITIONS {
                let mut consumer = kestrel_glommio::Consumer::for_partition(
                    kestrel_glommio::Glommio,
                    &brokers(),
                    "bench",
                    &topic,
                    partition,
                    kestrel_glommio::EARLIEST,
                    kestrel_core::IsolationLevel::ReadCommitted,
                )
                .await
                .expect("consumer");
                consumer.set_max_wait(Duration::from_millis(100));
                consumers.push(consumer);
            }

            let start = Instant::now();
            let mut seen = 0;
            while seen < expect {
                let mut got = 0;
                for consumer in &mut consumers {
                    let groups = consumer.poll().await.expect("fetch");
                    got += groups.iter().map(ConsumerRecords::len).sum::<usize>();
                }
                if got == 0 {
                    break;
                }
                seen += got;
            }
            assert_eq!(seen, expect, "per-partition consumed {seen} of {expect}");
            start.elapsed()
        })
}

// ── rskafka ──────────────────────────────────────────────────────────────────
//
// The other pure-Rust client, and the closest peer to this one: async, no C
// library, no background thread pool. Two differences shape every number below
// and neither is a criticism — they are design choices.
//
// - **A client is bound to one partition.** `partition_client(topic, p)` is the
//   unit, so eight partitions mean eight clients and eight connections, and a
//   write to eight partitions is eight requests. This client's whole produce
//   path is built the other way round: one request per *broker*, carrying every
//   partition it leads.
// - **No idempotent producer.** `acks` is -1 as ours is, but there are no
//   sequence numbers, so rskafka does less work per batch than we do. That
//   favours rskafka here.

fn rskafka_client(runtime: &tokio::runtime::Runtime) -> rskafka::client::Client {
    runtime.block_on(async {
        rskafka::client::ClientBuilder::new(brokers())
            .build()
            .await
            .expect("rskafka client")
    })
}

fn rskafka_record(value: &[u8], key: &str) -> rskafka::record::Record {
    rskafka::record::Record {
        key: Some(key.as_bytes().to_vec()),
        value: Some(value.to_vec()),
        headers: std::collections::BTreeMap::new(),
        timestamp: chrono::DateTime::from_timestamp_millis(0).expect("epoch"),
    }
}

fn rskafka_produce(topic: &str, cell: &Cell) -> Duration {
    use rskafka::client::partition::{Compression, UnknownTopicHandling};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let client = rskafka_client(&runtime);

    runtime.block_on(async {
        let mut partitions = Vec::new();
        for partition in 0..PARTITIONS {
            partitions.push(
                client
                    .partition_client(topic.to_owned(), partition, UnknownTopicHandling::Retry)
                    .await
                    .expect("partition client"),
            );
        }

        let value = payload(cell.value_bytes);
        let batch: Vec<rskafka::record::Record> = (0..cell.batch)
            .map(|i| rskafka_record(&value, &format!("k{i}")))
            .collect();

        let start = Instant::now();
        for round in 0..(cell.records / cell.batch) {
            let partition = &partitions[round % partitions.len()];
            partition
                .produce(batch.clone(), Compression::NoCompression)
                .await
                .expect("produce");
        }
        start.elapsed()
    })
}

fn rskafka_consume(topic: &str, expect: usize) -> Duration {
    use rskafka::client::partition::UnknownTopicHandling;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let client = rskafka_client(&runtime);

    runtime.block_on(async {
        let mut partitions = Vec::new();
        for partition in 0..PARTITIONS {
            partitions.push(
                client
                    .partition_client(topic.to_owned(), partition, UnknownTopicHandling::Retry)
                    .await
                    .expect("partition client"),
            );
        }

        let start = Instant::now();
        let mut offsets = vec![0i64; partitions.len()];
        let mut seen = 0;
        let mut polls = 0u64;
        while seen < expect {
            let mut got = 0;
            for (i, partition) in partitions.iter().enumerate() {
                polls += 1;
                let (records, _high) = partition
                    .fetch_records(offsets[i], 1..10_485_760, 100)
                    .await
                    .expect("fetch");
                if let Some(last) = records.last() {
                    offsets[i] = last.offset + 1;
                }
                got += records.len();
            }
            if got == 0 {
                break;
            }
            seen += got;
        }
        assert_eq!(seen, expect, "rskafka consumed {seen} of {expect}");
        if std::env::var("KESTREL_DEBUG").is_ok() {
            eprintln!(
                "  rskafka: {polls} fetches, {:.0} records/fetch",
                seen as f64 / polls as f64
            );
        }
        start.elapsed()
    })
}

// ── decode, in isolation ─────────────────────────────────────────────────────
//
// No broker, no sockets. One record batch in memory, decoded by both clients,
// so the per-record cost that the consume cell only hints at can be attributed.
//
// The two suspects from profiling the consume gap:
//   1. `Bytes` refcounting — every key and value is a slice into the fetch
//      buffer, so a million records is two million atomic increments and as
//      many decrements. rskafka copies into `Vec<u8>` instead.
//   2. Struct size — `kafka_protocol`'s `Record` carries producer id, epoch,
//      sequence, timestamp type, two flags and an `IndexMap` of headers.

/// One record batch, encoded the way a broker would send it.
fn encoded_batch(count: usize, value_bytes: usize) -> Bytes {
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };

    let value = payload(value_bytes);
    let records: Vec<Record> = (0..count)
        .map(|i| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: 1,
            producer_epoch: 0,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            sequence: i as i32,
            timestamp: 0,
            key: Some(Bytes::from(format!("k{i}"))),
            value: Some(value.clone()),
            headers: Default::default(),
        })
        .collect();

    let mut buf = bytes::BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .expect("encode");
    buf.freeze()
}

fn time<T>(label: &str, iterations: usize, records: usize, mut f: impl FnMut() -> T) {
    // One untimed pass, so the first-touch page faults and any lazy
    // initialisation are not charged to the measurement.
    let _ = f();

    let start = Instant::now();
    for _ in 0..iterations {
        std::hint::black_box(f());
    }
    let elapsed = start.elapsed();
    let per_record = elapsed.as_secs_f64() / (iterations * records) as f64;
    println!(
        "{label:<34} {:>9.1} ns/record   {:>12.0} records/s",
        per_record * 1e9,
        1.0 / per_record
    );
}

fn decode_cell() {
    const COUNT: usize = 1_000;
    const ITERATIONS: usize = 2_000;

    let batch = encoded_batch(COUNT, 128);
    println!(
        "\ndecode in isolation: {COUNT} records x 128 B, {} B on the wire\n",
        batch.len()
    );

    // The floor: what the bytes cost to look at at all.
    time("crc32c over the batch", ITERATIONS, COUNT, || {
        crc32c::crc32c(&batch)
    });

    // What this client does today.
    time("kafka-protocol -> Vec<Record>", ITERATIONS, COUNT, || {
        let mut cursor = batch.clone();
        kafka_protocol::records::RecordBatchDecoder::decode(&mut cursor).expect("decode")
    });

    // What rskafka does, on the same bytes.
    time("rskafka -> RecordBatch", ITERATIONS, COUNT, || {
        use rskafka::protocol::traits::ReadType;
        let mut cursor = std::io::Cursor::new(batch.as_ref());
        rskafka::protocol::record::RecordBatch::read(&mut cursor).expect("decode")
    });

    // Suspect 1, isolated: refcounting a slice against copying the same bytes.
    let decoded = {
        let mut cursor = batch.clone();
        kafka_protocol::records::RecordBatchDecoder::decode(&mut cursor)
            .expect("decode")
            .records
    };
    time("  clone every key+value (Bytes)", ITERATIONS, COUNT, || {
        let mut sink = Vec::with_capacity(decoded.len());
        for record in &decoded {
            sink.push((record.key.clone(), record.value.clone()));
        }
        sink
    });
    time("  copy every key+value (Vec<u8>)", ITERATIONS, COUNT, || {
        let mut sink = Vec::with_capacity(decoded.len());
        for record in &decoded {
            sink.push((
                record.key.as_ref().map(|k| k.to_vec()),
                record.value.as_ref().map(|v| v.to_vec()),
            ));
        }
        sink
    });

    // Where the decode time actually goes.
    time("lean: parse only, keep nothing", ITERATIONS, COUNT, || {
        lean::parse_only(&batch)
    });
    time("lean: + 24-byte record (offsets)", ITERATIONS, COUNT, || {
        lean::lean_offsets(&batch)
    });
    time("lean: + Bytes slices", ITERATIONS, COUNT, || {
        lean::lean_bytes(&batch)
    });

    // Suspect 2, isolated: moving the decoded records once, which is what the
    // filter used to do unconditionally and what any extra pass costs.
    time("  move Vec<Record> once", ITERATIONS, COUNT, || {
        decoded.clone().into_iter().collect::<Vec<_>>()
    });
}

/// A hand-written record-batch v2 reader, to find out **where the 66 ns go**.
///
/// Three variants of the same parse, so parsing can be separated from what is
/// built out of it:
///
/// - `parse only` walks the records and keeps nothing. This is the floor SIMD
///   would attack: varint decoding and pointer arithmetic.
/// - `lean (offsets)` stores 24 bytes per record — offset, and the key/value
///   positions within the batch.
/// - `lean (Bytes)` stores refcounted slices, like `kafka_protocol` does.
///
/// The gap between them is materialisation cost, which no amount of SIMD in
/// the parser can remove.
mod lean {
    use bytes::Bytes;

    /// Zigzag LEB128, the encoding Kafka uses inside a record batch.
    #[inline]
    fn varint(buf: &[u8], pos: &mut usize) -> i64 {
        let mut raw: u64 = 0;
        let mut shift = 0;
        loop {
            let byte = buf[*pos];
            *pos += 1;
            raw |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        ((raw >> 1) as i64) ^ -((raw & 1) as i64)
    }

    /// Records start after the fixed 61-byte batch header.
    const HEADER: usize = 61;

    fn record_count(buf: &[u8]) -> usize {
        let bytes = [buf[57], buf[58], buf[59], buf[60]];
        i32::from_be_bytes(bytes).max(0) as usize
    }

    /// Walk every record, keep nothing.
    pub fn parse_only(buf: &[u8]) -> usize {
        let count = record_count(buf);
        let mut pos = HEADER;
        for _ in 0..count {
            let len = varint(buf, &mut pos) as usize;
            let end = pos + len;
            pos += 1; // attributes
            varint(buf, &mut pos); // timestamp delta
            varint(buf, &mut pos); // offset delta
            let key_len = varint(buf, &mut pos);
            if key_len > 0 {
                pos += key_len as usize;
            }
            // The value is not walked: the record's length prefix already says
            // where the next one starts, and headers would be skipped the same
            // way. Reading its length keeps the parse honest about the work.
            let _value_len = varint(buf, &mut pos);
            pos = end;
        }
        count
    }

    /// 24 bytes per record: no refcounts, no copies, no fat struct.
    #[derive(Clone, Copy)]
    pub struct Slim {
        pub offset: i64,
        pub key: (u32, u32),
        pub value: (u32, u32),
    }

    pub fn lean_offsets(buf: &[u8]) -> Vec<Slim> {
        let count = record_count(buf);
        let base_offset = i64::from_be_bytes(buf[0..8].try_into().expect("8 bytes"));
        let mut out = Vec::with_capacity(count);
        let mut pos = HEADER;
        for _ in 0..count {
            let len = varint(buf, &mut pos) as usize;
            let end = pos + len;
            pos += 1;
            varint(buf, &mut pos);
            let offset_delta = varint(buf, &mut pos);
            let key_len = varint(buf, &mut pos);
            let key = if key_len > 0 {
                let at = (pos as u32, key_len as u32);
                pos += key_len as usize;
                at
            } else {
                (0, 0)
            };
            let value_len = varint(buf, &mut pos);
            let value = if value_len > 0 {
                (pos as u32, value_len as u32)
            } else {
                (0, 0)
            };
            out.push(Slim {
                offset: base_offset + offset_delta,
                key,
                value,
            });
            pos = end;
        }
        out
    }

    /// The same, but building refcounted slices as `kafka_protocol` does.
    pub fn lean_bytes(batch: &Bytes) -> Vec<(i64, Bytes, Bytes)> {
        let slim = lean_offsets(batch);
        slim.into_iter()
            .map(|r| {
                (
                    r.offset,
                    batch.slice(r.key.0 as usize..(r.key.0 + r.key.1) as usize),
                    batch.slice(r.value.0 as usize..(r.value.0 + r.value.1) as usize),
                )
            })
            .collect()
    }
}

fn main() {
    println!(
        "{PARTITIONS} partitions, acks=all, idempotent, RF 1, {} broker(s)\n",
        brokers().len()
    );

    if want("decode") {
        decode_cell();
    }

    if want("produce") {
        println!(
            "produce throughput      kestrel     1-partition      rdkafka      rskafka   vs rsk"
        );
        let cells = [
            // Small batches first, because that is where an internal accumulator
            // earns its keep and where this client has none.
            Cell { batch: 1, value_bytes: 128, records: 5_000 },
            Cell { batch: 10, value_bytes: 128, records: 20_000 },
            Cell { batch: 100, value_bytes: 128, records: 100_000 },
            Cell { batch: 1_000, value_bytes: 128, records: 200_000 },
            Cell { batch: 1_000, value_bytes: 1_024, records: 200_000 },
            Cell { batch: 1_000, value_bytes: 8_192, records: 50_000 },
        ];
        for cell in &cells {
            let kestrel_topic = topic("k");
            create_topic(&kestrel_topic);
            let kestrel = cell.rate(kestrel_produce(&kestrel_topic, cell));

            let rd_topic = topic("r");
            create_topic(&rd_topic);
            let rdkafka = cell.rate(rdkafka_produce(&rd_topic, cell));

            // A whole batch on one partition is one Kafka message, and the
            // broker's default `max.message.bytes` is 1 MiB. `send_keyed`
            // spreads across eight partitions and stays under it; the
            // single-partition shapes cannot, so that cell is not run for them
            // rather than being run and reported as a failure.
            // Kafka's default `max.message.bytes` is just over 1 MiB.
            let fits = cell.batch * cell.value_bytes <= 1_100_000;

            let rskafka = if fits {
                let rs_topic = topic("s");
                create_topic(&rs_topic);
                cell.rate(rskafka_produce(&rs_topic, cell))
            } else {
                f64::NAN
            };

            // Matched to rskafka's shape, so the small-batch rows compare the
            // same work rather than one request against three.
            let one_partition = if fits {
                let one_topic = topic("o");
                create_topic(&one_topic);
                cell.rate(kestrel_produce_one_partition(&one_topic, cell))
            } else {
                f64::NAN
            };

            // Our best of the two shapes against theirs: reporting the
            // mismatched one as a loss would be measuring the harness.
            let ours = if one_partition.is_nan() {
                kestrel
            } else {
                kestrel.max(one_partition)
            };
            let ratio = ours / rskafka;
            let marker = if ours < rdkafka || (!rskafka.is_nan() && ours < rskafka) {
                "  <-- slower"
            } else {
                ""
            };
            println!(
                "batch {:>5}, {:>5} B  {:>12.0} {:>12.0} {:>12.0} {:>12.0}   {:>5.2}x{marker}",
                cell.batch, cell.value_bytes, kestrel, one_partition, rdkafka, rskafka, ratio
            );
        }
    }

    if want("latency") {
        println!("\nproduce latency (1 record)      kestrel        rdkafka");
        let samples = 2_000;
        let kestrel_topic = topic("kl");
        create_topic(&kestrel_topic);
        let mut kestrel = kestrel_latency(&kestrel_topic, samples, 128);

        let rd_topic = topic("rl");
        create_topic(&rd_topic);
        let mut rdkafka = rdkafka_latency(&rd_topic, samples, 128);

        for (label, p) in [("p50", 0.50), ("p99", 0.99), ("max", 1.0)] {
            let k = kestrel.percentile(p);
            let r = rdkafka.percentile(p);
            let marker = if k > r { "  <-- slower" } else { "" };
            println!(
                "{label:<8} {:>18.3} ms {:>12.3} ms{marker}",
                k.as_secs_f64() * 1e3,
                r.as_secs_f64() * 1e3
            );
        }

        // ── many cores, the shape the design exists for ─────────────────────────
    }

    if want("cores") {
        println!(
            "\nproduce, N cores      kestrel rec/s      per core   scaling   rdkafka rec/s   ratio"
        );
        let cell = Cell { batch: 1_000, value_bytes: 128, records: 400_000 };
        let mut single_core = 0.0;
        for cores in [1usize, 2, 4, 8] {
            let topic = topic("mc");
            create_topic(&topic);
            let rate = cell.rate(kestrel_produce_many_cores(&topic, cores, &cell));
            if cores == 1 {
                single_core = rate;
            }

            // **The same shape for librdkafka**, so the scaling claim has
            // something to be a claim against. Without this the row said only
            // that we scale, not that we scale better than anyone.
            let rd_topic = unique_topic("mr");
            create_topic(&rd_topic);
            let rd_rate = cell.rate(rdkafka_produce_many_clients(&rd_topic, cores, &cell));

            println!(
                "{cores:>2} cores        {:>12.0}  {:>12.0}   {:>6.2}x   {:>12.0}   {:>5.1}x",
                rate,
                rate / cores as f64,
                rate / single_core,
                rd_rate,
                rate / rd_rate
            );
        }
    }

    if want("consume") {
        println!(
            "\nconsume throughput    kestrel rec/s   rdkafka rec/s   rskafka rec/s   vs rsk"
        );
        let cell = Cell { batch: 1_000, value_bytes: 128, records: consume_records() };

        let kestrel_topic = topic("kc");
        create_topic(&kestrel_topic);
        kestrel_produce(&kestrel_topic, &cell);
        let kestrel_rate = cell.rate(kestrel_consume(&kestrel_topic, cell.records));

        let rd_topic = topic("rc");
        create_topic(&rd_topic);
        rdkafka_produce(&rd_topic, &cell);
        let rdkafka_rate = cell.rate(rdkafka_consume(&rd_topic, cell.records));

        let pp_topic = topic("pp");
        create_topic(&pp_topic);
        kestrel_produce(&pp_topic, &cell);
        let pp_rate = cell.rate(kestrel_consume_per_partition(&pp_topic, cell.records));
        println!("      (kestrel, one consumer per partition: {pp_rate:.0} rec/s)");

        let tk_topic = topic("tc");
        create_topic(&tk_topic);
        kestrel_produce(&tk_topic, &cell);
        let tokio_rate = cell.rate(kestrel_consume_tokio(&tk_topic, cell.records));
        println!("      (kestrel on tokio: {tokio_rate:.0} rec/s)");

        let rs_topic = topic("sc");
        create_topic(&rs_topic);
        kestrel_produce(&rs_topic, &cell);
        let rskafka_rate = cell.rate(rskafka_consume(&rs_topic, cell.records));

        let ratio = kestrel_rate / rskafka_rate;
        let marker = if kestrel_rate < rdkafka_rate || kestrel_rate < rskafka_rate {
            "  <-- slower"
        } else {
            ""
        };
        println!(
            "{:>5} B              {:>12.0}   {:>12.0}   {:>12.0}   {:>5.2}x{marker}",
            cell.value_bytes, kestrel_rate, rdkafka_rate, rskafka_rate, ratio
        );

        // ── compression ─────────────────────────────────────────────────────────
    }

    if want("compression") {
        println!("\ncompression (1 KiB records)   kestrel rec/s   rdkafka rec/s     ratio");
        let cell = Cell { batch: 1_000, value_bytes: 1_024, records: 200_000 };
        for (codec, name) in [
            (CompressionCodec::None, "none"),
            (CompressionCodec::Gzip, "gzip"),
            (CompressionCodec::Snappy, "snappy"),
            (CompressionCodec::Lz4, "lz4"),
            (CompressionCodec::Zstd, "zstd"),
        ] {
            let kestrel_topic = topic("kz");
            create_topic(&kestrel_topic);
            let kestrel = cell.rate(kestrel_produce_compressed(&kestrel_topic, &cell, codec));

            let rd_topic = topic("rz");
            create_topic(&rd_topic);
            let rdkafka = cell.rate(rdkafka_produce_compressed(&rd_topic, &cell, name));

            let ratio = kestrel / rdkafka;
            let marker = if ratio < 1.0 { "  <-- slower" } else { "" };
            println!(
                "{name:<10}                {:>12.0}   {:>12.0}   {:>6.2}x{marker}",
                kestrel, rdkafka, ratio
            );
        }

        // ── end-to-end latency ──────────────────────────────────────────────────
    }

    if want("e2e") {
        println!("\nend-to-end latency (produce to consume)");
        println!("           kestrel   no prefetch       rdkafka");
        let samples = 500;
        let kestrel_topic = topic("ke");
        create_topic(&kestrel_topic);
        let mut kestrel = kestrel_end_to_end(&kestrel_topic, samples, 128, true);

        // The same measurement with prefetch off. Running both in one process
        // is the only honest way to say what it bought: the cluster moves
        // enough between runs to swamp the difference otherwise.
        let plain_topic = topic("kn");
        create_topic(&plain_topic);
        let mut no_prefetch = kestrel_end_to_end(&plain_topic, samples, 128, false);

        let rd_topic = topic("re");
        create_topic(&rd_topic);
        let mut rdkafka = rdkafka_end_to_end(&rd_topic, samples, 128);

        for (label, p) in [("p50", 0.50), ("p99", 0.99), ("max", 1.0)] {
            let k = kestrel.percentile(p);
            let n = no_prefetch.percentile(p);
            let r = rdkafka.percentile(p);
            let marker = if k > r { "  <-- slower" } else { "" };
            println!(
                "{label:<6} {:>9.3} ms {:>10.3} ms {:>10.3} ms{marker}",
                k.as_secs_f64() * 1e3,
                n.as_secs_f64() * 1e3,
                r.as_secs_f64() * 1e3
            );
        }

        // ── pipelining ──────────────────────────────────────────────────────────
    }

    if want("pipeline") {
        println!("\nin-flight per connection            rec/s      vs 1 in flight");
        // **One record per enqueue**, deeply queued. This is the analogue of a
        // Java producer with batching on and no linger: the caller hands over
        // records one at a time and the client decides what travels together.
        // Comparing that against our *synchronous* one-record `send` would be
        // comparing a queue with a round trip.
        let one = Cell { batch: 1, value_bytes: 128, records: 200_000 };
        for window in [1usize, 100, 1_000] {
            let topic = topic("p1");
            create_topic(&topic);
            let rate = one.rate(kestrel_produce_pipelined(&topic, &one, window));
            println!("1 record/enqueue, window {window:>5}   {rate:>12.0}");
        }

        let cell = Cell { batch: 100, value_bytes: 128, records: 200_000 };
        let mut single = 0.0;
        for in_flight in [1usize, 2, 5, 10] {
            let topic = topic("pl");
            create_topic(&topic);
            let rate = cell.rate(kestrel_produce_pipelined(&topic, &cell, in_flight));
            if in_flight == 1 {
                single = rate;
            }
            println!(
                "{in_flight:>2} in flight                 {:>12.0}   {:>12.2}x",
                rate,
                rate / single
            );
        }

    // ── fetch sessions ──────────────────────────────────────────────────────
    }

    if want("idle") {
        println!("\nidle partitions (64, none busy)     polls/s   vs full fetch");
        let idle_topic = topic("idle");
        create_topic_with(&idle_topic, 64);
        let polls = 2_000;
        let full = polls as f64 / kestrel_idle_polls(&idle_topic, 64, polls, false).as_secs_f64();
        let incremental =
            polls as f64 / kestrel_idle_polls(&idle_topic, 64, polls, true).as_secs_f64();
        println!("full fetch                    {full:>12.0}          1.00x");
        println!(
            "incremental (KIP-227)         {incremental:>12.0}   {:>12.2}x",
            incremental / full
        );
    }

    // ── soak ────────────────────────────────────────────────────────────────
    if let Some(seconds) = std::env::var("KESTREL_SOAK_SECONDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        println!("\nsustained produce ({seconds}s)");
        let soak_topic = topic("soak");
        create_topic(&soak_topic);
        kestrel_soak(&soak_topic, seconds);
    }
}
