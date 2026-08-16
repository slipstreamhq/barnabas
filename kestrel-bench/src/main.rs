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
use kestrel_client::ProducerRecord;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

const PARTITIONS: i32 = 8;

fn broker() -> String {
    std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".to_owned())
}

fn topic(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("bench-{prefix}-{nanos}")
}

fn create_topic(name: &str) {
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
                    &[NewTopic::new(name, PARTITIONS, TopicReplication::Fixed(1))],
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
            kestrel_tokio::Producer::idempotent(kestrel_tokio::Tokio, &[broker()], "bench")
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

fn kestrel_latency(topic: &str, samples: usize, value_bytes: usize) -> Latency {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let mut producer =
            kestrel_tokio::Producer::idempotent(kestrel_tokio::Tokio, &[broker()], "bench")
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
                &[broker()],
                "bench",
                kestrel_core::IsolationLevel::ReadCommitted,
            )
            .await
            .expect("consumer");
            for partition in 0..PARTITIONS {
                consumer
                    .add(&topic, partition, kestrel_glommio::EARLIEST)
                    .await
                    .expect("assign");
            }
            consumer.set_max_wait(Duration::from_millis(100));

            let start = Instant::now();
            let mut seen = 0;
            while seen < expect {
                let groups = consumer.fetch().await.expect("fetch");
                let got: usize = groups.iter().map(|g| g.records.len()).sum();
                if got == 0 {
                    break;
                }
                seen += got;
            }
            assert_eq!(seen, expect, "kestrel consumed {seen} of {expect}");
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
    let per_core = cell.records / cores;
    let partitions_per_core = (PARTITIONS as usize).div_ceil(cores);

    let start = Instant::now();
    let handles: Vec<_> = (0..cores)
        .map(|core| {
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
                            &[broker()],
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
                        let first = (core * partitions_per_core) as i32;
                        let partition = first.min(PARTITIONS - 1);
                        for _ in 0..(per_core / batch_size) {
                            producer.send(&topic, partition, &batch).await.expect("send");
                        }
                    });
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("core finished");
    }
    start.elapsed()
}

fn main() {
    println!("{PARTITIONS} partitions, acks=all, idempotent, one local broker\n");

    println!("produce throughput            kestrel rec/s   rdkafka rec/s     ratio");
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

        let ratio = kestrel / rdkafka;
        let marker = if ratio < 1.0 { "  <-- slower" } else { "" };
        println!(
            "batch {:>5}, {:>5} B      {:>12.0}   {:>12.0}   {:>6.2}x{marker}",
            cell.batch, cell.value_bytes, kestrel, rdkafka, ratio
        );
    }

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
    println!("\nproduce, N cores (glommio)          rec/s      per core   scaling");
    let cell = Cell { batch: 1_000, value_bytes: 128, records: 400_000 };
    let mut single_core = 0.0;
    for cores in [1usize, 2, 4, 8] {
        let topic = topic("mc");
        create_topic(&topic);
        let rate = cell.rate(kestrel_produce_many_cores(&topic, cores, &cell));
        if cores == 1 {
            single_core = rate;
        }
        println!(
            "{cores:>2} cores                     {:>12.0}  {:>12.0}   {:>6.2}x",
            rate,
            rate / cores as f64,
            rate / single_core
        );
    }

    println!("\nconsume throughput            kestrel rec/s   rdkafka rec/s     ratio");
    let cell = Cell { batch: 1_000, value_bytes: 128, records: 200_000 };

    let kestrel_topic = topic("kc");
    create_topic(&kestrel_topic);
    kestrel_produce(&kestrel_topic, &cell);
    let kestrel_rate = cell.rate(kestrel_consume(&kestrel_topic, cell.records));

    let rd_topic = topic("rc");
    create_topic(&rd_topic);
    rdkafka_produce(&rd_topic, &cell);
    let rdkafka_rate = cell.rate(rdkafka_consume(&rd_topic, cell.records));

    let ratio = kestrel_rate / rdkafka_rate;
    let marker = if ratio < 1.0 { "  <-- slower" } else { "" };
    println!(
        "{:>5} B                    {:>12.0}   {:>12.0}   {:>6.2}x{marker}",
        cell.value_bytes, kestrel_rate, rdkafka_rate, ratio
    );
}
