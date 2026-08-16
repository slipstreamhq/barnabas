//! Throughput against a real broker, for `kestrel` and for `rdkafka`.
//!
//! # What is being compared, precisely
//!
//! **`rdkafka` batches internally; `kestrel` batches at the call site.** That is
//! an API difference, not a performance one, and comparing a per-record
//! `kestrel` loop against librdkafka's accumulator would measure the difference
//! between the two designs' *defaults* rather than their speed. So both are
//! given the same work in the same shape: a fixed number of records, produced
//! in batches of `BATCH`, with `acks=all` on both sides.
//!
//! Every number here is one process against one local broker over loopback.
//! That deliberately excludes the thing a per-core client is best at — many
//! cores each owning partitions — so treat these as a floor, not a ceiling.
//!
//! # Running it
//!
//! ```sh
//! podman run -d --name kafka-bench -p 9092:9092 ... apache/kafka:3.9.0
//! cargo run -p kestrel-bench --release
//! ```
//!
//! Release matters: a debug build measures the compiler, not the client.

use std::time::{Duration, Instant};

use bytes::Bytes;
use kestrel_client::ProducerRecord;

const RECORDS: usize = 200_000;
const BATCH: usize = 1_000;
const VALUE_BYTES: usize = 256;
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

fn payload() -> Bytes {
    Bytes::from(vec![b'x'; VALUE_BYTES])
}

/// Records per second, and the megabytes that implies.
struct Rate {
    label: &'static str,
    records: usize,
    elapsed: Duration,
}

impl Rate {
    /// A rate computed over fewer records than were asked for is not a rate,
    /// it is a truncated run — so say so rather than divide by a short count.
    fn report_expecting(&self, expected: usize) {
        assert_eq!(
            self.records, expected,
            "{} handled {} of {expected} records; the number below would be a lie",
            self.label, self.records
        );
        self.report();
    }

    fn report(&self) {
        let per_second = self.records as f64 / self.elapsed.as_secs_f64();
        let mb = (self.records * VALUE_BYTES) as f64 / (1024.0 * 1024.0);
        let mb_per_second = mb / self.elapsed.as_secs_f64();
        println!(
            "{:<28} {:>10.0} rec/s  {:>8.1} MiB/s  ({:.2}s)",
            self.label,
            per_second,
            mb_per_second,
            self.elapsed.as_secs_f64()
        );
    }
}

// ── setup, done with rdkafka's admin client so neither side is favoured ──────

fn create_topic(name: &str) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::config::ClientConfig;

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
            tokio::time::sleep(Duration::from_millis(500)).await;
        });
}

// ── kestrel ──────────────────────────────────────────────────────────────────

fn kestrel_produce_tokio(topic: &str) -> Rate {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let mut producer =
            kestrel_tokio::Producer::idempotent(kestrel_tokio::Tokio, &[broker()], "bench")
                .await
                .expect("producer");

        let batch: Vec<ProducerRecord> = (0..BATCH)
            .map(|i| {
                ProducerRecord::new(Some(Bytes::from(format!("k{i}"))), Some(payload()))
            })
            .collect();

        let start = Instant::now();
        for _ in 0..(RECORDS / BATCH) {
            producer.send_keyed(topic, &batch).await.expect("send");
        }
        Rate {
            label: "kestrel produce (tokio)",
            records: RECORDS,
            elapsed: start.elapsed(),
        }
    })
}

fn kestrel_produce_glommio(topic: &str) -> Rate {
    let topic = topic.to_owned();
    glommio::LocalExecutorBuilder::default()
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

            let batch: Vec<ProducerRecord> = (0..BATCH)
                .map(|i| {
                    ProducerRecord::new(Some(Bytes::from(format!("k{i}"))), Some(payload()))
                })
                .collect();

            let start = Instant::now();
            for _ in 0..(RECORDS / BATCH) {
                producer.send_keyed(&topic, &batch).await.expect("send");
            }
            Rate {
                label: "kestrel produce (glommio)",
                records: RECORDS,
                elapsed: start.elapsed(),
            }
        })
}

fn kestrel_consume_glommio(topic: &str, expect: usize) -> Rate {
    let topic = topic.to_owned();
    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("executor")
        .run(async move {
            // One consumer per partition: the shape the client offers today,
            // and the reason multi-partition fetch is the next thing to build.
            let mut consumers = Vec::new();
            for partition in 0..PARTITIONS {
                let mut consumer = kestrel_glommio::Consumer::assign(
                    kestrel_glommio::Glommio,
                    &[broker()],
                    "bench",
                    &topic,
                    partition,
                    kestrel_glommio::EARLIEST,
                    kestrel_core::IsolationLevel::ReadCommitted,
                )
                .await
                .expect("assign");
                consumer.set_max_wait(Duration::from_millis(100));
                consumers.push(consumer);
            }

            let start = Instant::now();
            let mut seen = 0;
            let deadline = Instant::now() + Duration::from_secs(120);
            while seen < expect && Instant::now() < deadline {
                let mut progressed = false;
                for consumer in &mut consumers {
                    let records = consumer.fetch().await.expect("fetch");
                    if !records.is_empty() {
                        progressed = true;
                        seen += records.len();
                    }
                }
                if !progressed {
                    break;
                }
            }
            Rate {
                label: "kestrel consume (glommio)",
                records: seen,
                elapsed: start.elapsed(),
            }
        })
}

// ── rdkafka, the incumbent ───────────────────────────────────────────────────

fn rdkafka_produce(topic: &str) -> Rate {
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        // librdkafka is given its best configuration, not its defaults. Its
        // 5 ms linger costs a flush's worth of waiting per batch under a
        // chunked await, which would make this a measurement of a default
        // rather than of the client.
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", broker())
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("linger.ms", "0")
            .set("batch.num.messages", "10000")
            .set("batch.size", "1048576")
            .set("queue.buffering.max.messages", "1000000")
            .set("queue.buffering.max.kbytes", "1048576")
            .create()
            .expect("producer");

        let value = payload();
        let start = Instant::now();
        // Fire a batch's worth concurrently, then await them — librdkafka
        // accumulates internally, so awaiting each in turn would measure
        // round-trip latency rather than throughput.
        // Keys are precomputed so neither side pays for formatting inside the
        // measured loop; `kestrel`'s batch is built once for the same reason.
        //
        // Everything is enqueued before anything is awaited, which is
        // librdkafka's best case: its accumulator gets the whole stream and
        // decides its own batch boundaries, rather than being flushed every
        // `BATCH` records by our awaiting.
        let keys: Vec<String> = (0..BATCH).map(|i| format!("k{i}")).collect();
        let mut inflight = Vec::with_capacity(RECORDS);
        for chunk in 0..(RECORDS / BATCH) {
            for key in &keys {
                let _ = chunk;
                inflight.push(
                    producer
                        .send_result(FutureRecord::to(topic).key(key).payload(&value[..]))
                        .expect("enqueue"),
                );
            }
        }
        for delivery in inflight {
            delivery.await.expect("delivery").expect("broker ack");
        }
        Rate {
            label: "rdkafka produce (tokio)",
            records: RECORDS,
            elapsed: start.elapsed(),
        }
    })
}

fn rdkafka_consume(topic: &str, expect: usize) -> Rate {
    use rdkafka::config::ClientConfig;
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
        Rate {
            label: "rdkafka consume (tokio)",
            records: seen,
            elapsed: start.elapsed(),
        }
    })
}

fn main() {
    println!(
        "{RECORDS} records x {VALUE_BYTES} B, batches of {BATCH}, {PARTITIONS} partitions, acks=all\n"
    );

    let kestrel_topic = topic("kestrel");
    create_topic(&kestrel_topic);
    kestrel_produce_tokio(&kestrel_topic).report_expecting(RECORDS);

    let glommio_topic = topic("kestrel-glommio");
    create_topic(&glommio_topic);
    kestrel_produce_glommio(&glommio_topic).report_expecting(RECORDS);

    let rdkafka_topic = topic("rdkafka");
    create_topic(&rdkafka_topic);
    rdkafka_produce(&rdkafka_topic).report_expecting(RECORDS);

    println!();
    kestrel_consume_glommio(&glommio_topic, RECORDS).report_expecting(RECORDS);
    rdkafka_consume(&rdkafka_topic, RECORDS).report_expecting(RECORDS);
}
