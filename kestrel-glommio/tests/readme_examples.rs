//! The README's examples, compiled.
//!
//! Documentation that does not build is worse than none: it is confidently
//! wrong. Two of these were wrong when first written — `scram_sha_256` for
//! `scram_sha256`, and a `GlommioTls::new` taking an argument it does not take
//! — and nothing would have caught that, because a README is not code.
//!
//! **Functions, not tests.** They need a broker to run, and an `#[ignore]`d
//! test still runs under `--ignored`, which the full suite uses — so these are
//! never-called functions instead. The compiler checks them; nothing executes
//! them.

use bytes::Bytes;
use kestrel_glommio::{
    Consumer, Credentials, Glommio, IsolationLevel, Producer, ProducerRecord, EARLIEST,
};

fn brokers() -> Vec<String> {
    vec!["localhost:9092".to_owned()]
}

#[allow(dead_code)]
fn readme_consume() {
    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("executor")
        .run(async {
            let mut consumer =
                Consumer::new(Glommio, &brokers(), "my-app", IsolationLevel::ReadCommitted)
                    .await
                    .expect("consumer");

            for partition in 0..8 {
                consumer.assign("events", partition, EARLIEST).await.expect("assign");
            }

            for group in consumer.poll().await.expect("fetch") {
                for record in group.iter() {
                    let value = record.value();
                    println!(
                        "{}-{} @{}: {:?}",
                        group.topic,
                        group.partition,
                        record.offset(),
                        value
                    );
                }
            }

            consumer.seek_to("events", 3, 42);
        });
}

#[allow(dead_code)]
fn readme_produce_and_transactions() {
    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("executor")
        .run(async {
            let mut producer = Producer::idempotent(Glommio, &brokers(), "my-app")
                .await
                .expect("producer");

            let records = vec![
                ProducerRecord::new(Some(Bytes::from("user-1")), Some(Bytes::from("hello"))),
                ProducerRecord::new(Some(Bytes::from("user-2")), Some(Bytes::from("world"))),
            ];
            producer.send_keyed("events", &records).await.expect("send");

            // Pipelined: enqueue encodes and sends nothing, flush collects.
            for _ in 0..3 {
                producer.enqueue("events", 0, &records).await.expect("enqueue");
            }
            let _written = producer.flush().await.expect("flush");

            let mut txn = Producer::transactional(Glommio, &brokers(), "my-app", "my-app-sink-0")
                .await
                .expect("transactional producer");
            txn.begin_transaction().expect("begin");
            txn.send_keyed("events", &records).await.expect("send");
            txn.commit_transaction().await.expect("commit");
        });
}

#[allow(dead_code)]
fn readme_sasl() {
    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("executor")
        .run(async {
            let mut cluster = kestrel_glommio::Cluster::connect(Glommio, &brokers(), "my-app")
                .await
                .expect("cluster");
            cluster.set_credentials(Credentials::scram_sha256("user", "pass"));
        });
}

/// The staged builder, which is the guided way in.
#[allow(dead_code)]
fn readme_builder() {
    use kestrel_glommio::StartOffset;
    use std::time::Duration;

    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("executor")
        .run(async {
            let _consumer = Consumer::builder(Glommio)
                .bootstrap(["localhost:9092"])
                .client_id("my-app")
                .assign_all("events", StartOffset::Earliest)
                .assign_range("shard", 0..4, StartOffset::Latest)
                .assign("late-events", 0, StartOffset::At(1_234))
                .max_wait(Duration::from_millis(100))
                .build()
                .await
                .expect("consumer");

            let _producer = Producer::builder(Glommio)
                .bootstrap(["localhost:9092"])
                .client_id("my-app")
                .transactional_id("my-app-sink-0")
                .compression(kestrel_glommio::CompressionCodec::Snappy)
                .max_in_flight(5)
                .build()
                .await
                .expect("producer");
        });
}

#[allow(dead_code)]
fn readme_groups() {
    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("executor")
        .run(async {
            let mut consumer =
                Consumer::new(Glommio, &brokers(), "my-app", IsolationLevel::ReadCommitted)
                    .await
                    .expect("consumer");
            consumer
                .subscribe(
                    "my-group",
                    vec!["events".into()],
                    Box::new(kestrel_core::group::CooperativeStickyAssignor),
                    EARLIEST,
                )
                .await
                .expect("subscribe");
            consumer.set_auto_commit(Some(std::time::Duration::from_secs(5)));
            consumer.set_group_timeouts(
                std::time::Duration::from_secs(30),
                std::time::Duration::from_secs(60),
            );

            for batch in consumer.poll().await.expect("poll") {
                for record in batch.iter() {
                    let _ = record.value();
                }
            }
        });
}

#[allow(dead_code)]
fn readme_exactly_once_with_a_group() {
    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("executor")
        .run(async {
            let mut consumer =
                Consumer::new(Glommio, &brokers(), "my-app", IsolationLevel::ReadCommitted)
                    .await
                    .expect("consumer");
            let mut producer =
                Producer::transactional(Glommio, &brokers(), "my-app", "my-app-sink-0")
                    .await
                    .expect("producer");
            let transformed = vec![ProducerRecord::new(None, Some(Bytes::from("out")))];

            let _records = consumer.poll().await.expect("poll");
            producer.begin_transaction().expect("begin");
            producer
                .send_keyed("output", &transformed)
                .await
                .expect("send");

            producer
                .send_offsets_to_transaction(
                    &consumer.positions(),
                    &consumer.group_metadata().unwrap(),
                )
                .await
                .expect("offsets");
            producer.commit_transaction().await.expect("commit");
        });
}

#[allow(dead_code)]
fn readme_accumulator() {
    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("executor")
        .run(async {
            let mut producer = Producer::idempotent(Glommio, &brokers(), "my-app")
                .await
                .expect("producer");
            producer.set_linger(std::time::Duration::from_millis(5));
            producer.set_batch_size(32 * 1024);

            for i in 0..10 {
                producer
                    .produce(
                        "events",
                        ProducerRecord::new(None, Some(Bytes::from(format!("{i}")))),
                    )
                    .await
                    .expect("produce");
            }
            producer.flush().await.expect("flush");

            if let Some(_deadline) = producer.linger_deadline() {
                producer.tick().await.expect("tick");
            }
        });
}

#[allow(dead_code)]
fn readme_pausing_and_offsets() {
    use kestrel_core::group::TopicPartition;

    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("executor")
        .run(async {
            let mut consumer =
                Consumer::new(Glommio, &brokers(), "my-app", IsolationLevel::ReadCommitted)
                    .await
                    .expect("consumer");
            let partitions = vec![TopicPartition::new("events", 0)];
            let when = 0i64;

            let p3 = TopicPartition::new("events", 3);
            consumer.pause(&[p3.clone()]);
            consumer.resume(&[p3]);

            let _ends = consumer.end_offsets(&partitions).await.expect("end");
            let _start = consumer
                .beginning_offsets(&partitions)
                .await
                .expect("beginning");
            let _at = consumer
                .offsets_for_times(&[(TopicPartition::new("events", 0), when)])
                .await
                .expect("times");
            let _lag = consumer.lag().await.expect("lag");
            let _done = consumer.committed(&partitions).await.expect("committed");
        });
}

#[allow(dead_code)]
fn readme_admin() {
    use kestrel_core::group::TopicPartition;
    use kestrel_glommio::{Admin, NewTopic};

    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("executor")
        .run(async {
            let mut admin = Admin::connect(Glommio, &brokers(), "my-tool")
                .await
                .expect("admin");

            admin
                .create_topics(&[
                    NewTopic::new("events", 8, 3).with_config("retention.ms", "604800000")
                ])
                .await
                .expect("create");
            admin
                .create_partitions("events", 16)
                .await
                .expect("expand");
            let _brokers = admin.describe_cluster().await.expect("cluster");
            let _config = admin
                .describe_topic_config("events")
                .await
                .expect("config");
            admin
                .delete_records(&[(TopicPartition::new("events", 0), 1_000)])
                .await
                .expect("trim");
            admin
                .delete_topics(&["events".to_owned()])
                .await
                .expect("delete");
        });
}

#[allow(dead_code)]
fn readme_topic_expansion() {
    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("executor")
        .run(async {
            let mut consumer =
                Consumer::new(Glommio, &brokers(), "my-app", IsolationLevel::ReadCommitted)
                    .await
                    .expect("consumer");
            consumer.set_metadata_max_age(std::time::Duration::from_secs(60));

            for (topic, before, after) in consumer.take_expansions() {
                eprintln!("{topic} grew from {before} to {after}");
            }
        });
}
