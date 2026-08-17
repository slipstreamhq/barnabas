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
