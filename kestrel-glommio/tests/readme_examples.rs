//! The README's examples, compiled.
//!
//! Documentation that does not build is worse than none: it is confidently
//! wrong. Two of these were wrong when first written — `scram_sha_256` for
//! `scram_sha256`, and a `GlommioTls::new` taking an argument it does not take
//! — and nothing would have caught that, because a README is not code.
//!
//! `#[ignore]`d rather than `#[test]`ed: they need a broker to *run*. Compiling
//! them is the point, and `cargo test --no-run` does that.

use bytes::Bytes;
use kestrel_glommio::{
    Consumer, Credentials, Glommio, IsolationLevel, Producer, ProducerRecord, EARLIEST,
};

fn brokers() -> Vec<String> {
    vec!["localhost:9092".to_owned()]
}

#[test]
#[ignore = "compiled, not run: needs a broker"]
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
                consumer.add("events", partition, EARLIEST).await.expect("assign");
            }

            for group in consumer.fetch().await.expect("fetch") {
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

#[test]
#[ignore = "compiled, not run: needs a broker"]
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

#[test]
#[ignore = "compiled, not run: needs a broker"]
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
