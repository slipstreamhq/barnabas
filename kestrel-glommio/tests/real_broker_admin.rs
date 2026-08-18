//! The admin client against a real broker.
//!
//! These are the first tests in this workspace that do **not** need the
//! `TestProducer` scaffolding to make a topic, which was the point of writing
//! them: until now every test created topics through a hand-rolled request
//! because the client could not.

use std::time::Duration;

use kestrel_glommio::{Admin, NewTopic};

fn bootstrap() -> Vec<String> {
    std::env::var("KAFKA_BOOTSTRAP")
        .unwrap_or_else(|_| "127.0.0.1:9092".to_owned())
        .split(',')
        .map(|s| s.trim().to_owned())
        .collect()
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("kestrel-{prefix}-{nanos}")
}

fn run<F: std::future::Future<Output = ()>>(fut: impl FnOnce() -> F + 'static) {
    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("build executor")
        .run(async move { fut().await });
}

async fn admin() -> Admin {
    Admin::connect(kestrel_glommio::Glommio, &bootstrap(), "kestrel-admin")
        .await
        .expect("admin")
}

/// Create a topic, read back its shape and configuration, expand it, delete it.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn a_topic_can_be_created_described_expanded_and_deleted() {
    run(|| async {
        let topic = unique("admin");
        let mut admin = admin().await;

        admin
            .create_topics(&[NewTopic::new(&topic, 2, 1).with_config("retention.ms", "86400000")])
            .await
            .expect("create");

        let config = admin
            .describe_topic_config(&topic)
            .await
            .expect("describe config");
        assert_eq!(
            config.get("retention.ms").and_then(Option::as_deref),
            Some("86400000"),
            "the config we set should come back"
        );
        assert!(
            config.contains_key("cleanup.policy"),
            "defaults are reported too, not only what we set"
        );

        admin.cluster().refresh_metadata(&topic).await.expect("metadata");
        assert_eq!(admin.cluster().metadata().partition_count(&topic), 2);

        // `count` is the new total, not a delta.
        admin.create_partitions(&topic, 4).await.expect("expand");
        for _ in 0..40 {
            admin.cluster().refresh_metadata(&topic).await.expect("metadata");
            if admin.cluster().metadata().partition_count(&topic) == 4 {
                break;
            }
            glommio::timer::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(admin.cluster().metadata().partition_count(&topic), 4);

        admin
            .delete_topics(&[topic.clone()])
            .await
            .expect("delete");

        // Creating it again is proof the delete took: the controller refuses a
        // duplicate name.
        for attempt in 0..40 {
            match admin.create_topics(&[NewTopic::new(&topic, 1, 1)]).await {
                Ok(()) => break,
                Err(e) if attempt == 39 => panic!("recreate after delete: {e}"),
                Err(_) => glommio::timer::sleep(Duration::from_millis(100)).await,
            }
        }
        admin.delete_topics(&[topic]).await.expect("delete again");
    });
}

/// Creating a topic that exists is an error, not a silent success.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn creating_an_existing_topic_is_an_error() {
    run(|| async {
        let topic = unique("admin-dup");
        let mut admin = admin().await;
        admin
            .create_topics(&[NewTopic::new(&topic, 1, 1)])
            .await
            .expect("create");

        let again = admin.create_topics(&[NewTopic::new(&topic, 1, 1)]).await;
        assert!(again.is_err(), "the second create must not be silent");

        admin.delete_topics(&[topic]).await.expect("delete");
    });
}

/// The cluster describes itself, and exactly one broker is the controller.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn describe_cluster_names_one_controller() {
    run(|| async {
        let mut admin = admin().await;
        let brokers = admin.describe_cluster().await.expect("describe");
        assert!(!brokers.is_empty(), "a cluster has brokers");
        assert_eq!(
            brokers.iter().filter(|b| b.is_controller).count(),
            1,
            "exactly one controller: {brokers:?}"
        );
    });
}

/// `delete_records` moves the log start, which is what makes
/// `beginning_offsets` more than a constant zero.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn deleting_records_moves_the_log_start() {
    run(|| async {
        let topic = unique("admin-trim");
        let mut admin = admin().await;
        admin
            .create_topics(&[NewTopic::new(&topic, 1, 1)])
            .await
            .expect("create");

        let mut producer = kestrel_glommio::Producer::idempotent(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "kestrel-admin-test",
        )
        .await
        .expect("producer");
        let records: Vec<_> = (0..5)
            .map(|i| {
                kestrel_glommio::ProducerRecord::new(
                    None,
                    Some(bytes::Bytes::from(format!("r{i}"))),
                )
            })
            .collect();
        producer.send(&topic, 0, &records).await.expect("send");

        let p0 = kestrel_core::group::TopicPartition::new(topic.clone(), 0);
        let watermarks = admin
            .delete_records(&[(p0.clone(), 3)])
            .await
            .expect("delete records");
        assert_eq!(
            watermarks.get(&p0),
            Some(&3),
            "the log now starts at 3: {watermarks:?}"
        );

        let mut consumer = kestrel_glommio::Consumer::new(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "kestrel-admin-test",
            kestrel_core::IsolationLevel::ReadUncommitted,
        )
        .await
        .expect("consumer");
        let begins = consumer
            .beginning_offsets(std::slice::from_ref(&p0))
            .await
            .expect("beginning");
        assert_eq!(
            begins.get(&p0),
            Some(&3),
            "beginning_offsets is not a constant zero"
        );

        admin.delete_topics(&[topic]).await.expect("delete");
    });
}
