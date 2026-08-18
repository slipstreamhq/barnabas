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
            .delete_topics(std::slice::from_ref(&topic))
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

// ── topic expansion ────────────────────────────────────────────────────────

/// **A manually assigned consumer is told a topic grew, and not extended.**
///
/// The silent failure this prevents: records land on partitions nobody reads,
/// nothing errors, and `lag` looks perfect because it only covers what is
/// assigned.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn an_assigned_consumer_is_told_when_a_topic_grows() {
    run(|| async {
        let topic = unique("expand-assign");
        let mut admin = admin().await;
        admin
            .create_topics(&[NewTopic::new(&topic, 1, 1)])
            .await
            .expect("create");

        let mut consumer = kestrel_glommio::Consumer::for_partition(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "kestrel-expand",
            &topic,
            0,
            kestrel_glommio::EARLIEST,
            kestrel_core::IsolationLevel::ReadUncommitted,
        )
        .await
        .expect("assign");
        consumer.set_max_wait(Duration::from_millis(50));
        // Every poll re-reads, so the test does not wait five minutes.
        consumer.set_metadata_max_age(Duration::ZERO);

        consumer.poll().await.expect("poll");
        assert!(
            consumer.take_expansions().is_empty(),
            "nothing has grown yet"
        );

        admin.create_partitions(&topic, 3).await.expect("expand");

        let mut told = Vec::new();
        for _ in 0..40 {
            consumer.poll().await.expect("poll");
            told = consumer.take_expansions();
            if !told.is_empty() {
                break;
            }
            glommio::timer::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(told, vec![(topic.clone(), 1, 3)], "told once, with both counts");
        assert_eq!(
            consumer.assignments().count(),
            1,
            "a manual assignment is not extended behind the caller's back"
        );
        assert!(
            consumer.take_expansions().is_empty(),
            "reporting drains it"
        );

        admin.delete_topics(std::slice::from_ref(&topic)).await.expect("delete");
    });
}

/// A subscribed consumer rejoins and is assigned the new partitions, without
/// anything else happening in the group.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn a_group_picks_up_new_partitions() {
    run(|| async {
        let topic = unique("expand-group");
        let group = unique("expand-groupg");
        let mut admin = admin().await;
        admin
            .create_topics(&[NewTopic::new(&topic, 2, 1)])
            .await
            .expect("create");

        let mut consumer = kestrel_glommio::Consumer::new(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "kestrel-expand-group",
            kestrel_core::IsolationLevel::ReadUncommitted,
        )
        .await
        .expect("consumer");
        consumer.set_max_wait(Duration::from_millis(50));
        consumer.set_metadata_max_age(Duration::ZERO);
        consumer
            .subscribe(
                &group,
                vec![topic.clone()],
                Box::new(kestrel_core::group::RangeAssignor),
                kestrel_glommio::EARLIEST,
            )
            .await
            .expect("subscribe");

        for _ in 0..100 {
            consumer.poll().await.expect("poll");
            if consumer.assignments().count() == 2 {
                break;
            }
        }
        assert_eq!(consumer.assignments().count(), 2, "both to start with");

        admin.create_partitions(&topic, 5).await.expect("expand");

        for _ in 0..100 {
            consumer.poll().await.expect("poll");
            if consumer.assignments().count() == 5 {
                break;
            }
            glommio::timer::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            consumer.assignments().count(),
            5,
            "the group rejoined and the leader assigned the new partitions"
        );
        assert!(
            consumer.take_expansions().is_empty(),
            "a subscribed consumer rebalances instead of reporting"
        );

        consumer.unsubscribe().await.expect("leave");
        admin.delete_topics(std::slice::from_ref(&topic)).await.expect("delete");
    });
}

/// A producer with an expanded topic starts using the new partitions rather
/// than hashing keys against a count that no longer exists.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn a_producer_places_keys_against_the_new_count() {
    run(|| async {
        let topic = unique("expand-produce");
        let mut admin = admin().await;
        admin
            .create_topics(&[NewTopic::new(&topic, 1, 1)])
            .await
            .expect("create");

        let mut producer = kestrel_glommio::Producer::idempotent(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "kestrel-expand-produce",
        )
        .await
        .expect("producer");
        producer.set_metadata_max_age(Duration::ZERO);

        for i in 0..8 {
            producer
                .produce(
                    &topic,
                    kestrel_glommio::ProducerRecord::new(
                        Some(bytes::Bytes::from(format!("k{i}"))),
                        Some(bytes::Bytes::from("v")),
                    ),
                )
                .await
                .expect("produce");
        }
        producer.flush().await.expect("flush");

        admin.create_partitions(&topic, 4).await.expect("expand");
        glommio::timer::sleep(Duration::from_millis(200)).await;

        let mut used = std::collections::BTreeSet::new();
        for i in 0..40 {
            let partition = producer
                .partition_for(&topic, Some(format!("k{i}").as_bytes()))
                .await
                .expect("partition for");
            used.insert(partition);
        }
        assert!(
            used.len() > 1,
            "keys should spread across the new partitions, got {used:?}"
        );

        admin.delete_topics(std::slice::from_ref(&topic)).await.expect("delete");
    });
}
