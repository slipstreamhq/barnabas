//! Consumer tests against a real Kafka broker.
//!
//! `#[ignore]`d: they need a broker on `localhost:9092`. The unit tests in
//! `kestrel-core` pin the filtering rules deterministically; these check the
//! rules were derived from what a broker actually sends, which is the half a
//! hand-written model cannot verify.
//!
//! ```sh
//! podman run -d --name kestrel-test -p 9092:9092 \
//!   -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
//!   -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
//!   -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
//!   -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
//!   -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
//!   -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
//!   -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
//!   -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
//!   -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
//!   docker.io/apache/kafka:3.9.0
//! cargo test -p kestrel-glommio -- --ignored --test-threads=1
//! ```
//!
//! The three replication-factor settings are required: `__transaction_state`
//! defaults to RF 3, which a single-node broker cannot create.
//!
//! **The producer here is test scaffolding, not library code.** `kestrel` has
//! no producer until P2; this file drives `kafka-protocol` directly to put data
//! on the broker, including an aborted transaction, so the consumer has
//! something real to filter.

mod producer;

use std::time::Duration;

use kestrel_core::IsolationLevel;
use kestrel_glommio::{Consumer, EARLIEST};

use producer::TestProducer;

/// `KAFKA_BOOTSTRAP` so these can run against the three-broker cluster in
/// `cluster.sh` as well as a single broker. The multi-broker configuration is
/// the one that exercises concurrent per-broker requests, so it is worth being
/// able to point at it.
fn bootstrap() -> Vec<String> {
    std::env::var("KAFKA_BOOTSTRAP")
        .unwrap_or_else(|_| "127.0.0.1:9092".to_owned())
        .split(',')
        .map(|s| s.trim().to_owned())
        .collect()
}

fn broker() -> String {
    bootstrap().first().expect("a bootstrap address").clone()
}

/// A topic per test, so tests neither see each other's records nor depend on
/// run order.
fn unique_topic(prefix: &str) -> String {
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

#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn plain_records_round_trip() {
    run(|| async {
        let topic = unique_topic("plain");
        let mut prod = TestProducer::connect(&broker(), &topic).await;
        prod.create_topic().await;
        prod.produce_plain(3).await;

        let mut consumer = Consumer::assign(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "kestrel-test",
            &topic,
            0,
            EARLIEST,
            IsolationLevel::ReadCommitted,
        )
        .await
        .expect("assign");

        let records = fetch_until(&mut consumer, 3).await;
        let values: Vec<String> = records
            .iter()
            .map(|r| String::from_utf8_lossy(r.value.as_ref().unwrap()).into_owned())
            .collect();
        assert_eq!(values, vec!["v0", "v1", "v2"]);
        assert_eq!(consumer.position(), 3);
    });
}

/// **The P0 bug, as a regression test.** An aborted transaction must be
/// invisible under READ_COMMITTED — and the broker does not do that for us.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn an_aborted_transaction_is_invisible() {
    run(|| async {
        let topic = unique_topic("aborted");
        let mut prod = TestProducer::connect(&broker(), &topic).await;
        prod.create_topic().await;
        prod.init_transactions().await;

        prod.begin().await;
        prod.produce_txn(3).await;
        prod.end(false).await;

        let mut consumer = Consumer::assign(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "kestrel-test",
            &topic,
            0,
            EARLIEST,
            IsolationLevel::ReadCommitted,
        )
        .await
        .expect("assign");

        // Two fetches: the first may return nothing but must still advance past
        // the aborted range, which is the property that stops a consumer
        // looping on a partition of only-aborted data.
        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.extend(
            consumer
                .fetch()
                .await
                .expect("fetch")
                .into_iter()
                .flat_map(|group| group.records),
        );
        }
        assert!(
            seen.is_empty(),
            "aborted records reached the caller: {seen:?}"
        );
        assert!(
            consumer.position() > 0,
            "position must advance past a fully-aborted fetch, got {}",
            consumer.position()
        );
    });
}

/// The case a naive filter gets wrong: the same producer aborts, then commits.
/// Only the committed records may survive.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn a_commit_after_an_abort_survives() {
    run(|| async {
        let topic = unique_topic("abort-then-commit");
        let mut prod = TestProducer::connect(&broker(), &topic).await;
        prod.create_topic().await;
        prod.init_transactions().await;

        prod.begin().await;
        prod.produce_txn(3).await; // aborted
        prod.end(false).await;

        prod.begin().await;
        prod.produce_txn(2).await; // committed
        prod.end(true).await;

        let mut consumer = Consumer::assign(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "kestrel-test",
            &topic,
            0,
            EARLIEST,
            IsolationLevel::ReadCommitted,
        )
        .await
        .expect("assign");

        let records = fetch_until(&mut consumer, 2).await;
        assert_eq!(
            records.len(),
            2,
            "expected exactly the committed transaction's records"
        );
    });
}

/// READ_UNCOMMITTED means what it says.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn read_uncommitted_sees_aborted_records() {
    run(|| async {
        let topic = unique_topic("uncommitted");
        let mut prod = TestProducer::connect(&broker(), &topic).await;
        prod.create_topic().await;
        prod.init_transactions().await;

        prod.begin().await;
        prod.produce_txn(3).await;
        prod.end(false).await;

        let mut consumer = Consumer::assign(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "kestrel-test",
            &topic,
            0,
            EARLIEST,
            IsolationLevel::ReadUncommitted,
        )
        .await
        .expect("assign");

        let records = fetch_until(&mut consumer, 3).await;
        assert_eq!(records.len(), 3, "aborted records must be visible here");
    });
}

/// Seek is how a restored checkpoint is applied, and the caller owns the
/// offset — there is no committed position on the broker to fall back to.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn seek_positions_the_next_fetch() {
    run(|| async {
        let topic = unique_topic("seek");
        let mut prod = TestProducer::connect(&broker(), &topic).await;
        prod.create_topic().await;
        prod.produce_plain(5).await;

        let mut consumer = Consumer::assign(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "kestrel-test",
            &topic,
            0,
            EARLIEST,
            IsolationLevel::ReadCommitted,
        )
        .await
        .expect("assign");

        consumer.seek(3);
        let records = fetch_until(&mut consumer, 2).await;
        let values: Vec<String> = records
            .iter()
            .map(|r| String::from_utf8_lossy(r.value.as_ref().unwrap()).into_owned())
            .collect();
        assert_eq!(values, vec!["v3", "v4"]);
    });
}

/// Fetches go to the **leader**, which is discovered from metadata rather than
/// assumed to be the bootstrap broker.
///
/// A single-node cluster cannot prove routing chose the right broker — there is
/// only one — but it does prove the path is taken: the consumer resolves a
/// leader address from metadata, connects to it, and fetches over that
/// connection.
///
/// Pointed at `cluster.sh`'s three brokers it proves the stronger thing, that
/// the topic's partitions resolve to more than one leader.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn fetches_are_routed_to_the_partition_leader() {
    run(|| async {
        let topic = unique_topic("routing");
        let mut prod = TestProducer::connect(&broker(), &topic).await;
        prod.create_topic_with_partitions(8).await;
        prod.produce_plain(1).await;

        let mut consumer = Consumer::assign(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "kestrel-test",
            &topic,
            0,
            EARLIEST,
            IsolationLevel::ReadCommitted,
        )
        .await
        .expect("assign");

        let leader = consumer
            .metadata_leader(&topic, 0)
            .expect("a leader in the cluster map");
        assert!(
            leader.contains(':') && leader.rsplit(':').next().unwrap().parse::<u16>().is_ok(),
            "leader address came from metadata: {leader}"
        );

        // **On a real cluster, prove routing rather than assume it.** With more
        // than one broker the topic's partitions are led by more than one of
        // them, so the cluster map must hold more than one distinct address —
        // something a consumer that just reused its bootstrap connection could
        // never produce. Against a single broker there is nothing to prove and
        // the check is skipped.
        if bootstrap().len() > 1 {
            let mut leaders: Vec<String> = (0..8)
                .filter_map(|p| consumer.metadata_leader(&topic, p))
                .collect();
            leaders.sort_unstable();
            leaders.dedup();
            assert!(
                leaders.len() > 1,
                "partitions on a multi-broker cluster resolved to one leader: {leaders:?}"
            );
        }

        let records = fetch_until(&mut consumer, 1).await;
        assert_eq!(records.len(), 1);
    });
}

/// Fetch until `want` records have arrived or the attempts run out. A single
/// fetch is allowed to return nothing — an empty fetch is normal, not a
/// failure — so tests that assert on content must poll.
/// **Prefetch must not change what a consumer sees**, only when the request for
/// it left. The same topic read both ways must give the same records in the
/// same order.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn prefetch_returns_the_same_records() {
    run(|| async {
        let topic = unique_topic("prefetch");
        let mut prod = TestProducer::connect(&broker(), &topic).await;
        prod.create_topic().await;
        prod.produce_plain(20).await;

        let mut values = Vec::new();
        for prefetch in [true, false] {
            let mut consumer = Consumer::assign(
                kestrel_glommio::Glommio,
                &bootstrap(),
                "kestrel-test",
                &topic,
                0,
                EARLIEST,
                IsolationLevel::ReadCommitted,
            )
            .await
            .expect("assign");
            consumer.set_prefetch(prefetch);

            let records = fetch_until(&mut consumer, 20).await;
            values.push(
                records
                    .iter()
                    .map(|r| String::from_utf8_lossy(r.value.as_ref().unwrap()).into_owned())
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(values[0].len(), 20, "prefetching consumer lost records");
        assert_eq!(values[0], values[1], "prefetch changed what was delivered");
    });
}

/// **A seek must beat the fetch already in flight.**
///
/// With prefetch on, the request for the next poll goes out before the caller
/// gets a chance to seek. That outstanding answer is to the old position, and
/// returning it would silently replay or skip records — so it has to be
/// discarded rather than decoded.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn a_seek_discards_the_fetch_already_in_flight() {
    run(|| async {
        let topic = unique_topic("seek-prefetch");
        let mut prod = TestProducer::connect(&broker(), &topic).await;
        prod.create_topic().await;
        prod.produce_plain(10).await;

        let mut consumer = Consumer::assign(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "kestrel-test",
            &topic,
            0,
            EARLIEST,
            IsolationLevel::ReadCommitted,
        )
        .await
        .expect("assign");
        consumer.set_max_wait(Duration::from_millis(200));

        // Read something, which also puts the next fetch in flight.
        let first = fetch_until(&mut consumer, 1).await;
        assert!(!first.is_empty());

        // Now rewind. The in-flight fetch is asking from wherever the consumer
        // had reached, not from 0.
        consumer.seek(0);
        let after = fetch_until(&mut consumer, 10).await;

        let values: Vec<String> = after
            .iter()
            .map(|r| String::from_utf8_lossy(r.value.as_ref().unwrap()).into_owned())
            .collect();
        assert_eq!(
            values.first().map(String::as_str),
            Some("v0"),
            "seek did not take effect; got {values:?}"
        );
        assert_eq!(consumer.position(), 10);
    });
}

async fn fetch_until(
    consumer: &mut Consumer,
    want: usize,
) -> Vec<kafka_protocol::records::Record> {
    consumer.set_max_wait(Duration::from_millis(200));
    let mut out = Vec::new();
    for _ in 0..25 {
        out.extend(
            consumer
                .fetch()
                .await
                .expect("fetch")
                .into_iter()
                .flat_map(|group| group.records),
        );
        if out.len() >= want {
            break;
        }
    }
    out
}
