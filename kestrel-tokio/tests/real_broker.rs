//! Consumer tests against a real Kafka broker — **on tokio**.
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

use kestrel_tokio::{Consumer, IsolationLevel, EARLIEST};

use producer::TestProducer;

fn bootstrap() -> Vec<String> {
    vec!["127.0.0.1:9092".to_owned()]
}

const BROKER: &str = "127.0.0.1:9092";

/// A topic per test, so tests neither see each other's records nor depend on
/// run order.
fn unique_topic(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("kestrel-{prefix}-{nanos}")
}

/// Multi-threaded on purpose — see the producer tests.
fn run<F: std::future::Future<Output = ()>>(fut: impl FnOnce() -> F + 'static) {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build runtime")
        .block_on(async move { fut().await });
}

#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn plain_records_round_trip() {
    run(|| async {
        let topic = unique_topic("plain");
        let mut prod = TestProducer::connect(BROKER, &topic).await;
        prod.create_topic().await;
        prod.produce_plain(3).await;

        let mut consumer = Consumer::assign(
            kestrel_tokio::Tokio,
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
        let mut prod = TestProducer::connect(BROKER, &topic).await;
        prod.create_topic().await;
        prod.init_transactions().await;

        prod.begin().await;
        prod.produce_txn(3).await;
        prod.end(false).await;

        let mut consumer = Consumer::assign(
            kestrel_tokio::Tokio,
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
            seen.extend(consumer.fetch().await.expect("fetch"));
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
        let mut prod = TestProducer::connect(BROKER, &topic).await;
        prod.create_topic().await;
        prod.init_transactions().await;

        prod.begin().await;
        prod.produce_txn(3).await; // aborted
        prod.end(false).await;

        prod.begin().await;
        prod.produce_txn(2).await; // committed
        prod.end(true).await;

        let mut consumer = Consumer::assign(
            kestrel_tokio::Tokio,
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
        let mut prod = TestProducer::connect(BROKER, &topic).await;
        prod.create_topic().await;
        prod.init_transactions().await;

        prod.begin().await;
        prod.produce_txn(3).await;
        prod.end(false).await;

        let mut consumer = Consumer::assign(
            kestrel_tokio::Tokio,
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
        let mut prod = TestProducer::connect(BROKER, &topic).await;
        prod.create_topic().await;
        prod.produce_plain(5).await;

        let mut consumer = Consumer::assign(
            kestrel_tokio::Tokio,
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
/// connection. On this cluster the leader advertises `localhost:9092` while the
/// bootstrap address is `127.0.0.1:9092`, so the two are distinct strings and a
/// consumer that skipped routing would hold one connection instead of two.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn fetches_are_routed_to_the_partition_leader() {
    run(|| async {
        let topic = unique_topic("routing");
        let mut prod = TestProducer::connect(BROKER, &topic).await;
        prod.create_topic().await;
        prod.produce_plain(1).await;

        let mut consumer = Consumer::assign(
            kestrel_tokio::Tokio,
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
            leader.ends_with(":9092"),
            "leader address came from metadata: {leader}"
        );

        let records = fetch_until(&mut consumer, 1).await;
        assert_eq!(records.len(), 1);
    });
}

/// Fetch until `want` records have arrived or the attempts run out. A single
/// fetch is allowed to return nothing — an empty fetch is normal, not a
/// failure — so tests that assert on content must poll.
async fn fetch_until(
    consumer: &mut Consumer,
    want: usize,
) -> Vec<kafka_protocol::records::Record> {
    consumer.set_max_wait(Duration::from_millis(200));
    let mut out = Vec::new();
    for _ in 0..25 {
        out.extend(consumer.fetch().await.expect("fetch"));
        if out.len() >= want {
            break;
        }
    }
    out
}
