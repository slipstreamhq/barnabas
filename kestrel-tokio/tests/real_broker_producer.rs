//! Producer tests against a real Kafka broker — **on tokio**.
//!
//! `#[ignore]`d — they need a broker on `localhost:9092`; the invocation is in
//! `real_broker.rs`. The state machine's rules are pinned deterministically in
//! `kestrel-core`; these check the rules match what a broker actually enforces,
//! and that the requests are routed to the brokers that will accept them.
//!
//! **The consumer is the oracle here.** Asserting on `Produce`'s status code
//! proves almost nothing — every failure P0 found returned `Ok`. So every test
//! reads back what it wrote, through the READ_COMMITTED path.

use std::time::Duration;

use bytes::Bytes;
use kestrel_tokio::{Consumer, IsolationLevel, Producer, ProducerRecord, EARLIEST};

mod producer;
use producer::TestProducer;

const BROKER: &str = "127.0.0.1:9092";

fn bootstrap() -> Vec<String> {
    vec![BROKER.to_owned()]
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("kestrel-{prefix}-{nanos}")
}

/// The whole runtime difference in this file. Multi-threaded on purpose: the
/// tokio binding's handles are `Send`, and a current-thread runtime would not
/// exercise that.
fn run<F: std::future::Future<Output = ()>>(fut: impl FnOnce() -> F + 'static) {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build runtime")
        .block_on(async move { fut().await });
}

fn records(values: &[&str]) -> Vec<ProducerRecord> {
    values
        .iter()
        .map(|v| {
            ProducerRecord::new(
                Some(Bytes::from(format!("k{v}"))),
                Some(Bytes::from((*v).to_owned())),
            )
        })
        .collect()
}

/// Topic creation still uses the test scaffolding — `kestrel` has no admin
/// client, and it does not need one.
async fn make_topic(topic: &str) {
    make_topic_with_partitions(topic, 1).await;
}

async fn make_topic_with_partitions(topic: &str, partitions: i32) {
    let mut admin = TestProducer::connect(BROKER, topic).await;
    admin.create_topic_with_partitions(partitions).await;
}

async fn read_all(topic: &str, want: usize, isolation: IsolationLevel) -> Vec<String> {
    let mut consumer = Consumer::assign(
        kestrel_tokio::Tokio,
        &bootstrap(),
        "kestrel-producer-test",
        topic,
        0,
        EARLIEST,
        isolation,
    )
    .await
    .expect("assign");
    consumer.set_max_wait(Duration::from_millis(200));

    let mut out = Vec::new();
    for _ in 0..25 {
        for record in consumer.fetch().await.expect("fetch") {
            out.push(
                record
                    .value
                    .as_ref()
                    .map(|v| String::from_utf8_lossy(v).into_owned())
                    .unwrap_or_default(),
            );
        }
        if out.len() >= want {
            break;
        }
    }
    out
}

#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn an_idempotent_producer_round_trips() {
    run(|| async {
        let topic = unique("idem");
        make_topic(&topic).await;

        let mut producer = Producer::idempotent(kestrel_tokio::Tokio, &bootstrap(), "kestrel-test")
            .await
            .expect("idempotent producer");
        producer
            .send(&topic, 0, &records(&["a", "b", "c"]))
            .await
            .expect("send");

        assert_eq!(
            read_all(&topic, 3, IsolationLevel::ReadCommitted).await,
            vec!["a", "b", "c"]
        );
    });
}

/// A committed transaction is visible to a READ_COMMITTED reader.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn a_committed_transaction_is_visible() {
    run(|| async {
        let topic = unique("txn-commit");
        make_topic(&topic).await;

        let mut producer = Producer::transactional(kestrel_tokio::Tokio, &bootstrap(), "kestrel-test", &unique("tid"))
            .await
            .expect("transactional producer");

        producer.begin_transaction().expect("begin");
        producer
            .send(&topic, 0, &records(&["x", "y"]))
            .await
            .expect("send");
        producer.commit_transaction().await.expect("commit");

        assert_eq!(
            read_all(&topic, 2, IsolationLevel::ReadCommitted).await,
            vec!["x", "y"]
        );
    });
}

/// An aborted transaction is not.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn an_aborted_transaction_is_invisible() {
    run(|| async {
        let topic = unique("txn-abort");
        make_topic(&topic).await;

        let mut producer = Producer::transactional(kestrel_tokio::Tokio, &bootstrap(), "kestrel-test", &unique("tid"))
            .await
            .expect("transactional producer");

        producer.begin_transaction().expect("begin");
        producer
            .send(&topic, 0, &records(&["gone", "also-gone"]))
            .await
            .expect("send");
        producer.abort_transaction().await.expect("abort");

        let seen = read_all(&topic, 1, IsolationLevel::ReadCommitted).await;
        assert!(seen.is_empty(), "aborted records were visible: {seen:?}");
    });
}

/// **The P0 bug, as a regression test.**
///
/// A second transaction from the same producer must continue the sequence.
/// When it restarted, the broker treated it as a duplicate: `Ok`, the original
/// base offset echoed back, nothing written, and the transaction committed
/// empty — with no error anywhere. So this asserts on the *data*, which is the
/// only thing that would have caught it.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn a_second_transaction_is_not_deduplicated() {
    run(|| async {
        let topic = unique("txn-sequence");
        make_topic(&topic).await;

        let mut producer = Producer::transactional(kestrel_tokio::Tokio, &bootstrap(), "kestrel-test", &unique("tid"))
            .await
            .expect("transactional producer");

        producer.begin_transaction().expect("begin 1");
        let first = producer
            .send(&topic, 0, &records(&["one", "two"]))
            .await
            .expect("send 1");
        producer.commit_transaction().await.expect("commit 1");

        producer.begin_transaction().expect("begin 2");
        let second = producer
            .send(&topic, 0, &records(&["three", "four"]))
            .await
            .expect("send 2");
        producer.commit_transaction().await.expect("commit 2");

        assert!(
            second > first,
            "the second transaction's base offset ({second}) did not advance past \
             the first ({first}) — the broker deduplicated it and committed an \
             empty transaction"
        );
        assert_eq!(
            read_all(&topic, 4, IsolationLevel::ReadCommitted).await,
            vec!["one", "two", "three", "four"]
        );
    });
}

/// Abort, then commit, from one producer: only the committed records survive,
/// and the sequence carries across both.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn a_commit_after_an_abort_writes_only_the_commit() {
    run(|| async {
        let topic = unique("txn-abort-commit");
        make_topic(&topic).await;

        let mut producer = Producer::transactional(kestrel_tokio::Tokio, &bootstrap(), "kestrel-test", &unique("tid"))
            .await
            .expect("transactional producer");

        producer.begin_transaction().expect("begin 1");
        producer
            .send(&topic, 0, &records(&["dropped"]))
            .await
            .expect("send 1");
        producer.abort_transaction().await.expect("abort");

        producer.begin_transaction().expect("begin 2");
        producer
            .send(&topic, 0, &records(&["kept"]))
            .await
            .expect("send 2");
        producer.commit_transaction().await.expect("commit");

        assert_eq!(
            read_all(&topic, 1, IsolationLevel::ReadCommitted).await,
            vec!["kept"]
        );
    });
}

/// **Zombie fencing**, which is what a transactional id is *for*: a second
/// producer taking the same id bumps the epoch, and the first must not be able
/// to commit afterwards.
///
/// This is the property Slipstream's sink depends on for exactly-once across a
/// restart — the recovered instance fences whatever the crashed one left open.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn a_new_producer_fences_the_old_one() {
    run(|| async {
        let topic = unique("txn-fence");
        make_topic(&topic).await;
        let txn_id = unique("tid");

        let mut old = Producer::transactional(kestrel_tokio::Tokio, &bootstrap(), "kestrel-test", &txn_id)
            .await
            .expect("old producer");
        old.begin_transaction().expect("begin");
        old.send(&topic, 0, &records(&["zombie"]))
            .await
            .expect("send");

        // A restarted instance takes the same transactional id.
        let new = Producer::transactional(kestrel_tokio::Tokio, &bootstrap(), "kestrel-test", &txn_id)
            .await
            .expect("new producer");
        assert!(
            new.identity().expect("identity").epoch > old.identity().expect("identity").epoch,
            "the new producer did not bump the epoch, so it fences nothing"
        );

        // The zombie's commit must fail, and it must fail *fatally* — a retry
        // would be a second writer.
        let err = old.commit_transaction().await.expect_err("zombie committed");
        assert!(
            matches!(
                err,
                kestrel_tokio::Error::Broker {
                    disposition: kestrel_tokio::Disposition::Fatal,
                    ..
                }
            ),
            "a fenced producer must fail fatally, got: {err}"
        );

        let seen = read_all(&topic, 1, IsolationLevel::ReadCommitted).await;
        assert!(seen.is_empty(), "the zombie's records landed: {seen:?}");
    });
}

/// **Keyed placement, end to end.** `send_keyed` hashes the key, picks the
/// partition, and routes the batch to *that partition's leader* — the same two
/// steps librdkafka and the Java client take. This checks the record really
/// arrives on the partition the partitioner named, by reading that partition
/// alone.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn keyed_records_land_on_the_partition_their_key_names() {
    run(|| async {
        let topic = unique("keyed");
        make_topic_with_partitions(&topic, 4).await;

        let mut producer = Producer::idempotent(kestrel_tokio::Tokio, &bootstrap(), "kestrel-test")
            .await
            .expect("producer");

        // Where each key should go, according to the partitioner alone.
        let keys = ["alpha", "beta", "gamma", "delta", "epsilon"];
        let mut expected = Vec::new();
        for key in keys {
            let partition = producer
                .partition_for(&topic, Some(key.as_bytes()))
                .await
                .expect("partition_for");
            expected.push((key, partition));
        }

        let written = producer
            .send_keyed(
                &topic,
                &keys
                    .iter()
                    .map(|k| {
                        ProducerRecord::new(
                            Some(Bytes::from((*k).to_owned())),
                            Some(Bytes::from(format!("v-{k}"))),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .await
            .expect("send_keyed");
        assert!(!written.is_empty(), "nothing was written");

        // Read each partition on its own and check the keys that arrived are
        // exactly the ones the partitioner assigned to it.
        for partition in 0..4 {
            let want: Vec<&str> = expected
                .iter()
                .filter(|(_, p)| *p == partition)
                .map(|(k, _)| *k)
                .collect();

            let mut consumer = Consumer::assign(
                kestrel_tokio::Tokio,
                &bootstrap(),
                "kestrel-producer-test",
                &topic,
                partition,
                EARLIEST,
                IsolationLevel::ReadCommitted,
            )
            .await
            .expect("assign");
            consumer.set_max_wait(Duration::from_millis(200));

            let mut got = Vec::new();
            for _ in 0..10 {
                for record in consumer.fetch().await.expect("fetch") {
                    got.push(
                        record
                            .key
                            .as_ref()
                            .map(|k| String::from_utf8_lossy(k).into_owned())
                            .unwrap_or_default(),
                    );
                }
                if got.len() >= want.len() {
                    break;
                }
            }
            assert_eq!(
                got, want,
                "partition {partition} held the wrong keys; the record did not \
                 reach the leader its key names"
            );
        }
    });
}

/// Producing outside a transaction is caught by the state machine, before any
/// request is sent.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn producing_outside_a_transaction_is_refused() {
    run(|| async {
        let topic = unique("txn-misuse");
        make_topic(&topic).await;

        let mut producer = Producer::transactional(kestrel_tokio::Tokio, &bootstrap(), "kestrel-test", &unique("tid"))
            .await
            .expect("transactional producer");

        let err = producer
            .send(&topic, 0, &records(&["nope"]))
            .await
            .expect_err("send outside a transaction");
        assert!(
            matches!(err, kestrel_tokio::Error::Producer(_)),
            "expected a producer-state error, got: {err}"
        );
    });
}
