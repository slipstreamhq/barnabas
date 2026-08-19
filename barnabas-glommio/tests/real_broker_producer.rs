//! Producer tests against a real Kafka broker.
//!
//! `#[ignore]`d — they need a broker on `localhost:9092`; the invocation is in
//! `real_broker.rs`. The state machine's rules are pinned deterministically in
//! `barnabas-core`; these check the rules match what a broker actually enforces,
//! and that the requests are routed to the brokers that will accept them.
//!
//! **The consumer is the oracle here.** Asserting on `Produce`'s status code
//! proves almost nothing — every failure P0 found returned `Ok`. So every test
//! reads back what it wrote, through the READ_COMMITTED path.

use std::time::Duration;

use barnabas_core::IsolationLevel;
use barnabas_glommio::{Consumer, Producer, ProducerRecord, EARLIEST};
use bytes::Bytes;

mod producer;
use producer::TestProducer;

/// `KAFKA_BOOTSTRAP` so these can run against the three-broker cluster in
/// `cluster.sh` as well as a single broker.
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

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("barnabas-{prefix}-{nanos}")
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

/// Topic creation still uses the test scaffolding — `barnabas` has no admin
/// client, and it does not need one.
async fn make_topic(topic: &str) {
    make_topic_with_partitions(topic, 1).await;
}

async fn make_topic_with_partitions(topic: &str, partitions: i32) {
    let mut admin = TestProducer::connect(&broker(), topic).await;
    admin.create_topic_with_partitions(partitions).await;
}

async fn read_all(topic: &str, want: usize, isolation: IsolationLevel) -> Vec<String> {
    let mut consumer = Consumer::for_partition(
        barnabas_glommio::Glommio,
        &bootstrap(),
        "barnabas-producer-test",
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
        for record in consumer
            .poll()
            .await
            .expect("fetch")
            .into_iter()
            .flat_map(|group| group.iter().filter_map(|r| r.value()).collect::<Vec<_>>())
        {
            out.push(String::from_utf8_lossy(&record).into_owned());
        }
        if out.len() >= want {
            break;
        }
    }
    out
}

#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn an_idempotent_producer_round_trips() {
    let topic = unique("idem");
    make_topic(&topic).await;

    let mut producer =
        Producer::idempotent(barnabas_glommio::Glommio, &bootstrap(), "barnabas-test")
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
}

/// A committed transaction is visible to a READ_COMMITTED reader.
#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn a_committed_transaction_is_visible() {
    let topic = unique("txn-commit");
    make_topic(&topic).await;

    let mut producer = Producer::transactional(
        barnabas_glommio::Glommio,
        &bootstrap(),
        "barnabas-test",
        &unique("tid"),
    )
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
}

/// An aborted transaction is not.
#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn an_aborted_transaction_is_invisible() {
    let topic = unique("txn-abort");
    make_topic(&topic).await;

    let mut producer = Producer::transactional(
        barnabas_glommio::Glommio,
        &bootstrap(),
        "barnabas-test",
        &unique("tid"),
    )
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
}

/// **The P0 bug, as a regression test.**
///
/// A second transaction from the same producer must continue the sequence.
/// When it restarted, the broker treated it as a duplicate: `Ok`, the original
/// base offset echoed back, nothing written, and the transaction committed
/// empty — with no error anywhere. So this asserts on the *data*, which is the
/// only thing that would have caught it.
#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn a_second_transaction_is_not_deduplicated() {
    let topic = unique("txn-sequence");
    make_topic(&topic).await;

    let mut producer = Producer::transactional(
        barnabas_glommio::Glommio,
        &bootstrap(),
        "barnabas-test",
        &unique("tid"),
    )
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
}

/// Abort, then commit, from one producer: only the committed records survive,
/// and the sequence carries across both.
#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn a_commit_after_an_abort_writes_only_the_commit() {
    let topic = unique("txn-abort-commit");
    make_topic(&topic).await;

    let mut producer = Producer::transactional(
        barnabas_glommio::Glommio,
        &bootstrap(),
        "barnabas-test",
        &unique("tid"),
    )
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
}

/// **Zombie fencing**, which is what a transactional id is *for*: a second
/// producer taking the same id bumps the epoch, and the first must not be able
/// to commit afterwards.
///
/// This is the property Slipstream's sink depends on for exactly-once across a
/// restart — the recovered instance fences whatever the crashed one left open.
#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn a_new_producer_fences_the_old_one() {
    let topic = unique("txn-fence");
    make_topic(&topic).await;
    let txn_id = unique("tid");

    let mut old = Producer::transactional(
        barnabas_glommio::Glommio,
        &bootstrap(),
        "barnabas-test",
        &txn_id,
    )
    .await
    .expect("old producer");
    old.begin_transaction().expect("begin");
    old.send(&topic, 0, &records(&["zombie"]))
        .await
        .expect("send");

    // A restarted instance takes the same transactional id.
    let new = Producer::transactional(
        barnabas_glommio::Glommio,
        &bootstrap(),
        "barnabas-test",
        &txn_id,
    )
    .await
    .expect("new producer");
    assert!(
        new.identity().expect("identity").epoch > old.identity().expect("identity").epoch,
        "the new producer did not bump the epoch, so it fences nothing"
    );

    // The zombie's commit must fail, and it must fail *fatally* — a retry
    // would be a second writer.
    let err = old
        .commit_transaction()
        .await
        .expect_err("zombie committed");
    assert!(
        matches!(
            err,
            barnabas_glommio::Error::Broker {
                disposition: barnabas_core::Disposition::Fatal,
                ..
            }
        ),
        "a fenced producer must fail fatally, got: {err}"
    );

    let seen = read_all(&topic, 1, IsolationLevel::ReadCommitted).await;
    assert!(seen.is_empty(), "the zombie's records landed: {seen:?}");
}

/// **Keyed placement, end to end.** `send_keyed` hashes the key, picks the
/// partition, and routes the batch to *that partition's leader* — the same two
/// steps librdkafka and the Java client take. This checks the record really
/// arrives on the partition the partitioner named, by reading that partition
/// alone.
#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn keyed_records_land_on_the_partition_their_key_names() {
    let topic = unique("keyed");
    make_topic_with_partitions(&topic, 4).await;

    let mut producer =
        Producer::idempotent(barnabas_glommio::Glommio, &bootstrap(), "barnabas-test")
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

        let mut consumer = Consumer::for_partition(
            barnabas_glommio::Glommio,
            &bootstrap(),
            "barnabas-producer-test",
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
            for key in consumer
                .poll()
                .await
                .expect("fetch")
                .iter()
                .flat_map(|group| {
                    group
                        .iter()
                        .map(|r| {
                            r.key()
                                .map(|k| String::from_utf8_lossy(&k).into_owned())
                                .unwrap_or_default()
                        })
                        .collect::<Vec<_>>()
                })
            {
                got.push(key);
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
}

/// **Compression, both directions.** The producer compresses whole batches and
/// the consumer must decompress them — a codec that encodes but does not decode
/// looks fine on the wire and returns nothing to the caller.
///
/// All four of Kafka's codecs, because they are independent implementations and
/// a client that quietly supports three of them is a client that fails on
/// somebody's cluster.
#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn every_compression_codec_round_trips() {
    use barnabas_glommio::CompressionCodec;

    for codec in [
        CompressionCodec::None,
        CompressionCodec::Gzip,
        CompressionCodec::Snappy,
        CompressionCodec::Lz4,
        CompressionCodec::Zstd,
    ] {
        let topic = unique("compress");
        make_topic(&topic).await;

        let mut producer =
            Producer::idempotent(barnabas_glommio::Glommio, &bootstrap(), "barnabas-test")
                .await
                .expect("producer");
        producer.set_compression(codec);
        producer
            .send(&topic, 0, &records(&["a", "b", "c"]))
            .await
            .unwrap_or_else(|e| panic!("send with {codec:?}: {e}"));

        assert_eq!(
            read_all(&topic, 3, IsolationLevel::ReadCommitted).await,
            vec!["a", "b", "c"],
            "{codec:?} did not round trip"
        );
    }
}

/// Producing outside a transaction is caught by the state machine, before any
/// request is sent.
#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn producing_outside_a_transaction_is_refused() {
    let topic = unique("txn-misuse");
    make_topic(&topic).await;

    let mut producer = Producer::transactional(
        barnabas_glommio::Glommio,
        &bootstrap(),
        "barnabas-test",
        &unique("tid"),
    )
    .await
    .expect("transactional producer");

    let err = producer
        .send(&topic, 0, &records(&["nope"]))
        .await
        .expect_err("send outside a transaction");
    assert!(
        matches!(err, barnabas_glommio::Error::Producer(_)),
        "expected a producer-state error, got: {err}"
    );
}

// ── exactly-once with a consumer group ─────────────────────────────────────

/// Read every record a group can currently see, into `(partition, value)`.
///
/// Takes what [`joined`] already collected: waiting for an assignment means
/// polling, and a poll that fetches records has consumed them whether or not
/// the caller was ready. Dropping those is how this test first "proved" a
/// topic was empty.
async fn drain(
    consumer: &mut Consumer,
    mut seen: Vec<(i32, String)>,
    want: usize,
) -> Vec<(i32, String)> {
    for _ in 0..200 {
        for group in consumer.poll().await.expect("poll") {
            for record in group.iter() {
                seen.push((
                    group.partition,
                    String::from_utf8_lossy(&record.value().expect("value")).into_owned(),
                ));
            }
        }
        if seen.len() >= want {
            break;
        }
    }
    seen
}

/// Subscribe a fresh consumer to `group` and wait until it holds partitions,
/// keeping anything it read on the way.
async fn joined(group: &str, topic: &str, client_id: &str) -> (Consumer, Vec<(i32, String)>) {
    let mut consumer = Consumer::new(
        barnabas_glommio::Glommio,
        &bootstrap(),
        client_id,
        IsolationLevel::ReadCommitted,
    )
    .await
    .expect("consumer");
    consumer.set_max_wait(Duration::from_millis(100));
    consumer
        .subscribe(
            group,
            vec![topic.to_owned()],
            Box::new(barnabas_core::group::RangeAssignor),
            EARLIEST,
        )
        .await
        .expect("subscribe");
    let mut seen = Vec::new();
    for _ in 0..100 {
        for group in consumer.poll().await.expect("poll") {
            for record in group.iter() {
                seen.push((
                    group.partition,
                    String::from_utf8_lossy(&record.value().expect("value")).into_owned(),
                ));
            }
        }
        if consumer.assignments().count() > 0 {
            return (consumer, seen);
        }
    }
    panic!("never assigned a partition");
}

/// **Consume-transform-produce, exactly once.**
///
/// The output and the input offsets commit together or not at all. Asserting
/// only that the output arrived would pass with the offsets committed
/// separately, which is the bug this protocol exists to prevent — so the test
/// that matters is the *second* consumer: a fresh member of the same group must
/// see nothing left to read.
#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn exactly_once_commits_offsets_with_the_output() {
    let input = unique("eos-in");
    let output = unique("eos-out");
    let group = unique("eos-group");
    make_topic_with_partitions(&input, 2).await;
    make_topic(&output).await;

    let mut source = Producer::idempotent(barnabas_glommio::Glommio, &bootstrap(), "barnabas-test")
        .await
        .expect("source producer");
    source
        .send(&input, 0, &records(&["a", "b"]))
        .await
        .expect("p0");
    source
        .send(&input, 1, &records(&["c", "d"]))
        .await
        .expect("p1");

    let (mut consumer, prefetched) = joined(&group, &input, "eos-1").await;
    let mut producer = Producer::transactional(
        barnabas_glommio::Glommio,
        &bootstrap(),
        "barnabas-test",
        &unique("tid"),
    )
    .await
    .expect("transactional producer");

    let seen = drain(&mut consumer, prefetched, 4).await;
    assert_eq!(seen.len(), 4, "the input should be readable: {seen:?}");

    producer.begin_transaction().expect("begin");
    let transformed: Vec<ProducerRecord> = seen
        .iter()
        .map(|(_, value)| ProducerRecord::new(None, Some(Bytes::from(format!("{value}!")))))
        .collect();
    producer
        .send(&output, 0, &transformed)
        .await
        .expect("send output");

    // **After producing, before committing.** The offsets account for the
    // records just written; reading them earlier would commit input the
    // transaction has not yet produced output for.
    let metadata = consumer.group_metadata().expect("stable member");
    producer
        .send_offsets_to_transaction(&consumer.positions(), &metadata)
        .await
        .expect("send offsets");
    producer.commit_transaction().await.expect("commit");

    let mut written = read_all(&output, 4, IsolationLevel::ReadCommitted).await;
    written.sort();
    assert_eq!(written, vec!["a!", "b!", "c!", "d!"]);

    consumer.unsubscribe().await.expect("leave");

    // The offsets committed inside the transaction are the group's now.
    let (mut next, prefetched) = joined(&group, &input, "eos-2").await;
    let leftover = drain(&mut next, prefetched, 1).await;
    assert!(
        leftover.is_empty(),
        "the transaction committed the offsets, so nothing should be left: {leftover:?}"
    );
    next.unsubscribe().await.expect("leave 2");
}

/// An aborted transaction takes its offsets down with it: the input is read
/// again, which is the "at least once on failure" half of exactly-once.
#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn an_aborted_transaction_does_not_commit_offsets() {
    let input = unique("eos-abort-in");
    let output = unique("eos-abort-out");
    let group = unique("eos-abort-group");
    make_topic(&input).await;
    make_topic(&output).await;

    let mut source = Producer::idempotent(barnabas_glommio::Glommio, &bootstrap(), "barnabas-test")
        .await
        .expect("source producer");
    source
        .send(&input, 0, &records(&["x", "y"]))
        .await
        .expect("seed");

    let (mut consumer, prefetched) = joined(&group, &input, "abort-1").await;
    let mut producer = Producer::transactional(
        barnabas_glommio::Glommio,
        &bootstrap(),
        "barnabas-test",
        &unique("tid"),
    )
    .await
    .expect("transactional producer");

    let seen = drain(&mut consumer, prefetched, 2).await;
    assert_eq!(seen.len(), 2);

    producer.begin_transaction().expect("begin");
    producer
        .send(&output, 0, &records(&["doomed"]))
        .await
        .expect("send output");
    let metadata = consumer.group_metadata().expect("stable member");
    producer
        .send_offsets_to_transaction(&consumer.positions(), &metadata)
        .await
        .expect("send offsets");
    producer.abort_transaction().await.expect("abort");

    consumer.unsubscribe().await.expect("leave");

    let (mut next, prefetched) = joined(&group, &input, "abort-2").await;
    let again = drain(&mut next, prefetched, 2).await;
    let values: Vec<String> = again.into_iter().map(|(_, v)| v).collect();
    assert_eq!(
        values,
        vec!["x", "y"],
        "the abort discarded the offsets, so the input must be read again"
    );
    next.unsubscribe().await.expect("leave 2");
}

/// Offsets cannot be sent outside a transaction. Cheap to get wrong, and the
/// symptom without this check is offsets that commit unconditionally.
#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn sending_offsets_outside_a_transaction_is_rejected() {
    let topic = unique("eos-guard");
    let group = unique("eos-guard-group");
    make_topic(&topic).await;

    let (mut consumer, _) = joined(&group, &topic, "guard-1").await;
    let mut producer = Producer::transactional(
        barnabas_glommio::Glommio,
        &bootstrap(),
        "barnabas-test",
        &unique("tid"),
    )
    .await
    .expect("transactional producer");

    let metadata = consumer.group_metadata().expect("stable member");
    let mut offsets = std::collections::BTreeMap::new();
    offsets.insert(
        barnabas_core::group::TopicPartition::new(topic.clone(), 0),
        0,
    );

    let result = producer
        .send_offsets_to_transaction(&offsets, &metadata)
        .await;
    assert!(result.is_err(), "no transaction is open");

    consumer.unsubscribe().await.expect("leave");
}

// ── the accumulator ────────────────────────────────────────────────────────

/// With a linger set, single records coalesce into **one** batch, and a flush
/// sends whatever is still partial.
#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn lingering_coalesces_single_records_into_one_batch() {
    let topic = unique("linger");
    make_topic(&topic).await;

    let mut producer =
        Producer::idempotent(barnabas_glommio::Glommio, &bootstrap(), "barnabas-test")
            .await
            .expect("producer");
    producer.set_linger(Duration::from_secs(30));

    for i in 0..5 {
        producer
            .produce_to(
                &topic,
                0,
                ProducerRecord::new(None, Some(Bytes::from(format!("r{i}")))),
            )
            .await
            .expect("produce");
    }

    // Nothing has been sent: the linger has not expired and the batch is
    // nowhere near full.
    assert_eq!(producer.queued(), 1, "one partition staged, unsent");

    producer.flush().await.expect("flush");
    assert_eq!(producer.queued(), 0);

    let written = read_all(&topic, 5, IsolationLevel::ReadCommitted).await;
    assert_eq!(written, vec!["r0", "r1", "r2", "r3", "r4"]);
}

/// A batch that reaches `batch_size` is sent without waiting for the linger.
#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn a_full_batch_does_not_wait_for_the_linger() {
    let topic = unique("batchsize");
    make_topic(&topic).await;

    let mut producer =
        Producer::idempotent(barnabas_glommio::Glommio, &bootstrap(), "barnabas-test")
            .await
            .expect("producer");
    producer.set_linger(Duration::from_secs(30));
    producer.set_batch_size(64);

    // Each record is well under 64 bytes, so this crosses the threshold
    // partway through and sends without any flush.
    for i in 0..8 {
        producer
            .produce_to(
                &topic,
                0,
                ProducerRecord::new(None, Some(Bytes::from(format!("value-{i}")))),
            )
            .await
            .expect("produce");
    }

    // Each record is ~23 bytes estimated, so the threshold is crossed at
    // three and again at six; the last two are still staged. That the
    // first six are *readable with no flush at all* is the assertion.
    assert_eq!(producer.queued(), 1, "a partial batch remains");
    let early = read_all(&topic, 6, IsolationLevel::ReadCommitted).await;
    assert_eq!(early.len(), 6, "full batches went out on their own");

    producer.flush().await.expect("flush");
    let all = read_all(&topic, 8, IsolationLevel::ReadCommitted).await;
    assert_eq!(all.len(), 8);
}

/// `tick` sends an overdue batch, and `linger_deadline` says when that is.
#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn tick_sends_what_the_linger_has_made_due() {
    let topic = unique("tick");
    make_topic(&topic).await;

    let mut producer =
        Producer::idempotent(barnabas_glommio::Glommio, &bootstrap(), "barnabas-test")
            .await
            .expect("producer");
    producer.set_linger(Duration::from_millis(150));

    assert!(producer.linger_deadline().is_none(), "nothing staged yet");
    producer
        .produce_to(
            &topic,
            0,
            ProducerRecord::new(None, Some(Bytes::from("late"))),
        )
        .await
        .expect("produce");
    let deadline = producer.linger_deadline().expect("a deadline once staged");
    assert_eq!(producer.queued(), 1, "still waiting");

    // An early tick must not send it — otherwise the linger is decorative.
    producer.tick().await.expect("early tick");
    assert_eq!(producer.queued(), 1, "not due yet");

    glommio::timer::sleep(deadline.saturating_duration_since(std::time::Instant::now())).await;
    glommio::timer::sleep(Duration::from_millis(20)).await;
    producer.tick().await.expect("tick");
    assert_eq!(producer.queued(), 0, "due, so sent");

    assert_eq!(
        read_all(&topic, 1, IsolationLevel::ReadCommitted).await,
        vec!["late"]
    );
}

/// `produce` picks the partition from the key, the same way `send_keyed` does:
/// equal keys land together.
#[glommio::test]
#[ignore = "needs a Kafka broker on localhost:9092"]
async fn produce_places_records_by_key() {
    let topic = unique("produce-key");
    make_topic_with_partitions(&topic, 3).await;

    let mut producer =
        Producer::idempotent(barnabas_glommio::Glommio, &bootstrap(), "barnabas-test")
            .await
            .expect("producer");
    producer.set_linger(Duration::from_secs(30));

    let expected = producer
        .partition_for(&topic, Some(b"same"))
        .await
        .expect("partition for key");

    for i in 0..4 {
        producer
            .produce(
                &topic,
                ProducerRecord::new(
                    Some(Bytes::from_static(b"same")),
                    Some(Bytes::from(format!("v{i}"))),
                ),
            )
            .await
            .expect("produce");
    }
    assert_eq!(
        producer.queued(),
        1,
        "one key means one partition means one staged batch"
    );

    let written = producer.flush().await.expect("flush");
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].1, expected, "the key decided the partition");
}
