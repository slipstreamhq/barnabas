//! What the client does when the cluster misbehaves.
//!
//! These are the cases a broker-backed test cannot reach: a broker cannot be
//! asked to move a leader mid-produce, migrate a coordinator, or fence a
//! producer on cue. They are also the cases where being wrong is *silent* — a
//! retry that writes twice returns `Ok` and reports success — so the assertions
//! are on what the simulated broker **accepted**, never on the return value
//! alone.
//!
//! No runtime, no sockets, no clock: retries are free, so a test that forces
//! forty of them runs instantly and reproduces exactly.

mod sim;

use kafka_protocol::messages::ApiKey;
use kestrel_client::{Producer, ProducerRecord};
use sim::{drive, journal, start, Fault, Sim};

const BOOTSTRAP: &str = "broker-1:9092";

fn bootstrap() -> Vec<String> {
    vec![BOOTSTRAP.to_owned()]
}

fn records(n: usize) -> Vec<ProducerRecord> {
    (0..n)
        .map(|i| ProducerRecord::new(None, Some(bytes::Bytes::from(format!("v{i}")))))
        .collect()
}

async fn transactional() -> Producer<Sim> {
    Producer::<Sim>::transactional(&bootstrap(), "sim", "txn-1")
        .await
        .expect("producer")
}

/// **A leader that moves mid-produce must not duplicate.**
///
/// The broker answers `NOT_LEADER_OR_FOLLOWER` once; the client must refresh
/// metadata and re-send. The re-send carries the *same* sequence numbers, so
/// exactly one batch is accepted — asserted on what the broker took, because a
/// duplicate would return `Ok` just the same.
#[test]
fn a_moved_leader_is_retried_without_duplicating() {
    start(vec![Fault {
        api: ApiKey::Produce,
        code: 6, // NOT_LEADER_OR_FOLLOWER
        times: 1,
    }]);

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        producer.send("t", 0, &records(3)).await.expect("send");
        producer.commit_transaction().await.expect("commit");
    });

    let j = journal();
    assert_eq!(
        j.produced,
        vec![(0, 0, 3)],
        "the retry wrote a second batch, or the first was lost"
    );
    assert_eq!(j.count(ApiKey::Produce), 2, "expected one retry");
    assert!(
        j.count(ApiKey::Metadata) >= 2,
        "a moved leader must trigger a metadata refresh, not a blind retry"
    );
}

/// **A retry re-sends the same sequence numbers**, which is what makes it a
/// retry rather than a second write. Five failures deep, the accepted batch
/// still starts at sequence 0.
#[test]
fn repeated_retries_keep_the_same_sequence() {
    start(vec![Fault {
        api: ApiKey::Produce,
        code: 6,
        times: 5,
    }]);

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        producer.send("t", 0, &records(2)).await.expect("send");
    });

    let j = journal();
    assert_eq!(j.produced, vec![(0, 0, 2)]);
}

/// **`NOT_COORDINATOR` must re-discover, not retry in place.**
///
/// The coordinator genuinely moves; a client that retries the same broker spins
/// against one that will never answer. Asserted by counting `FindCoordinator`
/// requests, since the return value looks identical either way.
#[test]
fn a_moved_coordinator_is_rediscovered() {
    start(vec![Fault {
        api: ApiKey::InitProducerId,
        code: 16, // NOT_COORDINATOR
        times: 1,
    }]);

    drive(async {
        let _ = transactional().await;
    });

    let j = journal();
    assert!(
        j.count(ApiKey::FindCoordinator) >= 2,
        "NOT_COORDINATOR was retried in place instead of re-discovering \
         (FindCoordinator seen {} times)",
        j.count(ApiKey::FindCoordinator)
    );
}

/// A cold cluster answers `COORDINATOR_LOAD_IN_PROGRESS` while it replays the
/// internal topic. That is a state, not a failure.
#[test]
fn a_warming_coordinator_is_waited_out() {
    start(vec![Fault {
        api: ApiKey::InitProducerId,
        code: 14, // COORDINATOR_LOAD_IN_PROGRESS
        times: 3,
    }]);

    drive(async {
        let _ = transactional().await;
    });

    assert_eq!(journal().count(ApiKey::InitProducerId), 4);
}

/// Starting a transaction immediately after ending one gets
/// `CONCURRENT_TRANSACTIONS` while the previous markers are written. Retriable.
#[test]
fn concurrent_transactions_is_waited_out() {
    start(vec![Fault {
        api: ApiKey::AddPartitionsToTxn,
        code: 51,
        times: 2,
    }]);

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        producer.send("t", 0, &records(1)).await.expect("send");
    });

    let j = journal();
    assert_eq!(j.count(ApiKey::AddPartitionsToTxn), 3);
    assert_eq!(j.produced, vec![(0, 0, 1)]);
}

/// **A fenced producer must stop, and stay stopped.**
///
/// `PRODUCER_FENCED` means another instance took this transactional id. A
/// client that retries is a split brain, and one that merely reports the error
/// but stays usable is worse — the next `send` would write under a dead epoch.
#[test]
fn a_fenced_producer_refuses_everything_afterwards() {
    start(vec![Fault {
        api: ApiKey::Produce,
        code: 90, // PRODUCER_FENCED
        times: 1,
    }]);

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");

        let err = producer
            .send("t", 0, &records(1))
            .await
            .expect_err("a fenced produce must fail");
        assert!(
            matches!(
                err,
                kestrel_client::Error::Broker {
                    disposition: kestrel_core::Disposition::Fatal,
                    ..
                }
            ),
            "fencing must be fatal, got: {err}"
        );

        // And the producer is done: no further writes under a dead epoch.
        let again = producer.send("t", 0, &records(1)).await;
        assert!(
            matches!(again, Err(kestrel_client::Error::Producer(_))),
            "a fenced producer accepted another send"
        );
    });

    assert!(
        journal().produced.is_empty(),
        "a fenced producer wrote records"
    );
}

/// An out-of-sequence error means the stream is already wrong. Retrying it is
/// how duplicates get written, so it must be fatal — not retried like a leader
/// move.
#[test]
fn an_out_of_sequence_error_is_not_retried() {
    start(vec![Fault {
        api: ApiKey::Produce,
        code: 45, // OUT_OF_ORDER_SEQUENCE_NUMBER
        times: 1,
    }]);

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        let err = producer.send("t", 0, &records(1)).await.expect_err("fatal");
        assert!(matches!(
            err,
            kestrel_client::Error::Broker {
                disposition: kestrel_core::Disposition::Fatal,
                ..
            }
        ));
    });

    assert_eq!(
        journal().count(ApiKey::Produce),
        1,
        "a sequence error was retried; that is how duplicates are written"
    );
}

/// **The P0 bug, with no broker.** Sequences continue across transactions, so
/// the second transaction's batch starts where the first left off. Restarting
/// them makes a real broker silently deduplicate — `Ok`, nothing written.
#[test]
fn sequences_continue_across_transactions() {
    start(vec![]);

    drive(async {
        let mut producer = transactional().await;

        producer.begin_transaction().expect("begin 1");
        producer.send("t", 0, &records(3)).await.expect("send 1");
        producer.commit_transaction().await.expect("commit 1");

        producer.begin_transaction().expect("begin 2");
        producer.send("t", 0, &records(2)).await.expect("send 2");
        producer.commit_transaction().await.expect("commit 2");
    });

    assert_eq!(
        journal().produced,
        vec![(0, 0, 3), (0, 3, 2)],
        "the second transaction restarted the sequence"
    );
}

/// Enrollment happens once per transaction, not once per batch: the
/// coordinator needs to know the partition is in the transaction, and repeating
/// it per send would be a request per batch for nothing.
#[test]
fn a_partition_is_enrolled_once_per_transaction() {
    start(vec![]);

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        for _ in 0..3 {
            producer.send("t", 0, &records(1)).await.expect("send");
        }
        producer.commit_transaction().await.expect("commit");
    });

    let j = journal();
    assert_eq!(j.count(ApiKey::AddPartitionsToTxn), 1);
    assert_eq!(j.count(ApiKey::Produce), 3);
}
