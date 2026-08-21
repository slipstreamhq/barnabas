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

use barnabas_client::{Producer, ProducerRecord};
use kafka_protocol::messages::ApiKey;
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
    Producer::<Sim>::transactional(Sim, &bootstrap(), "sim", "txn-1")
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
        after: 0,
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
        after: 0,
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
        after: 0,
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
        after: 0,
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
        after: 0,
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
        after: 0,
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
                barnabas_client::Error::Broker {
                    disposition: barnabas_core::Disposition::Fatal,
                    ..
                }
            ),
            "fencing must be fatal, got: {err}"
        );

        // And the producer is done: no further writes under a dead epoch.
        let again = producer.send("t", 0, &records(1)).await;
        assert!(
            matches!(again, Err(barnabas_client::Error::Producer(_))),
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
        after: 0,
    }]);

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        let err = producer.send("t", 0, &records(1)).await.expect_err("fatal");
        assert!(matches!(
            err,
            barnabas_client::Error::Broker {
                disposition: barnabas_core::Disposition::Fatal,
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

// ── connections that die ─────────────────────────────────────────────────────

/// **A broker restart must not be permanent.**
///
/// Pooled connections are closed all the time — rolling upgrades, idle reapers,
/// network blips — and the client only finds out when it writes. Without a
/// reconnect the dead socket stays in the pool and *every* later request fails,
/// which is a client that dies the first time its cluster is maintained. No
/// broker-backed test can see this, because nothing there kills a connection on
/// cue.
#[test]
fn a_closed_connection_is_reconnected() {
    sim::start_with(
        Vec::new(),
        vec![sim::Close {
            api: ApiKey::Produce,
            times: 1,
        }],
        Vec::new(),
    );

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        producer
            .send("t", 0, &records(2))
            .await
            .expect("a closed connection must be reconnected, not fatal");
    });

    let j = journal();
    assert_eq!(
        j.produced,
        vec![(0, 0, 2)],
        "the reconnect wrote twice, or lost the batch"
    );
    assert_eq!(
        j.count(ApiKey::Produce),
        2,
        "expected exactly one reconnect"
    );
}

/// The retry after a reconnect carries the same sequence numbers, so a broker
/// that had *already* persisted the first attempt deduplicates it rather than
/// writing it twice. Losing that is how a connection blip becomes a duplicate.
#[test]
fn a_reconnect_does_not_renumber() {
    sim::start_with(
        Vec::new(),
        vec![sim::Close {
            api: ApiKey::Produce,
            times: 1,
        }],
        Vec::new(),
    );

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        producer.send("t", 0, &records(3)).await.expect("first");
        producer.send("t", 0, &records(2)).await.expect("second");
    });

    assert_eq!(
        journal().produced,
        vec![(0, 0, 3), (0, 3, 2)],
        "sequences restarted across a reconnect"
    );
}

/// **A broker that never answers must not hang the caller.**
///
/// The socket stays open, so nothing but a deadline ends this. The connection
/// is dropped with the request: the response may still arrive, and reading it
/// as the answer to the *next* request would desynchronise the stream.
#[test]
fn a_hung_broker_times_out_rather_than_hanging() {
    sim::start_with(
        Vec::new(),
        Vec::new(),
        vec![sim::Hang {
            api: ApiKey::Produce,
            times: 1,
        }],
    );

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        let err = producer
            .send("t", 0, &records(1))
            .await
            .expect_err("a hung broker must time out");
        assert!(
            matches!(err, barnabas_client::Error::Timeout { .. }),
            "expected a timeout, got: {err}"
        );
    });

    assert!(
        journal().produced.is_empty(),
        "nothing was accepted, so nothing may be reported as written"
    );
}

// ── batching ────────────────────────────────────────────────────────────────

/// **One request per broker, not per partition.**
///
/// A producer writing to four partitions on one broker sends *one* `Produce`.
/// The unbatched version sent four, which is four round trips where the network
/// cost is identical — and it is the difference that makes a per-core client
/// able to own many partitions at all.
#[test]
fn one_produce_request_covers_every_partition_on_a_broker() {
    start(vec![]);

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        // Keys chosen to spread: the simulated topic has four partitions.
        let records: Vec<ProducerRecord> = (0..40)
            .map(|i| {
                ProducerRecord::new(
                    Some(bytes::Bytes::from(format!("key-{i}"))),
                    Some(bytes::Bytes::from(format!("v{i}"))),
                )
            })
            .collect();
        let written = producer.send_keyed("t", &records).await.expect("send");
        assert!(
            written.len() > 1,
            "the test needs keys spread over several partitions, got {written:?}"
        );
    });

    let j = journal();
    assert_eq!(
        j.produce_widths.len(),
        1,
        "expected one batched request, got {} ({:?})",
        j.produce_widths.len(),
        j.produce_widths
    );
    assert!(
        j.produce_widths[0] > 1,
        "the single request carried only one partition: {:?}",
        j.produce_widths
    );
    assert_eq!(
        j.produced.len(),
        j.produce_widths[0],
        "every partition in the request must be accounted for"
    );
}

/// **One enrollment request, not one per partition.** `AddPartitionsToTxn`
/// takes a list, and a transaction as short as a checkpoint interval pays this
/// cost every time.
#[test]
fn partitions_are_enrolled_in_one_request() {
    start(vec![]);

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        let records: Vec<ProducerRecord> = (0..40)
            .map(|i| {
                ProducerRecord::new(
                    Some(bytes::Bytes::from(format!("key-{i}"))),
                    Some(bytes::Bytes::from(format!("v{i}"))),
                )
            })
            .collect();
        producer.send_keyed("t", &records).await.expect("send");
    });

    let j = journal();
    assert_eq!(
        j.count(ApiKey::AddPartitionsToTxn),
        1,
        "enrollment was sent per partition instead of once"
    );
    assert!(
        j.enroll_widths[0] > 1,
        "the enrollment carried one partition: {:?}",
        j.enroll_widths
    );
}

/// A partition whose leader moved is re-sent **alone** — the ones that
/// succeeded are not sent again, because re-sending them would rely on the
/// broker's deduplication to undo work the client should not have done.
#[test]
fn only_the_failed_partition_is_retried() {
    start(vec![Fault {
        api: ApiKey::Produce,
        code: 6, // NOT_LEADER_OR_FOLLOWER — applied to the whole first request
        times: 1,
        after: 0,
    }]);

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        let records: Vec<ProducerRecord> = (0..40)
            .map(|i| {
                ProducerRecord::new(
                    Some(bytes::Bytes::from(format!("key-{i}"))),
                    Some(bytes::Bytes::from(format!("v{i}"))),
                )
            })
            .collect();
        producer.send_keyed("t", &records).await.expect("send");
    });

    let j = journal();
    // The first request failed wholesale, so the retry carries everything —
    // and each partition still appears exactly once in what was accepted.
    let mut partitions: Vec<i32> = j.produced.iter().map(|(p, _, _)| *p).collect();
    partitions.sort_unstable();
    let unique: std::collections::BTreeSet<i32> = partitions.iter().copied().collect();
    assert_eq!(
        partitions.len(),
        unique.len(),
        "a partition was written twice: {partitions:?}"
    );
}

// ── pipelining ───────────────────────────────────────────────────────────────

/// **Several batches in flight at once, and the log still reads in order.**
///
/// Five enqueued batches for one partition go out as five `Produce` requests
/// before any is awaited. The simulator now enforces the sequence rule a real
/// broker enforces, so a client that reordered or gapped them would be rejected
/// rather than quietly accepted.
#[test]
fn a_window_of_batches_arrives_in_sequence() {
    sim::start(Vec::new());

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        for _ in 0..5 {
            producer
                .enqueue("t", 0, &records(2))
                .await
                .expect("enqueue");
        }
        assert_eq!(producer.queued(), 5);

        let written = producer.flush().await.expect("flush");
        assert_eq!(written.len(), 5, "every batch must be acknowledged");
        assert_eq!(producer.queued(), 0);

        let journal = journal();
        assert_eq!(
            journal.produced,
            vec![(0, 0, 2), (0, 2, 2), (0, 4, 2), (0, 6, 2), (0, 8, 2)],
            "sequences must be contiguous and in order"
        );
        // Five batches, one request each: they were pipelined, not merged.
        assert_eq!(
            journal.produce_widths.len(),
            5,
            "expected five requests, got {:?}",
            journal.produce_widths
        );
    });
}

/// **The window is bounded.** Ten batches with a limit of three must go out as
/// windows of three, not as ten requests at once — the recovery cost of a
/// failure is proportional to the window, which is why it has a limit at all.
#[test]
fn the_window_is_bounded_by_max_in_flight() {
    sim::start(Vec::new());

    drive(async {
        let mut producer = transactional().await;
        producer.set_max_in_flight(3);
        producer.begin_transaction().expect("begin");
        for _ in 0..10 {
            producer
                .enqueue("t", 0, &records(1))
                .await
                .expect("enqueue");
        }
        producer.flush().await.expect("flush");

        let journal = journal();
        assert_eq!(journal.produced.len(), 10);
        for (i, (_, base, _)) in journal.produced.iter().enumerate() {
            assert_eq!(*base, i as i32, "sequences must be 0..10 in order");
        }
    });
}

/// **The case pipelining exists to get wrong: a failure in the middle of the
/// window.**
///
/// The broker rejects the first request with `NOT_LEADER_OR_FOLLOWER`. Every
/// later request in that window is then for a partition whose earlier batch was
/// never written, so the broker answers `OUT_OF_ORDER_SEQUENCE_NUMBER` — which
/// this client classifies as fatal when it appears on its own. Here it must be
/// read as a consequence, the whole window must be re-sent in order, and
/// nothing may be written twice or skipped.
#[test]
fn a_failure_mid_window_resends_everything_behind_it() {
    sim::start(vec![Fault {
        api: ApiKey::Produce,
        code: 6, // NOT_LEADER_OR_FOLLOWER
        times: 1,
        after: 0,
    }]);

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        for _ in 0..4 {
            producer
                .enqueue("t", 0, &records(2))
                .await
                .expect("enqueue");
        }
        let written = producer.flush().await.expect("flush");

        assert_eq!(written.len(), 4, "every batch must land exactly once");
        let journal = journal();
        assert_eq!(
            journal.produced,
            vec![(0, 0, 2), (0, 2, 2), (0, 4, 2), (0, 6, 2)],
            "the log must read in order with no gap and no duplicate"
        );
        assert!(
            !matches!(producer.state(), barnabas_core::TxnState::Fatal),
            "an out-of-sequence answer caused by an earlier failure must not fence"
        );
    });
}

/// A sequence error with **nothing failed before it** is still fatal. That is
/// the check the pipelining path must not have weakened: it means the
/// producer's stream is genuinely wrong, and retrying is how duplicates happen.
#[test]
fn an_unexplained_sequence_error_still_fences() {
    sim::start(vec![Fault {
        api: ApiKey::Produce,
        code: 45, // OUT_OF_ORDER_SEQUENCE_NUMBER
        times: 1,
        after: 0,
    }]);

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        producer
            .enqueue("t", 0, &records(1))
            .await
            .expect("enqueue");
        let err = producer.flush().await.expect_err("must not be tolerated");
        assert!(
            matches!(
                err,
                barnabas_client::Error::Broker {
                    disposition: barnabas_core::Disposition::Fatal,
                    ..
                }
            ),
            "expected a fatal sequence error, got: {err}"
        );
        assert!(matches!(producer.state(), barnabas_core::TxnState::Fatal));
    });
}

/// **Only the contiguous leading run of successes is retired.**
///
/// The first request in the window lands; the second is rejected. Batch 0 is
/// therefore done, while batches 1 onward — including the ones the broker
/// answered *after* the failure, which it rejected for being out of sequence —
/// must all go again, in order. Retiring a batch because its own round happened
/// to succeed, without checking whether an earlier one failed, is how a
/// pipelined producer leaves a gap in the log.
#[test]
fn a_success_after_a_failure_is_not_retired() {
    sim::start(vec![Fault {
        api: ApiKey::Produce,
        code: 6, // NOT_LEADER_OR_FOLLOWER
        times: 1,
        after: 1, // let the first request through
    }]);

    drive(async {
        let mut producer = transactional().await;
        producer.begin_transaction().expect("begin");
        for _ in 0..4 {
            producer
                .enqueue("t", 0, &records(2))
                .await
                .expect("enqueue");
        }
        let written = producer.flush().await.expect("flush");

        assert_eq!(written.len(), 4, "every batch must land exactly once");
        let journal = journal();
        assert_eq!(
            journal.produced,
            vec![(0, 0, 2), (0, 2, 2), (0, 4, 2), (0, 6, 2)],
            "the log must read in order with no gap and no duplicate"
        );
    });
}

/// **A `call` on a busy connection is refused, not guessed at.**
///
/// Kafka answers a connection's requests in order, so a request/response call
/// issued while something else is outstanding reads that something else's
/// answer. Three bugs in this client were exactly that, each fixed by draining
/// at one more call site — which is remembering, not preventing. The last one
/// only surfaced because a `Fetch` response failed to parse as an
/// `OffsetCommit`; a response that happened to parse would have been silent,
/// and silence here means committed offsets for records nobody read.
///
/// So the connection refuses. Anything that pipelines on purpose uses
/// `send`/`recv` and owns the ordering itself.
#[test]
fn a_call_on_a_busy_connection_is_refused() {
    sim::start(Vec::new());

    drive(async {
        let mut cluster = barnabas_client::Cluster::connect(Sim, &bootstrap(), "sim")
            .await
            .expect("cluster");

        // Leave a request outstanding, as a prefetching consumer does.
        cluster
            .send_at_for_test(ApiKey::Metadata, 12, BOOTSTRAP, &metadata_request())
            .await
            .expect("send");

        // Now do what a forgetful call site does.
        let err = cluster
            .call_at_for_test::<_, kafka_protocol::messages::MetadataResponse>(
                BOOTSTRAP,
                ApiKey::Metadata,
                12,
                &metadata_request(),
            )
            .await
            .expect_err("a busy connection must refuse");

        assert!(
            matches!(
                err,
                barnabas_client::Error::ConnectionBusy { in_flight: 1, .. }
            ),
            "expected ConnectionBusy, got: {err}"
        );
    });
}

fn metadata_request() -> kafka_protocol::messages::MetadataRequest {
    let mut req = kafka_protocol::messages::MetadataRequest::default();
    req.topics = Some(vec![]);
    req
}
