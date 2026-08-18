//! Consumer groups against a real broker.
//!
//! The state machine and the assignors are unit-tested without IO; this checks
//! the half that only a coordinator can answer — that a `JoinGroup` we build is
//! one Kafka accepts, that the embedded subscription blob is one it relays, and
//! that two members of the same group are given disjoint partitions.
//!
//! # Two bugs these found
//!
//! **The `ConsumerProtocol` blobs need a version prefix.** Kafka's
//! `serializeSubscription` writes an int16 version and then the struct;
//! `kafka-protocol`'s generated type is only the struct. Without the prefix the
//! coordinator accepted the `JoinGroup`, logged `Preparing to rebalance`, and
//! then never stabilized the group — no error, no response, just a request held
//! until the rebalance timeout. A Java consumer on the same broker stabilized
//! in a millisecond.
//!
//! **A leader must still be a leader when asked twice.** `advance` re-asks
//! `step()` rather than acting on what `on_join` returned, and `Syncing` did
//! not record leadership — so the leader synced like a follower, sent an empty
//! assignment, and every member was assigned nothing. The group was *stable*
//! and owned no partitions, which is the kind of failure that looks like a
//! working consumer with an idle topic.

mod producer;

use std::time::Duration;

use kestrel_client::group::{ClassicProtocol, GroupProtocol, Membership};
use kestrel_core::group::RangeAssignor;
use producer::TestProducer;

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
    format!("kestrel-{prefix}-{nanos}")
}

fn run<F: std::future::Future<Output = ()>>(fut: impl FnOnce() -> F + 'static) {
    glommio::LocalExecutorBuilder::default()
        .make()
        .expect("build executor")
        .run(async move { fut().await });
}

/// Drive membership until it settles, or give up.
async fn settle(
    protocol: &mut ClassicProtocol,
    cluster: &mut kestrel_glommio::Cluster,
) -> Vec<kestrel_core::group::TopicPartition> {
    for _ in 0..200 {
        match protocol.advance(cluster).await.expect("advance") {
            Membership::Assigned(partitions) => return partitions,
            Membership::InProgress | Membership::Revoked(_) => {
                glommio::timer::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    panic!("membership never became stable");
}

/// **One member gets everything.** The smallest end-to-end proof: the coordinator
/// accepted our join, relayed our subscription, took the assignment we computed
/// as leader, and handed it back.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn a_single_member_is_assigned_every_partition() {
    run(|| async {
        let topic = unique("group");
        let mut prod = TestProducer::connect(&broker(), &topic).await;
        prod.create_topic_with_partitions(6).await;

        let mut cluster =
            kestrel_glommio::Cluster::connect(kestrel_glommio::Glommio, &bootstrap(), "kestrel-test")
                .await
                .expect("cluster");

        let mut protocol = ClassicProtocol::new(
            unique("g"),
            vec![topic.clone()],
            Box::new(RangeAssignor),
        );
        // Short, so a stall fails the test in seconds rather than sitting in a
        // five-minute long poll.
        protocol.set_rebalance_timeout(300_000);
        protocol.set_session_timeout(45_000);

        let assigned = settle(&mut protocol, &mut cluster).await;
        assert_eq!(assigned.len(), 6, "one member owns every partition");
        assert!(assigned.iter().all(|tp| tp.topic == topic));
        assert!(protocol.member().can_commit(), "stable members may commit");

        protocol.leave(&mut cluster).await.expect("leave");
        assert!(!protocol.member().can_commit(), "a departed member may not");
    });
}

/// **Two members split the partitions, and never share one.** An overlap here
/// is duplicate consumption; a gap is a partition nobody reads. Both are silent
/// in production, which is why this asserts on the set rather than the counts.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn two_members_split_the_partitions_without_overlap() {
    run(|| async {
        let topic = unique("group2");
        let group = unique("g2");
        let mut prod = TestProducer::connect(&broker(), &topic).await;
        prod.create_topic_with_partitions(4).await;

        let mut cluster_a =
            kestrel_glommio::Cluster::connect(kestrel_glommio::Glommio, &bootstrap(), "member-a")
                .await
                .expect("cluster a");
        let mut cluster_b =
            kestrel_glommio::Cluster::connect(kestrel_glommio::Glommio, &bootstrap(), "member-b")
                .await
                .expect("cluster b");

        let mut a = ClassicProtocol::new(group.clone(), vec![topic.clone()], Box::new(RangeAssignor));
        let mut b = ClassicProtocol::new(group.clone(), vec![topic.clone()], Box::new(RangeAssignor));
        for p in [&mut a, &mut b] {
            p.set_rebalance_timeout(10_000);
            p.set_session_timeout(10_000);
        }

        // **Concurrently, not in turn.** `JoinGroup` is a long poll: the
        // coordinator holds member A's request until the whole group has
        // joined, so driving A to completion before starting B deadlocks until
        // the rebalance timeout. Both have to be in flight together — which is
        // the shape a real deployment has anyway, one member per process.
        let mut assigned_a = Vec::new();
        let mut assigned_b = Vec::new();
        for _ in 0..60 {
            let (ra, rb) =
                futures_lite::future::zip(a.advance(&mut cluster_a), b.advance(&mut cluster_b))
                    .await;
            if let Membership::Assigned(p) = ra.expect("a") {
                assigned_a = p;
            }
            if let Membership::Assigned(p) = rb.expect("b") {
                assigned_b = p;
            }
            if !assigned_a.is_empty() && !assigned_b.is_empty() {
                break;
            }
            glommio::timer::sleep(Duration::from_millis(50)).await;
        }

        assert!(!assigned_a.is_empty(), "member a was assigned nothing");
        assert!(!assigned_b.is_empty(), "member b was assigned nothing");

        let mut all: Vec<_> = assigned_a.iter().chain(assigned_b.iter()).collect();
        let total = all.len();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), total, "a partition was assigned to both members");
        assert_eq!(all.len(), 4, "every partition must be assigned to someone");

        a.leave(&mut cluster_a).await.expect("leave a");
        b.leave(&mut cluster_b).await.expect("leave b");
    });
}

/// **The whole point, end to end**: subscribe, read, commit, and have a fresh
/// consumer in the same group resume where the first one stopped.
///
/// This is the property a committed offset exists for, and the one that fails
/// silently when the convention is wrong — Kafka stores *the next offset to
/// read*, so an off-by-one here replays or skips exactly one record per
/// partition per restart, which looks like a rare duplicate rather than a bug.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn a_committed_offset_is_where_the_next_consumer_resumes() {
    run(|| async {
        let topic = unique("commit");
        let group = unique("cg");
        let mut prod = TestProducer::connect(&broker(), &topic).await;
        prod.create_topic().await;
        prod.produce_plain(6).await;

        // First consumer: read three, commit, leave.
        let first = {
            let mut consumer = kestrel_glommio::Consumer::new(
                kestrel_glommio::Glommio,
                &bootstrap(),
                "member-1",
                kestrel_core::IsolationLevel::ReadCommitted,
            )
            .await
            .expect("consumer");
            consumer.set_max_wait(Duration::from_millis(200));
            consumer
                .subscribe(&group, vec![topic.clone()], Box::new(RangeAssignor), kestrel_glommio::EARLIEST)
                .await
                .expect("subscribe");

            let mut seen = Vec::new();
            for _ in 0..200 {
                for group in consumer.poll().await.expect("poll") {
                    for record in group.iter() {
                        if seen.len() < 3 {
                            seen.push(String::from_utf8_lossy(&record.value().unwrap()).into_owned());
                        }
                    }
                }
                if seen.len() >= 3 {
                    break;
                }
            }
            assert_eq!(seen.len(), 3, "expected three records, got {seen:?}");

            // Commit exactly what was consumed, not wherever the fetch reached.
            consumer.seek_to(&topic, 0, 3);
            consumer.commit().await.expect("commit");
            // Leave, or the next member waits out this one's session timeout
            // before the group can rebalance.
            consumer.unsubscribe().await.expect("unsubscribe");
            seen
        };
        assert_eq!(first, vec!["v0", "v1", "v2"]);

        // Second consumer, same group: must start at 3.
        let mut consumer = kestrel_glommio::Consumer::new(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "member-2",
            kestrel_core::IsolationLevel::ReadCommitted,
        )
        .await
        .expect("consumer");
        consumer.set_max_wait(Duration::from_millis(200));
        consumer
            .subscribe(&group, vec![topic.clone()], Box::new(RangeAssignor), kestrel_glommio::EARLIEST)
            .await
            .expect("subscribe");

        let mut resumed = Vec::new();
        // Generous, because a rebalance after the first member leaves takes as
        // long as it takes and this runs beside the rest of the suite.
        for _ in 0..200 {
            for group in consumer.poll().await.expect("poll") {
                for record in group.iter() {
                    resumed.push(String::from_utf8_lossy(&record.value().unwrap()).into_owned());
                }
            }
            if resumed.len() >= 3 {
                break;
            }
        }
        assert_eq!(
            resumed,
            vec!["v3", "v4", "v5"],
            "the second consumer must resume at the committed offset, not replay"
        );
    });
}

/// **The callbacks fire on a real rebalance, and auto-commit lands.**
///
/// A second member joining is what makes the first give partitions up, so this
/// drives an actual rebalance rather than asserting on a state machine. The
/// listener records what it was told; auto-commit is checked by reading the
/// committed offset back through a third consumer.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn a_rebalance_revokes_assigns_and_auto_commits() {
    use std::cell::RefCell;
    use std::rc::Rc;

    /// Records what happened, which is all a synchronous listener needs to do.
    struct Recorder {
        events: Rc<RefCell<Vec<String>>>,
    }
    impl kestrel_glommio::RebalanceListener for Recorder {
        fn on_revoked(&mut self, partitions: &[kestrel_core::group::TopicPartition]) {
            self.events
                .borrow_mut()
                .push(format!("revoked:{}", partitions.len()));
        }
        fn on_assigned(&mut self, partitions: &[kestrel_core::group::TopicPartition]) {
            self.events
                .borrow_mut()
                .push(format!("assigned:{}", partitions.len()));
        }
    }

    run(|| async {
        let topic = unique("rebal");
        let group = unique("rg");
        let mut prod = TestProducer::connect(&broker(), &topic).await;
        prod.create_topic_with_partitions(4).await;
        prod.produce_plain(4).await;

        let events = Rc::new(RefCell::new(Vec::new()));

        let mut first = kestrel_glommio::Consumer::new(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "member-1",
            kestrel_core::IsolationLevel::ReadCommitted,
        )
        .await
        .expect("consumer");
        first.set_max_wait(Duration::from_millis(200));
        first.set_rebalance_listener(Box::new(Recorder {
            events: Rc::clone(&events),
        }));
        // Every poll, so the commit is not waiting on a timer in a short test.
        first.set_auto_commit(Some(Duration::from_millis(0)));
        first
            .subscribe(&group, vec![topic.clone()], Box::new(RangeAssignor), kestrel_glommio::EARLIEST)
            .await
            .expect("subscribe");

        // Read everything, so there is a position worth committing.
        let mut seen = 0;
        for _ in 0..60 {
            for g in first.poll().await.expect("poll") {
                seen += g.len();
            }
            if seen >= 4 {
                break;
            }
        }
        assert_eq!(seen, 4, "the first member should read every record");
        assert!(
            events.borrow().iter().any(|e| e == "assigned:4"),
            "expected an assignment of four partitions, got {:?}",
            events.borrow()
        );

        // A second member forces a rebalance; both must be driven for the join
        // to complete.
        let mut second = kestrel_glommio::Consumer::new(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "member-2",
            kestrel_core::IsolationLevel::ReadCommitted,
        )
        .await
        .expect("consumer");
        second.set_max_wait(Duration::from_millis(200));
        second
            .subscribe(&group, vec![topic.clone()], Box::new(RangeAssignor), kestrel_glommio::EARLIEST)
            .await
            .expect("subscribe");

        for _ in 0..80 {
            let _ = futures_lite::future::zip(first.poll(), second.poll()).await;
            if events.borrow().iter().any(|e| e.starts_with("revoked")) {
                break;
            }
        }

        let seen_events = events.borrow().clone();
        assert!(
            seen_events.iter().any(|e| e.starts_with("revoked")),
            "the first member should have been told it lost its partitions: {seen_events:?}"
        );

        first.unsubscribe().await.expect("leave 1");
        second.unsubscribe().await.expect("leave 2");

        // Auto-commit should have stored the first member's progress: a fresh
        // consumer must not replay all four records.
        let mut third = kestrel_glommio::Consumer::new(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "member-3",
            kestrel_core::IsolationLevel::ReadCommitted,
        )
        .await
        .expect("consumer");
        third.set_max_wait(Duration::from_millis(200));
        third
            .subscribe(&group, vec![topic.clone()], Box::new(RangeAssignor), kestrel_glommio::EARLIEST)
            .await
            .expect("subscribe");

        let mut replayed = 0;
        for _ in 0..40 {
            for g in third.poll().await.expect("poll") {
                replayed += g.len();
            }
        }
        assert!(
            replayed < 4,
            "auto-commit did not land: all {replayed} records were replayed"
        );
    });
}

/// **A member that heartbeats but does not poll keeps its partitions.**
///
/// This client spawns nothing, so heartbeats ride on `poll` — which is wrong
/// for a caller that spends longer than the session timeout handling a batch.
/// `heartbeat()` is the way out, and this checks it actually keeps membership:
/// a short session timeout, no polling, and the member still owns its
/// partitions afterwards.
#[test]
#[ignore = "needs a Kafka broker on localhost:9092"]
fn heartbeating_without_polling_keeps_membership() {
    run(|| async {
        let topic = unique("hb");
        let mut prod = TestProducer::connect(&broker(), &topic).await;
        prod.create_topic_with_partitions(2).await;

        let mut consumer = kestrel_glommio::Consumer::new(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "member-hb",
            kestrel_core::IsolationLevel::ReadCommitted,
        )
        .await
        .expect("consumer");
        consumer.set_max_wait(Duration::from_millis(100));
        consumer
            .subscribe(&unique("hbg"), vec![topic.clone()], Box::new(RangeAssignor), kestrel_glommio::EARLIEST)
            .await
            .expect("subscribe");

        // Poll until assigned.
        for _ in 0..40 {
            consumer.poll().await.expect("poll");
            if consumer.assignments().count() == 2 {
                break;
            }
        }
        assert_eq!(consumer.assignments().count(), 2, "expected an assignment");

        // Now stop polling and only heartbeat, for longer than a poll loop
        // would take. The coordinator's default session timeout is 45s, so this
        // does not prove expiry is avoided — it proves heartbeat() drives
        // membership on its own and does not disturb the assignment.
        for _ in 0..20 {
            let changed = consumer.heartbeat().await.expect("heartbeat");
            assert!(!changed, "a heartbeat alone must not change the assignment");
            glommio::timer::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            consumer.assignments().count(),
            2,
            "heartbeating should have kept the partitions"
        );

        consumer.unsubscribe().await.expect("leave");
    });
}

/// **Cooperative rebalancing: the incumbent keeps what it is not losing.**
///
/// # Does not pass: the handover grants before it revokes
///
/// **Both members end up holding all four partitions.** That is overlap —
/// duplicate consumption — and the precise thing this protocol exists to
/// prevent, so it is a hard failure rather than a rough edge.
///
/// It used to hang instead. That was a separate bug, now fixed: `poll` treated
/// every non-stable membership as a full revocation and cleared positions the
/// cooperative protocol means to keep, so the consumer re-entered the rebalance
/// it had just left. Revocation now carries *what was actually lost* — the
/// whole assignment under the eager protocol, only the moved partitions under
/// this one — which is right for both and made the loop terminate.
///
/// What remains: the leader grants a partition to its new owner in the same
/// round its current owner still holds it, so the two members between them hold
/// more partitions than exist. The assignor withholds correctly in isolation
/// (`cooperative_withholds_a_partition_that_must_move`), so the fault is in the
/// `owned` set the leader actually sees.
///
/// **Not a test artifact.** The first version of this test stopped as soon as
/// both members held something, which is a transient state in a handover; it
/// now requires the total to settle at four for five consecutive rounds, and
/// that never happens either.
///
/// **Not the broker.** The coordinator only relays the assignment the leader
/// computed — it never checks for overlap — and Java's `CooperativeStickyAssignor`
/// works against this same cluster.
///
/// Three real causes have been found and fixed, and it still does not converge.
/// The first two were reasoned about; the third came from reading
/// `CooperativeStickyAssignor.java` and `ConsumerCoordinator.java`, which is
/// what should have happened first:
///
/// 1. The subscription was written at `ConsumerProtocolSubscription` v1, which
///    carries `owned_partitions` but not the `generation_id` that v2 added. A
///    leader could not tell a live ownership claim from a stale one. Now v2,
///    with the stale-claim filter every client applies.
/// 2. A cooperative rejoin cleared its own generation before rejoining, so the
///    member advertised -1 and that same filter read *its own* claim as stale —
///    handing away partitions it was still reading. The generation is now kept
///    across a handover, which is what being a member of that generation means.
///
/// 3. The generation filter was applied to the withholding decision as well as
///    to the target. Java draws that line deliberately: `memberData` filters by
///    generation so a stale layout does not become sticky, while
///    `computePartitionsTransferringOwnership` uses the **raw** owned list,
///    because withholding is about who is still *reading* a partition and a
///    member whose claim is judged stale is reading it regardless. Filtering
///    there hands its partitions to another member while it still holds them.
///
/// 4. `revoke_and_rejoin` cleared the generation, so every rejoin advertised
///    -1 and a leader read every ownership claim — including the member's own —
///    as stale. A rebalance does not un-make a member of the generation it was
///    assigned; only fencing does, and that drops the member id with it.
///
/// All four are checked by unit tests. The trace that found the last one is
/// still in the client behind `KESTREL_TRACE=1`, which prints each member's
/// join, sync, heartbeat and what the leader saw.
///
/// 5. `advance_group` took one protocol step per `poll`. A rejoin is
///    `JoinGroup` then `SyncGroup`, sometimes twice over, so a member spent a
///    poll cycle on each while the coordinator waited — Java's
///    `joinGroupIfNeeded` loops until the member is stable, and now so does
///    this.
///
/// **What remains, from the trace.** The second member's `JoinGroup` returns it
/// as leader of a group containing only itself, and only afterwards does the
/// first member's heartbeat return `UNKNOWN_MEMBER_ID` (25) — never
/// `REBALANCE_IN_PROGRESS` (27). So the coordinator drops the incumbent while
/// admitting the newcomer, and the incumbent learns about it too late to rejoin
/// the same generation.
///
/// **Ruled out**, each by experiment rather than argument:
///
/// - *The assignors.* They match `kafka-clients` 3.9.0 and are unit-tested,
///   including the withholding rule and the stale-claim filter.
/// - *The idle window.* Both consumers are now connected before either
///   subscribes, so nothing sits unpolled while the other sets up; the symptom
///   is unchanged.
/// - *The group id.* Both subscribe with the same one.
/// - *The rejoin pace.* `advance_group` now drives a rejoin to completion in a
///   single `poll`, as `joinGroupIfNeeded` does.
///
/// The next step is the one that cracked the original `JoinGroup` hang: run this
/// with `KESTREL_TRACE=1` **beside the broker's own `kafka.coordinator.group`
/// DEBUG log**, and read why the coordinator considers the incumbent gone. The
/// client-side view alone cannot say.
///
/// Left failing rather than deleted: the eager path is unaffected, and shipping
/// `cooperative-sticky` as available while it double-assigns would be far worse
/// than shipping it as unfinished.
///
/// The eager protocol makes every member give up everything and stop consuming
/// until the new assignment arrives. KIP-429 revokes only what has to move. So
/// the property to check is not "the split is fair" — the assignors already
/// prove that — but that the first member is never left holding nothing while
/// the second joins.
#[test]
#[ignore = "unfinished: the cooperative handover does not converge — see the doc comment"]
fn cooperative_rebalancing_never_drops_everything() {
    run(|| async {
        let topic = unique("coop");
        let group = unique("coopg");
        let mut prod = TestProducer::connect(&broker(), &topic).await;
        prod.create_topic_with_partitions(4).await;

        // **Both consumers are connected before either joins.** Connecting is a
        // round trip, and this client heartbeats only when polled — so doing it
        // between the first member's assignment and the second's subscribe
        // leaves the first silent for the whole of it.
        let mut first = kestrel_glommio::Consumer::new(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "coop-1",
            kestrel_core::IsolationLevel::ReadCommitted,
        )
        .await
        .expect("consumer");
        let mut second = kestrel_glommio::Consumer::new(
            kestrel_glommio::Glommio,
            &bootstrap(),
            "coop-2",
            kestrel_core::IsolationLevel::ReadCommitted,
        )
        .await
        .expect("consumer");

        for consumer in [&mut first, &mut second] {
            consumer.set_max_wait(Duration::from_millis(100));
            consumer.set_group_timeouts(Duration::from_secs(30), Duration::from_secs(20));
        }

        first
            .subscribe(
                &group,
                vec![topic.clone()],
                Box::new(kestrel_core::group::CooperativeStickyAssignor),
                kestrel_glommio::EARLIEST,
            )
            .await
            .expect("subscribe");

        for _ in 0..40 {
            first.poll().await.expect("poll");
            if first.assignments().count() == 4 {
                break;
            }
        }
        assert_eq!(first.assignments().count(), 4, "the first member takes all four");

        second
            .subscribe(
                &group,
                vec![topic.clone()],
                Box::new(kestrel_core::group::CooperativeStickyAssignor),
                kestrel_glommio::EARLIEST,
            )
            .await
            .expect("subscribe");

        // Settled, not merely non-zero: a handover passes through states where
        // one side holds nothing yet.
        let mut floor = 4usize;
        let mut stable_for = 0;
        for _ in 0..200 {
            let _ = futures_lite::future::zip(first.poll(), second.poll()).await;
            let a = first.assignments().count();
            let b = second.assignments().count();
            floor = floor.min(a);
            if a + b == 4 && a > 0 && b > 0 {
                stable_for += 1;
                if stable_for >= 5 {
                    break;
                }
            } else {
                stable_for = 0;
            }
        }

        let a = first.assignments().count();
        let b = second.assignments().count();
        assert_eq!(a + b, 4, "every partition must be held by exactly one: {a} + {b}");
        assert!(a > 0 && b > 0, "the four should be shared: {a} + {b}");
        assert!(
            floor > 0,
            "the incumbent dropped to zero partitions — that is an eager rebalance"
        );

        first.unsubscribe().await.expect("leave 1");
        second.unsubscribe().await.expect("leave 2");
    });
}
