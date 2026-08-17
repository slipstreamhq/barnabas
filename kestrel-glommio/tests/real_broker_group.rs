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
    for _ in 0..40 {
        match protocol.advance(cluster).await.expect("advance") {
            Membership::Assigned(partitions) => return partitions,
            Membership::InProgress | Membership::Revoked => {
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
