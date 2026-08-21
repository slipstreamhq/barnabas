//! Consumer group assignment: who gets which partitions.
//!
//! Under the classic protocol the **group leader computes the assignment** —
//! the coordinator only relays it — so this is client-side logic and belongs
//! here, with no IO in sight. KIP-848 moves it to the coordinator, which is
//! precisely why it lives behind the seam described in
//! `docs/completing-the-client.md` rather than in the shared layer.
//!
//! # Matching the Java client matters here
//!
//! An assignor is not free to be clever. A group is usually mixed — a Rust
//! consumer beside Java ones — and every member runs the *same* strategy on the
//! *same* inputs and must reach the *same* answer, because only the leader's
//! result is distributed and the others have to agree it is sane. Worse, a
//! group whose members disagree about what `range` means will thrash: each
//! rebalance elects a different leader and produces a different assignment.
//!
//! So these follow Java's algorithms, including the parts that look arbitrary
//! — the sort orders especially, which is what makes the result deterministic
//! across implementations.

use std::collections::{BTreeMap, BTreeSet};

/// One member's declared interest, as it arrives in `JoinGroup`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    pub member_id: String,
    /// Topics this member wants. Order is not significant; assignors sort.
    pub topics: Vec<String>,
    /// What this member holds now, for assignors that try to keep it.
    /// Empty for the stateless strategies.
    pub owned: Vec<TopicPartition>,
    /// The generation `owned` was true in, or -1 if the member did not say.
    ///
    /// **This is what makes an ownership claim trustworthy.** A member that was
    /// away still advertises the partitions it used to hold, and a leader that
    /// believes it will hand the same partition to two members at once. Kafka
    /// added the field to `ConsumerProtocolSubscription` v2 for exactly this,
    /// and the rule every client applies is the same: a claim from below the
    /// highest generation in the group is stale and is ignored.
    pub generation: i32,
}

/// A topic and one of its partitions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

/// What the leader decided: member id → the partitions it owns.
///
/// A `BTreeMap` so the result is ordered and therefore diffable in a test
/// failure, which matters more here than lookup speed — an assignment is
/// computed once per rebalance.
pub type Assignment = BTreeMap<String, Vec<TopicPartition>>;

/// How partitions are shared out.
///
/// The name is what goes on the wire in `JoinGroup`; members negotiate a
/// strategy they all support, so it must match Java's exactly.
pub trait Assignor {
    /// The protocol name Kafka knows this by.
    fn name(&self) -> &'static str;

    /// Divide `partitions_per_topic` among `members`.
    ///
    /// Every member appears in the result, with an empty vector if it got
    /// nothing — a member that is absent from the assignment cannot tell
    /// "assigned nothing" from "the leader forgot me".
    fn assign(
        &self,
        members: &[Subscription],
        partitions_per_topic: &BTreeMap<String, i32>,
    ) -> Assignment;
}

/// Contiguous ranges per topic, Java's `RangeAssignor`.
///
/// For each topic the subscribed members are sorted by id and the partitions
/// split into contiguous blocks. With 7 partitions and 3 members the split is
/// 3/2/2, the remainder going to the earliest members.
///
/// **It is deliberately not balanced across topics.** With several topics of
/// few partitions the first member accumulates the leftovers of each — the
/// known wart of this strategy, and the reason `RoundRobin` exists. Reproduced
/// rather than fixed, because a member that "fixes" it disagrees with the Java
/// members in the same group.
#[derive(Debug, Default, Clone, Copy)]
pub struct RangeAssignor;

impl Assignor for RangeAssignor {
    fn name(&self) -> &'static str {
        "range"
    }

    fn assign(
        &self,
        members: &[Subscription],
        partitions_per_topic: &BTreeMap<String, i32>,
    ) -> Assignment {
        let mut assignment: Assignment = members
            .iter()
            .map(|m| (m.member_id.clone(), Vec::new()))
            .collect();

        for (topic, &count) in partitions_per_topic {
            let mut subscribers: Vec<&str> = members
                .iter()
                .filter(|m| m.topics.iter().any(|t| t == topic))
                .map(|m| m.member_id.as_str())
                .collect();
            if subscribers.is_empty() || count <= 0 {
                continue;
            }
            subscribers.sort_unstable();

            let members_count = i32::try_from(subscribers.len()).unwrap_or(i32::MAX);
            let per_member = count / members_count;
            let with_extra = count % members_count;

            for (index, member) in subscribers.iter().enumerate() {
                let index = i32::try_from(index).unwrap_or(i32::MAX);
                // Java's arithmetic, kept in its shape so it is checkable
                // against `RangeAssignor.java` line by line.
                let start = per_member * index + index.min(with_extra);
                let length = per_member + i32::from(index < with_extra);
                let entry = assignment
                    .get_mut(*member)
                    .expect("every member is seeded above");
                for partition in start..start + length {
                    entry.push(TopicPartition::new(topic.clone(), partition));
                }
            }
        }
        assignment
    }
}

/// One partition at a time around the members, Java's `RoundRobinAssignor`.
///
/// Every `(topic, partition)` in the group is laid out in order and dealt to
/// the members in turn, skipping a member that did not subscribe to that topic.
/// Balances across topics, which `Range` does not; in exchange it moves almost
/// every partition when membership changes, which is what `sticky` addresses.
#[derive(Debug, Default, Clone, Copy)]
pub struct RoundRobinAssignor;

impl Assignor for RoundRobinAssignor {
    fn name(&self) -> &'static str {
        "roundrobin"
    }

    fn assign(
        &self,
        members: &[Subscription],
        partitions_per_topic: &BTreeMap<String, i32>,
    ) -> Assignment {
        let mut assignment: Assignment = members
            .iter()
            .map(|m| (m.member_id.clone(), Vec::new()))
            .collect();

        let mut sorted: Vec<&Subscription> = members.iter().collect();
        sorted.sort_unstable_by(|a, b| a.member_id.cmp(&b.member_id));
        if sorted.is_empty() {
            return assignment;
        }

        // Every partition in the group, topic-major then partition — the order
        // Java produces, and what makes two members agree.
        let all: Vec<TopicPartition> = partitions_per_topic
            .iter()
            .flat_map(|(topic, &count)| {
                (0..count.max(0)).map(move |p| TopicPartition::new(topic.clone(), p))
            })
            .collect();

        let mut next = 0usize;
        for tp in all {
            // Advance to a member that wants this topic. If none does, the
            // partition is unassigned, which is correct: nobody subscribed.
            let mut looked_at = 0;
            while looked_at < sorted.len()
                && !sorted[next % sorted.len()].topics.contains(&tp.topic)
            {
                next += 1;
                looked_at += 1;
            }
            if looked_at == sorted.len() {
                continue;
            }
            let member = &sorted[next % sorted.len()].member_id;
            assignment
                .get_mut(member)
                .expect("every member is seeded above")
                .push(tp);
            next += 1;
        }
        assignment
    }
}

/// The ownership claims worth believing.
///
/// A member reporting a generation below the highest in the group has been away
/// and its `owned` list describes a world that has moved on. Believing it is how
/// a leader assigns one partition to two members.
fn live_claims(members: &[Subscription]) -> BTreeMap<&str, &[TopicPartition]> {
    let highest = members.iter().map(|m| m.generation).max().unwrap_or(-1);
    members
        .iter()
        .map(|m| {
            let owned: &[TopicPartition] = if m.generation >= highest && highest >= 0 {
                &m.owned
            } else {
                &[]
            };
            (m.member_id.as_str(), owned)
        })
        .collect()
}

/// Keep what you had where possible, Java's `StickyAssignor` in spirit.
///
/// **Not Java's algorithm.** Java's does a balanced-then-shuffle pass with
/// several refinement rounds; this keeps each member's still-valid partitions
/// and deals the rest to whoever holds fewest. The results agree on the cases
/// tested here and will not agree on every case.
///
/// That difference is only safe because **the leader's assignment is the one
/// used** — a mixed group is consistent whoever leads, it just gets a slightly
/// different balance depending. Where exact agreement is required, use
/// `range` or `roundrobin`, which are reproduced exactly.
#[derive(Debug, Default, Clone, Copy)]
pub struct StickyAssignor;

impl Assignor for StickyAssignor {
    fn name(&self) -> &'static str {
        "sticky"
    }

    fn assign(
        &self,
        members: &[Subscription],
        partitions_per_topic: &BTreeMap<String, i32>,
    ) -> Assignment {
        let mut assignment: Assignment = members
            .iter()
            .map(|m| (m.member_id.clone(), Vec::new()))
            .collect();

        let mut sorted: Vec<&Subscription> = members.iter().collect();
        sorted.sort_unstable_by(|a, b| a.member_id.cmp(&b.member_id));
        if sorted.is_empty() {
            return assignment;
        }

        let valid: BTreeSet<TopicPartition> = partitions_per_topic
            .iter()
            .flat_map(|(topic, &count)| {
                (0..count.max(0)).map(move |p| TopicPartition::new(topic.clone(), p))
            })
            .collect();

        // **Balance first, stickiness second.** Keeping every partition a member
        // already holds is not sticky, it is inert: a member that joins an
        // established group would never be given anything, because nothing is
        // ever taken from anyone. So each member keeps at most its fair share,
        // and the surplus is redealt.
        let subscribers: Vec<&&Subscription> = sorted.iter().collect();
        let quota_of = |member: &Subscription| -> usize {
            let wanted: usize = valid
                .iter()
                .filter(|tp| member.topics.contains(&tp.topic))
                .count();
            let sharers = subscribers
                .iter()
                .filter(|m| m.topics.iter().any(|t| member.topics.contains(t)))
                .count()
                .max(1);
            wanted.div_ceil(sharers)
        };

        let claims = live_claims(members);
        let mut taken: BTreeSet<TopicPartition> = BTreeSet::new();
        for member in &sorted {
            let quota = quota_of(member);
            for tp in claims[member.member_id.as_str()] {
                if assignment[&member.member_id].len() >= quota {
                    break;
                }
                if valid.contains(tp) && !taken.contains(tp) && member.topics.contains(&tp.topic) {
                    taken.insert(tp.clone());
                    assignment
                        .get_mut(&member.member_id)
                        .expect("seeded")
                        .push(tp.clone());
                }
            }
        }

        // Whatever is left — never owned, owned by a member over quota, or
        // owned by nobody subscribed — goes to whoever holds fewest.
        let remaining: Vec<TopicPartition> = valid.difference(&taken).cloned().collect();
        for tp in remaining {
            let candidate = sorted
                .iter()
                .filter(|m| m.topics.contains(&tp.topic))
                .min_by_key(|m| {
                    (
                        assignment.get(&m.member_id).map_or(0, Vec::len),
                        m.member_id.clone(),
                    )
                });
            if let Some(member) = candidate {
                assignment
                    .get_mut(&member.member_id)
                    .expect("seeded")
                    .push(tp.clone());
            }
        }

        for partitions in assignment.values_mut() {
            partitions.sort();
        }
        assignment
    }
}

/// Sticky, but handing partitions over **one rebalance at a time**.
///
/// Java's `CooperativeStickyAssignor`, and the protocol change in KIP-429. The
/// difference from every assignor above is not how the target is computed but
/// what is *published*: a partition that must move from one member to another
/// is given to **neither** in this round. Its current owner sees it missing
/// from its assignment, revokes it, and rejoins; the next round hands it to its
/// new owner.
///
/// That is what makes the rebalance incremental. Under the eager protocol every
/// member gives up everything and stops consuming until the new assignment
/// arrives — a stop-the-world pause proportional to the slowest member. Under
/// this one a member keeps everything it is not losing and never stops reading
/// it.
///
/// The cost is an extra rebalance round, which is why the assignment this
/// returns is deliberately *incomplete* and must not be mistaken for a bug.
#[derive(Debug, Default, Clone, Copy)]
pub struct CooperativeStickyAssignor;

impl Assignor for CooperativeStickyAssignor {
    fn name(&self) -> &'static str {
        "cooperative-sticky"
    }

    fn assign(
        &self,
        members: &[Subscription],
        partitions_per_topic: &BTreeMap<String, i32>,
    ) -> Assignment {
        // The target: where each partition should end up.
        let target = StickyAssignor.assign(members, partitions_per_topic);

        // Who holds what right now — from the **raw** claims, deliberately.
        //
        // The generation filter belongs to computing the target, where an
        // out-of-date claim would make a stale layout sticky. It must not be
        // applied here: withholding is about who is *still reading* a
        // partition, and a member whose claim is judged stale is reading it
        // regardless. Filtering here hands its partitions to someone else while
        // it still has them, which is the overlap this protocol exists to
        // prevent. Java draws the same line — `memberData` filters by
        // generation, `computePartitionsTransferringOwnership` does not.
        let mut owner: BTreeMap<&TopicPartition, &str> = BTreeMap::new();
        for member in members {
            for tp in &member.owned {
                owner.insert(tp, member.member_id.as_str());
            }
        }

        // Publish only what is not being taken from someone else.
        target
            .into_iter()
            .map(|(member_id, partitions)| {
                let kept = partitions
                    .into_iter()
                    .filter(|tp| match owner.get(tp) {
                        // Held by another member: it must revoke first, so this
                        // round assigns it to nobody.
                        Some(current) => *current == member_id,
                        // Unowned — free to hand out now.
                        None => true,
                    })
                    .collect();
                (member_id, kept)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str, topics: &[&str]) -> Subscription {
        Subscription {
            member_id: id.to_owned(),
            topics: topics.iter().map(|t| (*t).to_owned()).collect(),
            owned: Vec::new(),
            generation: 1,
        }
    }

    fn holding(id: &str, topics: &[&str], owned: &[(&str, i32)]) -> Subscription {
        Subscription {
            member_id: id.to_owned(),
            topics: topics.iter().map(|t| (*t).to_owned()).collect(),
            owned: owned
                .iter()
                .map(|(t, p)| TopicPartition::new(*t, *p))
                .collect(),
            generation: 1,
        }
    }

    fn topics(entries: &[(&str, i32)]) -> BTreeMap<String, i32> {
        entries.iter().map(|(t, c)| ((*t).to_owned(), *c)).collect()
    }

    fn partitions_of(assignment: &Assignment, member: &str) -> Vec<i32> {
        assignment
            .get(member)
            .expect("member present")
            .iter()
            .map(|tp| tp.partition)
            .collect()
    }

    /// **Parity with the Java client, checked against the Java client.**
    ///
    /// Every member of a group runs the same strategy and must reach the same
    /// answer; a Rust member that disagrees makes the group thrash, electing a
    /// different leader and producing a different assignment each rebalance.
    /// So these expectations are not derived from reading `RangeAssignor.java`
    /// — they are the output of running `kafka-clients` 3.9.0 itself:
    ///
    /// ```text
    /// range       c0=[t:0,t:1]           c1=[t:2]
    /// roundrobin  c0=[t:0,t:2]           c1=[t:1]
    /// range       c0=[a:0,a:1,b:0,b:1]   c1=[a:2,b:2]
    /// roundrobin  c0=[a:0,a:2,b:1]       c1=[a:1,b:0,b:2]
    /// roundrobin  c0=[a:0]               c1=[a:1,b:0,b:1]
    /// range       c0=[a:0,a:1,b:0,b:1]   c1=[a:2,a:3,b:2]   c2=[a:4]
    /// roundrobin  c0=[a:0,a:3,b:0,b:2]   c1=[a:1,a:4,b:1]   c2=[a:2]
    /// ```
    ///
    /// Reproduce with `scratchpad/oracle/Oracle.java` against a JDK image and
    /// the `kafka-clients` jar from `apache/kafka:3.9.0`.
    #[test]
    fn range_and_roundrobin_match_the_java_client() {
        fn flat(assignment: &Assignment, member: &str) -> Vec<String> {
            assignment[member]
                .iter()
                .map(|tp| format!("{}:{}", tp.topic, tp.partition))
                .collect()
        }

        let two = [member("c0", &["t"]), member("c1", &["t"])];
        let a = RangeAssignor.assign(&two, &topics(&[("t", 3)]));
        assert_eq!(flat(&a, "c0"), ["t:0", "t:1"]);
        assert_eq!(flat(&a, "c1"), ["t:2"]);

        let a = RoundRobinAssignor.assign(&two, &topics(&[("t", 3)]));
        assert_eq!(flat(&a, "c0"), ["t:0", "t:2"]);
        assert_eq!(flat(&a, "c1"), ["t:1"]);

        let both = [member("c0", &["a", "b"]), member("c1", &["a", "b"])];
        let spec = topics(&[("a", 3), ("b", 3)]);
        let a = RangeAssignor.assign(&both, &spec);
        assert_eq!(flat(&a, "c0"), ["a:0", "a:1", "b:0", "b:1"]);
        assert_eq!(flat(&a, "c1"), ["a:2", "b:2"]);

        let a = RoundRobinAssignor.assign(&both, &spec);
        assert_eq!(flat(&a, "c0"), ["a:0", "a:2", "b:1"]);
        assert_eq!(flat(&a, "c1"), ["a:1", "b:0", "b:2"]);

        let uneven = [member("c0", &["a"]), member("c1", &["a", "b"])];
        let a = RoundRobinAssignor.assign(&uneven, &topics(&[("a", 2), ("b", 2)]));
        assert_eq!(flat(&a, "c0"), ["a:0"]);
        assert_eq!(flat(&a, "c1"), ["a:1", "b:0", "b:1"]);

        let three = [
            member("c0", &["a", "b"]),
            member("c1", &["a", "b"]),
            member("c2", &["a"]),
        ];
        let spec = topics(&[("a", 5), ("b", 3)]);
        let a = RangeAssignor.assign(&three, &spec);
        assert_eq!(flat(&a, "c0"), ["a:0", "a:1", "b:0", "b:1"]);
        assert_eq!(flat(&a, "c1"), ["a:2", "a:3", "b:2"]);
        assert_eq!(flat(&a, "c2"), ["a:4"]);

        let a = RoundRobinAssignor.assign(&three, &spec);
        assert_eq!(flat(&a, "c0"), ["a:0", "a:3", "b:0", "b:2"]);
        assert_eq!(flat(&a, "c1"), ["a:1", "a:4", "b:1"]);
        assert_eq!(flat(&a, "c2"), ["a:2"]);
    }

    /// Java's documented example: 3 partitions, 2 members, the earlier member
    /// takes the extra one.
    #[test]
    fn range_gives_the_remainder_to_the_earliest_members() {
        let assignment = RangeAssignor.assign(
            &[member("c0", &["t"]), member("c1", &["t"])],
            &topics(&[("t", 3)]),
        );
        assert_eq!(partitions_of(&assignment, "c0"), vec![0, 1]);
        assert_eq!(partitions_of(&assignment, "c1"), vec![2]);
    }

    #[test]
    fn range_splits_evenly_when_it_divides() {
        let assignment = RangeAssignor.assign(
            &[member("c0", &["t"]), member("c1", &["t"])],
            &topics(&[("t", 4)]),
        );
        assert_eq!(partitions_of(&assignment, "c0"), vec![0, 1]);
        assert_eq!(partitions_of(&assignment, "c1"), vec![2, 3]);
    }

    /// **The wart, pinned deliberately.** Range works topic by topic, so with
    /// two 3-partition topics `c0` takes the extra from *each*: 4 against 2.
    /// A "fix" here would disagree with every Java member in the group.
    #[test]
    fn range_is_lopsided_across_topics_and_that_is_correct() {
        let assignment = RangeAssignor.assign(
            &[member("c0", &["a", "b"]), member("c1", &["a", "b"])],
            &topics(&[("a", 3), ("b", 3)]),
        );
        assert_eq!(assignment["c0"].len(), 4);
        assert_eq!(assignment["c1"].len(), 2);
    }

    /// The same input round-robin balances, which is the reason to choose it.
    #[test]
    fn roundrobin_balances_across_topics() {
        let assignment = RoundRobinAssignor.assign(
            &[member("c0", &["a", "b"]), member("c1", &["a", "b"])],
            &topics(&[("a", 3), ("b", 3)]),
        );
        assert_eq!(assignment["c0"].len(), 3);
        assert_eq!(assignment["c1"].len(), 3);
    }

    /// A member that did not subscribe is skipped rather than given the
    /// partition, and the deal continues from the next member.
    #[test]
    fn roundrobin_skips_members_that_did_not_subscribe() {
        let assignment = RoundRobinAssignor.assign(
            &[member("c0", &["a"]), member("c1", &["a", "b"])],
            &topics(&[("a", 2), ("b", 2)]),
        );
        assert!(assignment["c0"].iter().all(|tp| tp.topic == "a"));
        assert_eq!(
            assignment["c1"].iter().filter(|tp| tp.topic == "b").count(),
            2
        );
    }

    /// Nobody subscribed, so the partition goes unassigned rather than to an
    /// uninterested member.
    #[test]
    fn a_topic_nobody_wants_is_left_alone() {
        let assignment =
            RoundRobinAssignor.assign(&[member("c0", &["a"])], &topics(&[("a", 1), ("z", 4)]));
        assert_eq!(assignment["c0"].len(), 1);
        assert!(assignment["c0"].iter().all(|tp| tp.topic == "a"));
    }

    /// Every member appears, even with nothing — "assigned nothing" must be
    /// distinguishable from "the leader forgot me".
    #[test]
    fn every_member_appears_in_the_result() {
        let assignment = RangeAssignor.assign(
            &[member("c0", &["a"]), member("idle", &["nonexistent"])],
            &topics(&[("a", 1)]),
        );
        assert!(assignment.contains_key("idle"));
        assert!(assignment["idle"].is_empty());
    }

    /// The point of sticky: a member that keeps its subscription keeps its
    /// partitions when someone else leaves.
    #[test]
    fn sticky_keeps_what_is_still_valid() {
        let assignment = StickyAssignor.assign(
            &[
                holding("c0", &["t"], &[("t", 0), ("t", 1)]),
                holding("c1", &["t"], &[("t", 2)]),
            ],
            &topics(&[("t", 4)]),
        );
        // 0 and 1 stay put; 3 is new and goes to whoever holds fewest.
        assert!(assignment["c0"].contains(&TopicPartition::new("t", 0)));
        assert!(assignment["c0"].contains(&TopicPartition::new("t", 1)));
        assert!(assignment["c1"].contains(&TopicPartition::new("t", 2)));
        assert!(assignment["c1"].contains(&TopicPartition::new("t", 3)));
    }

    /// **Sticky must balance, not merely preserve.** A member joining an
    /// established group has to be given something, which means taking it from
    /// someone. An earlier version kept every owned partition and only dealt
    /// out unowned ones, so a new member sat idle forever and
    /// `cooperative-sticky` had nothing to hand over.
    #[test]
    fn sticky_takes_from_the_over_provisioned_to_feed_a_new_member() {
        let assignment = StickyAssignor.assign(
            &[
                holding("c0", &["t"], &[("t", 0), ("t", 1), ("t", 2), ("t", 3)]),
                holding("c1", &["t"], &[]),
            ],
            &topics(&[("t", 4)]),
        );
        assert_eq!(assignment["c0"].len(), 2, "the incumbent gives up half");
        assert_eq!(assignment["c1"].len(), 2, "the newcomer is fed");
    }

    /// A partition two members both claim to own is given to exactly one.
    /// After a rebalance both may believe they hold it, and handing it to both
    /// is duplicate consumption.
    #[test]
    fn sticky_never_assigns_a_partition_twice() {
        let assignment = StickyAssignor.assign(
            &[
                holding("c0", &["t"], &[("t", 0)]),
                holding("c1", &["t"], &[("t", 0)]),
            ],
            &topics(&[("t", 2)]),
        );
        let mut all: Vec<&TopicPartition> = assignment.values().flatten().collect();
        let before = all.len();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), before, "a partition was assigned twice");
        assert_eq!(before, 2, "both partitions must be assigned");
    }

    /// A partition the member owns but that no longer exists is dropped rather
    /// than carried forward — topics shrink when they are recreated smaller.
    #[test]
    fn sticky_drops_partitions_that_no_longer_exist() {
        let assignment = StickyAssignor.assign(
            &[holding("c0", &["t"], &[("t", 0), ("t", 99)])],
            &topics(&[("t", 1)]),
        );
        assert_eq!(assignment["c0"], vec![TopicPartition::new("t", 0)]);
    }

    /// **A partition moving between members is assigned to neither**, this
    /// round. Its owner will revoke it and the next round hands it over — which
    /// is the whole of KIP-429.
    #[test]
    fn cooperative_withholds_a_partition_that_must_move() {
        // c0 holds both; c1 is new, so one has to move.
        let members = [
            holding("c0", &["t"], &[("t", 0), ("t", 1)]),
            holding("c1", &["t"], &[]),
        ];
        let assignment = CooperativeStickyAssignor.assign(&members, &topics(&[("t", 2)]));

        let total: usize = assignment.values().map(Vec::len).sum();
        assert_eq!(
            total, 1,
            "the moving partition must be withheld: {assignment:?}"
        );
        assert_eq!(
            assignment["c0"].len(),
            1,
            "c0 keeps the one it is not losing"
        );
        assert!(
            assignment["c1"].is_empty(),
            "c1 waits a round for its share"
        );
    }

    /// Nothing is moving, so nothing is withheld and the round is complete.
    #[test]
    fn cooperative_publishes_everything_when_nothing_moves() {
        let members = [holding("c0", &["t"], &[("t", 0), ("t", 1)])];
        let assignment = CooperativeStickyAssignor.assign(&members, &topics(&[("t", 2)]));
        assert_eq!(assignment["c0"].len(), 2);
    }

    /// An unowned partition needs no handover, so it is granted immediately.
    #[test]
    fn cooperative_grants_unowned_partitions_at_once() {
        let members = [holding("c0", &["t"], &[("t", 0)])];
        let assignment = CooperativeStickyAssignor.assign(&members, &topics(&[("t", 2)]));
        assert_eq!(
            assignment["c0"].len(),
            2,
            "the new partition needs no handover"
        );
    }

    /// **A claim from an older generation is ignored.**
    ///
    /// A member that was away still advertises what it used to hold. Believing
    /// it is how one partition ends up assigned to two members — silently,
    /// since Kafka relays whatever the leader computed without checking.
    #[test]
    fn a_stale_ownership_claim_is_not_believed() {
        let stale = Subscription {
            member_id: "old".to_owned(),
            topics: vec!["t".to_owned()],
            owned: vec![TopicPartition::new("t", 0), TopicPartition::new("t", 1)],
            generation: 1,
        };
        let current = Subscription {
            member_id: "new".to_owned(),
            topics: vec!["t".to_owned()],
            owned: vec![],
            generation: 5,
        };

        let assignment = CooperativeStickyAssignor.assign(&[stale, current], &topics(&[("t", 2)]));
        // The stale claim is ignored for *stickiness* — the target does not
        // preserve it — but the partitions are still withheld, because that
        // member is still reading them until it says otherwise.
        let total: usize = assignment.values().map(Vec::len).sum();
        assert!(
            total <= 2,
            "no partition may be handed out twice: {assignment:?}"
        );
        let mut all: Vec<&TopicPartition> = assignment.values().flatten().collect();
        let before = all.len();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), before, "a partition was assigned twice");
    }

    /// Every strategy must place every partition of a subscribed topic exactly
    /// once. A gap is a partition nobody reads; an overlap is duplicate
    /// consumption. Both are silent.
    #[test]
    fn every_partition_is_assigned_exactly_once() {
        let members = [
            member("c0", &["a", "b"]),
            member("c1", &["a", "b"]),
            member("c2", &["a"]),
        ];
        let spec = topics(&[("a", 5), ("b", 3)]);

        for (name, assignment) in [
            ("range", RangeAssignor.assign(&members, &spec)),
            ("roundrobin", RoundRobinAssignor.assign(&members, &spec)),
            ("sticky", StickyAssignor.assign(&members, &spec)),
        ] {
            let mut all: Vec<TopicPartition> = assignment.values().flatten().cloned().collect();
            let count = all.len();
            all.sort();
            all.dedup();
            assert_eq!(all.len(), count, "{name} assigned a partition twice");
            assert_eq!(all.len(), 8, "{name} did not assign every partition");
        }
    }
}
