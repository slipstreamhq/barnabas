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
        let mut assignment: Assignment =
            members.iter().map(|m| (m.member_id.clone(), Vec::new())).collect();

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
        let mut assignment: Assignment =
            members.iter().map(|m| (m.member_id.clone(), Vec::new())).collect();

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
        let mut assignment: Assignment =
            members.iter().map(|m| (m.member_id.clone(), Vec::new())).collect();

        let valid: BTreeSet<TopicPartition> = partitions_per_topic
            .iter()
            .flat_map(|(topic, &count)| {
                (0..count.max(0)).map(move |p| TopicPartition::new(topic.clone(), p))
            })
            .collect();

        // Keep what is still real, still subscribed, and not claimed twice.
        let mut taken: BTreeSet<TopicPartition> = BTreeSet::new();
        let mut sorted: Vec<&Subscription> = members.iter().collect();
        sorted.sort_unstable_by(|a, b| a.member_id.cmp(&b.member_id));

        for member in &sorted {
            for tp in &member.owned {
                if valid.contains(tp)
                    && !taken.contains(tp)
                    && member.topics.contains(&tp.topic)
                {
                    taken.insert(tp.clone());
                    assignment
                        .get_mut(&member.member_id)
                        .expect("seeded")
                        .push(tp.clone());
                }
            }
        }

        // Deal the rest to whoever holds fewest, breaking ties by member id so
        // the outcome does not depend on map iteration order.
        for tp in valid.difference(&taken.clone()) {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str, topics: &[&str]) -> Subscription {
        Subscription {
            member_id: id.to_owned(),
            topics: topics.iter().map(|t| (*t).to_owned()).collect(),
            owned: Vec::new(),
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
        }
    }

    fn topics(entries: &[(&str, i32)]) -> BTreeMap<String, i32> {
        entries
            .iter()
            .map(|(t, c)| ((*t).to_owned(), *c))
            .collect()
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
        assert_eq!(assignment["c1"].iter().filter(|tp| tp.topic == "b").count(), 2);
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
            let mut all: Vec<TopicPartition> =
                assignment.values().flatten().cloned().collect();
            let count = all.len();
            all.sort();
            all.dedup();
            assert_eq!(all.len(), count, "{name} assigned a partition twice");
            assert_eq!(all.len(), 8, "{name} did not assign every partition");
        }
    }
}
