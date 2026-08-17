//! Group membership: the classic protocol's state machine, with no IO.
//!
//! The IO layer drives this — it sends what [`Step`] says to send, feeds the
//! answers back, and holds no protocol state of its own. That split is what
//! makes the fencing rules testable, and they are the rules worth testing:
//! every one of them fails *silently* when it is wrong, as duplicate
//! consumption or as committed offsets for partitions this member no longer
//! owns.
//!
//! # The shape of the classic protocol
//!
//! ```text
//! Unjoined ──JoinGroup──▶ Joining ──response──▶ Syncing ──response──▶ Stable
//!     ▲                                                                 │
//!     └──────────── rebalance, fencing, or lost coordinator ────────────┘
//! ```
//!
//! Two details that are easy to miss and expensive to get wrong:
//!
//! - **The first `JoinGroup` is expected to fail.** A member with no id sends an
//!   empty one, and the coordinator answers `MEMBER_ID_REQUIRED` *with* an id to
//!   use (KIP-394, which exists so a crash-looping member cannot fill a group
//!   with ghosts). That is a normal step, not an error.
//! - **Only the leader receives the member list.** Followers send an empty
//!   assignment in `SyncGroup` and are told theirs in the response.

use crate::group::{Assignment, Subscription, TopicPartition};

/// Coordinator error codes this machine reacts to.
pub mod codes {
    pub const NONE: i16 = 0;
    pub const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
    pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
    pub const NOT_COORDINATOR: i16 = 16;
    pub const ILLEGAL_GENERATION: i16 = 22;
    pub const UNKNOWN_MEMBER_ID: i16 = 25;
    pub const REBALANCE_IN_PROGRESS: i16 = 27;
    pub const FENCED_INSTANCE_ID: i16 = 82;
    pub const MEMBER_ID_REQUIRED: i16 = 79;
}

/// No generation yet, which is what the protocol calls -1.
pub const NO_GENERATION: i32 = -1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberState {
    /// Not a member: no id, or ours was rejected.
    Unjoined,
    /// `JoinGroup` is outstanding.
    Joining,
    /// `SyncGroup` is outstanding.
    Syncing,
    /// Assigned and heartbeating. **The only state in which offsets may be
    /// committed.**
    Stable,
}

/// What the IO layer should do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Send `JoinGroup` with this member id — empty the first time.
    Join { member_id: String },
    /// We are the leader: compute an assignment for these members and send it
    /// in `SyncGroup`.
    AssignAndSync { members: Vec<Subscription> },
    /// We are a follower: send `SyncGroup` with no assignment.
    Sync,
    /// Steady state.
    Heartbeat,
    /// Re-discover the coordinator, then carry on.
    FindCoordinator,
}

/// One member of one group.
#[derive(Debug, Clone)]
pub struct GroupMember {
    group_id: String,
    member_id: String,
    generation: i32,
    state: MemberState,
    topics: Vec<String>,
    assignment: Vec<TopicPartition>,
    leader: bool,
}

impl GroupMember {
    #[must_use]
    pub fn new(group_id: impl Into<String>, topics: Vec<String>) -> Self {
        Self {
            group_id: group_id.into(),
            member_id: String::new(),
            generation: NO_GENERATION,
            state: MemberState::Unjoined,
            topics,
            assignment: Vec::new(),
            leader: false,
        }
    }

    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    #[must_use]
    pub fn state(&self) -> MemberState {
        self.state
    }

    #[must_use]
    pub fn generation(&self) -> i32 {
        self.generation
    }

    #[must_use]
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.leader
    }

    /// What this member currently owns. Empty unless [`MemberState::Stable`].
    #[must_use]
    pub fn assignment(&self) -> &[TopicPartition] {
        &self.assignment
    }

    /// **Whether offsets may be committed right now.**
    ///
    /// Only in [`MemberState::Stable`]. Committing mid-rebalance is how a
    /// member writes an offset for a partition another member already owns —
    /// the coordinator would reject it as a stale generation, but only if the
    /// generation had already moved, and between revocation and rejoin it has
    /// not. The rule is cheaper than reasoning about the race.
    #[must_use]
    pub fn can_commit(&self) -> bool {
        self.state == MemberState::Stable && self.generation != NO_GENERATION
    }

    /// What to do next, given where we are.
    #[must_use]
    pub fn step(&self) -> Step {
        match self.state {
            MemberState::Unjoined | MemberState::Joining => Step::Join {
                member_id: self.member_id.clone(),
            },
            MemberState::Syncing => Step::Sync,
            MemberState::Stable => Step::Heartbeat,
        }
    }

    /// The subscription this member sends in `JoinGroup`.
    #[must_use]
    pub fn subscription(&self) -> Subscription {
        Subscription {
            member_id: self.member_id.clone(),
            topics: self.topics.clone(),
            // Sent so a sticky assignor can keep what we hold.
            owned: self.assignment.clone(),
        }
    }

    /// Change what this member wants to read. Forces a rejoin, because the
    /// group has to agree on the subscription before it can be assigned.
    pub fn set_topics(&mut self, topics: Vec<String>) {
        if topics != self.topics {
            self.topics = topics;
            self.revoke_and_rejoin();
        }
    }

    /// `JoinGroup` answered.
    ///
    /// `members` is non-empty only for the leader. Returns the next step.
    pub fn on_join(
        &mut self,
        error: i16,
        generation: i32,
        member_id: &str,
        leader_id: &str,
        members: Vec<Subscription>,
    ) -> Step {
        match error {
            codes::NONE => {
                self.member_id = member_id.to_owned();
                self.generation = generation;
                self.leader = leader_id == member_id;
                self.state = MemberState::Syncing;
                if self.leader {
                    Step::AssignAndSync { members }
                } else {
                    Step::Sync
                }
            }
            // Normal, and the reason a first join "fails": take the id offered
            // and go again.
            codes::MEMBER_ID_REQUIRED => {
                self.member_id = member_id.to_owned();
                self.state = MemberState::Unjoined;
                Step::Join {
                    member_id: self.member_id.clone(),
                }
            }
            codes::UNKNOWN_MEMBER_ID | codes::FENCED_INSTANCE_ID => {
                // Our identity is gone. Forget it *and* what it owned.
                self.member_id = String::new();
                self.revoke_and_rejoin();
                Step::Join {
                    member_id: String::new(),
                }
            }
            codes::COORDINATOR_NOT_AVAILABLE
            | codes::NOT_COORDINATOR
            | codes::COORDINATOR_LOAD_IN_PROGRESS => {
                self.state = MemberState::Unjoined;
                Step::FindCoordinator
            }
            _ => {
                self.revoke_and_rejoin();
                Step::Join {
                    member_id: self.member_id.clone(),
                }
            }
        }
    }

    /// `SyncGroup` answered, carrying this member's assignment.
    pub fn on_sync(&mut self, error: i16, assigned: Vec<TopicPartition>) -> Step {
        match error {
            codes::NONE => {
                self.assignment = assigned;
                self.assignment.sort();
                self.state = MemberState::Stable;
                Step::Heartbeat
            }
            // The group moved on while we were syncing.
            codes::REBALANCE_IN_PROGRESS | codes::ILLEGAL_GENERATION => {
                self.revoke_and_rejoin();
                Step::Join {
                    member_id: self.member_id.clone(),
                }
            }
            codes::UNKNOWN_MEMBER_ID | codes::FENCED_INSTANCE_ID => {
                self.member_id = String::new();
                self.revoke_and_rejoin();
                Step::Join {
                    member_id: String::new(),
                }
            }
            codes::COORDINATOR_NOT_AVAILABLE | codes::NOT_COORDINATOR => {
                self.revoke_and_rejoin();
                Step::FindCoordinator
            }
            _ => {
                self.revoke_and_rejoin();
                Step::Join {
                    member_id: self.member_id.clone(),
                }
            }
        }
    }

    /// A `Heartbeat` answered.
    pub fn on_heartbeat(&mut self, error: i16) -> Step {
        match error {
            codes::NONE => Step::Heartbeat,
            // Someone joined or left. Give up the partitions *before*
            // rejoining: another member is about to be told it owns them.
            codes::REBALANCE_IN_PROGRESS | codes::ILLEGAL_GENERATION => {
                self.revoke_and_rejoin();
                Step::Join {
                    member_id: self.member_id.clone(),
                }
            }
            codes::UNKNOWN_MEMBER_ID | codes::FENCED_INSTANCE_ID => {
                self.member_id = String::new();
                self.revoke_and_rejoin();
                Step::Join {
                    member_id: String::new(),
                }
            }
            codes::COORDINATOR_NOT_AVAILABLE | codes::NOT_COORDINATOR => {
                self.revoke_and_rejoin();
                Step::FindCoordinator
            }
            _ => {
                self.revoke_and_rejoin();
                Step::Join {
                    member_id: self.member_id.clone(),
                }
            }
        }
    }

    /// Leaving deliberately, so the group rebalances now rather than at the
    /// session timeout.
    pub fn on_leave(&mut self) {
        self.member_id = String::new();
        self.revoke_and_rejoin();
    }

    /// The assignment the leader computed, for the leader's own `SyncGroup`.
    #[must_use]
    pub fn my_share(&self, assignment: &Assignment) -> Vec<TopicPartition> {
        assignment.get(&self.member_id).cloned().unwrap_or_default()
    }

    /// Drop everything owned and go back to the start.
    ///
    /// **The assignment is cleared, not kept.** A member that holds its
    /// partitions across a rebalance keeps fetching them while their new owner
    /// does too, which is duplicate consumption that no error reports.
    fn revoke_and_rejoin(&mut self) {
        self.assignment.clear();
        self.generation = NO_GENERATION;
        self.leader = false;
        self.state = MemberState::Unjoined;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member() -> GroupMember {
        GroupMember::new("g", vec!["t".to_owned()])
    }

    fn tp(partition: i32) -> TopicPartition {
        TopicPartition::new("t", partition)
    }

    /// The happy path, including the first join being refused on purpose.
    #[test]
    fn a_first_join_is_refused_and_then_accepted() {
        let mut m = member();
        assert_eq!(m.step(), Step::Join { member_id: String::new() });

        // KIP-394: the coordinator hands back an id rather than admitting us.
        let step = m.on_join(codes::MEMBER_ID_REQUIRED, NO_GENERATION, "m-1", "", vec![]);
        assert_eq!(step, Step::Join { member_id: "m-1".to_owned() });
        assert_eq!(m.member_id(), "m-1");
        assert_eq!(m.state(), MemberState::Unjoined);

        let step = m.on_join(codes::NONE, 7, "m-1", "m-2", vec![]);
        assert_eq!(step, Step::Sync, "a follower syncs with no assignment");
        assert_eq!(m.state(), MemberState::Syncing);
        assert!(!m.is_leader());

        assert_eq!(m.on_sync(codes::NONE, vec![tp(0), tp(1)]), Step::Heartbeat);
        assert_eq!(m.state(), MemberState::Stable);
        assert_eq!(m.assignment(), &[tp(0), tp(1)]);
        assert_eq!(m.generation(), 7);
    }

    /// The leader is told the membership and must assign.
    #[test]
    fn the_leader_is_asked_to_assign() {
        let mut m = member();
        let members = vec![Subscription {
            member_id: "m-1".to_owned(),
            topics: vec!["t".to_owned()],
            owned: vec![],
        }];
        let step = m.on_join(codes::NONE, 3, "m-1", "m-1", members.clone());
        assert_eq!(step, Step::AssignAndSync { members });
        assert!(m.is_leader());
    }

    /// **Offsets may only be committed while stable.** Between a revocation and
    /// the next assignment, a commit would write an offset for a partition
    /// another member is being handed.
    #[test]
    fn commits_are_refused_outside_stable() {
        let mut m = member();
        assert!(!m.can_commit(), "not a member yet");

        m.on_join(codes::NONE, 1, "m-1", "m-1", vec![]);
        assert!(!m.can_commit(), "syncing is not stable");

        m.on_sync(codes::NONE, vec![tp(0)]);
        assert!(m.can_commit());

        m.on_heartbeat(codes::REBALANCE_IN_PROGRESS);
        assert!(!m.can_commit(), "a rebalance suspends commits");
    }

    /// **Partitions are given up before rejoining, not after.** Holding them
    /// across a rebalance is duplicate consumption that nothing reports.
    #[test]
    fn a_rebalance_revokes_the_assignment_immediately() {
        let mut m = member();
        m.on_join(codes::NONE, 1, "m-1", "m-2", vec![]);
        m.on_sync(codes::NONE, vec![tp(0), tp(1)]);
        assert_eq!(m.assignment().len(), 2);

        let step = m.on_heartbeat(codes::REBALANCE_IN_PROGRESS);
        assert_eq!(step, Step::Join { member_id: "m-1".to_owned() });
        assert!(m.assignment().is_empty(), "partitions must be given up");
        assert_eq!(m.generation(), NO_GENERATION);
        assert_eq!(m.state(), MemberState::Unjoined);
    }

    /// A stale generation is the same situation, discovered a different way.
    #[test]
    fn an_illegal_generation_revokes_too() {
        let mut m = member();
        m.on_join(codes::NONE, 4, "m-1", "m-2", vec![]);
        m.on_sync(codes::NONE, vec![tp(0)]);

        m.on_heartbeat(codes::ILLEGAL_GENERATION);
        assert!(m.assignment().is_empty());
        assert!(!m.can_commit());
        assert_eq!(
            m.member_id(),
            "m-1",
            "the member id survives a generation bump"
        );
    }

    /// An unknown member id is stronger: the identity itself is gone, so it is
    /// dropped and the next join starts from nothing.
    #[test]
    fn an_unknown_member_id_forgets_the_identity() {
        let mut m = member();
        m.on_join(codes::NONE, 4, "m-1", "m-2", vec![]);
        m.on_sync(codes::NONE, vec![tp(0)]);

        let step = m.on_heartbeat(codes::UNKNOWN_MEMBER_ID);
        assert_eq!(step, Step::Join { member_id: String::new() });
        assert_eq!(m.member_id(), "", "the id is no longer ours to use");
        assert!(m.assignment().is_empty());
    }

    /// A lost coordinator is not a fencing event: re-discover and carry on.
    #[test]
    fn a_lost_coordinator_is_rediscovered() {
        let mut m = member();
        m.on_join(codes::NONE, 2, "m-1", "m-2", vec![]);
        m.on_sync(codes::NONE, vec![tp(0)]);

        assert_eq!(m.on_heartbeat(codes::NOT_COORDINATOR), Step::FindCoordinator);
        assert!(m.assignment().is_empty(), "still revoked: we cannot heartbeat");
    }

    /// A rebalance that starts while we are syncing sends us round again.
    #[test]
    fn a_rebalance_during_sync_restarts_the_join() {
        let mut m = member();
        m.on_join(codes::NONE, 5, "m-1", "m-1", vec![]);
        assert_eq!(m.state(), MemberState::Syncing);

        let step = m.on_sync(codes::REBALANCE_IN_PROGRESS, vec![]);
        assert_eq!(step, Step::Join { member_id: "m-1".to_owned() });
        assert!(!m.is_leader(), "leadership is not carried across a rebalance");
    }

    /// Changing the subscription is a rebalance: the group has to agree on it
    /// before anyone can be assigned against it.
    #[test]
    fn changing_topics_forces_a_rejoin() {
        let mut m = member();
        m.on_join(codes::NONE, 1, "m-1", "m-2", vec![]);
        m.on_sync(codes::NONE, vec![tp(0)]);

        m.set_topics(vec!["t".to_owned(), "u".to_owned()]);
        assert_eq!(m.state(), MemberState::Unjoined);
        assert!(m.assignment().is_empty());

        // Setting the same topics again is not a rebalance.
        m.on_join(codes::NONE, 2, "m-1", "m-2", vec![]);
        m.on_sync(codes::NONE, vec![tp(0)]);
        m.set_topics(vec!["t".to_owned(), "u".to_owned()]);
        assert_eq!(m.state(), MemberState::Stable);
    }

    /// What a member tells the group about itself, including what it holds —
    /// which is what lets a sticky assignor keep it there.
    #[test]
    fn the_subscription_carries_what_is_owned() {
        let mut m = member();
        m.on_join(codes::NONE, 1, "m-1", "m-2", vec![]);
        m.on_sync(codes::NONE, vec![tp(3)]);

        let s = m.subscription();
        assert_eq!(s.member_id, "m-1");
        assert_eq!(s.topics, vec!["t".to_owned()]);
        assert_eq!(s.owned, vec![tp(3)]);
    }

    /// Leaving gives everything up, so the group can rebalance without waiting
    /// for the session to time out.
    #[test]
    fn leaving_gives_everything_up() {
        let mut m = member();
        m.on_join(codes::NONE, 1, "m-1", "m-2", vec![]);
        m.on_sync(codes::NONE, vec![tp(0)]);

        m.on_leave();
        assert_eq!(m.member_id(), "");
        assert!(m.assignment().is_empty());
        assert!(!m.can_commit());
    }

    /// **The invariant behind all of it**: whenever this member is not stable,
    /// it owns nothing and may not commit. Checked over every error code the
    /// machine reacts to, from every state that can receive one.
    #[test]
    fn outside_stable_it_owns_nothing_and_commits_nothing() {
        let errors = [
            codes::REBALANCE_IN_PROGRESS,
            codes::ILLEGAL_GENERATION,
            codes::UNKNOWN_MEMBER_ID,
            codes::FENCED_INSTANCE_ID,
            codes::NOT_COORDINATOR,
            codes::COORDINATOR_NOT_AVAILABLE,
            9_999, // anything unrecognised
        ];

        for error in errors {
            let mut m = member();
            m.on_join(codes::NONE, 1, "m-1", "m-2", vec![]);
            m.on_sync(codes::NONE, vec![tp(0), tp(1)]);
            assert!(m.can_commit());

            m.on_heartbeat(error);
            assert_ne!(m.state(), MemberState::Stable, "error {error}");
            assert!(m.assignment().is_empty(), "error {error} kept partitions");
            assert!(!m.can_commit(), "error {error} still allowed a commit");
        }
    }
}
