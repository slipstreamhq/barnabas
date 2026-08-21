//! The group protocol on a socket: the classic one, behind the seam.
//!
//! [`barnabas_core::member`] holds the state machine and decides *what* to do;
//! this sends it. The split is the seam KIP-848 will plug into — see
//! `docs/completing-the-client.md` — and the rule that keeps it honest is that
//! nothing above [`GroupProtocol`] names a generation id or a member epoch.
//! Both are fencing tokens; the moment shared code branches on which kind it
//! holds, the seam has leaked.
//!
//! # The embedded blobs
//!
//! `JoinGroup` and `SyncGroup` carry the subscription and the assignment as
//! **opaque bytes**, in a format the group's members agree on rather than one
//! the broker parses. That is why a Rust member and a Java member can share a
//! group at all — and why these encoders have to match, byte for byte, what
//! `ConsumerProtocol` writes. `kafka-protocol` generates both from Kafka's own
//! schemas, so the agreement is inherited rather than hand-rolled.

use std::collections::BTreeMap;
use std::time::Duration;

use barnabas_core::group::{Assignor, Subscription, TopicPartition};
use barnabas_core::member::{codes, GroupMember, Step};
use bytes::Bytes;
use kafka_protocol::messages::{
    consumer_protocol_assignment::{
        ConsumerProtocolAssignment, TopicPartition as AssignmentTopicPartition,
    },
    consumer_protocol_subscription::{
        ConsumerProtocolSubscription, TopicPartition as SubscriptionTopicPartition,
    },
    join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
    offset_fetch_request::OffsetFetchRequestTopic,
    sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
    ApiKey, FindCoordinatorRequest, FindCoordinatorResponse, GroupId, HeartbeatRequest,
    HeartbeatResponse, JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse,
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

use crate::cluster::Cluster;
use crate::{Error, Result, Transport};

/// `protocol_type` for a consumer group. The coordinator rejects a member whose
/// type does not match the group's, which is what stops a consumer joining a
/// Connect group by accident.
const CONSUMER_PROTOCOL: &str = "consumer";

/// How many times to wait for a coordinator that is still being created.
const COORDINATOR_RETRIES: usize = 40;
const COORDINATOR_BACKOFF: Duration = Duration::from_millis(250);

/// Added to the rebalance timeout so the *broker* decides a slow rebalance has
/// failed, rather than us abandoning a request it is still working on.
const JOIN_SLACK: Duration = Duration::from_secs(5);

/// Coordinator requests other than `JoinGroup` answer promptly.
const COORDINATOR_TIMEOUT: Duration = Duration::from_secs(30);

/// The `ConsumerProtocol` version this client writes.
///
/// **v2, because v1 cannot carry a generation.** A member that has been away
/// still advertises the partitions it used to own, and a leader with no way to
/// date that claim hands the same partition to two members — which is what
/// `cooperative-sticky` did here until this changed. v2 added `generation_id`
/// for exactly that.
///
/// Members negotiate down to the lowest version any of them wrote, so a reader
/// must honour what it is given rather than assume this one.
const PROTOCOL_VERSION: i16 = 2;

/// The int16 that prefixes every subscription and assignment blob.
fn read_version(cursor: &mut Bytes) -> Result<i16> {
    use bytes::Buf;
    if cursor.remaining() < 2 {
        return Err(Error::Core(barnabas_core::Error::Codec(
            "consumer protocol blob is too short for its version".to_owned(),
        )));
    }
    Ok(cursor.get_i16())
}

/// A member's identity and its fencing token, opaque above the seam.
///
/// **Nothing outside this module reads the fields.** They are what
/// `TxnOffsetCommit` must carry so the coordinator can reject a commit from a
/// member that has already been replaced (KIP-447), and under KIP-848 the same
/// two positions hold a member epoch instead of a generation. A caller that
/// could name them would be a caller that has to change when the protocol does,
/// so the only way to get one is to ask a live consumer for it and hand it
/// straight to a producer.
#[derive(Debug, Clone)]
pub struct GroupMetadata {
    pub(crate) group_id: String,
    pub(crate) generation_id: i32,
    pub(crate) member_id: String,
    /// Static membership (KIP-345) is not implemented, so this is always
    /// `None`. It is here because the field is on the wire and leaving it out
    /// would mean changing this type later.
    pub(crate) group_instance_id: Option<String>,
}

impl GroupMetadata {
    /// The group these offsets belong to. The one field a caller may read,
    /// because they chose it.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }
}

/// How this client becomes and stays a member of a group, and how it learns
/// what it is assigned.
///
/// **The only thing KIP-848 changes.** Offset commit, fetching, the callbacks
/// and the assignment's effect all sit above this.
pub trait GroupProtocol<T: Transport> {
    /// Drive one step of membership. Called until it reports [`Membership`]
    /// stable.
    fn advance(
        &mut self,
        cluster: &mut Cluster<T>,
    ) -> impl std::future::Future<Output = Result<Membership>>;

    /// Leave deliberately, so the group rebalances now rather than at the
    /// session timeout.
    fn leave(&mut self, cluster: &mut Cluster<T>) -> impl std::future::Future<Output = Result<()>>;

    /// Commit these offsets for the group.
    ///
    /// **On the seam, not above it**, because a commit carries the fencing
    /// token — a generation id here, a member epoch under KIP-848 — and the
    /// rule that keeps the seam honest is that nothing above it names either.
    /// The caller supplies positions; which token proves they are still ours is
    /// the protocol's business.
    ///
    /// The offset committed is **the next one to read**, not the last one read.
    /// Kafka's convention, and getting it wrong replays or skips exactly one
    /// record per partition per restart.
    fn commit(
        &mut self,
        cluster: &mut Cluster<T>,
        offsets: &BTreeMap<TopicPartition, i64>,
    ) -> impl std::future::Future<Output = Result<()>>;

    /// What this member is subscribed to, so the caller can watch those topics
    /// for changes the group must agree on.
    fn topics(&self) -> Vec<String>;

    /// Ask to rejoin the group at the next [`Self::advance`].
    ///
    /// **On the seam** for the same reason [`Self::commit`] is: what a rejoin
    /// costs and what it is called differ completely between the classic
    /// protocol and KIP-848. The caller only says that something the group
    /// agreed on has changed.
    fn request_rejoin(&mut self);

    /// What proves this member is current, for a transactional producer.
    ///
    /// `None` until the member is stable: an unjoined member has no token to
    /// offer, and committing without one is how a zombie writes.
    fn group_metadata(&self) -> Option<GroupMetadata>;

    /// Where this group last committed, for the partitions given.
    ///
    /// A partition with no committed offset is **absent** from the result
    /// rather than zero — "never committed" and "committed at 0" are different
    /// answers, and conflating them replays a whole partition.
    fn committed(
        &mut self,
        cluster: &mut Cluster<T>,
        partitions: &[TopicPartition],
    ) -> impl std::future::Future<Output = Result<BTreeMap<TopicPartition, i64>>>;
}

/// Where membership stands after a step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Membership {
    /// Still joining or syncing; call again.
    InProgress,
    /// Assigned and stable. The partitions are this member's to fetch.
    Assigned(Vec<TopicPartition>),
    /// These partitions were given up. The caller must stop fetching them
    /// **before** the next call, because another member is about to be given
    /// them.
    ///
    /// Under the eager protocol that is everything this member held; under the
    /// cooperative one it is only what moved, and the rest keeps being read.
    Revoked(Vec<TopicPartition>),
}

/// The classic protocol: `JoinGroup`, `SyncGroup`, `Heartbeat`.
pub struct ClassicProtocol {
    member: GroupMember,
    assignor: Box<dyn Assignor>,
    coordinator: Option<String>,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    /// What the last `advance` reported, so a revocation is announced once.
    announced_revoked: bool,
    /// Whether anything was ever assigned. Without it the first join reports a
    /// revocation, and a caller with a revoke callback runs it for partitions
    /// it never had.
    ever_assigned: bool,
}

impl ClassicProtocol {
    #[must_use]
    pub fn new(
        group_id: impl Into<String>,
        topics: Vec<String>,
        assignor: Box<dyn Assignor>,
    ) -> Self {
        // **The assignor decides the protocol**, because they are the same
        // choice: `cooperative-sticky` is a name the group agrees on *and* a
        // different revocation flow. Letting them be set separately is letting
        // them disagree.
        let protocol = if assignor.name() == "cooperative-sticky" {
            barnabas_core::member::RebalanceProtocol::Cooperative
        } else {
            barnabas_core::member::RebalanceProtocol::Eager
        };
        Self {
            member: GroupMember::new(group_id, topics).with_protocol(protocol),
            assignor,
            coordinator: None,
            session_timeout_ms: 45_000,
            rebalance_timeout_ms: 300_000,
            announced_revoked: false,
            ever_assigned: false,
        }
    }

    #[must_use]
    pub fn member(&self) -> &GroupMember {
        &self.member
    }

    /// How long the coordinator waits for a heartbeat before removing us.
    pub fn set_session_timeout(&mut self, ms: i32) {
        self.session_timeout_ms = ms;
    }

    /// How long the coordinator waits for every member to rejoin. Should exceed
    /// the longest a caller can spend between polls, or a slow consumer is
    /// dropped mid-rebalance.
    pub fn set_rebalance_timeout(&mut self, ms: i32) {
        self.rebalance_timeout_ms = ms;
    }

    /// The **group** coordinator, which is a different broker from a
    /// transaction coordinator and is found the same way.
    async fn coordinator_addr<T: Transport>(&mut self, cluster: &mut Cluster<T>) -> Result<String> {
        if let Some(addr) = &self.coordinator {
            return Ok(addr.clone());
        }
        let mut req = FindCoordinatorRequest::default();
        req.key = StrBytes::from_string(self.member.group_id().to_owned());
        req.key_type = 0; // GROUP

        // **The first call for a new group is expected to fail.**
        // `__consumer_offsets` is created lazily by this very request, so a
        // fresh cluster answers COORDINATOR_NOT_AVAILABLE until it exists. The
        // transactional producer met the same thing on `__transaction_state`.
        for attempt in 0..COORDINATOR_RETRIES {
            let resp: FindCoordinatorResponse =
                cluster.call_any(ApiKey::FindCoordinator, 3, &req).await?;
            match resp.error_code {
                codes::NONE => {
                    let addr = format!("{}:{}", resp.host.as_str(), resp.port);
                    self.coordinator = Some(addr.clone());
                    return Ok(addr);
                }
                codes::COORDINATOR_NOT_AVAILABLE | codes::COORDINATOR_LOAD_IN_PROGRESS => {
                    let _ = attempt;
                    T::sleep(COORDINATOR_BACKOFF).await;
                }
                code => crate::check("FindCoordinator", code)?,
            }
        }
        Err(Error::Broker {
            op: "FindCoordinator",
            code: codes::COORDINATOR_NOT_AVAILABLE,
            disposition: barnabas_core::Disposition::Retry,
        })
    }

    async fn join<T: Transport>(&mut self, cluster: &mut Cluster<T>) -> Result<Step> {
        let addr = self.coordinator_addr(cluster).await?;

        let mut protocol = JoinGroupRequestProtocol::default();
        protocol.name = StrBytes::from_string(self.assignor.name().to_owned());
        protocol.metadata = encode_subscription(&self.member.subscription())?;

        let mut req = JoinGroupRequest::default();
        req.group_id = GroupId(StrBytes::from_string(self.member.group_id().to_owned()));
        req.session_timeout_ms = self.session_timeout_ms;
        req.rebalance_timeout_ms = self.rebalance_timeout_ms;
        req.member_id = StrBytes::from_string(self.member.member_id().to_owned());
        req.protocol_type = StrBytes::from_static_str(CONSUMER_PROTOCOL);
        req.protocols = vec![protocol];

        // The coordinator holds this until the whole group has joined, so its
        // deadline is the rebalance timeout, not the general request timeout.
        let deadline =
            Duration::from_millis(u64::try_from(self.rebalance_timeout_ms).unwrap_or(300_000))
                + JOIN_SLACK;
        let resp: JoinGroupResponse = cluster
            .call_coordinator(&addr, ApiKey::JoinGroup, 7, &req, deadline)
            .await?;

        // Only the leader is given the members, and only it needs to decode
        // them.
        let members = if resp.leader == resp.member_id {
            resp.members
                .iter()
                .map(|m| decode_subscription(m.member_id.as_str(), &m.metadata))
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };

        if std::env::var("BARNABAS_TRACE").is_ok() {
            eprintln!(
                "[{}] JOIN<- err={} gen={} id={} leader={}",
                self.member.member_id(),
                resp.error_code,
                resp.generation_id,
                resp.member_id.as_str(),
                resp.leader.as_str()
            );
        }
        Ok(self.member.on_join(
            resp.error_code,
            resp.generation_id,
            resp.member_id.as_str(),
            resp.leader.as_str(),
            members,
        ))
    }

    async fn sync<T: Transport>(
        &mut self,
        cluster: &mut Cluster<T>,
        assignments: Vec<SyncGroupRequestAssignment>,
    ) -> Result<Step> {
        let addr = self.coordinator_addr(cluster).await?;

        if std::env::var("BARNABAS_TRACE").is_ok() {
            let sub = self.member.subscription();
            eprintln!(
                "[{}] SYNC-> gen={} owned={:?} sending={:?}",
                self.member.member_id(),
                self.member.generation(),
                sub.owned.iter().map(|t| t.partition).collect::<Vec<_>>(),
                assignments
                    .iter()
                    .map(|a| (a.member_id.to_string(), a.assignment.len()))
                    .collect::<Vec<_>>()
            );
        }
        let mut req = SyncGroupRequest::default();
        req.group_id = GroupId(StrBytes::from_string(self.member.group_id().to_owned()));
        req.generation_id = self.member.generation();
        req.member_id = StrBytes::from_string(self.member.member_id().to_owned());
        req.protocol_type = Some(StrBytes::from_static_str(CONSUMER_PROTOCOL));
        req.protocol_name = Some(StrBytes::from_string(self.assignor.name().to_owned()));
        req.assignments = assignments;

        let resp: SyncGroupResponse = cluster
            .call_coordinator(&addr, ApiKey::SyncGroup, 4, &req, COORDINATOR_TIMEOUT)
            .await?;
        let assigned = if resp.error_code == codes::NONE && !resp.assignment.is_empty() {
            decode_assignment(&resp.assignment)?
        } else {
            Vec::new()
        };
        if std::env::var("BARNABAS_TRACE").is_ok() {
            eprintln!(
                "[{}] SYNC<- err={} assigned={:?}",
                self.member.member_id(),
                resp.error_code,
                assigned.iter().map(|t| t.partition).collect::<Vec<_>>()
            );
        }
        let step = self.member.on_sync(resp.error_code, assigned);
        if std::env::var("BARNABAS_TRACE").is_ok() {
            eprintln!(
                "[{}]   after: gen={} assignment={:?} lost={:?} step={:?}",
                self.member.member_id(),
                self.member.generation(),
                self.member
                    .assignment()
                    .iter()
                    .map(|t| t.partition)
                    .collect::<Vec<_>>(),
                self.member
                    .lost()
                    .iter()
                    .map(|t| t.partition)
                    .collect::<Vec<_>>(),
                step
            );
        }
        Ok(step)
    }

    async fn heartbeat<T: Transport>(&mut self, cluster: &mut Cluster<T>) -> Result<Step> {
        let addr = self.coordinator_addr(cluster).await?;

        let mut req = HeartbeatRequest::default();
        req.group_id = GroupId(StrBytes::from_string(self.member.group_id().to_owned()));
        req.generation_id = self.member.generation();
        req.member_id = StrBytes::from_string(self.member.member_id().to_owned());

        let resp: HeartbeatResponse = cluster
            .call_coordinator(&addr, ApiKey::Heartbeat, 4, &req, COORDINATOR_TIMEOUT)
            .await?;
        if std::env::var("BARNABAS_TRACE").is_ok() {
            eprintln!(
                "[{}] HB<- err={} gen={}",
                self.member.member_id(),
                resp.error_code,
                self.member.generation()
            );
        }
        Ok(self.member.on_heartbeat(resp.error_code))
    }

    /// Compute the assignment as leader, and send it.
    async fn assign_and_sync<T: Transport>(
        &mut self,
        cluster: &mut Cluster<T>,
        members: Vec<Subscription>,
    ) -> Result<Step> {
        // The count comes from metadata, not from the members: a member only
        // says which *topics* it wants.
        let mut partitions_per_topic: BTreeMap<String, i32> = BTreeMap::new();
        for topic in members.iter().flat_map(|m| m.topics.iter()) {
            if !partitions_per_topic.contains_key(topic) {
                let count = cluster.partition_count(topic).await?;
                partitions_per_topic.insert(topic.clone(), count);
            }
        }

        if std::env::var("BARNABAS_TRACE").is_ok() {
            eprintln!(
                "[{}] LEADER sees: {:?}",
                self.member.member_id(),
                members
                    .iter()
                    .map(|m| (
                        m.member_id.clone(),
                        m.generation,
                        m.owned.iter().map(|t| t.partition).collect::<Vec<_>>()
                    ))
                    .collect::<Vec<_>>()
            );
        }
        let assignment = self.assignor.assign(&members, &partitions_per_topic);
        if std::env::var("BARNABAS_TRACE").is_ok() {
            eprintln!(
                "[{}] LEADER assigns: {:?}",
                self.member.member_id(),
                assignment
                    .iter()
                    .map(|(m, p)| (m.clone(), p.iter().map(|t| t.partition).collect::<Vec<_>>()))
                    .collect::<Vec<_>>()
            );
        }
        let encoded = assignment
            .iter()
            .map(|(member_id, partitions)| {
                let mut entry = SyncGroupRequestAssignment::default();
                entry.member_id = StrBytes::from_string(member_id.clone());
                entry.assignment = encode_assignment(partitions)?;
                Ok(entry)
            })
            .collect::<Result<Vec<_>>>()?;

        self.sync(cluster, encoded).await
    }
}

impl<T: Transport> GroupProtocol<T> for ClassicProtocol {
    async fn advance(&mut self, cluster: &mut Cluster<T>) -> Result<Membership> {
        let step = self.member.step();
        let outcome = match step {
            Step::Join { .. } => self.join(cluster).await?,
            Step::AssignAndSync { members } => self.assign_and_sync(cluster, members).await?,
            Step::Sync => self.sync(cluster, Vec::new()).await?,
            Step::Heartbeat => self.heartbeat(cluster).await?,
            Step::FindCoordinator => {
                self.coordinator = None;
                Step::Join {
                    member_id: self.member.member_id().to_owned(),
                }
            }
        };

        // A step that asked us to re-discover clears the coordinator, whichever
        // call produced it.
        if outcome == Step::FindCoordinator {
            self.coordinator = None;
        }

        if self.member.state() == barnabas_core::member::MemberState::Stable {
            self.announced_revoked = false;
            self.ever_assigned = true;
            return Ok(Membership::Assigned(self.member.assignment().to_vec()));
        }
        // **Announced once, and before the next request goes out.** The caller
        // has to stop fetching these partitions now: another member is about to
        // be told it owns them.
        if self.ever_assigned && !self.announced_revoked {
            self.announced_revoked = true;
            return Ok(Membership::Revoked(self.member.lost().to_vec()));
        }
        Ok(Membership::InProgress)
    }

    async fn commit(
        &mut self,
        cluster: &mut Cluster<T>,
        offsets: &BTreeMap<TopicPartition, i64>,
    ) -> Result<()> {
        self.commit_offsets(cluster, offsets).await
    }

    fn topics(&self) -> Vec<String> {
        self.member.topics().to_vec()
    }

    fn request_rejoin(&mut self) {
        self.member.request_rejoin();
    }

    fn group_metadata(&self) -> Option<GroupMetadata> {
        // `can_commit` is exactly the right gate: a metadata that cannot commit
        // is a metadata that will be rejected, and finding that out at
        // `TxnOffsetCommit` time aborts a transaction that never had to start.
        if !self.member.can_commit() {
            return None;
        }
        Some(GroupMetadata {
            group_id: self.member.group_id().to_owned(),
            generation_id: self.member.generation(),
            member_id: self.member.member_id().to_owned(),
            group_instance_id: None,
        })
    }

    async fn committed(
        &mut self,
        cluster: &mut Cluster<T>,
        partitions: &[TopicPartition],
    ) -> Result<BTreeMap<TopicPartition, i64>> {
        self.fetch_offsets(cluster, partitions).await
    }

    async fn leave(&mut self, cluster: &mut Cluster<T>) -> Result<()> {
        if self.member.member_id().is_empty() {
            return Ok(());
        }
        let addr = self.coordinator_addr(cluster).await?;

        let mut req = LeaveGroupRequest::default();
        req.group_id = GroupId(StrBytes::from_string(self.member.group_id().to_owned()));
        req.member_id = StrBytes::from_string(self.member.member_id().to_owned());

        // A failure here costs a rebalance delay, not correctness: the
        // coordinator drops us at the session timeout anyway.
        let _: std::result::Result<LeaveGroupResponse, Error> = cluster
            .call_coordinator(&addr, ApiKey::LeaveGroup, 3, &req, COORDINATOR_TIMEOUT)
            .await;
        self.member.on_leave();
        Ok(())
    }
}

impl ClassicProtocol {
    async fn commit_offsets<T: Transport>(
        &mut self,
        cluster: &mut Cluster<T>,
        offsets: &BTreeMap<TopicPartition, i64>,
    ) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }
        // Refused rather than sent: between a revocation and the next
        // assignment these partitions may already belong to someone else, and
        // the coordinator will not reject the write because the generation has
        // not moved yet.
        if !self.member.can_commit() {
            return Err(Error::Broker {
                op: "OffsetCommit",
                code: codes::REBALANCE_IN_PROGRESS,
                disposition: barnabas_core::Disposition::Retry,
            });
        }
        let addr = self.coordinator_addr(cluster).await?;

        let mut by_topic: BTreeMap<String, Vec<OffsetCommitRequestPartition>> = BTreeMap::new();
        for (tp, offset) in offsets {
            let mut partition = OffsetCommitRequestPartition::default();
            partition.partition_index = tp.partition;
            partition.committed_offset = *offset;
            partition.committed_leader_epoch = -1;
            by_topic
                .entry(tp.topic.clone())
                .or_default()
                .push(partition);
        }

        let mut req = OffsetCommitRequest::default();
        req.group_id = GroupId(StrBytes::from_string(self.member.group_id().to_owned()));
        req.generation_id_or_member_epoch = self.member.generation();
        req.member_id = StrBytes::from_string(self.member.member_id().to_owned());
        req.topics = by_topic
            .into_iter()
            .map(|(name, partitions)| {
                let mut topic = OffsetCommitRequestTopic::default();
                topic.name = TopicName(StrBytes::from_string(name));
                topic.partitions = partitions;
                topic
            })
            .collect();

        let resp: OffsetCommitResponse = cluster
            .call_coordinator(&addr, ApiKey::OffsetCommit, 8, &req, COORDINATOR_TIMEOUT)
            .await?;
        for topic in &resp.topics {
            for partition in &topic.partitions {
                crate::check("OffsetCommit", partition.error_code)?;
            }
        }
        Ok(())
    }

    async fn fetch_offsets<T: Transport>(
        &mut self,
        cluster: &mut Cluster<T>,
        partitions: &[TopicPartition],
    ) -> Result<BTreeMap<TopicPartition, i64>> {
        if partitions.is_empty() {
            return Ok(BTreeMap::new());
        }
        let addr = self.coordinator_addr(cluster).await?;

        let mut by_topic: BTreeMap<String, Vec<i32>> = BTreeMap::new();
        for tp in partitions {
            by_topic
                .entry(tp.topic.clone())
                .or_default()
                .push(tp.partition);
        }

        let mut req = OffsetFetchRequest::default();
        req.group_id = GroupId(StrBytes::from_string(self.member.group_id().to_owned()));
        req.topics = Some(
            by_topic
                .into_iter()
                .map(|(name, partition_indexes)| {
                    let mut topic = OffsetFetchRequestTopic::default();
                    topic.name = TopicName(StrBytes::from_string(name));
                    topic.partition_indexes = partition_indexes;
                    topic
                })
                .collect(),
        );

        let resp: OffsetFetchResponse = cluster
            .call_coordinator(&addr, ApiKey::OffsetFetch, 6, &req, COORDINATOR_TIMEOUT)
            .await?;
        crate::check("OffsetFetch", resp.error_code)?;

        let mut out = BTreeMap::new();
        for topic in &resp.topics {
            for partition in &topic.partitions {
                crate::check("OffsetFetch partition", partition.error_code)?;
                // -1 is "never committed", which is not an offset. Left out so
                // the caller falls back to its reset policy rather than
                // replaying from zero.
                if partition.committed_offset >= 0 {
                    out.insert(
                        TopicPartition::new(topic.name.0.to_string(), partition.partition_index),
                        partition.committed_offset,
                    );
                }
            }
        }
        Ok(out)
    }
}

// ── the embedded blobs ───────────────────────────────────────────────────────

fn encode_subscription(subscription: &Subscription) -> Result<Bytes> {
    let mut owned: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    for tp in &subscription.owned {
        owned
            .entry(tp.topic.clone())
            .or_default()
            .push(tp.partition);
    }

    let mut body = ConsumerProtocolSubscription::default();
    body.topics = subscription
        .topics
        .iter()
        .map(|t| StrBytes::from_string(t.clone()))
        .collect();
    body.generation_id = subscription.generation;
    body.owned_partitions = owned
        .into_iter()
        .map(|(topic, partitions)| {
            let mut entry = SubscriptionTopicPartition::default();
            entry.topic = TopicName(StrBytes::from_string(topic));
            entry.partitions = partitions;
            entry
        })
        .collect();

    // **The version goes on the wire ahead of the struct.** Kafka's
    // `ConsumerProtocol.serializeSubscription` writes an int16 version and then
    // the body; `kafka-protocol`'s generated type is only the body, so the
    // prefix is ours to add. Without it every member — ours and Java's — reads
    // the blob shifted by two bytes.
    let mut buf = bytes::BytesMut::new();
    buf.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    body.encode(&mut buf, PROTOCOL_VERSION)
        .map_err(|e| Error::Core(barnabas_core::Error::Codec(format!("subscription: {e}"))))?;
    Ok(buf.freeze())
}

fn decode_subscription(member_id: &str, metadata: &Bytes) -> Result<Subscription> {
    let mut cursor = metadata.clone();
    let version = read_version(&mut cursor)?;
    let body = ConsumerProtocolSubscription::decode(&mut cursor, version)
        .map_err(|e| Error::Core(barnabas_core::Error::Codec(format!("subscription: {e}"))))?;

    Ok(Subscription {
        member_id: member_id.to_owned(),
        // A v0 or v1 blob has no generation; -1 says "unknown", and an unknown
        // claim is treated as stale rather than trusted.
        generation: if version >= 2 { body.generation_id } else { -1 },
        topics: body.topics.iter().map(|t| t.to_string()).collect(),
        owned: body
            .owned_partitions
            .iter()
            .flat_map(|tp| {
                let topic = tp.topic.0.to_string();
                tp.partitions
                    .iter()
                    .map(move |p| TopicPartition::new(topic.clone(), *p))
            })
            .collect(),
    })
}

fn encode_assignment(partitions: &[TopicPartition]) -> Result<Bytes> {
    let mut by_topic: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    for tp in partitions {
        by_topic
            .entry(tp.topic.clone())
            .or_default()
            .push(tp.partition);
    }

    let mut body = ConsumerProtocolAssignment::default();
    body.assigned_partitions = by_topic
        .into_iter()
        .map(|(topic, partitions)| {
            let mut entry = AssignmentTopicPartition::default();
            entry.topic = TopicName(StrBytes::from_string(topic));
            entry.partitions = partitions;
            entry
        })
        .collect();

    let mut buf = bytes::BytesMut::new();
    buf.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    body.encode(&mut buf, PROTOCOL_VERSION)
        .map_err(|e| Error::Core(barnabas_core::Error::Codec(format!("assignment: {e}"))))?;
    Ok(buf.freeze())
}

fn decode_assignment(assignment: &Bytes) -> Result<Vec<TopicPartition>> {
    let mut cursor = assignment.clone();
    let version = read_version(&mut cursor)?;
    let body = ConsumerProtocolAssignment::decode(&mut cursor, version)
        .map_err(|e| Error::Core(barnabas_core::Error::Codec(format!("assignment: {e}"))))?;

    let mut out: Vec<TopicPartition> = body
        .assigned_partitions
        .iter()
        .flat_map(|tp| {
            let topic = tp.topic.0.to_string();
            tp.partitions
                .iter()
                .map(move |p| TopicPartition::new(topic.clone(), *p))
        })
        .collect();
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The blobs must survive a round trip: they are what a Java member reads.
    #[test]
    fn a_subscription_round_trips() {
        let subscription = Subscription {
            member_id: "m-1".to_owned(),
            topics: vec!["a".to_owned(), "b".to_owned()],
            owned: vec![TopicPartition::new("a", 0), TopicPartition::new("a", 3)],
            generation: 7,
        };
        let encoded = encode_subscription(&subscription).expect("encode");
        let decoded = decode_subscription("m-1", &encoded).expect("decode");
        assert_eq!(decoded.topics, subscription.topics);
        assert_eq!(decoded.owned, subscription.owned);
        assert_eq!(
            decoded.generation, 7,
            "the generation must survive: without it a leader cannot tell a \
             live ownership claim from a stale one"
        );
    }

    #[test]
    fn an_assignment_round_trips() {
        let partitions = vec![
            TopicPartition::new("a", 0),
            TopicPartition::new("a", 1),
            TopicPartition::new("b", 7),
        ];
        let encoded = encode_assignment(&partitions).expect("encode");
        assert_eq!(decode_assignment(&encoded).expect("decode"), partitions);
    }

    /// An empty assignment is a real answer — "you got nothing this round" —
    /// and must not decode as an error.
    #[test]
    fn an_empty_assignment_decodes_to_nothing() {
        let encoded = encode_assignment(&[]).expect("encode");
        assert!(decode_assignment(&encoded).expect("decode").is_empty());
    }
}
