//! The assign-only consumer.
//!
//! # There is no consumer group, and that is a scope decision
//!
//! No `subscribe`, no JoinGroup/SyncGroup, no heartbeats, no rebalance, no
//! offset commit. **You choose the partitions** — see [`Consumer::assign`] and the
//! builder's `assign_all` — and you store the offsets.
//!
//! That suits a system whose own control plane places partitions and whose
//! checkpoints hold offsets, because a group would be a second authority for
//! both. It suits most other programs badly: if you want partitions
//! redistributed when an instance dies, this client cannot do it yet, and
//! `rdkafka` or the Java client can. Groups are planned — see
//! `docs/completing-the-client.md` — as a layer over this one, not a replacement for
//! it.
//!
//! # One fetch per broker, not per partition
//!
//! A consumer holds any number of `(topic, partition)` assignments and fetches
//! them **together**: partitions are grouped by the broker that leads them, and
//! each broker gets one `Fetch` carrying all of them.
//!
//! This is the read-side twin of the producer's batching, and it matters more
//! for a per-core client than for a threaded one. A core that owns thirty-two
//! partitions was previously thirty-two connections and thirty-two round trips
//! per poll; it is now one connection per broker and one request. That is also
//! what makes the connection count affordable — the thing `PERF.md` and the
//! design doc both flagged as the cost of being per-core.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::{
    fetch_request::{FetchPartition, FetchTopic},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    ApiKey, BrokerId, FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse,
    TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Record, RecordBatchDecoder};
use kestrel_core::consumer::{self, AbortedTransaction, Fetched};
use kestrel_core::records::LeanBatch;
use kestrel_core::{Disposition, ErrorCode, IsolationLevel};

use crate::cluster::Cluster;
use crate::{check, Error, Result, Transport};

/// Timestamps `ListOffsets` understands.
pub const EARLIEST: i64 = -2;
pub const LATEST: i64 = -1;

/// How many times a request is retried when the broker says leadership moved.
const MAX_LEADER_RETRIES: usize = 5;

/// How long to wait before asking again after a leadership answer that said
/// "not yet".
///
/// Refreshing metadata and retrying *immediately* asks the same question of the
/// same not-yet-propagated cluster state, so five attempts cost five round
/// trips and learn nothing. Assigning a consumer to a topic created moments ago
/// failed for exactly this reason.
const LEADER_BACKOFF: Duration = Duration::from_millis(100);

/// The broker forgot our fetch session, or our epoch is stale. Both mean
/// "start again with a full fetch" rather than "fail".
const FETCH_SESSION_ID_NOT_FOUND: i16 = 70;
const INVALID_FETCH_SESSION_EPOCH: i16 = 71;

/// Per-broker fetch session state (KIP-227).
///
/// **An incremental fetch names only what changed.** A full fetch restates
/// every partition's offset on every poll, which for a core owning many
/// partitions is most of the request — and the broker rebuilds its view each
/// time. With a session, the broker remembers the partition set and the client
/// sends only the partitions whose position moved.
///
/// Idle partitions are the case this exists for: a consumer holding thirty-two
/// partitions where two are busy sends two partitions per fetch instead of
/// thirty-two.
#[derive(Debug, Default, Clone)]
struct Session {
    /// 0 until the broker assigns one.
    id: i32,
    /// 0 opens a full fetch; each subsequent request increments.
    epoch: i32,
    /// What the broker believes our offsets are, so a request can send the
    /// difference.
    known: BTreeMap<(String, i32), i64>,
}

impl Session {
    /// Forget everything and ask for a full fetch next time.
    fn reset(&mut self) {
        self.id = 0;
        self.epoch = 0;
        self.known.clear();
    }
}

/// What one partition yielded.
#[derive(Debug)]
pub struct ConsumerRecords {
    pub topic: String,
    pub partition: i32,
    /// The batches as [`kestrel_core::records`] read them.
    pub batches: Vec<LeanBatch>,
    /// Records from a batch the lean reader handed back — only a pre-magic-2
    /// batch does — decoded the ordinary way. Kept separate rather than
    /// converted, because building a [`LeanBatch`] from decoded records would
    /// mean re-serialising them.
    pub fallback: Vec<Record>,
}

impl ConsumerRecords {
    /// Every record, whichever path decoded it.
    ///
    /// **This is what callers should use.** Records live in batches because
    /// that is how the format stores them and how the filtering works, but a
    /// caller almost never cares which batch a record came from — and having to
    /// nest two loops, plus handle the fallback, would be a bad trade for the
    /// speed it buys.
    pub fn iter(&self) -> impl Iterator<Item = RecordRef<'_>> {
        self.batches
            .iter()
            .flat_map(|batch| {
                batch
                    .records
                    .iter()
                    .map(move |record| RecordRef::Lean { batch, record })
            })
            .chain(self.fallback.iter().map(RecordRef::Full))
    }

    /// How many records this partition yielded.
    #[must_use]
    pub fn len(&self) -> usize {
        self.batches.iter().map(|b| b.records.len()).sum::<usize>() + self.fallback.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Told when the group gives this consumer partitions, and before it takes
/// them away.
///
/// Kafka's `ConsumerRebalanceListener`, with one difference worth knowing:
/// **these are synchronous**. This client spawns nothing and holds `!Send`
/// state, so an async callback would need a boxed future and a runtime to
/// drive it — and the useful work here (dropping per-partition state, noting
/// what changed) does not need one. A caller with async cleanup should record
/// what happened and do it after `poll` returns.
///
/// `on_revoked` runs **before** the partitions are given up, so the positions
/// it is handed are the ones about to be lost. It cannot commit them: by the
/// time a rebalance is visible the generation that would authorise a commit is
/// already gone, which is what makes auto-commit at-least-once.
pub trait RebalanceListener {
    fn on_revoked(&mut self, partitions: &[kestrel_core::group::TopicPartition]);
    fn on_assigned(&mut self, partitions: &[kestrel_core::group::TopicPartition]);
}

/// One record, without a record having been built for it.
///
/// A key, value or header list is materialised when asked for, not at decode
/// time — which is the point: every slice of a batch buffer increments the same
/// atomic refcount, so a caller that skips a record should not pay for it.
#[derive(Debug, Clone, Copy)]
pub enum RecordRef<'a> {
    Lean {
        batch: &'a LeanBatch,
        record: &'a kestrel_core::records::LeanRecord,
    },
    /// From a batch the lean reader handed back.
    Full(&'a Record),
}

impl RecordRef<'_> {
    #[must_use]
    pub fn offset(&self) -> i64 {
        match self {
            Self::Lean { record, .. } => record.offset,
            Self::Full(record) => record.offset,
        }
    }

    #[must_use]
    pub fn timestamp(&self) -> i64 {
        match self {
            Self::Lean { record, .. } => record.timestamp,
            Self::Full(record) => record.timestamp,
        }
    }

    #[must_use]
    pub fn key(&self) -> Option<Bytes> {
        match self {
            Self::Lean { batch, record } => batch.key(record),
            Self::Full(record) => record.key.clone(),
        }
    }

    #[must_use]
    pub fn value(&self) -> Option<Bytes> {
        match self {
            Self::Lean { batch, record } => batch.value(record),
            Self::Full(record) => record.value.clone(),
        }
    }

    /// # Errors
    /// If the header block is malformed.
    pub fn headers(&self) -> Result<Vec<(Bytes, Option<Bytes>)>> {
        match self {
            Self::Lean { batch, record } => Ok(batch.headers(record)?),
            Self::Full(record) => Ok(record
                .headers
                .iter()
                .map(|(k, v)| (Bytes::copy_from_slice(k.as_str().as_bytes()), v.clone()))
                .collect()),
        }
    }
}

/// An assign-only consumer over any number of partitions.
///
/// Assignment is the caller's: no group protocol, no rebalance, no offset
/// commit. Where the positions live is also the caller's problem, which is what
/// makes this usable from a system that checkpoints offsets itself.
pub struct Consumer<T: Transport> {
    cluster: Cluster<T>,
    /// `(topic, partition)` → the offset the next fetch asks for.
    positions: BTreeMap<(String, i32), i64>,
    isolation: IsolationLevel,
    max_wait: Duration,
    /// Per **partition** budget.
    max_bytes: i32,
    /// Per **response** budget, across every partition in the request.
    ///
    /// These were the same number, and that was the whole consume bottleneck.
    /// One `Fetch` per broker carries many partitions, so a single 10 MiB cap
    /// on the response is 10 MiB shared between them — while a client that
    /// fetches each partition separately gets 10 MiB *each*. Measured, the
    /// per-partition shape was nearly three times faster, which had nothing to
    /// do with connections or decoding and everything to do with this.
    max_response_bytes: i32,
    /// One session per broker address.
    sessions: BTreeMap<String, Session>,
    /// Whether to use incremental fetch at all. On by default; a caller with a
    /// broker that mishandles sessions can turn it off without changing code.
    incremental: bool,
    /// A fetch already in flight, waiting to be collected. See
    /// [`Self::poll`].
    outstanding: Option<Outstanding>,
    /// Whether to keep a fetch permanently in flight. On by default.
    prefetch: bool,
    /// The group this consumer belongs to, if it subscribed rather than
    /// assigned. See [`Self::subscribe`].
    group: Option<crate::group::ClassicProtocol>,
    /// Where to start a partition the group has no committed offset for.
    reset: i64,
    /// Commit positions periodically without being asked.
    auto_commit: Option<Duration>,
    /// When the last automatic commit happened.
    last_auto_commit: Option<std::time::Instant>,
    /// Told when partitions arrive and before they are taken away.
    listener: Option<Box<dyn RebalanceListener>>,
    /// `(session, rebalance)`, applied when a group is joined.
    group_timeouts: Option<(Duration, Duration)>,
    /// Partitions held but not fetched. Separate from `positions` because a
    /// paused partition is still **assigned**: it keeps its offset, it counts
    /// against the group, and resuming it must not re-resolve where it was.
    paused: BTreeSet<(String, i32)>,
    /// Bumped whenever the assignment or a position changes for a reason other
    /// than consuming records. An outstanding fetch from an older generation
    /// asked a question that is no longer the one being asked.
    generation: u64,
}

/// One broker's address, the partitions it was asked about, and the request.
type Planned = (String, Vec<(String, i32)>, FetchRequest);

/// A fetch that has been sent and not yet collected.
struct Outstanding {
    /// The partitions each broker was asked about, in request order.
    groups: Vec<(String, Vec<(String, i32)>)>,
    generation: u64,
}

impl<T: Transport> Consumer<T> {
    /// A staged builder, which is the guided way in — see
    /// [`builder`](crate::builder).
    pub fn builder(transport: T) -> crate::builder::ConsumerBuilder<T> {
        crate::builder::ConsumerBuilder::new(transport)
    }

    /// Wrap an already-configured cluster, so the builder can set credentials
    /// before the first request.
    pub(crate) fn from_cluster(cluster: Cluster<T>, isolation: IsolationLevel) -> Self {
        Self {
            cluster,
            positions: BTreeMap::new(),
            paused: BTreeSet::new(),
            isolation,
            max_wait: Duration::from_millis(500),
            max_bytes: 10 * 1024 * 1024,
            max_response_bytes: 64 * 1024 * 1024,
            sessions: BTreeMap::new(),
            incremental: true,
            outstanding: None,
            prefetch: true,
            group: None,
            reset: EARLIEST,
            auto_commit: None,
            last_auto_commit: None,
            listener: None,
            group_timeouts: None,
            generation: 0,
        }
    }

    /// Connect with no assignments. Add them with [`Self::assign`].
    ///
    /// # Errors
    /// If no bootstrap address answers.
    pub async fn new(
        transport: T,
        bootstrap: &[String],
        client_id: &str,
        isolation: IsolationLevel,
    ) -> Result<Self> {
        Ok(Self {
            cluster: Cluster::connect(transport, bootstrap, client_id).await?,
            positions: BTreeMap::new(),
            paused: BTreeSet::new(),
            isolation,
            max_wait: Duration::from_millis(500),
            max_bytes: 10 * 1024 * 1024,
            max_response_bytes: 64 * 1024 * 1024,
            sessions: BTreeMap::new(),
            incremental: true,
            outstanding: None,
            prefetch: true,
            group: None,
            reset: EARLIEST,
            auto_commit: None,
            last_auto_commit: None,
            listener: None,
            group_timeouts: None,
            generation: 0,
        })
    }

    /// Connect and assign one partition — the common case, and what the
    /// single-partition callers use.
    ///
    /// # Errors
    /// As [`Self::new`], plus a missing topic or partition.
    pub async fn for_partition(
        transport: T,
        bootstrap: &[String],
        client_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        isolation: IsolationLevel,
    ) -> Result<Self> {
        let mut me = Self::new(transport, bootstrap, client_id, isolation).await?;
        me.assign(topic, partition, offset).await?;
        Ok(me)
    }

    /// Assign another partition, starting at `offset`.
    ///
    /// `offset` may be [`EARLIEST`], [`LATEST`], or an absolute offset; the
    /// first two are resolved with `ListOffsets` before the first fetch.
    ///
    /// # Errors
    /// If the topic does not exist, or the broker answers with an error code.
    pub async fn assign(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        // **Before anything else touches a connection.** A prefetched `Fetch`
        // may be sitting unread on one of them, and a `Metadata` sent past it
        // would be answered by that fetch — responses come back in order, so
        // the decode would be of the wrong message for the wrong request.
        self.discard_outstanding().await;

        // Up front so a missing topic fails here rather than as a fetch loop
        // that never returns anything.
        self.cluster.refresh_metadata(topic).await?;
        self.positions
            .insert((topic.to_owned(), partition), offset);

        if offset == EARLIEST || offset == LATEST {
            let resolved = self.list_offset(topic, partition, offset).await?;
            self.positions
                .insert((topic.to_owned(), partition), resolved);
        }
        // A new assignment changes the set the broker remembers.
        for session in self.sessions.values_mut() {
            session.reset();
        }
        self.generation += 1;
        Ok(())
    }

    /// How many partitions `topic` has, from the broker's metadata.
    ///
    /// This client never chooses partitions for you — there is no consumer
    /// group, so nothing assigns them behind your back — but it does have to
    /// ask the broker where they live, and the count comes back with that.
    ///
    /// # Errors
    /// If metadata cannot be refreshed, or the topic does not exist.
    pub async fn partition_count(&mut self, topic: &str) -> Result<i32> {
        self.cluster.partition_count(topic).await
    }

    /// Join `group_id` and let the group decide which partitions this consumer
    /// reads.
    ///
    /// **This is the shape most programs want**, and the opposite of
    /// [`Self::assign`]: partitions arrive from the group's leader and change
    /// when membership does. [`Self::poll`] drives the membership as a side
    /// effect, so a caller that keeps polling keeps its place in the group.
    ///
    /// `reset` decides where a partition starts when the group has never
    /// committed an offset for it — Kafka's `auto.offset.reset`.
    ///
    /// # Errors
    /// If the coordinator cannot be found.
    pub async fn subscribe(
        &mut self,
        group_id: &str,
        topics: Vec<String>,
        assignor: Box<dyn kestrel_core::group::Assignor>,
        reset: i64,
    ) -> Result<()> {
        self.discard_outstanding().await;
        self.positions.clear();
        self.paused.clear();
        self.reset = reset;
        let mut protocol =
            crate::group::ClassicProtocol::new(group_id.to_owned(), topics, assignor);
        if let Some((session, rebalance)) = self.group_timeouts {
            protocol.set_session_timeout(i32::try_from(session.as_millis()).unwrap_or(i32::MAX));
            protocol
                .set_rebalance_timeout(i32::try_from(rebalance.as_millis()).unwrap_or(i32::MAX));
        }
        self.group = Some(protocol);
        self.generation += 1;
        Ok(())
    }

    /// Commit positions on a timer, without the caller asking.
    ///
    /// Kafka's `enable.auto.commit` with `auto.commit.interval.ms`, and the same
    /// guarantee: **at least once**. The commit happens at the *start* of a
    /// [`Self::poll`], so what is committed is where the previous poll's records
    /// ended — records handed to the caller and not yet committed are
    /// re-delivered after a crash. A caller that needs a record committed only
    /// once it is durably handled should commit itself, with
    /// [`Self::commit`], after it has.
    ///
    /// Off by default, because "at least once, silently" is a worse surprise
    /// than having to ask.
    pub fn set_auto_commit(&mut self, interval: Option<Duration>) {
        self.auto_commit = interval;
        self.last_auto_commit = None;
    }

    /// Be told when partitions arrive and before they are taken away.
    ///
    /// The Kafka equivalent of `ConsumerRebalanceListener`. See
    /// [`RebalanceListener`] for why these are synchronous.
    pub fn set_rebalance_listener(&mut self, listener: Box<dyn RebalanceListener>) {
        self.listener = Some(listener);
    }

    /// How long the coordinator waits for a heartbeat before removing this
    /// member, and how long it waits for the group to rejoin a rebalance.
    ///
    /// Kafka's `session.timeout.ms` and `max.poll.interval.ms`. The rebalance
    /// timeout also bounds how long a `JoinGroup` may be held: the coordinator
    /// keeps it until every member has rejoined, so a small value fails a stuck
    /// rebalance quickly and a large one waits patiently for slow members.
    ///
    /// Must be set before [`Self::subscribe`]; changing it afterwards would
    /// disagree with what the group was told.
    pub fn set_group_timeouts(&mut self, session: Duration, rebalance: Duration) {
        self.group_timeouts = Some((session, rebalance));
    }

    /// Tell the coordinator this member is alive, without polling.
    ///
    /// **This client spawns nothing, so heartbeats ride on [`Self::poll`].**
    /// That is fine for a caller that polls in a loop, and wrong for one that
    /// spends longer than `session.timeout.ms` handling a batch: the
    /// coordinator removes a member it has not heard from, its partitions are
    /// given to someone else, and the slow member's next commit is rejected.
    ///
    /// Java hides this with a background heartbeat thread and a separate
    /// `max.poll.interval.ms`. The equivalent here is to call this from your
    /// own task while you work — it needs only `&mut Consumer`, so a caller
    /// that processes on the same executor can interleave it.
    ///
    /// Returns whether the assignment changed, which is the same signal
    /// [`Self::poll`] acts on: `true` means partitions were revoked or granted
    /// and any in-flight work on the old ones should stop.
    ///
    /// # Errors
    /// If the coordinator cannot be reached. Not being in a group is not an
    /// error — it simply does nothing.
    pub async fn heartbeat(&mut self) -> Result<bool> {
        if self.group.is_none() {
            return Ok(false);
        }
        self.advance_group().await
    }

    /// Leave the group, giving up every partition.
    ///
    /// **Worth calling before dropping a consumer.** Without it the coordinator
    /// keeps this member until its session times out — tens of seconds during
    /// which its partitions are read by nobody, and any other member joining
    /// waits out the same delay.
    ///
    /// # Errors
    /// If the coordinator cannot be reached. The member is forgotten locally
    /// either way: the coordinator drops it at the session timeout regardless.
    pub async fn unsubscribe(&mut self) -> Result<()> {
        self.discard_outstanding().await;
        self.positions.clear();
        self.generation += 1;
        let Some(mut group) = self.group.take() else {
            return Ok(());
        };
        crate::group::GroupProtocol::leave(&mut group, &mut self.cluster).await
    }

    /// Commit the position of every partition this consumer holds.
    ///
    /// The position is **the next offset to read**, which is what Kafka stores
    /// and what a restart resumes from.
    ///
    /// # Errors
    /// If this consumer is not in a group, or the group is mid-rebalance — a
    /// commit then would write an offset for a partition that may already
    /// belong to another member.
    pub async fn commit(&mut self) -> Result<()> {
        // **Before anything else touches a connection.** A prefetched `Fetch`
        // is sitting unread on one of them, and the coordinator for this group
        // may be that same broker — responses come back in order, so the commit
        // would decode the fetch's answer. `add` and `list_offset` drain for
        // the same reason; this one was missed, and the symptom was a commit
        // failing with "decode Fetch v12 response".
        self.discard_outstanding().await;

        let offsets: BTreeMap<kestrel_core::group::TopicPartition, i64> = self
            .positions
            .iter()
            .map(|((topic, partition), offset)| {
                (
                    kestrel_core::group::TopicPartition::new(topic.clone(), *partition),
                    *offset,
                )
            })
            .collect();

        let Some(group) = self.group.as_mut() else {
            return Err(Error::Missing("a group to commit to"));
        };
        crate::group::GroupProtocol::commit(group, &mut self.cluster, &offsets).await
    }

    /// Where this consumer would commit to, per partition: **the next offset
    /// to read**, not the last one read.
    ///
    /// For exactly-once with a group, this is the map to hand
    /// [`Producer::send_offsets_to_transaction`](crate::Producer::send_offsets_to_transaction)
    /// together with [`Self::group_metadata`]. Read it *after* the records it
    /// covers have been produced, or the transaction commits offsets for output
    /// it did not write.
    #[must_use]
    pub fn positions(&self) -> BTreeMap<kestrel_core::group::TopicPartition, i64> {
        self.positions
            .iter()
            .map(|((topic, partition), offset)| {
                (
                    kestrel_core::group::TopicPartition::new(topic.clone(), *partition),
                    *offset,
                )
            })
            .collect()
    }

    /// This member's identity and fencing token, or `None` if it is not in a
    /// group or not currently stable.
    ///
    /// Hand it to a transactional producer so the coordinator can reject
    /// offsets from a member that has already been replaced. **Fetch it fresh
    /// per transaction** — a rebalance in between invalidates it, and that is
    /// the whole point of it.
    #[must_use]
    pub fn group_metadata(&self) -> Option<crate::group::GroupMetadata> {
        self.group
            .as_ref()
            .and_then(crate::group::GroupProtocol::<T>::group_metadata)
    }

    /// Commit if auto-commit is on and its interval has elapsed.
    async fn maybe_auto_commit(&mut self) -> Result<()> {
        let Some(interval) = self.auto_commit else {
            return Ok(());
        };
        let due = self
            .last_auto_commit
            .is_none_or(|last| last.elapsed() >= interval);
        if !due || self.positions.is_empty() {
            return Ok(());
        }
        // A commit refused because the group is mid-rebalance is not an error
        // for the caller: the partitions are about to belong to someone else,
        // and the offsets go with them.
        match self.commit().await {
            Ok(()) | Err(Error::Broker { op: "OffsetCommit", .. }) => {}
            Err(e) => return Err(e),
        }
        self.last_auto_commit = Some(std::time::Instant::now());
        Ok(())
    }

    /// Drive group membership to a settled state, if this consumer subscribed.
    ///
    /// **Loops until the member is stable**, rather than taking one protocol
    /// step per call. A rejoin is `JoinGroup` then `SyncGroup`, sometimes twice
    /// over — and a coordinator waits only `rebalance.timeout.ms` for every
    /// member to come back. A client that advanced one step per poll spent a
    /// whole poll cycle on each, so the rebalance completed without it, and its
    /// next heartbeat returned `UNKNOWN_MEMBER_ID`: dropped, rejoined as a new
    /// member, and the group never settled. Java's `joinGroupIfNeeded` loops
    /// for the same reason.
    ///
    /// Returns whether the assignment changed.
    async fn advance_group(&mut self) -> Result<bool> {
        // Bounded so a group that genuinely cannot settle returns to the caller
        // instead of spinning here: a poll that never comes back is worse than
        // one that reports no records.
        const MAX_STEPS: usize = 20;

        let mut changed = false;
        for _ in 0..MAX_STEPS {
            let Some(mut group) = self.group.take() else {
                return Ok(changed);
            };
            let outcome =
                crate::group::GroupProtocol::advance(&mut group, &mut self.cluster).await;

            let settled = match &outcome {
                Ok(crate::group::Membership::Assigned(partitions)) => {
                    let wanted: BTreeMap<(String, i32), ()> = partitions
                        .iter()
                        .map(|tp| ((tp.topic.clone(), tp.partition), ()))
                        .collect();
                    let same = wanted.len() == self.positions.len()
                        && wanted.keys().all(|k| self.positions.contains_key(k));
                    if !same {
                        let committed = crate::group::GroupProtocol::committed(
                            &mut group,
                            &mut self.cluster,
                            partitions,
                        )
                        .await?;
                        self.positions.clear();
                        for tp in partitions {
                            let start = committed.get(tp).copied().unwrap_or(self.reset);
                            self.positions
                                .insert((tp.topic.clone(), tp.partition), start);
                        }
                        if let Some(listener) = self.listener.as_mut() {
                            listener.on_assigned(partitions);
                        }
                        changed = true;
                    }
                    true
                }
                Ok(crate::group::Membership::Revoked(lost)) => {
                    // Only what was actually lost: the eager protocol reports
                    // everything, the cooperative one only what moved.
                    if let Some(listener) = self.listener.as_mut() {
                        listener.on_revoked(lost);
                    }
                    for tp in lost {
                        self.positions.remove(&(tp.topic.clone(), tp.partition));
                        // **A pause does not survive losing the partition.**
                        // Another member is about to read it, and if this one
                        // is given it back the pause would be invisible state
                        // that silently stops consumption.
                        self.paused.remove(&(tp.topic.clone(), tp.partition));
                    }
                    changed |= !lost.is_empty();
                    false
                }
                Ok(crate::group::Membership::InProgress) | Err(_) => false,
            };

            self.group = Some(group);
            outcome?;
            if settled {
                break;
            }
        }

        if changed {
            self.generation += 1;
            for session in self.sessions.values_mut() {
                session.reset();
            }
            let unresolved: Vec<(String, i32, i64)> = self
                .positions
                .iter()
                .filter(|(_, offset)| **offset == EARLIEST || **offset == LATEST)
                .map(|((topic, partition), offset)| (topic.clone(), *partition, *offset))
                .collect();
            for (topic, partition, offset) in unresolved {
                let resolved = self.list_offset(&topic, partition, offset).await?;
                self.positions.insert((topic, partition), resolved);
            }
        }
        Ok(changed)
    }

    /// Stop fetching a partition.
    ///
    /// Resets the fetch sessions: the broker's remembered partition set no
    /// longer matches ours, and correcting it with `forgotten_topics_data` is
    /// more machinery than a fresh full fetch costs.
    pub fn remove(&mut self, topic: &str, partition: i32) {
        self.positions.remove(&(topic.to_owned(), partition));
        self.paused.remove(&(topic.to_owned(), partition));
        self.generation += 1;
        for session in self.sessions.values_mut() {
            session.reset();
        }
    }

    /// Stop fetching these partitions without giving them up.
    ///
    /// The partitions stay assigned and keep their positions — this is
    /// backpressure, not a revocation, and a paused consumer must keep polling
    /// or the group will decide it is gone.
    ///
    /// Resets the fetch sessions for the same reason [`Self::remove`] does: the
    /// broker's remembered set no longer matches ours.
    pub fn pause(&mut self, partitions: &[kestrel_core::group::TopicPartition]) {
        for tp in partitions {
            self.paused.insert((tp.topic.clone(), tp.partition));
        }
        self.on_fetch_set_changed();
    }

    /// Fetch these partitions again, from wherever they stopped.
    pub fn resume(&mut self, partitions: &[kestrel_core::group::TopicPartition]) {
        for tp in partitions {
            self.paused.remove(&(tp.topic.clone(), tp.partition));
        }
        self.on_fetch_set_changed();
    }

    /// Every partition currently paused, whether or not it is still assigned.
    pub fn paused(&self) -> impl Iterator<Item = (&str, i32)> {
        self.paused
            .iter()
            .map(|(topic, partition)| (topic.as_str(), *partition))
    }

    /// Whether one partition is paused.
    #[must_use]
    pub fn is_paused(&self, topic: &str, partition: i32) -> bool {
        self.paused.contains(&(topic.to_owned(), partition))
    }

    /// A prefetch in flight asked about a set that no longer applies, and the
    /// broker's session remembers that set too.
    fn on_fetch_set_changed(&mut self) {
        self.generation += 1;
        for session in self.sessions.values_mut() {
            session.reset();
        }
    }

    /// Assigned and not paused: what a fetch may ask about.
    fn fetchable(&self) -> Vec<(String, i32)> {
        self.positions
            .keys()
            .filter(|key| !self.paused.contains(*key))
            .cloned()
            .collect()
    }

    /// Every partition this consumer holds.
    pub fn assignments(&self) -> impl Iterator<Item = (&str, i32)> {
        self.positions
            .keys()
            .map(|(topic, partition)| (topic.as_str(), *partition))
    }

    /// Where the next fetch will start for one partition.
    #[must_use]
    pub fn position_of(&self, topic: &str, partition: i32) -> Option<i64> {
        self.positions.get(&(topic.to_owned(), partition)).copied()
    }

    /// Where the next fetch will start, for a consumer holding exactly one
    /// partition.
    ///
    /// # Panics
    /// If the consumer holds anything other than one assignment — with several,
    /// "the position" is not a question with an answer.
    #[must_use]
    pub fn position(&self) -> i64 {
        assert_eq!(
            self.positions.len(),
            1,
            "position() needs exactly one assignment; use position_of()"
        );
        *self.positions.values().next().expect("checked length")
    }

    /// Seek one partition. The caller owns its offsets, so this is how a
    /// restored checkpoint is applied.
    pub fn seek_to(&mut self, topic: &str, partition: i32, offset: i64) {
        self.positions
            .insert((topic.to_owned(), partition), offset);
        self.generation += 1;
    }

    /// Seek, for a consumer holding exactly one partition.
    ///
    /// # Panics
    /// As [`Self::position`].
    pub fn seek(&mut self, offset: i64) {
        assert_eq!(
            self.positions.len(),
            1,
            "seek() needs exactly one assignment; use seek_to()"
        );
        let key = self.positions.keys().next().expect("checked length").clone();
        self.positions.insert(key, offset);
        self.generation += 1;
    }

    /// Use incremental fetch sessions (KIP-227). On by default.
    ///
    /// Turning this off makes every fetch restate every partition, which is
    /// what the client did before sessions existed — useful if a broker or
    /// proxy mishandles them.
    pub fn set_incremental_fetch(&mut self, incremental: bool) {
        self.incremental = incremental;
        if !incremental {
            self.sessions.clear();
        }
        self.generation += 1;
    }

    /// Keep a fetch permanently in flight. On by default.
    ///
    /// **This is what overlaps the network with the caller's work.** Without
    /// it a fetch is issued only when [`Self::poll`] is called, so every poll
    /// pays a full round trip before it can return anything; with it the
    /// request for the next poll goes out as soon as the current one is
    /// decoded, and the caller's processing happens while the broker is
    /// already working.
    ///
    /// Exactly one fetch per broker is outstanding, never more: the fetch
    /// session epoch advances per accepted response, so a second in-flight
    /// request would carry an epoch the broker has not reached.
    pub fn set_prefetch(&mut self, prefetch: bool) {
        self.prefetch = prefetch;
        self.generation += 1;
    }

    /// How long a fetch waits for data before returning empty.
    pub fn set_max_wait(&mut self, max_wait: Duration) {
        self.max_wait = max_wait;
        self.generation += 1;
    }

    /// The connections this consumer holds — one per broker it fetches from,
    /// **not** one per partition.
    #[must_use]
    pub fn connection_count(&self) -> usize {
        self.cluster.connection_count()
    }

    /// The address the cluster map names as this partition's leader.
    #[must_use]
    pub fn metadata_leader(&self, topic: &str, partition: i32) -> Option<String> {
        self.cluster
            .metadata()
            .leader_for(topic, partition)
            .map(kestrel_core::BrokerAddr::addr)
    }

    /// Resolve a timestamp to an offset for one partition. [`EARLIEST`] and
    /// [`LATEST`] are the two a consumer normally wants.
    ///
    /// # Errors
    /// If the broker answers with an error code.
    pub async fn list_offset(
        &mut self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<i64> {
        // Public, so it can be called with a prefetch in flight. See
        // [`Self::assign`].
        self.discard_outstanding().await;

        let mut req_partition = ListOffsetsPartition::default();
        req_partition.partition_index = partition;
        req_partition.timestamp = timestamp;

        let mut req_topic = ListOffsetsTopic::default();
        req_topic.name = TopicName(StrBytes::from_string(topic.to_owned()));
        req_topic.partitions = vec![req_partition];

        let mut req = ListOffsetsRequest::default();
        req.replica_id = BrokerId(-1);
        req.isolation_level = self.isolation.as_i8();
        req.topics = vec![req_topic];

        for attempt in 0..=MAX_LEADER_RETRIES {
            // A partition mid-election has no leader *yet*. That is a wait, not
            // a failure — the producer has always treated it that way, and a
            // consumer assigned to a topic created a moment ago hit the other
            // behaviour and simply failed.
            let addr = match self.cluster.leader_addr(topic, partition).await {
                Ok(addr) => addr,
                Err(e @ Error::NoLeader { .. }) => {
                    if attempt == MAX_LEADER_RETRIES {
                        return Err(e);
                    }
                    T::sleep(LEADER_BACKOFF).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp: ListOffsetsResponse = self
                .cluster
                .call_at(&addr, ApiKey::ListOffsets, 7, &req)
                .await?;

            let found = resp
                .topics
                .iter()
                .flat_map(|t| t.partitions.iter())
                .find(|p| p.partition_index == partition)
                .ok_or(Error::Missing("partition"))?;

            let code = ErrorCode(found.error_code);
            if code.disposition() == Disposition::RefreshMetadata {
                self.cluster.invalidate(topic, partition);
                if attempt == MAX_LEADER_RETRIES {
                    return Err(Error::Broker {
                        op: "ListOffsets",
                        code: code.0,
                        disposition: code.disposition(),
                    });
                }
                self.cluster.refresh_metadata(topic).await?;
                T::sleep(LEADER_BACKOFF).await;
                continue;
            }
            check("ListOffsets", found.error_code)?;
            return Ok(found.offset);
        }
        unreachable!("the loop returns on its last attempt")
    }

    /// `ListOffsets` for many partitions at once: **one request per leader**,
    /// not one per partition.
    ///
    /// Every lookup below is this: `end_offsets` on a 64-partition assignment
    /// is one or two round trips rather than 64. Returns `(offset, timestamp)`
    /// per partition, and **omits** a partition whose answer is "no such
    /// offset" (`-1`), which is what `offsets_for_times` needs to distinguish
    /// from offset zero.
    async fn list_offsets_many(
        &mut self,
        want: &[(kestrel_core::group::TopicPartition, i64)],
    ) -> Result<BTreeMap<kestrel_core::group::TopicPartition, (i64, i64)>> {
        // See [`Self::assign`]: a prefetch is sitting unread on a connection
        // this is about to reuse.
        self.discard_outstanding().await;

        let mut found = BTreeMap::new();
        if want.is_empty() {
            return Ok(found);
        }

        let mut remaining: Vec<(kestrel_core::group::TopicPartition, i64)> = want.to_vec();
        for attempt in 0..=MAX_LEADER_RETRIES {
            // Group by leader afresh each attempt: a retry is usually here
            // *because* leadership moved.
            let mut by_leader: BTreeMap<String, Vec<(kestrel_core::group::TopicPartition, i64)>> =
                BTreeMap::new();
            let mut no_leader: Option<Error> = None;
            for (tp, timestamp) in &remaining {
                match self.cluster.leader_addr(&tp.topic, tp.partition).await {
                    Ok(addr) => by_leader
                        .entry(addr)
                        .or_default()
                        .push((tp.clone(), *timestamp)),
                    Err(e @ Error::NoLeader { .. }) => no_leader = Some(e),
                    Err(e) => return Err(e),
                }
            }

            let mut retry: Vec<(kestrel_core::group::TopicPartition, i64)> = Vec::new();
            let mut refresh: Vec<String> = Vec::new();
            for (addr, group) in by_leader {
                let mut topics: BTreeMap<String, Vec<ListOffsetsPartition>> = BTreeMap::new();
                for (tp, timestamp) in &group {
                    let mut entry = ListOffsetsPartition::default();
                    entry.partition_index = tp.partition;
                    entry.timestamp = *timestamp;
                    topics.entry(tp.topic.clone()).or_default().push(entry);
                }

                let mut req = ListOffsetsRequest::default();
                req.replica_id = BrokerId(-1);
                req.isolation_level = self.isolation.as_i8();
                req.topics = topics
                    .into_iter()
                    .map(|(name, partitions)| {
                        let mut topic = ListOffsetsTopic::default();
                        topic.name = TopicName(StrBytes::from_string(name));
                        topic.partitions = partitions;
                        topic
                    })
                    .collect();

                let resp: ListOffsetsResponse = self
                    .cluster
                    .call_at(&addr, ApiKey::ListOffsets, 7, &req)
                    .await?;

                for topic in &resp.topics {
                    for partition in &topic.partitions {
                        let tp = kestrel_core::group::TopicPartition::new(
                            topic.name.0.to_string(),
                            partition.partition_index,
                        );
                        let code = ErrorCode(partition.error_code);
                        if code.is_ok() {
                            // `-1` is "nothing at or after that timestamp", not
                            // an error and not offset -1.
                            if partition.offset >= 0 {
                                found.insert(tp, (partition.offset, partition.timestamp));
                            }
                            continue;
                        }
                        if code.disposition() == Disposition::RefreshMetadata {
                            self.cluster.invalidate(&tp.topic, tp.partition);
                            refresh.push(tp.topic.clone());
                            let timestamp = group
                                .iter()
                                .find(|(w, _)| *w == tp)
                                .map_or(LATEST, |(_, t)| *t);
                            retry.push((tp, timestamp));
                            continue;
                        }
                        check("ListOffsets", partition.error_code)?;
                    }
                }
            }

            // A partition whose leader is mid-election is a wait, not a
            // failure — the same rule `list_offset` follows.
            if let Some(e) = no_leader {
                if retry.is_empty() && attempt == MAX_LEADER_RETRIES {
                    return Err(e);
                }
                for (tp, timestamp) in &remaining {
                    if !found.contains_key(tp) && !retry.iter().any(|(r, _)| r == tp) {
                        retry.push((tp.clone(), *timestamp));
                    }
                }
            }

            if retry.is_empty() {
                return Ok(found);
            }
            if attempt == MAX_LEADER_RETRIES {
                return Err(Error::Broker {
                    op: "ListOffsets",
                    code: ErrorCode::NOT_LEADER_OR_FOLLOWER.0,
                    disposition: Disposition::RefreshMetadata,
                });
            }
            refresh.sort();
            refresh.dedup();
            for topic in refresh {
                self.cluster.refresh_metadata(&topic).await?;
            }
            T::sleep(LEADER_BACKOFF).await;
            remaining = retry;
        }
        unreachable!("the loop returns on its last attempt")
    }

    /// The offset **after** the last record of each partition — the log end.
    ///
    /// Under READ_COMMITTED this is the last stable offset, so it does not run
    /// ahead of what a committed reader can see, and lag computed from it does
    /// not sit permanently at the size of an open transaction.
    ///
    /// # Errors
    /// If no leader answers.
    pub async fn end_offsets(
        &mut self,
        partitions: &[kestrel_core::group::TopicPartition],
    ) -> Result<BTreeMap<kestrel_core::group::TopicPartition, i64>> {
        let want: Vec<_> = partitions.iter().cloned().map(|tp| (tp, LATEST)).collect();
        Ok(self
            .list_offsets_many(&want)
            .await?
            .into_iter()
            .map(|(tp, (offset, _))| (tp, offset))
            .collect())
    }

    /// The offset of the oldest record still retained in each partition.
    ///
    /// Not zero: retention and `DeleteRecords` move it forward, and assuming
    /// zero is how a consumer asks for an offset the broker has deleted.
    ///
    /// # Errors
    /// As [`Self::end_offsets`].
    pub async fn beginning_offsets(
        &mut self,
        partitions: &[kestrel_core::group::TopicPartition],
    ) -> Result<BTreeMap<kestrel_core::group::TopicPartition, i64>> {
        let want: Vec<_> = partitions.iter().cloned().map(|tp| (tp, EARLIEST)).collect();
        Ok(self
            .list_offsets_many(&want)
            .await?
            .into_iter()
            .map(|(tp, (offset, _))| (tp, offset))
            .collect())
    }

    /// The first offset at or after each timestamp, with the timestamp of the
    /// record found.
    ///
    /// A partition with **no** record at or after its timestamp is absent from
    /// the result rather than present with a sentinel, the same distinction
    /// [`Self::committed`] draws. Timestamps are milliseconds since the epoch.
    ///
    /// # Errors
    /// As [`Self::end_offsets`].
    pub async fn offsets_for_times(
        &mut self,
        want: &[(kestrel_core::group::TopicPartition, i64)],
    ) -> Result<BTreeMap<kestrel_core::group::TopicPartition, (i64, i64)>> {
        self.list_offsets_many(want).await
    }

    /// How far each assigned partition is behind its log end.
    ///
    /// A partition this consumer has not read from yet has no position, so it
    /// is absent — "unknown lag" and "zero lag" are different answers, and an
    /// alert built on the second one stays quiet through a consumer that never
    /// started.
    ///
    /// # Errors
    /// As [`Self::end_offsets`].
    pub async fn lag(&mut self) -> Result<BTreeMap<kestrel_core::group::TopicPartition, i64>> {
        let positions = self.positions();
        let assigned: Vec<_> = positions.keys().cloned().collect();
        let ends = self.end_offsets(&assigned).await?;
        Ok(positions
            .into_iter()
            .filter_map(|(tp, position)| {
                ends.get(&tp).map(|end| (tp, (end - position).max(0)))
            })
            .collect())
    }

    /// Where this consumer's group last committed, for the partitions given.
    ///
    /// A partition with no committed offset is **absent**, not zero.
    ///
    /// # Errors
    /// If this consumer is not in a group.
    pub async fn committed(
        &mut self,
        partitions: &[kestrel_core::group::TopicPartition],
    ) -> Result<BTreeMap<kestrel_core::group::TopicPartition, i64>> {
        self.discard_outstanding().await;
        let Some(group) = self.group.as_mut() else {
            return Err(Error::Missing("a group to read commits from"));
        };
        crate::group::GroupProtocol::committed(group, &mut self.cluster, partitions).await
    }

    /// Send one `Fetch` per broker and record what was asked, without waiting.
    ///
    /// If a send fails partway, whatever was already sent is still recorded —
    /// those answers are outstanding whether or not the rest went out, and
    /// leaving them unrecorded would strand them on the connection.
    async fn issue_fetch(&mut self) -> Result<()> {
        let mut by_broker: BTreeMap<String, Vec<(String, i32)>> = BTreeMap::new();
        for (topic, partition) in self.fetchable() {
            let addr = self.cluster.leader_addr(&topic, partition).await?;
            by_broker.entry(addr).or_default().push((topic, partition));
        }

        // Built before sending: `fetch_request` borrows `self`, and sending
        // borrows the cluster mutably.
        let planned: Vec<Planned> = by_broker
            .into_iter()
            .map(|(addr, partitions)| {
                let req = self.fetch_request(&addr, &partitions);
                (addr, partitions, req)
            })
            .collect();

        let mut sent: Vec<(String, Vec<(String, i32)>)> = Vec::with_capacity(planned.len());
        let mut failure = None;
        for (addr, partitions, req) in planned {
            match self.cluster.send_at(ApiKey::Fetch, 12, &addr, &req).await {
                Ok(()) => sent.push((addr, partitions)),
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            }
        }

        if sent.is_empty() {
            return failure.map_or(Ok(()), Err);
        }
        self.outstanding = Some(Outstanding {
            groups: sent,
            generation: self.generation,
        });
        failure.map_or(Ok(()), Err)
    }

    /// Put the next round's fetch in flight, if prefetch is on.
    ///
    /// A failure here is deliberately not surfaced: nothing is outstanding
    /// afterwards, so the next [`Self::poll`] issues the request itself and
    /// reports whatever goes wrong then. Returning it from *this* call would
    /// fail a poll that had already succeeded.
    async fn start_prefetch(&mut self) {
        if self.prefetch && !self.fetchable().is_empty() {
            let _ = self.issue_fetch().await;
        }
    }

    /// Read and throw away an outstanding fetch whose question is stale.
    ///
    /// The sessions are reset because the broker advanced its own view when it
    /// answered; the next request has to be a full fetch for the two to agree.
    async fn discard_outstanding(&mut self) {
        let Some(outstanding) = self.outstanding.take() else {
            return;
        };
        let addrs: Vec<String> = outstanding
            .groups
            .iter()
            .map(|(addr, _)| addr.clone())
            .collect();
        self.cluster
            .discard_many::<FetchResponse>(ApiKey::Fetch, &addrs)
            .await;
        for session in self.sessions.values_mut() {
            session.reset();
        }
    }

    /// One `Fetch` for this broker.
    ///
    /// With a session open, only the partitions whose position moved since the
    /// last accepted response are named — the broker remembers the rest. That
    /// is the whole of KIP-227's benefit: a poll over thirty-two partitions
    /// where two are busy sends two.
    fn fetch_request(&self, addr: &str, partitions: &[(String, i32)]) -> FetchRequest {
        let session = self.sessions.get(addr);
        let incremental = self.incremental && session.is_some_and(|s| s.id != 0);

        let mut by_topic: BTreeMap<&str, Vec<i32>> = BTreeMap::new();
        for (topic, partition) in partitions {
            if incremental {
                let known = session
                    .and_then(|s| s.known.get(&(topic.clone(), *partition)))
                    .copied();
                let current = self.positions.get(&(topic.clone(), *partition)).copied();
                if known == current {
                    // The broker already knows where we are on this partition.
                    continue;
                }
            }
            by_topic.entry(topic.as_str()).or_default().push(*partition);
        }

        let topics: Vec<FetchTopic> = by_topic
            .into_iter()
            .map(|(topic, partitions)| {
                let mut fetch_topic = FetchTopic::default();
                fetch_topic.topic = TopicName(StrBytes::from_string(topic.to_owned()));
                fetch_topic.partitions = partitions
                    .into_iter()
                    .map(|partition| {
                        let mut fetch_partition = FetchPartition::default();
                        fetch_partition.partition = partition;
                        fetch_partition.fetch_offset = self
                            .positions
                            .get(&(topic.to_owned(), partition))
                            .copied()
                            .unwrap_or(0);
                        fetch_partition.partition_max_bytes = self.max_bytes;
                        fetch_partition.current_leader_epoch = -1;
                        fetch_partition.log_start_offset = -1;
                        fetch_partition
                    })
                    .collect();
                fetch_topic
            })
            .collect();

        let mut req = FetchRequest::default();
        req.replica_id = BrokerId(-1);
        req.max_wait_ms = i32::try_from(self.max_wait.as_millis()).unwrap_or(i32::MAX);
        req.min_bytes = 1;
        req.max_bytes = self.max_response_bytes;
        req.isolation_level = self.isolation.as_i8();
        req.topics = topics;
        if self.incremental {
            req.session_id = session.map_or(0, |s| s.id);
            req.session_epoch = session.map_or(0, |s| s.epoch);
        } else {
            // -1 is FINAL_EPOCH: "no session, do not make one".
            req.session_epoch = -1;
        }
        req
    }
}

/// Decode the record batches in one partition's fetch data.
///
/// **The last batch may be truncated, and that is not corruption.** When a
/// fetch hits `max_bytes` the broker cuts the response mid-batch rather than
/// dropping it, and expects the client to ignore the fragment and ask again
/// from where it got to. A decoder that treats the fragment as an error fails
/// the whole fetch — which is exactly what happened the moment several
/// partitions shared a response and the limit started binding.
///
/// So the length prefix is checked before decoding: a batch that is not
/// entirely present ends the loop, while a batch that *is* present and fails to
/// decode is still an error.
fn decode_records(mut bytes: Bytes) -> Result<Vec<Record>> {
    /// `baseOffset` (8) + `batchLength` (4) precede the rest of a v2 batch.
    const HEADER: usize = 12;

    let mut all = Vec::new();
    while bytes.len() >= HEADER {
        let batch_length = i32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        let Ok(batch_length) = usize::try_from(batch_length) else {
            return Err(Error::Core(kestrel_core::Error::Codec(format!(
                "record batch declares a negative length: {batch_length}"
            ))));
        };
        if bytes.len() < HEADER + batch_length {
            // Truncated by the broker's byte limit. Stop here; the next fetch
            // starts from the offset this one reached.
            break;
        }

        let set = RecordBatchDecoder::decode(&mut bytes).map_err(|e| {
            Error::Core(kestrel_core::Error::Codec(format!(
                "decode record batch: {e}"
            )))
        })?;
        all.extend(set.records);
    }
    Ok(all)
}


impl<T: Transport> Consumer<T> {
    /// Fetch every assigned partition, **one request per broker**.
    ///
    /// Records come back grouped in the batches the broker sent, because that
    /// is how the format stores them and how the filtering works. Use
    /// [`ConsumerRecords::iter`] to walk them without caring; a key, value or
    /// header list is materialised when asked for rather than at decode time.
    ///
    /// An empty result is normal: a fetch that waits out `max_wait` with no new
    /// data is not an error. Positions advance past filtered records as well as
    /// returned ones, so an all-aborted fetch makes progress rather than
    /// looping.
    ///
    /// All four compression codecs and record headers are handled; only a
    /// pre-magic-2 batch falls back to the ordinary decoder, and then only that
    /// partition pays the old cost.
    ///
    /// Filtering happens **per batch** here rather than per record, which it
    /// can because `transactional`, `control` and `producer_id` are batch-level
    /// in the format. That is most of why this path is cheaper.
    ///
    /// # Errors
    /// As [`Self::poll`].
    pub async fn poll(&mut self) -> Result<Vec<ConsumerRecords>> {
        // A subscribed consumer keeps its place in the group by polling, which
        // is why membership is driven here rather than by a background task:
        // this client spawns nothing.
        if self.group.is_some() {
            // **Before membership is advanced, not after.** A rebalance is
            // discovered by advancing, and by then the generation that would
            // authorise a commit is gone — so the last chance to commit is
            // now. It is also why auto-commit is at-least-once: anything read
            // since this commit will be read again by whoever takes the
            // partition.
            self.maybe_auto_commit().await?;
            let changed = self.advance_group().await?;
            if changed {
                self.discard_outstanding().await;
            }
        }
        // Not `positions`: a consumer with every partition paused still has an
        // assignment, and must still have polled — the heartbeat above is what
        // keeps it in the group.
        if self.fetchable().is_empty() {
            return Ok(Vec::new());
        }
        if self
            .outstanding
            .as_ref()
            .is_some_and(|o| o.generation != self.generation)
        {
            self.discard_outstanding().await;
        }
        if self.outstanding.is_none() {
            self.issue_fetch().await?;
        }

        let groups = self.outstanding.take().expect("just issued").groups;
        let addrs: Vec<String> = groups.iter().map(|(addr, _)| addr.clone()).collect();
        let responses = self
            .cluster
            .recv_many::<FetchResponse>(ApiKey::Fetch, &addrs)
            .await;

        let mut out = Vec::new();
        for ((addr, partitions), response) in groups.into_iter().zip(responses) {
            let resp = response?;
            if matches!(
                resp.error_code,
                FETCH_SESSION_ID_NOT_FOUND | INVALID_FETCH_SESSION_EPOCH
            ) {
                self.sessions.entry(addr.clone()).or_default().reset();
                continue;
            }
            check("Fetch", resp.error_code)?;

            if self.incremental {
                let session = self.sessions.entry(addr.clone()).or_default();
                session.id = resp.session_id;
                session.epoch = session.epoch.wrapping_add(1).max(1);
                for (topic, partition) in &partitions {
                    if let Some(offset) = self.positions.get(&(topic.clone(), *partition)) {
                        session.known.insert((topic.clone(), *partition), *offset);
                    }
                }
            }

            for topic_response in &resp.responses {
                let topic = topic_response.topic.0.to_string();
                for part in &topic_response.partitions {
                    check("Fetch partition", part.error_code)?;
                    let key = (topic.clone(), part.partition_index);
                    let Some(fetch_offset) = self.positions.get(&key).copied() else {
                        continue;
                    };
                    let Some(bytes) = part.records.clone().filter(|b| !b.is_empty()) else {
                        continue;
                    };

                    let Some(decoded) = kestrel_core::records::decode_lean(&bytes)? else {
                        // Only a pre-magic-2 batch reaches this now: compression
                        // and headers are both handled. Kept because a broker
                        // holding very old data can still serve it, and being
                        // wrong here means bad records rather than an error.
                        let records = decode_records(bytes)?;
                        let aborted = aborted_of(part);
                        let Fetched {
                            records,
                            next_offset,
                        } = consumer::filter(
                            records,
                            &aborted,
                            part.last_stable_offset,
                            self.isolation,
                            fetch_offset,
                        );
                        self.positions.insert(key, next_offset);
                        if !records.is_empty() {
                            out.push(ConsumerRecords {
                                topic: topic.clone(),
                                partition: part.partition_index,
                                batches: Vec::new(),
                                fallback: records,
                            });
                        }
                        continue;
                    };

                    let aborted = aborted_of(part);
                    let (batches, next_offset) = kestrel_core::records::filter_batches(
                        decoded,
                        &aborted,
                        part.last_stable_offset,
                        self.isolation,
                        fetch_offset,
                    );
                    self.positions.insert(key, next_offset);
                    if !batches.is_empty() {
                        out.push(ConsumerRecords {
                            topic: topic.clone(),
                            partition: part.partition_index,
                            batches,
                            fallback: Vec::new(),
                        });
                    }
                }
            }
        }

        self.start_prefetch().await;
        Ok(out)
    }
}

/// The aborted-transaction list a fetch response carries for one partition.
fn aborted_of(part: &kafka_protocol::messages::fetch_response::PartitionData) -> Vec<AbortedTransaction> {
    part.aborted_transactions
        .as_ref()
        .map(|list| {
            list.iter()
                .map(|a| AbortedTransaction {
                    producer_id: a.producer_id.0,
                    first_offset: a.first_offset,
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };

    fn batch(base_offset: i64, count: usize) -> Bytes {
        let records: Vec<Record> = (0..count)
            .map(|i| Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: base_offset + i as i64,
                sequence: i as i32,
                timestamp: 0,
                key: None,
                value: Some(Bytes::from(format!("v{i}"))),
                headers: Default::default(),
            })
            .collect();
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .expect("encode");
        buf.freeze()
    }

    #[test]
    fn whole_batches_decode() {
        let mut wire = BytesMut::new();
        wire.extend_from_slice(&batch(0, 3));
        wire.extend_from_slice(&batch(3, 2));
        let records = decode_records(wire.freeze()).expect("decode");
        assert_eq!(records.len(), 5);
    }

    /// **A fetch that hits `max_bytes` ends mid-batch**, and the broker expects
    /// the fragment to be ignored rather than treated as corruption. Failing
    /// here fails the whole fetch — which is what happened the moment several
    /// partitions shared one response and the limit started binding.
    #[test]
    fn a_truncated_trailing_batch_is_ignored() {
        let complete = batch(0, 3);
        let partial = batch(3, 2);

        let mut wire = BytesMut::new();
        wire.extend_from_slice(&complete);
        wire.extend_from_slice(&partial[..partial.len() - 4]);

        let records = decode_records(wire.freeze()).expect("a truncated tail is not an error");
        assert_eq!(
            records.len(),
            3,
            "the complete batch must survive and the fragment must be dropped"
        );
    }

    /// Even a fragment too short to hold a header is just "nothing more here".
    #[test]
    fn a_fragment_shorter_than_a_header_is_ignored() {
        let mut wire = BytesMut::new();
        wire.extend_from_slice(&batch(0, 1));
        wire.extend_from_slice(&[0u8; 5]);
        assert_eq!(decode_records(wire.freeze()).expect("decode").len(), 1);
    }

    /// A batch that claims a negative length is corruption, not truncation, and
    /// must not be silently skipped.
    #[test]
    fn a_negative_batch_length_is_an_error() {
        let mut wire = BytesMut::new();
        wire.extend_from_slice(&0i64.to_be_bytes());
        wire.extend_from_slice(&(-1i32).to_be_bytes());
        wire.extend_from_slice(&[0u8; 32]);
        assert!(decode_records(wire.freeze()).is_err());
    }
}
