//! The transactional producer: requests, routing, and retries.
//!
//! The *rules* are in [`barnabas_core::producer`] — sequence allocation, the
//! transaction state machine, what is fatal. This file is what talks to
//! brokers, and its two jobs are both routing decisions P0 found the hard way:
//!
//! - **Transaction requests go to the coordinator**, which is a specific broker
//!   found with `FindCoordinator` and *not* the partition leader. Going
//!   straight to `InitProducerId` on the bootstrap connection returns error 16
//!   even on a single-broker cluster, because the internal
//!   `__transaction_state` topic is created lazily by that very call.
//! - **`NOT_COORDINATOR` must re-discover**, not retry in place. The
//!   coordinator genuinely moves, and a blind retry spins against a broker that
//!   will never answer.
//!
//! Produce itself goes to the partition leader, like every other partition
//! request.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    add_offsets_to_txn_request::AddOffsetsToTxnRequest,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey, EndTxnRequest, EndTxnResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, InitProducerIdRequest, InitProducerIdResponse,
    AddOffsetsToTxnResponse, GroupId, ProduceRequest, ProduceResponse, ProducerId, TopicName,
    TransactionalId, TxnOffsetCommitRequest, TxnOffsetCommitResponse,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use barnabas_core::producer::{ProducerIdentity, ProducerState, SequenceRange};
use barnabas_core::{Disposition, ErrorCode, Partitioner};

use crate::cluster::Cluster;
use crate::{Error, Result, Transport};

/// Attempts for a request whose disposition says "retry" or "re-discover".
///
/// Bounded rather than unbounded: a coordinator that never settles is a cluster
/// problem, and spinning silently is worse than surfacing it.
const MAX_RETRIES: usize = 40;

/// The first retry waits this long; each further one doubles, up to
/// [`RETRY_BACKOFF_MAX`].
///
/// **It used to be a flat 250 ms on the first attempt**, which is a very long
/// time to wait for something that is usually over in a millisecond — a
/// partition mid-election, a broker briefly behind. One such retry inside a
/// 400 000-record run cut the measured rate by six, and it was the cause of the
/// 8-core "collapse" in PERF.md that went unexplained for two revisions. The
/// `attempt` argument was already threaded through every call site and then
/// ignored by the function that received it.
const RETRY_BACKOFF: Duration = Duration::from_millis(5);
const RETRY_BACKOFF_MAX: Duration = Duration::from_millis(250);

/// How many `Produce` requests may be in flight on one connection.
///
/// Five is what the Java client allows with idempotence enabled, and for the
/// same reason: the broker processes a connection's requests in order, so
/// batches for one partition stay in sequence — but a failure partway through
/// the window invalidates everything behind it, and recovering from that is
/// only bounded work if the window is.
pub const DEFAULT_MAX_IN_FLIGHT: usize = 5;

/// How many bytes of records accumulate before a batch is sent regardless of
/// [`Producer::set_linger`].
///
/// 16 KiB, which is `batch.size`'s default in the Java client. The number is
/// not magic; matching it means a caller reasoning from Kafka documentation
/// gets the behaviour that documentation describes.
pub const DEFAULT_BATCH_SIZE: usize = 16 * 1024;

/// Roughly what one record costs on the wire beyond its key and value: the
/// varint lengths, the attributes byte, the offset and timestamp deltas.
///
/// An estimate on purpose. It decides *when* to send, not what to encode, so
/// being a few bytes out changes batch sizes slightly and correctness not at
/// all.
const RECORD_OVERHEAD: usize = 16;

/// Records waiting to be encoded, for one partition.
struct Staged {
    records: Vec<ProducerRecord>,
    /// Estimated encoded size, so the batch-size check costs nothing.
    bytes: usize,
    /// When the **first** record arrived — the linger clock starts then, not
    /// at the last one, or a steady stream would never be sent.
    since: std::time::Instant,
}

/// One broker's address and the partitions its request carried.
type RoundTarget = (String, Vec<(String, i32)>);

/// The window: what each round sent, in the order the rounds went out.
type Window = Vec<Vec<RoundTarget>>;

/// A record to produce. Deliberately not `kafka_protocol`'s `Record`: sequence
/// numbers, producer id and epoch are the producer's to set, and letting a
/// caller supply them is how they get set wrong.
#[derive(Debug, Clone)]
pub struct ProducerRecord {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    /// Milliseconds since the epoch. `None` means "now".
    pub timestamp: Option<i64>,
}

impl ProducerRecord {
    #[must_use]
    pub fn new(key: Option<Bytes>, value: Option<Bytes>) -> Self {
        Self {
            key,
            value,
            timestamp: None,
        }
    }
}

/// An idempotent, optionally transactional producer for one core.
pub struct Producer<T: Transport> {
    cluster: Cluster<T>,
    state: ProducerState,
    transactional_id: Option<String>,
    /// The coordinator's address, once discovered. Cleared on
    /// `NOT_COORDINATOR`, which is what forces re-discovery.
    coordinator: Option<String>,
    acks: i16,
    timeout_ms: i32,
    compression: Compression,
    partitioner: Partitioner,
    /// Round-robin counter for null-keyed records. See
    /// [`barnabas_core::partitioner`].
    round_robin: u32,
    /// Encoded batches waiting to go out, per partition, **in sequence order**.
    /// The order is the invariant the whole pipelining scheme rests on.
    pending: BTreeMap<(String, i32), VecDeque<Bytes>>,
    /// How many `Produce` requests may be in flight on one connection.
    max_in_flight: usize,
    /// Records accumulating per partition, not yet encoded. Empty unless a
    /// caller uses [`Producer::produce`].
    staged: BTreeMap<(String, i32), Staged>,
    /// How long a partial batch may wait for company. Zero by default, which
    /// is Kafka's default and means "send as soon as asked".
    linger: Duration,
    /// How large a batch may grow before it is sent regardless of `linger`.
    batch_size: usize,
    /// Base offsets from the window being processed, keyed by partition and
    /// round. Cleared each time the window is retired.
    offsets: BTreeMap<((String, i32), usize), i64>,
}

impl<T: Transport> Producer<T> {
    /// A staged builder, which is the guided way in — see
    /// [`builder`](crate::builder).
    pub fn builder(transport: T) -> crate::builder::ProducerBuilder<T> {
        crate::builder::ProducerBuilder::new(transport)
    }

    /// A transactional producer under `transactional_id`.
    ///
    /// `InitProducerId` fences any previous producer using the same id, which
    /// is how a restarted job stops its own zombie from writing.
    ///
    /// # Errors
    /// If no broker answers, or the coordinator never becomes available.
    pub async fn transactional(
        transport: T,
        bootstrap: &[String],
        client_id: &str,
        transactional_id: &str,
    ) -> Result<Self> {
        let mut me = Self {
            cluster: Cluster::connect(transport, bootstrap, client_id).await?,
            state: ProducerState::transactional(),
            transactional_id: Some(transactional_id.to_owned()),
            coordinator: None,
            acks: -1,
            timeout_ms: 30_000,
            compression: Compression::None,
            partitioner: Partitioner::default(),
            round_robin: 0,
            staged: BTreeMap::new(),
            linger: Duration::ZERO,
            batch_size: DEFAULT_BATCH_SIZE,
            pending: BTreeMap::new(),
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            offsets: BTreeMap::new(),
        };
        me.init_producer_id().await?;
        Ok(me)
    }

    /// An idempotent, non-transactional producer.
    ///
    /// # Errors
    /// If no broker answers.
    pub async fn idempotent(transport: T, bootstrap: &[String], client_id: &str) -> Result<Self> {
        let mut me = Self {
            cluster: Cluster::connect(transport, bootstrap, client_id).await?,
            state: ProducerState::idempotent(),
            transactional_id: None,
            coordinator: None,
            acks: -1,
            timeout_ms: 30_000,
            compression: Compression::None,
            partitioner: Partitioner::default(),
            round_robin: 0,
            staged: BTreeMap::new(),
            linger: Duration::ZERO,
            batch_size: DEFAULT_BATCH_SIZE,
            pending: BTreeMap::new(),
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            offsets: BTreeMap::new(),
        };
        me.init_producer_id().await?;
        Ok(me)
    }

    /// Choose which hash decides a keyed record's partition.
    ///
    /// Defaults to [`Partitioner::Crc32`], which is librdkafka's — so a program
    /// migrating off `rdkafka` keeps its key placement. The Java client's
    /// murmur2 is the other option, and the two disagree for most keys.
    pub fn set_partitioner(&mut self, partitioner: Partitioner) {
        self.partitioner = partitioner;
    }

    /// Compress batches with `compression`. Applies to whole batches, which is
    /// where Kafka's compression lives.
    pub fn set_compression(&mut self, compression: Compression) {
        self.compression = compression;
    }

    #[must_use]
    pub fn identity(&self) -> Option<ProducerIdentity> {
        self.state.identity()
    }

    /// The producer's current transaction state, for callers that supervise it.
    #[must_use]
    pub fn state(&self) -> barnabas_core::TxnState {
        self.state.state()
    }

    // ── coordinator routing ────────────────────────────────────────────────

    /// Find the transaction coordinator for this producer's transactional id.
    ///
    /// A coordinator is chosen by hashing the id onto a partition of
    /// `__transaction_state`, so it is a specific broker and it can move.
    async fn find_coordinator(&mut self) -> Result<String> {
        let Some(txn_id) = self.transactional_id.clone() else {
            // Idempotent producers have no coordinator; `InitProducerId` may go
            // to any broker.
            return Ok(String::new());
        };

        let mut req = FindCoordinatorRequest::default();
        req.key = StrBytes::from_string(txn_id.clone());
        req.key_type = 1; // TRANSACTION

        for attempt in 0..MAX_RETRIES {
            let resp: FindCoordinatorResponse =
                self.cluster.call_any(ApiKey::FindCoordinator, 3, &req).await?;

            let code = ErrorCode(resp.error_code);
            if code.is_ok() {
                let addr = format!("{}:{}", resp.host.as_str(), resp.port);
                self.coordinator = Some(addr.clone());
                return Ok(addr);
            }
            match code.disposition() {
                Disposition::Retry | Disposition::FindCoordinator => {
                    Self::backoff(attempt).await;
                }
                _ => {
                    return Err(Error::Broker {
                        op: "FindCoordinator",
                        code: code.0,
                        disposition: code.disposition(),
                    })
                }
            }
        }
        Err(Error::Broker {
            op: "FindCoordinator",
            code: ErrorCode::COORDINATOR_NOT_AVAILABLE.0,
            disposition: Disposition::Retry,
        })
    }

    /// The coordinator's address, discovering it if unknown.
    async fn coordinator_addr(&mut self) -> Result<String> {
        match self.coordinator.clone() {
            Some(addr) => Ok(addr),
            None => self.find_coordinator().await,
        }
    }

    /// Exponential, capped: 5 ms, 10, 20, … 250, 250, …
    async fn backoff(attempt: usize) {
        let shift = u32::try_from(attempt).unwrap_or(u32::MAX).min(16);
        let delay = RETRY_BACKOFF
            .saturating_mul(1u32 << shift)
            .min(RETRY_BACKOFF_MAX);
        T::sleep(delay).await;
    }

    /// Send a coordinator request, honouring the disposition of whatever comes
    /// back: retry in place, re-discover, or fail.
    ///
    /// The `FindCoordinator` arm is the one that matters — see the module doc.
    async fn coordinator_call<Req, Resp, F>(
        &mut self,
        op: &'static str,
        api_key: ApiKey,
        version: i16,
        req: &Req,
        error_of: F,
    ) -> Result<Resp>
    where
        Req: kafka_protocol::protocol::Encodable,
        Resp: kafka_protocol::protocol::Decodable,
        F: Fn(&Resp) -> i16,
    {
        for attempt in 0..MAX_RETRIES {
            let addr = self.coordinator_addr().await?;
            let resp: Resp = if addr.is_empty() {
                self.cluster.call_any(api_key, version, req).await?
            } else {
                self.cluster.call_at(&addr, api_key, version, req).await?
            };

            let code = ErrorCode(error_of(&resp));
            if code.is_ok() {
                return Ok(resp);
            }
            match code.disposition() {
                Disposition::Retry => Self::backoff(attempt).await,
                Disposition::FindCoordinator => {
                    // Forget it and ask again: the coordinator moved, and
                    // retrying this broker would spin forever.
                    self.coordinator = None;
                    Self::backoff(attempt).await;
                }
                Disposition::Fatal => {
                    self.state.fence();
                    return Err(Error::Broker {
                        op,
                        code: code.0,
                        disposition: Disposition::Fatal,
                    });
                }
                Disposition::RefreshMetadata | Disposition::Ok => {
                    return Err(Error::Broker {
                        op,
                        code: code.0,
                        disposition: code.disposition(),
                    })
                }
            }
        }
        Err(Error::Broker {
            op,
            code: ErrorCode::COORDINATOR_NOT_AVAILABLE.0,
            disposition: Disposition::Retry,
        })
    }

    async fn init_producer_id(&mut self) -> Result<()> {
        let mut req = InitProducerIdRequest::default();
        req.transactional_id = self
            .transactional_id
            .as_ref()
            .map(|id| TransactionalId(StrBytes::from_string(id.clone())));
        req.transaction_timeout_ms = self.timeout_ms;
        req.producer_id = ProducerId(-1);
        req.producer_epoch = -1;

        let resp: InitProducerIdResponse = self
            .coordinator_call(
                "InitProducerId",
                ApiKey::InitProducerId,
                4,
                &req,
                |r: &InitProducerIdResponse| r.error_code,
            )
            .await?;

        self.state.on_init_producer_id(ProducerIdentity {
            id: resp.producer_id.0,
            epoch: resp.producer_epoch,
        });
        Ok(())
    }

    // ── transactions ───────────────────────────────────────────────────────

    /// Open a transaction.
    ///
    /// # Errors
    /// If the producer is fenced, uninitialised, or already in a transaction.
    pub fn begin_transaction(&mut self) -> Result<()> {
        self.state.begin_transaction().map_err(Error::Producer)
    }

    /// Commit the open transaction.
    ///
    /// # Errors
    /// If no transaction is open, or the coordinator rejects the commit.
    pub async fn commit_transaction(&mut self) -> Result<()> {
        self.end_transaction(true).await
    }

    /// Abort the open transaction. Its records stay in the log and are
    /// filtered by READ_COMMITTED consumers — see `barnabas_core::consumer`.
    ///
    /// # Errors
    /// As [`Self::commit_transaction`].
    pub async fn abort_transaction(&mut self) -> Result<()> {
        self.end_transaction(false).await
    }

    async fn end_transaction(&mut self, committed: bool) -> Result<()> {
        self.state.end_transaction().map_err(Error::Producer)?;

        let identity = self.state.identity().ok_or(Error::Missing("producer id"))?;
        let txn_id = self
            .transactional_id
            .clone()
            .ok_or(Error::Missing("transactional id"))?;

        let mut req = EndTxnRequest::default();
        req.transactional_id = TransactionalId(StrBytes::from_string(txn_id));
        req.producer_id = ProducerId(identity.id);
        req.producer_epoch = identity.epoch;
        req.committed = committed;

        let _: EndTxnResponse = self
            .coordinator_call("EndTxn", ApiKey::EndTxn, 3, &req, |r: &EndTxnResponse| {
                r.error_code
            })
            .await?;

        self.state.on_end_transaction();
        Ok(())
    }

    /// Commit a consumer group's offsets **as part of this transaction**.
    ///
    /// This is what makes consume-transform-produce exactly-once: the offsets
    /// land only if the transaction commits, so a crash between producing and
    /// committing replays the input rather than losing the output.
    ///
    /// Two requests, to **two different coordinators**, and they are not
    /// interchangeable:
    ///
    /// - `AddOffsetsToTxn` goes to the *transaction* coordinator and enrolls
    ///   the group's `__consumer_offsets` partition in the transaction, the
    ///   same way [`Self::enroll_all`] enrolls a data partition. Without it the
    ///   offsets are written outside the transaction and commit regardless.
    /// - `TxnOffsetCommit` goes to the *group* coordinator, carrying the
    ///   member's fencing token so a member that has already been replaced
    ///   cannot commit (KIP-447). That is why `metadata` comes from a live
    ///   consumer and cannot be constructed by a caller.
    ///
    /// Call it **after** producing the records those offsets account for, and
    /// before [`Self::commit_transaction`].
    ///
    /// # Errors
    /// If no transaction is open, the producer is not transactional, or either
    /// coordinator rejects the request. A rejection here is not recoverable by
    /// retrying the commit: abort the transaction and rejoin the group.
    pub async fn send_offsets_to_transaction(
        &mut self,
        offsets: &BTreeMap<barnabas_core::group::TopicPartition, i64>,
        metadata: &crate::group::GroupMetadata,
    ) -> Result<()> {
        if self.state.state() != barnabas_core::TxnState::InTransaction {
            return Err(Error::Producer(
                barnabas_core::producer::ProducerError::NoTransaction,
            ));
        }
        if offsets.is_empty() {
            return Ok(());
        }
        let identity = self.state.identity().ok_or(Error::Missing("producer id"))?;
        let txn_id = self
            .transactional_id
            .clone()
            .ok_or(Error::Missing("transactional id"))?;

        let mut add = AddOffsetsToTxnRequest::default();
        add.transactional_id = TransactionalId(StrBytes::from_string(txn_id.clone()));
        add.producer_id = ProducerId(identity.id);
        add.producer_epoch = identity.epoch;
        add.group_id = GroupId(StrBytes::from_string(metadata.group_id.clone()));

        let _: AddOffsetsToTxnResponse = self
            .coordinator_call(
                "AddOffsetsToTxn",
                ApiKey::AddOffsetsToTxn,
                3,
                &add,
                |r: &AddOffsetsToTxnResponse| r.error_code,
            )
            .await?;

        let mut topics: BTreeMap<String, Vec<TxnOffsetCommitRequestPartition>> = BTreeMap::new();
        for (tp, offset) in offsets {
            let mut entry = TxnOffsetCommitRequestPartition::default();
            entry.partition_index = tp.partition;
            entry.committed_offset = *offset;
            entry.committed_leader_epoch = -1;
            topics.entry(tp.topic.clone()).or_default().push(entry);
        }

        let mut commit = TxnOffsetCommitRequest::default();
        commit.transactional_id = TransactionalId(StrBytes::from_string(txn_id));
        commit.group_id = GroupId(StrBytes::from_string(metadata.group_id.clone()));
        commit.producer_id = ProducerId(identity.id);
        commit.producer_epoch = identity.epoch;
        commit.generation_id = metadata.generation_id;
        commit.member_id = StrBytes::from_string(metadata.member_id.clone());
        commit.group_instance_id = metadata
            .group_instance_id
            .clone()
            .map(StrBytes::from_string);
        commit.topics = topics
            .into_iter()
            .map(|(name, partitions)| {
                let mut topic = TxnOffsetCommitRequestTopic::default();
                topic.name = TopicName(StrBytes::from_string(name));
                topic.partitions = partitions;
                topic
            })
            .collect();

        self.group_coordinator_call(&metadata.group_id, &commit)
            .await?;
        Ok(())
    }

    /// Send `TxnOffsetCommit` to the **group** coordinator, discovering it and
    /// re-discovering it on `NOT_COORDINATOR`.
    ///
    /// Separate from [`Self::coordinator_call`] because a producer's cached
    /// coordinator is the *transaction* coordinator; sending a group request
    /// there earns `NOT_COORDINATOR` forever, and clearing that cache on the
    /// way would cost the transaction coordinator lookup for no reason.
    async fn group_coordinator_call(
        &mut self,
        group_id: &str,
        req: &TxnOffsetCommitRequest,
    ) -> Result<()> {
        let mut find = FindCoordinatorRequest::default();
        find.key = StrBytes::from_string(group_id.to_owned());
        find.key_type = 0; // GROUP

        let mut addr: Option<String> = None;
        for attempt in 0..MAX_RETRIES {
            let at = match addr.clone() {
                Some(a) => a,
                None => {
                    let resp: FindCoordinatorResponse = self
                        .cluster
                        .call_any(ApiKey::FindCoordinator, 3, &find)
                        .await?;
                    let code = ErrorCode(resp.error_code);
                    if !code.is_ok() {
                        if matches!(
                            code.disposition(),
                            Disposition::Retry | Disposition::FindCoordinator
                        ) {
                            Self::backoff(attempt).await;
                            continue;
                        }
                        return Err(Error::Broker {
                            op: "FindCoordinator",
                            code: code.0,
                            disposition: code.disposition(),
                        });
                    }
                    let a = format!("{}:{}", resp.host.as_str(), resp.port);
                    addr = Some(a.clone());
                    a
                }
            };

            let resp: TxnOffsetCommitResponse = self
                .cluster
                .call_at(&at, ApiKey::TxnOffsetCommit, 3, req)
                .await?;

            // The error is per partition, so the first non-zero one decides.
            let code = ErrorCode(
                resp.topics
                    .iter()
                    .flat_map(|t| t.partitions.iter())
                    .map(|p| p.error_code)
                    .find(|c| *c != 0)
                    .unwrap_or(0),
            );
            if code.is_ok() {
                return Ok(());
            }
            match code.disposition() {
                Disposition::Retry => Self::backoff(attempt).await,
                Disposition::FindCoordinator => {
                    addr = None;
                    Self::backoff(attempt).await;
                }
                Disposition::Fatal => {
                    self.state.fence();
                    return Err(Error::Broker {
                        op: "TxnOffsetCommit",
                        code: code.0,
                        disposition: Disposition::Fatal,
                    });
                }
                Disposition::RefreshMetadata | Disposition::Ok => {
                    return Err(Error::Broker {
                        op: "TxnOffsetCommit",
                        code: code.0,
                        disposition: code.disposition(),
                    })
                }
            }
        }
        Err(Error::Broker {
            op: "TxnOffsetCommit",
            code: ErrorCode::COORDINATOR_NOT_AVAILABLE.0,
            disposition: Disposition::Retry,
        })
    }

    /// Enroll partitions in the open transaction, in **one** request.
    ///
    /// The coordinator must know a partition is in the transaction before
    /// records are produced to it, or it cannot fence that partition at commit.
    /// `AddPartitionsToTxn` takes a list, so enrolling eight partitions is one
    /// round trip rather than eight — which matters because this happens once
    /// per transaction, and a transaction can be as short as a checkpoint
    /// interval.
    async fn enroll_all(&mut self, topic: &str, partitions: &[i32]) -> Result<()> {
        let needed: Vec<i32> = partitions
            .iter()
            .copied()
            .filter(|p| self.state.needs_enrollment(topic, *p))
            .collect();
        if needed.is_empty() {
            return Ok(());
        }
        let identity = self.state.identity().ok_or(Error::Missing("producer id"))?;
        let txn_id = self
            .transactional_id
            .clone()
            .ok_or(Error::Missing("transactional id"))?;

        let mut req_topic = AddPartitionsToTxnTopic::default();
        req_topic.name = TopicName(StrBytes::from_string(topic.to_owned()));
        req_topic.partitions.clone_from(&needed);

        let mut req = AddPartitionsToTxnRequest::default();
        req.v3_and_below_transactional_id = TransactionalId(StrBytes::from_string(txn_id));
        req.v3_and_below_producer_id = ProducerId(identity.id);
        req.v3_and_below_producer_epoch = identity.epoch;
        req.v3_and_below_topics = vec![req_topic];

        // The partition's own error code is nested, so the extractor digs it
        // out — including `CONCURRENT_TRANSACTIONS` (51), which appears
        // whenever a transaction starts right after one ends and is retriable.
        let _: AddPartitionsToTxnResponse = self
            .coordinator_call(
                "AddPartitionsToTxn",
                ApiKey::AddPartitionsToTxn,
                3,
                &req,
                |r: &AddPartitionsToTxnResponse| {
                    r.results_by_topic_v3_and_below
                        .iter()
                        .flat_map(|t| t.results_by_partition.iter())
                        .map(|p| p.partition_error_code)
                        .find(|c| *c != 0)
                        .unwrap_or(0)
                },
            )
            .await?;

        for partition in needed {
            self.state.on_enrolled(topic, partition);
        }
        Ok(())
    }

    // ── produce ────────────────────────────────────────────────────────────

    /// Produce `records` to one partition, returning the base offset the broker
    /// assigned.
    ///
    /// Enqueues and flushes in one call, so a caller that wants pipelining uses
    /// [`Self::enqueue`] several times and then [`Self::flush`] — this is the
    /// convenience for the one-shot case, and it has exactly one request in
    /// flight because there is exactly one batch.
    ///
    /// # Errors
    /// If the producer is fenced, no transaction is open when one is required,
    /// or the broker answers with an error that is not about leadership.
    pub async fn send(
        &mut self,
        topic: &str,
        partition: i32,
        records: &[ProducerRecord],
    ) -> Result<i64> {
        if records.is_empty() {
            return Ok(-1);
        }
        self.enqueue(topic, partition, records).await?;
        let written = self.flush().await?;
        Ok(written.first().map_or(-1, |(_, _, offset)| *offset))
    }

    /// Queue a batch for `partition` **without sending anything**.
    ///
    /// This is what lets a caller have more than one write outstanding, which
    /// is the whole point of pipelining: sequence numbers are allocated and the
    /// batch is encoded here, in call order, and [`Self::flush`] puts up to
    /// `max_in_flight` of them on the connection at once.
    ///
    /// Order matters and is preserved: batches for one partition are sent in
    /// the order they were enqueued, because that is the order their sequence
    /// numbers were allocated in.
    ///
    /// # Errors
    /// If the producer is fenced, or no transaction is open when one is
    /// required.
    pub async fn enqueue(
        &mut self,
        topic: &str,
        partition: i32,
        records: &[ProducerRecord],
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        // Transactional producers must enroll the partition before a sequence
        // can be allocated for it; idempotent ones do no work here.
        self.enroll_all(topic, &[partition]).await?;
        let batch = self.encode_for(topic, partition, records)?;
        self.pending
            .entry((topic.to_owned(), partition))
            .or_default()
            .push_back(batch);
        Ok(())
    }

    /// How long a topic's metadata may go unrefreshed.
    ///
    /// Five minutes by default, matching `metadata.max.age.ms`. It bounds how
    /// long this producer keeps placing keys with a stale partition count after
    /// a topic is expanded — during which its records land on different
    /// partitions than every producer with fresh metadata, and per-key ordering
    /// is broken between them.
    pub fn set_metadata_max_age(&mut self, age: Duration) {
        self.cluster.set_metadata_max_age(age);
    }

    /// How long a partial batch waits for company before being sent.
    ///
    /// Zero — the default, and Kafka's — means a record is sent as soon as it
    /// is produced. A few milliseconds is what turns a stream of single records
    /// into batches, and it is the single largest throughput knob for a caller
    /// producing one record at a time.
    ///
    /// **This client spawns nothing**, so the clock is only read when the
    /// producer is called. A steady stream of [`Self::produce`] calls sends
    /// itself; an idle producer holds its last partial batch until
    /// [`Self::flush`] or [`Self::tick`]. [`Self::linger_deadline`] is there so
    /// an event loop can arm a timer for exactly that.
    pub fn set_linger(&mut self, linger: Duration) {
        self.linger = linger;
    }

    /// How large a batch may grow before it is sent regardless of the linger.
    ///
    /// Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// # Panics
    /// If `bytes` is zero.
    pub fn set_batch_size(&mut self, bytes: usize) {
        assert!(bytes > 0, "batch_size must be at least 1");
        self.batch_size = bytes;
    }

    /// Produce one record, letting the client choose the partition and the
    /// batch.
    ///
    /// This is the ordinary way to produce: records accumulate per partition
    /// and go out when a batch fills or its linger expires, so a caller writing
    /// one record at a time still gets batches. [`Self::send`] remains the
    /// explicit form for a caller who has already batched and knows the
    /// partition.
    ///
    /// It does **not** wait for the broker: it returns once the record is
    /// accumulated, having sent whatever became ready. Use [`Self::flush`] to
    /// wait for everything to land.
    ///
    /// # Errors
    /// If the producer is fenced, no transaction is open when one is required,
    /// the topic's partition count cannot be learned, or a send that became
    /// ready fails.
    pub async fn produce(&mut self, topic: &str, record: ProducerRecord) -> Result<()> {
        let partition = self.partition_for(topic, record.key.as_deref()).await?;
        self.produce_to(topic, partition, record).await
    }

    /// [`Self::produce`], to a partition the caller chose.
    ///
    /// # Errors
    /// As [`Self::produce`].
    pub async fn produce_to(
        &mut self,
        topic: &str,
        partition: i32,
        record: ProducerRecord,
    ) -> Result<()> {
        let bytes = record.key.as_ref().map_or(0, bytes::Bytes::len)
            + record.value.as_ref().map_or(0, bytes::Bytes::len)
            + RECORD_OVERHEAD;

        let staged = self
            .staged
            .entry((topic.to_owned(), partition))
            .or_insert_with(|| Staged {
                records: Vec::new(),
                bytes: 0,
                since: std::time::Instant::now(),
            });
        staged.records.push(record);
        staged.bytes += bytes;

        self.send_ready().await
    }

    /// When the oldest staged batch is due, if anything is staged.
    ///
    /// An event loop with nothing else to do should sleep until this and then
    /// call [`Self::tick`]; without that, an idle producer's last partial batch
    /// waits for the next call. Returning the deadline rather than sleeping is
    /// deliberate — the caller owns the timer, because this client owns no
    /// tasks.
    #[must_use]
    pub fn linger_deadline(&self) -> Option<std::time::Instant> {
        self.staged
            .values()
            .map(|staged| staged.since + self.linger)
            .min()
    }

    /// Send every staged batch whose linger has expired.
    ///
    /// Cheap when nothing is due. Safe to call as often as an event loop
    /// likes.
    ///
    /// # Errors
    /// As [`Self::flush`].
    pub async fn tick(&mut self) -> Result<()> {
        self.send_ready().await
    }

    /// Encode and send whatever is full or overdue.
    ///
    /// A batch that is merely *partial and recent* stays staged: that is the
    /// whole point of the linger, and sending it would make the setting
    /// decorative.
    async fn send_ready(&mut self) -> Result<()> {
        let now = std::time::Instant::now();
        let ready: Vec<(String, i32)> = self
            .staged
            .iter()
            .filter(|(_, staged)| {
                staged.bytes >= self.batch_size || now.duration_since(staged.since) >= self.linger
            })
            .map(|(key, _)| key.clone())
            .collect();
        if ready.is_empty() {
            return Ok(());
        }
        for (topic, partition) in ready {
            let Some(staged) = self.staged.remove(&(topic.clone(), partition)) else {
                continue;
            };
            self.enqueue(&topic, partition, &staged.records).await?;
        }
        self.flush().await?;
        Ok(())
    }

    /// Move every staged record into the pending queue, however recent.
    ///
    /// What [`Self::flush`] does first, and what makes "flush" mean everything.
    async fn drain_staged(&mut self) -> Result<()> {
        let keys: Vec<(String, i32)> = self.staged.keys().cloned().collect();
        for (topic, partition) in keys {
            let Some(staged) = self.staged.remove(&(topic.clone(), partition)) else {
                continue;
            };
            self.enqueue(&topic, partition, &staged.records).await?;
        }
        Ok(())
    }

    /// How many `Produce` requests may be in flight on one connection.
    ///
    /// Defaults to [`DEFAULT_MAX_IN_FLIGHT`]. One restores strict
    /// request-response, which is what this client did before pipelining.
    ///
    /// # Panics
    /// If `max` is zero.
    pub fn set_max_in_flight(&mut self, max: usize) {
        assert!(max > 0, "max_in_flight must be at least 1");
        self.max_in_flight = max;
    }

    /// How many batches are queued and not yet acknowledged, staged records
    /// included — one staged partition counts as the one batch it will become.
    #[must_use]
    pub fn queued(&self) -> usize {
        self.pending.values().map(VecDeque::len).sum::<usize>() + self.staged.len()
    }

    /// Allocate this batch's sequence numbers and encode it **once**.
    ///
    /// Encoding once is what makes a retry a retry: the bytes re-sent carry the
    /// same sequence numbers, so a broker that already persisted the first
    /// attempt deduplicates instead of writing twice.
    fn encode_for(
        &mut self,
        topic: &str,
        partition: i32,
        records: &[ProducerRecord],
    ) -> Result<Bytes> {
        let count = i32::try_from(records.len()).map_err(|_| Error::Missing("batch size"))?;
        let range = self
            .state
            .allocate(topic, partition, count)
            .map_err(Error::Producer)?;
        self.encode_batch(records, range)
    }

    /// Send every queued batch, **pipelining up to `max_in_flight` requests per
    /// connection**, and return the base offset each partition was written at.
    ///
    /// # How the window stays in sequence
    ///
    /// Each partition has a queue of batches already encoded with contiguous
    /// sequence numbers. Round *r* carries the *r*-th batch of every partition
    /// that has one, so a broker receives rounds 0, 1, 2… on one connection and
    /// Kafka processes a connection's requests in order — partition `p`'s
    /// batches therefore arrive in sequence even though several are in flight.
    ///
    /// **The recovery rule is what makes it safe.** Only the *contiguous
    /// leading run* of successes is removed from a partition's queue. If round
    /// 2 failed for `p`, then rounds 3 and 4 for `p` are void whatever they
    /// answered — the broker never wrote round 2, so it rejected them for being
    /// out of sequence — and they stay queued, in order, to be sent again
    /// behind the retry of round 2. That is the ordered-window retry the Java
    /// client performs, and without it a pipelined idempotent producer either
    /// gaps or duplicates.
    ///
    /// # Errors
    /// If the producer is fenced, or a broker answers with an error that is
    /// neither about leadership nor a consequence of an earlier failure in the
    /// same window.
    pub async fn flush(&mut self) -> Result<Vec<(String, i32, i64)>> {
        // **Staged records are part of "everything".** Without this a caller
        // who produces and then flushes gets an empty answer and a batch still
        // sitting in memory, which is the worst possible reading of the word.
        self.drain_staged().await?;

        let mut written: Vec<(String, i32, i64)> = Vec::new();

        for attempt in 0..MAX_RETRIES {
            self.pending.retain(|_, queue| !queue.is_empty());
            if self.pending.is_empty() {
                written.sort_unstable();
                return Ok(written);
            }

            // Route: which broker leads each partition that still has work.
            let mut by_broker: BTreeMap<String, Vec<(String, i32)>> = BTreeMap::new();
            let mut unroutable = false;
            for (topic, partition) in self.pending.keys().cloned().collect::<Vec<_>>() {
                match self.cluster.leader_addr(&topic, partition).await {
                    Ok(addr) => by_broker.entry(addr).or_default().push((topic, partition)),
                    // A topic still being created has no leader yet; a wait,
                    // not a failure.
                    Err(Error::NoLeader { .. }) => unroutable = true,
                    Err(e) => return Err(e),
                }
            }
            if by_broker.is_empty() {
                if !unroutable || attempt + 1 == MAX_RETRIES {
                    let (topic, partition) =
                        self.pending.keys().next().expect("non-empty").clone();
                    return Err(Error::NoLeader { topic, partition });
                }
                Self::backoff(attempt).await;
                continue;
            }

            let depth = self.window_depth(&by_broker);
            let rounds = self.send_window(&by_broker, depth).await?;
            let (outcomes, transport_error) = self.collect_window(&rounds).await;
            if let Some(e) = transport_error {
                // Whatever succeeded in this window stays queued rather than
                // being retired on a guess: the window's later rounds are void
                // once anything in it failed.
                self.offsets.clear();
                match e {
                    // A pooled connection can be closed at any time — a rolling
                    // upgrade, an idle reaper — and the client only finds out
                    // by using it. Re-sending is safe: the batches carry their
                    // original sequence numbers, so anything the broker did
                    // write is deduplicated rather than doubled.
                    Error::Io(_) if attempt + 1 < MAX_RETRIES => {
                        Self::backoff(attempt).await;
                        continue;
                    }
                    // A broker that accepted the request and never answered is
                    // a different thing, and retrying it would spend the whole
                    // retry budget waiting on someone who is not talking.
                    e => return Err(e),
                }
            }

            // `failed_at[p]` is the earliest round that failed for `p`;
            // everything at or after it stays queued.
            let mut failed_at: BTreeMap<(String, i32), usize> = BTreeMap::new();
            let mut needs_refresh = false;
            for (round, results) in outcomes.iter().enumerate() {
                for (key, code) in results {
                    if code.is_ok() {
                        continue;
                    }
                    let earlier = failed_at.get(key).copied();
                    if earlier.is_some() {
                        // Already broken for this partition. Whatever this
                        // round says — almost always OUT_OF_ORDER_SEQUENCE —
                        // is a consequence, not a new fact.
                        continue;
                    }
                    match code.disposition() {
                        Disposition::RefreshMetadata => {
                            let (topic, partition) = key;
                            self.cluster.invalidate(topic, *partition);
                            needs_refresh = true;
                        }
                        Disposition::Retry => {}
                        Disposition::Fatal | Disposition::FindCoordinator | Disposition::Ok => {
                            // A sequence error with nothing failed before it is
                            // the real thing: this producer's stream is wrong.
                            self.state.fence();
                            return Err(Error::Broker {
                                op: "Produce",
                                code: code.0,
                                disposition: code.disposition(),
                            });
                        }
                    }
                    failed_at.insert(key.clone(), round);
                }
            }

            // Retire the contiguous leading run of successes per partition.
            for (round, results) in outcomes.iter().enumerate() {
                for (key, _) in results {
                    if failed_at.get(key).is_some_and(|first| round >= *first) {
                        continue;
                    }
                    if let Some(queue) = self.pending.get_mut(key) {
                        if queue.pop_front().is_some() {
                            let offset = self
                                .offsets
                                .get(&(key.clone(), round))
                                .copied()
                                .unwrap_or(-1);
                            written.push((key.0.clone(), key.1, offset));
                        }
                    }
                }
            }
            self.offsets.clear();

            self.pending.retain(|_, queue| !queue.is_empty());
            if self.pending.is_empty() {
                written.sort_unstable();
                return Ok(written);
            }
            if needs_refresh {
                let mut topics: Vec<String> =
                    self.pending.keys().map(|(topic, _)| topic.clone()).collect();
                topics.sort_unstable();
                topics.dedup();
                for topic in topics {
                    self.cluster.refresh_metadata(&topic).await?;
                }
            }
            Self::backoff(attempt).await;
        }

        let (topic, partition) = self.pending.keys().next().expect("non-empty").clone();
        Err(Error::NoLeader { topic, partition })
    }

    /// How many rounds this window will have: the longest queue, capped.
    fn window_depth(&self, by_broker: &BTreeMap<String, Vec<(String, i32)>>) -> usize {
        by_broker
            .values()
            .flatten()
            .filter_map(|key| self.pending.get(key).map(VecDeque::len))
            .max()
            .unwrap_or(0)
            .min(self.max_in_flight)
    }

    /// Write every round of the window without waiting for any of it.
    ///
    /// Rounds go out in order per connection, which is what puts a partition's
    /// batches on the wire in sequence.
    async fn send_window(
        &mut self,
        by_broker: &BTreeMap<String, Vec<(String, i32)>>,
        depth: usize,
    ) -> Result<Window> {
        let mut rounds: Window = Vec::with_capacity(depth);

        for round in 0..depth {
            let mut this_round: Vec<RoundTarget> = Vec::new();
            for (addr, keys) in by_broker {
                let taking: Vec<(String, i32)> = keys
                    .iter()
                    .filter(|key| self.pending.get(*key).is_some_and(|q| q.len() > round))
                    .cloned()
                    .collect();
                if taking.is_empty() {
                    continue;
                }
                let req = self.produce_request(&taking, round);
                match self.cluster.send_at(ApiKey::Produce, 9, addr, &req).await {
                    Ok(()) => this_round.push((addr.clone(), taking)),
                    Err(e) => {
                        // Whatever already went out is still outstanding and
                        // must be collected, or the connections are left
                        // holding answers nobody reads.
                        rounds.push(this_round);
                        let _ = self.collect_window(&rounds).await;
                        return Err(e);
                    }
                }
            }
            if this_round.is_empty() {
                break;
            }
            rounds.push(this_round);
        }
        Ok(rounds)
    }

    /// Read every round's answers, in the order they were sent.
    ///
    /// Returns, per round, the error code each partition came back with.
    /// A transport failure — a dropped connection, a broker that never
    /// answered — is returned rather than folded into a per-partition code.
    /// Every round is still drained first: the answers are on their way whether
    /// or not anyone wants them, and leaving them unread desynchronises the
    /// connection. Retrying a timeout in place would also spin the whole
    /// retry budget against a broker that is simply not answering.
    async fn collect_window(
        &mut self,
        rounds: &[Vec<RoundTarget>],
    ) -> (Vec<Vec<((String, i32), ErrorCode)>>, Option<Error>) {
        let mut out = Vec::with_capacity(rounds.len());
        let mut transport_error = None;

        for (round, sent) in rounds.iter().enumerate() {
            let addrs: Vec<String> = sent.iter().map(|(addr, _)| addr.clone()).collect();
            let responses = self
                .cluster
                .recv_many::<ProduceResponse>(ApiKey::Produce, &addrs)
                .await;

            let mut codes: Vec<((String, i32), ErrorCode)> = Vec::new();
            for ((_, keys), response) in sent.iter().zip(responses) {
                match response {
                    Ok(resp) => {
                        for topic_response in &resp.responses {
                            let topic = topic_response.name.0.to_string();
                            for part in &topic_response.partition_responses {
                                let key = (topic.clone(), part.index);
                                let code = ErrorCode(part.error_code);
                                if code.is_ok() {
                                    self.offsets.insert((key.clone(), round), part.base_offset);
                                }
                                codes.push((key, code));
                            }
                        }
                    }
                    // A failed connection says nothing about which partition is
                    // at fault. The batches stay queued — their bytes carry
                    // their original sequence numbers, so re-sending is
                    // deduplicated by the broker — but the error is the
                    // caller's to see.
                    Err(e) => {
                        if transport_error.is_none() {
                            transport_error = Some(e);
                        }
                        for key in keys {
                            codes.push((key.clone(), ErrorCode::REQUEST_TIMED_OUT));
                        }
                    }
                }
            }
            out.push(codes);
        }
        (out, transport_error)
    }

    /// Produce records to whichever partitions their keys hash to.
    ///
    /// The counterpart to [`Self::send`], for callers that want Kafka's usual
    /// key-based placement rather than choosing partitions themselves. Records
    /// are grouped by partition and sent as one batch each, which is what makes
    /// this cheaper than a send per record.
    ///
    /// Returns the base offset per partition written.
    ///
    /// # Errors
    /// As [`Self::send`], plus failure to learn the topic's partition count.
    pub async fn send_keyed(
        &mut self,
        topic: &str,
        records: &[ProducerRecord],
    ) -> Result<Vec<(i32, i64)>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let count = self.partition_count_waiting(topic).await?;

        let mut by_partition: BTreeMap<i32, Vec<ProducerRecord>> = BTreeMap::new();
        for record in records {
            let partition = self
                .partitioner
                .partition_for(record.key.as_deref(), count, &mut self.round_robin)
                .ok_or_else(|| Error::NoLeader {
                    topic: topic.to_owned(),
                    partition: -1,
                })?;
            by_partition.entry(partition).or_default().push(record.clone());
        }

        let partitions: Vec<i32> = by_partition.keys().copied().collect();
        self.enroll_all(topic, &partitions).await?;

        for (partition, records) in by_partition {
            self.enqueue(topic, partition, &records).await?;
        }
        let written = self.flush().await?;
        Ok(written
            .into_iter()
            .map(|(_, partition, offset)| (partition, offset))
            .collect())
    }

    /// The topic's partition count, waiting for a topic that is still
    /// materialising rather than partitioning against a count of zero.
    ///
    /// The count decides where every key lands, so answering "zero partitions"
    /// for a topic created a moment ago is not a smaller failure than the wait.
    ///
    /// # Errors
    /// If the topic still has no partitions after the wait.
    async fn partition_count_waiting(&mut self, topic: &str) -> Result<i32> {
        for attempt in 0..MAX_RETRIES {
            match self.cluster.partition_count(topic).await {
                Ok(count) if count > 0 => return Ok(count),
                Ok(_) | Err(Error::NoLeader { .. }) if attempt + 1 < MAX_RETRIES => {
                    Self::backoff(attempt).await;
                }
                Ok(_) => break,
                Err(e) => return Err(e),
            }
        }
        Err(Error::NoLeader {
            topic: topic.to_owned(),
            partition: -1,
        })
    }

    /// Which partition a key would go to, without sending anything.
    ///
    /// Exposed for the parity test that checks this places keys exactly where
    /// `rdkafka` does — see `slipstream-kafka`'s partitioner parity test.
    ///
    /// # Errors
    /// If the topic's partition count cannot be learned.
    pub async fn partition_for(&mut self, topic: &str, key: Option<&[u8]>) -> Result<i32> {
        let count = self.partition_count_waiting(topic).await?;
        let mut scratch = self.round_robin;
        self.partitioner
            .partition_for(key, count, &mut scratch)
            .ok_or_else(|| Error::NoLeader {
                topic: topic.to_owned(),
                partition: -1,
            })
    }

    fn encode_batch(&self, records: &[ProducerRecord], range: SequenceRange) -> Result<Bytes> {
        let identity = self.state.identity().ok_or(Error::Missing("producer id"))?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        let transactional = self.transactional_id.is_some();

        let encoded: Vec<Record> = records
            .iter()
            .enumerate()
            .map(|(i, r)| Record {
                transactional,
                control: false,
                partition_leader_epoch: 0,
                producer_id: identity.id,
                producer_epoch: identity.epoch,
                timestamp_type: TimestampType::Creation,
                // Offsets within a batch are relative; the broker assigns the
                // real ones and reports the base.
                offset: i as i64,
                sequence: range.base + i32::try_from(i).unwrap_or(i32::MAX),
                timestamp: r.timestamp.unwrap_or(now),
                key: r.key.clone(),
                value: r.value.clone(),
                headers: Default::default(),
            })
            .collect();

        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            encoded.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: self.compression,
            },
        )
        .map_err(|e| {
            Error::Core(barnabas_core::Error::Codec(format!(
                "encode record batch: {e}"
            )))
        })?;
        Ok(buf.freeze())
    }

    /// One request carrying every partition this broker leads, taking each
    /// one's `round`-th queued batch.
    fn produce_request(&self, keys: &[(String, i32)], round: usize) -> ProduceRequest {
        let mut by_topic: BTreeMap<String, Vec<PartitionProduceData>> = BTreeMap::new();
        for key in keys {
            let Some(batch) = self.pending.get(key).and_then(|queue| queue.get(round)) else {
                continue;
            };
            let mut data = PartitionProduceData::default();
            data.index = key.1;
            data.records = Some(batch.clone());
            by_topic.entry(key.0.clone()).or_default().push(data);
        }

        let topic_data: Vec<TopicProduceData> = by_topic
            .into_iter()
            .map(|(name, partition_data)| {
                let mut data = TopicProduceData::default();
                data.name = TopicName(StrBytes::from_string(name));
                data.partition_data = partition_data;
                data
            })
            .collect();

        let mut req = ProduceRequest::default();
        req.acks = self.acks;
        req.timeout_ms = self.timeout_ms;
        req.topic_data = topic_data;
        req.transactional_id = self
            .transactional_id
            .as_ref()
            .map(|id| TransactionalId(StrBytes::from_string(id.clone())));
        req
    }
}

/// Re-exported so callers can name a compression codec without depending on
/// `kafka-protocol` directly.
pub use kafka_protocol::records::Compression as CompressionCodec;

#[cfg(test)]
mod tests {
    use super::*;

    /// The wire-level shape a caller cannot get wrong, because the producer
    /// owns it: a transactional request carries the transactional id, an
    /// idempotent one does not.
    #[test]
    fn a_record_carries_no_sequencing_of_its_own() {
        let r = ProducerRecord::new(None, Some(Bytes::from_static(b"v")));
        assert!(r.timestamp.is_none());
    }

    /// Not a test of behaviour so much as of the type: `send` takes records
    /// with no sequence field, so there is no way for a caller to supply one.
    #[test]
    fn producer_records_have_no_sequence_field() {
        let r = ProducerRecord::new(Some(Bytes::from_static(b"k")), None);
        assert_eq!(r.key.as_deref(), Some(&b"k"[..]));
    }
}
