//! The assign-only consumer.
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

use std::collections::BTreeMap;
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
pub struct FetchedRecords {
    pub topic: String,
    pub partition: i32,
    pub records: Vec<Record>,
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
    /// [`Self::fetch`].
    outstanding: Option<Outstanding>,
    /// Whether to keep a fetch permanently in flight. On by default.
    prefetch: bool,
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
    /// Connect with no assignments. Add them with [`Self::add`].
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
            isolation,
            max_wait: Duration::from_millis(500),
            max_bytes: 10 * 1024 * 1024,
            max_response_bytes: 64 * 1024 * 1024,
            sessions: BTreeMap::new(),
            incremental: true,
            outstanding: None,
            prefetch: true,
            generation: 0,
        })
    }

    /// Connect and assign one partition — the common case, and what the
    /// single-partition callers use.
    ///
    /// # Errors
    /// As [`Self::new`], plus a missing topic or partition.
    pub async fn assign(
        transport: T,
        bootstrap: &[String],
        client_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        isolation: IsolationLevel,
    ) -> Result<Self> {
        let mut me = Self::new(transport, bootstrap, client_id, isolation).await?;
        me.add(topic, partition, offset).await?;
        Ok(me)
    }

    /// Assign another partition, starting at `offset`.
    ///
    /// `offset` may be [`EARLIEST`], [`LATEST`], or an absolute offset; the
    /// first two are resolved with `ListOffsets` before the first fetch.
    ///
    /// # Errors
    /// If the topic does not exist, or the broker answers with an error code.
    pub async fn add(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
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

    /// Stop fetching a partition.
    ///
    /// Resets the fetch sessions: the broker's remembered partition set no
    /// longer matches ours, and correcting it with `forgotten_topics_data` is
    /// more machinery than a fresh full fetch costs.
    pub fn remove(&mut self, topic: &str, partition: i32) {
        self.positions.remove(&(topic.to_owned(), partition));
        self.generation += 1;
        for session in self.sessions.values_mut() {
            session.reset();
        }
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
    /// it a fetch is issued only when [`Self::fetch`] is called, so every poll
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
        // [`Self::add`].
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

    /// Send one `Fetch` per broker and record what was asked, without waiting.
    ///
    /// If a send fails partway, whatever was already sent is still recorded —
    /// those answers are outstanding whether or not the rest went out, and
    /// leaving them unrecorded would strand them on the connection.
    async fn issue_fetch(&mut self) -> Result<()> {
        let mut by_broker: BTreeMap<String, Vec<(String, i32)>> = BTreeMap::new();
        for (topic, partition) in self.positions.keys().cloned().collect::<Vec<_>>() {
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
    /// afterwards, so the next [`Self::fetch`] issues the request itself and
    /// reports whatever goes wrong then. Returning it from *this* call would
    /// fail a poll that had already succeeded.
    async fn start_prefetch(&mut self) {
        if self.prefetch && !self.positions.is_empty() {
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

    /// Fetch every assigned partition, **one request per broker**.
    ///
    /// An empty result is normal: a fetch that waits out `max_wait` with no new
    /// data is not an error. Positions advance past filtered records as well as
    /// returned ones, so an all-aborted fetch makes progress rather than
    /// looping.
    ///
    /// # Errors
    /// If the connection fails, leadership cannot be resolved, or a broker
    /// answers with a code that is not about leadership.
    pub async fn fetch(&mut self) -> Result<Vec<FetchedRecords>> {
        if self.positions.is_empty() {
            return Ok(Vec::new());
        }

        // An outstanding fetch asked about the assignment as it was; if that
        // changed, its answer is to a question nobody is asking any more.
        if self
            .outstanding
            .as_ref()
            .is_some_and(|o| o.generation != self.generation)
        {
            self.discard_outstanding().await;
        }

        for attempt in 0..=MAX_LEADER_RETRIES {
            if self.outstanding.is_none() {
                self.issue_fetch().await?;
            }
            let groups = self.outstanding.take().expect("just issued").groups;
            let addrs: Vec<String> = groups.iter().map(|(addr, _)| addr.clone()).collect();

            let mut out = Vec::new();
            let mut needs_refresh: Vec<(String, i32)> = Vec::new();

            let responses = self
                .cluster
                .recv_many::<FetchResponse>(ApiKey::Fetch, &addrs)
                .await;

            for ((addr, partitions), response) in groups.into_iter().zip(responses) {
                let resp = response?;

                // A session the broker has forgotten is not a failure: drop it
                // and the next fetch is a full one.
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
                    // The epoch advances only on a response the broker
                    // accepted; incrementing optimistically would desynchronise
                    // it from the broker's view.
                    session.epoch = session.epoch.wrapping_add(1).max(1);
                    for (topic, partition) in &partitions {
                        if let Some(offset) = self.positions.get(&(topic.clone(), *partition)) {
                            session
                                .known
                                .insert((topic.clone(), *partition), *offset);
                        }
                    }
                }

                for topic_response in &resp.responses {
                    let topic = topic_response.topic.0.to_string();
                    for part in &topic_response.partitions {
                        let code = ErrorCode(part.error_code);
                        if code.disposition() == Disposition::RefreshMetadata {
                            // The leader moved. Forget just this partition and
                            // ask again — the others in this response are fine.
                            self.cluster.invalidate(&topic, part.partition_index);
                            needs_refresh.push((topic.clone(), part.partition_index));
                            continue;
                        }
                        check("Fetch partition", part.error_code)?;

                        let key = (topic.clone(), part.partition_index);
                        let Some(fetch_offset) = self.positions.get(&key).copied() else {
                            continue;
                        };

                        let records = match &part.records {
                            Some(bytes) if !bytes.is_empty() => decode_records(bytes.clone())?,
                            _ => continue,
                        };
                        let aborted: Vec<AbortedTransaction> = part
                            .aborted_transactions
                            .as_ref()
                            .map(|list| {
                                list.iter()
                                    .map(|a| AbortedTransaction {
                                        producer_id: a.producer_id.0,
                                        first_offset: a.first_offset,
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();

                        // The filtering the broker does not do. See
                        // `kestrel_core::consumer`.
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
                            out.push(FetchedRecords {
                                topic: topic.clone(),
                                partition: part.partition_index,
                                records,
                            });
                        }
                    }
                }
            }

            if needs_refresh.is_empty() || attempt == MAX_LEADER_RETRIES {
                self.start_prefetch().await;
                return Ok(out);
            }
            // Something moved. Refresh the topics involved and go round again;
            // whatever was read this time is kept, because its offsets have
            // already advanced.
            if !out.is_empty() {
                return Ok(out);
            }
            let mut topics: Vec<String> = needs_refresh.into_iter().map(|(t, _)| t).collect();
            topics.sort_unstable();
            topics.dedup();
            for topic in topics {
                self.cluster.refresh_metadata(&topic).await?;
            }
            T::sleep(LEADER_BACKOFF).await;
        }
        unreachable!("the loop returns on its last attempt")
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

/// One partition's batches, as [`kestrel_core::records`] read them.
///
/// The counterpart to [`FetchedRecords`] for [`Consumer::fetch_lean`]: batches
/// rather than a flat list of records, because the facts a consumer filters on
/// — producer id, whether the batch is transactional, whether it is a control
/// batch — belong to the batch, and flattening them is what makes the ordinary
/// path expensive.
#[derive(Debug)]
pub struct LeanFetched {
    pub topic: String,
    pub partition: i32,
    pub batches: Vec<LeanBatch>,
    /// Records from a batch the lean reader handed back — compressed, or
    /// carrying headers — decoded the ordinary way. Kept separate rather than
    /// converted, because building a `LeanBatch` from decoded records would
    /// mean re-serialising them, which is worse than the cost being avoided.
    pub fallback: Vec<kafka_protocol::records::Record>,
}

impl<T: Transport> Consumer<T> {
    /// As [`Self::fetch`], without building a record per record.
    ///
    /// **Experimental, and deliberately a second method rather than a change to
    /// the first.** Two things differ for a caller: records carry no headers,
    /// and a key or value is materialised by asking the batch for it
    /// ([`LeanBatch::value`]) rather than being built up front.
    ///
    /// Falls back to the ordinary decoder for anything
    /// [`kestrel_core::records::decode_lean`] hands back — a compressed batch,
    /// or one carrying headers — so the result is correct either way, at the
    /// old cost for those.
    ///
    /// Filtering happens **per batch** here rather than per record, which it
    /// can because `transactional`, `control` and `producer_id` are batch-level
    /// in the format. That is most of why this path is cheaper.
    ///
    /// # Errors
    /// As [`Self::fetch`].
    pub async fn fetch_lean(&mut self) -> Result<Vec<LeanFetched>> {
        if self.positions.is_empty() {
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
                        // Compressed, or carrying headers: the ordinary decoder
                        // handles it, and this partition pays the old price.
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
                            out.push(LeanFetched {
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
                        out.push(LeanFetched {
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
