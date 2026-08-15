//! The transactional producer: requests, routing, and retries.
//!
//! The *rules* are in [`kestrel_core::producer`] — sequence allocation, the
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

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    produce_request::{PartitionProduceData, TopicProduceData},
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey, EndTxnRequest, EndTxnResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, InitProducerIdRequest, InitProducerIdResponse,
    ProduceRequest, ProduceResponse, ProducerId, TopicName, TransactionalId,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use kestrel_core::producer::{ProducerIdentity, ProducerState, SequenceRange};
use kestrel_core::{Disposition, ErrorCode, Partitioner};

use crate::cluster::Cluster;
use crate::{Error, Result};

/// Attempts for a request whose disposition says "retry" or "re-discover".
///
/// Bounded rather than unbounded: a coordinator that never settles is a cluster
/// problem, and spinning silently is worse than surfacing it.
const MAX_RETRIES: usize = 40;
const RETRY_BACKOFF: Duration = Duration::from_millis(250);

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
pub struct Producer {
    cluster: Cluster,
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
    /// [`kestrel_core::partitioner`].
    round_robin: u32,
}

impl Producer {
    /// A transactional producer under `transactional_id`.
    ///
    /// `InitProducerId` fences any previous producer using the same id, which
    /// is how a restarted job stops its own zombie from writing.
    ///
    /// # Errors
    /// If no broker answers, or the coordinator never becomes available.
    pub async fn transactional(
        bootstrap: &[String],
        client_id: &str,
        transactional_id: &str,
    ) -> Result<Self> {
        let mut me = Self {
            cluster: Cluster::connect(bootstrap, client_id).await?,
            state: ProducerState::transactional(),
            transactional_id: Some(transactional_id.to_owned()),
            coordinator: None,
            acks: -1,
            timeout_ms: 30_000,
            compression: Compression::None,
            partitioner: Partitioner::default(),
            round_robin: 0,
        };
        me.init_producer_id().await?;
        Ok(me)
    }

    /// An idempotent, non-transactional producer.
    ///
    /// # Errors
    /// If no broker answers.
    pub async fn idempotent(bootstrap: &[String], client_id: &str) -> Result<Self> {
        let mut me = Self {
            cluster: Cluster::connect(bootstrap, client_id).await?,
            state: ProducerState::idempotent(),
            transactional_id: None,
            coordinator: None,
            acks: -1,
            timeout_ms: 30_000,
            compression: Compression::None,
            partitioner: Partitioner::default(),
            round_robin: 0,
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
    pub fn state(&self) -> kestrel_core::TxnState {
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
            let resp: FindCoordinatorResponse = self
                .cluster
                .any_broker()
                .await?
                .call(ApiKey::FindCoordinator, 3, &req)
                .await?;

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

    async fn backoff(_attempt: usize) {
        glommio::timer::sleep(RETRY_BACKOFF).await;
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
                self.cluster.any_broker().await?.call(api_key, version, req).await?
            } else {
                self.cluster.broker_at(&addr).await?.call(api_key, version, req).await?
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
    /// filtered by READ_COMMITTED consumers — see `kestrel_core::consumer`.
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

    /// Enroll a partition in the open transaction, if it is not already.
    ///
    /// The coordinator must know a partition is in the transaction *before*
    /// records are produced to it, or it cannot fence that partition at commit.
    async fn enroll(&mut self, topic: &str, partition: i32) -> Result<()> {
        if !self.state.needs_enrollment(topic, partition) {
            return Ok(());
        }
        let identity = self.state.identity().ok_or(Error::Missing("producer id"))?;
        let txn_id = self
            .transactional_id
            .clone()
            .ok_or(Error::Missing("transactional id"))?;

        let mut req_topic = AddPartitionsToTxnTopic::default();
        req_topic.name = TopicName(StrBytes::from_string(topic.to_owned()));
        req_topic.partitions = vec![partition];

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

        self.state.on_enrolled(topic, partition);
        Ok(())
    }

    // ── produce ────────────────────────────────────────────────────────────

    /// Produce `records` to one partition, returning the base offset the broker
    /// assigned.
    ///
    /// Enrolls the partition first when transactional. Sequence numbers come
    /// from [`kestrel_core::producer`] and are **not** the caller's to choose.
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
        self.enroll(topic, partition).await?;

        let count = i32::try_from(records.len()).map_err(|_| Error::Missing("batch size"))?;
        let range = self
            .state
            .allocate(topic, partition, count)
            .map_err(Error::Producer)?;

        // The same encoded batch is re-sent on every retry, sequence numbers
        // included. That is what makes a retry idempotent rather than a second
        // write.
        let batch = self.encode_batch(records, range)?;

        for attempt in 0..MAX_RETRIES {
            // A topic that is still being auto-created has no leader yet, which
            // is a wait rather than a failure — the same reason error 3
            // refreshes instead of failing.
            let addr = match self.cluster.leader_addr(topic, partition).await {
                Ok(addr) => addr,
                Err(Error::NoLeader { .. }) if attempt + 1 < MAX_RETRIES => {
                    Self::backoff(attempt).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let req = self.produce_request(topic, partition, batch.clone());

            let resp: ProduceResponse = self
                .cluster
                .broker_at(&addr)
                .await?
                .call(ApiKey::Produce, 9, &req)
                .await?;

            let Some(part) = resp
                .responses
                .iter()
                .flat_map(|t| t.partition_responses.iter())
                .find(|p| p.index == partition)
            else {
                return Err(Error::Missing("partition"));
            };

            let code = ErrorCode(part.error_code);
            if code.is_ok() {
                return Ok(part.base_offset);
            }
            match code.disposition() {
                Disposition::RefreshMetadata => {
                    self.cluster.invalidate(topic, partition);
                    self.cluster.refresh_metadata(topic).await?;
                    Self::backoff(attempt).await;
                }
                Disposition::Retry => Self::backoff(attempt).await,
                Disposition::Fatal => {
                    // A sequence or fencing error means this producer's stream
                    // is already wrong. Refusing further writes is the point.
                    self.state.fence();
                    return Err(Error::Broker {
                        op: "Produce",
                        code: code.0,
                        disposition: Disposition::Fatal,
                    });
                }
                Disposition::FindCoordinator | Disposition::Ok => {
                    return Err(Error::Broker {
                        op: "Produce",
                        code: code.0,
                        disposition: code.disposition(),
                    })
                }
            }
        }
        Err(Error::Broker {
            op: "Produce",
            code: ErrorCode::NOT_LEADER_OR_FOLLOWER.0,
            disposition: Disposition::RefreshMetadata,
        })
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
        // The partition count decides where every key lands, so a topic that is
        // still materialising must be waited for rather than partitioned
        // against a count of zero.
        let mut count = 0;
        for attempt in 0..MAX_RETRIES {
            match self.cluster.partition_count(topic).await {
                Ok(n) => {
                    count = n;
                    break;
                }
                Err(Error::NoLeader { .. }) if attempt + 1 < MAX_RETRIES => {
                    Self::backoff(attempt).await;
                }
                Err(e) => return Err(e),
            }
        }
        if count == 0 {
            return Err(Error::NoLeader {
                topic: topic.to_owned(),
                partition: -1,
            });
        }

        let mut by_partition: std::collections::BTreeMap<i32, Vec<ProducerRecord>> =
            std::collections::BTreeMap::new();
        for record in records {
            let partition = self.partitioner.partition_for(
                record.key.as_deref(),
                count,
                &mut self.round_robin,
            );
            by_partition.entry(partition).or_default().push(record.clone());
        }

        let mut offsets = Vec::with_capacity(by_partition.len());
        for (partition, batch) in by_partition {
            let base = self.send(topic, partition, &batch).await?;
            offsets.push((partition, base));
        }
        Ok(offsets)
    }

    /// Which partition a key would go to, without sending anything.
    ///
    /// Exposed for the parity test that checks this places keys exactly where
    /// `rdkafka` does — see `slipstream-kafka`'s partitioner parity test.
    ///
    /// # Errors
    /// If the topic's partition count cannot be learned.
    pub async fn partition_for(&mut self, topic: &str, key: Option<&[u8]>) -> Result<i32> {
        let count = self.cluster.partition_count(topic).await?;
        let mut scratch = self.round_robin;
        Ok(self.partitioner.partition_for(key, count, &mut scratch))
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
            Error::Core(kestrel_core::Error::Codec(format!(
                "encode record batch: {e}"
            )))
        })?;
        Ok(buf.freeze())
    }

    fn produce_request(&self, topic: &str, partition: i32, batch: Bytes) -> ProduceRequest {
        let mut partition_data = PartitionProduceData::default();
        partition_data.index = partition;
        partition_data.records = Some(batch);

        let mut topic_data = TopicProduceData::default();
        topic_data.name = TopicName(StrBytes::from_string(topic.to_owned()));
        topic_data.partition_data = vec![partition_data];

        let mut req = ProduceRequest::default();
        req.acks = self.acks;
        req.timeout_ms = self.timeout_ms;
        req.topic_data = vec![topic_data];
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
