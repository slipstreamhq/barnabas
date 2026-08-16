//! The assign-only consumer.

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
use kestrel_core::{Disposition, ErrorCode, IsolationLevel};

use crate::cluster::Cluster;
use crate::{check, Error, Result, Transport};

/// Timestamps `ListOffsets` understands.
pub const EARLIEST: i64 = -2;
pub const LATEST: i64 = -1;

/// How many times a request is retried when the broker says leadership moved.
///
/// Bounded rather than unbounded: a partition whose leader never settles is a
/// cluster problem, and spinning silently is worse than surfacing it.
const MAX_LEADER_RETRIES: usize = 5;

/// An assign-only consumer for one partition.
///
/// Assignment is the caller's: no group protocol, no rebalance, no offset
/// commit. Where the position lives is also the caller's problem, which is what
/// makes this usable from a system that checkpoints offsets itself.
pub struct Consumer<T: Transport> {
    cluster: Cluster<T>,
    topic: String,
    partition: i32,
    next_offset: i64,
    isolation: IsolationLevel,
    max_wait: Duration,
    max_bytes: i32,
}

impl<T: Transport> Consumer<T> {
    /// Connect and position on `topic`/`partition` at `offset`.
    ///
    /// `offset` may be [`EARLIEST`], [`LATEST`], or an absolute offset;
    /// the first two are resolved with `ListOffsets` before the first fetch.
    ///
    /// # Errors
    /// If no bootstrap address answers, the topic does not exist, or the broker
    /// answers with an error code.
    pub async fn assign(
        bootstrap: &[String],
        client_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        isolation: IsolationLevel,
    ) -> Result<Self> {
        let mut cluster = Cluster::connect(bootstrap, client_id).await?;
        // Up front so a missing topic fails here rather than as a fetch loop
        // that never returns anything.
        cluster.refresh_metadata(topic).await?;

        let mut me = Self {
            cluster,
            topic: topic.to_owned(),
            partition,
            next_offset: offset,
            isolation,
            max_wait: Duration::from_millis(500),
            max_bytes: 10 * 1024 * 1024,
        };
        if offset == EARLIEST || offset == LATEST {
            me.next_offset = me.list_offset(offset).await?;
        }
        Ok(me)
    }

    /// Where the next fetch will start.
    #[must_use]
    pub fn position(&self) -> i64 {
        self.next_offset
    }

    /// Seek. The caller owns its offsets, so this is how a restored checkpoint
    /// is applied.
    pub fn seek(&mut self, offset: i64) {
        self.next_offset = offset;
    }

    /// How long a fetch waits for data before returning empty.
    pub fn set_max_wait(&mut self, max_wait: Duration) {
        self.max_wait = max_wait;
    }

    /// The connections this consumer holds. See [`Cluster`] on why the count
    /// matters for a per-core client.
    #[must_use]
    pub fn connection_count(&self) -> usize {
        self.cluster.connection_count()
    }

    /// The address the cluster map currently names as this partition's leader,
    /// if it knows one. Exposed for tests and for callers that want to see the
    /// routing decision rather than infer it.
    #[must_use]
    pub fn metadata_leader(&self, topic: &str, partition: i32) -> Option<String> {
        self.cluster
            .metadata()
            .leader_for(topic, partition)
            .map(kestrel_core::BrokerAddr::addr)
    }

    /// Resolve a timestamp to an offset. [`EARLIEST`] and [`LATEST`] are the
    /// two a consumer normally wants; any millisecond timestamp works.
    ///
    /// # Errors
    /// If the broker answers with an error code.
    pub async fn list_offset(&mut self, timestamp: i64) -> Result<i64> {
        let mut partition = ListOffsetsPartition::default();
        partition.partition_index = self.partition;
        partition.timestamp = timestamp;

        let mut topic = ListOffsetsTopic::default();
        topic.name = TopicName(StrBytes::from_string(self.topic.clone()));
        topic.partitions = vec![partition];

        let mut req = ListOffsetsRequest::default();
        req.replica_id = BrokerId(-1);
        req.isolation_level = self.isolation.as_i8();
        req.topics = vec![topic];

        self.with_leader("ListOffsets", |resp: ListOffsetsResponse, want| {
            resp.topics
                .iter()
                .flat_map(|t| t.partitions.iter())
                .find(|p| p.partition_index == want)
                .map(|p| (p.error_code, p.offset))
        })
        .call(ApiKey::ListOffsets, 7, &req)
        .await
    }

    /// Fetch once from the current position, advancing it.
    ///
    /// An empty result is normal: a fetch that waits out `max_wait` with no new
    /// data is not an error. The position advances past filtered records as
    /// well as returned ones, so an all-aborted fetch makes progress rather
    /// than looping.
    ///
    /// # Errors
    /// If the connection fails, leadership cannot be resolved, or the broker
    /// answers with a code that is not about leadership.
    pub async fn fetch(&mut self) -> Result<Vec<Record>> {
        for attempt in 0..=MAX_LEADER_RETRIES {
            let addr = self.cluster.leader_addr(&self.topic, self.partition).await?;
            let req = self.fetch_request();

            let resp: FetchResponse = self.cluster.call_at(&addr, ApiKey::Fetch, 12, &req).await?;
            check("Fetch", resp.error_code)?;

            let Some(partition) = resp
                .responses
                .iter()
                .flat_map(|t| t.partitions.iter())
                .find(|p| p.partition_index == self.partition)
            else {
                // A response with no partition data is an empty fetch.
                return Ok(Vec::new());
            };

            let code = ErrorCode(partition.error_code);
            if code.disposition() == Disposition::RefreshMetadata {
                // The leader moved. Forget just this partition, ask again, and
                // retry — the flow every metadata cache is built around.
                self.cluster.invalidate(&self.topic, self.partition);
                if attempt == MAX_LEADER_RETRIES {
                    return Err(Error::Broker {
                        op: "Fetch",
                        code: code.0,
                        disposition: code.disposition(),
                    });
                }
                self.cluster.refresh_metadata(&self.topic).await?;
                continue;
            }
            check("Fetch partition", partition.error_code)?;

            let records = match &partition.records {
                Some(bytes) if !bytes.is_empty() => decode_records(bytes.clone())?,
                _ => return Ok(Vec::new()),
            };
            let aborted: Vec<AbortedTransaction> = partition
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

            // The filtering the broker does not do. See `kestrel_core::consumer`.
            let Fetched {
                records,
                next_offset,
            } = consumer::filter(
                records,
                &aborted,
                partition.last_stable_offset,
                self.isolation,
                self.next_offset,
            );
            self.next_offset = next_offset;
            return Ok(records);
        }
        unreachable!("the loop returns or errors on its last iteration")
    }

    fn fetch_request(&self) -> FetchRequest {
        let mut partition = FetchPartition::default();
        partition.partition = self.partition;
        partition.fetch_offset = self.next_offset;
        partition.partition_max_bytes = self.max_bytes;
        partition.current_leader_epoch = -1;
        partition.log_start_offset = -1;

        let mut topic = FetchTopic::default();
        topic.topic = TopicName(StrBytes::from_string(self.topic.clone()));
        topic.partitions = vec![partition];

        let mut req = FetchRequest::default();
        req.replica_id = BrokerId(-1);
        req.max_wait_ms = i32::try_from(self.max_wait.as_millis()).unwrap_or(i32::MAX);
        req.min_bytes = 1;
        req.max_bytes = self.max_bytes;
        req.isolation_level = self.isolation.as_i8();
        req.topics = vec![topic];
        req
    }

    /// Route a partition request to its leader, refreshing and retrying if the
    /// broker says leadership moved.
    ///
    /// `extract` pulls `(error_code, value)` for the partition out of whatever
    /// response shape the API uses — the one part that differs between them.
    fn with_leader<Resp, V, F>(&mut self, op: &'static str, extract: F) -> LeaderCall<'_, T, Resp, V, F>
    where
        F: Fn(Resp, i32) -> Option<(i16, V)>,
    {
        LeaderCall {
            consumer: self,
            op,
            extract,
            _resp: std::marker::PhantomData,
        }
    }
}

/// A partition request in the middle of being routed. See
/// [`Consumer::with_leader`].
///
/// A struct rather than a closure taking `&mut Consumer`: the closure would
/// borrow the consumer mutably and return a future that outlives the borrow,
/// which is a lifetime fight with nothing at the end of it.
pub struct LeaderCall<'a, T: Transport, Resp, V, F> {
    consumer: &'a mut Consumer<T>,
    op: &'static str,
    extract: F,
    _resp: std::marker::PhantomData<(Resp, V)>,
}

impl<T: Transport, Resp, V, F> LeaderCall<'_, T, Resp, V, F>
where
    Resp: kafka_protocol::protocol::Decodable,
    F: Fn(Resp, i32) -> Option<(i16, V)>,
{
    async fn call<Req: kafka_protocol::protocol::Encodable>(
        self,
        api_key: ApiKey,
        version: i16,
        req: &Req,
    ) -> Result<V> {
        let Self {
            consumer,
            op,
            extract,
            ..
        } = self;
        for attempt in 0..=MAX_LEADER_RETRIES {
            let addr = consumer
                .cluster
                .leader_addr(&consumer.topic, consumer.partition)
                .await?;
            let resp: Resp = consumer.cluster.call_at(&addr, api_key, version, req).await?;

            let (code, value) =
                extract(resp, consumer.partition).ok_or(Error::Missing("partition"))?;
            let code = ErrorCode(code);
            if code.disposition() == Disposition::RefreshMetadata {
                consumer
                    .cluster
                    .invalidate(&consumer.topic, consumer.partition);
                if attempt == MAX_LEADER_RETRIES {
                    return Err(Error::Broker {
                        op,
                        code: code.0,
                        disposition: code.disposition(),
                    });
                }
                consumer.cluster.refresh_metadata(&consumer.topic).await?;
                continue;
            }
            check(op, code.0)?;
            return Ok(value);
        }
        unreachable!("the loop returns or errors on its last iteration")
    }
}

fn decode_records(mut bytes: Bytes) -> Result<Vec<Record>> {
    // A fetch response can carry several batches back to back; the decoder
    // takes one at a time, so drain until the buffer is spent.
    let mut all = Vec::new();
    while !bytes.is_empty() {
        let set = RecordBatchDecoder::decode(&mut bytes).map_err(|e| {
            Error::Core(kestrel_core::Error::Codec(format!(
                "decode record batch: {e}"
            )))
        })?;
        all.extend(set.records);
    }
    Ok(all)
}
