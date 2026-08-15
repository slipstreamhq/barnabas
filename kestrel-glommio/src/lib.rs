//! The glommio binding: sockets and timers for [`kestrel_core`].
//!
//! Everything that touches a file descriptor lives here. The protocol logic —
//! framing, correlation, READ_COMMITTED filtering — is in the core and is
//! shared with every other binding, so a bug in it cannot be arm-specific.
//!
//! # `!Send` on purpose
//!
//! A [`Consumer`] holds a `glommio::net::TcpStream`, which belongs to the core
//! that opened it, so the handle is `!Send` and must not be moved between
//! executors. That is the point rather than a limitation: the core is neither
//! `Send` nor `!Send`, so a work-stealing binding can make the opposite choice
//! without either of them compromising.
//!
//! # Scope
//!
//! Assign-only consumer, as in the design's P1. One connection to one broker.
//! No metadata cache, no leader failover, no producer — those are P1's
//! remainder and P2, and putting sketches of them here would make the shape
//! harder to change, not easier.

use std::time::Duration;

use bytes::Bytes;
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;

use kafka_protocol::messages::{
    fetch_request::{FetchPartition, FetchTopic},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    metadata_request::MetadataRequestTopic,
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, FetchRequest, FetchResponse,
    ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Record, RecordBatchDecoder};

use kestrel_core::consumer::{self, AbortedTransaction, Fetched};
use kestrel_core::{Connection, Disposition, ErrorCode, IsolationLevel};

/// Timestamps `ListOffsets` understands.
pub const EARLIEST: i64 = -2;
pub const LATEST: i64 = -1;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("core: {0}")]
    Core(#[from] kestrel_core::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("connect {addr}: {source}")]
    Connect {
        addr: String,
        #[source]
        source: std::io::Error,
    },

    /// A broker error code, with what the client should do about it. Carrying
    /// the [`Disposition`] means a caller can react without re-deriving the
    /// taxonomy — and cannot accidentally retry something fatal.
    #[error("{op} failed with error code {code} ({disposition:?})")]
    Broker {
        op: &'static str,
        code: i16,
        disposition: Disposition,
    },

    #[error("the broker's response contained no {0}")]
    Missing(&'static str),
}

type Result<T> = std::result::Result<T, Error>;

fn check(op: &'static str, code: i16) -> Result<()> {
    let code = ErrorCode(code);
    if code.is_ok() {
        return Ok(());
    }
    Err(Error::Broker {
        op,
        code: code.0,
        disposition: code.disposition(),
    })
}

/// One broker connection, driven by this core.
struct Broker {
    stream: TcpStream,
    conn: Connection,
}

impl Broker {
    async fn connect(addr: &str, client_id: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await.map_err(|e| Error::Connect {
            addr: addr.to_owned(),
            source: std::io::Error::other(e.to_string()),
        })?;
        Ok(Self {
            stream,
            conn: Connection::new(StrBytes::from_string(client_id.to_owned())),
        })
    }

    /// Send one request and read its response.
    ///
    /// Strictly one in flight, which is all an assign-only consumer needs: it
    /// has a single outstanding fetch per partition by construction. The core
    /// already correlates pipelined requests ([`Connection::in_flight`]), so
    /// raising this is a change here, not there.
    async fn call<Req, Resp>(&mut self, api_key: ApiKey, version: i16, req: &Req) -> Result<Resp>
    where
        Req: kafka_protocol::protocol::Encodable,
        Resp: kafka_protocol::protocol::Decodable,
    {
        let wire = self.conn.request(api_key, version, req)?;
        self.stream.write_all(&wire).await?;
        self.stream.flush().await?;

        loop {
            if let Some(resp) = self.conn.next_response()? {
                return Ok(Connection::decode(&resp)?);
            }
            let mut buf = [0u8; 16 * 1024];
            let n = self.stream.read(&mut buf).await?;
            if n == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "broker closed the connection",
                )));
            }
            self.conn.push_bytes(&buf[..n]);
        }
    }
}

/// An assign-only consumer for one partition.
///
/// Assignment is the caller's: there is no group protocol, no rebalance, and no
/// offset commit. Where the partition's position lives is the caller's problem
/// too, which is what makes this usable from a system that checkpoints offsets
/// itself.
pub struct Consumer {
    broker: Broker,
    topic: String,
    partition: i32,
    next_offset: i64,
    isolation: IsolationLevel,
    max_wait: Duration,
    max_bytes: i32,
}

impl Consumer {
    /// Connect to `addr` and position on `topic`/`partition` at `offset`.
    ///
    /// `offset` may be [`EARLIEST`] or [`LATEST`], which are resolved with
    /// `ListOffsets` before the first fetch, or an absolute offset.
    ///
    /// # Errors
    /// If the broker is unreachable, the topic or partition does not exist, or
    /// the broker answers with an error code.
    pub async fn assign(
        addr: &str,
        client_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        isolation: IsolationLevel,
    ) -> Result<Self> {
        let mut broker = Broker::connect(addr, client_id).await?;

        // `ApiVersions` first, as every client does: it is how a broker's
        // supported range is discovered, and doing it up front means an
        // unsupported version fails at connect rather than mid-stream.
        let versions: ApiVersionsResponse = broker
            .call(ApiKey::ApiVersions, 3, &{
                let mut req = ApiVersionsRequest::default();
                req.client_software_name = StrBytes::from_string(client_id.to_owned());
                req.client_software_version = StrBytes::from_static_str("0.0.1");
                req
            })
            .await?;
        check("ApiVersions", versions.error_code)?;

        let mut me = Self {
            broker,
            topic: topic.to_owned(),
            partition,
            next_offset: offset,
            isolation,
            max_wait: Duration::from_millis(500),
            max_bytes: 10 * 1024 * 1024,
        };

        // Metadata is fetched for its error code, not (yet) for routing: a
        // topic that does not exist should fail here rather than as an empty
        // fetch loop. Leader routing is the next slice of P1.
        me.metadata().await?;

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

    /// How long a fetch waits for `min_bytes` before returning empty.
    pub fn set_max_wait(&mut self, max_wait: Duration) {
        self.max_wait = max_wait;
    }

    async fn metadata(&mut self) -> Result<MetadataResponse> {
        let mut topic = MetadataRequestTopic::default();
        topic.name = Some(TopicName(StrBytes::from_string(self.topic.clone())));
        let mut req = MetadataRequest::default();
        req.topics = Some(vec![topic]);

        let resp: MetadataResponse = self.broker.call(ApiKey::Metadata, 12, &req).await?;
        for t in &resp.topics {
            check("Metadata", t.error_code)?;
            for p in &t.partitions {
                check("Metadata partition", p.error_code)?;
            }
        }
        Ok(resp)
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

        let resp: ListOffsetsResponse = self.broker.call(ApiKey::ListOffsets, 7, &req).await?;
        // One topic, one partition was asked for, so the first answer is the
        // answer — but it is found rather than indexed, because a broker is
        // free to send an empty list and indexing would panic on it.
        let partition = resp
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter())
            .find(|p| p.partition_index == self.partition)
            .ok_or(Error::Missing("partition"))?;
        check("ListOffsets", partition.error_code)?;
        Ok(partition.offset)
    }

    /// Fetch once from the current position, advancing it.
    ///
    /// Returns whatever the broker had, which may be empty — a fetch that times
    /// out with no data is normal, not an error. The position advances past
    /// filtered records as well as returned ones, so an all-aborted fetch makes
    /// progress rather than looping.
    ///
    /// # Errors
    /// If the connection fails or the broker answers with an error code.
    pub async fn fetch(&mut self) -> Result<Vec<Record>> {
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

        let resp: FetchResponse = self.broker.call(ApiKey::Fetch, 12, &req).await?;
        check("Fetch", resp.error_code)?;

        let mut out = Vec::new();
        for t in &resp.responses {
            for p in &t.partitions {
                check("Fetch partition", p.error_code)?;

                let records = match &p.records {
                    Some(bytes) if !bytes.is_empty() => decode_records(bytes.clone())?,
                    _ => continue,
                };
                let aborted: Vec<AbortedTransaction> = p
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
                    p.last_stable_offset,
                    self.isolation,
                    self.next_offset,
                );
                self.next_offset = next_offset;
                out.extend(records);
            }
        }
        Ok(out)
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
