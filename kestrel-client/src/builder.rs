//! Staged builders, so the type says what is still missing.
//!
//! # Why not one builder struct
//!
//! The usual `Builder::new().a().b().build()` shape answers "what can I set?"
//! but not "what must I still set?" — `build()` is always offered, and a
//! missing bootstrap list is a runtime error. Here each stage is **its own
//! type**, exposing only what is valid at that point:
//!
//! ```text
//! Consumer::builder(Glommio)   → needs a bootstrap list, and offers nothing else
//!   .bootstrap([..])           → needs a client id, and offers nothing else
//!   .client_id("my-app")       → now optional settings appear, and `build` exists
//! ```
//!
//! An editor's completion list is therefore the set of legal next steps, and
//! forgetting a required one is a compile error naming the stage rather than a
//! failure at connect.
//!
//! # What this fixes about the constructors
//!
//! `Consumer::assign` takes seven positional arguments, two of which are an
//! adjacent `partition: i32` and `offset: i64` — transposing them compiles.
//! `EARLIEST` and `LATEST` are `i64` sentinels sharing that offset parameter.
//! And every option is a setter that only exists *after* construction, so the
//! thing that tells you `max_wait` is adjustable is documentation rather than
//! the type.
//!
//! The constructors remain: they are the shortest path when nothing optional is
//! wanted, and the builder is written in terms of them.

use std::time::Duration;

use kafka_protocol::records::Compression;
use kestrel_core::{IsolationLevel, Partitioner};

use crate::{Consumer, Credentials, Producer, Result, Transport, EARLIEST, LATEST};

/// Where a partition starts reading.
///
/// An enum rather than the `i64` the protocol uses, because `EARLIEST` and
/// `LATEST` are negative sentinels sharing a parameter with real offsets — a
/// distinction worth having the compiler keep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartOffset {
    Earliest,
    Latest,
    /// A stored offset — the case a system with its own checkpoints uses.
    At(i64),
}

impl StartOffset {
    fn as_i64(self) -> i64 {
        match self {
            Self::Earliest => EARLIEST,
            Self::Latest => LATEST,
            Self::At(offset) => offset,
        }
    }
}

// ── consumer ─────────────────────────────────────────────────────────────────

/// Stage 1: has a transport, needs a bootstrap list.
pub struct ConsumerBuilder<T> {
    transport: T,
}

impl<T: Transport> ConsumerBuilder<T> {
    pub(crate) fn new(transport: T) -> Self {
        Self { transport }
    }

    /// The brokers to contact first. Any one that answers is enough; the rest
    /// of the cluster comes from its metadata.
    #[must_use]
    pub fn bootstrap<I, S>(self, addrs: I) -> ConsumerNeedsClientId<T>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        ConsumerNeedsClientId {
            transport: self.transport,
            bootstrap: addrs.into_iter().map(Into::into).collect(),
        }
    }
}

/// Stage 2: needs a client id.
pub struct ConsumerNeedsClientId<T> {
    transport: T,
    bootstrap: Vec<String>,
}

impl<T: Transport> ConsumerNeedsClientId<T> {
    /// How this client identifies itself to the broker. It appears in the
    /// broker's logs and metrics, so it is worth making it say which service
    /// this is.
    #[must_use]
    pub fn client_id(self, client_id: impl Into<String>) -> ConsumerReady<T> {
        ConsumerReady {
            transport: self.transport,
            bootstrap: self.bootstrap,
            client_id: client_id.into(),
            isolation: IsolationLevel::ReadCommitted,
            credentials: None,
            max_wait: None,
            prefetch: None,
            incremental: None,
            assignments: Vec::new(),
            every_partition: Vec::new(),
        }
    }
}

/// Stage 3: everything required is present; the rest is optional.
pub struct ConsumerReady<T> {
    transport: T,
    bootstrap: Vec<String>,
    client_id: String,
    isolation: IsolationLevel,
    credentials: Option<Credentials>,
    max_wait: Option<Duration>,
    prefetch: Option<bool>,
    incremental: Option<bool>,
    assignments: Vec<(String, i32, StartOffset)>,
    every_partition: Vec<(String, StartOffset)>,
}

impl<T: Transport> ConsumerReady<T> {
    /// Defaults to [`IsolationLevel::ReadCommitted`] — the safe end, since
    /// READ_UNCOMMITTED shows records from transactions that later aborted.
    #[must_use]
    pub fn isolation(mut self, isolation: IsolationLevel) -> Self {
        self.isolation = isolation;
        self
    }

    /// SASL credentials. Pair `PLAIN` with TLS; it sends the password in the
    /// clear.
    #[must_use]
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Assign a partition and where to start it. Call it once per partition.
    ///
    /// Assignment is the caller's: there is no consumer group and no rebalance,
    /// so nothing assigns partitions behind your back.
    #[must_use]
    pub fn assign(mut self, topic: impl Into<String>, partition: i32, start: StartOffset) -> Self {
        self.assignments.push((topic.into(), partition, start));
        self
    }

    /// Assign a range of partitions, all starting at the same place.
    #[must_use]
    pub fn assign_range(
        mut self,
        topic: impl Into<String>,
        partitions: impl IntoIterator<Item = i32>,
        start: StartOffset,
    ) -> Self {
        let topic = topic.into();
        for partition in partitions {
            self.assignments.push((topic.clone(), partition, start));
        }
        self
    }

    /// Assign **every** partition of `topic`, asking the broker how many there
    /// are.
    ///
    /// The count is the one thing about an assignment worth asking for: it
    /// changes when a topic is expanded, and hardcoding it in an
    /// [`assign_range`](Self::assign_range) silently stops consuming the new
    /// partitions. *Which* partitions this client owns is still the caller's —
    /// there is no consumer group here, so a process that wants a share of a
    /// topic rather than all of it assigns that share itself.
    ///
    /// **Resolved once, at [`build`](Self::build), and that is a real hazard for
    /// a topic you do not own.** Adding partitions is how a topic is scaled,
    /// and it is usually done by whoever produces to it. A topic expanded from
    /// 8 to 16 partitions after this call leaves partitions 8–15 unread
    /// indefinitely: nothing errors, and the consumer looks healthy while
    /// missing a share of its input.
    ///
    /// Until this client can watch for that — see `docs/consumer-groups.md`,
    /// which is where the fix is scoped — `assign_all` means *all of them as of
    /// now*, and a caller reading a topic owned by someone else should poll
    /// [`Consumer::partition_count`](crate::Consumer::partition_count) and
    /// rebuild when it grows.
    #[must_use]
    pub fn assign_all(mut self, topic: impl Into<String>, start: StartOffset) -> Self {
        self.every_partition.push((topic.into(), start));
        self
    }

    /// How long a fetch waits at the broker for data before coming back empty.
    #[must_use]
    pub fn max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait = Some(max_wait);
        self
    }

    /// Keep a fetch permanently in flight, so the broker is already working
    /// while the caller processes the last batch. On by default.
    #[must_use]
    pub fn prefetch(mut self, prefetch: bool) -> Self {
        self.prefetch = Some(prefetch);
        self
    }

    /// Incremental fetch sessions (KIP-227). On by default; turn it off for a
    /// broker or proxy that mishandles them.
    #[must_use]
    pub fn incremental_fetch(mut self, incremental: bool) -> Self {
        self.incremental = Some(incremental);
        self
    }

    /// Connect, authenticate, and resolve every assignment's starting offset.
    ///
    /// # Errors
    /// If no bootstrap address answers, authentication fails, or a topic or
    /// partition in an assignment does not exist.
    pub async fn build(self) -> Result<Consumer<T>> {
        let mut consumer = if self.credentials.is_some() {
            let mut cluster =
                crate::Cluster::connect(self.transport, &self.bootstrap, &self.client_id).await?;
            if let Some(credentials) = self.credentials {
                cluster.set_credentials(credentials);
            }
            Consumer::from_cluster(cluster, self.isolation)
        } else {
            Consumer::new(
                self.transport,
                &self.bootstrap,
                &self.client_id,
                self.isolation,
            )
            .await?
        };

        // Settings before assignments: `add` resolves offsets with a request,
        // and it should use the settings the caller asked for.
        if let Some(max_wait) = self.max_wait {
            consumer.set_max_wait(max_wait);
        }
        if let Some(prefetch) = self.prefetch {
            consumer.set_prefetch(prefetch);
        }
        if let Some(incremental) = self.incremental {
            consumer.set_incremental_fetch(incremental);
        }
        for (topic, start) in self.every_partition {
            let count = consumer.partition_count(&topic).await?;
            for partition in 0..count {
                consumer.add(&topic, partition, start.as_i64()).await?;
            }
        }
        for (topic, partition, start) in self.assignments {
            consumer.add(&topic, partition, start.as_i64()).await?;
        }
        Ok(consumer)
    }
}

// ── producer ─────────────────────────────────────────────────────────────────

/// Stage 1: has a transport, needs a bootstrap list.
pub struct ProducerBuilder<T> {
    transport: T,
}

impl<T: Transport> ProducerBuilder<T> {
    pub(crate) fn new(transport: T) -> Self {
        Self { transport }
    }

    /// See [`ConsumerBuilder::bootstrap`].
    #[must_use]
    pub fn bootstrap<I, S>(self, addrs: I) -> ProducerNeedsClientId<T>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        ProducerNeedsClientId {
            transport: self.transport,
            bootstrap: addrs.into_iter().map(Into::into).collect(),
        }
    }
}

/// Stage 2: needs a client id.
pub struct ProducerNeedsClientId<T> {
    transport: T,
    bootstrap: Vec<String>,
}

impl<T: Transport> ProducerNeedsClientId<T> {
    /// See [`ConsumerNeedsClientId::client_id`].
    #[must_use]
    pub fn client_id(self, client_id: impl Into<String>) -> ProducerReady<T> {
        ProducerReady {
            transport: self.transport,
            bootstrap: self.bootstrap,
            client_id: client_id.into(),
            transactional_id: None,
            compression: None,
            partitioner: None,
            max_in_flight: None,
        }
    }
}

/// Stage 3: everything required is present; the rest is optional.
///
/// Builds an **idempotent** producer unless
/// [`transactional_id`](Self::transactional_id) is given — idempotence is not
/// optional here, because a producer without it silently duplicates on retry.
pub struct ProducerReady<T> {
    transport: T,
    bootstrap: Vec<String>,
    client_id: String,
    transactional_id: Option<String>,
    compression: Option<Compression>,
    partitioner: Option<Partitioner>,
    max_in_flight: Option<usize>,
}

impl<T: Transport> ProducerReady<T> {
    /// Make this a transactional producer under `id`.
    ///
    /// **Stable per instance, and held by one process at a time.** Building a
    /// producer fences any earlier one holding the same id, which is how a
    /// restarted job stops its own zombie — and, if two live instances share an
    /// id, how they stop each other.
    #[must_use]
    pub fn transactional_id(mut self, id: impl Into<String>) -> Self {
        self.transactional_id = Some(id.into());
        self
    }

    /// Compress whole batches. `snappy` and `zstd` measure fastest here — see
    /// `PERF.md`.
    #[must_use]
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Which hash places a keyed record. Defaults to librdkafka's CRC-32, so a
    /// program migrating off `rdkafka` keeps its key placement;
    /// [`Partitioner::Murmur2`] matches the Java client instead.
    #[must_use]
    pub fn partitioner(mut self, partitioner: Partitioner) -> Self {
        self.partitioner = partitioner.into();
        self
    }

    /// Requests in flight per connection. Five by default, as in the Java
    /// client; one restores strict request-response.
    #[must_use]
    pub fn max_in_flight(mut self, max: usize) -> Self {
        self.max_in_flight = Some(max);
        self
    }

    /// Connect and acquire a producer id.
    ///
    /// # Errors
    /// If no bootstrap address answers, or the transaction coordinator never
    /// becomes available.
    pub async fn build(self) -> Result<Producer<T>> {
        let mut producer = match &self.transactional_id {
            Some(id) => {
                Producer::transactional(self.transport, &self.bootstrap, &self.client_id, id)
                    .await?
            }
            None => Producer::idempotent(self.transport, &self.bootstrap, &self.client_id).await?,
        };
        if let Some(compression) = self.compression {
            producer.set_compression(compression);
        }
        if let Some(partitioner) = self.partitioner {
            producer.set_partitioner(partitioner);
        }
        if let Some(max) = self.max_in_flight {
            producer.set_max_in_flight(max);
        }
        Ok(producer)
    }
}
