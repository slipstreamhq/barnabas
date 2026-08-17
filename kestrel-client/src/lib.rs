//! The client, written once and generic over its sockets.
//!
//! [`kestrel_core`] holds the parts with no IO at all — framing, correlation,
//! filtering, the producer's sequencing rules. This crate holds the part that
//! *sends*: connection pooling, leader routing, metadata refresh, the
//! consumer's fetch loop and the producer's transaction flow. All of it is
//! generic over one small [`Transport`], so a runtime binding supplies four
//! functions and nothing else.
//!
//! # Why the seam is four functions
//!
//! The first version of this crate did not exist: `kestrel-glommio` held all
//! 1368 lines of it. That made the design's claim — "bindings are ~300 lines"
//! — false, and would have made a second binding a copy of the first, with the
//! usual consequence that the two drift and only one gets the bug fix.
//!
//! So the split is by *what actually differs between runtimes*, which turns out
//! to be: open a socket, read, write, sleep. Everything above that is protocol
//! and is identical everywhere.
//!
//! # No `Send` bounds, deliberately
//!
//! Nothing here requires `Send`, on the futures or on the transport. That is
//! what lets a thread-per-core binding hold `!Send` sockets while a
//! work-stealing binding hands out a handle usable across threads — the
//! *binding* decides, and neither choice is imposed by this crate.
//!
//! An abstraction that required `Send` here would forbid the per-core case
//! outright, which is exactly the lowest-common-denominator failure that made
//! a runtime *trait* the wrong shape for `slipstream-rt` (see that crate's
//! `RtCtx`). The difference is that this trait abstracts over **sockets**, not
//! over runtimes: it never spawns, never names an executor, and has nothing to
//! lose by staying bound-free.

use std::future::Future;
use std::io;
use std::time::Duration;

pub mod cluster;
pub mod sasl;
pub mod builder;
pub mod group;
mod join;
mod timeout;
pub mod consumer;
pub mod producer;

pub use cluster::Cluster;
pub use sasl::{Credentials, SaslMechanism};
pub use builder::{ConsumerBuilder, ProducerBuilder, StartOffset};
pub use consumer::{Consumer, ConsumerRecords, RecordRef, EARLIEST, LATEST};
pub use group::{ClassicProtocol, GroupProtocol, Membership};
pub use producer::{Producer, ProducerRecord};

use kestrel_core::{Disposition, ErrorCode};

/// What a runtime must provide: a socket and a timer.
///
/// **`connect` takes `&self`** so a transport can carry configuration —
/// a TLS client config, a root store, a server-name policy. That is what makes
/// encryption a binding-level concern rather than something this crate has to
/// know about: `Consumer<GlommioTls>` and `Consumer<Glommio>` are the same
/// client over different sockets.
///
/// The rest are associated functions: reading, writing and sleeping need no
/// configuration, and requiring `&self` for them would mean borrowing the
/// transport across every request for nothing.
pub trait Transport: 'static {
    /// The runtime's stream — a TCP socket, or a TLS session over one.
    type Stream: 'static;

    /// Open a connection to `host:port`.
    fn connect(&self, addr: &str) -> impl Future<Output = io::Result<Self::Stream>>;

    /// Read into `buf`, returning the byte count. Zero means the peer closed.
    fn read(stream: &mut Self::Stream, buf: &mut [u8]) -> impl Future<Output = io::Result<usize>>;

    /// Write all of `buf`.
    fn write_all(stream: &mut Self::Stream, buf: &[u8]) -> impl Future<Output = io::Result<()>>;

    /// Sleep, for retry backoff.
    fn sleep(dur: Duration) -> impl Future<Output = ()>;
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("core: {0}")]
    Core(#[from] kestrel_core::Error),

    #[error("io: {0}")]
    Io(#[from] io::Error),

    #[error("connect {addr}: {source}")]
    Connect {
        addr: String,
        #[source]
        source: io::Error,
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

    /// The partition has no leader even after a metadata refresh — what a
    /// partition mid-election looks like. Separate from [`Self::Broker`] so a
    /// caller can back off and retry rather than treat it as fatal.
    #[error("{topic}-{partition} has no leader")]
    NoLeader { topic: String, partition: i32 },

    /// A misuse of the producer, caught by the state machine rather than by a
    /// broker — producing outside a transaction, to an unenrolled partition, or
    /// after being fenced.
    #[error("producer: {0}")]
    Producer(#[from] kestrel_core::producer::ProducerError),

    /// A request outlived its deadline. The connection is dropped with it —
    /// see [`Cluster::call_at`](cluster::Cluster).
    #[error("{op:?} to {addr} timed out")]
    Timeout {
        op: kafka_protocol::messages::ApiKey,
        addr: String,
    },

    /// Authentication failed, or the broker does not offer the mechanism.
    #[error("sasl: {0}")]
    Sasl(String),

    #[error("the broker's response contained no {0}")]
    Missing(&'static str),
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn check(op: &'static str, code: i16) -> Result<()> {
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
