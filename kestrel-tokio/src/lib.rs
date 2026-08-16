//! The tokio binding: four functions.
//!
//! Everything else — connection pooling, leader routing, metadata refresh, the
//! consumer's filtering, the producer's transaction flow — is
//! [`kestrel_client`], written once and shared with the per-core binding. This
//! file is the whole difference between the two runtimes.
//!
//! # What this binding proves
//!
//! That the sans-io split holds. The claim was that a state machine naming no
//! runtime can serve both a thread-per-core client and a work-stealing one; the
//! evidence is that adding tokio took a socket adapter and no protocol code at
//! all. If the split had been wrong, this file would be a copy of
//! `kestrel-glommio` with the types changed.
//!
//! # `Send`
//!
//! `tokio::net::TcpStream` is `Send`, so everything built on this transport is
//! too, and a handle can move between worker threads. Nothing in
//! `kestrel-client` asked for that or forbade it — the binding decides, which
//! is the point.

use std::io;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

#[cfg(feature = "tls")]
pub mod tls;

pub use kestrel_client::{Error, ProducerRecord, Result, EARLIEST, LATEST};
pub use kestrel_core::{Disposition, ErrorCode, IsolationLevel, Partitioner};

/// The runtime selector. Never instantiated — it exists to name a choice.
#[derive(Debug, Clone, Copy)]
pub struct Tokio;

impl kestrel_client::Transport for Tokio {
    type Stream = TcpStream;

    async fn connect(&self, addr: &str) -> io::Result<Self::Stream> {
        TcpStream::connect(addr).await
    }

    async fn read(stream: &mut Self::Stream, buf: &mut [u8]) -> io::Result<usize> {
        stream.read(buf).await
    }

    async fn write_all(stream: &mut Self::Stream, buf: &[u8]) -> io::Result<()> {
        stream.write_all(buf).await?;
        // Kafka framing is length-prefixed, so a half-written request is a
        // protocol error rather than a slow one — flush rather than let the
        // buffer decide when the broker sees it.
        stream.flush().await
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

/// A cluster handle on tokio.
pub type Cluster = kestrel_client::Cluster<Tokio>;

/// An assign-only consumer on tokio.
pub type Consumer = kestrel_client::Consumer<Tokio>;

/// An idempotent, optionally transactional producer on tokio.
pub type Producer = kestrel_client::Producer<Tokio>;
