//! The glommio binding: four functions.
//!
//! Everything else — connection pooling, leader routing, metadata refresh, the
//! consumer's filtering, the producer's transaction flow — is
//! [`kestrel_client`], written once and shared with the tokio binding. This
//! file is the whole difference between the two runtimes.
//!
//! # `!Send`, and why nothing above had to know
//!
//! `glommio::net::TcpStream` belongs to the core that opened it, so everything
//! built on this transport is `!Send` and must stay on its executor. That is a
//! property of *this* binding, not of the client: `kestrel-client` places no
//! `Send` bound anywhere, so the tokio binding is free to be `Send` from the
//! same code. An abstraction that required `Send` would have forbidden this
//! side outright.

use std::io;
use std::time::Duration;

use futures_lite::{AsyncReadExt as _, AsyncWriteExt as _};
use glommio::net::TcpStream;

#[cfg(feature = "tls")]
pub mod tls;

pub use kestrel_client::producer::CompressionCodec;
pub use kestrel_client::{Error, FetchedRecords, ProducerRecord, Result, EARLIEST, LATEST};
pub use kestrel_core::{Disposition, ErrorCode, IsolationLevel, Partitioner};

/// The runtime selector. Never instantiated — it exists to name a choice.
#[derive(Debug, Clone, Copy)]
pub struct Glommio;

impl kestrel_client::Transport for Glommio {
    type Stream = TcpStream;

    async fn connect(&self, addr: &str) -> io::Result<Self::Stream> {
        // glommio's error type is its own; the seam speaks `io::Error`, which
        // every runtime can produce.
        TcpStream::connect(addr)
            .await
            .map_err(|e| io::Error::other(e.to_string()))
    }

    async fn read(stream: &mut Self::Stream, buf: &mut [u8]) -> io::Result<usize> {
        stream.read(buf).await
    }

    async fn write_all(stream: &mut Self::Stream, buf: &[u8]) -> io::Result<()> {
        stream.write_all(buf).await?;
        // Kafka framing is length-prefixed, so a half-written request is a
        // protocol error rather than a slow one.
        stream.flush().await
    }

    async fn sleep(dur: Duration) {
        glommio::timer::sleep(dur).await;
    }
}

/// A cluster handle on this core.
pub type Cluster = kestrel_client::Cluster<Glommio>;

/// An assign-only consumer on this core.
pub type Consumer = kestrel_client::Consumer<Glommio>;

/// An idempotent, optionally transactional producer on this core.
pub type Producer = kestrel_client::Producer<Glommio>;
