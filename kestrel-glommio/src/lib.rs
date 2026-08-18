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
pub use kestrel_client::{
    BrokerInfo, Error, ConsumerRecords, GroupMetadata, NewTopic, ProducerRecord, RebalanceListener, RecordRef, Result,
    EARLIEST, LATEST,
};
pub use kestrel_core::{Disposition, ErrorCode, IsolationLevel, Partitioner};
/// SASL works on either runtime — it is the client's business, not the
/// socket's — so the bindings re-export it too. Without this a caller had to
/// depend on `kestrel-client` directly just to name a password.
pub use kestrel_client::{Credentials, SaslMechanism, StartOffset};

/// The runtime selector. Never instantiated — it exists to name a choice.
#[derive(Debug, Clone, Copy)]
pub struct Glommio;

impl kestrel_client::Transport for Glommio {
    type Stream = TcpStream;

    async fn connect(&self, addr: &str) -> io::Result<Self::Stream> {
        // glommio's error type is its own; the seam speaks `io::Error`, which
        // every runtime can produce.
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        // **TCP_NODELAY.** Kafka is request/response over a long-lived
        // connection, which is the exact shape Nagle's algorithm penalises: a
        // request whose last segment is partial waits for the peer's ACK, and
        // the peer's delayed-ACK timer holds that for tens of milliseconds. The
        // Java client and librdkafka both set this. Measuring rskafka, which
        // does not, showed the cost precisely — a reproducible ~41 ms per
        // request at one payload size, and 24 requests/s where neighbouring
        // sizes managed 20 000.
        stream
            .set_nodelay(true)
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok(stream)
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

/// The admin client on glommio. See [`kestrel_client::admin`].
pub type Admin = kestrel_client::Admin<Glommio>;
