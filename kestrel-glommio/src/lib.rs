//! The glommio binding: sockets and timers for [`kestrel_core`].
//!
//! Everything that touches a file descriptor lives here. The protocol logic —
//! framing, correlation, READ_COMMITTED filtering, the cluster map — is in the
//! core and is shared with every other binding, so a bug in it cannot be
//! arm-specific.
//!
//! # `!Send` on purpose
//!
//! A [`Consumer`] holds `glommio::net::TcpStream`s, which belong to the core
//! that opened them, so the handle is `!Send` and must not be moved between
//! executors. That is the point rather than a limitation: the core is neither
//! `Send` nor `!Send`, so a work-stealing binding can make the opposite choice
//! without either of them compromising.
//!
//! # Scope
//!
//! Assign-only consumer, with leader routing and a metadata cache. No producer
//! — that is P2, and a sketch of one here would make the shape harder to
//! change rather than easier.

pub mod cluster;
pub mod consumer;

pub use cluster::Cluster;
pub use consumer::{Consumer, EARLIEST, LATEST};
pub use kestrel_core::IsolationLevel;

use kestrel_core::{Disposition, ErrorCode};

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

    /// The partition has no leader even after a metadata refresh — what a
    /// partition mid-election looks like. Separate from [`Self::Broker`] so a
    /// caller can back off and retry rather than treat it as fatal.
    #[error("{topic}-{partition} has no leader")]
    NoLeader { topic: String, partition: i32 },

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
