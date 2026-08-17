//! A Kafka client core with no IO, no runtime, and no clock.
//!
//! Everything here is a state machine over bytes. Nothing opens a socket, waits
//! on a timer, or spawns a task — a caller feeds it received bytes and drains
//! the bytes it wants sent. Binding crates (`kestrel-glommio`, and later
//! `kestrel-tokio`) supply the sockets.
//!
//! # Why
//!
//! An abstraction spanning async runtimes is lowest-common-denominator, and LCD
//! requires `Send` — which forbids exactly the per-core, `!Send` design a
//! thread-per-core runtime exists for. A sans-io core sidesteps the argument by
//! naming no runtime at all: the *binding* decides `Send`-ness, so one state
//! machine serves both a per-core `Rc` handle and a work-stealing one.
//!
//! The second reason is testing, and in a Kafka client it is the bigger one.
//! Exactly-once bugs are silent — a wrong retry duplicates records and every
//! status code stays green — so they have to be caught by driving the state
//! machine adversarially rather than by watching a broker behave. A core with
//! no IO can be driven that way in a unit test, deterministically, with no
//! broker and no executor. Every test in this crate is one.
//!
//! # Layout
//!
//! - [`frame`] — Kafka's length-prefixed framing, the one place partial reads
//!   are handled.
//! - [`conn`] — request/response correlation over a single broker connection.
//! - [`consumer`] — assign-only fetch positions and READ_COMMITTED filtering.
//! - [`metadata`] — the cluster map, and knowing when it is stale.
//! - [`partitioner`] — which partition a keyed record lands on, and why the
//!   answer differs between Kafka clients.
//! - [`producer`] — idempotent sequencing and the transaction state machine.

pub mod conn;
pub mod consumer;
pub mod frame;
pub mod group;
pub mod member;
pub mod metadata;
pub mod partitioner;
pub mod producer;
pub mod records;

pub use conn::{Connection, PendingResponse};
pub use member::{GroupMember, MemberState, Step};
pub use group::{Assignment, Assignor, RangeAssignor, RoundRobinAssignor, StickyAssignor, Subscription, TopicPartition};
pub use consumer::{FetchPosition, IsolationLevel};
pub use metadata::{BrokerAddr, Metadata};
pub use partitioner::Partitioner;
pub use producer::{ProducerIdentity, ProducerState, SequenceRange, TxnState};

/// Everything that can go wrong in the core.
///
/// Deliberately small and deliberately *not* an alias for the protocol crate's
/// error: a caller distinguishes "the peer sent something impossible" (fatal,
/// reconnect) from "the broker answered with an error code" (which is
/// [`ErrorCode`]'s business and often retriable).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("protocol encode/decode: {0}")]
    Codec(String),

    /// A frame arrived that this connection did not ask for. Fatal: the stream
    /// is no longer interpretable, so the connection must be dropped rather
    /// than resynchronised.
    #[error("unexpected correlation id {got}, expected {expected}")]
    Correlation { got: i32, expected: i32 },

    /// A response arrived with no request outstanding.
    #[error("response with no request in flight")]
    Unsolicited,

    /// A frame longer than the caller's limit. Guards against a hostile or
    /// corrupt peer steering us into an enormous allocation.
    #[error("frame of {len} bytes exceeds the {limit} byte limit")]
    FrameTooLarge { len: usize, limit: usize },
}

pub type Result<T> = std::result::Result<T, Error>;

/// A broker error code, classified.
///
/// **This classification is the client.** P0 found that a cold cluster answers
/// `FindCoordinator` with 15, then 14, then 16 — all of them states of a
/// healthy cluster rather than failures — so a client that treats a non-zero
/// code as an error cannot start a transaction at all. The taxonomy is
/// therefore load-bearing from the first request, not late hardening.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorCode(pub i16);

/// What a caller should *do* about an error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// No error.
    Ok,
    /// Retry the same request against the same broker after a backoff.
    Retry,
    /// Refresh metadata — the partition leader moved — then retry.
    RefreshMetadata,
    /// Re-run `FindCoordinator` before retrying.
    ///
    /// Distinct from [`Self::Retry`] because the coordinator genuinely moves:
    /// P0 saw `NOT_COORDINATOR` *after* a successful discovery, and a client
    /// that retries in place spins against a broker that will never answer.
    FindCoordinator,
    /// Unrecoverable for this producer or consumer. Fencing, authorization,
    /// and the sequence errors that mean the stream is already wrong.
    Fatal,
}

impl ErrorCode {
    pub const NONE: Self = Self(0);
    pub const OFFSET_OUT_OF_RANGE: Self = Self(1);
    /// The topic or partition is not (yet) known to this broker. Transient
    /// while a topic is being auto-created, and part of Kafka's
    /// invalid-metadata family — so it refreshes rather than failing.
    pub const UNKNOWN_TOPIC_OR_PARTITION: Self = Self(3);
    pub const LEADER_NOT_AVAILABLE: Self = Self(5);
    pub const NOT_LEADER_OR_FOLLOWER: Self = Self(6);
    pub const REQUEST_TIMED_OUT: Self = Self(7);
    pub const COORDINATOR_LOAD_IN_PROGRESS: Self = Self(14);
    pub const COORDINATOR_NOT_AVAILABLE: Self = Self(15);
    pub const NOT_COORDINATOR: Self = Self(16);
    pub const OUT_OF_ORDER_SEQUENCE_NUMBER: Self = Self(45);
    pub const DUPLICATE_SEQUENCE_NUMBER: Self = Self(46);
    pub const INVALID_PRODUCER_EPOCH: Self = Self(47);
    /// The previous transaction's markers are still being written. Starting a
    /// second transaction immediately after ending the first hits this every
    /// time — found while writing P1's broker tests.
    pub const CONCURRENT_TRANSACTIONS: Self = Self(51);
    pub const PRODUCER_FENCED: Self = Self(90);

    #[must_use]
    pub fn is_ok(self) -> bool {
        self == Self::NONE
    }

    /// How to react.
    ///
    /// Unknown codes are [`Disposition::Fatal`] on purpose. Guessing "probably
    /// retriable" for a code we have never seen is how a client retries an
    /// operation that already partially succeeded — which, for a producer, is
    /// how records get duplicated.
    #[must_use]
    pub fn disposition(self) -> Disposition {
        match self {
            Self::NONE => Disposition::Ok,
            Self::LEADER_NOT_AVAILABLE
            | Self::NOT_LEADER_OR_FOLLOWER
            | Self::UNKNOWN_TOPIC_OR_PARTITION => Disposition::RefreshMetadata,
            Self::COORDINATOR_LOAD_IN_PROGRESS | Self::COORDINATOR_NOT_AVAILABLE => {
                Disposition::Retry
            }
            Self::NOT_COORDINATOR => Disposition::FindCoordinator,
            Self::REQUEST_TIMED_OUT | Self::CONCURRENT_TRANSACTIONS => Disposition::Retry,
            _ => Disposition::Fatal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The three codes P0 met on a cold cluster, and the distinction that
    /// matters: 16 is not a plain retry.
    #[test]
    fn coordinator_warmup_codes_are_not_fatal() {
        assert_eq!(
            ErrorCode::COORDINATOR_NOT_AVAILABLE.disposition(),
            Disposition::Retry
        );
        assert_eq!(
            ErrorCode::COORDINATOR_LOAD_IN_PROGRESS.disposition(),
            Disposition::Retry
        );
        assert_eq!(
            ErrorCode::NOT_COORDINATOR.disposition(),
            Disposition::FindCoordinator,
            "NOT_COORDINATOR must re-discover: the coordinator moves, and \
             retrying in place spins against a broker that will never answer"
        );
    }

    /// A sequence error means the stream is already wrong. Retrying it is how
    /// duplicates get written, so it must never be classified as retriable.
    #[test]
    fn sequence_and_fencing_errors_are_fatal() {
        for code in [
            ErrorCode::OUT_OF_ORDER_SEQUENCE_NUMBER,
            ErrorCode::DUPLICATE_SEQUENCE_NUMBER,
            ErrorCode::INVALID_PRODUCER_EPOCH,
            ErrorCode::PRODUCER_FENCED,
        ] {
            assert_eq!(code.disposition(), Disposition::Fatal, "code {}", code.0);
        }
    }

    /// Starting a transaction right after ending one is a normal thing to do,
    /// and it must not be a failure.
    #[test]
    fn concurrent_transactions_is_retriable() {
        assert_eq!(
            ErrorCode::CONCURRENT_TRANSACTIONS.disposition(),
            Disposition::Retry
        );
    }

    /// A partition that has just been created reports this even after metadata
    /// named a leader, so it must refresh rather than fail.
    #[test]
    fn not_leader_refreshes_metadata() {
        assert_eq!(
            ErrorCode::NOT_LEADER_OR_FOLLOWER.disposition(),
            Disposition::RefreshMetadata
        );
    }

    /// A topic being auto-created answers 3 for a moment. Treating it as fatal
    /// makes the first write to a new topic fail, which is what happened when
    /// the sink moved onto this client.
    #[test]
    fn an_unknown_topic_refreshes_metadata() {
        assert_eq!(
            ErrorCode::UNKNOWN_TOPIC_OR_PARTITION.disposition(),
            Disposition::RefreshMetadata
        );
    }

    /// An unknown code is fatal rather than optimistically retried.
    #[test]
    fn unknown_codes_are_fatal() {
        assert_eq!(ErrorCode(9999).disposition(), Disposition::Fatal);
    }
}
