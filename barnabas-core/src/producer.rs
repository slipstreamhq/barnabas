//! The idempotent, transactional producer — as a state machine over nothing.
//!
//! No sockets, no clock: this owns the *rules*, and the binding owns the
//! requests. That split exists because the rules are where exactly-once is won
//! or lost, and they are unusually hostile to testing against a live broker —
//! every failure mode here returns `Ok` on the wire.
//!
//! # The three rules that make idempotence real
//!
//! Kafka deduplicates by `(producer id, epoch, partition, sequence)`. That
//! gives the client three obligations, and getting any of them wrong is silent:
//!
//! 1. **Sequences continue across transactions.** They reset on an epoch bump
//!    and at no other time. Restarting them per transaction makes the broker
//!    treat the second transaction as a duplicate: it answers `Ok`, echoes the
//!    *original* base offset, and writes nothing — and the transaction then
//!    commits successfully with no data in it. Found against a real broker in
//!    P0; [`ProducerState::begin_transaction`] is why it cannot happen here.
//! 2. **A retry re-sends the same sequence.** Allocating a fresh one turns a
//!    retry into a second record. [`ProducerState::allocate`] hands out a
//!    [`SequenceRange`] that a retry replays rather than re-allocating.
//! 3. **A sequence error is fatal, not retriable.** `OUT_OF_ORDER_SEQUENCE` and
//!    `DUPLICATE_SEQUENCE` mean the stream is already wrong; retrying is how
//!    duplicates get written. See [`crate::ErrorCode::disposition`].
//!
//! # What "fatal" means here
//!
//! Once fenced or out of sequence, the producer refuses to do anything until it
//! is re-initialised with a new epoch. Refusing is the point: a fenced producer
//! that keeps writing is a split brain, and the whole reason Kafka has epochs.

use std::collections::{HashMap, HashSet};

/// A producer's identity, as the coordinator issued it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerIdentity {
    pub id: i64,
    pub epoch: i16,
}

/// Where a transactional producer is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TxnState {
    /// No `InitProducerId` yet.
    #[default]
    Uninitialised,
    /// Initialised, no transaction open.
    Ready,
    /// `begin_transaction` called; partitions may be enrolled and produced to.
    InTransaction,
    /// `EndTxn` sent, outcome not yet known.
    Ending,
    /// Fenced, or a sequence error. Nothing further is permitted.
    Fatal,
}

/// A sequence range reserved for one batch.
///
/// Held by the caller across a retry: re-sending the *same* range is what keeps
/// a retry idempotent, and is why this is a value rather than a counter the
/// caller reads twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequenceRange {
    pub base: i32,
    pub count: i32,
}

impl SequenceRange {
    /// One past the last sequence in this range.
    #[must_use]
    pub fn end(self) -> i32 {
        self.base + self.count
    }
}

/// What went wrong, in terms of what the caller did rather than what the wire
/// said.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProducerError {
    #[error("the producer is not initialised")]
    Uninitialised,

    #[error("no transaction is open")]
    NoTransaction,

    #[error("a transaction is already open")]
    TransactionAlreadyOpen,

    #[error("{topic}-{partition} was not added to the transaction")]
    NotEnrolled { topic: String, partition: i32 },

    #[error("the producer is fenced or out of sequence and must be re-initialised")]
    Fatal,
}

type PartitionKey = (String, i32);

/// The producer's rules, with no IO attached.
#[derive(Debug, Default)]
pub struct ProducerState {
    identity: Option<ProducerIdentity>,
    txn: TxnState,
    /// Next unused sequence per partition. **Not** cleared per transaction —
    /// see rule 1.
    sequences: HashMap<PartitionKey, i32>,
    /// Partitions added to the *current* transaction, so enrollment is sent
    /// once rather than per batch.
    enrolled: HashSet<PartitionKey>,
    /// Whether this producer is transactional at all. A plain idempotent
    /// producer keeps the sequence rules and skips the transaction ones.
    transactional: bool,
}

impl ProducerState {
    /// A transactional producer. Requires `InitProducerId` with a
    /// transactional id, and `begin_transaction` before producing.
    #[must_use]
    pub fn transactional() -> Self {
        Self {
            transactional: true,
            ..Self::default()
        }
    }

    /// An idempotent, non-transactional producer: sequence rules, no
    /// transaction state machine.
    #[must_use]
    pub fn idempotent() -> Self {
        Self {
            transactional: false,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn state(&self) -> TxnState {
        self.txn
    }

    #[must_use]
    pub fn identity(&self) -> Option<ProducerIdentity> {
        self.identity
    }

    /// Record a successful `InitProducerId`.
    ///
    /// **The one place sequences reset.** A new epoch fences everything written
    /// under the old one, so the broker expects sequences to start again — and
    /// only then.
    pub fn on_init_producer_id(&mut self, identity: ProducerIdentity) {
        let epoch_changed = self.identity.map(|i| i.epoch) != Some(identity.epoch)
            || self.identity.map(|i| i.id) != Some(identity.id);
        self.identity = Some(identity);
        if epoch_changed {
            self.sequences.clear();
        }
        self.enrolled.clear();
        self.txn = TxnState::Ready;
    }

    /// Open a transaction.
    ///
    /// Note what this does **not** do: it does not touch `sequences`. That is
    /// rule 1, and the reason this method is three lines rather than four.
    ///
    /// # Errors
    /// If uninitialised, already in a transaction, or fatal.
    pub fn begin_transaction(&mut self) -> Result<(), ProducerError> {
        self.check_usable()?;
        match self.txn {
            TxnState::Ready => {
                self.enrolled.clear();
                self.txn = TxnState::InTransaction;
                Ok(())
            }
            TxnState::InTransaction | TxnState::Ending => {
                Err(ProducerError::TransactionAlreadyOpen)
            }
            TxnState::Uninitialised => Err(ProducerError::Uninitialised),
            TxnState::Fatal => Err(ProducerError::Fatal),
        }
    }

    /// Whether `AddPartitionsToTxn` must be sent for this partition before
    /// producing to it. False once enrolled, so enrollment is per transaction
    /// rather than per batch.
    #[must_use]
    pub fn needs_enrollment(&self, topic: &str, partition: i32) -> bool {
        self.transactional
            && self.txn == TxnState::InTransaction
            && !self.enrolled.contains(&(topic.to_owned(), partition))
    }

    /// Record a successful `AddPartitionsToTxn`.
    pub fn on_enrolled(&mut self, topic: &str, partition: i32) {
        self.enrolled.insert((topic.to_owned(), partition));
    }

    /// Reserve `count` sequence numbers for a batch.
    ///
    /// The returned range is what the caller sends, and what it re-sends
    /// verbatim on a retry. The counter advances **here**, not on
    /// acknowledgement: an unacknowledged batch still occupies its sequences,
    /// because the broker may well have persisted it.
    ///
    /// # Errors
    /// If not usable, no transaction is open (when transactional), or the
    /// partition was not enrolled.
    pub fn allocate(
        &mut self,
        topic: &str,
        partition: i32,
        count: i32,
    ) -> Result<SequenceRange, ProducerError> {
        self.check_usable()?;
        if self.transactional {
            if self.txn != TxnState::InTransaction {
                return Err(ProducerError::NoTransaction);
            }
            if !self.enrolled.contains(&(topic.to_owned(), partition)) {
                return Err(ProducerError::NotEnrolled {
                    topic: topic.to_owned(),
                    partition,
                });
            }
        }
        let next = self
            .sequences
            .entry((topic.to_owned(), partition))
            .or_insert(0);
        let base = *next;
        *next += count;
        Ok(SequenceRange { base, count })
    }

    /// The next sequence this partition would allocate. For assertions and
    /// diagnostics; the producer's own path uses [`Self::allocate`].
    #[must_use]
    pub fn next_sequence(&self, topic: &str, partition: i32) -> i32 {
        self.sequences
            .get(&(topic.to_owned(), partition))
            .copied()
            .unwrap_or(0)
    }

    /// Begin committing or aborting.
    ///
    /// # Errors
    /// If no transaction is open.
    pub fn end_transaction(&mut self) -> Result<(), ProducerError> {
        self.check_usable()?;
        if self.txn != TxnState::InTransaction {
            return Err(ProducerError::NoTransaction);
        }
        self.txn = TxnState::Ending;
        Ok(())
    }

    /// Record that `EndTxn` succeeded.
    ///
    /// Back to `Ready`, enrollment cleared — and sequences **untouched**, which
    /// is rule 1 again and the single most important line in this file.
    pub fn on_end_transaction(&mut self) {
        if self.txn == TxnState::Ending {
            self.enrolled.clear();
            self.txn = TxnState::Ready;
        }
    }

    /// Fence the producer: nothing further until re-initialised.
    ///
    /// Called for `PRODUCER_FENCED`, `INVALID_PRODUCER_EPOCH`, and the sequence
    /// errors — everything [`crate::Disposition::Fatal`] covers on a producer
    /// path.
    pub fn fence(&mut self) {
        self.txn = TxnState::Fatal;
    }

    fn check_usable(&self) -> Result<(), ProducerError> {
        match self.txn {
            TxnState::Fatal => Err(ProducerError::Fatal),
            TxnState::Uninitialised if self.identity.is_none() => Err(ProducerError::Uninitialised),
            _ => Ok(()),
        }
    }
}

/// Did the broker silently deduplicate this batch?
///
/// A `Produce` response whose `base_offset` precedes the log's own append is
/// how a duplicate looks: the broker answers `Ok` and echoes where the
/// *original* copy landed. Every status code is green and nothing was written,
/// which is exactly the failure P0 hit — so the check is a named function with
/// a test rather than a comment somewhere.
///
/// `previous_high` is the highest base offset this producer has seen
/// acknowledged for the partition.
#[must_use]
pub fn looks_deduplicated(base_offset: i64, previous_high: Option<i64>) -> bool {
    match previous_high {
        Some(high) => base_offset <= high,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready_transactional() -> ProducerState {
        let mut p = ProducerState::transactional();
        p.on_init_producer_id(ProducerIdentity { id: 7, epoch: 0 });
        p
    }

    fn enrolled_in_transaction() -> ProducerState {
        let mut p = ready_transactional();
        p.begin_transaction().unwrap();
        p.on_enrolled("t", 0);
        p
    }

    /// **Rule 1, and the bug that cost P0 an afternoon.** A second transaction
    /// continues the sequence. If it restarted, the broker would answer `Ok`,
    /// echo the original base offset, write nothing, and commit an empty
    /// transaction — with no error anywhere.
    #[test]
    fn sequences_continue_across_transactions() {
        let mut p = enrolled_in_transaction();
        let first = p.allocate("t", 0, 3).unwrap();
        assert_eq!(first, SequenceRange { base: 0, count: 3 });

        p.end_transaction().unwrap();
        p.on_end_transaction();

        p.begin_transaction().unwrap();
        p.on_enrolled("t", 0);
        let second = p.allocate("t", 0, 2).unwrap();
        assert_eq!(
            second,
            SequenceRange { base: 3, count: 2 },
            "a new transaction restarted the sequence; the broker will \
             deduplicate it and commit an empty transaction"
        );
    }

    /// **Rule 2.** A retry re-sends the range it already has. The state machine
    /// enforces this by handing out a value: there is no way to "re-read" the
    /// counter and accidentally advance it.
    #[test]
    fn a_retry_reuses_its_range_and_does_not_advance() {
        let mut p = enrolled_in_transaction();
        let range = p.allocate("t", 0, 5).unwrap();
        let next_before = p.next_sequence("t", 0);
        // A retry sends `range` again. Nothing is re-allocated.
        assert_eq!(range.base, 0);
        assert_eq!(next_before, 5);
        assert_eq!(p.next_sequence("t", 0), 5, "a retry must not advance");
    }

    /// The counter advances at allocation, not acknowledgement: an
    /// unacknowledged batch may well have been persisted, so its sequences are
    /// spent either way.
    #[test]
    fn allocation_advances_even_without_acknowledgement() {
        let mut p = enrolled_in_transaction();
        p.allocate("t", 0, 4).unwrap();
        assert_eq!(p.next_sequence("t", 0), 4);
        let next = p.allocate("t", 0, 1).unwrap();
        assert_eq!(next.base, 4);
    }

    /// **The one legitimate reset.** A new epoch fences everything written
    /// under the old one.
    #[test]
    fn a_new_epoch_resets_sequences() {
        let mut p = enrolled_in_transaction();
        p.allocate("t", 0, 3).unwrap();
        assert_eq!(p.next_sequence("t", 0), 3);

        p.on_init_producer_id(ProducerIdentity { id: 7, epoch: 1 });
        assert_eq!(p.next_sequence("t", 0), 0);
    }

    /// Re-initialising to the *same* identity must not reset: that would be a
    /// silent duplicate-writer, since the broker still expects the old
    /// sequence.
    #[test]
    fn re_initialising_the_same_identity_keeps_sequences() {
        let mut p = enrolled_in_transaction();
        p.allocate("t", 0, 3).unwrap();
        p.on_init_producer_id(ProducerIdentity { id: 7, epoch: 0 });
        assert_eq!(p.next_sequence("t", 0), 3);
    }

    /// Sequences are per partition; one partition's traffic must not shift
    /// another's.
    #[test]
    fn sequences_are_per_partition() {
        let mut p = ready_transactional();
        p.begin_transaction().unwrap();
        p.on_enrolled("t", 0);
        p.on_enrolled("t", 1);
        p.allocate("t", 0, 3).unwrap();
        let other = p.allocate("t", 1, 1).unwrap();
        assert_eq!(other.base, 0);
    }

    /// **Rule 3.** A fenced producer refuses everything, because a fenced
    /// producer that keeps writing is a split brain.
    #[test]
    fn a_fenced_producer_refuses_everything() {
        let mut p = enrolled_in_transaction();
        p.fence();
        assert_eq!(p.begin_transaction(), Err(ProducerError::Fatal));
        assert_eq!(p.allocate("t", 0, 1), Err(ProducerError::Fatal));
        assert_eq!(p.end_transaction(), Err(ProducerError::Fatal));
    }

    /// Only re-initialisation clears it, and that means a new epoch from the
    /// coordinator.
    #[test]
    fn re_initialising_clears_the_fatal_state() {
        let mut p = enrolled_in_transaction();
        p.fence();
        p.on_init_producer_id(ProducerIdentity { id: 7, epoch: 1 });
        assert_eq!(p.state(), TxnState::Ready);
        assert!(p.begin_transaction().is_ok());
    }

    /// A partition must be enrolled before it is produced to, or the
    /// coordinator cannot fence it at commit time.
    #[test]
    fn producing_to_an_unenrolled_partition_is_refused() {
        let mut p = ready_transactional();
        p.begin_transaction().unwrap();
        assert_eq!(
            p.allocate("t", 0, 1),
            Err(ProducerError::NotEnrolled {
                topic: "t".to_owned(),
                partition: 0
            })
        );
    }

    /// Enrollment is per transaction: needed once at the start, and again after
    /// the transaction ends.
    #[test]
    fn enrollment_is_once_per_transaction() {
        let mut p = ready_transactional();
        p.begin_transaction().unwrap();
        assert!(p.needs_enrollment("t", 0));
        p.on_enrolled("t", 0);
        assert!(!p.needs_enrollment("t", 0));

        p.end_transaction().unwrap();
        p.on_end_transaction();
        p.begin_transaction().unwrap();
        assert!(
            p.needs_enrollment("t", 0),
            "a new transaction must enroll its partitions again"
        );
    }

    #[test]
    fn producing_outside_a_transaction_is_refused() {
        let mut p = ready_transactional();
        assert_eq!(p.allocate("t", 0, 1), Err(ProducerError::NoTransaction));
    }

    #[test]
    fn nesting_transactions_is_refused() {
        let mut p = ready_transactional();
        p.begin_transaction().unwrap();
        assert_eq!(
            p.begin_transaction(),
            Err(ProducerError::TransactionAlreadyOpen)
        );
    }

    #[test]
    fn an_uninitialised_producer_refuses_to_begin() {
        let mut p = ProducerState::transactional();
        assert_eq!(p.begin_transaction(), Err(ProducerError::Uninitialised));
    }

    /// An idempotent producer keeps the sequence rules and skips the
    /// transaction machinery entirely.
    #[test]
    fn an_idempotent_producer_needs_no_transaction() {
        let mut p = ProducerState::idempotent();
        p.on_init_producer_id(ProducerIdentity { id: 1, epoch: 0 });
        assert!(!p.needs_enrollment("t", 0));
        assert_eq!(p.allocate("t", 0, 2).unwrap().base, 0);
        assert_eq!(p.allocate("t", 0, 2).unwrap().base, 2);
    }

    /// A base offset at or below one already seen means the broker
    /// deduplicated: `Ok`, nothing written.
    #[test]
    fn a_repeated_base_offset_reads_as_deduplication() {
        assert!(!looks_deduplicated(0, None));
        assert!(!looks_deduplicated(10, Some(7)));
        assert!(looks_deduplicated(7, Some(7)));
        assert!(looks_deduplicated(3, Some(7)));
    }

    /// **The invariant, over an arbitrary interleaving.** Whatever order
    /// transactions, partitions and batch sizes come in, each partition's
    /// sequences must be contiguous from zero and never repeat — that is the
    /// whole of what the broker checks, so it is what the state machine must
    /// guarantee.
    #[test]
    fn sequences_are_contiguous_under_arbitrary_interleaving() {
        let mut p = ready_transactional();
        let partitions = [0, 1, 2];
        let mut expected: HashMap<i32, i32> = partitions.iter().map(|p| (*p, 0)).collect();

        // A deterministic but irregular schedule — no rand dependency, and a
        // failure reproduces exactly.
        let mut counter = 0u32;
        for txn in 0..5 {
            p.begin_transaction().unwrap();
            for step in 0..7 {
                counter = counter.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                let partition = partitions[(counter >> 16) as usize % partitions.len()];
                let count = i32::try_from((counter >> 8) % 4 + 1).unwrap();

                if p.needs_enrollment("t", partition) {
                    p.on_enrolled("t", partition);
                }
                let range = p.allocate("t", partition, count).unwrap();

                let want = expected.get_mut(&partition).unwrap();
                assert_eq!(
                    range.base, *want,
                    "txn {txn} step {step}: partition {partition} sequence jumped"
                );
                *want += count;
            }
            p.end_transaction().unwrap();
            p.on_end_transaction();
        }

        for (partition, want) in expected {
            assert_eq!(p.next_sequence("t", partition), want);
        }
    }
}
