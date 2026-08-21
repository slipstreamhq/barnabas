//! Assign-only consumer state: fetch positions, and READ_COMMITTED filtering.
//!
//! There is no consumer group protocol here and there will not be one. Callers
//! assign partitions themselves — in Slipstream's case a vnode owns partitions
//! and a lease decides which node owns the vnode — so there is no `JoinGroup`,
//! no heartbeat, no rebalance, and no generation fencing. That is the hardest
//! half of a Kafka client, and it is out of scope by construction.
//!
//! # READ_COMMITTED is the client's job
//!
//! P0 found this against a real broker, and it is worth stating plainly because
//! the API's shape suggests otherwise: setting `isolation_level = 1` does
//! **not** make the broker withhold aborted records. It returns them, together
//! with a list of aborted transactions and a last-stable-offset, and the
//! consumer filters. A client that only sets the flag hands aborted data to its
//! caller and reports it as committed — with no error anywhere, which is the
//! silent exactly-once failure this crate is most concerned with.
//!
//! [`filter`] implements the three rules, and its tests are the specification.

use std::collections::HashSet;

use kafka_protocol::records::Record;

/// Whether aborted records are visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Everything the broker has, aborted transactions included.
    ReadUncommitted,
    /// Only committed data, and only below the last stable offset.
    ReadCommitted,
}

impl IsolationLevel {
    /// The wire value for `FetchRequest::isolation_level`.
    #[must_use]
    pub fn as_i8(self) -> i8 {
        match self {
            Self::ReadUncommitted => 0,
            Self::ReadCommitted => 1,
        }
    }
}

/// An aborted transaction, as the broker reports it in a fetch response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortedTransaction {
    pub producer_id: i64,
    pub first_offset: i64,
}

/// Where a consumer is in one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPosition {
    pub topic: String,
    pub partition: i32,
    /// The offset the next fetch asks for.
    pub next_offset: i64,
}

impl FetchPosition {
    #[must_use]
    pub fn new(topic: impl Into<String>, partition: i32, next_offset: i64) -> Self {
        Self {
            topic: topic.into(),
            partition,
            next_offset,
        }
    }
}

/// What a fetch yielded, after filtering.
#[derive(Debug, Default)]
pub struct Fetched {
    /// Records the caller may have.
    pub records: Vec<Record>,
    /// Where the next fetch should start.
    ///
    /// **Advances past filtered records too**, which is not a detail: a fetch
    /// whose every record was aborted must still make progress, or the consumer
    /// re-requests the same offset forever and stalls on a partition that is
    /// perfectly healthy.
    pub next_offset: i64,
}

/// Kafka's control-record types, read from a control record's key.
///
/// The key of a control record is a 2-byte version followed by a 2-byte type.
const CONTROL_ABORT: i16 = 0;

fn control_type(record: &Record) -> Option<i16> {
    let key = record.key.as_ref()?;
    if key.len() < 4 {
        return None;
    }
    Some(i16::from_be_bytes([key[2], key[3]]))
}

/// Apply the READ_COMMITTED rules to one partition's records.
///
/// `records` must be in offset order, which is how the broker sends them.
/// `last_stable_offset` is the partition's LSO from the same fetch response.
///
/// Three rules:
/// 1. **Nothing at or past the LSO.** Above it, the outcome of a transaction is
///    not yet decided.
/// 2. **Control records are never data.** They are the commit and abort markers
///    themselves, and they are dropped under *both* isolation levels — a caller
///    asking for READ_UNCOMMITTED wants uncommitted records, not protocol
///    machinery.
/// 3. **Nothing below `fetch_offset`.** The broker returns whole record
///    batches, so a fetch from the middle of a batch comes back with the
///    records before it too. Dropping them is the client's job; a consumer that
///    forgets re-delivers records it has already emitted, which for a
///    checkpointing caller is a duplicate after every restore.
/// 4. **Records from an aborted transaction are dropped**, over the range from
///    that transaction's `first_offset` to the producer's abort marker. The
///    range matters: one producer can interleave an aborted and a committed
///    transaction within a single fetch response, and a rule that drops
///    everything from an aborted producer after `first_offset` would silently
///    discard the committed records that follow.
#[must_use]
pub fn filter(
    records: Vec<Record>,
    aborted: &[AbortedTransaction],
    last_stable_offset: i64,
    isolation: IsolationLevel,
    fetch_offset: i64,
) -> Fetched {
    let mut sorted: Vec<AbortedTransaction> = aborted.to_vec();
    sorted.sort_by_key(|a| a.first_offset);
    let mut pending = sorted.into_iter().peekable();

    // Producers whose transaction is open-and-aborted at the current offset.
    let mut aborted_producers: HashSet<i64> = HashSet::new();

    let read_committed = isolation == IsolationLevel::ReadCommitted;

    // **The common case is that nothing is dropped**, and moving a million
    // records into a second `Vec` to discover that is most of the cost of
    // consuming. A scan that touches only three fields per record decides it
    // without moving anything, and hands the input straight back.
    //
    // The conditions are exactly the four rules below, negated: no aborted
    // ranges to track, nothing above the LSO to withhold, no control records to
    // strip, and nothing below `fetch_offset` to skip.
    if aborted.is_empty()
        && !records.iter().any(|r| {
            r.control
                || r.offset < fetch_offset
                || (read_committed && r.offset >= last_stable_offset)
        })
    {
        let next_offset = records.last().map_or(fetch_offset, |r| r.offset + 1);
        return Fetched {
            records,
            next_offset,
        };
    }

    let mut kept = Vec::with_capacity(records.len());
    let mut next_offset = fetch_offset;

    for record in records {
        if read_committed && record.offset >= last_stable_offset {
            // Rule 1. Everything after this is also above the LSO, so stop —
            // and do not advance past it.
            break;
        }

        // Progress is recorded for every record the broker sent, whether or not
        // the caller gets to see it. See `Fetched::next_offset`.
        next_offset = record.offset + 1;

        // Rule 4, first half: a transaction becomes aborted at its first
        // offset. Done before the `fetch_offset` skip below, so a transaction
        // that began in an earlier batch is still known to be aborted.
        while pending
            .peek()
            .is_some_and(|a| a.first_offset <= record.offset)
        {
            let a = pending.next().expect("peeked");
            aborted_producers.insert(a.producer_id);
        }

        if record.control {
            // Rule 4, second half: the abort marker closes the range, so a
            // later transaction from the same producer is judged on its own.
            if control_type(&record) == Some(CONTROL_ABORT) {
                aborted_producers.remove(&record.producer_id);
            }
            // Rule 2.
            continue;
        }

        if read_committed && record.transactional && aborted_producers.contains(&record.producer_id)
        {
            continue;
        }

        // Rule 3, applied last so the aborted-range bookkeeping above still
        // sees every record the broker sent.
        if record.offset < fetch_offset {
            continue;
        }

        kept.push(record);
    }

    Fetched {
        records: kept,
        next_offset,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use kafka_protocol::records::TimestampType;

    fn record(offset: i64, producer_id: i64, transactional: bool) -> Record {
        Record {
            transactional,
            control: false,
            partition_leader_epoch: 0,
            producer_id,
            producer_epoch: 0,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: offset as i32,
            timestamp: 0,
            key: Some(Bytes::from(format!("k{offset}"))),
            value: Some(Bytes::from(format!("v{offset}"))),
            headers: Default::default(),
        }
    }

    /// A marker as the broker writes it: control record whose key is
    /// `[version:i16][type:i16]`.
    fn marker(offset: i64, producer_id: i64, control_type: i16) -> Record {
        let mut key = Vec::new();
        key.extend_from_slice(&0i16.to_be_bytes());
        key.extend_from_slice(&control_type.to_be_bytes());
        Record {
            transactional: true,
            control: true,
            partition_leader_epoch: 0,
            producer_id,
            producer_epoch: 0,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: 0,
            timestamp: 0,
            key: Some(Bytes::from(key)),
            value: None,
            headers: Default::default(),
        }
    }

    const ABORT: i16 = 0;
    const COMMIT: i16 = 1;

    fn offsets(f: &Fetched) -> Vec<i64> {
        f.records.iter().map(|r| r.offset).collect()
    }

    /// The plain case: non-transactional data passes through untouched.
    #[test]
    fn plain_records_pass_through() {
        let recs = vec![record(0, -1, false), record(1, -1, false)];
        let out = filter(recs, &[], 2, IsolationLevel::ReadCommitted, 0);
        assert_eq!(offsets(&out), vec![0, 1]);
        assert_eq!(out.next_offset, 2);
    }

    /// **The bug P0 hit.** Aborted records must not reach the caller.
    #[test]
    fn aborted_records_are_dropped_under_read_committed() {
        let recs = vec![record(0, 7, true), record(1, 7, true), marker(2, 7, ABORT)];
        let aborted = [AbortedTransaction {
            producer_id: 7,
            first_offset: 0,
        }];
        let out = filter(recs, &aborted, 3, IsolationLevel::ReadCommitted, 0);
        assert!(out.records.is_empty(), "aborted data reached the caller");
    }

    /// A fetch that is entirely aborted must still advance, or the consumer
    /// re-requests the same offset forever.
    #[test]
    fn an_entirely_aborted_fetch_still_makes_progress() {
        let recs = vec![record(10, 7, true), marker(11, 7, ABORT)];
        let aborted = [AbortedTransaction {
            producer_id: 7,
            first_offset: 10,
        }];
        let out = filter(recs, &aborted, 12, IsolationLevel::ReadCommitted, 10);
        assert!(out.records.is_empty());
        assert_eq!(
            out.next_offset, 12,
            "a fully-filtered fetch must advance the position"
        );
    }

    /// **The case a naive rule gets wrong**, and the reason the abort marker
    /// closes the range: one producer, an aborted transaction followed by a
    /// committed one, in a single fetch response.
    #[test]
    fn a_committed_transaction_after_an_aborted_one_survives() {
        let recs = vec![
            record(0, 7, true),  // aborted
            record(1, 7, true),  // aborted
            marker(2, 7, ABORT), // closes the aborted range
            record(3, 7, true),  // committed
            record(4, 7, true),  // committed
            marker(5, 7, COMMIT),
        ];
        let aborted = [AbortedTransaction {
            producer_id: 7,
            first_offset: 0,
        }];
        let out = filter(recs, &aborted, 6, IsolationLevel::ReadCommitted, 0);
        assert_eq!(
            offsets(&out),
            vec![3, 4],
            "the committed transaction after an abort was discarded"
        );
        assert_eq!(out.next_offset, 6);
    }

    /// Two producers interleaved: only the aborted one's records go.
    #[test]
    fn only_the_aborted_producer_is_filtered() {
        let recs = vec![
            record(0, 7, true),
            record(1, 8, true),
            record(2, 7, true),
            record(3, 8, true),
            marker(4, 7, ABORT),
            marker(5, 8, COMMIT),
        ];
        let aborted = [AbortedTransaction {
            producer_id: 7,
            first_offset: 0,
        }];
        let out = filter(recs, &aborted, 6, IsolationLevel::ReadCommitted, 0);
        assert_eq!(offsets(&out), vec![1, 3]);
    }

    /// Rule 1: nothing at or past the last stable offset, and the position does
    /// not advance past it either — those records are re-fetched once their
    /// transaction resolves.
    #[test]
    fn records_at_or_past_the_lso_are_withheld() {
        let recs = vec![record(0, -1, false), record(1, 9, true), record(2, 9, true)];
        let out = filter(recs, &[], 1, IsolationLevel::ReadCommitted, 0);
        assert_eq!(offsets(&out), vec![0]);
        assert_eq!(
            out.next_offset, 1,
            "the position must not advance past the LSO"
        );
    }

    /// Rule 2 holds under both isolation levels: markers are protocol
    /// machinery, never data.
    #[test]
    fn control_records_are_never_returned() {
        for isolation in [
            IsolationLevel::ReadCommitted,
            IsolationLevel::ReadUncommitted,
        ] {
            let recs = vec![record(0, 7, true), marker(1, 7, COMMIT)];
            let out = filter(recs, &[], 2, isolation, 0);
            assert_eq!(offsets(&out), vec![0], "isolation {isolation:?}");
        }
    }

    /// READ_UNCOMMITTED means what it says: aborted records are visible, and
    /// the LSO does not apply.
    #[test]
    fn read_uncommitted_sees_aborted_records() {
        let recs = vec![record(0, 7, true), record(1, 7, true)];
        let aborted = [AbortedTransaction {
            producer_id: 7,
            first_offset: 0,
        }];
        let out = filter(recs, &aborted, 0, IsolationLevel::ReadUncommitted, 0);
        assert_eq!(offsets(&out), vec![0, 1]);
    }

    /// The aborted list arrives in whatever order the broker chose; the filter
    /// sorts it rather than assuming.
    #[test]
    fn the_aborted_list_need_not_be_sorted() {
        let recs = vec![
            record(0, 7, true),
            marker(1, 7, ABORT),
            record(2, 8, true),
            marker(3, 8, ABORT),
        ];
        let aborted = [
            AbortedTransaction {
                producer_id: 8,
                first_offset: 2,
            },
            AbortedTransaction {
                producer_id: 7,
                first_offset: 0,
            },
        ];
        let out = filter(recs, &aborted, 4, IsolationLevel::ReadCommitted, 0);
        assert!(out.records.is_empty());
    }

    /// **The broker returns whole batches.** A fetch from the middle of one
    /// comes back with the earlier records too, and returning them would
    /// re-deliver data the caller has already seen.
    #[test]
    fn records_below_the_fetch_offset_are_dropped() {
        let recs = vec![
            record(0, -1, false),
            record(1, -1, false),
            record(2, -1, false),
        ];
        let out = filter(recs, &[], 3, IsolationLevel::ReadCommitted, 2);
        assert_eq!(offsets(&out), vec![2]);
        assert_eq!(out.next_offset, 3);
    }

    /// The skip must not lose the aborted-range bookkeeping: a transaction that
    /// began before the fetch offset is still aborted after it.
    #[test]
    fn an_abort_beginning_before_the_fetch_offset_still_applies() {
        let recs = vec![
            record(0, 7, true),
            record(1, 7, true),
            record(2, 7, true),
            marker(3, 7, ABORT),
        ];
        let aborted = [AbortedTransaction {
            producer_id: 7,
            first_offset: 0,
        }];
        let out = filter(recs, &aborted, 4, IsolationLevel::ReadCommitted, 2);
        assert!(
            out.records.is_empty(),
            "an abort that began before the fetch offset was forgotten"
        );
    }

    /// An empty fetch leaves the position where it was.
    #[test]
    fn an_empty_fetch_does_not_move_the_position() {
        let out = filter(Vec::new(), &[], 5, IsolationLevel::ReadCommitted, 5);
        assert!(out.records.is_empty());
        assert_eq!(out.next_offset, 5);
    }
}
