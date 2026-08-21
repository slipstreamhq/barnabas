//! Choosing a partition for a keyed record.
//!
//! # There is no single "Kafka default"
//!
//! The two clients everyone actually uses disagree, and a client that picks the
//! wrong one silently relocates every key:
//!
//! - **The Java client** hashes with **murmur2** (`Utils.murmur2`), takes
//!   `hash & 0x7fffffff`, and takes that modulo the partition count.
//! - **librdkafka** — and therefore `rdkafka`, and therefore every Rust program
//!   using it today — defaults to `consistent_random`, which hashes with
//!   **CRC-32** for keyed records and picks at random for null keys.
//!
//! Same key, same topic, different partition. Nothing errors; the data simply
//! lands somewhere else, which for a consumer that assigns partitions itself
//! means it reads a different subset than it used to.
//!
//! So this offers both and makes the caller choose. [`Partitioner::Crc32`] is
//! the one that keeps a program's placement identical when it migrates off
//! `rdkafka`, which is the migration this crate exists to make possible.
//!
//! # Null keys
//!
//! Both clients spread null-keyed records across partitions rather than
//! concentrating them. This uses a round robin rather than a random choice: it
//! spreads just as evenly, and it makes a test reproducible.

/// Which hash decides a keyed record's partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Partitioner {
    /// CRC-32, as librdkafka's `consistent_random` does. **The default here**,
    /// because it is what a program migrating off `rdkafka` already has.
    #[default]
    Crc32,
    /// murmur2, as the Java client does.
    Murmur2,
}

impl Partitioner {
    /// The partition for `key` in a topic with `partition_count` partitions.
    ///
    /// `next` is the round-robin counter used for null keys; it is advanced
    /// only when it is used.
    ///
    /// Returns `None` when `partition_count` is not positive. **A library must
    /// not panic on a number that came off the network**: a topic mid-creation
    /// reports zero partitions, and taking down the caller's process for it is
    /// not a reasonable answer to a transient cluster state.
    #[must_use]
    pub fn partition_for(
        self,
        key: Option<&[u8]>,
        partition_count: i32,
        next: &mut u32,
    ) -> Option<i32> {
        if partition_count <= 0 {
            return None;
        }
        let count = u32::try_from(partition_count).ok()?;

        let Some(key) = key else {
            let chosen = *next % count;
            *next = next.wrapping_add(1);
            return i32::try_from(chosen).ok();
        };

        let slot = match self {
            // librdkafka: `rd_crc32(key, len) % partition_cnt`, on the unsigned
            // value.
            Self::Crc32 => crc32(key) % count,
            // Java: `toPositive(murmur2(key)) % numPartitions`, where
            // `toPositive` masks the sign bit rather than taking an absolute
            // value — `abs(i32::MIN)` would overflow.
            Self::Murmur2 => {
                let h = murmur2(key) & 0x7fff_ffff;
                u32::try_from(h).ok()? % count
            }
        };
        i32::try_from(slot).ok()
    }
}

/// CRC-32 (IEEE 802.3, reflected), which is what librdkafka's `rd_crc32` is.
///
/// Bitwise rather than table-driven: a partition is computed once per record
/// key, not per byte of payload, so the table is not worth the static.
#[must_use]
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// murmur2 as Kafka's `Utils.murmur2` computes it.
///
/// Transcribed with Java's arithmetic made explicit: `>>>` is a logical shift,
/// and every multiply and add wraps. Getting either wrong produces a hash that
/// looks plausible and places keys wrongly.
#[must_use]
#[allow(clippy::cast_possible_wrap)]
pub fn murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let length = data.len();
    let mut h: u32 = SEED ^ (length as u32);

    let blocks = length / 4;
    for i in 0..blocks {
        let i4 = i * 4;
        let mut k = u32::from(data[i4])
            | (u32::from(data[i4 + 1]) << 8)
            | (u32::from(data[i4 + 2]) << 16)
            | (u32::from(data[i4 + 3]) << 24);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    // The tail, with Java's deliberate fall-through.
    let tail = length & !3;
    match length % 4 {
        3 => {
            h ^= u32::from(data[tail + 2]) << 16;
            h ^= u32::from(data[tail + 1]) << 8;
            h ^= u32::from(data[tail]);
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= u32::from(data[tail + 1]) << 8;
            h ^= u32::from(data[tail]);
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= u32::from(data[tail]);
            h = h.wrapping_mul(M);
        }
        _ => {}
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;

    h as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The standard CRC-32 check value. If this passes, the polynomial,
    /// reflection, initial value and final xor are all right — which is more
    /// than a hand-rolled test of our own could establish.
    #[test]
    fn crc32_matches_the_standard_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn crc32_of_empty_is_zero() {
        assert_eq!(crc32(b""), 0);
    }

    /// murmur2 has no published check vector, so this pins the two properties
    /// that a transcription error breaks: the tail path (lengths 1–3 take
    /// different branches) and stability.
    ///
    /// The values that matter are checked against a *real broker* in
    /// `slipstream-kafka`'s parity test, which is the only authority available
    /// — see this module's docs.
    #[test]
    fn murmur2_is_stable_across_tail_lengths() {
        let hashes: Vec<i32> = ["", "a", "ab", "abc", "abcd", "abcde"]
            .iter()
            .map(|s| murmur2(s.as_bytes()))
            .collect();
        // Distinct: a tail branch that fell through wrongly would collide.
        for i in 0..hashes.len() {
            for j in (i + 1)..hashes.len() {
                assert_ne!(hashes[i], hashes[j], "murmur2 collided on short keys");
            }
        }
        // Stable across calls.
        assert_eq!(murmur2(b"abc"), hashes[3]);
    }

    /// The same key always lands on the same partition — the property that
    /// makes keyed streams work at all.
    #[test]
    fn a_key_is_stable() {
        for p in [Partitioner::Crc32, Partitioner::Murmur2] {
            let mut counter = 0;
            let first = p.partition_for(Some(b"user-42"), 12, &mut counter);
            assert!(first.is_some());
            for _ in 0..10 {
                assert_eq!(p.partition_for(Some(b"user-42"), 12, &mut counter), first);
            }
        }
    }

    /// Every partition is reachable, and none is out of range.
    #[test]
    fn partitions_are_in_range_and_spread() {
        for p in [Partitioner::Crc32, Partitioner::Murmur2] {
            let mut counter = 0;
            let mut seen = std::collections::HashSet::new();
            for i in 0..2000 {
                let key = format!("key-{i}");
                let part = p
                    .partition_for(Some(key.as_bytes()), 8, &mut counter)
                    .expect("a positive partition count");
                assert!((0..8).contains(&part), "{p:?} produced partition {part}");
                seen.insert(part);
            }
            assert_eq!(seen.len(), 8, "{p:?} never used some partitions");
        }
    }

    /// The two disagree, which is the entire reason both exist. If this ever
    /// starts passing as equality, one of them is wrong.
    #[test]
    fn the_two_partitioners_disagree() {
        let mut c1 = 0;
        let mut c2 = 0;
        let differing = (0..100).filter(|i| {
            let key = format!("key-{i}");
            Partitioner::Crc32.partition_for(Some(key.as_bytes()), 16, &mut c1)
                != Partitioner::Murmur2.partition_for(Some(key.as_bytes()), 16, &mut c2)
        });
        assert!(
            differing.count() > 50,
            "the CRC-32 and murmur2 partitioners agreed suspiciously often"
        );
    }

    /// Null keys round-robin rather than piling onto one partition.
    #[test]
    fn null_keys_are_spread() {
        let mut counter = 0;
        let picks: Vec<i32> = (0..6)
            .map(|_| {
                Partitioner::Crc32
                    .partition_for(None, 3, &mut counter)
                    .expect("a positive partition count")
            })
            .collect();
        assert_eq!(picks, vec![0, 1, 2, 0, 1, 2]);
    }

    /// The counter advances only for null keys — a keyed record must not shift
    /// where the next null-keyed one lands, or the round robin becomes
    /// dependent on unrelated traffic.
    #[test]
    fn a_keyed_record_does_not_advance_the_round_robin() {
        let mut counter = 0;
        let _ = Partitioner::Crc32.partition_for(Some(b"k"), 4, &mut counter);
        assert_eq!(counter, 0);
        let _ = Partitioner::Crc32.partition_for(None, 4, &mut counter);
        assert_eq!(counter, 1);
    }

    /// A topic mid-creation reports zero partitions. That is a transient
    /// cluster state, so it is `None` rather than a panic in the caller's
    /// process.
    #[test]
    fn a_topic_with_no_partitions_yields_no_partition() {
        let mut counter = 0;
        assert_eq!(
            Partitioner::Crc32.partition_for(Some(b"k"), 0, &mut counter),
            None
        );
        assert_eq!(
            Partitioner::Crc32.partition_for(None, -1, &mut counter),
            None
        );
    }
}
