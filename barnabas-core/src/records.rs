//! A record-batch reader that does not build a record per record.
//!
//! # Why this exists
//!
//! `kafka_protocol`'s decoder produces a `Vec<Record>`, and each `Record`
//! carries producer id, producer epoch, sequence, partition leader epoch,
//! timestamp type and two flags — **every one of which is a property of the
//! batch, copied into each of its records** — plus an `IndexMap` for headers.
//!
//! Measured on one batch of a thousand 128-byte records (`BARNABAS_CELLS=decode`
//! in the bench):
//!
//! | | ns/record |
//! |---|---:|
//! | walking the records and keeping nothing | 3.0 |
//! | keeping a 24-byte record | 5.5 |
//! | keeping `Bytes` slices | 27.5 |
//! | `kafka_protocol` → `Vec<Record>` | 70 |
//!
//! Parsing is 4% of it. The rest is materialisation, which is what this module
//! avoids: batch-level facts are stored once, and a record is an offset, a
//! timestamp and two ranges into the batch buffer.
//!
//! # What it does not do
//!
//! Falls back — returns `None` — only for a batch that is not magic 2. All four
//! compression codecs and record headers are handled: compressed records are
//! decompressed into a buffer the batch then owns, and headers are recorded as
//! a byte range and parsed on demand.

use bytes::Bytes;

use crate::{Error, Result};

/// Batch header layout (magic 2), by byte offset from the start of the batch.
mod field {
    pub const BASE_OFFSET: usize = 0;
    pub const LENGTH: usize = 8;
    pub const MAGIC: usize = 16;
    pub const CRC: usize = 17;
    pub const ATTRIBUTES: usize = 21;
    pub const BASE_TIMESTAMP: usize = 27;
    pub const PRODUCER_ID: usize = 43;
    pub const RECORD_COUNT: usize = 57;
    /// Everything above, and where the records begin.
    pub const HEADER_LEN: usize = 61;
    /// The CRC covers from just after itself to the end of the batch.
    pub const CRC_FROM: usize = 21;
}

/// One record: where it is, not what it contains.
#[derive(Debug, Clone, Copy)]
pub struct LeanRecord {
    pub offset: i64,
    pub timestamp: i64,
    key: (u32, u32),
    value: (u32, u32),
    /// The header block: where it is, how many, and *not* what it contains.
    ///
    /// Headers are variable in number, so holding them inline would put a
    /// `Vec` in every record and undo the point of this type. Holding the
    /// region and its count costs eight bytes and nothing at all unless a
    /// caller asks for them.
    headers: (u32, u32),
    header_count: u32,
}

/// One batch, with the facts that belong to the batch held once.
#[derive(Debug, Clone)]
pub struct LeanBatch {
    /// Retained so a record's key and value can be sliced from it on demand.
    buffer: Bytes,
    pub base_offset: i64,
    pub producer_id: i64,
    pub transactional: bool,
    /// A control batch carries markers, not caller data. Batch-level in the
    /// format, which is why it is stored here and not per record.
    pub control: bool,
    pub records: Vec<LeanRecord>,
}

impl LeanBatch {
    /// This record's key, as a slice of the batch buffer.
    #[must_use]
    pub fn key(&self, record: &LeanRecord) -> Option<Bytes> {
        self.slice(record.key)
    }

    /// This record's value, as a slice of the batch buffer.
    ///
    /// **The `Bytes` is built here rather than at decode time.** Every slice of
    /// one buffer increments the same atomic refcount, so materialising two per
    /// record up front is two million read-modify-writes on one cache line for
    /// a million records — self-contended, and measurably as expensive as
    /// copying the bytes outright. A caller that skips a record never pays for
    /// it.
    #[must_use]
    pub fn value(&self, record: &LeanRecord) -> Option<Bytes> {
        self.slice(record.value)
    }

    /// `u32::MAX` marks absent, which is how the format distinguishes a null
    /// key from an empty one.
    fn slice(&self, (at, len): (u32, u32)) -> Option<Bytes> {
        if at == u32::MAX {
            return None;
        }
        let at = at as usize;
        Some(self.buffer.slice(at..at + len as usize))
    }

    /// This record's headers, parsed on demand.
    ///
    /// Returns an empty vector when there are none, which is the common case
    /// and costs nothing — the region was never touched at decode time.
    ///
    /// # Errors
    /// [`Error::Codec`] if the header block is malformed.
    pub fn headers(&self, record: &LeanRecord) -> Result<Vec<(Bytes, Option<Bytes>)>> {
        if record.header_count == 0 {
            return Ok(Vec::new());
        }
        let at = record.headers.0 as usize;
        let end = at + record.headers.1 as usize;
        let block = self
            .buffer
            .get(at..end)
            .ok_or_else(|| Error::Codec("header block".to_owned()))?;

        let mut out = Vec::with_capacity(record.header_count as usize);
        let mut pos = 0usize;
        for _ in 0..record.header_count {
            let key_len = varint(block, &mut pos)
                .ok_or_else(|| Error::Codec("header key length".to_owned()))?;
            let key_at = at + pos;
            let key_len = key_len.max(0) as usize;
            pos += key_len;

            let value_len = varint(block, &mut pos)
                .ok_or_else(|| Error::Codec("header value length".to_owned()))?;
            let value = if value_len >= 0 {
                let value_at = at + pos;
                pos += value_len as usize;
                Some(self.buffer.slice(value_at..value_at + value_len as usize))
            } else {
                None
            };
            out.push((self.buffer.slice(key_at..key_at + key_len), value));
        }
        Ok(out)
    }

    /// The control-marker type, for a control batch. 0 is abort.
    ///
    /// Reads the marker's key, which is where the type lives.
    #[must_use]
    pub fn control_type(&self, record: &LeanRecord) -> Option<i16> {
        let key = self.slice(record.key)?;
        if key.len() < 4 {
            return None;
        }
        Some(i16::from_be_bytes([key[2], key[3]]))
    }
}

/// Zigzag LEB128, the integer encoding inside a record batch.
///
/// Returns `None` rather than panicking on a truncated or over-long encoding:
/// this parses bytes off a socket, and a malformed batch must not be able to
/// index out of bounds or spin.
#[inline]
fn varint(buf: &[u8], pos: &mut usize) -> Option<i64> {
    let mut raw: u64 = 0;
    let mut shift = 0;
    loop {
        if shift > 63 {
            return None;
        }
        let byte = *buf.get(*pos)?;
        *pos += 1;
        raw |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Some(((raw >> 1) as i64) ^ -((raw & 1) as i64))
}

fn i16_at(buf: &[u8], at: usize) -> Option<i16> {
    Some(i16::from_be_bytes(buf.get(at..at + 2)?.try_into().ok()?))
}

fn i32_at(buf: &[u8], at: usize) -> Option<i32> {
    Some(i32::from_be_bytes(buf.get(at..at + 4)?.try_into().ok()?))
}

fn i64_at(buf: &[u8], at: usize) -> Option<i64> {
    Some(i64::from_be_bytes(buf.get(at..at + 8)?.try_into().ok()?))
}

/// Decode every batch in `buffer`.
///
/// Returns `Ok(None)` when any batch is something this reader does not handle —
/// compressed, not magic 2, or carrying record headers — so the caller can fall
/// back to the full decoder. Returns `Err` only when the bytes are actually
/// wrong, which is the same distinction the rest of this crate draws.
///
/// # Errors
/// [`Error::Codec`] if a batch is truncated or fails its CRC.
pub fn decode_lean(buffer: &Bytes) -> Result<Option<Vec<LeanBatch>>> {
    let mut batches = Vec::new();
    let mut at = 0usize;

    while at < buffer.len() {
        // A fetch response is cut off at `max_bytes`, so a trailing partial
        // batch is normal and means "stop", not "corrupt".
        let Some(length) = i32_at(buffer, at + field::LENGTH) else {
            break;
        };
        let end = at + field::LENGTH + 4 + length.max(0) as usize;
        if length <= 0 || end > buffer.len() {
            break;
        }
        let batch = &buffer[at..end];
        if batch.len() < field::HEADER_LEN {
            break;
        }

        if batch[field::MAGIC] != 2 {
            return Ok(None);
        }
        let attributes = i16_at(batch, field::ATTRIBUTES)
            .ok_or_else(|| Error::Codec("batch attributes".to_owned()))?;

        let expected =
            i32_at(batch, field::CRC).ok_or_else(|| Error::Codec("batch crc".to_owned()))? as u32;
        let actual = crc32c::crc32c(&batch[field::CRC_FROM..]);
        if expected != actual {
            return Err(Error::Codec(format!(
                "record batch crc: expected {expected:#x}, got {actual:#x}"
            )));
        }

        let base_offset = i64_at(batch, field::BASE_OFFSET)
            .ok_or_else(|| Error::Codec("base offset".to_owned()))?;
        let base_timestamp = i64_at(batch, field::BASE_TIMESTAMP)
            .ok_or_else(|| Error::Codec("base timestamp".to_owned()))?;
        let producer_id = i64_at(batch, field::PRODUCER_ID)
            .ok_or_else(|| Error::Codec("producer id".to_owned()))?;
        let count = i32_at(batch, field::RECORD_COUNT)
            .ok_or_else(|| Error::Codec("record count".to_owned()))?
            .max(0) as usize;

        // **The records section, however it arrived.** Uncompressed, it is a
        // slice of the response buffer and nothing is copied. Compressed, it is
        // the decompressed bytes — one allocation per batch, which the codec
        // requires and which `kafka_protocol` pays too. Everything below parses
        // the same way either way, because ranges are relative to this and the
        // batch holds it.
        let body: Bytes = match attributes & 0x07 {
            0 => buffer.slice(at + field::HEADER_LEN..end),
            codec => decompress(codec, &batch[field::HEADER_LEN..])?,
        };

        let mut records = Vec::with_capacity(count);
        let mut pos = 0usize;
        for _ in 0..count {
            let Some(len) = varint(&body, &mut pos) else {
                return Err(Error::Codec("record length".to_owned()));
            };
            let record_end = pos + len.max(0) as usize;
            if record_end > body.len() {
                return Err(Error::Codec("record overruns its batch".to_owned()));
            }

            pos += 1; // per-record attributes, unused in the format
            let timestamp_delta =
                varint(&body, &mut pos).ok_or_else(|| Error::Codec("timestamp delta".to_owned()))?;
            let offset_delta =
                varint(&body, &mut pos).ok_or_else(|| Error::Codec("offset delta".to_owned()))?;

            let key_len =
                varint(&body, &mut pos).ok_or_else(|| Error::Codec("key length".to_owned()))?;
            let key = if key_len >= 0 {
                let range = (pos as u32, key_len as u32);
                pos += key_len as usize;
                range
            } else {
                (u32::MAX, 0)
            };

            let value_len =
                varint(&body, &mut pos).ok_or_else(|| Error::Codec("value length".to_owned()))?;
            let value = if value_len >= 0 {
                let range = (pos as u32, value_len as u32);
                pos += value_len as usize;
                range
            } else {
                (u32::MAX, 0)
            };

            // The header block is recorded, not parsed. See
            // [`LeanBatch::headers`].
            let header_count = varint(&body, &mut pos)
                .ok_or_else(|| Error::Codec("header count".to_owned()))?
                .max(0) as u32;
            let headers = (pos as u32, record_end.saturating_sub(pos) as u32);

            records.push(LeanRecord {
                offset: base_offset + offset_delta,
                timestamp: base_timestamp + timestamp_delta,
                key,
                value,
                headers,
                header_count,
            });
            pos = record_end;
        }

        batches.push(LeanBatch {
            buffer: body,
            base_offset,
            producer_id,
            transactional: attributes & 0x10 != 0,
            control: attributes & 0x20 != 0,
            records,
        });
        at = end;
    }

    Ok(Some(batches))
}

/// Kafka's snappy is **xerial-framed**, not raw: a 16-byte magic header, then
/// `[u32 length][block]` repeated. Java's reader falls back to raw snappy when
/// the header is absent, and so does this — some producers write it that way.
const SNAPPY_MAGIC: &[u8; 16] = b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01";

fn snappy(compressed: &[u8]) -> Result<Vec<u8>> {
    let raw = |bytes: &[u8]| {
        snap::raw::Decoder::new()
            .decompress_vec(bytes)
            .map_err(|e| Error::Codec(format!("snappy: {e}")))
    };

    if compressed.len() < SNAPPY_MAGIC.len() || &compressed[..SNAPPY_MAGIC.len()] != SNAPPY_MAGIC {
        return raw(compressed);
    }

    let mut out = Vec::new();
    let mut at = SNAPPY_MAGIC.len();
    while at < compressed.len() {
        let len = compressed
            .get(at..at + 4)
            .and_then(|b| b.try_into().ok())
            .map(u32::from_be_bytes)
            .ok_or_else(|| Error::Codec("snappy block length".to_owned()))?
            as usize;
        at += 4;
        let block = compressed
            .get(at..at + len)
            .ok_or_else(|| Error::Codec("snappy block overruns".to_owned()))?;
        out.extend_from_slice(&raw(block)?);
        at += len;
    }
    Ok(out)
}

/// Decompress a batch's records section.
///
/// The four codecs Kafka defines. Each is already in the dependency tree via
/// `kafka_protocol`, so supporting them here adds no crates — only the code to
/// call them.
fn decompress(codec: i16, compressed: &[u8]) -> Result<Bytes> {
    use std::io::Read;

    let mut out = Vec::new();
    match codec {
        1 => {
            flate2::read::GzDecoder::new(compressed)
                .read_to_end(&mut out)
                .map_err(|e| Error::Codec(format!("gzip: {e}")))?;
        }
        2 => out = snappy(compressed)?,
        3 => {
            lz4::Decoder::new(compressed)
                .map_err(|e| Error::Codec(format!("lz4: {e}")))?
                .read_to_end(&mut out)
                .map_err(|e| Error::Codec(format!("lz4: {e}")))?;
        }
        4 => {
            zstd::stream::copy_decode(compressed, &mut out)
                .map_err(|e| Error::Codec(format!("zstd: {e}")))?;
        }
        other => return Err(Error::Codec(format!("unknown compression codec {other}"))),
    }
    Ok(Bytes::from(out))
}

/// Apply the READ_COMMITTED rules **per batch**.
///
/// The ordinary filter works record by record because `kafka_protocol` flattens
/// batches away. Here `transactional`, `control` and `producer_id` are still
/// where the format puts them — on the batch — so an aborted transaction is
/// dropped a batch at a time instead of a record at a time.
///
/// Returns the batches to hand the caller and the offset the next fetch should
/// start from. As with the record-wise filter, the position advances past
/// records that were dropped, so a partition of nothing but aborted data still
/// makes progress.
#[must_use]
pub fn filter_batches(
    batches: Vec<LeanBatch>,
    aborted: &[crate::consumer::AbortedTransaction],
    last_stable_offset: i64,
    isolation: crate::IsolationLevel,
    fetch_offset: i64,
) -> (Vec<LeanBatch>, i64) {
    let read_committed = isolation == crate::IsolationLevel::ReadCommitted;

    let mut sorted: Vec<crate::consumer::AbortedTransaction> = aborted.to_vec();
    sorted.sort_by_key(|a| a.first_offset);
    let mut pending = sorted.into_iter().peekable();
    let mut aborted_producers: std::collections::HashSet<i64> = std::collections::HashSet::new();

    let mut kept = Vec::with_capacity(batches.len());
    let mut next_offset = fetch_offset;

    for mut batch in batches {
        let Some(first) = batch.records.first().map(|r| r.offset) else {
            continue;
        };
        // Everything at or above the LSO is withheld, and so is everything
        // after it — the broker sends them in order.
        if read_committed && first >= last_stable_offset {
            break;
        }

        while pending.peek().is_some_and(|a| a.first_offset <= first) {
            let a = pending.next().expect("peeked");
            aborted_producers.insert(a.producer_id);
        }

        let last = batch.records.last().map_or(first, |r| r.offset);
        next_offset = last + 1;

        if batch.control {
            // The abort marker closes the range, so a later transaction from
            // the same producer is judged on its own.
            for record in &batch.records {
                if batch.control_type(record) == Some(CONTROL_ABORT) {
                    aborted_producers.remove(&batch.producer_id);
                }
            }
            continue;
        }

        if read_committed && batch.transactional && aborted_producers.contains(&batch.producer_id) {
            continue;
        }

        // A batch can begin before the requested offset, since the broker sends
        // whole batches.
        if first < fetch_offset {
            batch.records.retain(|r| r.offset >= fetch_offset);
        }
        if read_committed {
            batch.records.retain(|r| r.offset < last_stable_offset);
        }
        if !batch.records.is_empty() {
            kept.push(batch);
        }
    }

    (kept, next_offset)
}

/// An abort marker. Matches `barnabas_core::consumer`.
const CONTROL_ABORT: i16 = 0;

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };

    fn encode(records: &[Record], compression: Compression) -> Bytes {
        let mut buf = bytes::BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .expect("encode");
        buf.freeze()
    }

    fn record(offset: i64, key: Option<&[u8]>, value: Option<&[u8]>) -> Record {
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: 7,
            producer_epoch: 0,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: offset as i32,
            timestamp: 1_000 + offset,
            key: key.map(Bytes::copy_from_slice),
            value: value.map(Bytes::copy_from_slice),
            headers: Default::default(),
        }
    }

    /// The property that matters: the same bytes, read two ways, agree.
    #[test]
    fn agrees_with_the_reference_decoder() {
        let records: Vec<Record> = (0..64)
            .map(|i| {
                record(
                    i,
                    Some(format!("k{i}").as_bytes()),
                    Some(format!("value-{i}").as_bytes()),
                )
            })
            .collect();
        let encoded = encode(&records, Compression::None);

        let reference = kafka_protocol::records::RecordBatchDecoder::decode(&mut encoded.clone())
            .expect("reference decode")
            .records;
        let lean = decode_lean(&encoded)
            .expect("lean decode")
            .expect("handled");

        let flat: Vec<_> = lean
            .iter()
            .flat_map(|batch| batch.records.iter().map(move |r| (batch, r)))
            .collect();
        assert_eq!(flat.len(), reference.len());

        for ((batch, lean), reference) in flat.iter().zip(&reference) {
            assert_eq!(lean.offset, reference.offset, "offset");
            assert_eq!(lean.timestamp, reference.timestamp, "timestamp");
            assert_eq!(batch.key(lean), reference.key, "key");
            assert_eq!(batch.value(lean), reference.value, "value");
            assert_eq!(batch.producer_id, reference.producer_id, "producer id");
        }
    }

    #[test]
    fn a_null_key_stays_null() {
        let encoded = encode(&[record(0, None, Some(b"v"))], Compression::None);
        let lean = decode_lean(&encoded).expect("decode").expect("handled");
        let batch = &lean[0];
        assert_eq!(batch.key(&batch.records[0]), None);
        assert_eq!(batch.value(&batch.records[0]), Some(Bytes::from_static(b"v")));
    }

    /// Every codec round-trips, and against the reference decoder's output.
    #[test]
    fn every_compression_codec_round_trips() {
        for compression in [
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let records: Vec<Record> = (0..32)
                .map(|i| {
                    record(
                        i,
                        Some(format!("k{i}").as_bytes()),
                        Some(format!("value-{i}").as_bytes()),
                    )
                })
                .collect();
            let encoded = encode(&records, compression);

            let lean = decode_lean(&encoded)
                .unwrap_or_else(|e| panic!("{compression:?}: {e}"))
                .unwrap_or_else(|| panic!("{compression:?} was handed back"));
            let flat: Vec<_> = lean
                .iter()
                .flat_map(|b| b.records.iter().map(move |r| (b, r)))
                .collect();
            assert_eq!(flat.len(), records.len(), "{compression:?}");
            for ((batch, lean), reference) in flat.iter().zip(&records) {
                assert_eq!(lean.offset, reference.offset, "{compression:?} offset");
                assert_eq!(batch.value(lean), reference.value, "{compression:?} value");
            }
        }
    }

    /// Headers survive, and are read from the region rather than at decode time.
    #[test]
    fn headers_are_read_on_demand() {
        let mut with_headers = record(0, Some(b"k"), Some(b"v"));
        with_headers.headers.insert(
            kafka_protocol::protocol::StrBytes::from_static_str("trace"),
            Some(Bytes::from_static(b"abc")),
        );
        with_headers.headers.insert(
            kafka_protocol::protocol::StrBytes::from_static_str("empty"),
            None,
        );
        let encoded = encode(&[with_headers], Compression::None);

        let lean = decode_lean(&encoded).expect("decode").expect("handled");
        let batch = &lean[0];
        let headers = batch.headers(&batch.records[0]).expect("headers");
        assert_eq!(headers.len(), 2);
        assert_eq!(headers[0].0, Bytes::from_static(b"trace"));
        assert_eq!(headers[0].1, Some(Bytes::from_static(b"abc")));
        assert_eq!(headers[1].0, Bytes::from_static(b"empty"));
        assert_eq!(headers[1].1, None);
    }

    /// A record with no headers costs nothing and reports nothing.
    #[test]
    fn no_headers_is_empty() {
        let encoded = encode(&[record(0, None, Some(b"v"))], Compression::None);
        let lean = decode_lean(&encoded).expect("decode").expect("handled");
        let batch = &lean[0];
        assert!(batch.headers(&batch.records[0]).expect("headers").is_empty());
    }

    /// A response cut off at `max_bytes` ends mid-batch. That fragment is not
    /// an error — the broker does it on purpose — and must simply be ignored.
    #[test]
    fn a_truncated_trailing_batch_is_ignored() {
        let encoded = encode(&[record(0, None, Some(b"v"))], Compression::None);
        let mut truncated = bytes::BytesMut::from(&encoded[..]);
        truncated.extend_from_slice(&encoded[..encoded.len() / 2]);
        let lean = decode_lean(&truncated.freeze())
            .expect("decode")
            .expect("handled");
        assert_eq!(lean.len(), 1, "the whole batch is kept, the fragment is not");
        assert_eq!(lean[0].records.len(), 1);
    }

    /// A corrupted body must be caught, not handed to the caller.
    #[test]
    fn a_bad_crc_is_an_error() {
        let encoded = encode(&[record(0, None, Some(b"value"))], Compression::None);
        let mut corrupt = bytes::BytesMut::from(&encoded[..]);
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xff;
        assert!(decode_lean(&corrupt.freeze()).is_err());
    }
}
