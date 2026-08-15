//! Kafka's framing: a 4-byte big-endian length, then that many bytes.
//!
//! The only place in the crate that deals with partial reads. A socket hands
//! back whatever arrived — half a header, three frames and a fragment — and
//! this turns that into whole frames or nothing.

use bytes::{Buf, Bytes, BytesMut};

use crate::{Error, Result};

/// 100 MiB. Larger than any sane `fetch.max.bytes`, small enough that a corrupt
/// or hostile length prefix cannot steer us into an unbounded allocation.
pub const DEFAULT_MAX_FRAME: usize = 100 * 1024 * 1024;

/// Reassembles length-prefixed frames from a byte stream.
#[derive(Debug)]
pub struct FrameDecoder {
    buf: BytesMut,
    max_frame: usize,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_FRAME)
    }
}

impl FrameDecoder {
    #[must_use]
    pub fn new(max_frame: usize) -> Self {
        Self {
            buf: BytesMut::new(),
            max_frame,
        }
    }

    /// Feed bytes straight off the socket.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Bytes held pending a complete frame.
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Take the next complete frame, if one has arrived.
    ///
    /// The length prefix is consumed and not returned: every caller wants the
    /// body, and handing back the prefix invites double-counting it.
    ///
    /// # Errors
    /// [`Error::FrameTooLarge`] if the prefix exceeds the configured limit. The
    /// check happens *before* reserving, which is the entire point of it.
    pub fn next_frame(&mut self) -> Result<Option<Bytes>> {
        if self.buf.len() < 4 {
            return Ok(None);
        }
        let len = i32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
        let len = usize::try_from(len).map_err(|_| Error::FrameTooLarge {
            len: usize::MAX,
            limit: self.max_frame,
        })?;
        if len > self.max_frame {
            return Err(Error::FrameTooLarge {
                len,
                limit: self.max_frame,
            });
        }
        if self.buf.len() < 4 + len {
            return Ok(None);
        }
        self.buf.advance(4);
        Ok(Some(self.buf.split_to(len).freeze()))
    }
}

/// Prefix `body` with its length, ready to write.
///
/// # Errors
/// [`Error::FrameTooLarge`] if the body does not fit in an `i32`.
pub fn frame(body: &[u8]) -> Result<Bytes> {
    let len = i32::try_from(body.len()).map_err(|_| Error::FrameTooLarge {
        len: body.len(),
        limit: i32::MAX as usize,
    })?;
    let mut out = BytesMut::with_capacity(body.len() + 4);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    Ok(out.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_whole_frame_round_trips() {
        let mut dec = FrameDecoder::default();
        dec.push(&frame(b"hello").unwrap());
        assert_eq!(dec.next_frame().unwrap().as_deref(), Some(&b"hello"[..]));
        assert!(dec.next_frame().unwrap().is_none());
    }

    /// A socket splits wherever it likes. Feeding one byte at a time is the
    /// harshest version of that, and must yield exactly one frame at the end.
    #[test]
    fn a_frame_split_byte_by_byte_still_arrives() {
        let bytes = frame(b"a longer payload").unwrap();
        let mut dec = FrameDecoder::default();
        for (i, b) in bytes.iter().enumerate() {
            dec.push(&[*b]);
            if i + 1 < bytes.len() {
                assert!(
                    dec.next_frame().unwrap().is_none(),
                    "yielded a frame after {} of {} bytes",
                    i + 1,
                    bytes.len()
                );
            }
        }
        assert_eq!(
            dec.next_frame().unwrap().as_deref(),
            Some(&b"a longer payload"[..])
        );
    }

    /// Several frames in one read, plus a fragment of the next: all the whole
    /// ones come out, the fragment waits.
    #[test]
    fn many_frames_and_a_fragment_in_one_read() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&frame(b"one").unwrap());
        wire.extend_from_slice(&frame(b"two").unwrap());
        let third = frame(b"three").unwrap();
        wire.extend_from_slice(&third[..4]); // header only

        let mut dec = FrameDecoder::default();
        dec.push(&wire);
        assert_eq!(dec.next_frame().unwrap().as_deref(), Some(&b"one"[..]));
        assert_eq!(dec.next_frame().unwrap().as_deref(), Some(&b"two"[..]));
        assert!(dec.next_frame().unwrap().is_none());

        dec.push(&third[4..]);
        assert_eq!(dec.next_frame().unwrap().as_deref(), Some(&b"three"[..]));
    }

    /// An empty frame is legal framing and must not be confused with "nothing
    /// yet" — the difference between `Some(empty)` and `None` is whether the
    /// caller advances.
    #[test]
    fn an_empty_frame_is_a_frame() {
        let mut dec = FrameDecoder::default();
        dec.push(&frame(b"").unwrap());
        assert_eq!(dec.next_frame().unwrap().as_deref(), Some(&b""[..]));
    }

    /// The allocation guard fires on the prefix, before any reservation.
    #[test]
    fn an_absurd_length_prefix_is_rejected_before_allocating() {
        let mut dec = FrameDecoder::new(1024);
        dec.push(&i32::MAX.to_be_bytes());
        assert!(matches!(
            dec.next_frame(),
            Err(Error::FrameTooLarge { limit: 1024, .. })
        ));
        assert_eq!(
            dec.buffered(),
            4,
            "a rejected frame must not consume the buffer; the caller drops the connection"
        );
    }

    /// A negative prefix is a corrupt peer, not a small frame.
    #[test]
    fn a_negative_length_prefix_is_rejected() {
        let mut dec = FrameDecoder::default();
        dec.push(&(-1i32).to_be_bytes());
        assert!(matches!(dec.next_frame(), Err(Error::FrameTooLarge { .. })));
    }
}
