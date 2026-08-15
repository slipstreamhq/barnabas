//! Request/response correlation over one broker connection.
//!
//! Kafka answers a connection's requests **in the order they were sent**, which
//! is what makes pipelining cheap: the client can have several requests in
//! flight and still match responses by position. The correlation id is then a
//! check rather than a lookup, and this type treats it that way — a mismatch is
//! fatal, because a stream that has desynchronised cannot be resynchronised.
//!
//! Two things this type exists to stop a caller getting wrong, both found in
//! P0 against a real broker:
//!
//! - **The header version is not the API version.** Flexible versions added a
//!   tagged-field section to the header, so each API and version pair has its
//!   own header version. Guess it and the response decodes into plausible
//!   garbage rather than failing.
//! - **The response header version differs from the request's** for several
//!   APIs, so it is derived separately.

use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{encode_request_header_into_buffer, Decodable, Encodable, StrBytes};

use crate::frame::{self, FrameDecoder};
use crate::{Error, Result};

/// A request awaiting its response.
#[derive(Debug, Clone, Copy)]
struct Pending {
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
}

/// A response frame, matched to the request that asked for it.
///
/// The body is handed back undecoded so the caller decodes into the concrete
/// response type it expects. That keeps this type free of the 185 message types
/// and keeps the decode where the caller's error handling already is.
#[derive(Debug)]
pub struct PendingResponse {
    pub api_key: ApiKey,
    pub version: i16,
    pub correlation_id: i32,
    /// The response body, header already consumed.
    pub body: Bytes,
}

/// One broker connection's protocol state. Owns no socket.
#[derive(Debug)]
pub struct Connection {
    client_id: StrBytes,
    next_correlation_id: i32,
    in_flight: VecDeque<Pending>,
    decoder: FrameDecoder,
}

impl Connection {
    #[must_use]
    pub fn new(client_id: impl Into<StrBytes>) -> Self {
        Self {
            client_id: client_id.into(),
            next_correlation_id: 0,
            in_flight: VecDeque::new(),
            decoder: FrameDecoder::default(),
        }
    }

    /// How many requests are awaiting responses.
    ///
    /// The producer's in-flight limit is enforced against this: Kafka retains
    /// idempotence with up to five in flight, and only because sequence numbers
    /// let the broker order them.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    /// Encode `req` into a framed, ready-to-write buffer and record it as in
    /// flight.
    ///
    /// # Errors
    /// [`Error::Codec`] if encoding fails.
    pub fn request<R: Encodable>(
        &mut self,
        api_key: ApiKey,
        version: i16,
        req: &R,
    ) -> Result<Bytes> {
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1);
        let correlation_id = self.next_correlation_id;

        let mut header = RequestHeader::default();
        header.request_api_key = api_key as i16;
        header.request_api_version = version;
        header.correlation_id = correlation_id;
        header.client_id = Some(self.client_id.clone());

        let mut body = BytesMut::new();
        // The helper derives the header version from the key and version, which
        // is exactly the thing not to hand-pick.
        encode_request_header_into_buffer(&mut body, &header)
            .map_err(|e| Error::Codec(format!("encode header: {e}")))?;
        req.encode(&mut body, version)
            .map_err(|e| Error::Codec(format!("encode {api_key:?} v{version}: {e}")))?;

        self.in_flight.push_back(Pending {
            api_key,
            version,
            correlation_id,
        });
        frame::frame(&body)
    }

    /// Feed bytes from the socket.
    pub fn push_bytes(&mut self, bytes: &[u8]) {
        self.decoder.push(bytes);
    }

    /// Take the next complete response, matched to its request.
    ///
    /// # Errors
    /// [`Error::Unsolicited`] if nothing was in flight, [`Error::Correlation`]
    /// if the id does not match the oldest in-flight request. Both are fatal
    /// for the connection.
    pub fn next_response(&mut self) -> Result<Option<PendingResponse>> {
        let Some(mut frame) = self.decoder.next_frame()? else {
            return Ok(None);
        };
        let pending = self.in_flight.front().copied().ok_or(Error::Unsolicited)?;

        let header = ResponseHeader::decode(
            &mut frame,
            pending.api_key.response_header_version(pending.version),
        )
        .map_err(|e| Error::Codec(format!("decode response header: {e}")))?;

        if header.correlation_id != pending.correlation_id {
            // Left in flight deliberately: the caller's only correct move is to
            // drop the connection, and popping would imply recovery.
            return Err(Error::Correlation {
                got: header.correlation_id,
                expected: pending.correlation_id,
            });
        }
        self.in_flight.pop_front();

        Ok(Some(PendingResponse {
            api_key: pending.api_key,
            version: pending.version,
            correlation_id: header.correlation_id,
            body: frame,
        }))
    }

    /// Decode a response body into its concrete type.
    ///
    /// # Errors
    /// [`Error::Codec`] if decoding fails.
    pub fn decode<R: Decodable>(resp: &PendingResponse) -> Result<R> {
        let mut body = resp.body.clone();
        R::decode(&mut body, resp.version).map_err(|e| {
            Error::Codec(format!(
                "decode {:?} v{} response: {e}",
                resp.api_key, resp.version
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::{ApiVersionsRequest, ApiVersionsResponse};

    /// Encode a response the way a broker would, so the tests drive the real
    /// decode path rather than a mock of it.
    fn broker_response<R: Encodable>(
        api_key: ApiKey,
        version: i16,
        correlation_id: i32,
        resp: &R,
    ) -> Bytes {
        let mut header = ResponseHeader::default();
        header.correlation_id = correlation_id;
        let mut body = BytesMut::new();
        header
            .encode(&mut body, api_key.response_header_version(version))
            .unwrap();
        resp.encode(&mut body, version).unwrap();
        frame::frame(&body).unwrap()
    }

    fn api_versions_response(api_count: usize) -> ApiVersionsResponse {
        let mut resp = ApiVersionsResponse::default();
        resp.api_keys = (0..api_count)
            .map(|i| {
                let mut k = kafka_protocol::messages::api_versions_response::ApiVersion::default();
                k.api_key = i as i16;
                k.max_version = 1;
                k
            })
            .collect();
        resp
    }

    #[test]
    fn a_request_and_its_response_are_matched() {
        let mut conn = Connection::new(StrBytes::from_static_str("test"));
        let req = ApiVersionsRequest::default();
        let _wire = conn.request(ApiKey::ApiVersions, 3, &req).unwrap();
        assert_eq!(conn.in_flight(), 1);

        conn.push_bytes(&broker_response(
            ApiKey::ApiVersions,
            3,
            1,
            &api_versions_response(2),
        ));
        let resp = conn.next_response().unwrap().expect("a response");
        assert_eq!(resp.correlation_id, 1);
        assert_eq!(conn.in_flight(), 0);

        let decoded: ApiVersionsResponse = Connection::decode(&resp).unwrap();
        assert_eq!(decoded.api_keys.len(), 2);
    }

    /// Pipelining is the point of correlation ids: three requests out, three
    /// responses back in order, all matched.
    #[test]
    fn pipelined_requests_match_in_order() {
        let mut conn = Connection::new(StrBytes::from_static_str("test"));
        for _ in 0..3 {
            conn.request(ApiKey::ApiVersions, 3, &ApiVersionsRequest::default())
                .unwrap();
        }
        assert_eq!(conn.in_flight(), 3);

        for id in 1..=3 {
            conn.push_bytes(&broker_response(
                ApiKey::ApiVersions,
                3,
                id,
                &api_versions_response(id as usize),
            ));
        }
        for id in 1..=3 {
            let resp = conn.next_response().unwrap().expect("a response");
            assert_eq!(resp.correlation_id, id);
            let decoded: ApiVersionsResponse = Connection::decode(&resp).unwrap();
            assert_eq!(decoded.api_keys.len(), id as usize);
        }
        assert_eq!(conn.in_flight(), 0);
    }

    /// A desynchronised stream is fatal, and must not silently pop the pending
    /// request — otherwise the *next* response would be matched to the wrong
    /// request and the corruption would spread instead of stopping.
    #[test]
    fn a_correlation_mismatch_is_fatal_and_does_not_advance() {
        let mut conn = Connection::new(StrBytes::from_static_str("test"));
        conn.request(ApiKey::ApiVersions, 3, &ApiVersionsRequest::default())
            .unwrap();
        conn.push_bytes(&broker_response(
            ApiKey::ApiVersions,
            3,
            99,
            &api_versions_response(1),
        ));
        assert!(matches!(
            conn.next_response(),
            Err(Error::Correlation {
                got: 99,
                expected: 1
            })
        ));
        assert_eq!(conn.in_flight(), 1);
    }

    #[test]
    fn a_response_with_nothing_in_flight_is_an_error() {
        let mut conn = Connection::new(StrBytes::from_static_str("test"));
        conn.push_bytes(&broker_response(
            ApiKey::ApiVersions,
            3,
            1,
            &api_versions_response(1),
        ));
        assert!(matches!(conn.next_response(), Err(Error::Unsolicited)));
    }

    /// Half a response is not a response.
    #[test]
    fn a_partial_response_yields_nothing_yet() {
        let mut conn = Connection::new(StrBytes::from_static_str("test"));
        conn.request(ApiKey::ApiVersions, 3, &ApiVersionsRequest::default())
            .unwrap();
        let wire = broker_response(ApiKey::ApiVersions, 3, 1, &api_versions_response(4));
        conn.push_bytes(&wire[..wire.len() - 1]);
        assert!(conn.next_response().unwrap().is_none());
        conn.push_bytes(&wire[wire.len() - 1..]);
        assert!(conn.next_response().unwrap().is_some());
    }

    /// Correlation ids must be distinct per request; a duplicate would make the
    /// mismatch check useless.
    #[test]
    fn correlation_ids_advance() {
        let mut conn = Connection::new(StrBytes::from_static_str("test"));
        conn.request(ApiKey::ApiVersions, 3, &ApiVersionsRequest::default())
            .unwrap();
        conn.request(ApiKey::ApiVersions, 3, &ApiVersionsRequest::default())
            .unwrap();
        conn.push_bytes(&broker_response(
            ApiKey::ApiVersions,
            3,
            1,
            &api_versions_response(1),
        ));
        conn.push_bytes(&broker_response(
            ApiKey::ApiVersions,
            3,
            2,
            &api_versions_response(1),
        ));
        assert_eq!(conn.next_response().unwrap().unwrap().correlation_id, 1);
        assert_eq!(conn.next_response().unwrap().unwrap().correlation_id, 2);
    }
}
