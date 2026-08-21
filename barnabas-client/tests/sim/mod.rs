//! A broker that lies to you, on purpose.
//!
//! A [`Transport`] whose sockets are in-memory queues and whose peer is a small
//! scripted broker speaking the **real wire protocol** — requests are decoded
//! with `kafka-protocol` and responses encoded with it, so the client under
//! test runs its actual codec, framing and correlation paths.
//!
//! # Why this exists
//!
//! Exactly-once bugs are silent: a wrong retry duplicates records while every
//! status code stays green. A live broker cannot be asked to fail on cue, so
//! the failures that matter — leadership moving mid-produce, a coordinator
//! migrating, a fenced epoch — are exactly the ones a broker-backed test never
//! covers. Here they are one line of script.
//!
//! There is no runtime and no clock: `Transport::sleep` returns immediately, so
//! a test that retries forty times finishes instantly and deterministically.
//! Failures reproduce exactly rather than "sometimes on CI".
//!
//! # What it is not
//!
//! Not a Kafka implementation. It answers the requests this client sends, with
//! the fields the client reads, and asserts nothing about the rest. Anything it
//! gets *wrong* would show up in the broker-backed suites, which is why both
//! exist.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

use barnabas_client::Transport;

/// One scripted failure: the next `api` request gets `code` instead of success.
#[derive(Debug, Clone, Copy)]
pub struct Fault {
    pub api: ApiKey,
    pub code: i16,
    /// How many times to answer with it before behaving.
    pub times: usize,
    /// How many matching requests to let through **first**.
    ///
    /// Without this a fault can only ever hit the first request, so a window of
    /// pipelined requests could not be made to fail in the *middle* — which is
    /// the case that decides whether recovery re-sends the right batches.
    pub after: usize,
}

/// What the simulated cluster did, so a test can assert on behaviour rather
/// than only on the client's return value.
#[derive(Debug, Default)]
pub struct Journal {
    /// Every request the client sent, in order.
    pub requests: Vec<ApiKey>,
    /// `(partition, base_sequence, record_count)` for each **accepted**
    /// produce. A duplicate would appear here twice; a lost retry would be
    /// missing.
    pub produced: Vec<(i32, i32, usize)>,
    /// How many partitions each `Produce` request carried. One entry per
    /// request, so `[4]` is one batched request and `[1,1,1,1]` is four
    /// unbatched ones — the difference this client's throughput rests on.
    pub produce_widths: Vec<usize>,
    /// How many partitions each `AddPartitionsToTxn` enrolled.
    pub enroll_widths: Vec<usize>,
}

impl Journal {
    pub fn count(&self, api: ApiKey) -> usize {
        self.requests.iter().filter(|a| **a == api).count()
    }
}

/// A connection the broker closes rather than answers — a rolling upgrade, an
/// idle reaper, a network blip. The client only discovers it on the next write.
#[derive(Debug, Clone, Copy)]
pub struct Close {
    pub api: ApiKey,
    pub times: usize,
}

/// A broker that accepts the request and never answers. Distinct from a close:
/// the socket stays open, so only a deadline gets the caller out.
#[derive(Debug, Clone, Copy)]
pub struct Hang {
    pub api: ApiKey,
    pub times: usize,
}

#[derive(Default)]
struct World {
    faults: Vec<Fault>,
    closes: Vec<Close>,
    hangs: Vec<Hang>,
    journal: Journal,
    /// Offsets handed out per partition, so base offsets advance like a log.
    next_offset: i64,
    /// Bumped by every `InitProducerId`, as a coordinator does.
    epoch: i16,
    /// The next sequence number each partition will accept. A real broker keeps
    /// this per producer id; one producer at a time is all the simulator needs.
    expected_sequence: std::collections::HashMap<i32, i32>,
}

thread_local! {
    static WORLD: RefCell<World> = RefCell::new(World::default());
}

/// Reset the world and install `faults`.
pub fn start(faults: Vec<Fault>) {
    start_with(faults, Vec::new(), Vec::new());
}

/// Reset the world with faults, connection closes and hangs.
pub fn start_with(faults: Vec<Fault>, closes: Vec<Close>, hangs: Vec<Hang>) {
    WORLD.with(|w| {
        *w.borrow_mut() = World {
            faults,
            closes,
            hangs,
            ..World::default()
        };
    });
}

/// Take the journal for assertions.
pub fn journal() -> Journal {
    WORLD.with(|w| std::mem::take(&mut w.borrow_mut().journal))
}

/// Pop a scripted fault for `api`, if one is due.
fn take_fault(world: &mut World, api: ApiKey) -> Option<i16> {
    let idx = world
        .faults
        .iter()
        .position(|f| f.api == api && f.times > 0)?;
    if world.faults[idx].after > 0 {
        world.faults[idx].after -= 1;
        return None;
    }
    world.faults[idx].times -= 1;
    Some(world.faults[idx].code)
}

/// An in-memory socket: what the client writes is answered immediately.
pub struct SimStream {
    inbox: VecDeque<u8>,
    /// Set when the scripted broker closed this connection. Reads then report
    /// end-of-stream, exactly as a real closed socket does.
    closed: bool,
    /// Set when the broker accepted the request and will never answer. Reads
    /// then never complete — which is the whole difference from a close, and
    /// the only case a deadline is needed for.
    hung: bool,
}

/// The simulated runtime.
pub struct Sim;

impl Transport for Sim {
    type Stream = SimStream;

    async fn connect(&self, _addr: &str) -> io::Result<Self::Stream> {
        Ok(SimStream {
            inbox: VecDeque::new(),
            closed: false,
            hung: false,
        })
    }

    async fn read(stream: &mut Self::Stream, buf: &mut [u8]) -> io::Result<usize> {
        if stream.hung {
            // Never resolves. Only `with_timeout` gets the caller out, which is
            // exactly what this case exists to exercise.
            std::future::pending::<()>().await;
        }
        if stream.closed {
            // Zero bytes is how a peer says "gone"; the client must treat it as
            // a dead connection rather than an empty response.
            return Ok(0);
        }
        if stream.inbox.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "simulated broker had nothing to say",
            ));
        }
        let n = buf.len().min(stream.inbox.len());
        for slot in buf.iter_mut().take(n) {
            *slot = stream.inbox.pop_front().expect("checked len");
        }
        Ok(n)
    }

    async fn write_all(stream: &mut Self::Stream, buf: &[u8]) -> io::Result<()> {
        match WORLD.with(|w| handle(&mut w.borrow_mut(), buf)) {
            Answer::Bytes(response) => stream.inbox.extend(response),
            Answer::Closed => stream.closed = true,
            // The socket stays open and nothing arrives: only a timeout ends
            // this, which is the case a closed connection would never produce.
            Answer::Hang => stream.hung = true,
        }
        Ok(())
    }

    /// **No clock.** Retry backoff is free, so an adversarial test that forces
    /// forty retries runs in microseconds and deterministically.
    async fn sleep(_dur: Duration) {}
}

/// What the scripted broker does with a request.
enum Answer {
    Bytes(Vec<u8>),
    Closed,
    Hang,
}

/// Decode one framed request and produce the framed response.
fn handle(world: &mut World, wire: &[u8]) -> Answer {
    let mut buf = Bytes::copy_from_slice(&wire[4..]); // strip the length prefix

    // The header must be decoded with the *request* header version for this
    // api key and version — the same rule the client follows.
    let api_key_raw = i16::from_be_bytes([buf[0], buf[1]]);
    let version = i16::from_be_bytes([buf[2], buf[3]]);
    let api_key = ApiKey::try_from(api_key_raw).expect("known api key");
    let header = RequestHeader::decode(&mut buf, api_key.request_header_version(version))
        .expect("decode request header");
    world.journal.requests.push(api_key);

    if let Some(idx) = world
        .closes
        .iter()
        .position(|c| c.api == api_key && c.times > 0)
    {
        world.closes[idx].times -= 1;
        return Answer::Closed;
    }
    if let Some(idx) = world
        .hangs
        .iter()
        .position(|h| h.api == api_key && h.times > 0)
    {
        world.hangs[idx].times -= 1;
        return Answer::Hang;
    }

    let fault = take_fault(world, api_key);
    let body = match api_key {
        ApiKey::ApiVersions => encode(api_key, version, &header, &{
            let mut r = ApiVersionsResponse::default();
            r.error_code = fault.unwrap_or(0);
            r
        }),
        ApiKey::Metadata => {
            let req = MetadataRequest::decode(&mut buf, version).expect("metadata request");
            encode(api_key, version, &header, &metadata_response(&req, fault))
        }
        ApiKey::FindCoordinator => encode(api_key, version, &header, &{
            let mut r = FindCoordinatorResponse::default();
            r.error_code = fault.unwrap_or(0);
            r.node_id = BrokerId(1);
            r.host = StrBytes::from_static_str("broker-1");
            r.port = 9092;
            r
        }),
        ApiKey::InitProducerId => encode(api_key, version, &header, &{
            let mut r = InitProducerIdResponse::default();
            r.error_code = fault.unwrap_or(0);
            if r.error_code == 0 {
                world.epoch += 1;
            }
            r.producer_id = ProducerId(7);
            r.producer_epoch = world.epoch;
            r
        }),
        ApiKey::AddPartitionsToTxn => {
            let enrolled = AddPartitionsToTxnRequest::decode(&mut buf.clone(), version)
                .map(|r| {
                    r.v3_and_below_topics
                        .iter()
                        .map(|t| t.partitions.len())
                        .sum::<usize>()
                })
                .unwrap_or(0);
            world.journal.enroll_widths.push(enrolled);
            let mut partition =
                add_partitions_to_txn_response::AddPartitionsToTxnPartitionResult::default();
            partition.partition_index = 0;
            partition.partition_error_code = fault.unwrap_or(0);
            let mut topic =
                add_partitions_to_txn_response::AddPartitionsToTxnTopicResult::default();
            topic.name = TopicName(StrBytes::from_static_str("t"));
            topic.results_by_partition = vec![partition];
            let mut r = AddPartitionsToTxnResponse::default();
            r.results_by_topic_v3_and_below = vec![topic];
            encode(api_key, version, &header, &r)
        }
        ApiKey::EndTxn => encode(api_key, version, &header, &{
            let mut r = EndTxnResponse::default();
            r.error_code = fault.unwrap_or(0);
            r
        }),
        ApiKey::Produce => {
            let req = ProduceRequest::decode(&mut buf, version).expect("produce request");
            encode(
                api_key,
                version,
                &header,
                &produce_response(world, &req, fault),
            )
        }
        ApiKey::ListOffsets => {
            let mut partition = list_offsets_response::ListOffsetsPartitionResponse::default();
            partition.partition_index = 0;
            partition.error_code = fault.unwrap_or(0);
            partition.offset = 0;
            let mut topic = list_offsets_response::ListOffsetsTopicResponse::default();
            topic.name = TopicName(StrBytes::from_static_str("t"));
            topic.partitions = vec![partition];
            let mut r = ListOffsetsResponse::default();
            r.topics = vec![topic];
            encode(api_key, version, &header, &r)
        }
        other => panic!("the simulator was not taught {other:?}"),
    };
    Answer::Bytes(body)
}

fn metadata_response(req: &MetadataRequest, fault: Option<i16>) -> MetadataResponse {
    let mut broker = metadata_response::MetadataResponseBroker::default();
    broker.node_id = BrokerId(1);
    broker.host = StrBytes::from_static_str("broker-1");
    broker.port = 9092;

    let name = req
        .topics
        .as_ref()
        .and_then(|t| t.first())
        .and_then(|t| t.name.clone())
        .unwrap_or(TopicName(StrBytes::from_static_str("t")));

    // Four partitions, all led by the one simulated broker: enough for keys to
    // spread, so a test can tell one batched request from four unbatched ones.
    let partitions = (0..4)
        .map(|index| {
            let mut partition = metadata_response::MetadataResponsePartition::default();
            partition.partition_index = index;
            partition.leader_id = BrokerId(1);
            partition
        })
        .collect();

    let mut topic = metadata_response::MetadataResponseTopic::default();
    topic.name = Some(name);
    topic.error_code = fault.unwrap_or(0);
    topic.partitions = partitions;

    let mut r = MetadataResponse::default();
    r.brokers = vec![broker];
    r.topics = vec![topic];
    r
}

/// The interesting one: record what was accepted, so a test can prove a retry
/// did **not** write twice.
fn produce_response(
    world: &mut World,
    req: &ProduceRequest,
    fault: Option<i16>,
) -> ProduceResponse {
    let code = fault.unwrap_or(0);

    world.journal.produce_widths.push(
        req.topic_data
            .iter()
            .map(|t| t.partition_data.len())
            .sum::<usize>(),
    );

    // **One response entry per partition in the request.** A broker answers
    // every partition it was asked about, and a client that batches depends on
    // that to know which ones landed.
    let mut partition_responses = Vec::new();
    for topic in &req.topic_data {
        for partition in &topic.partition_data {
            let mut response = produce_response::PartitionProduceResponse::default();
            response.index = partition.index;
            response.error_code = code;

            if code == 0 {
                if let Some(records) = partition.records.clone() {
                    let mut bytes = records;
                    let set = kafka_protocol::records::RecordBatchDecoder::decode(&mut bytes)
                        .expect("decode produced batch");
                    let base_sequence = set.records.first().map_or(-1, |r| r.sequence);

                    // **The sequence check a real broker does.** Without it the
                    // simulator accepts anything and a pipelined producer that
                    // gapped or reordered its batches would still pass — which
                    // is precisely the bug pipelining risks. 45 is
                    // OUT_OF_ORDER_SEQUENCE_NUMBER, 46 DUPLICATE_SEQUENCE_NUMBER.
                    let expected = world.expected_sequence.entry(partition.index).or_insert(0);
                    if base_sequence < *expected {
                        response.error_code = 46;
                    } else if base_sequence > *expected {
                        response.error_code = 45;
                    } else {
                        *expected += set.records.len() as i32;
                        world.journal.produced.push((
                            partition.index,
                            base_sequence,
                            set.records.len(),
                        ));
                        response.base_offset = world.next_offset;
                        world.next_offset += set.records.len() as i64;
                    }
                }
            }
            partition_responses.push(response);
        }
    }

    let mut topic_response = produce_response::TopicProduceResponse::default();
    topic_response.name = TopicName(StrBytes::from_static_str("t"));
    topic_response.partition_responses = partition_responses;

    let mut r = ProduceResponse::default();
    r.responses = vec![topic_response];
    r
}

fn encode<R: Encodable>(
    api_key: ApiKey,
    version: i16,
    header: &RequestHeader,
    resp: &R,
) -> Vec<u8> {
    let mut out = BytesMut::new();
    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = header.correlation_id;
    response_header
        .encode(&mut out, api_key.response_header_version(version))
        .expect("encode response header");
    resp.encode(&mut out, version).expect("encode response");

    let mut framed = Vec::with_capacity(out.len() + 4);
    framed.extend_from_slice(
        &i32::try_from(out.len())
            .expect("response fits")
            .to_be_bytes(),
    );
    framed.extend_from_slice(&out);
    framed
}

/// Drive a future to completion with no runtime at all.
///
/// Every future here is IO-free — the simulator answers inline — so there is
/// nothing to wait on and a plain block-on suffices.
pub fn drive<F: std::future::Future>(fut: F) -> F::Output {
    futures_lite::future::block_on(fut)
}
