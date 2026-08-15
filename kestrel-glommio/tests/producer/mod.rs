//! A minimal producer, for putting data in front of the consumer under test.
//!
//! **Test scaffolding, not library code.** It drives `kafka-protocol` directly,
//! panics on everything, and implements only what the tests need. `kestrel`'s
//! own producer is deliberately not used here: a test whose fixture is the code
//! under test cannot fail honestly.
//!
//! Its one non-obvious behaviour is the one P0 found: transaction sequence
//! numbers continue across transactions.

// The producer tests use only `create_topic`; the rest is what the consumer
// tests need. Shared rather than split so there is one scaffolding producer to
// keep correct.
#![allow(dead_code)]

use bytes::{Bytes, BytesMut};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;

use kafka_protocol::messages::{
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    metadata_request::MetadataRequestTopic, MetadataRequest, MetadataResponse,
    create_topics_request::CreatableTopic,
    produce_request::{PartitionProduceData, TopicProduceData},
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey, CreateTopicsRequest,
    CreateTopicsResponse, EndTxnRequest, EndTxnResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, InitProducerIdRequest, InitProducerIdResponse, ProduceRequest,
    ProduceResponse, ProducerId, TopicName, TransactionalId,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use kestrel_core::Connection;

pub struct TestProducer {
    stream: TcpStream,
    conn: Connection,
    topic: String,
    txn_id: String,
    producer_id: ProducerId,
    producer_epoch: i16,
    /// Continues across transactions — restarting it makes the broker treat the
    /// next transaction as a duplicate, answer `Ok` with the *original* base
    /// offset, and write nothing. Found in P0; it is why the tests below would
    /// otherwise silently assert on an empty topic.
    next_sequence: i32,
}

impl TestProducer {
    pub async fn connect(addr: &str, topic: &str) -> Self {
        let stream = TcpStream::connect(addr).await.expect("connect");
        let txn_id = format!("{topic}-txn");
        Self {
            stream,
            conn: Connection::new(StrBytes::from_static_str("kestrel-test-producer")),
            topic: topic.to_owned(),
            txn_id,
            producer_id: ProducerId(-1),
            producer_epoch: -1,
            next_sequence: 0,
        }
    }

    async fn call<Req, Resp>(&mut self, api_key: ApiKey, version: i16, req: &Req) -> Resp
    where
        Req: kafka_protocol::protocol::Encodable,
        Resp: kafka_protocol::protocol::Decodable,
    {
        let wire = self.conn.request(api_key, version, req).expect("encode");
        self.stream.write_all(&wire).await.expect("write");
        self.stream.flush().await.expect("flush");
        loop {
            if let Some(resp) = self.conn.next_response().expect("frame") {
                return Connection::decode(&resp).expect("decode");
            }
            let mut buf = [0u8; 16 * 1024];
            let n = self.stream.read(&mut buf).await.expect("read");
            assert!(n > 0, "broker closed the connection");
            self.conn.push_bytes(&buf[..n]);
        }
    }

    /// Create the topic **and wait for a leader**.
    ///
    /// Producing the instant `CreateTopics` returns gets error 6
    /// (`NOT_LEADER_OR_FOLLOWER`): creation is acknowledged before leadership
    /// has propagated. Real clients hide this behind a metadata refresh and a
    /// retry; the tests wait explicitly so a failure here is never mistaken for
    /// a consumer bug.
    pub async fn create_topic(&mut self) {
        self.create_topic_with_partitions(1).await;
    }

    pub async fn create_topic_with_partitions(&mut self, partitions: i32) {
        let mut topic = CreatableTopic::default();
        topic.name = TopicName(StrBytes::from_string(self.topic.clone()));
        topic.num_partitions = partitions;
        topic.replication_factor = 1;

        let mut req = CreateTopicsRequest::default();
        req.timeout_ms = 5_000;
        req.topics = vec![topic];

        let resp: CreateTopicsResponse = self.call(ApiKey::CreateTopics, 7, &req).await;
        for t in &resp.topics {
            // 36 = TOPIC_ALREADY_EXISTS.
            assert!(
                t.error_code == 0 || t.error_code == 36,
                "CreateTopics error {}: {:?}",
                t.error_code,
                t.error_message
            );
        }
        self.await_leader().await;
    }

    async fn await_leader(&mut self) {
        for attempt in 0..40 {
            let mut topic = MetadataRequestTopic::default();
            topic.name = Some(TopicName(StrBytes::from_string(self.topic.clone())));
            let mut req = MetadataRequest::default();
            req.topics = Some(vec![topic]);

            let resp: MetadataResponse = self.call(ApiKey::Metadata, 12, &req).await;
            let ready = resp.topics.iter().any(|t| {
                t.error_code == 0
                    && t.partitions
                        .iter()
                        .any(|p| p.error_code == 0 && p.leader_id.0 >= 0)
            });
            if ready {
                return;
            }
            Self::backoff(attempt).await;
        }
        panic!("topic {} never got a leader", self.topic);
    }

    /// `FindCoordinator` then `InitProducerId`, both retrying the coordinator
    /// warm-up codes: a cold cluster answers 15 (the internal topic is created
    /// lazily by this very call), then 14, then 16 — the last *after* a
    /// successful discovery.
    pub async fn init_transactions(&mut self) {
        let mut find = FindCoordinatorRequest::default();
        find.key = StrBytes::from_string(self.txn_id.clone());
        find.key_type = 1; // TRANSACTION

        for attempt in 0..40 {
            let resp: FindCoordinatorResponse =
                self.call(ApiKey::FindCoordinator, 3, &find).await;
            match resp.error_code {
                0 => break,
                14..=16 => Self::backoff(attempt).await,
                code => panic!("FindCoordinator error {code}: {:?}", resp.error_message),
            }
        }

        let mut init = InitProducerIdRequest::default();
        init.transactional_id = Some(TransactionalId(StrBytes::from_string(self.txn_id.clone())));
        init.transaction_timeout_ms = 60_000;
        init.producer_id = ProducerId(-1);
        init.producer_epoch = -1;

        for attempt in 0..40 {
            let resp: InitProducerIdResponse = self.call(ApiKey::InitProducerId, 4, &init).await;
            match resp.error_code {
                0 => {
                    self.producer_id = resp.producer_id;
                    self.producer_epoch = resp.producer_epoch;
                    // A fresh epoch resets the sequence — the one case where
                    // restarting at zero is correct.
                    self.next_sequence = 0;
                    return;
                }
                14..=16 => Self::backoff(attempt).await,
                code => panic!("InitProducerId error {code}"),
            }
        }
        panic!("InitProducerId: coordinator never became available");
    }

    async fn backoff(_attempt: usize) {
        glommio::timer::sleep(std::time::Duration::from_millis(250)).await;
    }

    /// `AddPartitionsToTxn` — the coordinator must know a partition is in the
    /// transaction before records are produced to it.
    pub async fn begin(&mut self) {
        let mut topic = AddPartitionsToTxnTopic::default();
        topic.name = TopicName(StrBytes::from_string(self.topic.clone()));
        topic.partitions = vec![0];

        let mut req = AddPartitionsToTxnRequest::default();
        req.v3_and_below_transactional_id =
            TransactionalId(StrBytes::from_string(self.txn_id.clone()));
        req.v3_and_below_producer_id = self.producer_id;
        req.v3_and_below_producer_epoch = self.producer_epoch;
        req.v3_and_below_topics = vec![topic];

        // 51 (`CONCURRENT_TRANSACTIONS`) means the previous transaction's
        // markers are still being written: starting a second transaction
        // immediately after ending the first hits it every time. Retriable, and
        // another entry for the taxonomy P2 inherits.
        for attempt in 0..40 {
            let resp: AddPartitionsToTxnResponse =
                self.call(ApiKey::AddPartitionsToTxn, 3, &req).await;
            let code = resp
                .results_by_topic_v3_and_below
                .iter()
                .flat_map(|t| t.results_by_partition.iter())
                .map(|p| p.partition_error_code)
                .find(|c| *c != 0)
                .unwrap_or(0);
            match code {
                0 => return,
                51 => Self::backoff(attempt).await,
                code => panic!("AddPartitionsToTxn error {code}"),
            }
        }
        panic!("AddPartitionsToTxn: transaction never became startable");
    }

    pub async fn end(&mut self, committed: bool) {
        let mut req = EndTxnRequest::default();
        req.transactional_id = TransactionalId(StrBytes::from_string(self.txn_id.clone()));
        req.producer_id = self.producer_id;
        req.producer_epoch = self.producer_epoch;
        req.committed = committed;

        let resp: EndTxnResponse = self.call(ApiKey::EndTxn, 3, &req).await;
        assert_eq!(resp.error_code, 0, "EndTxn error {}", resp.error_code);
    }

    pub async fn produce_plain(&mut self, count: i64) {
        self.produce(count, false).await;
    }

    pub async fn produce_txn(&mut self, count: i64) {
        self.produce(count, true).await;
    }

    async fn produce(&mut self, count: i64, transactional: bool) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let (producer_id, producer_epoch) = if transactional {
            (self.producer_id.0, self.producer_epoch)
        } else {
            (-1, -1)
        };
        let base_sequence = self.next_sequence;

        let records: Vec<Record> = (0..count)
            .map(|i| Record {
                transactional,
                control: false,
                partition_leader_epoch: 0,
                producer_id,
                producer_epoch,
                timestamp_type: TimestampType::Creation,
                offset: i,
                sequence: base_sequence + i as i32,
                timestamp: now,
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from(format!("v{i}"))),
                headers: Default::default(),
            })
            .collect();

        let mut batch = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut batch,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .expect("encode batch");

        let mut partition = PartitionProduceData::default();
        partition.index = 0;
        partition.records = Some(batch.freeze());

        let mut topic_data = TopicProduceData::default();
        topic_data.name = TopicName(StrBytes::from_string(self.topic.clone()));
        topic_data.partition_data = vec![partition];

        let mut req = ProduceRequest::default();
        req.acks = -1;
        req.timeout_ms = 5_000;
        req.topic_data = vec![topic_data];
        if transactional {
            req.transactional_id =
                Some(TransactionalId(StrBytes::from_string(self.txn_id.clone())));
        }

        // Retry on 6 (`NOT_LEADER_OR_FOLLOWER`) after a metadata refresh, which
        // is `Disposition::RefreshMetadata` — the same thing the library will
        // do in P2. A freshly created partition reports it for a moment even on
        // a single-broker cluster, *after* metadata already named a leader.
        //
        // Resending is safe precisely because the sequence number does not
        // change: that is what the idempotent producer is for, and the broker
        // deduplicates if the first attempt did land.
        let mut resp: ProduceResponse = self.call(ApiKey::Produce, 9, &req).await;
        for attempt in 0..40 {
            let code = resp
                .responses
                .iter()
                .flat_map(|t| t.partition_responses.iter())
                .map(|p| p.error_code)
                .find(|c| *c != 0)
                .unwrap_or(0);
            if code != 6 {
                break;
            }
            Self::backoff(attempt).await;
            self.await_leader().await;
            resp = self.call(ApiKey::Produce, 9, &req).await;
        }
        for t in &resp.responses {
            for p in &t.partition_responses {
                assert_eq!(
                    p.error_code, 0,
                    "Produce to {}-{} error {} ({:?})",
                    t.name.0.as_str(),
                    p.index,
                    p.error_code,
                    p.error_message
                );
            }
        }
        if transactional {
            self.next_sequence += i32::try_from(count).unwrap();
        }
    }
}
