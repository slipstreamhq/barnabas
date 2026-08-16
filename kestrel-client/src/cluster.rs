//! Connections to brokers, and the routing that decides which one to use.
//!
//! A [`Cluster`] owns one connection per broker plus the metadata that says
//! which broker leads which partition. It is the piece that turns "send this
//! fetch" into "send it to node 3, and if node 3 says it is no longer the
//! leader, find out who is and try again".
//!
//! # Locality is the binding's choice
//!
//! The connection map is a plain `HashMap` behind `&mut self`, with no locking
//! anywhere. Under a per-core binding that is simply correct — the sockets
//! belong to the core that opened them and there is no other thread to contend
//! with. Under a work-stealing binding the handle is what carries the
//! synchronisation, not this map.
//!
//! One consequence is worth stating, because it is the interesting cost of the
//! design: **each core keeps its own connections**, so a node with C cores and
//! B brokers holds C×B connections rather than B. In exchange every partition a
//! core owns shares one connection to each broker, and their fetches batch into
//! one request — which is strictly better than a client per *partition*, the
//! shape a wrapper around a threaded C client forces.

use std::collections::HashMap;
use std::time::Duration;

use kafka_protocol::messages::{
    metadata_request::MetadataRequestTopic, ApiKey, ApiVersionsRequest, ApiVersionsResponse,
    MetadataRequest, MetadataResponse, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest, SaslHandshakeResponse, TopicName,
};
use bytes::Bytes;
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kestrel_core::{BrokerAddr, Connection, Metadata};

use crate::sasl::{plain_message, Credentials, SaslMechanism, ScramExchange};
use crate::timeout::with_timeout;
use crate::{check, Error, Result, Transport};

/// How long a single request may take before its connection is considered
/// broken.
///
/// A broker that accepts a connection and then never answers is
/// indistinguishable from a slow one, so a deadline is the only way out. It is
/// deliberately generous: this is a liveness backstop, not a latency budget.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// One broker connection.
pub(crate) struct Broker<T: Transport> {
    stream: T::Stream,
    conn: Connection,
}

impl<T: Transport> Broker<T> {
    pub(crate) async fn connect(
        transport: &T,
        addr: &str,
        client_id: &str,
        credentials: Option<&Credentials>,
    ) -> Result<Self> {
        let stream = transport
            .connect(addr)
            .await
            .map_err(|source| Error::Connect {
                addr: addr.to_owned(),
                source,
            })?;
        let mut me = Self {
            stream,
            conn: Connection::new(StrBytes::from_string(client_id.to_owned())),
        };

        // `ApiVersions` first, as every client does: an unsupported version
        // then fails at connect rather than in the middle of a fetch loop.
        let mut req = ApiVersionsRequest::default();
        req.client_software_name = StrBytes::from_string(client_id.to_owned());
        req.client_software_version = StrBytes::from_static_str("0.0.1");
        let resp: ApiVersionsResponse = me.call(ApiKey::ApiVersions, 3, &req).await?;
        check("ApiVersions", resp.error_code)?;

        // Authentication comes after `ApiVersions` and before anything else: a
        // broker on a SASL listener rejects every other request until it has
        // happened.
        if let Some(credentials) = credentials {
            me.authenticate(credentials).await?;
        }

        Ok(me)
    }

    /// `SaslHandshake` then one or more `SaslAuthenticate` round trips.
    ///
    /// Version 1 of `SaslAuthenticate`, which wraps the SASL bytes in a Kafka
    /// request. The older form wrote raw SASL frames onto the socket ahead of
    /// the protocol, which is unframed, undiagnosable, and not supported here.
    async fn authenticate(&mut self, credentials: &Credentials) -> Result<()> {
        let mut handshake = SaslHandshakeRequest::default();
        handshake.mechanism = StrBytes::from_string(credentials.mechanism.as_str().to_owned());

        let resp: SaslHandshakeResponse = self.call(ApiKey::SaslHandshake, 1, &handshake).await?;
        if resp.error_code != 0 {
            // The broker lists what it *would* accept, which is the single most
            // useful thing to say when authentication fails at this stage.
            let offered: Vec<String> = resp
                .mechanisms
                .iter()
                .map(|m| m.to_string())
                .collect();
            return Err(Error::Sasl(format!(
                "broker rejected mechanism {}; it offers {}",
                credentials.mechanism.as_str(),
                if offered.is_empty() {
                    "nothing".to_owned()
                } else {
                    offered.join(", ")
                }
            )));
        }

        match credentials.mechanism {
            SaslMechanism::Plain => {
                self.sasl_exchange(plain_message(credentials)).await?;
            }
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                let nonce = self.scram_nonce();
                let (mut exchange, client_first) = ScramExchange::start(credentials, &nonce);
                let server_first = self.sasl_exchange(client_first).await?;
                let client_final = exchange
                    .client_final(&server_first)
                    .map_err(|e| Error::Sasl(e.to_string()))?;
                let server_final = self.sasl_exchange(client_final).await?;
                // Verifying is what proves the peer knows the password. A
                // client that skips it authenticates to anyone.
                exchange
                    .verify(&server_final)
                    .map_err(|e| Error::Sasl(e.to_string()))?;
            }
        }
        Ok(())
    }

    async fn sasl_exchange(&mut self, bytes: Vec<u8>) -> Result<Bytes> {
        let mut req = SaslAuthenticateRequest::default();
        req.auth_bytes = Bytes::from(bytes);
        let resp: SaslAuthenticateResponse =
            self.call(ApiKey::SaslAuthenticate, 1, &req).await?;
        if resp.error_code != 0 {
            return Err(Error::Sasl(
                resp.error_message
                    .map_or_else(|| "authentication failed".to_owned(), |m| m.to_string()),
            ));
        }
        Ok(resp.auth_bytes)
    }

    /// A nonce for SCRAM: unique per exchange, and unpredictable enough that a
    /// replayed server-first cannot be matched to a future exchange.
    ///
    /// Built from the address of a stack local and a counter rather than a
    /// `rand` dependency — SCRAM needs the client nonce to be *fresh*, not
    /// cryptographically random, since the security rests on the shared secret.
    fn scram_nonce(&self) -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let local = 0u8;
        let entropy = std::ptr::addr_of!(local) as usize;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        format!("{counter:x}{entropy:x}{nanos:x}")
    }

    /// Send one request and read its response.
    ///
    /// Strictly one in flight, which is all an assign-only consumer needs: it
    /// has a single outstanding fetch per partition by construction. The core
    /// already correlates pipelined requests ([`Connection::in_flight`]), so
    /// raising this is a change here, not there.
    pub(crate) async fn call<Req, Resp>(
        &mut self,
        api_key: ApiKey,
        version: i16,
        req: &Req,
    ) -> Result<Resp>
    where
        Req: Encodable,
        Resp: Decodable,
    {
        let wire = self.conn.request(api_key, version, req)?;
        T::write_all(&mut self.stream, &wire).await?;

        loop {
            if let Some(resp) = self.conn.next_response()? {
                return Ok(Connection::decode(&resp)?);
            }
            let mut buf = [0u8; 16 * 1024];
            let n = T::read(&mut self.stream, &mut buf).await?;
            if n == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "broker closed the connection",
                )));
            }
            self.conn.push_bytes(&buf[..n]);
        }
    }
}

/// Connections plus the cluster map.
pub struct Cluster<T: Transport> {
    transport: T,
    client_id: String,
    bootstrap: Vec<String>,
    /// Whether a metadata request may create the topic.
    ///
    /// **True by default, which is what librdkafka and the Java client do.** A
    /// producer's first write to a new topic otherwise fails with error 3
    /// rather than creating it — and a caller that expected the usual
    /// behaviour has no way to tell that from a genuinely missing topic.
    allow_auto_topic_creation: bool,
    /// Keyed by `host:port` rather than node id: a broker that restarts with a
    /// new id at the same address should reuse the socket, and a bootstrap
    /// address has no node id until the first `Metadata` reply.
    conns: HashMap<String, Broker<T>>,
    metadata: Metadata,
    request_timeout: Duration,
    credentials: Option<Credentials>,
}

impl<T: Transport> Cluster<T> {
    /// Connect to the first reachable bootstrap address and load metadata.
    ///
    /// # Errors
    /// If no bootstrap address answers.
    pub async fn connect(transport: T, bootstrap: &[String], client_id: &str) -> Result<Self> {
        let mut me = Self {
            transport,
            client_id: client_id.to_owned(),
            bootstrap: bootstrap.to_vec(),
            conns: HashMap::new(),
            metadata: Metadata::new(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            credentials: None,
            allow_auto_topic_creation: true,
        };
        // Touch one bootstrap address so a bad configuration fails here rather
        // than at the first fetch.
        me.any_broker().await?;
        Ok(me)
    }

    /// Authenticate every connection with `credentials`.
    ///
    /// Must be set before the first request; connections already open are not
    /// re-authenticated, because Kafka has no way to do so.
    pub fn set_credentials(&mut self, credentials: Credentials) {
        self.credentials = Some(credentials);
    }

    /// How long a request may take before its connection is dropped as broken.
    pub fn set_request_timeout(&mut self, timeout: Duration) {
        self.request_timeout = timeout;
    }

    /// Send a request to a specific broker, reconnecting once if the
    /// connection turns out to be dead.
    ///
    /// **This is what makes a broker restart survivable.** A pooled connection
    /// can be closed at any time — a rolling upgrade, an idle reaper, a network
    /// blip — and the client only finds out when it writes to it. Without this,
    /// the dead socket stays in the pool and every subsequent request fails
    /// forever, which is a client that dies the first time its cluster is
    /// maintained.
    ///
    /// **A timed-out request also drops the connection**, and that is not
    /// caution: the response may still arrive later, and reading it as the
    /// answer to the *next* request would desynchronise the stream. Kafka
    /// answers in order, so a request abandoned mid-flight poisons everything
    /// behind it.
    ///
    /// Retried exactly once: a second failure is the peer, not the socket.
    pub(crate) async fn call_at<Req, Resp>(
        &mut self,
        addr: &str,
        api_key: ApiKey,
        version: i16,
        req: &Req,
    ) -> Result<Resp>
    where
        Req: Encodable,
        Resp: Decodable,
    {
        let timeout = self.request_timeout;
        for attempt in 0..2 {
            let broker = self.broker_at(addr).await?;
            let outcome = with_timeout::<T, _>(timeout, broker.call(api_key, version, req)).await;

            match outcome {
                Some(Ok(resp)) => return Ok(resp),
                Some(Err(Error::Io(e))) => {
                    self.conns.remove(addr);
                    if attempt == 1 {
                        return Err(Error::Io(e));
                    }
                }
                Some(Err(e)) => return Err(e),
                None => {
                    self.conns.remove(addr);
                    return Err(Error::Timeout {
                        op: api_key,
                        addr: addr.to_owned(),
                    });
                }
            }
        }
        unreachable!("the loop returns on its last attempt")
    }

    /// As [`Self::call_at`], for requests any broker can serve.
    pub(crate) async fn call_any<Req, Resp>(
        &mut self,
        api_key: ApiKey,
        version: i16,
        req: &Req,
    ) -> Result<Resp>
    where
        Req: Encodable,
        Resp: Decodable,
    {
        let addr = {
            let broker_addr = self.any_broker_addr().await?;
            broker_addr
        };
        self.call_at(&addr, api_key, version, req).await
    }

    /// The address of some live-looking broker, connecting if the pool is
    /// empty.
    async fn any_broker_addr(&mut self) -> Result<String> {
        if let Some(addr) = self.conns.keys().next().cloned() {
            return Ok(addr);
        }
        let mut last_err = None;
        for addr in self.bootstrap.clone() {
            match Broker::<T>::connect(&self.transport, &addr, &self.client_id, self.credentials.as_ref())
                .await
            {
                Ok(broker) => {
                    self.conns.insert(addr.clone(), broker);
                    return Ok(addr);
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or(Error::Missing("bootstrap address")))
    }

    /// Whether metadata requests may create missing topics. See the field.
    pub fn set_allow_auto_topic_creation(&mut self, allow: bool) {
        self.allow_auto_topic_creation = allow;
    }

    /// The cluster map, for callers that want to inspect leadership.
    #[must_use]
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// A connection to *some* broker: an existing one if there is one, else the
    /// first bootstrap address that answers.
    ///
    /// Used for requests that any broker can serve — `Metadata` above all,
    /// which is what makes bootstrapping work at all.
    pub(crate) async fn any_broker(&mut self) -> Result<&mut Broker<T>> {
        if let Some(addr) = self.conns.keys().next().cloned() {
            return Ok(self.conns.get_mut(&addr).expect("just found"));
        }

        let mut last_err = None;
        for addr in self.bootstrap.clone() {
            match Broker::<T>::connect(&self.transport, &addr, &self.client_id, self.credentials.as_ref())
                .await
            {
                Ok(broker) => {
                    self.conns.insert(addr.clone(), broker);
                    return Ok(self.conns.get_mut(&addr).expect("just inserted"));
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or(Error::Missing("bootstrap address")))
    }

    /// Connect to `addr` if not already connected, and return it.
    pub(crate) async fn broker_at(&mut self, addr: &str) -> Result<&mut Broker<T>> {
        if !self.conns.contains_key(addr) {
            let broker =
                Broker::<T>::connect(&self.transport, addr, &self.client_id, self.credentials.as_ref())
                    .await?;
            self.conns.insert(addr.to_owned(), broker);
        }
        Ok(self.conns.get_mut(addr).expect("just inserted"))
    }

    /// Ask any broker for `topic`'s metadata and merge it in.
    ///
    /// # Errors
    /// If no broker answers, or the topic carries an error code.
    pub async fn refresh_metadata(&mut self, topic: &str) -> Result<()> {
        let mut req_topic = MetadataRequestTopic::default();
        req_topic.name = Some(TopicName(StrBytes::from_string(topic.to_owned())));
        let mut req = MetadataRequest::default();
        req.topics = Some(vec![req_topic]);
        req.allow_auto_topic_creation = self.allow_auto_topic_creation;

        let resp: MetadataResponse = self.call_any(ApiKey::Metadata, 12, &req).await?;
        for t in &resp.topics {
            let code = kestrel_core::ErrorCode(t.error_code);
            // An invalid-metadata code is "not yet", not "no": a topic being
            // auto-created reports 3 until it exists. The response is still
            // merged — its brokers are real — and the caller retries.
            if !code.is_ok() && code.disposition() != kestrel_core::Disposition::RefreshMetadata {
                check("Metadata", t.error_code)?;
            }
        }
        self.metadata.update(&resp);
        Ok(())
    }

    /// The address of `topic`/`partition`'s leader, refreshing if unknown.
    ///
    /// # Errors
    /// If metadata cannot be refreshed, or the partition still has no leader
    /// afterwards — which is what a partition mid-election looks like, and is
    /// [`Error::NoLeader`] so a caller can back off and try again rather than
    /// treat it as fatal.
    pub async fn leader_addr(&mut self, topic: &str, partition: i32) -> Result<String> {
        if let Some(addr) = self.metadata.leader_for(topic, partition) {
            return Ok(addr.addr());
        }
        self.refresh_metadata(topic).await?;
        self.metadata
            .leader_for(topic, partition)
            .map(BrokerAddr::addr)
            .ok_or_else(|| Error::NoLeader {
                topic: topic.to_owned(),
                partition,
            })
    }

    /// How many partitions `topic` has, refreshing metadata if the client has
    /// not seen it yet.
    ///
    /// # Errors
    /// If metadata cannot be refreshed, or the topic has no partitions after it.
    pub async fn partition_count(&mut self, topic: &str) -> Result<i32> {
        if self.metadata.partition_count(topic) == 0 {
            self.refresh_metadata(topic).await?;
        }
        let count = self.metadata.partition_count(topic);
        if count == 0 {
            return Err(Error::NoLeader {
                topic: topic.to_owned(),
                partition: -1,
            });
        }
        Ok(count)
    }

    /// Forget one partition's leader after the broker said it is not the
    /// leader. Per partition on purpose — see [`kestrel_core::metadata`].
    pub fn invalidate(&mut self, topic: &str, partition: i32) {
        self.metadata.invalidate_partition(topic, partition);
    }

    /// Number of open connections. Exposed because connection count is a real
    /// cost of the per-core design, and something a caller may want to watch.
    #[must_use]
    pub fn connection_count(&self) -> usize {
        self.conns.len()
    }
}
