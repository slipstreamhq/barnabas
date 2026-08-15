//! Connections to brokers, and the routing that decides which one to use.
//!
//! A [`Cluster`] owns one connection per broker plus the metadata that says
//! which broker leads which partition. It is the piece that turns "send this
//! fetch" into "send it to node 3, and if node 3 says it is no longer the
//! leader, find out who is and try again".
//!
//! # Per core, and not shared
//!
//! Connections are `glommio::net::TcpStream`s, so a `Cluster` belongs to the
//! core that built it and is `!Send`. That is why the map is a plain
//! `HashMap` behind `&mut self` with no locking anywhere: there is no other
//! thread to contend with, which is the whole reason to be per-core.
//!
//! One consequence is worth stating, because it is the interesting cost of the
//! design: **each core keeps its own connections**, so a node with C cores and
//! B brokers holds C×B connections rather than B. In exchange every partition a
//! core owns shares one connection to each broker, and their fetches batch into
//! one request — which is strictly better than a client per *partition*, the
//! shape a wrapper around a threaded C client forces.

use std::collections::HashMap;

use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;
use kafka_protocol::messages::{
    metadata_request::MetadataRequestTopic, ApiKey, ApiVersionsRequest, ApiVersionsResponse,
    MetadataRequest, MetadataResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kestrel_core::{BrokerAddr, Connection, Metadata};

use crate::{check, Error, Result};

/// One broker connection.
pub(crate) struct Broker {
    stream: TcpStream,
    conn: Connection,
}

impl Broker {
    pub(crate) async fn connect(addr: &str, client_id: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await.map_err(|e| Error::Connect {
            addr: addr.to_owned(),
            source: std::io::Error::other(e.to_string()),
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

        Ok(me)
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
        self.stream.write_all(&wire).await?;
        self.stream.flush().await?;

        loop {
            if let Some(resp) = self.conn.next_response()? {
                return Ok(Connection::decode(&resp)?);
            }
            let mut buf = [0u8; 16 * 1024];
            let n = self.stream.read(&mut buf).await?;
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
pub struct Cluster {
    client_id: String,
    bootstrap: Vec<String>,
    /// Keyed by `host:port` rather than node id: a broker that restarts with a
    /// new id at the same address should reuse the socket, and a bootstrap
    /// address has no node id until the first `Metadata` reply.
    conns: HashMap<String, Broker>,
    metadata: Metadata,
}

impl Cluster {
    /// Connect to the first reachable bootstrap address and load metadata.
    ///
    /// # Errors
    /// If no bootstrap address answers.
    pub async fn connect(bootstrap: &[String], client_id: &str) -> Result<Self> {
        let mut me = Self {
            client_id: client_id.to_owned(),
            bootstrap: bootstrap.to_vec(),
            conns: HashMap::new(),
            metadata: Metadata::new(),
        };
        // Touch one bootstrap address so a bad configuration fails here rather
        // than at the first fetch.
        me.any_broker().await?;
        Ok(me)
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
    pub(crate) async fn any_broker(&mut self) -> Result<&mut Broker> {
        if let Some(addr) = self.conns.keys().next().cloned() {
            return Ok(self.conns.get_mut(&addr).expect("just found"));
        }

        let mut last_err = None;
        for addr in self.bootstrap.clone() {
            match Broker::connect(&addr, &self.client_id).await {
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
    pub(crate) async fn broker_at(&mut self, addr: &str) -> Result<&mut Broker> {
        if !self.conns.contains_key(addr) {
            let broker = Broker::connect(addr, &self.client_id).await?;
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

        let resp: MetadataResponse = self.any_broker().await?.call(ApiKey::Metadata, 12, &req).await?;
        for t in &resp.topics {
            check("Metadata", t.error_code)?;
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
