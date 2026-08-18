//! The admin client: enough to create a topic, inspect a cluster, and trim a
//! log — no more.
//!
//! **Two routing rules, and both are load-bearing:**
//!
//! - **Topic creation, deletion and expansion go to the controller.** Every
//!   other broker answers `NOT_CONTROLLER` (41), and the controller moves on
//!   election, so a 41 forgets the cached controller and asks again rather than
//!   retrying the same broker forever. This is the same shape as the producer's
//!   `NOT_COORDINATOR` handling, for the same reason.
//! - **`DeleteRecords` goes to the partition leader**, like a produce or a
//!   fetch. It is not a cluster operation; it moves one log's start offset.
//!
//! `librdkafka`'s admin surface is far larger. This is the subset that makes it
//! possible to write a test suite and an operational tool without reaching for
//! a second client, which is the whole reason it exists.

use std::collections::BTreeMap;
use std::time::Duration;

use kafka_protocol::messages::{
    create_partitions_request::CreatePartitionsTopic,
    create_topics_request::CreatableTopic,
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
    delete_topics_request::DeleteTopicState,
    describe_configs_request::DescribeConfigsResource,
    ApiKey, CreatePartitionsRequest, CreatePartitionsResponse, CreateTopicsRequest,
    CreateTopicsResponse, DeleteRecordsRequest, DeleteRecordsResponse, DeleteTopicsRequest,
    DeleteTopicsResponse, DescribeConfigsRequest, DescribeConfigsResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use barnabas_core::{Disposition, ErrorCode};

use crate::cluster::Cluster;
use crate::{check, Error, Result, Transport};

/// Attempts for a request whose disposition says "retry" or "re-discover".
const MAX_RETRIES: usize = 20;
const BACKOFF: Duration = Duration::from_millis(50);

/// `NOT_CONTROLLER`. Named because the whole controller-routing rule turns on
/// it and a bare 41 in a match arm says nothing.
const NOT_CONTROLLER: i16 = 41;

/// A topic to create.
#[derive(Debug, Clone)]
pub struct NewTopic {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,
    /// Topic configuration, as the broker names it — `retention.ms`,
    /// `cleanup.policy`, and so on.
    pub config: BTreeMap<String, String>,
}

impl NewTopic {
    /// A topic with broker defaults for everything but its shape.
    #[must_use]
    pub fn new(name: impl Into<String>, partitions: i32, replication_factor: i16) -> Self {
        Self {
            name: name.into(),
            partitions,
            replication_factor,
            config: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.insert(key.into(), value.into());
        self
    }
}

/// One broker, as the cluster describes itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerInfo {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    /// Whether this broker is the controller.
    pub is_controller: bool,
}

/// Administrative operations, on one core like everything else here.
pub struct Admin<T: Transport> {
    cluster: Cluster<T>,
    timeout_ms: i32,
}

impl<T: Transport> Admin<T> {
    /// Connect to the cluster.
    ///
    /// # Errors
    /// If no bootstrap address answers.
    pub async fn connect(transport: T, bootstrap: &[String], client_id: &str) -> Result<Self> {
        Ok(Self {
            cluster: Cluster::connect(transport, bootstrap, client_id).await?,
            timeout_ms: 30_000,
        })
    }

    /// How long the **broker** may take to complete an operation before it
    /// gives up on it. Not a client-side deadline.
    pub fn set_operation_timeout(&mut self, timeout: Duration) {
        self.timeout_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
    }

    /// The underlying cluster, for callers that want metadata directly.
    pub fn cluster(&mut self) -> &mut Cluster<T> {
        &mut self.cluster
    }

    /// Send a request to the controller, re-discovering it on
    /// `NOT_CONTROLLER`.
    ///
    /// `error_of` returns the first error the response carries; `Ok(0)` means
    /// the whole response succeeded.
    async fn controller_call<Req, Resp, F>(
        &mut self,
        op: &'static str,
        api_key: ApiKey,
        version: i16,
        req: &Req,
        error_of: F,
    ) -> Result<Resp>
    where
        Req: kafka_protocol::protocol::Encodable,
        Resp: kafka_protocol::protocol::Decodable,
        F: Fn(&Resp) -> i16,
    {
        for attempt in 0..MAX_RETRIES {
            let addr = match self.cluster.controller_addr().await {
                Ok(addr) => addr,
                Err(Error::Missing("a controller")) if attempt + 1 < MAX_RETRIES => {
                    // A controller election is a wait, not a failure.
                    T::sleep(BACKOFF).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let resp: Resp = self.cluster.call_at(&addr, api_key, version, req).await?;

            let code = ErrorCode(error_of(&resp));
            if code.is_ok() {
                return Ok(resp);
            }
            if code.0 == NOT_CONTROLLER {
                self.cluster.invalidate_controller();
                T::sleep(BACKOFF).await;
                continue;
            }
            if code.disposition() == Disposition::Retry && attempt + 1 < MAX_RETRIES {
                T::sleep(BACKOFF).await;
                continue;
            }
            return Err(Error::Broker {
                op,
                code: code.0,
                disposition: code.disposition(),
            });
        }
        Err(Error::Broker {
            op,
            code: NOT_CONTROLLER,
            disposition: Disposition::Retry,
        })
    }

    /// Create topics.
    ///
    /// **`TOPIC_ALREADY_EXISTS` is an error here**, not a silent success. A
    /// caller who wants "create if absent" can say so by ignoring that code;
    /// a caller who does not want it and never learns is the one who ends up
    /// producing to a topic with the wrong partition count.
    ///
    /// # Errors
    /// If the controller rejects any of them.
    pub async fn create_topics(&mut self, topics: &[NewTopic]) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }
        let mut req = CreateTopicsRequest::default();
        req.timeout_ms = self.timeout_ms;
        req.topics = topics
            .iter()
            .map(|topic| {
                let mut entry = CreatableTopic::default();
                entry.name = TopicName(StrBytes::from_string(topic.name.clone()));
                entry.num_partitions = topic.partitions;
                entry.replication_factor = topic.replication_factor;
                entry.configs = topic
                    .config
                    .iter()
                    .map(|(key, value)| {
                        let mut config =
                            kafka_protocol::messages::create_topics_request::CreatableTopicConfig::default();
                        config.name = StrBytes::from_string(key.clone());
                        config.value = Some(StrBytes::from_string(value.clone()));
                        config
                    })
                    .collect();
                entry
            })
            .collect();

        let _: CreateTopicsResponse = self
            .controller_call(
                "CreateTopics",
                ApiKey::CreateTopics,
                5,
                &req,
                |r: &CreateTopicsResponse| {
                    r.topics
                        .iter()
                        .map(|t| t.error_code)
                        .find(|c| *c != 0)
                        .unwrap_or(0)
                },
            )
            .await?;

        // **Return when the topics are usable, not when the controller said
        // yes.** The two are seconds apart: a producer that writes immediately
        // after this asks a broker that has not heard about the topic and gets
        // "no leader", and a consumer that subscribes gets a partition count of
        // zero. Every caller would otherwise write this loop, and the ones who
        // forgot would have a test that fails once a fortnight.
        for topic in topics {
            for attempt in 0..MAX_RETRIES {
                let _ = self.cluster.refresh_metadata(&topic.name).await;
                if self.cluster.metadata().partition_count(&topic.name) >= topic.partitions {
                    break;
                }
                if attempt + 1 == MAX_RETRIES {
                    return Err(Error::NoLeader {
                        topic: topic.name.clone(),
                        partition: -1,
                    });
                }
                T::sleep(BACKOFF).await;
            }
        }
        Ok(())
    }

    /// Delete topics. Asynchronous on the broker: the response means the
    /// deletion was accepted, not that the log files are gone.
    ///
    /// # Errors
    /// If the controller rejects any of them.
    pub async fn delete_topics(&mut self, names: &[String]) -> Result<()> {
        if names.is_empty() {
            return Ok(());
        }
        let mut req = DeleteTopicsRequest::default();
        req.timeout_ms = self.timeout_ms;
        req.topics = names
            .iter()
            .map(|name| {
                let mut entry = DeleteTopicState::default();
                entry.name = Some(TopicName(StrBytes::from_string(name.clone())));
                entry
            })
            .collect();

        let _: DeleteTopicsResponse = self
            .controller_call(
                "DeleteTopics",
                ApiKey::DeleteTopics,
                6,
                &req,
                |r: &DeleteTopicsResponse| {
                    r.responses
                        .iter()
                        .map(|t| t.error_code)
                        .find(|c| *c != 0)
                        .unwrap_or(0)
                },
            )
            .await?;
        Ok(())
    }

    /// Grow a topic to `count` partitions **in total**, not by `count`.
    ///
    /// The broker's own field is named `count` and means the new total; a
    /// wrapper that treated it as a delta would shrink a topic on the second
    /// call, which the broker refuses — loudly, which is the only reason that
    /// bug is survivable.
    ///
    /// Expanding a topic **changes where keys land** for every default
    /// partitioner, this client's included. It is not a transparent operation.
    ///
    /// # Errors
    /// If the controller rejects it.
    pub async fn create_partitions(&mut self, topic: &str, count: i32) -> Result<()> {
        let mut entry = CreatePartitionsTopic::default();
        entry.name = TopicName(StrBytes::from_string(topic.to_owned()));
        entry.count = count;
        // `None` lets the controller choose the replicas, which is what an
        // operator wants unless they are placing them by hand.
        entry.assignments = None;

        let mut req = CreatePartitionsRequest::default();
        req.timeout_ms = self.timeout_ms;
        req.validate_only = false;
        req.topics = vec![entry];

        let _: CreatePartitionsResponse = self
            .controller_call(
                "CreatePartitions",
                ApiKey::CreatePartitions,
                3,
                &req,
                |r: &CreatePartitionsResponse| {
                    r.results
                        .iter()
                        .map(|t| t.error_code)
                        .find(|c| *c != 0)
                        .unwrap_or(0)
                },
            )
            .await?;

        // As in [`Self::create_topics`]: visible, not merely accepted.
        for attempt in 0..MAX_RETRIES {
            let _ = self.cluster.refresh_metadata(topic).await;
            if self.cluster.metadata().partition_count(topic) >= count {
                return Ok(());
            }
            if attempt + 1 == MAX_RETRIES {
                return Err(Error::NoLeader {
                    topic: topic.to_owned(),
                    partition: -1,
                });
            }
            T::sleep(BACKOFF).await;
        }
        Ok(())
    }

    /// Every broker in the cluster, and which one is the controller.
    ///
    /// # Errors
    /// If no broker answers.
    pub async fn describe_cluster(&mut self) -> Result<Vec<BrokerInfo>> {
        self.cluster.refresh_cluster().await?;
        let metadata = self.cluster.metadata();
        let controller = metadata.controller().map(|b| b.node_id);
        Ok(metadata
            .brokers()
            .map(|broker| BrokerInfo {
                node_id: broker.node_id,
                host: broker.host.clone(),
                port: broker.port,
                is_controller: controller == Some(broker.node_id),
            })
            .collect())
    }

    /// A topic's effective configuration: every key the broker reports,
    /// including the ones it defaulted.
    ///
    /// # Errors
    /// If the topic does not exist, or no broker answers.
    pub async fn describe_topic_config(
        &mut self,
        topic: &str,
    ) -> Result<BTreeMap<String, Option<String>>> {
        let mut resource = DescribeConfigsResource::default();
        // 2 is TOPIC. 4 is BROKER, which this does not expose: a broker
        // config must be asked of *that* broker, and a wrapper that hid the
        // routing would return one broker's answer for all of them.
        resource.resource_type = 2;
        resource.resource_name = StrBytes::from_string(topic.to_owned());
        resource.configuration_keys = None;

        let mut req = DescribeConfigsRequest::default();
        req.resources = vec![resource];
        req.include_synonyms = false;
        req.include_documentation = false;

        // **A topic that was just created is not yet on every broker.** This
        // request goes to whichever broker answers, and the controller having
        // accepted a `CreateTopics` does not mean the metadata has reached that
        // one — the answer is `UNKNOWN_TOPIC_OR_PARTITION` for a topic that
        // certainly exists. Retrying briefly is what every caller would
        // otherwise write; after the bound a genuinely absent topic still
        // errors, a second later.
        for attempt in 0..MAX_RETRIES {
            let resp: DescribeConfigsResponse = self
                .cluster
                .call_any(ApiKey::DescribeConfigs, 4, &req)
                .await?;

            let first = resp.results.first().ok_or(Error::Missing("a resource"))?;
            let code = ErrorCode(first.error_code);
            if !code.is_ok()
                && code.disposition() == Disposition::RefreshMetadata
                && attempt + 1 < MAX_RETRIES
            {
                T::sleep(BACKOFF).await;
                continue;
            }

            let mut out = BTreeMap::new();
            for resource in &resp.results {
                check("DescribeConfigs", resource.error_code)?;
                for config in &resource.configs {
                    out.insert(
                        config.name.to_string(),
                        config.value.as_ref().map(ToString::to_string),
                    );
                }
            }
            return Ok(out);
        }
        unreachable!("the loop returns on its last attempt")
    }

    /// Delete every record **before** the given offset, per partition.
    ///
    /// Returns each partition's new log start offset. This is the operation
    /// that makes `beginning_offsets` interesting: after it, offset zero is
    /// gone and a consumer that assumes zero asks for a record the broker no
    /// longer has.
    ///
    /// Goes to the **leader**, not the controller: it moves one log's start.
    ///
    /// # Errors
    /// If a leader cannot be found, or rejects the deletion.
    pub async fn delete_records(
        &mut self,
        before: &[(barnabas_core::group::TopicPartition, i64)],
    ) -> Result<BTreeMap<barnabas_core::group::TopicPartition, i64>> {
        let mut out = BTreeMap::new();
        if before.is_empty() {
            return Ok(out);
        }

        let mut by_leader: BTreeMap<String, Vec<(barnabas_core::group::TopicPartition, i64)>> =
            BTreeMap::new();
        for (tp, offset) in before {
            let addr = self.cluster.leader_addr(&tp.topic, tp.partition).await?;
            by_leader
                .entry(addr)
                .or_default()
                .push((tp.clone(), *offset));
        }

        for (addr, group) in by_leader {
            let mut topics: BTreeMap<String, Vec<DeleteRecordsPartition>> = BTreeMap::new();
            for (tp, offset) in &group {
                let mut entry = DeleteRecordsPartition::default();
                entry.partition_index = tp.partition;
                entry.offset = *offset;
                topics.entry(tp.topic.clone()).or_default().push(entry);
            }

            let mut req = DeleteRecordsRequest::default();
            req.timeout_ms = self.timeout_ms;
            req.topics = topics
                .into_iter()
                .map(|(name, partitions)| {
                    let mut topic = DeleteRecordsTopic::default();
                    topic.name = TopicName(StrBytes::from_string(name));
                    topic.partitions = partitions;
                    topic
                })
                .collect();

            let resp: DeleteRecordsResponse = self
                .cluster
                .call_at(&addr, ApiKey::DeleteRecords, 2, &req)
                .await?;

            for topic in &resp.topics {
                for partition in &topic.partitions {
                    check("DeleteRecords", partition.error_code)?;
                    out.insert(
                        barnabas_core::group::TopicPartition::new(
                            topic.name.0.to_string(),
                            partition.partition_index,
                        ),
                        partition.low_watermark,
                    );
                }
            }
        }
        Ok(out)
    }
}
