//! The cluster map: which broker leads which partition, and how to reach it.
//!
//! Sans-io like the rest of the core — it is fed a decoded `MetadataResponse`
//! and answers questions about it. Refreshing is the binding's job, because
//! refreshing means sending a request.
//!
//! # Staleness is the interesting part
//!
//! A metadata cache is easy; knowing when it is wrong is not. Leadership moves
//! whenever a broker restarts or a partition is reassigned, and the client
//! learns about it *from the error it gets for using the old leader*, not from
//! a notification. So the flow is always: use the cache, get
//! `NOT_LEADER_OR_FOLLOWER`, invalidate, refresh, retry — which is why
//! [`Disposition::RefreshMetadata`](crate::Disposition::RefreshMetadata) is its
//! own answer rather than a plain retry.
//!
//! Invalidation is deliberately *per partition*: a single moved partition does
//! not make the rest of the map wrong, and throwing it all away turns one
//! failover into a reconnect storm against every broker.

use std::collections::HashMap;

use kafka_protocol::messages::MetadataResponse;

/// How to reach a broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerAddr {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

impl BrokerAddr {
    /// `host:port`, ready for a connect.
    #[must_use]
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// A partition, named the way callers name it.
pub type PartitionKey = (String, i32);

/// What the client knows about the cluster.
#[derive(Debug, Default)]
pub struct Metadata {
    brokers: HashMap<i32, BrokerAddr>,
    leaders: HashMap<PartitionKey, i32>,
}

impl Metadata {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge a `MetadataResponse` in.
    ///
    /// Merge rather than replace: a response for one topic says nothing about
    /// the others, and replacing would silently drop leadership the client is
    /// actively using. Partitions carrying an error code are **skipped**, not
    /// recorded as leaderless — an error means "ask again", and writing it down
    /// would cache the failure.
    pub fn update(&mut self, resp: &MetadataResponse) {
        for broker in &resp.brokers {
            self.brokers.insert(
                broker.node_id.0,
                BrokerAddr {
                    node_id: broker.node_id.0,
                    host: broker.host.to_string(),
                    port: broker.port,
                },
            );
        }
        for topic in &resp.topics {
            if topic.error_code != 0 {
                continue;
            }
            let Some(name) = topic.name.as_ref() else {
                continue;
            };
            for partition in &topic.partitions {
                if partition.error_code != 0 || partition.leader_id.0 < 0 {
                    continue;
                }
                self.leaders.insert(
                    (name.0.to_string(), partition.partition_index),
                    partition.leader_id.0,
                );
            }
        }
    }

    /// Where to send a request for this partition, if known.
    #[must_use]
    pub fn leader_for(&self, topic: &str, partition: i32) -> Option<&BrokerAddr> {
        let node = self.leaders.get(&(topic.to_owned(), partition))?;
        self.brokers.get(node)
    }

    /// Look a broker up by node id — how a coordinator, which is named by id
    /// rather than address, is reached.
    #[must_use]
    pub fn broker(&self, node_id: i32) -> Option<&BrokerAddr> {
        self.brokers.get(&node_id)
    }

    /// Forget one partition's leader, leaving the rest of the map alone.
    pub fn invalidate_partition(&mut self, topic: &str, partition: i32) {
        self.leaders.remove(&(topic.to_owned(), partition));
    }

    /// How many partitions the client knows this topic has.
    ///
    /// Counted from known leaders, so it is only as complete as the last
    /// refresh — which is why [`Self::update`] skipping errored partitions
    /// matters here: a partition mid-election would otherwise shrink the count
    /// and silently move every key.
    #[must_use]
    pub fn partition_count(&self, topic: &str) -> i32 {
        i32::try_from(
            self.leaders
                .keys()
                .filter(|(name, _)| name == topic)
                .count(),
        )
        .unwrap_or(i32::MAX)
    }

    /// Every broker the client has heard of, for connection cleanup.
    pub fn brokers(&self) -> impl Iterator<Item = &BrokerAddr> {
        self.brokers.values()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.brokers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::{BrokerId, TopicName};
    use kafka_protocol::protocol::StrBytes;

    fn broker(node_id: i32, host: &str, port: i32) -> MetadataResponseBroker {
        let mut b = MetadataResponseBroker::default();
        b.node_id = BrokerId(node_id);
        b.host = StrBytes::from_string(host.to_owned());
        b.port = port;
        b
    }

    fn topic(
        name: &str,
        error_code: i16,
        partitions: Vec<MetadataResponsePartition>,
    ) -> MetadataResponseTopic {
        let mut t = MetadataResponseTopic::default();
        t.name = Some(TopicName(StrBytes::from_string(name.to_owned())));
        t.error_code = error_code;
        t.partitions = partitions;
        t
    }

    fn partition(index: i32, leader: i32, error_code: i16) -> MetadataResponsePartition {
        let mut p = MetadataResponsePartition::default();
        p.partition_index = index;
        p.leader_id = BrokerId(leader);
        p.error_code = error_code;
        p
    }

    fn response(
        brokers: Vec<MetadataResponseBroker>,
        topics: Vec<MetadataResponseTopic>,
    ) -> MetadataResponse {
        let mut r = MetadataResponse::default();
        r.brokers = brokers;
        r.topics = topics;
        r
    }

    #[test]
    fn a_leader_is_resolved_to_an_address() {
        let mut md = Metadata::new();
        md.update(&response(
            vec![broker(1, "kafka-1", 9092), broker(2, "kafka-2", 9092)],
            vec![topic("t", 0, vec![partition(0, 2, 0)])],
        ));
        assert_eq!(md.leader_for("t", 0).unwrap().addr(), "kafka-2:9092");
        assert!(md.leader_for("t", 1).is_none());
        assert!(md.leader_for("other", 0).is_none());
    }

    /// **Merge, not replace.** A response for one topic must not erase what the
    /// client knows about another it is actively fetching from.
    #[test]
    fn an_update_for_one_topic_leaves_the_others_alone() {
        let mut md = Metadata::new();
        md.update(&response(
            vec![broker(1, "kafka-1", 9092)],
            vec![topic("a", 0, vec![partition(0, 1, 0)])],
        ));
        md.update(&response(
            vec![broker(1, "kafka-1", 9092)],
            vec![topic("b", 0, vec![partition(0, 1, 0)])],
        ));
        assert!(md.leader_for("a", 0).is_some(), "topic a was forgotten");
        assert!(md.leader_for("b", 0).is_some());
    }

    /// Leadership moved. The new response wins.
    #[test]
    fn a_moved_leader_is_picked_up() {
        let mut md = Metadata::new();
        md.update(&response(
            vec![broker(1, "kafka-1", 9092), broker(2, "kafka-2", 9092)],
            vec![topic("t", 0, vec![partition(0, 1, 0)])],
        ));
        assert_eq!(md.leader_for("t", 0).unwrap().node_id, 1);

        md.update(&response(
            vec![broker(1, "kafka-1", 9092), broker(2, "kafka-2", 9092)],
            vec![topic("t", 0, vec![partition(0, 2, 0)])],
        ));
        assert_eq!(md.leader_for("t", 0).unwrap().node_id, 2);
    }

    /// An error is "ask again", not "there is no leader". Recording it would
    /// cache a transient failure as a fact.
    #[test]
    fn an_errored_partition_is_not_recorded() {
        let mut md = Metadata::new();
        md.update(&response(
            vec![broker(1, "kafka-1", 9092)],
            vec![topic("t", 0, vec![partition(0, 1, 9)])],
        ));
        assert!(md.leader_for("t", 0).is_none());
    }

    /// A partition mid-election has leader -1, which is not an address.
    #[test]
    fn a_leaderless_partition_is_not_recorded() {
        let mut md = Metadata::new();
        md.update(&response(
            vec![broker(1, "kafka-1", 9092)],
            vec![topic("t", 0, vec![partition(0, -1, 0)])],
        ));
        assert!(md.leader_for("t", 0).is_none());
    }

    /// An errored topic contributes nothing, but its brokers still count — the
    /// client will need them to ask again.
    #[test]
    fn an_errored_topic_still_yields_its_brokers() {
        let mut md = Metadata::new();
        md.update(&response(
            vec![broker(7, "kafka-7", 9092)],
            vec![topic("t", 3, vec![partition(0, 7, 0)])],
        ));
        assert!(md.leader_for("t", 0).is_none());
        assert_eq!(md.broker(7).unwrap().addr(), "kafka-7:9092");
    }

    /// The partition count follows what the last refresh knew.
    #[test]
    fn the_partition_count_follows_metadata() {
        let mut md = Metadata::new();
        assert_eq!(md.partition_count("t"), 0);
        md.update(&response(
            vec![broker(1, "kafka-1", 9092)],
            vec![topic(
                "t",
                0,
                vec![partition(0, 1, 0), partition(1, 1, 0), partition(2, 1, 0)],
            )],
        ));
        assert_eq!(md.partition_count("t"), 3);
        assert_eq!(md.partition_count("other"), 0);
    }

    /// **Invalidation is per partition.** One moved partition must not throw
    /// away the whole map: that turns a single failover into a reconnect storm.
    #[test]
    fn invalidating_one_partition_keeps_the_others() {
        let mut md = Metadata::new();
        md.update(&response(
            vec![broker(1, "kafka-1", 9092)],
            vec![topic(
                "t",
                0,
                vec![partition(0, 1, 0), partition(1, 1, 0)],
            )],
        ));
        md.invalidate_partition("t", 0);
        assert!(md.leader_for("t", 0).is_none());
        assert!(md.leader_for("t", 1).is_some());
        assert!(
            md.broker(1).is_some(),
            "the broker itself is still reachable and its connection stays open"
        );
    }
}
