# What using a real broker taught us

[← barnabas](../README.md)

These are in the code as comments and tests, and are the reason the filtering rules look the way
they do:

- **READ_COMMITTED is client-side.** The broker does not withhold aborted records under
  `isolation_level = 1`. It returns them with an aborted-transactions list and a last-stable-offset,
  and the consumer filters. Skip it and aborted data is reported as committed, with no error.
- **The abort marker closes the range.** One producer can interleave an aborted and a committed
  transaction in a single fetch, so "drop everything from an aborted producer" silently discards
  committed records.
- **Batches come back whole.** A fetch from the middle of a batch returns the records before it too;
  dropping them is the client's job, or a restore re-delivers data the caller has already emitted.
- **A fully-filtered fetch must still advance.** Otherwise a partition of only-aborted records
  stalls forever while looking perfectly healthy.
- **Sequence numbers continue across transactions.** Restarting them makes the broker dedupe: `Ok`,
  the original base offset echoed back, nothing written, transaction committed empty.
- **Invalidate per partition, not wholesale.** One moved partition does not make the rest of the
  map wrong, and throwing it all away turns a single failover into a reconnect storm.
- **There is no single "Kafka default" partitioner.** librdkafka hashes keys with CRC-32; the Java
  client uses murmur2. Same key, same topic, different partition, no error. `barnabas` implements
  both and defaults to CRC-32, so a program migrating off `rdkafka` keeps its placement — checked
  against `rdkafka` itself, key for key, while it is still around to be the oracle.
- **Metadata requests ask for topic auto-creation**, as librdkafka and Java do, and
  `UNKNOWN_TOPIC_OR_PARTITION` refreshes rather than failing: a topic being created reports it for
  a moment.
- **`ConsumerProtocol` blobs need a version prefix.** Kafka's `serializeSubscription` writes an
  int16 version and *then* the struct; the generated type is only the struct. Without the prefix
  the coordinator accepted the `JoinGroup`, logged `Preparing to rebalance`, and then held the
  request until the rebalance timeout — no error, no response.
- **Read the broker's log before inferring from the client side.** Five rounds of client-side
  reasoning fixed five real bugs and never touched the cause of the symptom. One coordinator DEBUG
  line ended it: `removed dynamic members who haven't joined`. Start a broker with
  `KAFKA_LOG4J_LOGGERS="kafka.coordinator.group=DEBUG"` and read it beside `BARNABAS_TRACE=1`.
- **A group cannot be driven in lockstep.** Polling two members from one loop deadlocks against the
  protocol: the joining member's `JoinGroup` is held until the whole group has rejoined, so the
  incumbent cannot take its turn until the join it is blocking returns. A member is a process; in
  tests, a member is a task.
- **Accepted is not visible.** `CreateTopics` returning success means the controller wrote it down,
  not that any other broker knows. So does `CreatePartitions`, and so does a `DescribeConfigs` sent
  a moment later, which answers `UNKNOWN_TOPIC_OR_PARTITION` for a topic that certainly exists.
- **Metadata that is merely stale is invisible.** Refreshing only when something is *missing* never
  notices a topic that grew partitions — every leader still known, every answer still wrong. Age is
  the only thing that catches it.
- **Count partitions from the response, not from known leaders.** A `MetadataResponse` lists every
  partition whether or not each has a leader right now, so counting leaders undercounts during an
  election — and a count that dips looks exactly like a topic that shrank, which Kafka never does.
- **Error codes are states, not failures.** A cold cluster answers 15, then 14, then 16 — the last
  *after* a successful `FindCoordinator`. `CONCURRENT_TRANSACTIONS` (51) appears whenever a
  transaction starts right after one ends. See `ErrorCode::disposition`.
