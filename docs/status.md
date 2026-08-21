# What works

[← barnabas](../README.md)

| | |
|---|---|
| **Consumer** | works, by group *or* by hand — `ApiVersions`, `Metadata`, `ListOffsets`, `Fetch`, seek, READ_COMMITTED filtering |
| **Producer** | idempotent and transactional — sequencing, coordinator routing, zombie fencing, keyed partitioning. **One `Produce` per broker**, not per partition; enrollment likewise |
| **Compression** | gzip, snappy, lz4, zstd — round-tripped against a real broker, both directions |
| **Leader routing, metadata cache** | works — fetches go to the leader from the cluster map, `NOT_LEADER_OR_FOLLOWER` invalidates that partition and retries |
| **Runtimes** | glommio and tokio, from one client. Adding a third is a `Transport` impl |
| **Consumer** | any number of partitions, **one `Fetch` per broker** rather than per partition, with incremental fetch sessions (KIP-227) — one connection where there were eight |
| **Connection recovery** | a closed connection is evicted and redialled once; a request that outlives its deadline drops its connection, because the late response would desynchronise the stream |
| **TLS** | opt-in `tls` feature on either binding. A second `Transport`, so `Consumer<GlommioTls>` is the same client over a different socket — the shared code needed no change at all |
| **SASL** | PLAIN and SCRAM-SHA-256/512. Shared, since the handshake is protocol |
| **Consumer groups** | `subscribe`, four assignors, **eager and cooperative** rebalancing (KIP-429), auto-commit, rebalance callbacks, `committed`. KIP-848 sits behind a seam, unimplemented |
| **Exactly-once with groups** | `send_offsets_to_transaction` — `AddOffsetsToTxn` to the transaction coordinator, `TxnOffsetCommit` to the group coordinator, with KIP-447 fencing |
| **Admin** | create/delete topics, expand partitions, describe cluster and topic configs, delete records. Controller-routed, and it re-discovers the controller on `NOT_CONTROLLER` |
| **Accumulator** | `produce` batches per partition with `linger` and `batch_size`. No background task: `tick`/`linger_deadline` hand the timer to the caller |
| **Topic expansion** | detected on a metadata age (`metadata.max.age.ms`'s five minutes): groups rejoin, manual assignments are reported to the caller, producers re-place keys |
| **Pause/resume, offset lookups** | `pause`/`resume`, `end_offsets`, `beginning_offsets`, `offsets_for_times`, `lag` — one batched `ListOffsets` per leader |
