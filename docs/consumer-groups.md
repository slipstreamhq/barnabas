# Consumer groups, and what "parity" should mean

## The decision

Build them. Assign-only serves a program whose control plane already places
partitions and whose checkpoints already hold offsets — Slipstream, and not
many others. "The go-to Rust Kafka client" and "assignment is the caller's
problem" cannot both be true.

## Slipstream will not use them, and that is not a contradiction

Slipstream's control plane assigns partitions and its checkpoints hold source
offsets. A group would be a **second authority for both**: Kafka's rebalance
would move partitions the control plane believes it placed, and committed group
offsets would compete with checkpointed ones for the answer to "where do we
resume". Slipstream should keep using `assign`.

That is the layering to build to. Assignment is the primitive; a group is
policy that computes an assignment and calls the same `add`. Everything below —
routing, fetch batching, the lean decoder, the filtering — is shared and does
not change. If groups were to require reworking that, the design would be
wrong.

## What "parity" should and should not mean

Parity with librdkafka in full includes Kerberos/GSSAPI, interceptors,
statistics callbacks, and something like two hundred configuration properties.
Chasing all of it is how a client spends years being 90% done. The target worth
naming is **the API surface a normal application uses**, which is much smaller.

### Phase 1 — the group protocol (the piece that unlocks most users)

- `FindCoordinator` (group), `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`
- `OffsetCommit` / `OffsetFetch`, auto-commit and manual
- Assignors: range, round-robin, sticky, cooperative-sticky
- Rebalance callbacks — revoke and assign — which a stateful consumer needs
- Generation and member-id fencing: the failure mode is silent duplicate
  consumption, so it belongs in the simulator with the producer's sequencing

**Decide first: KIP-848.** Kafka 4.x has a new group protocol that moves
assignment to the coordinator; the classic protocol is still supported but is
the past. Implementing classic only is a rewrite later; implementing the new
one only excludes every broker below 4.0 and Redpanda until it follows.
Recommendation: classic first for broker reach, but keep assignment strategy
behind a seam so the new protocol is a second implementation rather than a
second client. This decision should be made before any code, because it shapes
the state machine.

### Phase 2 — exactly-once with groups

- `AddOffsetsToTxn` + `TxnOffsetCommit`, so offsets commit inside the producer's
  transaction

Deliberately absent today because Slipstream does not need it: source offsets
live in its checkpoints. Every other EOS user does need it.

### Phase 3 — the rest of a normal application's surface

- Consumer: `pause`/`resume`, `committed`, `position`, `offsets_for_times`,
  `end_offsets`, lag
- Admin: `CreateTopics`, `DeletePartitions`, `DescribeCluster`,
  `DescribeConfigs`, `DeleteRecords` — enough to write a test suite and an
  operational tool without a second client
- Producer: a time-based accumulator (`linger.ms`). We have none, which is why
  the batch-1 benchmark row is our weakest and why callers must batch by hand

### Not planned, and worth saying so

Kerberos/GSSAPI, interceptors, and a configuration-string API. A caller wanting
librdkafka's full surface should use librdkafka.

## What this costs

The group protocol is the largest single piece of a Kafka client — larger than
the transactional producer already built here. It is months, not weeks, and the
failure modes are silent: a fencing bug duplicates records, and a rebalance bug
loses them. The correctness apparatus already in this workspace is the reason to
believe it is tractable — the sans-io core, the scripted broker, the TLA+ model
of the producer window. Groups should arrive with the same three.

## Sequencing against everything else

Groups are worth more than any remaining performance work. The one open
correctness item ahead of them is the fenced-sink bug in Slipstream's recovery
path, which is not this client's.
