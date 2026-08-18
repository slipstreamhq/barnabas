# Completing the client

> **Done.** Every phase below is implemented and tested against a real broker.
> The document is kept as the record of what was decided and why — the ordering
> of the phases, and the things deliberately left out, are the parts still worth
> reading. What is *left* out is listed under
> [Not planned](#not-planned-and-worth-saying-so).

## The goal

**Kestrel is a general-purpose Kafka client, and a user with Kafka experience
should be immediately at home.** Familiar names, familiar semantics, familiar
defaults. Group semantics are central, not optional.

Slipstream is no longer the thing driving the shape. The client gets completed
first; how Slipstream uses it is a question to answer afterwards, and it may
well keep using the assign-only layer underneath.

## Familiarity is a feature, and today we fail it

Someone arriving from the Java client or `rdkafka` knows a vocabulary. Where we
use a different word for the same idea we cost them a lookup and gain nothing.
This should be settled **before** the group work, not after, because groups
double the surface it would have to be applied to.

| what a Kafka user calls it | kestrel | state |
|---|---|---|
| `poll` | `poll` | ✅ renamed from `fetch` |
| `assign` | `assign` | ✅ renamed from `add`; the old `Consumer::assign` constructor is now `for_partition` |
| `ConsumerRecords` | `ConsumerRecords` | ✅ renamed from `FetchedRecords` |
| `subscribe` | `subscribe` | ✅ |
| `commitSync` / `commitAsync` | `commit` / `set_auto_commit` | ✅ |
| `pause` / `resume` | `pause` / `resume` | ✅ |
| `endOffsets` / `beginningOffsets` | `end_offsets` / `beginning_offsets` | ✅ |
| `offsetsForTimes` | `offsets_for_times` | ✅ |
| `committed` | `committed` | ✅ |
| `sendOffsetsToTransaction` | `send_offsets_to_transaction` | ✅ |
| `AdminClient` | `Admin` | ✅ a deliberate subset |
| `auto.offset.reset` | `StartOffset` | keep — typed, and the mapping is obvious |
| `isolation.level` | `IsolationLevel` | already matches |
| `max.poll.records` | *absent* | a batch per poll is the throughput; see below |
| `linger.ms` | `set_linger` | ✅ with `set_batch_size` (`batch.size`) |
| `metadata.max.age.ms` | `set_metadata_max_age` | ✅ same default, five minutes |
| `enable.idempotence` | always on | keep, and say why: off is how records get duplicated |

Two decisions belong with it:

- **Configuration shape.** Kafka users expect a property map; we have a staged
  builder. Keep the builder — it makes illegal states unrepresentable and guides
  completion — but name its methods after the properties people already know
  (`session_timeout`, `max_poll_interval`, `auto_commit`), so the knowledge
  transfers even though the syntax does not.
- **State what we do differently, and why**, in the docs rather than leaving it
  to be discovered: a batch per `poll` rather than a record at a time (it is
  most of the consume throughput), and no ambient runtime.

## ~~Topic expansion, which is not a group problem~~ — done

A topic is usually created and produced by another team. You do not choose its
partition count and you are not told when it changes — and **adding partitions
is how a topic is scaled**, so the change is routine.

`assign_all` resolves the count once, at build. A topic expanded from 8 to 16
partitions afterwards leaves 8–15 unread indefinitely: nothing errors, and the
consumer looks healthy while missing a share of its input.

Kafka's group protocol handles this incidentally — the coordinator reassigns on
a metadata change — which is an advantage of groups that has nothing to do with
coordinating instances. But the assign-only layer needs its own answer, because
it will outlive this gap.

**What was built:** metadata is age-stamped, `set_metadata_max_age` defaulting to
five minutes as `metadata.max.age.ms` does. A subscribed consumer rejoins its
group and the leader assigns the new partitions; a manually assigned one is told
through `take_expansions` and never extended behind the caller's back; a
producer's keyed placement re-reads the count on the same schedule, so it stops
disagreeing with every producer that has fresh metadata.

Only growth is acted on. Kafka has no operation that removes partitions from a
live topic, so a smaller count is a transient answer — a broker mid-election, a
topic mid-creation — and moving every key on it would be worse than the stale
count. `Metadata::partition_count` was changed to count the *response* rather
than known leaders for the same reason: a response lists every partition whether
or not each has a leader right now, so the count no longer dips during an
election and cannot be misread as a shrink.
- `assign_all` documented as *all of them as of now* until then

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

#### Decided: classic first, behind a seam for KIP-848

Kafka 4.x moves assignment to the coordinator (KIP-848). Classic is still
supported and is what every broker below 4.0 and Redpanda speak, so it goes
first for reach. The new protocol arrives later as a **second implementation of
one seam**, not a second client.

##### What actually differs

| | classic | KIP-848 |
|---|---|---|
| assignment computed by | the group **leader**, client-side | the **coordinator**, server-side |
| RPCs | `JoinGroup`, `SyncGroup`, `Heartbeat` | `ConsumerGroupHeartbeat` alone |
| fencing token | generation id | member epoch |
| assignors | client-side, pluggable | server-side; the client names a preference |

##### What does not differ, and therefore sits above the seam

- `FindCoordinator` for the group
- `OffsetCommit` / `OffsetFetch` — the same RPCs under both
- the resulting assignment feeding the existing `assign`/`poll` machinery
- revoke/assign callbacks: the *events* are the same concept either way
- session and heartbeat timing as configuration

So the seam is narrow — membership and assignment delivery — and everything
expensive is on the shared side.

##### The seam

```rust
/// How this client becomes and stays a member of a group, and how it learns
/// what it is assigned. The only thing KIP-848 changes.
trait GroupProtocol {
    /// Join or rejoin. Returns the assignment and the token that fences us.
    async fn ensure_member(
        &mut self,
        cluster: &mut Cluster<T>,
        subscription: &Subscription,
    ) -> Result<Membership>;

    /// Periodic liveness, and where a reassignment is discovered.
    async fn heartbeat(&mut self, cluster: &mut Cluster<T>) -> Result<HeartbeatOutcome>;

    /// Leave deliberately, so the group rebalances now rather than at timeout.
    async fn leave(&mut self, cluster: &mut Cluster<T>) -> Result<()>;
}

enum HeartbeatOutcome {
    Unchanged,
    Reassigned(Assignment),
    /// Generation or member epoch moved on without us. Stop, do not commit.
    Fenced,
}
```

`ClassicProtocol` implements it with `JoinGroup`/`SyncGroup`/`Heartbeat` and
owns the `Assignor` trait — range, round-robin, sticky, cooperative-sticky —
because client-side assignment is exactly what KIP-848 removes. `ConsumerGroupProtocol`
implements it later with the single heartbeat RPC and no assignor.

Above the seam, one `GroupCoordinator` holds the current assignment, drives the
revoke/assign callbacks, owns offset commit, and translates a `Fenced` outcome
into "stop consuming and do not commit". That is the part worth getting right,
and it is written once.

##### The rule that keeps the seam honest

Nothing above the seam may name a generation id or a member epoch. Both are
fencing tokens; the moment shared code branches on which kind it has, the seam
has leaked and the second implementation becomes a second client.

Version negotiation already tells us which is available: the broker advertises
`ConsumerGroupHeartbeat` or it does not, so selection can be automatic later
without a configuration knob.

### ~~Phase 2 — exactly-once with groups~~ — done

`Producer::send_offsets_to_transaction` sends `AddOffsetsToTxn` to the
transaction coordinator and `TxnOffsetCommit` to the group coordinator, so a
group's offsets commit inside the producer's transaction:

```rust
let records = consumer.poll().await?;
producer.begin_transaction()?;
producer.send(&output, 0, &transformed).await?;
// After producing, before committing: these offsets account for that output.
producer
    .send_offsets_to_transaction(&consumer.positions(), &consumer.group_metadata().unwrap())
    .await?;
producer.commit_transaction().await?;
```

`group_metadata()` returns an opaque `GroupMetadata` — the member id and the
fencing token the coordinator checks (KIP-447), so a member that has already
been replaced cannot commit. It is opaque because naming a *generation* above
the protocol seam is what KIP-848 changes; a caller only ever passes it along.
It is `None` unless the member is stable, and must be re-read per transaction.

Static membership (`group.instance.id`, KIP-345) is still absent; the field is
on the wire and always `None`.

### ~~Phase 3 — the rest of a normal application's surface~~ — done

- Consumer: `pause`/`resume`/`paused`/`is_paused`, `committed`, `position`,
  `offsets_for_times`, `end_offsets`, `beginning_offsets`, `lag`. All the
  offset lookups share one batched `ListOffsets` — one request per leader, not
  per partition.
- Admin: `create_topics`, `delete_topics`, `create_partitions`,
  `describe_cluster`, `describe_topic_config`, `delete_records`. Topic
  operations route to the controller and re-discover it on `NOT_CONTROLLER`;
  `delete_records` routes to the leader.
- Producer: `produce`/`produce_to` accumulate per partition and go out when a
  batch fills or its linger expires — `set_linger`, `set_batch_size`, `tick`,
  `linger_deadline`. **This client spawns nothing**, so the linger clock is
  read only when the producer is called: a steady stream sends itself, and an
  idle producer needs `tick` or `flush`. `linger_deadline` exists so an event
  loop can arm the timer, because the caller owns the timer.

Two absences remain deliberate: broker configs (a broker config must be asked
of that broker, and hiding the routing would return one broker's answer for
all of them) and static membership, KIP-345.

### Not planned, and worth saying so

Kerberos/GSSAPI, interceptors, and a configuration-string API. A caller wanting
librdkafka's full surface should use librdkafka.

Three more, each for a stated reason rather than for want of time:

- **KIP-848 server-side assignment.** The seam is built and the classic protocol
  sits behind it; version negotiation already reveals whether a broker offers
  `ConsumerGroupHeartbeat`, so this can be selected automatically later without
  a configuration knob.
- **Static membership (KIP-345).** `group.instance.id` is on the wire and always
  `None`.
- **Broker configs in `Admin`.** A broker config must be asked of *that* broker,
  and a wrapper hiding the routing would return one broker's answer for all of
  them. Topic configs have no such ambiguity and are exposed.
- **`max.poll.records`.** A batch per `poll` is most of the consume throughput —
  see `PERF.md` — and capping it per call would trade that away for a knob whose
  purpose in Java is bounding `max.poll.interval.ms`, which this client does not
  have.

## What this cost

Written before the work as an estimate: *"the largest single piece of a Kafka
client — larger than the transactional producer already built here. Months, not
weeks, and the failure modes are silent: a fencing bug duplicates records, and a
rebalance bug loses them."*

The failure modes were as advertised. The cooperative handover took six fixes,
five of them genuine protocol bugs — a subscription blob written at v1 so it
could not carry a generation, two paths that cleared the generation on rejoin, a
generation filter applied to the withholding decision as well as the target, and
a rejoin that took one protocol step per `poll` where Java loops. Every one of
them presented as the same symptom: both members holding every partition.

The sixth was in the test, and it is the lesson worth keeping: **a group cannot
be driven in lockstep**. The coordinator holds a joining member's `JoinGroup`
until the whole group has rejoined, so an incumbent polled in the same loop
cannot take its turn until the join it is blocking returns. What ended it was
not more client-side reasoning — five rounds of that fixed five real bugs and
never touched the cause — but one line of the coordinator's own DEBUG log:
`removed dynamic members who haven't joined`. Read the broker's log early.

## Sequencing

1. ~~**Naming and API alignment**~~ — done for the three that existed. The rest
   arrive with the features that need them.
2. ~~**Phase 1, the group protocol**~~ — done: classic join/sync/heartbeat,
   all four assignors, eager and cooperative rebalancing, auto-commit and
   rebalance callbacks, behind a seam for KIP-848.
3. ~~**Topic expansion**~~ — done. Metadata is age-stamped
   (`set_metadata_max_age`, five minutes by default like
   `metadata.max.age.ms`). A subscribed consumer rejoins its group, so the
   leader assigns the new partitions; a manually assigned one is **told** via
   `take_expansions` and never extended behind the caller's back; a producer's
   keyed placement re-reads the count on the same schedule. Only growth counts
   — Kafka never shrinks a live topic, so a smaller number is a transient
   answer and acting on it would move every key twice.
4. ~~**Phase 2, EOS with groups**~~ — done.
5. ~~**Phase 3, the ordinary surface**~~ — done.

Performance work is finished for now: the client leads the Java client and
librdkafka on every cell measured, and what remains are features, not speed.
`PERF.md` is the record, including the cells where it does not lead.

## Slipstream, afterwards

The client is complete, so these are now the open questions:

- Does Slipstream keep `assign` — its control plane genuinely does own
  placement, and a group would be a second authority — or adopt groups and
  retire that part of the control plane?
- Topic-expansion detection is done either way, which is why it was never filed
  under groups. If Slipstream keeps `assign`, `take_expansions` is the signal it
  needs.

The open correctness item there is the fenced-sink bug in its recovery path,
which is not this client's.
