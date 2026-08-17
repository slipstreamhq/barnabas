# Specification

`ProducerWindow.tla` — the idempotent producer's in-flight window and the rule
that makes it safe: only the **contiguous leading run** of successes is retired,
everything behind a failure is re-sent in order.

`kestrel-client`'s adversarial tests check that on scenarios chosen by hand.
This checks it over every interleaving TLC reaches within the bounds in the
`.cfg` (8 batches, 5 in flight, 3 failures — five in flight is what Kafka allows
with idempotence and what `DEFAULT_MAX_IN_FLIGHT` uses).

## Running it

No JDK is needed on the host; any container with one will do.

```sh
curl -sLO https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar
podman run --rm -v "$PWD":/spec:z -w /tmp docker.io/apache/kafka:3.9.0 \
  sh -c "cp /spec/*.tla /spec/*.cfg /spec/tla2tools.jar /tmp/ && \
         java -cp tla2tools.jar tlc2.TLC -workers 4 ProducerWindow"
```

Last run: **no error found**, 30 distinct states, safety and liveness both.

## Confirming it can fail

A specification that cannot fail proves nothing. Applying the mutation
described at the bottom of the module — retiring the requests *behind* a
failure as well — makes TLC report a counterexample in two states:

```
Error: Invariant LogIsExactPrefix is violated.
State 2: /\ log = <<2, 3, 4, 5>>
```

Batch 1 failed, and 2 through 5 were retired regardless: a gap in the log that
still returns `Ok`. That is the same weakening that makes
`a_failure_mid_window_resends_everything_behind_it` and
`a_success_after_a_failure_is_not_retired` fail in the Rust suite.

## Scope

One partition's sequencing and the client's retire rule. Deliberately not
modelled: leadership changes, coordinators, transactions, the network. The
value is concentrated where being wrong is *silent* — a gap or duplicate that
the caller is told succeeded.

Broker-side protocols are specified elsewhere and are worth reading rather than
redoing: [Vanlightly/kafka-tlaplus](https://github.com/Vanlightly/kafka-tlaplus)
covers ISR replication and KRaft, and his
[Kafka transactions series](https://jack-vanlightly.com/analyses/2024/12/3/verifying-kafka-transactions-diary-entry-2-writing-an-initial-tla-spec)
models the coordinator we talk to.
