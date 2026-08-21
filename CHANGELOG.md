# Changelog

Notable changes per release. The project is 0.x: breaking changes can happen,
are avoided where they can be, and get a minor bump and a migration note here
when they cannot. See the README's stability section.

## 0.2.0 — 2026-08-21

### Breaking

- **`glommio-ng` 0.10 → 0.11.1**, which `barnabas-glommio` exposes in its public
  API: `Transport::Stream` is `glommio::net::TcpStream`. Cargo treats a public
  dependency's major version as part of your own surface, and in 0.x the minor
  slot is the major one — hence 0.2.0 rather than 0.1.1.

  **Why it cannot be a patch.** Two copies of glommio in one binary means two
  reactors and two executor thread-locals, and a client built against one
  panics in `scoped-tls` when polled on the other. A patch bump would hand that
  to anyone pinning `barnabas-glommio = "0.1"` alongside `glommio-ng 0.10`
  directly. A minor bump makes cargo refuse the combination instead of
  producing a runtime panic.

  **Migration:** move your own `glommio-ng` dependency to `0.11` in the same
  change. `barnabas-tokio` and `barnabas-core` are unaffected in substance and
  move only to keep the workspace on one version.

### Notes

- No API of barnabas's own changed. The 42 broker tests and 151 offline tests
  pass on 0.11.1, on both bindings.

## 0.1.0 — 2026-08-21

First published release. The client is feature-complete against
`docs/completing-the-client.md`: consumer groups with eager and cooperative
rebalancing, exactly-once across a group, an admin client, a producing
accumulator, and partition-expansion detection.

The four crates — `barnabas-core`, `barnabas-client`, `barnabas-glommio`,
`barnabas-tokio` — were previously reserved at `0.0.0` placeholders.
