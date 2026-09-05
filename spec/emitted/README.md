# Re-emitted conformance fixtures

Generated. Do not hand-edit; regenerate with `make emit-<port>` or
`make emit-all`.

Each `<port>/` tree is that port's own re-emission of every streamed pair
under `../fixtures/`: the golden pair is loaded into the port's stream model
and written back through the port's cassette writer, unchanged.

```
spec/emitted/<port>/<fixture>/<adapter>-<fingerprint>.{req,resp}.yaml
```

`<port>` is the port's top-level directory name (`go`, `ts`, `py`, `rs`,
`php`). There are no manifests here: the golden `manifest.yaml` in
`../fixtures/<fixture>/` is the entry list, filtered to `streamed: true`.

## Why these are checked in

Every port's conformance suite already round-trips each streamed pair
(load → emit → reload → compare). That round-trip is self-load only: the
port's own reader reads the port's own output, so an emit slip its reader
happens to tolerate never fails — `payload: null` where the spec requires an
object, an `error` key on the req side, a quoted key another YAML library
resolves differently. The v1 guarantee is that a cassette written by any
port replays on every other port, and only another port's reader can vouch
for that.

Checking each port's emission in makes the cross-port check a plain file
walk, so it runs inside every port's existing suite with no multi-toolchain
CI job and no artifact hand-off. It also pins each port's emit format
byte-for-byte, so an emitter change shows up as a reviewable diff here.

## What each port's suite asserts

1. **Pinned** — a fresh emission of every streamed golden pair equals the
   port's own tree, file set and bytes alike. A stale tree fails here with a
   `regenerate with make emit-<port>` hint.
2. **Cross-port** — every tree under this directory (its own included) loads
   through the port's strict reader to a model field-for-field equal to the
   golden pair: envelope fields, payloads, stream type, `half_close`/`end`,
   and every frame's `seq`, `at_ms`, and decoded message bytes. The
   `message_b64`/`message_text` choice is free on re-emit per spec and is
   not compared.

Adding a port tree needs no code change elsewhere: every suite discovers
`<port>/` directories by listing this directory.

## When to regenerate

- After changing a port's stream emitter: `make emit-<port>` (its pinned
  test fails until you do; every other port's cross-port test then judges
  the new output).
- After adding or editing a streamed fixture under `../fixtures/`:
  `make emit-all`. This needs all five toolchains; the devcontainer has
  them (`make dev-exec CMD="make emit-all"`).

Regeneration is deterministic: `recorded_at` and every other field come
from the golden pair, never from a clock.
