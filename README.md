# xrr — Cross-Runtime Recorder

Generic multi-channel interaction recorder/replayer with a pluggable adapter interface.

## What is xrr?

`xrr` records and replays interactions across any channel type (exec, HTTP, gRPC, Redis, SQL, fs).
Cassettes are language-agnostic YAML — record in Go, replay in Python, or any other port.

Three modes:
- **record** — intercept real calls, write cassettes
- **replay** — serve cassettes, never touch the network
- **passthrough** — calls go through, cassette untouched

### When to use xrr

xrr intercepts calls at a wrapper seam inside the process that makes
them. Pick your topology:

- **In-process tests** (unit / integration): your test function
  directly makes the recordable call (HTTP client, DB driver,
  `exec.Command` from within the test) via an xrr-wrapped runner.
  Construct a `FileSession` in the test, pass it to the wrapper, done.
  This is what `go/examples/wrap_command_runner/main.go` demonstrates.
- **Subprocess / cross-process e2e tests**: your test shells out to a
  compiled binary and asserts on its side effects. xrr can only see
  the subprocess's calls if the **binary itself** is xrr-aware. The
  binary must call `xrr.SessionFromEnv()` at startup and wire the
  returned session into its internal runners. The parent test sets
  `XRR_MODE` and `XRR_CASSETTE_DIR` in the child's environment. See
  the "Cross-process e2e" section below.

If your topology is subprocess-based and the binary you're testing is
NOT xrr-aware, xrr alone cannot help — you need either to make the
binary xrr-aware or to use a different recording layer (e.g. a network
proxy).

## Quick Example (Go)

```go
// Record once
s := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette("./cassettes"))
adapter := exec.NewAdapter()
resp, err := s.Record(ctx, adapter, &exec.Request{
    Argv: []string{"gh", "pr", "view", "123"},
}, func() (xrr.Response, error) {
    return runCommand(...)
})

// Replay everywhere — real command never runs
s2 := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette("./cassettes"))
resp2, err := s2.Record(ctx, adapter, req, do)
```

## Adapters

| ID    | Intercepts      | Fingerprint fields                         | Ports         |
|-------|-----------------|--------------------------------------------|---------------|
| exec  | shell commands  | argv + stdin                               | all¹          |
| http  | HTTP requests   | method + path+query + sha256(body)[:8]     | all           |
| grpc  | gRPC calls      | service + method + sha256(proto-bytes)[:8] | go only       |
| redis | Redis commands  | command + args                             | all           |
| sql   | SQL queries     | normalized query + args                    | all           |

¹ The Go port additionally hashes `cwd` into the exec fingerprint when
non-empty — a backward-compatible extension for per-directory isolation
(see below). Other ports are expected to adopt the same rule.

### Exec adapter: per-directory isolation (Go-only extension)

If the same command runs in multiple working directories within one
cassette dir (common for cross-process e2e tests using `XRR_CASSETTE_DIR`),
populate `exec.Request.Cwd` so the fingerprint hashes the working
directory too. Within the Go port this is backward compatible: leaving
`Cwd` empty preserves the legacy `argv+stdin`-only fingerprint, so
existing cassettes keep replaying.

**Cross-runtime limitation:** until the ts / py / rs / php exec
adapters implement the same "include `cwd` when non-empty" rule,
cassettes recorded in Go with non-empty `Cwd` will **NOT** replay in
those ports — their fingerprint calculation will differ and the load
will miss. Use non-empty `Cwd` only when record and replay both happen
in runtimes that agree on the rule, or leave `Cwd` empty to preserve
the cross-runtime replay guarantee. See
`go/examples/wrap_command_runner/main.go` for the canonical Go
adoption pattern, and `spec/cassette-format-v1.md` for the formal
spec status of this extension.

## Cross-process e2e (XRR_MODE + XRR_CASSETTE_DIR)

For test suites that shell out to a compiled binary and assert on its
side effects, the xrr seam has to live **inside the binary**. Wire it
via environment variables so the parent test controls the session
without linking the library into the test process.

**In the binary's `main()`:**

```go
sess, err := xrr.SessionFromEnv()
if err != nil {
    log.Fatalf("xrr env: %v", err)
}
// sess == nil when XRR_MODE is unset — fall back to the normal,
// non-recorded execution path.
gitRunner := xrrx.NewRunner(realGit, sess)
dockerRunner := xrrx.NewRunner(realDocker, sess)
// ... wire runners into the app's dependency graph
```

**In the parent test:**

```go
cassetteDir := filepath.Join(t.TempDir(), "cassettes")
os.MkdirAll(cassetteDir, 0o755)

cmd := exec.Command("./my-binary", "do-thing")
cmd.Env = append(os.Environ(),
    "XRR_MODE=record",                    // or "replay"
    "XRR_CASSETTE_DIR="+cassetteDir,
)
require.NoError(t, cmd.Run())
```

Same binary, same test, flip `XRR_MODE=replay` once cassettes are
recorded. The child writes/reads cassettes from a directory the parent
controls; no IPC, no plumbing.

### Caveats

- **OS-allocated state can't be replayed.** Port numbers, file inodes,
  container IDs, PIDs — xrr replays the subprocess **calls**, not the
  OS the subprocess interacts with. Tests that assert on those values
  must still run against the real environment and should be gated
  separately.
- **Per-directory isolation needs `exec.Request.Cwd`.** Inside a
  single `XRR_CASSETTE_DIR`, the same command run from different
  working directories collides on one cassette key unless the binary
  populates `exec.Request.Cwd` (Go-only today — see the exec adapter
  section above).
- **No parent/child multi-writer safety.** If both the parent and the
  child write to the same `XRR_CASSETTE_DIR` concurrently, file
  collisions are possible. Either record from the child only, or give
  each writer its own dir.

## Daemons and stateful servers

xrr intercepts **calls**, not **state**. If you're testing code
that interacts with a long-lived process, pick the pattern that
matches your topology — xrr is the right tool for one of these
cases and explicitly the wrong tool for the other two.

### 1. Your code talks to the daemon over a wire

If the daemon speaks HTTP, gRPC, Redis, or SQL, use the matching
xrr adapter on the **client side**. The daemon stays real in
record mode; replay never starts it. This is the default xrr
workflow and what the existing adapters are designed for.

```go
// Record once against a real PostgreSQL.
sess := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette("cassettes"))
db := xsql.WrapDB(realDB, sess)  // client-side wrapper

// Replay with no PostgreSQL running.
sess := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette("cassettes"))
db := xsql.WrapDB(nil, sess)  // inner DB never called
```

### 2. The daemon is in-process

If your code interacts with a `*http.Server`, custom event bus,
or any type that holds state across calls inside the same
process — xrr is the wrong tool. Use a hand-written fake behind
an interface.

xrr's cassette model has no notion of "the daemon is now in state
X after the third call". Cassettes are keyed by request fingerprint
and replay in any order; sequence-dependent in-process state can't
be expressed in that model. A 30-line fake with a `map[string]Thing`
is the right primitive.

### 3. You need to assert on the daemon's internal state

If your test asserts on the daemon's internal counters, queue depth,
elected leader, or any in-memory state observed across calls —
that's a state-machine assertion, not a call-replay assertion. xrr
cassettes can't represent "and now the leader changed".

Instrument the type with a `Snapshot()` method (or expose enough
hooks to inspect what you need) and assert on snapshots directly.
xrr can sit alongside this approach to handle any I/O the daemon
performs, but the state assertions stay outside the cassette.

> **TL;DR:** xrr is a calls-at-a-boundary tool. If the boundary
> disappears (in-process) or the test asserts on state across the
> boundary, you have a different problem.

## Cassette Format

Language-agnostic YAML envelope. See [spec/cassette-format-v1.md](spec/cassette-format-v1.md).

```
cassettes/
  exec-a3f9c1b2.req.yaml
  exec-a3f9c1b2.resp.yaml
```

Cross-compat guarantee: cassettes recorded in any language replay in any other.
Every port runs the shared conformance fixtures from `spec/fixtures/`.

## Languages

| Dir  | Package       | Test command          |
|------|---------------|-----------------------|
| go/  | hop.top/xrr   | `go test ./...`       |
| ts/  | @hop-top/xrr  | `pnpm vitest run`     |
| py/  | xrr           | `uv run pytest -v`    |
| php/ | hop-top/xrr   | `phpunit tests/`      |
| rs/  | xrr (crate)   | `cargo test`          |

## Porting Guide

To add a new language:

1. **Implement `Adapter`** — `id`, `fingerprint(req)`, `serialize`/`deserialize`
2. **Implement `FileCassette`** — `save(adapterID, fp, req, resp)`, `load(adapterID, fp)`
   - Write YAML envelopes: `xrr:"1"`, `adapter`, `fingerprint`, `recorded_at`, `payload`
   - File naming: `<adapter>-<fingerprint>.<req|resp>.yaml`
3. **Implement `Session`** — dispatch record/replay/passthrough
   - replay miss → raise/return `ErrCassetteMiss`
4. **Run conformance** — point at `spec/fixtures/`, load every `manifest.yaml` interaction
5. Add a job to `.github/workflows/ci.yml`
