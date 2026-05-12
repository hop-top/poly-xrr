# fs Adapter + Daemon Use-Case Documentation

**Date:** 2026-05-11
**Track:** `fs-adapter`
**Owner:** Noor (`jad+noor@ideacrafters.com`)
**Status:** Approved, ready for implementation plan

## Problem

xrr today records and replays interactions across five channel types
(exec, HTTP, gRPC, Redis, SQL) at a wrapper seam inside the calling
process. Two categories of test concern fall outside these channels:

1. **Local disk mutations** — `WriteFile`, `Mkdir`, `Chmod`, `Chown`,
   etc. Tests that exercise code paths which mutate the filesystem
   have no xrr-native way to assert "the call happened with these
   arguments" while skipping the actual disk write on replay.

2. **Process-internal state** — daemons, long-running servers, and
   in-process types that hold state across calls. Users have asked
   how to make these deterministic with xrr.

This spec adds first-class support for (1) via a new `fs` adapter
and documents the right primitive for each variant of (2) so users
stop trying to fit daemon state into xrr's call-replay model.

## Goals

- Add a `fs` adapter to the Go port (`go/adapters/fs/`) supporting
  nine mutation operations with the same shape as the existing exec
  adapter.
- Make path normalization a pluggable hook so tmpdir-based tests
  produce stable cassettes.
- Inline payload bytes in cassettes (no sidecar files) to preserve
  the human-diffable two-file format.
- Lock the cross-runtime contract via a conformance fixture.
- Document the three daemon use-case patterns in README so users
  stop conflating "stateful process" with "needs an xrr adapter".

## Non-Goals

- **Read operations** (`read`, `stat`, `readdir`). Reads blur the
  line between cassettes and snapshot fixtures. Test setup should
  pre-seed disk state; xrr asserts on the writes.
- **Sidecar payload files.** Inline-in-YAML keeps the v1 cassette
  format at two file types (`.req.yaml`, `.resp.yaml`).
- **Daemon adapter.** xrr's call-keyed cassette model cannot
  represent "the daemon is in state X after the third call".
  Documented alternatives instead.
- **ts/py/rs/php ports of `fs`.** Each port is its own follow-up
  branch after the Go side and conformance fixture land. See
  "Follow-on work" below.

## Adapter Design

### Request shape

One op-tagged struct, matching how the SQL and Redis adapters
handle their many ops (not per-op types — too much surface for the
cross-runtime port).

```go
// go/adapters/fs/fs.go

type Request struct {
    Op        string  `yaml:"op"`                  // see "Operations" below
    Path      string  `yaml:"path"`                // normalized
    Data      []byte  `yaml:"data,omitempty"`      // write only
    Mode      *uint32 `yaml:"mode,omitempty"`      // write, mkdir, chmod
    UID       *int    `yaml:"uid,omitempty"`       // chown
    GID       *int    `yaml:"gid,omitempty"`       // chown
    Dest      string  `yaml:"dest,omitempty"`      // rename, symlink, hardlink (normalized)
    Size      *int64  `yaml:"size,omitempty"`      // truncate
    Flags     uint32  `yaml:"flags,omitempty"`     // write: O_TRUNC / O_APPEND / O_EXCL bits
    Recursive bool    `yaml:"recursive,omitempty"` // remove: RemoveAll vs Remove
}

func (r *Request) AdapterID() string { return "fs" }
```

Pointer types for `Mode`, `UID`, `GID`, `Size` distinguish "field
unset" from "field set to zero". The fingerprint omits unset fields
(see "Fingerprint algorithm" below).

### Response shape

Most fs mutations have no useful return value. Response captures
only what's worth replaying:

```go
type Response struct {
    DurationMs   int64 `yaml:"duration_ms,omitempty"`
    BytesWritten int   `yaml:"bytes_written,omitempty"` // write only
}

func (r *Response) AdapterID() string { return "fs" }
```

When the underlying op errored, the error string lands in the
cassette envelope's `error` field via the existing `FileSession`
machinery (`go/session.go:62`).

### Operations

| Op | Required fields | Notes |
|----|-----------------|-------|
| `write` | `Path`, `Data` | `Mode`, `Flags` optional. `BytesWritten` in Response. |
| `mkdir` | `Path` | `Mode` optional. |
| `remove` | `Path` | `Recursive=true` ⇒ `RemoveAll`. |
| `rename` | `Path`, `Dest` | |
| `chmod` | `Path`, `Mode` | |
| `chown` | `Path`, `UID`, `GID` | |
| `symlink` | `Path` (target), `Dest` (link) | |
| `hardlink` | `Path` (old), `Dest` (new) | |
| `truncate` | `Path`, `Size` | |

Validation: `NewAdapter` returns one Adapter that accepts all ops.
The wrapper is the right place to enforce "write requires Data";
the adapter just fingerprints whatever it receives. Mirrors how the
exec adapter doesn't validate `Argv` being non-empty.

## Fingerprint Algorithm

Canonical JSON over a fields map, sha256, first 4 bytes hex —
matches `go/adapters/exec/exec.go:64`. Go's `encoding/json` sorts
`map[string]any` keys lexicographically on marshal, so the same
field set always serializes to the same bytes. Other-language ports
must do the same (sort keys before serializing) to produce matching
fingerprints.

```go
func (a *Adapter) Fingerprint(req xrr.Request) (string, error) {
    r, ok := req.(*Request)
    if !ok {
        return "", fmt.Errorf("fs: unexpected request type %T", req)
    }
    fields := map[string]any{
        "op":   r.Op,
        "path": a.normalize(r.Path),
    }
    if len(r.Data) > 0 {
        sum := sha256.Sum256(r.Data)
        fields["data_sha256"] = fmt.Sprintf("%x", sum)
    }
    if r.Mode != nil      { fields["mode"] = *r.Mode }
    if r.UID  != nil      { fields["uid"]  = *r.UID }
    if r.GID  != nil      { fields["gid"]  = *r.GID }
    if r.Dest != ""       { fields["dest"] = a.normalize(r.Dest) }
    if r.Size != nil      { fields["size"] = *r.Size }
    if r.Flags != 0       { fields["flags"] = r.Flags }
    if r.Recursive        { fields["recursive"] = true }

    canonical, err := json.Marshal(fields)
    if err != nil {
        return "", fmt.Errorf("fs: fingerprint marshal: %w", err)
    }
    sum := sha256.Sum256(canonical)
    return fmt.Sprintf("%x", sum[:4]), nil
}
```

### Three deliberate choices

1. **Data is hashed, not raw, in the fingerprint.** Keeps the 8-char
   filename suffix bounded for any payload size. The raw `data`
   still lives in the `.req.yaml` envelope for human diffing.
   Two writes with different payloads to the same path produce
   different cassettes; identical payloads produce identical
   cassettes (idempotent re-record).

2. **Path is normalized before hashing.** The fingerprint sees
   `$TMP/config.yaml`, never `/var/folders/xy/.../config.yaml`.
   Same for `Dest`. The cassette envelope stores the normalized
   path too — raw paths never leave the adapter. This is the
   cross-runtime contract: ts/py/rs/php ports replay the
   normalized path verbatim and never re-derive it.

3. **Zero/empty values are omitted from the fields map.** Same
   trick as exec's `cwd` (`go/adapters/exec/exec.go:73`). A `write`
   without explicit `Mode` doesn't include `mode` in the hash, so
   adopters can leave it unset without affecting cassette keys
   recorded by adopters who set it.

   **Operational consequence:** if an adopter starts populating a
   previously-unset field, existing cassettes will not match.
   Same shape of break as adding `cwd` to exec. Documented in the
   cassette spec under "Extension status".

## Path Normalizer

The adapter holds an optional normalizer; the wrapper installs it
at construction.

```go
// PathNormalizer rewrites paths before they enter the fingerprint
// or the cassette envelope. Default is identity. Returning "" is
// allowed (treated literally).
type PathNormalizer func(string) string

type Adapter struct {
    normalizer PathNormalizer
}

func NewAdapter() *Adapter {
    return &Adapter{normalizer: func(p string) string { return p }}
}

// WithNormalizer returns a copy of a with the given normalizer
// installed.
func (a *Adapter) WithNormalizer(n PathNormalizer) *Adapter {
    cp := *a
    cp.normalizer = n
    return &cp
}

// Chain composes normalizers left to right.
func Chain(norms ...PathNormalizer) PathNormalizer {
    return func(p string) string {
        for _, n := range norms {
            p = n(p)
        }
        return p
    }
}
```

### Rules

1. Normalization runs once per fingerprint and again when building
   the cassette payload. What gets hashed and what gets stored
   agree exactly.
2. Normalizer applies to `Path` and `Dest` only. Not to `Data`,
   not to any field that might contain a path as a substring.
   Embedded paths in payloads are the caller's concern.
3. Per-adapter-instance scope, not global. Each test owns its
   normalizer. No process-level registry — too easy to leak state
   across tests.

### Canonical usage

```go
tmp := t.TempDir()
fsAdapter := xfs.NewAdapter().WithNormalizer(func(p string) string {
    return strings.Replace(p, tmp, "$TMP", 1)
})
wrapper := NewFSWrapper(realFS, sess, fsAdapter)
```

## Wrapper

Lives at `go/examples/wrap_fs_runner/main.go`, mirroring
`go/examples/wrap_command_runner/main.go`. Defines the canonical
adoption pattern: a `FS` interface the consuming codebase already
uses, a `RealFS` implementation that calls `os.WriteFile` etc.,
and a `Wrapper` satisfying `FS` that routes through xrr.

```go
type FS interface {
    WriteFile(ctx context.Context, path string, data []byte, mode os.FileMode) error
    Mkdir(ctx context.Context, path string, mode os.FileMode) error
    Remove(ctx context.Context, path string) error
    RemoveAll(ctx context.Context, path string) error
    Rename(ctx context.Context, oldpath, newpath string) error
    Chmod(ctx context.Context, path string, mode os.FileMode) error
    Chown(ctx context.Context, path string, uid, gid int) error
    Symlink(ctx context.Context, target, link string) error
    Link(ctx context.Context, oldpath, newpath string) error
    Truncate(ctx context.Context, path string, size int64) error
}
```

Each method builds a `Request`, calls `sess.Record(...)`, and
returns whatever the inner FS returned. Errors flow through xrr
unchanged via the cassette envelope's `error` field.

## Tests

In `go/adapters/fs/fs_test.go`:

1. **TestFSAdapterFingerprint** — same `(op, path, data)` ⇒ same
   hash; differing in any one ⇒ different hash. Verifies the
   omit-when-zero rule: `Mode: nil` and `Mode: ptr(0)` produce
   different fingerprints (one omits `mode`, the other includes
   `mode: 0`).
2. **TestFSAdapterRoundtrip** — serialize a Request with binary
   `Data`, deserialize, byte-equal.
3. **TestFSAdapterNormalizerApplied** — install a normalizer,
   verify both the fingerprint AND the envelope path are rewritten.
4. **TestFSAdapterBinaryPayload** — non-UTF8 `Data` round-trips
   through YAML `!!binary` cleanly.
5. **Conformance fixture** at `spec/fixtures/fs-write/` —
   one recorded interaction, picked up by `TestConformanceFixtures`
   (`go/conformance_test.go:24`). Locks the cross-runtime contract.

Wrapper-level integration tests live alongside the example:

- Record mode actually writes to disk in `t.TempDir()`.
- Replay mode uses a `RealFS` stub that panics on any call;
  the cassette has to be consulted instead of the inner FS.

## Documentation Changes

### `spec/cassette-format-v1.md`

New section "fs Adapter (v1)" covering:

- The nine ops and which fields each requires/permits.
- Fingerprint algorithm (verbatim from this spec).
- Normalized-path contract: cassettes contain post-normalizer
  paths; ports replay them verbatim and never touch the
  filesystem during replay.
- Extension-status framing borrowed from exec's `cwd` (Go-first,
  other ports adopt at their own pace).
- Operational consequence of adding fields to populated requests:
  invalidates existing cassettes for the affected adopter.

### `README.md`

Two additions:

1. New row in the channel table: exec, HTTP, gRPC, Redis, SQL, **fs**.
2. New subsection **"Daemons and stateful servers"** placed before
   "Cassette Format":

> **Daemons and stateful servers.** xrr intercepts *calls*, not
> *state*. If you're testing code that interacts with a long-lived
> process, pick the pattern that matches your topology:
>
> 1. **Your code talks to the daemon over a wire** (HTTP, gRPC,
>    Redis, SQL). Use the matching xrr adapter on the **client
>    side**. Record once against a real daemon, replay with no
>    daemon running. This is the default xrr workflow.
> 2. **The daemon is in-process** (`*http.Server`, custom event
>    bus, a type with internal state your code observes across
>    calls). xrr is the wrong tool — use a hand-written fake
>    behind an interface. xrr's cassette model has no notion of
>    "the daemon is now in state X after the third call".
> 3. **You need to assert on the daemon's internal state** (queue
>    depth, leader, in-memory counters). Instrument the type with
>    a `Snapshot()` method and assert on snapshots. xrr cassettes
>    can't represent "and now the leader changed" — that's a
>    state-machine assertion, not a call-replay assertion.

## Follow-on Work (Out of Scope for This Branch)

Tracked as separate tasks once Go lands:

1. **ts port of `fs` adapter** — `ts/src/adapters/fs.ts`,
   conformance against `spec/fixtures/fs/`.
2. **py port of `fs` adapter** — `py/src/xrr/adapters/fs.py`,
   same conformance.
3. **rs port of `fs` adapter** — `rs/src/adapters/fs.rs`, same
   conformance.
4. **php port of `fs` adapter** — `php/src/Adapters/Fs.php`, same
   conformance.

Each port replays Go-recorded cassettes (and produces cassettes
the Go port can replay). Conformance suite enforces the contract;
no port lands without it.

## Risks and Mitigations

- **Cassette portability.** Normalized paths are the cross-runtime
  contract. If a port forgets to skip normalization on already-
  normalized cassette data, replays will mis-key. Mitigation:
  conformance fixture exercises a normalized-path cassette;
  any port that mis-handles it fails CI.
- **Binary YAML across ports.** Different YAML libraries emit
  `!!binary` slightly differently. Mitigation: conformance fixture
  includes a `write` with non-UTF8 `Data`. Test golden bytes.
- **Pointer-vs-zero fingerprint trap.** Adopters who switch from
  `Mode: nil` to `Mode: ptr(0644)` will invalidate cassettes
  silently. Mitigation: documented in cassette spec + adopter
  guide; same trap exec adopters already navigate with `cwd`.

## Acceptance Criteria

- `go/adapters/fs/` package builds, all tests pass.
- `spec/fixtures/fs-write/` exists and is loaded by
  `TestConformanceFixtures`.
- `go/examples/wrap_fs_runner/main.go` builds and demonstrates
  the canonical adoption pattern.
- `spec/cassette-format-v1.md` documents the `fs` adapter.
- `README.md` lists `fs` in the channel table and includes the
  "Daemons and stateful servers" subsection.
- No changes to other adapters, the cassette envelope format, or
  the `Session`/`Adapter`/`Cassette` interfaces in `go/xrr.go`.
