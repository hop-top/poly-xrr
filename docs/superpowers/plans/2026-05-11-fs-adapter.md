# fs Adapter Implementation Plan

> ⚠️ **Historical artifact.** This plan was written before two contract
> changes that landed during implementation. Code snippets below
> showing `Data []byte` are SUPERSEDED — the final wire-format is
> `Data string` (UTF-8); binary callers base64-encode at the call
> site. See `spec/cassette-format-v1.md` "Data Field Encoding" for
> the authoritative contract. The scope also expanded from "Go only,
> ports as follow-on" to "all five ports in this PR" — see the
> design doc's Non-Goals section.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `fs` adapter to xrr's Go port supporting nine filesystem mutation operations (write, mkdir, remove, rename, chmod, chown, symlink, hardlink, truncate), with pluggable path normalization and inline binary payloads, plus README documentation for the three daemon use-case patterns.

**Architecture:** The `fs` adapter mirrors the existing `exec` adapter at `go/adapters/exec/`: one op-tagged `Request` struct, a minimal `Response`, a deterministic sha256-based `Fingerprint` over canonical JSON, and YAML serialization via `gopkg.in/yaml.v3`. A `PathNormalizer` hook on the adapter rewrites `Path` and `Dest` before fingerprinting and serialization so tmpdir-based tests produce stable cassettes. A wrapper example at `go/examples/wrap_fs_runner/main.go` demonstrates the canonical adoption pattern. Conformance is locked via a fixture at `spec/fixtures/fs-write/`.

**Tech Stack:** Go 1.21+, `gopkg.in/yaml.v3` (already a project dependency), `encoding/json` for canonical-JSON fingerprinting, `crypto/sha256` for hashing. Tests use `github.com/stretchr/testify/assert` + `require` (already in use throughout xrr).

**Spec:** `docs/superpowers/specs/2026-05-11-fs-adapter-design.md`

**Working branch:** `fs-adapter` (already created via `git hop create`)

**Working directory:** `/Users/jadb/.w/ideacrafterslabs/xrr/hops/fs-adapter`

---

## File Structure

**New files:**

- `go/adapters/fs/fs.go` — Adapter, Request, Response, PathNormalizer, NewAdapter, WithNormalizer, Chain, ID, Fingerprint, Serialize, Deserialize. ~140 LOC.
- `go/adapters/fs/fs_test.go` — Adapter unit tests. ~200 LOC.
- `go/examples/wrap_fs_runner/main.go` — Canonical adoption pattern: `FS` interface, `RealFS`, `Wrapper`. Runnable `main()` that records once, replays once, prints diff. ~220 LOC.
- `spec/fixtures/fs-write/manifest.yaml` — Lists adapter+fingerprint for conformance loader.
- `spec/fixtures/fs-write/fs-<fp>.req.yaml` — Conformance cassette: write op, normalized path, inline UTF-8 data.
- `spec/fixtures/fs-write/fs-<fp>.resp.yaml` — Matching response envelope.

**Modified files:**

- `spec/cassette-format-v1.md` — Add "fs Adapter (v1)" section with op table, fingerprint algorithm, normalized-path contract, extension-status note.
- `README.md` — Add `fs` row to channel table; add "Daemons and stateful servers" subsection before "Cassette Format".

**Untouched (verified during self-review):**

- `go/xrr.go` (Adapter, Cassette, Session interfaces)
- `go/session.go` (FileSession)
- `go/cassette.go` (FileCassette envelope shape)
- All other adapter directories (`exec/`, `grpc/`, `http/`, `redis/`, `sql/`)

---

## Task 1: Skeleton — package, types, constructor

**Files:**
- Create: `go/adapters/fs/fs.go`
- Create: `go/adapters/fs/fs_test.go`

- [ ] **Step 1: Create the adapter skeleton**

Write `go/adapters/fs/fs.go`:

```go
// Package fs is the xrr adapter for filesystem mutation operations.
//
// It records and replays calls to a filesystem-mutating interface
// (WriteFile, Mkdir, Chmod, ...) using the same cassette shape as
// the exec adapter. Reads are intentionally not supported: tests
// should pre-seed disk state via fixtures and use xrr only to
// assert on mutations.
package fs

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	xrr "hop.top/xrr"
	"gopkg.in/yaml.v3"
)

// Op constants for Request.Op. Adopters SHOULD use these rather
// than literal strings so a misspelling fails at compile time.
const (
	OpWrite    = "write"
	OpMkdir    = "mkdir"
	OpRemove   = "remove"
	OpRename   = "rename"
	OpChmod    = "chmod"
	OpChown    = "chown"
	OpSymlink  = "symlink"
	OpHardlink = "hardlink"
	OpTruncate = "truncate"
)

// Request represents one fs mutation. Op selects which fields are
// meaningful; the adapter does not validate field presence — the
// wrapper is the right place to enforce per-op invariants.
//
// Pointer types for Mode, UID, GID, Size distinguish "field unset"
// from "field set to zero". The fingerprint omits unset fields
// (same pattern as exec adapter's Cwd).
type Request struct {
	Op        string  `yaml:"op"             json:"op"`
	Path      string  `yaml:"path"           json:"path"`
	Data      []byte  `yaml:"data,omitempty" json:"data,omitempty"`
	Mode      *uint32 `yaml:"mode,omitempty" json:"mode,omitempty"`
	UID       *int    `yaml:"uid,omitempty"  json:"uid,omitempty"`
	GID       *int    `yaml:"gid,omitempty"  json:"gid,omitempty"`
	Dest      string  `yaml:"dest,omitempty" json:"dest,omitempty"`
	Size      *int64  `yaml:"size,omitempty" json:"size,omitempty"`
	Flags     uint32  `yaml:"flags,omitempty"     json:"flags,omitempty"`
	Recursive bool    `yaml:"recursive,omitempty" json:"recursive,omitempty"`
}

func (r *Request) AdapterID() string { return "fs" }

// Response captures the minimal observable outcome of a mutation.
// Errors flow through the cassette envelope's `error` field via
// FileSession (see go/session.go), not through Response.
type Response struct {
	DurationMs   int64 `yaml:"duration_ms,omitempty"`
	BytesWritten int   `yaml:"bytes_written,omitempty"`
}

func (r *Response) AdapterID() string { return "fs" }

// PathNormalizer rewrites a path before it enters the fingerprint
// or the cassette envelope. Default is identity. Returning ""
// is allowed (treated literally — adopters can drop path info
// if they really want to).
type PathNormalizer func(string) string

// Adapter implements xrr.Adapter for fs mutations.
type Adapter struct {
	normalizer PathNormalizer
}

// NewAdapter returns an fs Adapter with identity path normalization.
func NewAdapter() *Adapter {
	return &Adapter{normalizer: func(p string) string { return p }}
}

// WithNormalizer returns a copy of a with the given normalizer
// installed. Use Chain to compose multiple rules.
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

func (a *Adapter) normalize(p string) string {
	if p == "" {
		return ""
	}
	return a.normalizer(p)
}

// ID returns the adapter id.
func (a *Adapter) ID() string { return "fs" }

// Fingerprint, Serialize, Deserialize are implemented in
// subsequent tasks.
func (a *Adapter) Fingerprint(req xrr.Request) (string, error) {
	panic("not implemented")
}

// Serialize marshals v as YAML.
func (a *Adapter) Serialize(v any) ([]byte, error) {
	return yaml.Marshal(v)
}

// Deserialize unmarshals data into target.
func (a *Adapter) Deserialize(data []byte, target any) error {
	return yaml.Unmarshal(data, target)
}
```

- [ ] **Step 2: Write a smoke test asserting the package compiles and ID/AdapterID work**

Write `go/adapters/fs/fs_test.go`:

```go
package fs_test

import (
	"testing"

	"hop.top/xrr/adapters/fs"

	"github.com/stretchr/testify/assert"
)

func TestAdapterID(t *testing.T) {
	a := fs.NewAdapter()
	assert.Equal(t, "fs", a.ID())
}

func TestRequestAdapterID(t *testing.T) {
	r := &fs.Request{Op: fs.OpWrite, Path: "/x"}
	assert.Equal(t, "fs", r.AdapterID())
}

func TestResponseAdapterID(t *testing.T) {
	r := &fs.Response{}
	assert.Equal(t, "fs", r.AdapterID())
}
```

- [ ] **Step 3: Run the tests**

Run: `cd go && go test ./adapters/fs/ -run 'TestAdapterID|TestRequestAdapterID|TestResponseAdapterID' -v`

Expected: PASS, all three tests.

- [ ] **Step 4: Commit**

```bash
git add go/adapters/fs/fs.go go/adapters/fs/fs_test.go
git commit -m "feat(fs): scaffold fs adapter package with Request/Response/Adapter types"
```

---

## Task 2: Fingerprint implementation

**Files:**
- Modify: `go/adapters/fs/fs.go` (replace `Fingerprint` panic with real implementation)
- Modify: `go/adapters/fs/fs_test.go` (add fingerprint tests)

- [ ] **Step 1: Write the failing fingerprint tests first**

First, **replace** the existing import block in `go/adapters/fs/fs_test.go` (which only imports `testing` + `assert` from Task 1) with the expanded version below, then **append** the test functions after the existing tests.

Replace the import block at the top of `go/adapters/fs/fs_test.go` with:

```go
import (
	"testing"

	"hop.top/xrr/adapters/fs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)
```

Then append these test functions to the end of the file:

```go
func TestFingerprintDeterministic(t *testing.T) {
	a := fs.NewAdapter()
	req := &fs.Request{Op: fs.OpWrite, Path: "/etc/hosts", Data: []byte("127.0.0.1 localhost\n")}
	fp1, err := a.Fingerprint(req)
	require.NoError(t, err)
	assert.Len(t, fp1, 8, "fingerprint must be 8 hex chars")
	fp2, err := a.Fingerprint(req)
	require.NoError(t, err)
	assert.Equal(t, fp1, fp2, "same request must hash identically")
}

func TestFingerprintDiscriminatesPath(t *testing.T) {
	a := fs.NewAdapter()
	fpA, _ := a.Fingerprint(&fs.Request{Op: fs.OpWrite, Path: "/a", Data: []byte("x")})
	fpB, _ := a.Fingerprint(&fs.Request{Op: fs.OpWrite, Path: "/b", Data: []byte("x")})
	assert.NotEqual(t, fpA, fpB)
}

func TestFingerprintDiscriminatesData(t *testing.T) {
	a := fs.NewAdapter()
	fpA, _ := a.Fingerprint(&fs.Request{Op: fs.OpWrite, Path: "/x", Data: []byte("foo")})
	fpB, _ := a.Fingerprint(&fs.Request{Op: fs.OpWrite, Path: "/x", Data: []byte("bar")})
	assert.NotEqual(t, fpA, fpB)
}

func TestFingerprintDiscriminatesOp(t *testing.T) {
	a := fs.NewAdapter()
	fpW, _ := a.Fingerprint(&fs.Request{Op: fs.OpWrite, Path: "/x"})
	fpR, _ := a.Fingerprint(&fs.Request{Op: fs.OpRemove, Path: "/x"})
	assert.NotEqual(t, fpW, fpR)
}

func TestFingerprintOmitsZeroFields(t *testing.T) {
	a := fs.NewAdapter()
	// Request with Mode unset must hash identically to a "minimal" write.
	bare := &fs.Request{Op: fs.OpWrite, Path: "/x", Data: []byte("y")}
	withNilMode := &fs.Request{Op: fs.OpWrite, Path: "/x", Data: []byte("y"), Mode: nil}
	fpA, _ := a.Fingerprint(bare)
	fpB, _ := a.Fingerprint(withNilMode)
	assert.Equal(t, fpA, fpB, "Mode: nil must omit `mode` from fingerprint")
}

func TestFingerprintPointerToZeroIncludesField(t *testing.T) {
	a := fs.NewAdapter()
	zero := uint32(0)
	bare := &fs.Request{Op: fs.OpWrite, Path: "/x", Data: []byte("y")}
	withZeroMode := &fs.Request{Op: fs.OpWrite, Path: "/x", Data: []byte("y"), Mode: &zero}
	fpA, _ := a.Fingerprint(bare)
	fpB, _ := a.Fingerprint(withZeroMode)
	assert.NotEqual(t, fpA, fpB,
		"Mode: &0 must include `mode: 0` in fingerprint; differs from Mode: nil")
}

func TestFingerprintRejectsWrongType(t *testing.T) {
	a := fs.NewAdapter()
	type bogus struct{}
	type bogusReq struct{ bogus }
	// Use any other AdapterID-implementing type — the exec adapter's
	// Request would do, but we don't want an import cycle. Define a
	// minimal stand-in.
	_, err := a.Fingerprint(notFsRequest{})
	assert.Error(t, err)
}

type notFsRequest struct{}

func (notFsRequest) AdapterID() string { return "not-fs" }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd go && go test ./adapters/fs/ -run TestFingerprint -v`

Expected: all `TestFingerprint*` tests FAIL with panic "not implemented".

- [ ] **Step 3: Implement Fingerprint**

In `go/adapters/fs/fs.go`, replace the panicking `Fingerprint` method with:

```go
// Fingerprint returns sha256(canonical JSON of selected fields)[:8].
//
// Field selection rules:
//   - op and path are always included; path is path-normalized.
//   - data is hashed (full sha256 hex) and included as data_sha256
//     when non-empty. Raw bytes are NOT in the fingerprint — keeps
//     the 8-char filename suffix bounded for any payload size.
//   - Mode/UID/GID/Size pointers are included iff non-nil.
//   - dest is included iff non-empty (path-normalized).
//   - flags is included iff non-zero.
//   - recursive is included iff true.
//
// Go's encoding/json sorts map keys lexicographically on marshal, so
// the same field set always serializes to the same bytes. Other-
// language ports MUST sort keys identically.
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
	if r.Mode != nil {
		fields["mode"] = *r.Mode
	}
	if r.UID != nil {
		fields["uid"] = *r.UID
	}
	if r.GID != nil {
		fields["gid"] = *r.GID
	}
	if r.Dest != "" {
		fields["dest"] = a.normalize(r.Dest)
	}
	if r.Size != nil {
		fields["size"] = *r.Size
	}
	if r.Flags != 0 {
		fields["flags"] = r.Flags
	}
	if r.Recursive {
		fields["recursive"] = true
	}
	canonical, err := json.Marshal(fields)
	if err != nil {
		return "", fmt.Errorf("fs: fingerprint marshal: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return fmt.Sprintf("%x", sum[:4]), nil
}
```

- [ ] **Step 4: Run the fingerprint tests**

Run: `cd go && go test ./adapters/fs/ -run TestFingerprint -v`

Expected: all 7 `TestFingerprint*` tests PASS.

- [ ] **Step 5: Commit**

```bash
git add go/adapters/fs/fs.go go/adapters/fs/fs_test.go
git commit -m "feat(fs): fingerprint over canonical JSON with omit-on-zero"
```

---

## Task 3: PathNormalizer integration

**Files:**
- Modify: `go/adapters/fs/fs_test.go` (add normalizer tests)
- `go/adapters/fs/fs.go` already has `WithNormalizer`/`Chain`/`normalize` from Task 1 — no implementation changes needed.

- [ ] **Step 1: Write the failing normalizer tests**

First, add `"strings"` to the existing import block at the top of `go/adapters/fs/fs_test.go` so it reads:

```go
import (
	"strings"
	"testing"

	"hop.top/xrr/adapters/fs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)
```

Then append these test functions to the end of the file:

```go
func TestNormalizerAppliedToFingerprint(t *testing.T) {
	plain := fs.NewAdapter()
	norm := fs.NewAdapter().WithNormalizer(func(p string) string {
		return strings.Replace(p, "/var/folders/abc/T/Test123", "$TMP", 1)
	})

	rawReq := &fs.Request{Op: fs.OpWrite, Path: "/var/folders/abc/T/Test123/config.yaml", Data: []byte("k: v")}
	normReq := &fs.Request{Op: fs.OpWrite, Path: "$TMP/config.yaml", Data: []byte("k: v")}

	// Plain adapter sees raw path; normalized adapter sees rewritten path.
	// The two fingerprints from the NORMALIZED adapter must match —
	// regardless of which raw path was fed in — because both reduce
	// to the same normalized form.
	fpRawNorm, _ := norm.Fingerprint(rawReq)
	fpNormNorm, _ := norm.Fingerprint(normReq)
	assert.Equal(t, fpRawNorm, fpNormNorm,
		"normalizer must map both raw and pre-normalized paths to same fp")

	// And the plain adapter MUST disagree with the normalizing adapter
	// on the raw request, proving the normalizer actually ran.
	fpRawPlain, _ := plain.Fingerprint(rawReq)
	assert.NotEqual(t, fpRawPlain, fpRawNorm,
		"plain adapter and normalizing adapter must differ on raw path input")
}

func TestNormalizerAppliedToDest(t *testing.T) {
	norm := fs.NewAdapter().WithNormalizer(func(p string) string {
		return strings.Replace(p, "/tmp", "$TMP", 1)
	})
	req := &fs.Request{Op: fs.OpRename, Path: "/tmp/a", Dest: "/tmp/b"}
	fp1, _ := norm.Fingerprint(req)
	// Same request with already-normalized paths must produce same fp.
	req2 := &fs.Request{Op: fs.OpRename, Path: "$TMP/a", Dest: "$TMP/b"}
	fp2, _ := norm.Fingerprint(req2)
	assert.Equal(t, fp1, fp2)
}

func TestChainNormalizer(t *testing.T) {
	tmpNorm := func(p string) string { return strings.Replace(p, "/tmp", "$TMP", 1) }
	homeNorm := func(p string) string { return strings.Replace(p, "/home/u", "$HOME", 1) }
	a := fs.NewAdapter().WithNormalizer(fs.Chain(tmpNorm, homeNorm))

	req := &fs.Request{Op: fs.OpWrite, Path: "/tmp/foo", Data: []byte("x")}
	req2 := &fs.Request{Op: fs.OpWrite, Path: "$TMP/foo", Data: []byte("x")}
	fp1, _ := a.Fingerprint(req)
	fp2, _ := a.Fingerprint(req2)
	assert.Equal(t, fp1, fp2, "chained normalizer must compose left to right")
}

func TestNormalizerEmptyPathPassesThrough(t *testing.T) {
	// An empty Path is invalid per the wrapper, but the adapter
	// itself should not crash. The normalize() helper must short-
	// circuit on "" without invoking the user's function.
	calls := 0
	a := fs.NewAdapter().WithNormalizer(func(p string) string {
		calls++
		return "NEVER"
	})
	// Use chmod with no path field set (test of robustness only).
	mode := uint32(0o644)
	_, err := a.Fingerprint(&fs.Request{Op: fs.OpChmod, Path: "", Mode: &mode})
	require.NoError(t, err)
	assert.Equal(t, 0, calls, "empty path must not invoke normalizer")
}
```

- [ ] **Step 2: Run the normalizer tests**

Run: `cd go && go test ./adapters/fs/ -run TestNormalizer -v && go test ./adapters/fs/ -run TestChainNormalizer -v`

Expected: all four tests PASS (the implementation from Task 1 already supports them).

- [ ] **Step 3: Commit**

```bash
git add go/adapters/fs/fs_test.go
git commit -m "test(fs): PathNormalizer applied to fingerprint, dest, chain composition"
```

---

## Task 4: Serialize/Deserialize roundtrip + binary payload

**Files:**
- Modify: `go/adapters/fs/fs_test.go` (add roundtrip + binary tests)

- [ ] **Step 1: Write the failing roundtrip tests**

Append to `go/adapters/fs/fs_test.go`:

```go
func TestSerializeRoundtrip(t *testing.T) {
	a := fs.NewAdapter()
	mode := uint32(0o644)
	req := &fs.Request{
		Op:    fs.OpWrite,
		Path:  "/etc/hosts",
		Data:  []byte("127.0.0.1 localhost\n"),
		Mode:  &mode,
		Flags: 0,
	}
	data, err := a.Serialize(req)
	require.NoError(t, err)
	var got fs.Request
	require.NoError(t, a.Deserialize(data, &got))
	assert.Equal(t, req.Op, got.Op)
	assert.Equal(t, req.Path, got.Path)
	assert.Equal(t, req.Data, got.Data)
	require.NotNil(t, got.Mode)
	assert.Equal(t, *req.Mode, *got.Mode)
}

func TestSerializeBinaryPayload(t *testing.T) {
	// Non-UTF8 bytes — yaml.v3 should emit !!binary; round-trip must
	// recover the exact byte sequence.
	a := fs.NewAdapter()
	binary := []byte{0x00, 0xff, 0xc3, 0x28, 0x80, 0x01, 0x02, 0x03}
	req := &fs.Request{Op: fs.OpWrite, Path: "/bin/x", Data: binary}
	data, err := a.Serialize(req)
	require.NoError(t, err)
	t.Logf("YAML output:\n%s", string(data))
	var got fs.Request
	require.NoError(t, a.Deserialize(data, &got))
	assert.Equal(t, binary, got.Data, "binary payload must round-trip exactly")
}

func TestSerializeOmitsZeroOptionals(t *testing.T) {
	// A bare write request must not emit `dest:`, `mode:`, `uid:`,
	// `gid:`, `size:`, `flags:`, or `recursive:` in YAML.
	a := fs.NewAdapter()
	req := &fs.Request{Op: fs.OpWrite, Path: "/x", Data: []byte("y")}
	data, _ := a.Serialize(req)
	out := string(data)
	for _, forbidden := range []string{"dest:", "mode:", "uid:", "gid:", "size:", "flags:", "recursive:"} {
		assert.NotContains(t, out, forbidden,
			"bare write request must omit %q from YAML", forbidden)
	}
}

func TestSerializeResponseRoundtrip(t *testing.T) {
	a := fs.NewAdapter()
	resp := &fs.Response{DurationMs: 42, BytesWritten: 1024}
	data, err := a.Serialize(resp)
	require.NoError(t, err)
	var got fs.Response
	require.NoError(t, a.Deserialize(data, &got))
	assert.Equal(t, resp.DurationMs, got.DurationMs)
	assert.Equal(t, resp.BytesWritten, got.BytesWritten)
}
```

- [ ] **Step 2: Run the tests**

Run: `cd go && go test ./adapters/fs/ -run TestSerialize -v`

Expected: all four tests PASS (yaml.v3 handles `!!binary` automatically and `omitempty` is on every optional field).

- [ ] **Step 3: Commit**

```bash
git add go/adapters/fs/fs_test.go
git commit -m "test(fs): Serialize/Deserialize roundtrip incl. binary payloads"
```

---

## Task 5: Wrapper example

**Files:**
- Create: `go/examples/wrap_fs_runner/main.go`

- [ ] **Step 1: Write the wrapper example**

Create `go/examples/wrap_fs_runner/main.go`:

```go
// Package main demonstrates the canonical adoption pattern for the
// xrr fs adapter: wrap an existing filesystem interface so it
// transparently records and replays through an xrr session.
//
// Pattern in three parts (same shape as wrap_command_runner):
//
//  1. Real — the existing app interface (stable, in production).
//  2. Wrapper — satisfies Real but routes through an xrr session.
//  3. Caller — uses Real and never knows xrr exists.
//
// For tests that mutate disk and need determinism, install a
// PathNormalizer mapping the test tmpdir to a stable placeholder
// like "$TMP". The cassette will store the normalized path and
// replay cleanly across test runs.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	xrr "hop.top/xrr"
	xfs "hop.top/xrr/adapters/fs"
)

// FS is the existing app interface — stable, used everywhere in the
// consuming codebase. We do NOT change it.
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

// RealFS shells out to the standard library. Production impl.
type RealFS struct{}

func (RealFS) WriteFile(_ context.Context, p string, d []byte, m os.FileMode) error {
	return os.WriteFile(p, d, m)
}
func (RealFS) Mkdir(_ context.Context, p string, m os.FileMode) error {
	return os.Mkdir(p, m)
}
func (RealFS) Remove(_ context.Context, p string) error    { return os.Remove(p) }
func (RealFS) RemoveAll(_ context.Context, p string) error { return os.RemoveAll(p) }
func (RealFS) Rename(_ context.Context, o, n string) error { return os.Rename(o, n) }
func (RealFS) Chmod(_ context.Context, p string, m os.FileMode) error {
	return os.Chmod(p, m)
}
func (RealFS) Chown(_ context.Context, p string, u, g int) error {
	return os.Chown(p, u, g)
}
func (RealFS) Symlink(_ context.Context, t, l string) error { return os.Symlink(t, l) }
func (RealFS) Link(_ context.Context, o, n string) error    { return os.Link(o, n) }
func (RealFS) Truncate(_ context.Context, p string, s int64) error {
	return os.Truncate(p, s)
}

// Wrapper satisfies FS but routes every call through an xrr session.
type Wrapper struct {
	inner   FS
	sess    *xrr.FileSession
	adapter *xfs.Adapter
}

// NewWrapper wires inner + session + adapter.
func NewWrapper(inner FS, sess *xrr.FileSession, adapter *xfs.Adapter) *Wrapper {
	return &Wrapper{inner: inner, sess: sess, adapter: adapter}
}

func (w *Wrapper) record(ctx context.Context, req *xfs.Request, do func() error) error {
	start := time.Now()
	_, err := w.sess.Record(ctx, w.adapter, req, func() (xrr.Response, error) {
		runErr := do()
		return &xfs.Response{DurationMs: time.Since(start).Milliseconds()}, runErr
	})
	return err
}

func (w *Wrapper) WriteFile(ctx context.Context, path string, data []byte, mode os.FileMode) error {
	m := uint32(mode)
	req := &xfs.Request{Op: xfs.OpWrite, Path: path, Data: data, Mode: &m}
	return w.record(ctx, req, func() error {
		return w.inner.WriteFile(ctx, path, data, mode)
	})
}

func (w *Wrapper) Mkdir(ctx context.Context, path string, mode os.FileMode) error {
	m := uint32(mode)
	req := &xfs.Request{Op: xfs.OpMkdir, Path: path, Mode: &m}
	return w.record(ctx, req, func() error { return w.inner.Mkdir(ctx, path, mode) })
}

func (w *Wrapper) Remove(ctx context.Context, path string) error {
	req := &xfs.Request{Op: xfs.OpRemove, Path: path}
	return w.record(ctx, req, func() error { return w.inner.Remove(ctx, path) })
}

func (w *Wrapper) RemoveAll(ctx context.Context, path string) error {
	req := &xfs.Request{Op: xfs.OpRemove, Path: path, Recursive: true}
	return w.record(ctx, req, func() error { return w.inner.RemoveAll(ctx, path) })
}

func (w *Wrapper) Rename(ctx context.Context, oldpath, newpath string) error {
	req := &xfs.Request{Op: xfs.OpRename, Path: oldpath, Dest: newpath}
	return w.record(ctx, req, func() error { return w.inner.Rename(ctx, oldpath, newpath) })
}

func (w *Wrapper) Chmod(ctx context.Context, path string, mode os.FileMode) error {
	m := uint32(mode)
	req := &xfs.Request{Op: xfs.OpChmod, Path: path, Mode: &m}
	return w.record(ctx, req, func() error { return w.inner.Chmod(ctx, path, mode) })
}

func (w *Wrapper) Chown(ctx context.Context, path string, uid, gid int) error {
	req := &xfs.Request{Op: xfs.OpChown, Path: path, UID: &uid, GID: &gid}
	return w.record(ctx, req, func() error { return w.inner.Chown(ctx, path, uid, gid) })
}

func (w *Wrapper) Symlink(ctx context.Context, target, link string) error {
	req := &xfs.Request{Op: xfs.OpSymlink, Path: target, Dest: link}
	return w.record(ctx, req, func() error { return w.inner.Symlink(ctx, target, link) })
}

func (w *Wrapper) Link(ctx context.Context, oldpath, newpath string) error {
	req := &xfs.Request{Op: xfs.OpHardlink, Path: oldpath, Dest: newpath}
	return w.record(ctx, req, func() error { return w.inner.Link(ctx, oldpath, newpath) })
}

func (w *Wrapper) Truncate(ctx context.Context, path string, size int64) error {
	req := &xfs.Request{Op: xfs.OpTruncate, Path: path, Size: &size}
	return w.record(ctx, req, func() error { return w.inner.Truncate(ctx, path, size) })
}

// main demonstrates record-then-replay against a tmpdir, with the
// canonical PathNormalizer installed.
func main() {
	tmp, err := os.MkdirTemp("", "xrr-fs-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	cassetteDir := filepath.Join(tmp, "cassettes")
	if err := os.MkdirAll(cassetteDir, 0o755); err != nil {
		log.Fatal(err)
	}
	target := filepath.Join(tmp, "hello.txt")

	normalizer := func(p string) string { return strings.Replace(p, tmp, "$TMP", 1) }
	adapter := xfs.NewAdapter().WithNormalizer(normalizer)
	ctx := context.Background()

	// Record
	{
		sess := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(cassetteDir))
		w := NewWrapper(RealFS{}, sess, adapter)
		if err := w.WriteFile(ctx, target, []byte("hello\n"), 0o644); err != nil {
			log.Fatalf("record WriteFile: %v", err)
		}
		fmt.Println("recorded:", target)
	}

	// Verify cassette landed
	entries, _ := os.ReadDir(cassetteDir)
	for _, e := range entries {
		fmt.Println("cassette:", e.Name())
	}

	// Replay against a panic-on-call inner FS — proves no disk write
	// happens on replay.
	{
		sess := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(cassetteDir))
		w := NewWrapper(panickyFS{}, sess, adapter)
		if err := w.WriteFile(ctx, target, []byte("hello\n"), 0o644); err != nil {
			log.Fatalf("replay WriteFile: %v", err)
		}
		fmt.Println("replayed without touching disk")
	}
}

// panickyFS panics on any call — used in replay to prove the inner
// FS is never invoked.
type panickyFS struct{}

func (panickyFS) WriteFile(context.Context, string, []byte, os.FileMode) error {
	panic("inner FS called during replay")
}
func (panickyFS) Mkdir(context.Context, string, os.FileMode) error { panic("nope") }
func (panickyFS) Remove(context.Context, string) error             { panic("nope") }
func (panickyFS) RemoveAll(context.Context, string) error          { panic("nope") }
func (panickyFS) Rename(context.Context, string, string) error     { panic("nope") }
func (panickyFS) Chmod(context.Context, string, os.FileMode) error { panic("nope") }
func (panickyFS) Chown(context.Context, string, int, int) error    { panic("nope") }
func (panickyFS) Symlink(context.Context, string, string) error    { panic("nope") }
func (panickyFS) Link(context.Context, string, string) error       { panic("nope") }
func (panickyFS) Truncate(context.Context, string, int64) error    { panic("nope") }
```

- [ ] **Step 2: Run the example**

Run: `cd go && go run ./examples/wrap_fs_runner/`

Expected output (paths will vary):
```
recorded: /var/folders/.../xrr-fs-example-.../hello.txt
cassette: fs-<8hex>.req.yaml
cassette: fs-<8hex>.resp.yaml
replayed without touching disk
```

- [ ] **Step 3: Verify the build with the rest of xrr**

Run: `cd go && go build ./...`

Expected: clean build, no errors.

- [ ] **Step 4: Commit**

```bash
git add go/examples/wrap_fs_runner/main.go
git commit -m "feat(fs): wrap_fs_runner example demonstrates adoption pattern"
```

---

## Task 6: Conformance fixture

**Files:**
- Create: `spec/fixtures/fs-write/manifest.yaml`
- Create: `spec/fixtures/fs-write/fs-<fp>.req.yaml`
- Create: `spec/fixtures/fs-write/fs-<fp>.resp.yaml`

The fixture is a single recorded `write` interaction. Path is already
normalized (no normalizer needed at replay — cassettes store
post-normalizer paths).

- [ ] **Step 1: Compute the fingerprint for the fixture request**

The fixture writes the bytes `"hello, world\n"` to `$TMP/greeting.txt`
with `Mode: 0o644`.

Write a one-off script to compute the fingerprint authoritatively:

Run:
```bash
cd go && cat > /tmp/fp_calc.go <<'EOF'
package main

import (
	"fmt"
	"os"

	xfs "hop.top/xrr/adapters/fs"
)

func main() {
	a := xfs.NewAdapter()
	mode := uint32(0o644)
	req := &xfs.Request{
		Op:   xfs.OpWrite,
		Path: "$TMP/greeting.txt",
		Data: []byte("hello, world\n"),
		Mode: &mode,
	}
	fp, err := a.Fingerprint(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(fp)
}
EOF
go run /tmp/fp_calc.go
```

Expected: a single line of 8 hex chars. **Record this value** —
you'll substitute it into the file names and envelopes below.
Call it `<FP>` in the steps below.

- [ ] **Step 2: Create the manifest**

Write `spec/fixtures/fs-write/manifest.yaml`:

```yaml
interactions:
  - adapter: fs
    fingerprint: "<FP>"
```

(Substitute `<FP>` with the value from Step 1.)

- [ ] **Step 3: Create the request envelope**

Write `spec/fixtures/fs-write/fs-<FP>.req.yaml` (substituting `<FP>`
into both the filename and the `fingerprint:` field):

```yaml
xrr: "1"
adapter: fs
fingerprint: "<FP>"
recorded_at: "2026-05-11T12:00:00Z"
payload:
  op: write
  path: "$TMP/greeting.txt"
  data: "hello, world\n"
  mode: 420
```

(`420` is `0o644` in decimal — YAML doesn't carry the octal notation
through `yaml.Marshal` of a `uint32`.)

- [ ] **Step 4: Create the response envelope**

Write `spec/fixtures/fs-write/fs-<FP>.resp.yaml`:

```yaml
xrr: "1"
adapter: fs
fingerprint: "<FP>"
recorded_at: "2026-05-11T12:00:00Z"
payload:
  duration_ms: 1
  bytes_written: 13
```

- [ ] **Step 5: Run conformance tests**

Run: `cd go && go test -run TestConformanceFixtures -v`

Expected: PASS, with a new subtest `TestConformanceFixtures/fs-write`
that loads both envelopes without `cassette miss`.

- [ ] **Step 6: Clean up the one-off script**

Run: `rm /tmp/fp_calc.go`

- [ ] **Step 7: Commit**

```bash
git add spec/fixtures/fs-write/
git commit -m "test(fs): conformance fixture locks cross-runtime contract for write op"
```

---

## Task 7: Cassette format spec update

**Files:**
- Modify: `spec/cassette-format-v1.md` (append "fs Adapter (v1)" section)

- [ ] **Step 1: Append the fs adapter section to the spec**

Edit `spec/cassette-format-v1.md`. After the existing "Exec Fingerprint
Inputs" section (around the end of the `cwd` extension subsection),
add the following new top-level section just before "Response Envelope
Example (exec, success)":

```markdown
## fs Adapter (v1)

The `fs` adapter records filesystem mutation operations. Reads are
not supported — tests should pre-seed disk state via fixtures and
use xrr only to assert on mutations.

### Operations

| Op         | Required fields           | Optional fields              |
|------------|---------------------------|------------------------------|
| `write`    | `path`, `data`            | `mode`, `flags`              |
| `mkdir`    | `path`                    | `mode`                       |
| `remove`   | `path`                    | `recursive`                  |
| `rename`   | `path`, `dest`            |                              |
| `chmod`    | `path`, `mode`            |                              |
| `chown`    | `path`, `uid`, `gid`      |                              |
| `symlink`  | `path` (target), `dest` (link) |                         |
| `hardlink` | `path` (old), `dest` (new)|                              |
| `truncate` | `path`, `size`            |                              |

### Fingerprint Inputs

```
fingerprint = sha256(canonical(fields))[:8]
```

Where `fields` is a map containing `op` and normalized `path`, plus
the following fields when present:

- `data_sha256` — the full sha256 hex of `data` bytes, when `data`
  is non-empty. **The raw bytes do NOT participate in the
  fingerprint**, only their hash; this keeps the cassette filename
  bounded regardless of payload size. The raw bytes still appear in
  the `.req.yaml` payload for human inspection and exact replay.
- `mode` — when set (presence-bearing, distinct from zero).
- `uid`, `gid` — when set.
- `dest` — when non-empty, after path normalization.
- `size` — when set.
- `flags` — when non-zero.
- `recursive` — when true.

`canonical(fields)` is JSON with keys sorted lexicographically.

### Path Normalization

Cassettes store paths in their **post-normalizer** form. A test
that writes to `/var/folders/abc/T/Test123/file` records the path
as `$TMP/file` (or whatever the normalizer rewrites it to). Replay
loaders read paths verbatim from the cassette and never re-derive
them — they do not need to know the original tmpdir.

This is the cross-runtime contract: ts/py/rs/php ports MUST replay
the normalized path as stored and MUST apply normalization on
record-side when producing cassettes for cross-runtime replay.

### Extension Status

The fs adapter is part of v1 as of [date this lands]. Like exec's
`cwd`, omit-on-zero rules mean adding a previously-unset field to
a request shape invalidates existing cassettes recorded by adopters
who didn't populate it. Adopters who change which optional fields
they populate should expect cassette re-recording.

### Request Envelope Example (fs write)

```yaml
xrr: "1"
adapter: fs
fingerprint: "<8hex>"
recorded_at: "2026-05-11T12:00:00Z"
payload:
  op: write
  path: "$TMP/config.yaml"
  data: "key: value\n"
  mode: 420
```

### Response Envelope Example (fs write, success)

```yaml
xrr: "1"
adapter: fs
fingerprint: "<8hex>"
recorded_at: "2026-05-11T12:00:00Z"
payload:
  duration_ms: 2
  bytes_written: 11
```

### Response Envelope Example (fs write, failure)

```yaml
xrr: "1"
adapter: fs
fingerprint: "<8hex>"
recorded_at: "2026-05-11T12:00:00Z"
error: "open $TMP/config.yaml: permission denied"
payload:
  duration_ms: 0
  bytes_written: 0
```

```

- [ ] **Step 2: Run a syntax check on the markdown**

Run: `cat spec/cassette-format-v1.md | head -50 && echo "---" && grep -c "^## " spec/cassette-format-v1.md`

Expected: file reads cleanly; section count increased by 1 vs.
pre-edit (you added one `## fs Adapter (v1)` section).

- [ ] **Step 3: Commit**

```bash
git add spec/cassette-format-v1.md
git commit -m "docs(spec): document fs adapter v1 in cassette-format spec"
```

---

## Task 8: README updates

**Files:**
- Modify: `README.md` (channel table + "Daemons and stateful servers" subsection)

- [ ] **Step 1: Add `fs` to the channel table**

In `README.md`, locate the line:

```
`xrr` records and replays interactions across any channel type (exec, HTTP, gRPC, Redis, SQL).
```

Replace with:

```
`xrr` records and replays interactions across any channel type (exec, HTTP, gRPC, Redis, SQL, fs).
```

Then locate any channel/adapter listing table in the README. If the README has a table mapping adapter ids to ports, add an `fs` row. If no such table exists (current state — verify before editing), skip this part and proceed to Step 2.

- [ ] **Step 2: Add "Daemons and stateful servers" subsection**

In `README.md`, find the `## Cassette Format` heading. Insert the
following new section IMMEDIATELY BEFORE it:

```markdown
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
```

- [ ] **Step 3: Verify the README still renders sensibly**

Run: `grep -n "^## " README.md | head -20`

Expected: the new `## Daemons and stateful servers` section appears
immediately before `## Cassette Format`.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs(readme): add fs to channel list + 'Daemons and stateful servers' section"
```

---

## Task 9: Full test sweep + integration check

**Files:** none (verification only)

- [ ] **Step 1: Run all Go tests**

Run: `cd go && go test ./... -v 2>&1 | tail -50`

Expected: every test passes. Look specifically for:
- `ok  hop.top/xrr/adapters/fs` (new package)
- `ok  hop.top/xrr` (conformance test now includes `fs-write`)
- No new failures in any other adapter.

If `go test ./...` shows unrelated pre-existing failures (per
existing `feedback_use_kit_packages.md` memory, some upstream
kit tests can flake), confirm they fail on `main` too before
declaring done.

- [ ] **Step 2: Verify the wrap_fs_runner example still runs**

Run: `cd go && go run ./examples/wrap_fs_runner/`

Expected: same output as Task 5 Step 2.

- [ ] **Step 3: Verify wrap_command_runner still runs (no regression)**

Run: `cd go && go run ./examples/wrap_command_runner/`

Expected: no panic, no build error, normal output. We don't care
exactly what it prints — only that we haven't broken it.

- [ ] **Step 4: gofmt check**

Run: `cd go && gofmt -l .`

Expected: empty output. Any listed files are unformatted; run
`gofmt -w <file>` and amend the relevant commit.

- [ ] **Step 5: go vet**

Run: `cd go && go vet ./...`

Expected: empty output.

- [ ] **Step 6: If any of the above failed, fix and commit**

If formatting, vet, or test fixes are needed:

```bash
git add -u
git commit -m "chore(fs): formatting/vet fixes"
```

If clean, no commit needed.

---

## Task 10: Update tlc track + push

**Files:** none (workflow only)

- [ ] **Step 1: Mark the track done in tlc**

Run: `tlc track update fs-adapter --status done`

Expected: confirmation message.

- [ ] **Step 2: Push the branch**

Run: `git push -u origin fs-adapter`

Expected: branch published to GitHub.

- [ ] **Step 3: Open a PR**

Run:
```bash
gh pr create --title "feat(fs): adapter for filesystem mutations + daemon docs" --body "$(cat <<'EOF'
## Summary

- New `fs` adapter (`go/adapters/fs/`) recording 9 mutation ops: write, mkdir, remove, rename, chmod, chown, symlink, hardlink, truncate.
- Pluggable `PathNormalizer` hook handles tmpdir-based tests producing stable cassette keys.
- Inline binary payloads via yaml.v3 `!!binary`; data hashed (not raw) into fingerprint.
- Conformance fixture at `spec/fixtures/fs-write/` locks cross-runtime contract.
- README adds "Daemons and stateful servers" section documenting three patterns (wire-adapter / in-process fake / state-machine snapshot).

Spec: `docs/superpowers/specs/2026-05-11-fs-adapter-design.md`
Plan: `docs/superpowers/plans/2026-05-11-fs-adapter.md`

## Out of scope (follow-on tracks)

- ts/py/rs/php ports of `fs` — each port replays the new conformance fixture, lands on its own branch.

## Test plan

- [x] `go test ./adapters/fs/` passes
- [x] `go test -run TestConformanceFixtures` includes new `fs-write` subtest
- [x] `go run ./examples/wrap_fs_runner/` records + replays cleanly
- [x] `go run ./examples/wrap_command_runner/` still works (no regression)
- [x] `gofmt -l . && go vet ./...` clean
EOF
)"
```

Expected: PR URL printed.

---

## Self-Review Findings

After writing the plan, the spec was checked section by section:

**Spec coverage** — every spec requirement maps to a task:
- Request/Response shapes → Task 1
- Operations table → Task 1 (`Op` constants) + Task 7 (spec doc)
- Fingerprint algorithm → Task 2
- Path Normalizer (WithNormalizer, Chain, rules) → Tasks 1 + 3
- Wrapper example with canonical adoption pattern → Task 5
- Tests (5 explicit cases in spec) → Tasks 2, 3, 4, 6
- Cassette format doc updates → Task 7
- README updates (channel list + daemon subsection) → Task 8
- Conformance fixture → Task 6
- ts/py/rs/php follow-on framing → captured in Task 10 PR body

**Placeholder scan** — no TBDs, no "TODO later", no "similar to Task N". Every code block contains literal code to type.

**Type consistency** — `Op` constants used everywhere in tests and example; `PathNormalizer` signature stable; `Adapter`/`Request`/`Response` field names match between Task 1 definition, Task 4 serialization tests, Task 5 wrapper construction, and Task 6 fixture.

**One fix applied inline during review** — Task 2's `TestFingerprintRejectsWrongType` initially referenced an exec request type (potential import cycle). Switched to a local `notFsRequest` stub.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-11-fs-adapter.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration. Good for this plan: 10 tasks, each self-contained, no cross-task code reuse beyond what's already in `fs.go`.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
