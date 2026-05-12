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

	"gopkg.in/yaml.v3"
	xrr "hop.top/xrr"
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
//
// Data is a UTF-8 string, not a byte slice. yaml.v3 serializes
// []byte as a YAML sequence-of-ints (not !!binary), which would
// break cross-runtime cassette portability — every other YAML
// library handles strings identically. Callers MUST base64-encode
// non-UTF-8 binary payloads themselves before passing them to the
// adapter, and base64-decode on read. See
// spec/cassette-format-v1.md "Data Field Encoding".
type Request struct {
	Op        string  `yaml:"op"             json:"op"`
	Path      string  `yaml:"path"           json:"path"`
	Data      string  `yaml:"data,omitempty" json:"data,omitempty"`
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

// PathNormalizer rewrites a path before it enters the fingerprint.
// Default is identity. Returning "" is allowed (treated literally —
// adopters can drop path info if they really want to).
//
// To honor the spec's "cassettes store post-normalizer paths"
// contract, WRAPPERS must apply the normalizer to Request.Path and
// Request.Dest BEFORE constructing the Request — the adapter's
// Serialize method receives the Request verbatim and persists
// whatever it is given. Call Adapter.Normalize for the canonical
// rewrite at the call site.
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

// Normalize applies the installed PathNormalizer to p. Wrappers
// call this when building Request.Path / Request.Dest so the
// values stored on the cassette envelope agree with what the
// fingerprint hashes.
//
// Empty input short-circuits and returns "" without invoking the
// normalizer — adopters can pass paths through unconditionally
// without guarding for the optional-Dest case.
func (a *Adapter) Normalize(p string) string {
	if p == "" {
		return ""
	}
	return a.normalizer(p)
}

// normalize is the unexported internal entry point still used by
// Fingerprint. Kept as a thin alias so the call sites inside
// Fingerprint don't need rewriting.
func (a *Adapter) normalize(p string) string { return a.Normalize(p) }

// ID returns the adapter id.
func (a *Adapter) ID() string { return "fs" }

// Fingerprint returns sha256(canonical JSON of selected fields)[:8].
//
// Field selection rules:
//   - op and path are always included; path is path-normalized.
//   - data is hashed (full sha256 hex of its UTF-8 bytes) and included
//     as data_sha256 when non-empty. Raw bytes are NOT in the
//     fingerprint — keeps the 8-char filename suffix bounded for any
//     payload size.
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
	if r.Data != "" {
		sum := sha256.Sum256([]byte(r.Data))
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

// Serialize marshals v as YAML.
func (a *Adapter) Serialize(v any) ([]byte, error) {
	return yaml.Marshal(v)
}

// Deserialize unmarshals data into target.
func (a *Adapter) Deserialize(data []byte, target any) error {
	return yaml.Unmarshal(data, target)
}
