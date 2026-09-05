package fs_test

import (
	"encoding/base64"
	"path/filepath"
	"strings"
	"testing"

	xrr "hop.top/xrr"
	"hop.top/xrr/adapters/fs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Task 1: identity tests.

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

// Task 2: fingerprint tests.

func TestFingerprintDeterministic(t *testing.T) {
	a := fs.NewAdapter()
	req := &fs.Request{Op: fs.OpWrite, Path: "/etc/hosts", Data: "127.0.0.1 localhost\n"}
	fp1, err := a.Fingerprint(req)
	require.NoError(t, err)
	assert.Len(t, fp1, 8, "fingerprint must be 8 hex chars")
	fp2, err := a.Fingerprint(req)
	require.NoError(t, err)
	assert.Equal(t, fp1, fp2, "same request must hash identically")
}

func TestFingerprintDiscriminatesPath(t *testing.T) {
	a := fs.NewAdapter()
	fpA, _ := a.Fingerprint(&fs.Request{Op: fs.OpWrite, Path: "/a", Data: "x"})
	fpB, _ := a.Fingerprint(&fs.Request{Op: fs.OpWrite, Path: "/b", Data: "x"})
	assert.NotEqual(t, fpA, fpB)
}

func TestFingerprintDiscriminatesData(t *testing.T) {
	a := fs.NewAdapter()
	fpA, _ := a.Fingerprint(&fs.Request{Op: fs.OpWrite, Path: "/x", Data: "foo"})
	fpB, _ := a.Fingerprint(&fs.Request{Op: fs.OpWrite, Path: "/x", Data: "bar"})
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
	bare := &fs.Request{Op: fs.OpWrite, Path: "/x", Data: "y"}
	withNilMode := &fs.Request{Op: fs.OpWrite, Path: "/x", Data: "y", Mode: nil}
	fpA, _ := a.Fingerprint(bare)
	fpB, _ := a.Fingerprint(withNilMode)
	assert.Equal(t, fpA, fpB, "Mode: nil must omit `mode` from fingerprint")
}

func TestFingerprintPointerToZeroIncludesField(t *testing.T) {
	a := fs.NewAdapter()
	zero := uint32(0)
	bare := &fs.Request{Op: fs.OpWrite, Path: "/x", Data: "y"}
	withZeroMode := &fs.Request{Op: fs.OpWrite, Path: "/x", Data: "y", Mode: &zero}
	fpA, _ := a.Fingerprint(bare)
	fpB, _ := a.Fingerprint(withZeroMode)
	assert.NotEqual(t, fpA, fpB,
		"Mode: &0 must include `mode: 0` in fingerprint; differs from Mode: nil")
}

func TestFingerprintRejectsWrongType(t *testing.T) {
	a := fs.NewAdapter()
	_, err := a.Fingerprint(notFsRequest{})
	assert.Error(t, err)
}

type notFsRequest struct{}

func (notFsRequest) AdapterID() string { return "not-fs" }

// TestFingerprintConformanceFsWrite — cross-runtime conformance: the
// spec/fixtures/fs-write request MUST hash to 667a7680, the value the
// TypeScript, Python, Rust, and PHP ports each pin. The request is loaded
// through the cassette rather than hand-built so the fixture bytes are
// pinned too: a drift in either the fixture or the fingerprint fails here.
func TestFingerprintConformanceFsWrite(t *testing.T) {
	dir := filepath.Join("..", "..", "..", "spec", "fixtures", "fs-write")
	var req fs.Request
	var resp fs.Response
	_, err := xrr.NewFileCassette(dir).Load("fs", "667a7680", &req, &resp)
	require.NoError(t, err)

	assert.Equal(t, fs.OpWrite, req.Op)
	assert.Equal(t, "$TMP/greeting.txt", req.Path)
	assert.Equal(t, "hello, world\n", req.Data)
	require.NotNil(t, req.Mode)
	assert.Equal(t, uint32(420), *req.Mode) // 0o644

	fp, err := fs.NewAdapter().Fingerprint(&req)
	require.NoError(t, err)
	assert.Equal(t, "667a7680", fp, "spec conformance fingerprint mismatch")
}

// Task 3: PathNormalizer tests.

func TestNormalizerAppliedToFingerprint(t *testing.T) {
	plain := fs.NewAdapter()
	norm := fs.NewAdapter().WithNormalizer(func(p string) string {
		return strings.Replace(p, "/var/folders/abc/T/Test123", "$TMP", 1)
	})

	rawReq := &fs.Request{Op: fs.OpWrite, Path: "/var/folders/abc/T/Test123/config.yaml", Data: "k: v"}
	normReq := &fs.Request{Op: fs.OpWrite, Path: "$TMP/config.yaml", Data: "k: v"}

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

	req := &fs.Request{Op: fs.OpWrite, Path: "/tmp/foo", Data: "x"}
	req2 := &fs.Request{Op: fs.OpWrite, Path: "$TMP/foo", Data: "x"}
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

// TestDestGatedOnNormalizedValue — spec: `dest` participates only
// when non-empty AFTER path normalization. A normalizer that maps a
// non-empty dest to "" drops it from the fingerprint, so the request
// hashes identically to one with no dest at all. The no-dest hash is
// the cross-port vector sha256(`{"op":"rename","path":"/a"}`)[:8].
func TestDestGatedOnNormalizedValue(t *testing.T) {
	a := fs.NewAdapter().WithNormalizer(func(p string) string {
		if p == "/x/drop" {
			return ""
		}
		return p
	})
	noDest := &fs.Request{Op: fs.OpRename, Path: "/a"}
	dropped := &fs.Request{Op: fs.OpRename, Path: "/a", Dest: "/x/drop"}
	kept := &fs.Request{Op: fs.OpRename, Path: "/a", Dest: "/x/keep"}

	fpNoDest, err := a.Fingerprint(noDest)
	require.NoError(t, err)
	fpDropped, err := a.Fingerprint(dropped)
	require.NoError(t, err)
	fpKept, err := a.Fingerprint(kept)
	require.NoError(t, err)

	assert.Equal(t, "86f75341", fpNoDest, "cross-port no-dest vector")
	assert.Equal(t, fpNoDest, fpDropped,
		"dest normalized to \"\" must drop out of the fingerprint")
	assert.NotEqual(t, fpNoDest, fpKept,
		"dest that survives normalization must still discriminate")

	// The persisted copy agrees: a wrapper that pre-normalizes Dest
	// (per the PathNormalizer contract) stores no `dest:` at all.
	stored := &fs.Request{Op: fs.OpRename, Path: "/a", Dest: a.Normalize("/x/drop")}
	data, err := a.Serialize(stored)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "dest:")
}

// TestEmptyDestStaysOmittedRegardlessOfNormalizer — an unset Dest
// never reaches the normalizer (Normalize short-circuits on ""), so a
// normalizer that would rewrite "" cannot conjure a dest into the
// fingerprint.
func TestEmptyDestStaysOmittedRegardlessOfNormalizer(t *testing.T) {
	a := fs.NewAdapter().WithNormalizer(func(p string) string {
		if p == "" {
			return "/ghost"
		}
		return p
	})
	fpNoDest, err := a.Fingerprint(&fs.Request{Op: fs.OpRename, Path: "/a"})
	require.NoError(t, err)
	fpEmpty, err := a.Fingerprint(&fs.Request{Op: fs.OpRename, Path: "/a", Dest: ""})
	require.NoError(t, err)
	assert.Equal(t, fpNoDest, fpEmpty)
}

// Task 4: Serialize/Deserialize tests.

func TestSerializeRoundtrip(t *testing.T) {
	a := fs.NewAdapter()
	mode := uint32(0o644)
	req := &fs.Request{
		Op:    fs.OpWrite,
		Path:  "/etc/hosts",
		Data:  "127.0.0.1 localhost\n",
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

func TestSerializeBase64Payload(t *testing.T) {
	// Caller base64-encodes binary data before passing to the adapter.
	// The adapter serializes it as an opaque UTF-8 string. Round-trip
	// recovers the exact string; the caller is responsible for
	// base64-decoding back to bytes.
	a := fs.NewAdapter()
	rawBinary := []byte{0x00, 0xff, 0xc3, 0x28, 0x80, 0x01, 0x02, 0x03}
	encoded := base64.StdEncoding.EncodeToString(rawBinary)
	req := &fs.Request{Op: fs.OpWrite, Path: "/bin/x", Data: encoded}
	data, err := a.Serialize(req)
	require.NoError(t, err)
	t.Logf("YAML output:\n%s", string(data))
	var got fs.Request
	require.NoError(t, a.Deserialize(data, &got))
	assert.Equal(t, encoded, got.Data, "base64 string must round-trip exactly")
	// And caller can recover the original bytes.
	decoded, err := base64.StdEncoding.DecodeString(got.Data)
	require.NoError(t, err)
	assert.Equal(t, rawBinary, decoded)
}

func TestSerializeOmitsZeroOptionals(t *testing.T) {
	// A bare write request must not emit `dest:`, `mode:`, `uid:`,
	// `gid:`, `size:`, `flags:`, or `recursive:` in YAML.
	a := fs.NewAdapter()
	req := &fs.Request{Op: fs.OpWrite, Path: "/x", Data: "y"}
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

// TestNormalizeIsPublicAndIdempotent — wrappers call Adapter.Normalize
// when building Request.Path / Request.Dest so the persisted
// cassette payload agrees with what Fingerprint hashes. Regression
// test for the spec contract "cassettes store post-normalizer paths"
// (without this, Fingerprint normalizes but Serialize persists raw).
func TestNormalizeIsPublicAndIdempotent(t *testing.T) {
	a := fs.NewAdapter().WithNormalizer(func(p string) string {
		return strings.Replace(p, "/tmp/run-123", "$TMP", 1)
	})

	// Public API: wrappers can call Normalize at request construction.
	assert.Equal(t, "$TMP/x", a.Normalize("/tmp/run-123/x"))
	assert.Equal(t, "$TMP/x", a.Normalize("$TMP/x"),
		"normalizing an already-normalized path is a no-op for this rule")
	assert.Equal(t, "", a.Normalize(""),
		"empty path short-circuits without invoking the normalizer")
}

// TestSerializeStoresPostNormalizerPath — when a wrapper has
// pre-normalized Path/Dest before constructing the Request, the
// YAML payload written to disk MUST contain the normalized form.
// This is the contract cross-runtime replay relies on.
func TestSerializeStoresPostNormalizerPath(t *testing.T) {
	a := fs.NewAdapter().WithNormalizer(func(p string) string {
		return strings.Replace(p, "/tmp/run-123", "$TMP", 1)
	})

	// Wrappers MUST do this normalization at construction time.
	req := &fs.Request{
		Op:   fs.OpRename,
		Path: a.Normalize("/tmp/run-123/old"),
		Dest: a.Normalize("/tmp/run-123/new"),
	}

	data, err := a.Serialize(req)
	require.NoError(t, err)
	out := string(data)
	assert.Contains(t, out, "path: $TMP/old")
	assert.Contains(t, out, "dest: $TMP/new")
	assert.NotContains(t, out, "/tmp/run-123",
		"serialized payload must not leak the raw tmpdir path")
}

// Cross-port hazard vector (spec: Fingerprint Algorithm). The same path
// pins the same fingerprint in every port.
func TestFingerprintHazardVector(t *testing.T) {
	hazard := "a&b<c>/é" + string(rune(0x2028)) + string(rune(0x2029)) + "\b\f\x1f\x7f"
	fp, err := fs.NewAdapter().Fingerprint(&fs.Request{Op: "write", Path: hazard})
	require.NoError(t, err)
	assert.Equal(t, "6f2fb087", fp)
}
