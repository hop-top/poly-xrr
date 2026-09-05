package main

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	xrr "hop.top/xrr"
	xfs "hop.top/xrr/adapters/fs"
)

// tmpAdapter returns the canonical adapter for a tmpdir-based run:
// the tmpdir prefix is rewritten to "$TMP" so the cassette is stable
// across runs and machines.
func tmpAdapter(tmp string) *xfs.Adapter {
	return xfs.NewAdapter().WithNormalizer(func(p string) string {
		return strings.Replace(p, tmp, "$TMP", 1)
	})
}

// envelopeView is the subset of the on-disk envelope the tests inspect.
type envelopeView struct {
	Adapter     string         `yaml:"adapter"`
	Fingerprint string         `yaml:"fingerprint"`
	Error       string         `yaml:"error"`
	Payload     map[string]any `yaml:"payload"`
}

func readEnvelope(t *testing.T, path string) envelopeView {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var env envelopeView
	require.NoError(t, yaml.Unmarshal(raw, &env))
	return env
}

// cassetteNames lists dir sorted. The fingerprint is embedded in the
// filename, so equal listings mean equal fingerprints.
func cassetteNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

// snapshot returns name → content for every file in dir.
func snapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, name := range cassetteNames(t, dir) {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		out[name] = string(raw)
	}
	return out
}

// scenario drives every FS method once under base, ordered so each
// real op is valid when it runs (mkdir before write, write before
// chmod, ...). Returns the per-call errors in call order.
func scenario(ctx context.Context, w FS, base string) []error {
	dir := filepath.Join(base, "dir")
	file := filepath.Join(dir, "file.txt")
	link := filepath.Join(base, "file.lnk")
	hard := filepath.Join(base, "file.hard")
	moved := filepath.Join(base, "moved.txt")

	var errs []error
	errs = append(errs, w.Mkdir(ctx, dir, 0o755))
	errs = append(errs, w.WriteFile(ctx, file, []byte("hello\n"), 0o644))
	errs = append(errs, w.Chmod(ctx, file, 0o600))
	errs = append(errs, w.Chown(ctx, file, os.Getuid(), os.Getgid()))
	errs = append(errs, w.Truncate(ctx, file, 2))
	errs = append(errs, w.Symlink(ctx, file, link))
	errs = append(errs, w.Link(ctx, file, hard))
	errs = append(errs, w.Rename(ctx, file, moved))
	errs = append(errs, w.Remove(ctx, link))
	errs = append(errs, w.RemoveAll(ctx, dir))
	return errs
}

// TestReplayNeverTouchesInnerFS records the full scenario through
// RealFS, then replays it through panickyFS against a different
// tmpdir. Every call must come back from the cassette (nil error, no
// panic), the fresh tmpdir must stay empty, and the cassette must be
// byte-identical after replay.
func TestReplayNeverTouchesInnerFS(t *testing.T) {
	ctx := context.Background()
	cassettes := t.TempDir()

	recordBase := t.TempDir()
	rec := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(cassettes))
	for i, err := range scenario(ctx, NewWrapper(RealFS{}, rec, tmpAdapter(recordBase)), recordBase) {
		require.NoError(t, err, "record call %d", i)
	}
	before := snapshot(t, cassettes)
	require.Len(t, before, 20, "10 calls → 10 req/resp pairs")

	replayBase := t.TempDir()
	rep := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(cassettes))
	for i, err := range scenario(ctx, NewWrapper(panickyFS{}, rep, tmpAdapter(replayBase)), replayBase) {
		assert.NoError(t, err, "replay call %d", i)
	}

	entries, err := os.ReadDir(replayBase)
	require.NoError(t, err)
	assert.Empty(t, entries, "replay must not touch the filesystem")
	assert.Equal(t, before, snapshot(t, cassettes), "replay must not rewrite the cassette")
}

// TestRecordWriteFilePersistsToDiskAndCassette is the single-op
// contract: record mode performs the real write AND lands a req/resp
// pair whose payload carries the normalized path, never the tmpdir.
func TestRecordWriteFilePersistsToDiskAndCassette(t *testing.T) {
	ctx := context.Background()
	base, cassettes := t.TempDir(), t.TempDir()
	target := filepath.Join(base, "hello.txt")

	sess := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(cassettes))
	w := NewWrapper(RealFS{}, sess, tmpAdapter(base))
	require.NoError(t, w.WriteFile(ctx, target, []byte("hello\n"), 0o644))

	got, err := os.ReadFile(target)
	require.NoError(t, err)
	assert.Equal(t, "hello\n", string(got))

	names := cassetteNames(t, cassettes)
	require.Len(t, names, 2)
	assert.Regexp(t, `^fs-[0-9a-f]{8}\.req\.yaml$`, names[0])
	assert.Regexp(t, `^fs-[0-9a-f]{8}\.resp\.yaml$`, names[1])

	req := readEnvelope(t, filepath.Join(cassettes, names[0]))
	assert.Equal(t, "fs", req.Adapter)
	assert.Equal(t, xfs.OpWrite, req.Payload["op"])
	assert.Equal(t, "$TMP/hello.txt", req.Payload["path"])
	assert.Equal(t, "hello\n", req.Payload["data"])
	assert.Equal(t, 0o644, req.Payload["mode"])
	assert.Empty(t, req.Error)

	resp := readEnvelope(t, filepath.Join(cassettes, names[1]))
	assert.Equal(t, req.Fingerprint, resp.Fingerprint)
	assert.Empty(t, resp.Error, "successful write must not persist an error")

	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(cassettes, name))
		require.NoError(t, err)
		assert.NotContains(t, string(raw), base, "%s leaks the raw tmpdir", name)
	}
}

// TestRecordAllOpsMutateDiskAndNormalizePaths drives all nine ops
// through RealFS and checks both sides: the disk reflects every
// mutation, and every envelope stores "$TMP/"-prefixed paths.
func TestRecordAllOpsMutateDiskAndNormalizePaths(t *testing.T) {
	ctx := context.Background()
	base, cassettes := t.TempDir(), t.TempDir()

	sess := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(cassettes))
	for i, err := range scenario(ctx, NewWrapper(RealFS{}, sess, tmpAdapter(base)), base) {
		require.NoError(t, err, "record call %d", i)
	}

	// Disk: dir removed, file truncated+chmod'ed+renamed, hard link
	// kept, symlink removed.
	assert.NoDirExists(t, filepath.Join(base, "dir"))
	assert.NoFileExists(t, filepath.Join(base, "file.lnk"))
	moved, err := os.ReadFile(filepath.Join(base, "moved.txt"))
	require.NoError(t, err)
	assert.Equal(t, "he", string(moved), "truncate to 2 bytes must land")
	info, err := os.Stat(filepath.Join(base, "moved.txt"))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	hard, err := os.ReadFile(filepath.Join(base, "file.hard"))
	require.NoError(t, err)
	assert.Equal(t, "he", string(hard), "hard link shares the inode")

	// Cassette: one pair per call, every op represented, paths normalized.
	names := cassetteNames(t, cassettes)
	require.Len(t, names, 20)
	ops := map[string]int{}
	for _, name := range names {
		if !strings.HasSuffix(name, ".req.yaml") {
			continue
		}
		env := readEnvelope(t, filepath.Join(cassettes, name))
		op, _ := env.Payload["op"].(string)
		ops[op]++
		path, _ := env.Payload["path"].(string)
		assert.True(t, strings.HasPrefix(path, "$TMP/"), "%s path %q", name, path)
		if dest, ok := env.Payload["dest"].(string); ok {
			assert.True(t, strings.HasPrefix(dest, "$TMP/"), "%s dest %q", name, dest)
		}
		assert.NotContains(t, path, base)
	}
	assert.Equal(t, map[string]int{
		xfs.OpMkdir: 1, xfs.OpWrite: 1, xfs.OpChmod: 1, xfs.OpChown: 1,
		xfs.OpTruncate: 1, xfs.OpSymlink: 1, xfs.OpHardlink: 1,
		xfs.OpRename: 1, xfs.OpRemove: 2,
	}, ops)
}

// TestNormalizerStabilizesFingerprintAcrossTempDirs is the property
// adopters rely on: two runs in two different tmpdirs produce the
// same cassette filenames. The control run with the identity
// normalizer proves the stability comes from the normalizer.
func TestNormalizerStabilizesFingerprintAcrossTempDirs(t *testing.T) {
	ctx := context.Background()
	record := func(base string, adapter *xfs.Adapter) []string {
		cassettes := t.TempDir()
		sess := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(cassettes))
		for i, err := range scenario(ctx, NewWrapper(RealFS{}, sess, adapter), base) {
			require.NoError(t, err, "record call %d", i)
		}
		return cassetteNames(t, cassettes)
	}

	a, b := t.TempDir(), t.TempDir()
	require.NotEqual(t, a, b)
	assert.Equal(t, record(a, tmpAdapter(a)), record(b, tmpAdapter(b)),
		"normalized runs must share fingerprints")

	c, d := t.TempDir(), t.TempDir()
	assert.NotEqual(t, record(c, xfs.NewAdapter()), record(d, xfs.NewAdapter()),
		"identity normalizer leaks the tmpdir into the fingerprint")
}

// TestReplayMissSurfacesError: a request whose fingerprint has no
// cassette pair must fail with ErrCassetteMiss and never fall
// through to the inner FS.
func TestReplayMissSurfacesError(t *testing.T) {
	ctx := context.Background()
	base, cassettes := t.TempDir(), t.TempDir()
	target := filepath.Join(base, "hello.txt")
	adapter := tmpAdapter(base)

	rec := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(cassettes))
	require.NoError(t, NewWrapper(RealFS{}, rec, adapter).WriteFile(ctx, target, []byte("hello\n"), 0o644))

	rep := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(cassettes))
	w := NewWrapper(panickyFS{}, rep, adapter)

	assert.NoError(t, w.WriteFile(ctx, target, []byte("hello\n"), 0o644), "exact match replays")

	err := w.WriteFile(ctx, target, []byte("changed\n"), 0o644)
	require.ErrorIs(t, err, xrr.ErrCassetteMiss, "different data → different fingerprint")

	err = w.WriteFile(ctx, filepath.Join(base, "other.txt"), []byte("hello\n"), 0o644)
	require.ErrorIs(t, err, xrr.ErrCassetteMiss, "different path → different fingerprint")

	err = w.WriteFile(ctx, target, []byte("hello\n"), 0o600)
	require.ErrorIs(t, err, xrr.ErrCassetteMiss, "different mode → different fingerprint")
}

// TestRecordedErrorReplaysWithoutInnerFS: a failing real op is
// persisted in the resp envelope's error field, and replay re-emits
// the same error string without consulting the inner FS.
func TestRecordedErrorReplaysWithoutInnerFS(t *testing.T) {
	ctx := context.Background()
	base, cassettes := t.TempDir(), t.TempDir()
	target := filepath.Join(base, "missing", "x.txt")
	adapter := tmpAdapter(base)

	rec := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(cassettes))
	recErr := NewWrapper(RealFS{}, rec, adapter).WriteFile(ctx, target, []byte("x"), 0o644)
	require.Error(t, recErr)
	assert.ErrorIs(t, recErr, os.ErrNotExist, "real error surfaces verbatim in record mode")
	assert.NoFileExists(t, target)

	names := cassetteNames(t, cassettes)
	require.Len(t, names, 2, "failed ops still land a req/resp pair")
	resp := readEnvelope(t, filepath.Join(cassettes, names[1]))
	assert.Equal(t, recErr.Error(), resp.Error)

	rep := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(cassettes))
	repErr := NewWrapper(panickyFS{}, rep, adapter).WriteFile(ctx, target, []byte("x"), 0o644)
	require.Error(t, repErr)
	assert.Equal(t, recErr.Error(), repErr.Error())
	assert.NoFileExists(t, target)
}

// TestMainFlowRecordThenReplay mirrors main() step for step under
// t.TempDir(): record one write through RealFS, replay it through
// panickyFS with the same adapter, and require the cassette to be
// byte-identical afterwards (replay is read-only).
func TestMainFlowRecordThenReplay(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	cassetteDir := filepath.Join(tmp, "cassettes")
	require.NoError(t, os.MkdirAll(cassetteDir, 0o755))
	target := filepath.Join(tmp, "hello.txt")
	adapter := tmpAdapter(tmp)

	rec := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(cassetteDir))
	require.NoError(t, NewWrapper(RealFS{}, rec, adapter).WriteFile(ctx, target, []byte("hello\n"), 0o644))
	before := snapshot(t, cassetteDir)
	require.Len(t, before, 2)

	rep := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(cassetteDir))
	require.NoError(t, NewWrapper(panickyFS{}, rep, adapter).WriteFile(ctx, target, []byte("hello\n"), 0o644))
	assert.Equal(t, before, snapshot(t, cassetteDir), "record→replay must leave no diff")
}

// TestMainRuns executes the shipped example end to end via run(), the
// error-returning body behind main(), so a failure fails this test
// instead of exiting the test binary.
func TestMainRuns(t *testing.T) {
	require.NoError(t, run())
}
