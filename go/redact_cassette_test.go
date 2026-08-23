package xrr_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	xrr "hop.top/xrr"
	execa "hop.top/xrr/adapters/exec"
	httpa "hop.top/xrr/adapters/http"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readAll concatenates every file written under dir so a test can assert
// on "nothing anywhere in the cassette dir contains this string".
func readAll(t *testing.T, dir string) string {
	t.Helper()
	var sb strings.Builder
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, err)
		sb.Write(b)
		sb.WriteString("\n")
	}
	return sb.String()
}

const (
	secretToken  = "ghp_supersecrettokenvalue0123456789abcd"
	secretBearer = "Bearer sk-livesecretvalue0123456789ABCDEFGHIJ"
)

// TestRecord_ExecEnvSecretNeverHitsDisk is the headline guarantee: a
// credential present in Request.Env at record time must not appear in
// any byte written to the cassette directory.
func TestRecord_ExecEnvSecretNeverHitsDisk(t *testing.T) {
	dir := t.TempDir()
	sess := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	adapter := execa.NewAdapter()

	req := &execa.Request{
		Argv: []string{"gh", "pr", "view", "1"},
		Env: map[string]string{
			"GITHUB_TOKEN": secretToken,
			"PATH":         "/usr/local/bin:/usr/bin",
		},
	}
	_, err := sess.Record(context.Background(), adapter, req, func() (xrr.Response, error) {
		return &execa.Response{Stdout: "ok\n", ExitCode: 0}, nil
	})
	require.NoError(t, err)

	onDisk := readAll(t, dir)
	assert.NotContains(t, onDisk, secretToken, "secret env value leaked into cassette")
	assert.Contains(t, onDisk, "<redacted:GITHUB_TOKEN>", "expected deterministic placeholder")
	// Benign env survives — redaction must not nuke useful debugging context.
	assert.Contains(t, onDisk, "/usr/local/bin:/usr/bin")
}

// TestRecord_HTTPAuthHeaderNeverHitsDisk — same guarantee for the
// Authorization header on both request and response.
func TestRecord_HTTPAuthHeaderNeverHitsDisk(t *testing.T) {
	dir := t.TempDir()
	sess := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	adapter := httpa.NewAdapter()

	req := &httpa.Request{
		Method: "GET",
		URL:    "https://api.example.com/v1/user",
		Headers: map[string]string{
			"Authorization": secretBearer,
			"Accept":        "application/json",
		},
	}
	_, err := sess.Record(context.Background(), adapter, req, func() (xrr.Response, error) {
		return &httpa.Response{
			Status: 200,
			Headers: map[string]string{
				"Set-Cookie":   "session=abcdef; HttpOnly",
				"Content-Type": "application/json",
			},
			Body: `{"login":"octocat"}`,
		}, nil
	})
	require.NoError(t, err)

	onDisk := readAll(t, dir)
	assert.NotContains(t, onDisk, secretBearer, "Authorization header leaked into cassette")
	assert.NotContains(t, onDisk, "session=abcdef", "Set-Cookie leaked into cassette")
	assert.Contains(t, onDisk, "<redacted:AUTHORIZATION>")
	assert.Contains(t, onDisk, "<redacted:SET-COOKIE>")
	// Benign headers and body survive.
	assert.Contains(t, onDisk, "application/json")
	assert.Contains(t, onDisk, "octocat")
}

// TestRecord_ValuePatternSecretNeverHitsDisk — the env var name gives no
// hint, only the value shape does. This is the case name-based matching
// alone would miss.
func TestRecord_ValuePatternSecretNeverHitsDisk(t *testing.T) {
	dir := t.TempDir()
	sess := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	adapter := execa.NewAdapter()

	req := &execa.Request{
		Argv: []string{"deploy"},
		Env:  map[string]string{"DEPLOY_HANDLE": secretToken},
	}
	_, err := sess.Record(context.Background(), adapter, req, func() (xrr.Response, error) {
		return &execa.Response{Stdout: "done\n"}, nil
	})
	require.NoError(t, err)

	onDisk := readAll(t, dir)
	assert.NotContains(t, onDisk, secretToken)
	assert.Contains(t, onDisk, "<redacted:DEPLOY_HANDLE>")
}

// TestRecord_RedactionIsFingerprintStable — the same request recorded
// twice produces the same fingerprint (same filename), and that
// fingerprint equals the one computed with redaction disabled.
//
// This is the cross-language contract: fingerprints are computed from
// {argv, stdin, cwd?} only, so redacting env/headers cannot shift them.
// If this ever fails, ports would disagree on cassette filenames.
func TestRecord_RedactionIsFingerprintStable(t *testing.T) {
	adapter := execa.NewAdapter()
	newReq := func() *execa.Request {
		return &execa.Request{
			Argv: []string{"gh", "pr", "view", "1"},
			Env:  map[string]string{"GITHUB_TOKEN": secretToken},
		}
	}

	record := func(t *testing.T, dir string) []string {
		t.Helper()
		sess := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
		_, err := sess.Record(context.Background(), adapter, newReq(), func() (xrr.Response, error) {
			return &execa.Response{Stdout: "ok\n"}, nil
		})
		require.NoError(t, err)
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		return names
	}

	// Two independent record runs of the same request.
	first := record(t, t.TempDir())
	second := record(t, t.TempDir())
	assert.Equal(t, first, second, "re-recording must produce identical cassette filenames")

	// And the fingerprint must match the un-redacted computation, proving
	// redaction is fingerprint-neutral.
	fpDirect, err := adapter.Fingerprint(newReq())
	require.NoError(t, err)
	for _, n := range first {
		assert.Contains(t, n, fpDirect, "cassette filename must carry the canonical fingerprint")
	}
}

// TestRecord_ByteIdenticalAcrossRuns — placeholders must be stable, not
// derived from a counter or the secret's hash, or committed cassettes
// would churn on every re-record.
func TestRecord_ByteIdenticalAcrossRuns(t *testing.T) {
	adapter := execa.NewAdapter()
	run := func() string {
		dir := t.TempDir()
		sess := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
		req := &execa.Request{
			Argv: []string{"gh", "auth", "status"},
			Env:  map[string]string{"GITHUB_TOKEN": secretToken, "AWS_SECRET_ACCESS_KEY": "abc123"},
		}
		_, err := sess.Record(context.Background(), adapter, req, func() (xrr.Response, error) {
			return &execa.Response{Stdout: "ok\n"}, nil
		})
		require.NoError(t, err)
		b, err := os.ReadFile(filepath.Join(dir, "exec-"+mustFP(t, adapter, req)+".req.yaml"))
		require.NoError(t, err)
		// recorded_at is a timestamp and legitimately varies; drop it.
		return stripRecordedAt(string(b))
	}
	assert.Equal(t, run(), run(), "redacted cassette bytes must be stable across runs")
}

func mustFP(t *testing.T, a *execa.Adapter, r *execa.Request) string {
	t.Helper()
	fp, err := a.Fingerprint(r)
	require.NoError(t, err)
	return fp
}

func stripRecordedAt(s string) string {
	out := []string{}
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(line, "recorded_at:") {
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

// TestRecord_ReplayStillWorksAfterRedaction — redaction must not break
// the record→replay round trip. The response payload replays intact;
// redacted request fields are not part of matching.
func TestRecord_ReplayRoundTrip(t *testing.T) {
	dir := t.TempDir()
	adapter := execa.NewAdapter()
	req := &execa.Request{
		Argv: []string{"gh", "pr", "view", "7"},
		Env:  map[string]string{"GITHUB_TOKEN": secretToken},
	}

	rec := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	_, err := rec.Record(context.Background(), adapter, req, func() (xrr.Response, error) {
		return &execa.Response{Stdout: "title: hello\n", ExitCode: 0}, nil
	})
	require.NoError(t, err)

	rep := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(dir))
	got, err := rep.Record(context.Background(), adapter, req, func() (xrr.Response, error) {
		t.Fatal("do() must not be called in replay mode")
		return nil, nil
	})
	require.NoError(t, err)
	raw, ok := got.(*xrr.RawResponse)
	require.True(t, ok)
	assert.Equal(t, "title: hello\n", raw.Payload["stdout"])
}

// TestRecord_DisabledViaEnvLeavesSecrets documents the escape hatch:
// when explicitly disabled the recorder is back to verbatim behaviour.
func TestRecord_DisabledViaEnv(t *testing.T) {
	t.Setenv(xrr.EnvRedactDisable, "1")
	dir := t.TempDir()
	sess := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	adapter := execa.NewAdapter()
	req := &execa.Request{Argv: []string{"gh"}, Env: map[string]string{"GITHUB_TOKEN": secretToken}}
	_, err := sess.Record(context.Background(), adapter, req, func() (xrr.Response, error) {
		return &execa.Response{Stdout: "ok\n"}, nil
	})
	require.NoError(t, err)
	assert.Contains(t, readAll(t, dir), secretToken, "disable flag must restore verbatim recording")
}

// TestRecord_AllowListViaEnv — a value the adopter must preserve.
func TestRecord_AllowListViaEnv(t *testing.T) {
	t.Setenv(xrr.EnvRedactAllow, "GITHUB_TOKEN")
	dir := t.TempDir()
	sess := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	adapter := execa.NewAdapter()
	req := &execa.Request{
		Argv: []string{"gh"},
		Env:  map[string]string{"GITHUB_TOKEN": secretToken, "AWS_SECRET_ACCESS_KEY": "leakme"},
	}
	_, err := sess.Record(context.Background(), adapter, req, func() (xrr.Response, error) {
		return &execa.Response{Stdout: "ok\n"}, nil
	})
	require.NoError(t, err)
	onDisk := readAll(t, dir)
	assert.Contains(t, onDisk, secretToken, "allow-listed key must be preserved")
	assert.NotContains(t, onDisk, "leakme", "non-allow-listed secret must still be redacted")
}

// TestRecord_NestedAndNonStringValues — redaction walks nested maps and
// must not corrupt non-string scalars.
func TestRecord_NestedStructuresPreserved(t *testing.T) {
	dir := t.TempDir()
	c := xrr.NewFileCassette(dir)
	req := map[string]any{
		"argv": []any{"svc"},
		"config": map[string]any{
			"retries":    3,
			"verbose":    true,
			"api_key":    secretToken,
			"endpoint":   "https://example.com",
			"nested_map": map[string]any{"password": "hunter2"},
		},
	}
	require.NoError(t, c.Save("exec", "aabbccdd", req, map[string]any{"stdout": "ok"}, nil))

	onDisk := readAll(t, dir)
	assert.NotContains(t, onDisk, secretToken)
	assert.NotContains(t, onDisk, "hunter2")
	assert.Contains(t, onDisk, "<redacted:API_KEY>")
	assert.Contains(t, onDisk, "<redacted:PASSWORD>")
	// Non-string scalars keep their YAML type (not quoted into strings).
	assert.Contains(t, onDisk, "retries: 3")
	assert.Contains(t, onDisk, "verbose: true")
	assert.Contains(t, onDisk, "https://example.com")
}
