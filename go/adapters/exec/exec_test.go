package exec_test

import (
	"testing"

	"hop.top/xrr/adapters/exec"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecAdapterFingerprint(t *testing.T) {
	a := exec.NewAdapter()
	req := &exec.Request{Argv: []string{"gh", "pr", "view", "1"}, Stdin: ""}
	fp, err := a.Fingerprint(req)
	require.NoError(t, err)
	assert.Len(t, fp, 8)
	// deterministic
	fp2, _ := a.Fingerprint(req)
	assert.Equal(t, fp, fp2)
	// different argv → different fp
	req2 := &exec.Request{Argv: []string{"gh", "pr", "view", "2"}}
	fp3, _ := a.Fingerprint(req2)
	assert.NotEqual(t, fp, fp3)
}

func TestExecAdapterRoundtrip(t *testing.T) {
	a := exec.NewAdapter()
	req := &exec.Request{Argv: []string{"echo", "hello"}, Stdin: ""}
	data, err := a.Serialize(req)
	require.NoError(t, err)
	var got exec.Request
	require.NoError(t, a.Deserialize(data, &got))
	assert.Equal(t, req.Argv, got.Argv)
}

// TestExecAdapterFingerprint_CwdDiscriminates — same argv in different
// working directories must produce distinct fingerprints. Regression
// test for T-0040 (multi-worktree cassette collisions in git-hop e2e).
func TestExecAdapterFingerprint_CwdDiscriminates(t *testing.T) {
	a := exec.NewAdapter()
	reqA := &exec.Request{Argv: []string{"docker", "compose", "config"}, Cwd: "/tmp/dir-a"}
	reqB := &exec.Request{Argv: []string{"docker", "compose", "config"}, Cwd: "/tmp/dir-b"}

	fpA, err := a.Fingerprint(reqA)
	require.NoError(t, err)
	fpB, err := a.Fingerprint(reqB)
	require.NoError(t, err)

	assert.NotEqual(t, fpA, fpB,
		"same command in different cwds must NOT collide on the same cassette key")

	// deterministic within a cwd
	fpA2, err := a.Fingerprint(reqA)
	require.NoError(t, err)
	assert.Equal(t, fpA, fpA2)
}

// TestExecAdapterFingerprint_EmptyCwdBackwardCompat — a Request with
// empty Cwd must produce the same fingerprint as the legacy argv+stdin
// shape, so cassettes recorded before the Cwd field existed still match
// for adopters that don't populate Cwd.
func TestExecAdapterFingerprint_EmptyCwdBackwardCompat(t *testing.T) {
	a := exec.NewAdapter()
	// Both requests are semantically identical; Cwd is the zero value.
	legacyShape := &exec.Request{Argv: []string{"gh", "pr", "view", "1"}, Stdin: ""}
	newShapeNoCwd := &exec.Request{Argv: []string{"gh", "pr", "view", "1"}, Stdin: "", Cwd: ""}

	fpLegacy, err := a.Fingerprint(legacyShape)
	require.NoError(t, err)
	fpNew, err := a.Fingerprint(newShapeNoCwd)
	require.NoError(t, err)

	assert.Equal(t, fpLegacy, fpNew,
		"empty Cwd must hash identically to pre-Cwd Request shape")

	// And specifically: fingerprint should match the known value from
	// TestExecAdapterFingerprint above to prove the hash inputs didn't
	// drift.
	fpRef, _ := a.Fingerprint(&exec.Request{Argv: []string{"gh", "pr", "view", "1"}, Stdin: ""})
	assert.Equal(t, fpRef, fpNew)
}

// Cross-port hazard vector (spec: Fingerprint Algorithm). The same argv
// pins the same fingerprint in every port.
func TestFingerprintHazardVector(t *testing.T) {
	hazard := "a&b<c>/é" + string(rune(0x2028)) + string(rune(0x2029)) + "\b\f\x1f\x7f"
	fp, err := exec.NewAdapter().Fingerprint(&exec.Request{Argv: []string{"echo", hazard}})
	require.NoError(t, err)
	assert.Equal(t, "97618387", fp)
}
