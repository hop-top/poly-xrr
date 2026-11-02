package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	xrr "hop.top/xrr"
	xexec "hop.top/xrr/adapters/exec"
)

// xrrBin is the CLI built once for the whole package; the shim scripts
// re-exec it, so tests must run the real binary, not call main in-process.
var xrrBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "xrr-cli-test")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	xrrBin = filepath.Join(dir, "xrr")
	out, err := exec.Command("go", "build", "-buildvcs=false", "-o", xrrBin, ".").CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n%s", err, out)
		os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// sysPath is a PATH with only the standard tool dirs: enough for bash and
// coreutils, none of the fake "real" binaries a test plants.
const sysPath = "/usr/bin:/bin"

type result struct {
	stdout, stderr string
	code           int
}

// xrrRun runs the CLI with PATH=path and extra env (KEY=VAL).
func xrrRun(t *testing.T, path string, extraEnv []string, args ...string) result {
	t.Helper()
	return xrrRunIn(t, nil, path, extraEnv, args...)
}

// xrrRunIn is xrrRun with an explicit stdin (nil = /dev/null).
func xrrRunIn(t *testing.T, stdin io.Reader, path string, extraEnv []string, args ...string) result {
	t.Helper()
	cmd := exec.Command(xrrBin, args...)
	cmd.Stdin = stdin
	cmd.Env = append([]string{"PATH=" + path, "HOME=" + t.TempDir()}, extraEnv...)
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	err := cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("run xrr: %v", err)
	}
	return result{so.String(), se.String(), code}
}

// plant writes an executable script into dir.
func plant(t *testing.T, dir, name, body string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body), 0o755))
}

func writeSuite(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "suite.sh")
	require.NoError(t, os.WriteFile(p, []byte("#!/bin/bash\n"+body), 0o755))
	return p
}

func TestRecordThenReplayBashSuiteWithoutRealBinaries(t *testing.T) {
	realbin := t.TempDir()
	marker := filepath.Join(t.TempDir(), "real-ran")
	plant(t, realbin, "az", `echo "az:$*" ; echo "az-err" >&2; touch `+marker+`; [ "$1" = fail ] && exit 3; exit 0`)
	plant(t, realbin, "op", `echo "op:$(cat)"`)
	cass := filepath.Join(t.TempDir(), "cassettes")
	suite := writeSuite(t, `
set -u
a=$(az account show 2>/dev/null); echo "1=$a"
az fail >/dev/null 2>&1; echo "rc=$?"
o=$(echo payload | op inject); echo "3=$o"
`)

	rec := xrrRun(t, realbin+":"+sysPath, nil, "run", "--mode", "record", "--cassettes", cass, "--shim", "az,op", "--", suite)
	require.Equal(t, 0, rec.code, rec.stderr)
	assert.Equal(t, "1=az:account show\nrc=3\n3=op:payload\n", rec.stdout)
	assert.FileExists(t, marker, "record must exec the real binary")
	assert.Contains(t, rec.stderr, "recorded")

	// Cassettes are the existing exec format: loadable via the stock
	// adapter with a plain {argv, stdin} fingerprint.
	ad := xexec.NewAdapter()
	fp, err := ad.Fingerprint(&xexec.Request{Argv: []string{"az", "account", "show"}})
	require.NoError(t, err)
	var reqP, respP xexec.Response
	var req map[string]any
	_, err = xrr.NewFileCassette(cass).Load("exec", fp, &req, &respP)
	require.NoError(t, err)
	assert.Equal(t, "az:account show\n", respP.Stdout)
	assert.Equal(t, "az-err\n", respP.Stderr)
	_ = reqP
	fpFail, _ := ad.Fingerprint(&xexec.Request{Argv: []string{"az", "fail"}})
	recErr, err := xrr.NewFileCassette(cass).Load("exec", fpFail, &req, &respP)
	require.NoError(t, err)
	assert.Equal(t, "exit status 3", recErr)
	assert.Equal(t, 3, respP.ExitCode)

	// Replay with no real binaries on PATH at all.
	require.NoError(t, os.Remove(marker))
	rep := xrrRun(t, sysPath, nil, "run", "--mode", "replay", "--cassettes", cass, "--shim", "az,op", "--", suite)
	require.Equal(t, 0, rep.code, rep.stderr)
	assert.Equal(t, rec.stdout, rep.stdout)
	assert.NoFileExists(t, marker)
	assert.Contains(t, rep.stderr, "replayed")
}

func TestDenyHitFailsRunAndNeverExecsReal(t *testing.T) {
	realbin := t.TempDir()
	marker := filepath.Join(t.TempDir(), "opened")
	plant(t, realbin, "open", `touch `+marker)
	cass := t.TempDir()
	// Suite swallows the failure; the run must still fail.
	suite := writeSuite(t, `open https://example.test/unauthorized || true; echo done`)

	for _, mode := range []string{"replay", "passthrough"} {
		r := xrrRun(t, realbin+":"+sysPath, nil, "run", "--mode", mode, "--cassettes", cass, "--shim", "az", "--", suite)
		assert.NotEqual(t, 0, r.code, mode)
		assert.Equal(t, "done\n", r.stdout, mode)
		assert.Contains(t, r.stderr, "denied", mode)
		assert.Contains(t, r.stderr, "open", mode)
		assert.NoFileExists(t, marker, mode)
	}
}

func TestUserDenyAddsToDefaults(t *testing.T) {
	cass := t.TempDir()
	suite := writeSuite(t, `rm-rf-prod || true; echo after`)
	r := xrrRun(t, sysPath, nil, "run", "--mode", "replay", "--cassettes", cass, "--shim", "az", "--deny", "rm-rf-prod", "--", suite)
	assert.NotEqual(t, 0, r.code)
	assert.Contains(t, r.stderr, "denied: rm-rf-prod")
	assert.Equal(t, "after\n", r.stdout)
}

func TestExplicitShimOverridesDefaultDeny(t *testing.T) {
	realbin := t.TempDir()
	plant(t, realbin, "pbcopy", `cat >/dev/null; echo copied`)
	cass := filepath.Join(t.TempDir(), "c")
	suite := writeSuite(t, `echo hi | pbcopy`)
	rec := xrrRun(t, realbin+":"+sysPath, nil, "run", "--mode", "record", "--cassettes", cass, "--shim", "pbcopy", "--", suite)
	assert.Equal(t, 0, rec.code, rec.stderr)
	assert.Equal(t, "copied\n", rec.stdout)
}

func TestNoDefaultDeny(t *testing.T) {
	realbin := t.TempDir()
	plant(t, realbin, "open", `echo opened`)
	suite := writeSuite(t, `open x`)
	r := xrrRun(t, realbin+":"+sysPath, nil, "run", "--mode", "passthrough", "--no-default-deny", "--", suite)
	assert.Equal(t, 0, r.code, r.stderr)
	assert.Equal(t, "opened\n", r.stdout)
}

func TestStrictMissFails(t *testing.T) {
	realbin := t.TempDir()
	marker := filepath.Join(t.TempDir(), "real-ran")
	plant(t, realbin, "az", `touch `+marker+`; echo real`)
	cass := t.TempDir()
	suite := writeSuite(t, `az nope; echo "rc=$?"`)

	r := xrrRun(t, realbin+":"+sysPath, nil, "run", "--mode", "replay", "--cassettes", cass, "--shim", "az", "--", suite)
	assert.NotEqual(t, 0, r.code)
	assert.Contains(t, r.stderr, "cassette miss")
	assert.Contains(t, r.stderr, "az nope")
	assert.NotContains(t, r.stdout, "rc=0")
	assert.NoFileExists(t, marker)
}

func TestStrictMissIsDefaultInReplay(t *testing.T) {
	cass := t.TempDir()
	suite := writeSuite(t, `az nope`)
	r := xrrRun(t, sysPath, nil, "run", "--cassettes", cass, "--shim", "az", "--", suite)
	assert.NotEqual(t, 0, r.code)
	assert.Contains(t, r.stderr, "cassette miss")
}

func TestNonStrictMissFallsThroughToReal(t *testing.T) {
	realbin := t.TempDir()
	plant(t, realbin, "az", `echo real:$*`)
	cass := t.TempDir()
	suite := writeSuite(t, `az nope`)
	r := xrrRun(t, realbin+":"+sysPath, nil, "run", "--mode", "replay", "--strict=false", "--cassettes", cass, "--shim", "az", "--", suite)
	assert.Equal(t, 0, r.code, r.stderr)
	assert.Equal(t, "real:nope\n", r.stdout)
}

func TestRedactionOfArgvEnvAndOutput(t *testing.T) {
	realbin := t.TempDir()
	plant(t, realbin, "az", `echo "logged in"; echo "tok=$API_TOKEN" >&2`)
	cass := filepath.Join(t.TempDir(), "c")
	ghp := "ghp_" + strings.Repeat("a1B2", 8)
	suite := writeSuite(t, `
az login --token `+ghp+` --password hunter2 --client-secret=s3cr3t-value user=bob
`)
	env := []string{"API_TOKEN=env-secret-value", "REGION=eu"}
	rec := xrrRun(t, realbin+":"+sysPath, env, "run", "--mode", "record", "--cassettes", cass,
		"--shim", "az", "--env", "API_TOKEN,REGION", "--", suite)
	require.Equal(t, 0, rec.code, rec.stderr)

	files, err := filepath.Glob(filepath.Join(cass, "*"))
	require.NoError(t, err)
	require.Len(t, files, 2)
	for _, f := range files {
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		for _, secret := range []string{ghp, "hunter2", "s3cr3t-value", "env-secret-value"} {
			assert.NotContains(t, string(b), secret, f)
		}
	}
	for _, s := range []string{rec.stderr} {
		assert.NotContains(t, s, "hunter2", "summary must not leak argv secrets")
		assert.NotContains(t, s, ghp)
	}
	var reqB []byte
	for _, f := range files {
		if strings.HasSuffix(f, ".req.yaml") {
			reqB, _ = os.ReadFile(f)
		}
	}
	assert.Contains(t, string(reqB), "<redacted:PASSWORD>")
	assert.Contains(t, string(reqB), "<redacted:CLIENT-SECRET>")
	assert.Contains(t, string(reqB), "user=bob")
	assert.Contains(t, string(reqB), "REGION: eu")

	// Fingerprint is over live argv, so replay with the same secrets hits.
	rep := xrrRun(t, sysPath, nil, "run", "--mode", "replay", "--cassettes", cass, "--shim", "az", "--", suite)
	assert.Equal(t, 0, rep.code, rep.stderr)
}

func TestChildExitCodePropagates(t *testing.T) {
	cass := t.TempDir()
	r := xrrRun(t, sysPath, nil, "run", "--mode", "passthrough", "--cassettes", cass, "--", "bash", "-c", "exit 7")
	assert.Equal(t, 7, r.code)
}

func TestReportFileJSON(t *testing.T) {
	cass := t.TempDir()
	rf := filepath.Join(t.TempDir(), "report.json")
	suite := writeSuite(t, `open x; az y; true`)
	r := xrrRun(t, sysPath, nil, "run", "--mode", "replay", "--cassettes", cass, "--shim", "az", "--report-file", rf, "--", suite)
	assert.NotEqual(t, 0, r.code)
	b, err := os.ReadFile(rf)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"denied"`)
	assert.Contains(t, string(b), `"miss"`)
	assert.Contains(t, string(b), `"open"`)
}

func TestUsageErrors(t *testing.T) {
	cass := t.TempDir()
	cases := map[string][]string{
		"no command":           {"run", "--mode", "replay", "--cassettes", cass, "--shim", "az", "--"},
		"bad mode":             {"run", "--mode", "bogus", "--cassettes", cass, "--shim", "az", "--", "true"},
		"record needs shim":    {"run", "--mode", "record", "--cassettes", cass, "--", "true"},
		"replay dir missing":   {"run", "--mode", "replay", "--cassettes", filepath.Join(cass, "nope"), "--shim", "az", "--", "true"},
		"shim and deny clash":  {"run", "--mode", "replay", "--cassettes", cass, "--shim", "az", "--deny", "az", "--", "true"},
		"shim name with slash": {"run", "--mode", "replay", "--cassettes", cass, "--shim", "a/b", "--", "true"},
		"unknown subcommand":   {"frobnicate"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			r := xrrRun(t, sysPath, nil, args...)
			assert.Equal(t, 2, r.code, r.stderr)
		})
	}
}

func TestScrubArgv(t *testing.T) {
	rd := xrr.NewRedactor(xrr.RedactConfig{})
	cases := []struct{ in, want []string }{
		{[]string{"az", "login", "--password", "hunter2"}, []string{"az", "login", "--password", "<redacted:PASSWORD>"}},
		{[]string{"az", "--client-secret=abc"}, []string{"az", "--client-secret=<redacted:CLIENT-SECRET>"}},
		{[]string{"op", "item", "get", "name"}, []string{"op", "item", "get", "name"}},
		{[]string{"env", "TOKEN=abc", "x"}, []string{"env", "TOKEN=<redacted:TOKEN>", "x"}},
		{[]string{"cmd", "--token"}, []string{"cmd", "--token"}},
		{[]string{"cmd", "--token", "--next"}, []string{"cmd", "--token", "--next"}},
		{[]string{"cmd", "--region", "eu"}, []string{"cmd", "--region", "eu"}},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, scrubArgv(rd, c.in), "%v", c.in)
	}
	in := []string{"a", "--password", "x"}
	scrubArgv(rd, in)
	assert.Equal(t, []string{"a", "--password", "x"}, in, "input must not be mutated")
}

func TestDefaultDenySet(t *testing.T) {
	for _, c := range []string{"open", "xdg-open", "wslview", "explorer.exe", "start", "sendmail", "mail", "osascript", "say", "pbcopy", "notify-send"} {
		assert.Contains(t, defaultDeny, c)
	}
}

func TestListFlag(t *testing.T) {
	var l listFlag
	require.NoError(t, l.Set("a, b,,c"))
	require.NoError(t, l.Set("b,d"))
	assert.Equal(t, []string{"a", "b", "c", "d"}, []string(l))
}

func TestEnvSecretsScrubOutputButNotShortValues(t *testing.T) {
	rd := xrr.NewRedactor(xrr.RedactConfig{})
	es := collectEnvSecrets(rd, []string{"API_TOKEN=abcdefgh12345", "DB_PASSWORD=short", "REGION=eu-west-1-long"})
	assert.Equal(t, "x <redacted:API_TOKEN> y short eu-west-1-long", es.scrub("x abcdefgh12345 y short eu-west-1-long"))
}

// An inherited stdin that is open but silent (bats, CI runners) must not hang
// shims, in any mode, for commands that do not read it.
func TestIdleOpenStdinDoesNotHang(t *testing.T) {
	realbin := t.TempDir()
	plant(t, realbin, "az", `echo ok`)
	cass := filepath.Join(t.TempDir(), "c")
	suite := writeSuite(t, `az x`)
	pr, pw, err := os.Pipe()
	require.NoError(t, err)
	defer pw.Close()
	defer pr.Close()

	done := make(chan result, 2)
	go func() {
		done <- xrrRunIn(t, pr, realbin+":"+sysPath, nil, "run", "--mode", "record", "--cassettes", cass, "--shim", "az", "--", suite)
	}()
	select {
	case r := <-done:
		require.Equal(t, 0, r.code, r.stderr)
	case <-time.After(15 * time.Second):
		t.Fatal("record hung on idle stdin")
	}
	go func() {
		done <- xrrRunIn(t, pr, sysPath, nil, "run", "--mode", "replay", "--cassettes", cass, "--shim", "az", "--", suite)
	}()
	select {
	case r := <-done:
		require.Equal(t, 0, r.code, r.stderr)
		assert.Equal(t, "ok\n", r.stdout)
	case <-time.After(15 * time.Second):
		t.Fatal("replay hung on idle stdin")
	}
}

func TestStdinIsRecordedAndMatchesOnReplay(t *testing.T) {
	realbin := t.TempDir()
	plant(t, realbin, "op", `echo "got:$(cat)"`)
	cass := filepath.Join(t.TempDir(), "c")
	suite := writeSuite(t, `echo one | op inject; echo two | op inject; (sleep 0.6; echo late) | op inject`)

	rec := xrrRun(t, realbin+":"+sysPath, nil, "run", "--mode", "record", "--cassettes", cass, "--shim", "op", "--", suite)
	require.Equal(t, 0, rec.code, rec.stderr)
	assert.Equal(t, "got:one\ngot:two\ngot:late\n", rec.stdout, "slow producer must reach the real binary intact")

	rep := xrrRun(t, sysPath, nil, "run", "--mode", "replay", "--cassettes", cass, "--stdin-wait", "3s", "--shim", "op", "--", suite)
	require.Equal(t, 0, rep.code, rep.stderr)
	assert.Equal(t, rec.stdout, rep.stdout)
}
