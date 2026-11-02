package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	xrr "hop.top/xrr"
)

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
}

// ---- scrub lists (flags) ----

// echoSecretAz plants an az that prints the given env var to stdout.
func echoAz(t *testing.T, varName string) string {
	realbin := t.TempDir()
	plant(t, realbin, "az", `echo "v=$`+varName+`"`)
	return realbin
}

func recordedResp(t *testing.T, cass string) string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(cass, "*.resp.yaml"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	b, err := os.ReadFile(files[0])
	require.NoError(t, err)
	return string(b)
}

func TestScrubFlagAlwaysScrubsNamedEnvValues(t *testing.T) {
	realbin := echoAz(t, "PUBLIC_NOTE")
	cass := filepath.Join(t.TempDir(), "c")
	suite := writeSuite(t, `az x`)
	r := xrrRun(t, realbin+":"+sysPath, []string{"PUBLIC_NOTE=plain-looking-value"},
		"run", "--mode", "record", "--cassettes", cass, "--shim", "az", "--scrub", "PUBLIC_NOTE", "--", suite)
	require.Equal(t, 0, r.code, r.stderr)
	got := recordedResp(t, cass)
	assert.NotContains(t, got, "plain-looking-value")
	assert.Contains(t, got, "<redacted:PUBLIC_NOTE>")
}

func TestScrubFlagBeatsMinLength(t *testing.T) {
	realbin := echoAz(t, "SHORT")
	cass := filepath.Join(t.TempDir(), "c")
	r := xrrRun(t, realbin+":"+sysPath, []string{"SHORT=abc"},
		"run", "--mode", "record", "--cassettes", cass, "--shim", "az", "--scrub", "SHORT", "--", writeSuite(t, `az x`))
	require.Equal(t, 0, r.code, r.stderr)
	assert.Contains(t, recordedResp(t, cass), "<redacted:SHORT>")
}

func TestNoScrubFlagKeepsHeuristicMatch(t *testing.T) {
	realbin := echoAz(t, "API_TOKEN")
	cass := filepath.Join(t.TempDir(), "c")
	r := xrrRun(t, realbin+":"+sysPath, []string{"API_TOKEN=fixture-token-value"},
		"run", "--mode", "record", "--cassettes", cass, "--shim", "az", "--no-scrub", "API_TOKEN", "--", writeSuite(t, `az x`))
	require.Equal(t, 0, r.code, r.stderr)
	assert.Contains(t, recordedResp(t, cass), "fixture-token-value")
}

func TestScrubGlobPatterns(t *testing.T) {
	realbin := echoAz(t, "MY_THING")
	cass := filepath.Join(t.TempDir(), "c")
	r := xrrRun(t, realbin+":"+sysPath, []string{"MY_THING=glob-matched-value"},
		"run", "--mode", "record", "--cassettes", cass, "--shim", "az", "--scrub", "MY_*", "--", writeSuite(t, `az x`))
	require.Equal(t, 0, r.code, r.stderr)
	assert.NotContains(t, recordedResp(t, cass), "glob-matched-value")

	// no-scrub glob beats the heuristic too
	realbin = echoAz(t, "SVC_TOKEN")
	cass = filepath.Join(t.TempDir(), "c2")
	r = xrrRun(t, realbin+":"+sysPath, []string{"SVC_TOKEN=svc-fixture-token"},
		"run", "--mode", "record", "--cassettes", cass, "--shim", "az", "--no-scrub", "SVC_*", "--", writeSuite(t, `az x`))
	require.Equal(t, 0, r.code, r.stderr)
	assert.Contains(t, recordedResp(t, cass), "svc-fixture-token")
}

func TestScrubBeatsNoScrubWhenBothMatchByGlob(t *testing.T) {
	// scrub.deny wins over scrub.allow when different patterns both match.
	realbin := echoAz(t, "MY_TOKEN")
	cass := filepath.Join(t.TempDir(), "c")
	r := xrrRun(t, realbin+":"+sysPath, []string{"MY_TOKEN=both-lists-value"},
		"run", "--mode", "record", "--cassettes", cass, "--shim", "az", "--scrub", "MY_*", "--no-scrub", "*_TOKEN", "--", writeSuite(t, `az x`))
	require.Equal(t, 0, r.code, r.stderr)
	assert.NotContains(t, recordedResp(t, cass), "both-lists-value")
}

func TestScrubListsUsageErrors(t *testing.T) {
	cass := t.TempDir()
	base := []string{"run", "--mode", "replay", "--cassettes", cass, "--shim", "az"}
	cases := map[string][]string{
		"same name in both lists": {"--scrub", "FOO", "--no-scrub", "FOO"},
		"same glob in both lists": {"--scrub", "MY_*", "--no-scrub", "MY_*"},
		"bad scrub glob":          {"--scrub", "[oops"},
		"bad no-scrub glob":       {"--no-scrub", "[oops"},
	}
	for name, extra := range cases {
		t.Run(name, func(t *testing.T) {
			r := xrrRun(t, sysPath, nil, append(append(append([]string{}, base...), extra...), "--", "true")...)
			assert.Equal(t, 2, r.code, r.stderr)
		})
	}
}

func TestEnvSecretsPrecedence(t *testing.T) {
	rd := xrr.NewRedactor(xrr.RedactConfig{})
	env := []string{"API_TOKEN=abcdefgh12345", "NOTE=hello-world-1", "TINY=zq", "MY_OK_TOKEN=keepkeepkeep"}
	es := collectEnvSecrets(rd, env, []string{"NOTE", "TINY"}, []string{"MY_*", "API_TOKEN", "NOTE"})
	assert.Equal(t, "<redacted:NOTE> <redacted:TINY> abcdefgh12345 keepkeepkeep",
		es.scrub("hello-world-1 zq abcdefgh12345 keepkeepkeep"),
		"deny beats allow beats heuristic; deny ignores min length")
}

// ---- config file + layering ----

func TestConfigProjectFileSuppliesRunOptions(t *testing.T) {
	realbin := t.TempDir()
	plant(t, realbin, "az", `echo cfg-ok`)
	proj := t.TempDir()
	writeFile(t, filepath.Join(proj, ".xrr.yaml"), "mode: record\ncassettes: cass\nshim: [az]\n")
	r := xrrRunSpec(t, runSpec{path: realbin + ":" + sysPath, dir: proj}, "run", "--", writeSuite(t, `az x`))
	require.Equal(t, 0, r.code, r.stderr)
	assert.FileExists(t, filepath.Join(proj, "cass", "exec-"+fpOf(t, "az", "x")+".req.yaml"),
		"relative cassettes resolves against the config file's dir")
	assert.Contains(t, r.stderr, "recorded")
}

func fpOf(t *testing.T, argv ...string) string {
	t.Helper()
	return fingerprintOf(t, argv)
}

func TestConfigPrecedenceFlagsEnvProjectUser(t *testing.T) {
	proj := t.TempDir()
	xdg := t.TempDir()
	writeFile(t, filepath.Join(xdg, "xrr", "config.yaml"), "mode: record\nstrict: false\nreport_file: user.json\n")
	writeFile(t, filepath.Join(proj, ".xrr.yaml"), "mode: replay\nshim: [az]\ncassettes: cass\n")
	require.NoError(t, os.Mkdir(filepath.Join(proj, "cass"), 0o755))
	run := func(env []string, args ...string) result {
		return xrrRunSpec(t, runSpec{path: sysPath, dir: proj, env: append([]string{"XDG_CONFIG_HOME=" + xdg}, env...)},
			append([]string{"run"}, append(args, "--", "true")...)...)
	}
	// project beats user: replay, not record
	assert.Contains(t, run(nil).stderr, "xrr run: replay")
	// env beats project
	assert.Contains(t, run([]string{"XRR_MODE=passthrough"}).stderr, "xrr run: passthrough")
	// flag beats env
	assert.Contains(t, run([]string{"XRR_MODE=passthrough"}, "--mode", "replay").stderr, "xrr run: replay")
	// user-file-only key still applies (report_file relative to user file dir)
	run(nil)
	assert.FileExists(t, filepath.Join(xdg, "xrr", "user.json"))
}

func TestConfigListsUnionAcrossLayers(t *testing.T) {
	proj := t.TempDir()
	xdg := t.TempDir()
	writeFile(t, filepath.Join(xdg, "xrr", "config.yaml"), "deny: [from-user]\n")
	writeFile(t, filepath.Join(proj, ".xrr.yaml"), "mode: passthrough\ndeny: [from-project]\n")
	suite := writeSuite(t, `from-user || true; from-project || true; from-env || true; from-flag || true; open x || true; true`)
	r := xrrRunSpec(t, runSpec{path: sysPath, dir: proj, env: []string{"XDG_CONFIG_HOME=" + xdg, "XRR_DENY=from-env"}},
		"run", "--deny", "from-flag", "--", suite)
	assert.Equal(t, 3, r.code, r.stderr)
	for _, c := range []string{"from-user", "from-project", "from-env", "from-flag", "open"} {
		assert.Contains(t, r.stderr, "denied: "+c)
	}
}

func TestConfigScalarOverrideStrict(t *testing.T) {
	proj := t.TempDir()
	writeFile(t, filepath.Join(proj, ".xrr.yaml"), "mode: replay\nshim: [az]\ncassettes: cass\nstrict: false\n")
	require.NoError(t, os.Mkdir(filepath.Join(proj, "cass"), 0o755))
	realbin := t.TempDir()
	plant(t, realbin, "az", `echo real`)
	suite := writeSuite(t, `az nope || true`)
	p := runSpec{path: realbin + ":" + sysPath, dir: proj}
	r := xrrRunSpec(t, p, "run", "--", suite)
	assert.Equal(t, 0, r.code, r.stderr) // file: strict false -> falls through
	r = xrrRunSpec(t, runSpec{path: p.path, dir: proj, env: []string{"XRR_STRICT=true"}}, "run", "--", suite)
	assert.Equal(t, 3, r.code, "env beats file: strict again")
	r = xrrRunSpec(t, runSpec{path: p.path, dir: proj, env: []string{"XRR_STRICT=true"}}, "run", "--strict=false", "--", suite)
	assert.Equal(t, 0, r.code, "flag beats env")
}

func TestConfigScrubKeysAndEnv(t *testing.T) {
	realbin := echoAz(t, "PUBLIC_NOTE")
	proj := t.TempDir()
	writeFile(t, filepath.Join(proj, ".xrr.yaml"), "mode: record\ncassettes: cass\nshim: [az]\nscrub:\n  deny: [PUBLIC_NOTE]\n")
	r := xrrRunSpec(t, runSpec{path: realbin + ":" + sysPath, dir: proj, env: []string{"PUBLIC_NOTE=cfg-scrubbed-value"}},
		"run", "--", writeSuite(t, `az x`))
	require.Equal(t, 0, r.code, r.stderr)
	assert.NotContains(t, recordedResp(t, filepath.Join(proj, "cass")), "cfg-scrubbed-value")

	// scrub.allow from env wins over heuristic
	realbin = echoAz(t, "API_TOKEN")
	cass := filepath.Join(t.TempDir(), "c")
	r = xrrRunSpec(t, runSpec{path: realbin + ":" + sysPath, env: []string{"API_TOKEN=fixture-token-value", "XRR_NO_SCRUB=API_TOKEN"}},
		"run", "--mode", "record", "--cassettes", cass, "--shim", "az", "--", writeSuite(t, `az x`))
	require.Equal(t, 0, r.code, r.stderr)
	assert.Contains(t, recordedResp(t, cass), "fixture-token-value")
}

func TestConfigScrubConflictAcrossLayersIsUsageError(t *testing.T) {
	proj := t.TempDir()
	writeFile(t, filepath.Join(proj, ".xrr.yaml"), "scrub:\n  allow: [FOO]\n")
	r := xrrRunSpec(t, runSpec{path: sysPath, dir: proj}, "run", "--mode", "passthrough", "--scrub", "FOO", "--", "true")
	assert.Equal(t, 2, r.code, r.stderr)
}

func TestConfigExplicitPathReplacesProjectFile(t *testing.T) {
	proj := t.TempDir()
	writeFile(t, filepath.Join(proj, ".xrr.yaml"), "deny: [from-project-file]\n")
	other := filepath.Join(t.TempDir(), "other.yaml")
	writeFile(t, other, "deny: [from-other-file]\n")
	suite := writeSuite(t, `from-project-file || true; from-other-file || true; true`)
	r := xrrRunSpec(t, runSpec{path: sysPath, dir: proj}, "run", "--mode", "passthrough", "--config", other, "--", suite)
	assert.Contains(t, r.stderr, "denied: from-other-file")
	assert.NotContains(t, r.stderr, "denied: from-project-file")
}

func TestConfigErrors(t *testing.T) {
	cases := map[string]struct {
		file string
		args []string
		env  []string
	}{
		"unknown key":       {file: "mode: passthrough\nbogus: 1\n"},
		"unknown scrub key": {file: "scrub:\n  nope: [A]\n"},
		"bad yaml":          {file: "mode: [unclosed\n"},
		"wrong type":        {file: "shim: az\n"},
		"bad mode":          {file: "mode: sideways\n"},
		"bad duration":      {file: "stdin_wait: soon\n"},
		"missing explicit":  {args: []string{"--config", "/nonexistent/xrr.yaml"}},
		"bad env bool":      {file: "mode: passthrough\n", env: []string{"XRR_STRICT=maybe"}},
		"bad env duration":  {file: "mode: passthrough\n", env: []string{"XRR_STDIN_WAIT=soon"}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			proj := t.TempDir()
			if c.file != "" {
				writeFile(t, filepath.Join(proj, ".xrr.yaml"), c.file)
			}
			args := append([]string{"run"}, c.args...)
			args = append(args, "--", "true")
			r := xrrRunSpec(t, runSpec{path: sysPath, dir: proj, env: c.env}, args...)
			assert.Equal(t, 2, r.code, r.stderr)
			if c.file != "" && name != "bad env bool" && name != "bad env duration" {
				assert.Contains(t, r.stderr, ".xrr.yaml")
			}
		})
	}
}

func TestConfigEmptyFileIsFine(t *testing.T) {
	proj := t.TempDir()
	writeFile(t, filepath.Join(proj, ".xrr.yaml"), "")
	r := xrrRunSpec(t, runSpec{path: sysPath, dir: proj}, "run", "--mode", "passthrough", "--", "true")
	assert.Equal(t, 0, r.code, r.stderr)
}

func TestConfigUserFileDefaultsToHomeDotConfig(t *testing.T) {
	// Every test runs with a fresh HOME and no XDG_CONFIG_HOME, so the real
	// user config is unreachable. Here the fake HOME gets a (bad) config to
	// prove the default location is $HOME/.config/xrr/config.yaml.
	home := t.TempDir()
	writeFile(t, filepath.Join(home, ".config", "xrr", "config.yaml"), "bogus: 1\n")
	r := xrrRunSpec(t, runSpec{path: sysPath, env: []string{"HOME=" + home}}, "run", "--mode", "passthrough", "--", "true")
	assert.Equal(t, 2, r.code, "default user path is $HOME/.config/xrr/config.yaml")
}

func TestMergeSettings(t *testing.T) {
	a, b := "x", "y"
	t1, f := true, false
	lo := settings{Mode: &a, Strict: &t1, Deny: []string{"a", "b"}}
	hi := settings{Mode: &b, Deny: []string{"b", "c"}, NoDefaultDeny: &f}
	m := mergeSettings(lo, hi)
	assert.Equal(t, "y", *m.Mode)
	assert.True(t, *m.Strict)
	assert.False(t, *m.NoDefaultDeny)
	assert.Equal(t, []string{"a", "b", "c"}, m.Deny)
}
