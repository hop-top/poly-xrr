package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShimNameTable(t *testing.T) {
	cases := []struct{ in, win, posix, cassette string }{
		{"az", "az.exe", "az", "az"},
		{"AZ", "AZ.exe", "AZ", "az"},
		{"az.exe", "az.exe", "az.exe", "az"},
		{"az.CMD", "az.CMD.exe", "az.CMD", "az"},
		{"foo.bat", "foo.bat.exe", "foo.bat", "foo"},
		{"explorer.exe", "explorer.exe", "explorer.exe", "explorer"},
		{"Explorer.EXE", "Explorer.EXE", "Explorer.EXE", "explorer"},
		{"kubectl.v2", "kubectl.v2.exe", "kubectl.v2", "kubectl.v2"},
		{"foo.exe.exe", "foo.exe.exe", "foo.exe.exe", "foo.exe"}, // one extension only
		{".exe", ".exe", ".exe", ".exe"},                         // nothing left to strip to
	}
	for _, c := range cases {
		assert.Equal(t, c.win, shimFileName(c.in, "windows"), "windows file %q", c.in)
		assert.Equal(t, c.posix, shimFileName(c.in, "linux"), "posix file %q", c.in)
		assert.Equal(t, c.cassette, cassetteName(c.in), "cassette %q", c.in)
	}
}

func TestInNamesByCassetteName(t *testing.T) {
	list := []string{"open", "explorer.exe"}
	assert.True(t, inNames(list, "explorer"))
	assert.True(t, inNames(list, "Explorer.EXE"))
	assert.True(t, inNames(list, "OPEN"))
	assert.False(t, inNames(list, "opener"))
}

func TestCheckCollisions(t *testing.T) {
	assert.NoError(t, checkCollisions([]string{"az", "op"}, []string{"open"}))
	assert.NoError(t, checkCollisions([]string{"az", "az"}, nil), "same entry twice is not a collision")
	for _, c := range []struct{ shim, deny []string }{
		{[]string{"foo", "foo.exe"}, nil},
		{[]string{"AZ", "az"}, nil},
		{[]string{"az"}, []string{"az.CMD"}},
		{[]string{"az"}, []string{"az"}},
	} {
		err := checkCollisions(c.shim, c.deny)
		require.Error(t, err, "%v %v", c.shim, c.deny)
		all := append(append([]string{}, c.shim...), c.deny...)
		assert.Contains(t, err.Error(), `"`+all[0]+`"`)
		assert.Contains(t, err.Error(), `"`+all[1]+`"`)
	}
}

func TestShimCmdLookup(t *testing.T) {
	cfg := runConfig{Cmds: map[string]string{"AZ.exe": "AZ", "az.CMD": "az.CMD"}}
	assert.Equal(t, "AZ", cfg.cmdFor("AZ.exe", "linux"))
	assert.Equal(t, "AZ", cfg.cmdFor("az.exe", "windows"), "windows file names are case-insensitive")
	assert.Equal(t, "az.exe", cfg.cmdFor("az.exe", "linux"), "unknown file falls back to its own name")
}

func touch(t *testing.T, p string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, nil, 0o644)) // deliberately not executable
}

func TestLookPathInWindows(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	touch(t, filepath.Join(a, "readme.txt"))
	// File names carry the extension spelled as PATHEXT spells it: lookPathIn
	// probes name+ext and leaves case folding to the (Windows) filesystem,
	// so this test must not depend on the host filesystem folding case.
	touch(t, filepath.Join(b, "az.CMD"))
	touch(t, filepath.Join(b, "tool.EXE"))
	touch(t, filepath.Join(a, "tool.BAT"))
	sep := string(os.PathListSeparator)
	pathVar := a + sep + b
	ext := ".COM;.EXE;.BAT;.CMD"

	p, ok := lookPathIn("az", pathVar, "windows", ext)
	assert.True(t, ok)
	assert.Equal(t, filepath.Join(b, "az.CMD"), p)

	// dir order wins over extension order
	p, ok = lookPathIn("tool", pathVar, "windows", ext)
	assert.True(t, ok)
	assert.Equal(t, filepath.Join(a, "tool.BAT"), p)

	// name already carrying an extension
	p, ok = lookPathIn("az.CMD", pathVar, "windows", ext)
	assert.True(t, ok)
	assert.Equal(t, filepath.Join(b, "az.CMD"), p)

	// not in PATHEXT: not executable
	_, ok = lookPathIn("readme", pathVar, "windows", ext)
	assert.False(t, ok)
	_, ok = lookPathIn("readme.txt", pathVar, "windows", ext)
	assert.True(t, ok, "explicit name that exists is returned as-is")

	// empty PATHEXT falls back to the system default
	p, ok = lookPathIn("tool", b, "windows", "")
	assert.True(t, ok)
	assert.Equal(t, filepath.Join(b, "tool.EXE"), p)

	// custom PATHEXT limits the candidates
	_, ok = lookPathIn("az", b, "windows", ".EXE")
	assert.False(t, ok)
}

func TestLookPathInPOSIXNeedsExecBit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("exec bits are not modelled on Windows")
	}
	d := t.TempDir()
	touch(t, filepath.Join(d, "az")) // 0644
	_, ok := lookPathIn("az", d, "linux", "")
	assert.False(t, ok)
	require.NoError(t, os.Chmod(filepath.Join(d, "az"), 0o755))
	p, ok := lookPathIn("az", d, "linux", "")
	assert.True(t, ok)
	assert.Equal(t, filepath.Join(d, "az"), p)
	_, ok = lookPathIn("az", d, "linux", ".EXE")
	assert.True(t, ok, "PATHEXT is ignored off Windows")
}

func TestWithEnvOS(t *testing.T) {
	env := []string{"Path=C:\\old", "HOME=x", "PATHEXT=.EXE"}
	got := withEnvOS(env, "PATH", "new", "windows")
	assert.Equal(t, []string{"HOME=x", "PATHEXT=.EXE", "PATH=new"}, got, "case-insensitive replace; PATHEXT untouched")

	posix := withEnvOS([]string{"Path=a", "PATH=b"}, "PATH", "new", "linux")
	assert.Equal(t, []string{"Path=a", "PATH=new"}, posix, "POSIX keys are case-sensitive")
}

// mkRunDir builds <tmp>/xrr-run-x/{config.json,bin/} and returns both.
func mkRunDir(t *testing.T) (runDir, binDir string) {
	t.Helper()
	runDir = filepath.Join(t.TempDir(), runDirPrefix+"x")
	binDir = filepath.Join(runDir, binSubdir)
	require.NoError(t, os.MkdirAll(binDir, 0o755))
	require.NoError(t, writeConfig(runDir, runConfig{Mode: "replay"}))
	return
}

func TestShimIdentity(t *testing.T) {
	runDir, binDir := mkRunDir(t)

	rd, name, ok := shimIdentity(filepath.Join(binDir, "az"))
	assert.True(t, ok)
	assert.Equal(t, runDir, rd)
	assert.Equal(t, "az", name)

	_, name, ok = shimIdentity(filepath.Join(binDir, "AZ.EXE"))
	assert.True(t, ok)
	assert.Equal(t, "AZ.EXE", name, "the file name, as is")

	// the real CLI, wherever else it lives, is never a shim
	for _, p := range []string{
		filepath.Join(runDir, "xrr"),             // not in bin/
		filepath.Join(t.TempDir(), "bin", "xrr"), // bin/ but no run dir
		filepath.Join(t.TempDir(), "xrr"),
	} {
		_, _, ok = shimIdentity(p)
		assert.False(t, ok, p)
	}

	// right layout, wrong parent name
	other := filepath.Join(t.TempDir(), "elsewhere")
	require.NoError(t, os.MkdirAll(filepath.Join(other, binSubdir), 0o755))
	require.NoError(t, writeConfig(other, runConfig{}))
	_, _, ok = shimIdentity(filepath.Join(other, binSubdir, "az"))
	assert.False(t, ok)

	// right layout, config missing
	require.NoError(t, os.Remove(filepath.Join(runDir, configFile)))
	_, _, ok = shimIdentity(filepath.Join(binDir, "az"))
	assert.False(t, ok)
}

func writeBin(t *testing.T, p, body string) {
	t.Helper()
	require.NoError(t, os.WriteFile(p, []byte(body), 0o755))
}

func TestInstallShimsLinksOrCopies(t *testing.T) {
	src := filepath.Join(t.TempDir(), "xrr")
	writeBin(t, src, "BINARY-CONTENT")
	bin := t.TempDir()
	require.NoError(t, installShims(bin, src, []string{"az", "op", "explorer.exe"}, "windows"))
	for _, f := range []string{"az.exe", "op.exe", "explorer.exe"} {
		b, err := os.ReadFile(filepath.Join(bin, f))
		require.NoError(t, err, f)
		assert.Equal(t, "BINARY-CONTENT", string(b), f)
	}
	assert.Len(t, mustGlob(t, bin), 3)

	// POSIX naming, and a case-folding duplicate on Windows is created once
	bin2 := t.TempDir()
	require.NoError(t, installShims(bin2, src, []string{"az", "AZ"}, "windows"))
	assert.Len(t, mustGlob(t, bin2), 1)
	bin3 := t.TempDir()
	require.NoError(t, installShims(bin3, src, []string{"az"}, "linux"))
	assert.FileExists(t, filepath.Join(bin3, "az"))
}

func mustGlob(t *testing.T, d string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(d, "*"))
	require.NoError(t, err)
	return m
}

func TestLinkOrCopyHardlinksWhenPossible(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("inode identity is checked on POSIX; Windows covered by the end-to-end test")
	}
	d := t.TempDir()
	src := filepath.Join(d, "src")
	writeBin(t, src, "x")
	dst := filepath.Join(d, "dst")
	require.NoError(t, linkOrCopy(src, dst))
	a, _ := os.Stat(src)
	b, _ := os.Stat(dst)
	assert.True(t, os.SameFile(a, b), "expected a hardlink")
	assert.Error(t, linkOrCopy(src, dst), "existing destination is an error")
}

func TestCopyExecIsExecutableCopy(t *testing.T) {
	d := t.TempDir()
	src := filepath.Join(d, "src")
	writeBin(t, src, "payload")
	dst := filepath.Join(d, "dst")
	require.NoError(t, copyExec(src, dst))
	b, err := os.ReadFile(dst)
	require.NoError(t, err)
	assert.Equal(t, "payload", string(b))
	if runtime.GOOS != "windows" {
		fi, _ := os.Stat(dst)
		assert.NotZero(t, fi.Mode()&0o111)
	}
	a, _ := os.Stat(src)
	c, _ := os.Stat(dst)
	assert.False(t, os.SameFile(a, c), "copy, not a link")
	assert.Error(t, copyExec(src, dst), "never overwrites")
	assert.Error(t, copyExec(filepath.Join(d, "missing"), filepath.Join(d, "x")))
	assert.NoFileExists(t, filepath.Join(d, "x"))
}

func TestDefaultDenyHasWindowsEntries(t *testing.T) {
	for _, c := range []string{"explorer.exe", "rundll32.exe", "mshta.exe", "clip.exe", "msg.exe", "wscript.exe", "cscript.exe"} {
		assert.Contains(t, defaultDeny, c)
	}
}

// ---- end to end without sh: a Go-built fake CLI, on every OS ----

func exeExt() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

// fakeCLI returns a copy of the compiled testdata/fakecli named name in dir.
func fakeCLI(t *testing.T, dir, name string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	if exeExt() != "" && !strings.HasSuffix(strings.ToLower(name), ".exe") {
		name += exeExt()
	}
	dst := filepath.Join(dir, name)
	require.NoError(t, copyExec(fakeBin, dst))
	return dst
}

// nativeEnv is extra env for the Native* tests (SystemRoot is added by xrrRunSpec).
func nativeEnv() []string { return nil }

func TestNativeRecordReplayDenyWithoutSh(t *testing.T) {
	realbin := t.TempDir()
	fakeCLI(t, realbin, "az")
	suite := fakeCLI(t, t.TempDir(), "suite")
	cass := filepath.Join(t.TempDir(), "cassettes")

	rec := xrrRun(t, realbin, nativeEnv(), "run", "--mode", "record", "--cassettes", cass, "--shim", "az", "--", suite)
	require.Equal(t, 0, rec.code, rec.stderr)
	want := "az:account show\naccount rc=0\naz:fail\nfail rc=3\n"
	assert.Equal(t, want, normNL(rec.stdout))
	assert.Contains(t, rec.stderr, "recorded")

	// replay: no real az anywhere
	rep := xrrRun(t, "", nativeEnv(), "run", "--mode", "replay", "--cassettes", cass, "--shim", "az", "--", suite)
	require.Equal(t, 0, rep.code, rep.stderr)
	assert.Equal(t, want, normNL(rep.stdout))
	assert.Contains(t, rep.stderr, "replayed")

	// cassette keys use the bare command name on every OS
	fp := fingerprintOf(t, []string{"az", "account", "show"})
	assert.FileExists(t, filepath.Join(cass, "exec-"+fp+".req.yaml"))

	// deny: the real binary must not run, the run fails
	dn := xrrRun(t, realbin, nativeEnv(), "run", "--mode", "passthrough", "--cassettes", cass, "--shim", "az", "--", suite, "deny")
	assert.NotEqual(t, 0, dn.code)
	assert.Contains(t, dn.stderr, "denied")
	assert.Contains(t, dn.stdout, "x rc=126")
}

func TestNativeShimNeedsNoEnvironment(t *testing.T) {
	realbin := t.TempDir()
	fakeCLI(t, realbin, "az")
	suite := fakeCLI(t, t.TempDir(), "suite")
	cass := filepath.Join(t.TempDir(), "cassettes")
	rec := xrrRun(t, realbin, nativeEnv(), "run", "--mode", "record", "--cassettes", cass, "--shim", "az", "--", suite)
	require.Equal(t, 0, rec.code, rec.stderr)

	// The suite calls the shim with an empty environment (like `env -i`).
	rep := xrrRun(t, "", nativeEnv(), "run", "--mode", "replay", "--cassettes", cass, "--shim", "az", "--", suite, "noenv")
	require.Equal(t, 0, rep.code, rep.stderr)
	assert.Equal(t, "az:account show\naccount rc=0\n", normNL(rep.stdout))
}

func TestNativeRunDirEnvIsExported(t *testing.T) {
	cass := t.TempDir()
	suite := fakeCLI(t, t.TempDir(), "suite")
	r := xrrRun(t, "", nativeEnv(), "run", "--mode", "passthrough", "--cassettes", cass, "--", suite, "rundir")
	require.Equal(t, 0, r.code, r.stderr)
	assert.Regexp(t, `XRR_RUN_DIR=\S*`+runDirPrefix, r.stdout)
}

func normNL(s string) string { return strings.ReplaceAll(s, "\r\n", "\n") }

func TestInstallShimsFromSymlinkedSelf(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks need privileges on Windows")
	}
	d := t.TempDir()
	real := filepath.Join(d, "real")
	writeBin(t, real, "BIN")
	self := filepath.Join(d, "xrr")
	require.NoError(t, os.Symlink(real, self))
	bin := t.TempDir()
	require.NoError(t, installShims(bin, self, []string{"az"}, "linux"))
	fi, err := os.Lstat(filepath.Join(bin, "az"))
	require.NoError(t, err)
	assert.Zero(t, fi.Mode()&os.ModeSymlink, "shim must be a real file, not a link to a link")
}

// Cassette names are the same on every OS: a cassette recorded under one
// spelling of a shim replays under another, with no real binary. The suite
// calls the command by the spelling the shim was configured with.
func TestNativeCassetteNameIsSpellingIndependent(t *testing.T) {
	suite := fakeCLI(t, t.TempDir(), "suite")
	fp := fingerprintOf(t, []string{"az", "account", "show"})
	pairs := []struct{ rec, rep string }{
		{"az.exe", "az"}, {"az", "az.exe"}, {"az", "AZ.EXE"}, {"AZ", "az"}, {"az.exe", "AZ"},
	}
	for _, p := range pairs {
		realbin := t.TempDir()
		fakeCLI(t, realbin, p.rec)
		cass := filepath.Join(t.TempDir(), "cassettes")
		rec := xrrRun(t, realbin, nativeEnv(), "run", "--mode", "record", "--cassettes", cass, "--shim", p.rec, "--", suite, "bin="+p.rec)
		require.Equal(t, 0, rec.code, "%v: %s", p, rec.stderr)
		assert.FileExists(t, filepath.Join(cass, "exec-"+fp+".req.yaml"), "%v: argv[0] is the bare cassette name", p)
		rep := xrrRun(t, "", nativeEnv(), "run", "--mode", "replay", "--cassettes", cass, "--shim", p.rep, "--", suite, "bin="+p.rep)
		require.Equal(t, 0, rep.code, "%v: %s", p, rep.stderr)
		assert.Equal(t, normNL(rec.stdout), normNL(rep.stdout), "%v", p)
	}
}

func TestShimCollisionIsUsageError(t *testing.T) {
	cass := t.TempDir()
	for _, args := range [][]string{
		{"--shim", "foo,foo.exe"},
		{"--shim", "AZ,az"},
		{"--shim", "az", "--deny", "az.cmd"},
	} {
		r := xrrRun(t, "", nil, append(append([]string{"run", "--mode", "replay", "--cassettes", cass}, args...), "--", "x")...)
		assert.Equal(t, 2, r.code, "%v: %s", args, r.stderr)
		assert.Contains(t, r.stderr, "same command", "%v", args)
	}
}
