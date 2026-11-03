package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Shims are copies (or hardlinks) of the xrr binary itself, placed in
// <run-dir>/bin under the command's name (<cmd>, or <cmd>.exe on Windows).
// When started from there the binary acts as that command's shim: the name
// it was started under is the command, and the run dir is two levels up.
// One mechanism on every OS; no sh, no .cmd stubs.

const (
	runDirPrefix = "xrr-run-" // os.MkdirTemp pattern of a run dir
	runDirEnv    = "XRR_RUN_DIR"
)

func isWindows(goos string) bool { return goos == "windows" }

// exeExts are stripped from command names (one, at most) to form the
// cassette name.
var exeExts = []string{".exe", ".com", ".bat", ".cmd"}

// cassetteName is the identity of a configured command: the name that is
// stored as argv[0] in cassettes, fingerprinted, matched against the deny
// list and shown in reports. One trailing executable extension is dropped
// and the rest lowercased, on every OS, so "AZ", "az" and "az.exe" are the
// same command and cassettes replay across OSes. A name that is nothing but
// an extension, or has a non-executable suffix ("kubectl.v2"), keeps it.
func cassetteName(name string) string {
	n := strings.ToLower(name)
	for _, e := range exeExts {
		if strings.HasSuffix(n, e) && len(n) > len(e) {
			return strings.TrimSuffix(n, e)
		}
	}
	return n
}

// shimFileName is the file name of a configured command's shim in the bin
// dir: the name as given, plus ".exe" on Windows unless already there.
func shimFileName(name, goos string) string {
	if isWindows(goos) && !strings.HasSuffix(strings.ToLower(name), ".exe") {
		return name + ".exe"
	}
	return name
}

// inNames reports whether name is one of list, compared by cassette name.
func inNames(list []string, name string) bool {
	c := cassetteName(name)
	for _, x := range list {
		if cassetteName(x) == c {
			return true
		}
	}
	return false
}

// checkCollisions rejects configured commands that reduce to the same
// cassette name: two shims ("foo", "foo.exe"), or a shim that is also
// denied. The same entry repeated within one list is harmless.
func checkCollisions(shim, deny []string) error {
	type seen struct{ entry, list string }
	byName := map[string]seen{}
	for _, l := range []struct {
		name    string
		entries []string
	}{{"shim", shim}, {"deny", deny}} {
		for _, e := range l.entries {
			c := cassetteName(e)
			if prev, ok := byName[c]; ok && !(prev.entry == e && prev.list == l.name) {
				return fmt.Errorf("%s %q and %s %q are the same command %q (cassette names ignore case and one .exe/.com/.bat/.cmd suffix)",
					prev.list, prev.entry, l.name, e, c)
			}
			byName[c] = seen{e, l.name}
		}
	}
	return nil
}

// shimIdentity reports (with the shim's file name) whether the executable at exe is a shim: it lives in
// <run-dir>/bin of a run dir made by 'xrr run'. Location, not name, decides,
// so the real CLI works under any file name and a shimmed command may even
// be called "xrr". It needs no environment, so shims survive `env -i`.
func shimIdentity(exe string) (runDir, file string, ok bool) {
	dir := filepath.Dir(exe)
	if filepath.Base(dir) != binSubdir {
		return "", "", false
	}
	runDir = filepath.Dir(dir)
	if !strings.HasPrefix(filepath.Base(runDir), runDirPrefix) {
		return "", "", false
	}
	if fi, err := os.Stat(filepath.Join(runDir, configFile)); err != nil || fi.IsDir() {
		return "", "", false
	}
	return runDir, filepath.Base(exe), true
}

// installShims creates one shim per name in binDir. The first is a hardlink
// of self (copy if linking fails: cross-volume, no link support); later
// ones link from the first, so at most one full copy is made.
func installShims(binDir, self string, names []string, goos string) error {
	src := self
	if r, err := filepath.EvalSymlinks(self); err == nil {
		src = r // link(2) on a symlink would link the symlink itself
	}
	seen := map[string]bool{}
	first := true
	for _, n := range names {
		f := shimFileName(n, goos)
		key := f
		if isWindows(goos) {
			key = strings.ToLower(f)
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		dst := filepath.Join(binDir, f)
		if err := linkOrCopy(src, dst); err != nil {
			return err
		}
		if first {
			src, first = dst, false
		}
	}
	return nil
}

// linkOrCopy hardlinks src to dst, falling back to a copy. It never
// overwrites. A hardlink (not a symlink) is used because os.Executable
// resolves symlinks on Linux, which would lose the shim's name.
func linkOrCopy(src, dst string) error {
	err := os.Link(src, dst)
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrExist) {
		return err
	}
	return copyExec(src, dst)
}

func copyExec(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o755)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := out.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			_ = os.Remove(dst)
		}
	}()
	_, err = io.Copy(out, in)
	return err
}

const defaultPathExt = ".COM;.EXE;.BAT;.CMD"

// lookPathIn finds name as an executable file in the given PATH string.
// On Windows a file is executable by extension: PATHEXT (default
// .COM;.EXE;.BAT;.CMD) is tried per directory, in order, after name as
// given when it already has an extension.
func lookPathIn(name, pathVar, goos, pathext string) (string, bool) {
	exts := []string{""}
	if isWindows(goos) {
		if pathext == "" {
			pathext = defaultPathExt
		}
		if filepath.Ext(name) == "" {
			exts = nil
		}
		for _, e := range strings.Split(pathext, ";") {
			if e = strings.TrimSpace(e); e != "" {
				exts = append(exts, e)
			}
		}
	}
	for _, d := range filepath.SplitList(pathVar) {
		if d == "" {
			d = "."
		}
		for _, e := range exts {
			p := filepath.Join(d, name+e)
			if fi, err := os.Stat(p); err == nil && !fi.IsDir() && (isWindows(goos) || fi.Mode()&0o111 != 0) {
				return p, true
			}
		}
	}
	return "", false
}

// withEnv returns env with key set to val (replacing any existing entry).
func withEnv(env []string, key, val string) []string { return withEnvOS(env, key, val, runtime.GOOS) }

// withEnvOS is withEnv with the OS explicit: Windows names are
// case-insensitive (the variable is usually spelled "Path").
func withEnvOS(env []string, key, val, goos string) []string {
	out := make([]string, 0, len(env)+1)
	for _, e := range env {
		k, _, _ := strings.Cut(e, "=")
		if k == key || (isWindows(goos) && strings.EqualFold(k, key)) {
			continue
		}
		out = append(out, e)
	}
	return append(out, key+"="+val)
}
