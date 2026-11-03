package main

import (
	"errors"
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

// exeExts are stripped from command names on Windows.
var exeExts = []string{".exe", ".com", ".bat", ".cmd"}

// canonName is the identity of a command name. On Windows names are
// case-insensitive and an executable extension is not part of the command
// ("az.exe" is "az"); elsewhere the name is literal. Cassettes carry the
// canonical name as argv[0], so they replay across OSes.
func canonName(name, goos string) string {
	if !isWindows(goos) {
		return name
	}
	n := strings.ToLower(name)
	for _, e := range exeExts {
		if strings.HasSuffix(n, e) && len(n) > len(e) {
			return strings.TrimSuffix(n, e)
		}
	}
	return n
}

// shimFileName is the file name of name's shim in the bin dir.
func shimFileName(name, goos string) string {
	if isWindows(goos) {
		return canonName(name, goos) + ".exe"
	}
	return name
}

func inNames(list []string, name, goos string) bool {
	c := canonName(name, goos)
	for _, x := range list {
		if canonName(x, goos) == c {
			return true
		}
	}
	return false
}

// shimNames merges the shim and deny lists, one entry per distinct command.
func shimNames(shim, deny []string, goos string) []string {
	var out []string
	for _, n := range append(append([]string{}, shim...), deny...) {
		if !inNames(out, n, goos) {
			out = append(out, n)
		}
	}
	return out
}

// shimIdentity reports whether the executable at exe is a shim: it lives in
// <run-dir>/bin of a run dir made by 'xrr run'. Location, not name, decides,
// so the real CLI works under any file name and a shimmed command may even
// be called "xrr". It needs no environment, so shims survive `env -i`.
func shimIdentity(exe, goos string) (runDir, name string, ok bool) {
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
	return runDir, canonName(filepath.Base(exe), goos), true
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
