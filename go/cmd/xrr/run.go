package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	xrr "hop.top/xrr"
)

const runUsage = `usage: xrr run [flags] -- <cmd> [args...]

Runs <cmd> with a temp dir of shims prepended to PATH. Each --shim command is
recorded to / replayed from exec cassettes; each denied command is blocked.
A summary goes to stderr; the run fails on any deny hit or cassette miss.

flags:
`

type runOpts struct {
	mode, cassettes, reportFile string
	shim, deny, env             listFlag
	noDefaultDeny, strict       bool
	stdinWait                   time.Duration
}

func runCmd(args []string, stdout, stderr io.Writer) int {
	var o runOpts
	fs := flag.NewFlagSet("xrr run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, runUsage); fs.PrintDefaults() }
	fs.StringVar(&o.mode, "mode", "", "record | replay | passthrough (default: $XRR_MODE, else replay)")
	fs.StringVar(&o.cassettes, "cassettes", "", "cassette directory (default: $XRR_CASSETTE_DIR, else ./cassettes)")
	fs.Var(&o.shim, "shim", "comma-separated commands to record/replay (repeatable)")
	fs.Var(&o.deny, "deny", "comma-separated commands to block, added to the default side-effect set (repeatable)")
	fs.BoolVar(&o.noDefaultDeny, "no-default-deny", false, "do not block the built-in side-effect set (open, xdg-open, osascript, sendmail, pbcopy, ...)")
	fs.BoolVar(&o.strict, "strict", true, "replay: a shimmed call with no cassette fails (--strict=false falls through to the real binary)")
	fs.Var(&o.env, "env", "comma-separated env var names to capture into cassettes (redacted like any other field)")
	fs.DurationVar(&o.stdinWait, "stdin-wait", 250*time.Millisecond, "replay: stop reading a shim's stdin after this long with no data (an open, silent pipe counts as empty)")
	fs.StringVar(&o.reportFile, "report-file", "", "also write the summary as JSON to this file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	cfg, child, err := o.resolve(fs.Args())
	if err != nil {
		fmt.Fprintf(stderr, "xrr run: %v\n", err)
		return exitUsage
	}
	return execute(o, cfg, child, stdout, stderr)
}

// resolve validates flags and builds the config handed to the shims.
func (o runOpts) resolve(child []string) (runConfig, []string, error) {
	var cfg runConfig
	if len(child) == 0 {
		return cfg, nil, errors.New("no command to run (usage: xrr run [flags] -- <cmd> [args...])")
	}
	mode := o.mode
	if mode == "" {
		mode = os.Getenv(xrr.EnvMode)
	}
	if mode == "" {
		mode = string(xrr.ModeReplay)
	}
	switch xrr.Mode(mode) {
	case xrr.ModeRecord, xrr.ModeReplay, xrr.ModePassthrough:
	default:
		return cfg, nil, fmt.Errorf("invalid mode %q (want record|replay|passthrough)", mode)
	}
	for _, n := range append(append([]string{}, o.shim...), o.deny...) {
		if err := validCommandName(n); err != nil {
			return cfg, nil, err
		}
	}
	for _, n := range o.shim {
		if contains(o.deny, n) {
			return cfg, nil, fmt.Errorf("%q is in both --shim and --deny", n)
		}
	}
	dir := o.cassettes
	if dir == "" {
		dir = os.Getenv(xrr.EnvCassetteDir)
	}
	if dir == "" {
		dir = "cassettes"
	}
	if xrr.Mode(mode) != xrr.ModePassthrough {
		if len(o.shim) == 0 {
			return cfg, nil, fmt.Errorf("--shim is required in %s mode", mode)
		}
		if err := prepareCassetteDir(xrr.Mode(mode), dir); err != nil {
			return cfg, nil, err
		}
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return cfg, nil, err
	}
	deny := append([]string{}, o.deny...)
	if !o.noDefaultDeny {
		for _, d := range defaultDeny {
			if !contains(o.shim, d) && !contains(deny, d) {
				deny = append(deny, d)
			}
		}
	}
	return runConfig{
		Mode: mode, CassetteDir: abs, RealPath: os.Getenv("PATH"),
		Strict: o.strict, Deny: deny, EnvCapture: o.env, StdinWaitMs: int(o.stdinWait / time.Millisecond),
	}, child, nil
}

func prepareCassetteDir(mode xrr.Mode, dir string) error {
	fi, err := os.Stat(dir)
	switch {
	case err == nil && !fi.IsDir():
		return fmt.Errorf("cassette path %q is not a directory", dir)
	case err == nil:
		return nil
	case os.IsNotExist(err) && mode == xrr.ModeRecord:
		return os.MkdirAll(dir, 0o755)
	case os.IsNotExist(err):
		return fmt.Errorf("cassette directory %q does not exist (nothing to replay; use --mode record first)", dir)
	default:
		return err
	}
}

func execute(o runOpts, cfg runConfig, child []string, stdout, stderr io.Writer) int {
	self, err := os.Executable()
	if err != nil {
		fmt.Fprintf(stderr, "xrr run: locate own binary: %v\n", err)
		return exitUsage
	}
	runDir, err := os.MkdirTemp("", "xrr-run-")
	if err != nil {
		fmt.Fprintf(stderr, "xrr run: %v\n", err)
		return exitUsage
	}
	defer os.RemoveAll(runDir)
	binDir := filepath.Join(runDir, binSubdir)
	if err := os.Mkdir(binDir, 0o755); err != nil {
		fmt.Fprintf(stderr, "xrr run: %v\n", err)
		return exitUsage
	}
	for _, name := range append(append([]string{}, o.shim...), cfg.Deny...) {
		if err := writeShim(binDir, self, runDir, name); err != nil {
			fmt.Fprintf(stderr, "xrr run: write shim %s: %v\n", name, err)
			return exitUsage
		}
	}
	if err := writeConfig(runDir, cfg); err != nil {
		fmt.Fprintf(stderr, "xrr run: %v\n", err)
		return exitUsage
	}

	// Resolve the child against the original PATH, before shims exist on it.
	path, err := exec.LookPath(child[0])
	if err != nil {
		fmt.Fprintf(stderr, "xrr run: %v\n", err)
		return exitNotFound
	}
	cmd := exec.Command(path, child[1:]...)
	cmd.Args[0] = child[0]
	cmd.Env = withEnv(os.Environ(), "PATH", binDir+string(os.PathListSeparator)+cfg.RealPath)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, stdout, stderr
	code := waitForwardingSignals(cmd, stderr)

	calls, err := readCalls(runDir)
	if err != nil {
		fmt.Fprintf(stderr, "xrr run: read call log: %v\n", err)
		return exitViolation
	}
	renderReport(stderr, cfg.Mode, calls)
	if o.reportFile != "" {
		if err := writeReportFile(o.reportFile, cfg.Mode, calls); err != nil {
			fmt.Fprintf(stderr, "xrr run: write report: %v\n", err)
			return exitViolation
		}
	}
	switch {
	case code != 0:
		return code
	case anyFailing(calls):
		return exitViolation
	}
	return exitOK
}

// writeShim writes an executable sh stub that re-enters this binary. The
// run dir and binary are baked in, so the stub works under `env -i` and
// from any cwd. POSIX sh only: Windows is not supported.
func writeShim(binDir, self, runDir, name string) error {
	body := fmt.Sprintf("#!/bin/sh\nexec %s shim %s %s \"$@\"\n", shQuote(self), shQuote(runDir), shQuote(name))
	return os.WriteFile(filepath.Join(binDir, name), []byte(body), 0o755)
}

func shQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// withEnv returns env with key set to val (replacing any existing entry).
func withEnv(env []string, key, val string) []string {
	out := make([]string, 0, len(env)+1)
	for _, e := range env {
		if !strings.HasPrefix(e, key+"=") {
			out = append(out, e)
		}
	}
	return append(out, key+"="+val)
}

// waitForwardingSignals runs cmd, relaying SIGINT/SIGTERM so the child can
// clean up while xrr still prints its report and removes the shim dir.
func waitForwardingSignals(cmd *exec.Cmd, stderr io.Writer) int {
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(stderr, "xrr run: %v\n", err)
		return exitNotFound
	}
	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case s := <-sigs:
				_ = cmd.Process.Signal(s)
			case <-done:
				return
			}
		}
	}()
	err := cmd.Wait()
	close(done)
	return exitCodeOf(err)
}

// exitCodeOf maps a Wait/Run error to a shell-style exit code
// (128+signal for signalled processes).
func exitCodeOf(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			return 128 + int(ws.Signal())
		}
		if c := ee.ExitCode(); c > 0 {
			return c
		}
	}
	return 1
}
