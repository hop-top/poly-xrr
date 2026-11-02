package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// defaultDeny is the built-in side-effect set: commands a test suite should
// never run for real (they open browsers, send mail, touch the clipboard,
// speak). An explicit --shim of one of these wins over the default.
var defaultDeny = []string{
	"open", "xdg-open", "wslview", "explorer.exe", "start",
	"sendmail", "mail", "osascript", "say", "pbcopy", "notify-send",
}

// runConfig is written once by `xrr run` and read by every shim process.
// It lives in the run dir so shims need no env vars and survive `env -i`.
type runConfig struct {
	Mode        string   `json:"mode"` // record | replay | passthrough
	CassetteDir string   `json:"cassette_dir,omitempty"`
	RealPath    string   `json:"real_path"` // PATH as it was before shims
	Strict      bool     `json:"strict"`
	Deny        []string `json:"deny"`
	EnvCapture  []string `json:"env_capture,omitempty"`
}

const (
	configFile = "config.json"
	callsFile  = "calls.jsonl"
	binSubdir  = "bin"
)

func writeConfig(runDir string, c runConfig) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(runDir, configFile), b, 0o600)
}

func loadConfig(runDir string) (runConfig, error) {
	var c runConfig
	b, err := os.ReadFile(filepath.Join(runDir, configFile))
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("parse %s: %w", configFile, err)
	}
	return c, nil
}

// listFlag is a repeatable, comma-separated string-list flag.
type listFlag []string

func (l *listFlag) String() string { return strings.Join(*l, ",") }

func (l *listFlag) Set(v string) error {
	for _, p := range strings.Split(v, ",") {
		p = strings.TrimSpace(p)
		if p == "" || contains(*l, p) {
			continue
		}
		*l = append(*l, p)
	}
	return nil
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// validCommandName rejects names that cannot be a bare PATH entry.
func validCommandName(n string) error {
	switch {
	case n == "" || n == "." || n == "..":
		return fmt.Errorf("invalid command name %q", n)
	case strings.ContainsAny(n, "/\\\x00 \t\n"):
		return fmt.Errorf("command name %q must be a bare name (no path separators or spaces)", n)
	}
	return nil
}
