package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// settings is one configuration layer. Scalars are pointers so "unset" is
// distinguishable from the zero value; lists are plain slices (nil = unset).
//
// The same struct is the YAML schema of the config files, so unknown keys
// are rejected at decode time.
type settings struct {
	Mode          *string       `yaml:"mode"`
	Cassettes     *string       `yaml:"cassettes"`
	Shim          []string      `yaml:"shim"`
	Deny          []string      `yaml:"deny"`
	NoDefaultDeny *bool         `yaml:"no_default_deny"`
	Strict        *bool         `yaml:"strict"`
	Env           []string      `yaml:"env"`
	StdinWait     *string       `yaml:"stdin_wait"`
	ReportFile    *string       `yaml:"report_file"`
	Scrub         scrubSettings `yaml:"scrub"`
}

// scrubSettings: Deny = env names always scrubbed (--scrub); Allow = env
// names never scrubbed (--no-scrub).
type scrubSettings struct {
	Allow []string `yaml:"allow"`
	Deny  []string `yaml:"deny"`
}

// Names of the default config files.
const (
	projectConfigName = ".xrr.yaml"
	userConfigRel     = "xrr/config.yaml" // under $XDG_CONFIG_HOME or ~/.config
)

// mergeSettings overlays hi on lo: a set scalar in hi wins, lists are the
// order-preserving union of both.
func mergeSettings(lo, hi settings) settings {
	out := lo
	pick := func(dst **string, v *string) {
		if v != nil {
			*dst = v
		}
	}
	pickB := func(dst **bool, v *bool) {
		if v != nil {
			*dst = v
		}
	}
	pick(&out.Mode, hi.Mode)
	pick(&out.Cassettes, hi.Cassettes)
	pick(&out.StdinWait, hi.StdinWait)
	pick(&out.ReportFile, hi.ReportFile)
	pickB(&out.NoDefaultDeny, hi.NoDefaultDeny)
	pickB(&out.Strict, hi.Strict)
	out.Shim = union(lo.Shim, hi.Shim)
	out.Deny = union(lo.Deny, hi.Deny)
	out.Env = union(lo.Env, hi.Env)
	out.Scrub.Allow = union(lo.Scrub.Allow, hi.Scrub.Allow)
	out.Scrub.Deny = union(lo.Scrub.Deny, hi.Scrub.Deny)
	return out
}

func union(a, b []string) []string {
	var out []string
	for _, l := range [][]string{a, b} {
		for _, v := range l {
			if !contains(out, v) {
				out = append(out, v)
			}
		}
	}
	return out
}

// userConfigPath is $XDG_CONFIG_HOME/xrr/config.yaml, else
// ~/.config/xrr/config.yaml, else (Windows, where HOME is normally unset)
// %APPDATA%\xrr\config.yaml. Empty when no base can be determined.
func userConfigPath(getenv func(string) string) string {
	if x := getenv("XDG_CONFIG_HOME"); x != "" {
		return filepath.Join(x, filepath.FromSlash(userConfigRel))
	}
	if h := getenv("HOME"); h != "" {
		return filepath.Join(h, ".config", filepath.FromSlash(userConfigRel))
	}
	if a := getenv("APPDATA"); a != "" {
		return filepath.Join(a, filepath.FromSlash(userConfigRel))
	}
	return ""
}

// loadFile reads one config file. A missing file is not an error unless
// required. Relative `cassettes` / `report_file` paths resolve against the
// file's own directory, so a checked-in .xrr.yaml behaves the same from any
// cwd.
func loadFile(path string, required bool) (settings, error) {
	var s settings
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) && !required {
		return s, nil
	}
	if err != nil {
		return s, fmt.Errorf("config %s: %w", path, err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&s); err != nil && !errors.Is(err, io.EOF) {
		return settings{}, fmt.Errorf("config %s: %w", path, err)
	}
	dir := filepath.Dir(path)
	for _, p := range []*string{s.Cassettes, s.ReportFile} {
		if p != nil && *p != "" && !filepath.IsAbs(*p) {
			*p = filepath.Join(dir, *p)
		}
	}
	if err := s.validate(); err != nil {
		return settings{}, fmt.Errorf("config %s: %w", path, err)
	}
	return s, nil
}

// envKeys maps each setting to its environment variable. Lists are
// comma-separated.
var (
	envStrings = map[string]func(*settings) **string{
		"XRR_MODE":         func(s *settings) **string { return &s.Mode },
		"XRR_CASSETTE_DIR": func(s *settings) **string { return &s.Cassettes },
		"XRR_STDIN_WAIT":   func(s *settings) **string { return &s.StdinWait },
		"XRR_REPORT_FILE":  func(s *settings) **string { return &s.ReportFile },
	}
	envBools = map[string]func(*settings) **bool{
		"XRR_STRICT":          func(s *settings) **bool { return &s.Strict },
		"XRR_NO_DEFAULT_DENY": func(s *settings) **bool { return &s.NoDefaultDeny },
	}
	envLists = map[string]func(*settings) *[]string{
		"XRR_SHIM":     func(s *settings) *[]string { return &s.Shim },
		"XRR_DENY":     func(s *settings) *[]string { return &s.Deny },
		"XRR_ENV":      func(s *settings) *[]string { return &s.Env },
		"XRR_SCRUB":    func(s *settings) *[]string { return &s.Scrub.Deny },
		"XRR_NO_SCRUB": func(s *settings) *[]string { return &s.Scrub.Allow },
	}
)

// envSettings reads the XRR_* environment layer. Empty vars count as unset.
func envSettings(getenv func(string) string) (settings, error) {
	var s settings
	for name, f := range envStrings {
		if v := getenv(name); v != "" {
			v := v
			*f(&s) = &v
		}
	}
	for name, f := range envBools {
		if v := getenv(name); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return s, fmt.Errorf("%s=%q: want a boolean", name, v)
			}
			*f(&s) = &b
		}
	}
	for name, f := range envLists {
		if v := getenv(name); v != "" {
			var l listFlag
			_ = l.Set(v)
			*f(&s) = l
		}
	}
	if err := s.validate(); err != nil {
		return s, fmt.Errorf("environment: %w", err)
	}
	return s, nil
}

// validate checks values that can be checked in isolation, so a bad entry
// is reported against the layer that contains it.
func (s settings) validate() error {
	if s.Mode != nil {
		if err := validMode(*s.Mode); err != nil {
			return err
		}
	}
	if s.StdinWait != nil {
		if _, err := time.ParseDuration(*s.StdinWait); err != nil {
			return fmt.Errorf("stdin_wait %q: want a duration like 250ms", *s.StdinWait)
		}
	}
	for _, n := range append(append([]string{}, s.Shim...), s.Deny...) {
		if err := validCommandName(n); err != nil {
			return err
		}
	}
	if err := validatePatterns("scrub.deny", s.Scrub.Deny); err != nil {
		return err
	}
	return validatePatterns("scrub.allow", s.Scrub.Allow)
}

func validMode(m string) error {
	switch m {
	case "record", "replay", "passthrough":
		return nil
	}
	return fmt.Errorf("invalid mode %q (want record|replay|passthrough)", m)
}

// loadLayers composes user file < project file < env < flags.
// configPath, when non-empty, replaces the project file and must exist.
func loadLayers(configPath string, flags settings, getenv func(string) string) (settings, error) {
	var user settings
	if p := userConfigPath(getenv); p != "" {
		var err error
		if user, err = loadFile(p, false); err != nil {
			return settings{}, err
		}
	}
	var project settings
	var err error
	if configPath != "" {
		project, err = loadFile(configPath, true)
	} else {
		project, err = loadFile(projectConfigName, false)
	}
	if err != nil {
		return settings{}, err
	}
	env, err := envSettings(getenv)
	if err != nil {
		return settings{}, err
	}
	merged := mergeSettings(mergeSettings(mergeSettings(user, project), env), flags)
	if err := merged.checkConflicts(); err != nil {
		return settings{}, err
	}
	return merged, nil
}

// checkConflicts enforces cross-list rules once all layers are merged.
func (s settings) checkConflicts() error {
	for _, n := range s.Scrub.Deny {
		if contains(s.Scrub.Allow, n) {
			return fmt.Errorf("%q is in both scrub.deny (--scrub) and scrub.allow (--no-scrub)", n)
		}
	}
	for _, n := range s.Shim {
		if contains(s.Deny, n) {
			return fmt.Errorf("%q is in both shim and deny", n)
		}
	}
	return nil
}

// toOpts resolves a merged layer stack to concrete options, applying defaults.
func (s settings) toOpts() runOpts {
	o := runOpts{
		mode: "replay", cassettes: "cassettes", strict: true, stdinWait: 250 * time.Millisecond,
		shim: s.Shim, deny: s.Deny, env: s.Env,
		scrubDeny: s.Scrub.Deny, scrubAllow: s.Scrub.Allow,
	}
	if s.Mode != nil {
		o.mode = *s.Mode
	}
	if s.Cassettes != nil && *s.Cassettes != "" {
		o.cassettes = *s.Cassettes
	}
	if s.Strict != nil {
		o.strict = *s.Strict
	}
	if s.NoDefaultDeny != nil {
		o.noDefaultDeny = *s.NoDefaultDeny
	}
	if s.StdinWait != nil {
		o.stdinWait, _ = time.ParseDuration(*s.StdinWait) // validated per layer
	}
	if s.ReportFile != nil {
		o.reportFile = *s.ReportFile
	}
	return o
}
