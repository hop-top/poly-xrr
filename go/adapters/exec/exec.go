package exec

import (
	"fmt"

	"gopkg.in/yaml.v3"
	xrr "hop.top/xrr"
)

// Request represents an exec interaction request.
//
// Cwd is the working directory the subprocess was launched from. When
// non-empty it participates in the fingerprint, so the same command run
// in different directories produces distinct cassette keys — essential
// for cross-process e2e adopters (see XRR_CASSETTE_DIR pattern) whose
// tests invoke the same binary many times under different cwds within a
// single parent cassette dir.
//
// This is a Go-only extension to the v1 cassette spec. Within the Go
// port it is backward compatible: leaving Cwd empty preserves the
// canonical argv+stdin fingerprint and cassettes recorded before this
// field existed still match. But cassettes recorded with NON-EMPTY Cwd
// will NOT replay in ts / py / rs / php ports until those ports adopt
// the same rule — their fingerprint calculation will miss. Use
// non-empty Cwd only when record and replay happen in runtimes that
// agree on the extension, or leave Cwd empty to preserve cross-runtime
// replay. See spec/cassette-format-v1.md "Exec Fingerprint Inputs" for
// the formal status of this extension.
type Request struct {
	Argv  []string          `yaml:"argv"  json:"argv"`
	Stdin string            `yaml:"stdin,omitempty" json:"stdin,omitempty"`
	Cwd   string            `yaml:"cwd,omitempty"   json:"cwd,omitempty"`
	Env   map[string]string `yaml:"env,omitempty"   json:"env,omitempty"`
}

func (r *Request) AdapterID() string { return "exec" }

// Response represents an exec interaction response.
type Response struct {
	Stdout     string `yaml:"stdout"`
	Stderr     string `yaml:"stderr,omitempty"`
	ExitCode   int    `yaml:"exit_code"`
	DurationMs int64  `yaml:"duration_ms,omitempty"`
}

func (r *Response) AdapterID() string { return "exec" }

// Adapter implements xrr.Adapter for exec interactions.
type Adapter struct{}

// NewAdapter returns a new exec Adapter.
func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ID() string { return "exec" }

// Fingerprint returns sha256(canonical JSON of {argv, stdin, cwd?})[:8].
//
// cwd only participates in the hash when non-empty. This keeps
// backwards compatibility: adopters that don't populate Request.Cwd
// get the legacy argv+stdin-only fingerprint, so cassettes recorded
// before this field existed still match.
func (a *Adapter) Fingerprint(req xrr.Request) (string, error) {
	r, ok := req.(*Request)
	if !ok {
		return "", fmt.Errorf("exec: unexpected request type %T", req)
	}
	fields := map[string]any{
		"argv":  r.Argv,
		"stdin": r.Stdin,
	}
	if r.Cwd != "" {
		fields["cwd"] = r.Cwd
	}
	fp, err := xrr.CanonicalFingerprint(fields)
	if err != nil {
		return "", fmt.Errorf("exec: fingerprint marshal: %w", err)
	}
	return fp, nil
}

// Serialize marshals v as YAML.
func (a *Adapter) Serialize(v any) ([]byte, error) {
	return yaml.Marshal(v)
}

// Deserialize unmarshals data into target.
func (a *Adapter) Deserialize(data []byte, target any) error {
	return yaml.Unmarshal(data, target)
}
