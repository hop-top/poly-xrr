package xrr

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// envelope is the on-disk wrapper for both req and resp files.
//
// Error is optional; on a resp envelope it carries the error message
// returned by the original do() at record time. Empty/absent ⇒ success.
// See spec/cassette-format-v1.md.
type envelope struct {
	XRR         string `yaml:"xrr"`
	Adapter     string `yaml:"adapter"`
	Fingerprint string `yaml:"fingerprint"`
	RecordedAt  string `yaml:"recorded_at"`
	Error       string `yaml:"error,omitempty"`
	Payload     any    `yaml:"payload"`
}

// FileCassette stores interactions as YAML files in a directory.
type FileCassette struct {
	dir string
}

// NewFileCassette creates a FileCassette that reads/writes to dir.
func NewFileCassette(dir string) *FileCassette {
	return &FileCassette{dir: dir}
}

// Save writes req and resp as two YAML files under dir.
//
// recordedErr is the error returned by the original do() (or nil for
// success). When non-nil, recordedErr.Error() is persisted as the
// envelope-level "error" field on the resp file. The req file never
// carries an error.
//
// A nil resp (common when do() returns (nil, err)) is normalized to an
// empty map so the on-disk envelope still satisfies the v1 spec, which
// requires payload to be a non-null object. Replay will surface it as
// RawResponse{Payload: empty map} alongside the recorded error.
func (c *FileCassette) Save(adapterID, fingerprint string, req, resp any, recordedErr error) error {
	now := time.Now().UTC().Format(time.RFC3339)
	if err := c.write(adapterID, fingerprint, "req", now, "", normalizePayload(req)); err != nil {
		return err
	}
	respErr := ""
	if recordedErr != nil {
		respErr = recordedErr.Error()
	}
	return c.write(adapterID, fingerprint, "resp", now, respErr, normalizePayload(resp))
}

// normalizePayload returns an empty map when v is nil so the encoded
// envelope never contains payload: null (which violates the v1 spec
// "payload required object" rule and breaks downstream decoders that
// expect an object). Non-nil values pass through untouched.
func normalizePayload(v any) any {
	if v == nil {
		return map[string]any{}
	}
	return v
}

func (c *FileCassette) write(adapterID, fingerprint, kind, recordedAt, recordedErr string, payload any) error {
	env := envelope{
		XRR:         "1",
		Adapter:     adapterID,
		Fingerprint: fingerprint,
		RecordedAt:  recordedAt,
		Error:       recordedErr,
		Payload:     payload,
	}
	data, err := yaml.Marshal(env)
	if err != nil {
		return fmt.Errorf("xrr: marshal %s: %w", kind, err)
	}
	path := filepath.Join(c.dir, fmt.Sprintf("%s-%s.%s.yaml", adapterID, fingerprint, kind))
	return os.WriteFile(path, data, 0o644)
}

// Load reads the req and resp files and unmarshals payloads into targets.
//
// The returned recordedErr is the error string from the resp envelope's
// optional "error" field. Empty string ⇒ the recording succeeded; callers
// should treat that as nil. A non-empty string means the original do()
// returned a non-nil error at record time and replay must re-emit one.
func (c *FileCassette) Load(adapterID, fingerprint string, reqTarget, respTarget any) (recordedErr string, err error) {
	if _, err := c.read(adapterID, fingerprint, "req", reqTarget); err != nil {
		return "", err
	}
	return c.read(adapterID, fingerprint, "resp", respTarget)
}

func (c *FileCassette) read(adapterID, fingerprint, kind string, target any) (string, error) {
	path := filepath.Join(c.dir, fmt.Sprintf("%s-%s.%s.yaml", adapterID, fingerprint, kind))
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrCassetteMiss
		}
		return "", fmt.Errorf("xrr: read %s: %w", kind, err)
	}

	// First pass: decode full envelope to extract payload node + error field.
	var raw map[string]yaml.Node
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return "", fmt.Errorf("xrr: unmarshal envelope %s: %w", kind, err)
	}
	// A streamed cassette must never replay through the unary code path
	// (shape mismatch, distinct from a miss). Streamed pairs are read via
	// LoadStream instead.
	if _, streamed := raw["stream"]; streamed {
		return "", fmt.Errorf("xrr: streamed cassette in unary code path (%s): %w", kind, ErrShapeMismatch)
	}

	payloadNode, ok := raw["payload"]
	if !ok {
		return "", fmt.Errorf("xrr: missing payload in %s", kind)
	}

	// Second pass: decode payload node into target.
	if err := payloadNode.Decode(target); err != nil {
		return "", fmt.Errorf("xrr: decode payload %s: %w", kind, err)
	}

	// Pull recorded error string if present (resp only carries it; req
	// readers will get an empty string and ignore it).
	var recordedErr string
	if errNode, ok := raw["error"]; ok {
		_ = errNode.Decode(&recordedErr) // best effort; absent or non-string ⇒ ""
	}
	return recordedErr, nil
}
