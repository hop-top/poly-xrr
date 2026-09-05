package grpc

import (
	"crypto/sha256"
	"fmt"

	"gopkg.in/yaml.v3"
	xrr "hop.top/xrr"
)

// Request represents a gRPC interaction request.
type Request struct {
	Service string `yaml:"service" json:"service"`
	Method  string `yaml:"method"  json:"method"`
	Message []byte `yaml:"message,omitempty" json:"message,omitempty"`
}

func (r *Request) AdapterID() string { return "grpc" }

// Response represents a gRPC interaction response.
type Response struct {
	StatusCode int    `yaml:"status_code"`
	Message    []byte `yaml:"message,omitempty"`
}

func (r *Response) AdapterID() string { return "grpc" }

// Adapter implements xrr.Adapter for gRPC interactions.
type Adapter struct{}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ID() string { return "grpc" }

// Fingerprint: sha256(service + method + sha256(proto-bytes)[:8])[:8].
func (a *Adapter) Fingerprint(req xrr.Request) (string, error) {
	r, ok := req.(*Request)
	if !ok {
		return "", fmt.Errorf("grpc: unexpected request type %T", req)
	}
	msgHash := sha256.Sum256(r.Message)
	fp, err := xrr.CanonicalFingerprint(map[string]any{
		"service":  r.Service,
		"method":   r.Method,
		"msg_hash": fmt.Sprintf("%x", msgHash[:4]),
	})
	if err != nil {
		return "", fmt.Errorf("grpc: fingerprint marshal: %w", err)
	}
	return fp, nil
}

func (a *Adapter) Serialize(v any) ([]byte, error)           { return yaml.Marshal(v) }
func (a *Adapter) Deserialize(data []byte, target any) error { return yaml.Unmarshal(data, target) }
