package http

import (
	"crypto/sha256"
	"fmt"
	"net/url"

	"gopkg.in/yaml.v3"
	xrr "hop.top/xrr"
)

// Request represents an HTTP interaction request.
type Request struct {
	Method  string            `yaml:"method"  json:"method"`
	URL     string            `yaml:"url"     json:"url"`
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Body    string            `yaml:"body,omitempty"    json:"body,omitempty"`
}

func (r *Request) AdapterID() string { return "http" }

// Response represents an HTTP interaction response.
type Response struct {
	Status  int               `yaml:"status"`
	Headers map[string]string `yaml:"headers,omitempty"`
	Body    string            `yaml:"body,omitempty"`
}

func (r *Response) AdapterID() string { return "http" }

// Adapter implements xrr.Adapter for HTTP interactions.
type Adapter struct{}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ID() string { return "http" }

// Fingerprint: sha256(method + path+query + sha256(body)[:8])[:8].
func (a *Adapter) Fingerprint(req xrr.Request) (string, error) {
	r, ok := req.(*Request)
	if !ok {
		return "", fmt.Errorf("http: unexpected request type %T", req)
	}
	u, err := url.Parse(r.URL)
	if err != nil {
		return "", fmt.Errorf("http: parse url: %w", err)
	}
	pathQuery := u.Path
	if u.RawQuery != "" {
		pathQuery += "?" + u.RawQuery
	}
	bodyHash := sha256.Sum256([]byte(r.Body))
	fp, err := xrr.CanonicalFingerprint(map[string]any{
		"method":    r.Method,
		"path":      pathQuery,
		"body_hash": fmt.Sprintf("%x", bodyHash[:4]),
	})
	if err != nil {
		return "", fmt.Errorf("http: fingerprint marshal: %w", err)
	}
	return fp, nil
}

func (a *Adapter) Serialize(v any) ([]byte, error)           { return yaml.Marshal(v) }
func (a *Adapter) Deserialize(data []byte, target any) error { return yaml.Unmarshal(data, target) }
