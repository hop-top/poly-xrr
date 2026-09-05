package sql

import (
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
	xrr "hop.top/xrr"
)

var wsRe = regexp.MustCompile(`\s+`)

// normalizeQuery strips extra whitespace and lowercases.
func normalizeQuery(q string) string {
	return strings.TrimSpace(wsRe.ReplaceAllString(strings.ToLower(q), " "))
}

// Request represents a SQL interaction request.
type Request struct {
	Query string `yaml:"query" json:"query"`
	Args  []any  `yaml:"args,omitempty" json:"args,omitempty"`
}

func (r *Request) AdapterID() string { return "sql" }

// Response represents a SQL interaction response.
type Response struct {
	Rows     []map[string]any `yaml:"rows,omitempty"`
	Affected int64            `yaml:"affected,omitempty"`
}

func (r *Response) AdapterID() string { return "sql" }

// Adapter implements xrr.Adapter for SQL interactions.
type Adapter struct{}

func NewAdapter() *Adapter { return &Adapter{} }

func (a *Adapter) ID() string { return "sql" }

// Fingerprint: sha256(normalized query + args)[:8].
func (a *Adapter) Fingerprint(req xrr.Request) (string, error) {
	r, ok := req.(*Request)
	if !ok {
		return "", fmt.Errorf("sql: unexpected request type %T", req)
	}
	fp, err := xrr.CanonicalFingerprint(map[string]any{
		"query": normalizeQuery(r.Query),
		"args":  r.Args,
	})
	if err != nil {
		return "", fmt.Errorf("sql: fingerprint marshal: %w", err)
	}
	return fp, nil
}

func (a *Adapter) Serialize(v any) ([]byte, error)           { return yaml.Marshal(v) }
func (a *Adapter) Deserialize(data []byte, target any) error { return yaml.Unmarshal(data, target) }
