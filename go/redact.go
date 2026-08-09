package xrr

import (
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Env vars controlling redaction. Redaction is ON by default; these
// only exist to widen, narrow, or switch it off.
const (
	// EnvRedactDisable turns redaction off entirely when set to a
	// non-empty value other than "0"/"false". Recording then writes
	// values verbatim — only appropriate against fake credentials.
	EnvRedactDisable = "XRR_REDACT_DISABLE"

	// EnvRedactAllow is a comma-separated list of field names to
	// preserve verbatim even when they would otherwise be redacted.
	// The escape hatch for cassettes that must retain a value (e.g. a
	// deliberately fake fixture token).
	EnvRedactAllow = "XRR_REDACT_ALLOW"

	// EnvRedactDeny is a comma-separated list of extra field names to
	// always redact, for credentials whose names the defaults miss.
	EnvRedactDeny = "XRR_REDACT_DENY"
)

// redactedPrefix/Suffix wrap the field name in the placeholder.
const (
	redactedPrefix = "<redacted:"
	redactedSuffix = ">"
)

// secretKeySubstrings are matched against the *normalized* field name
// (uppercased, dashes → underscores). A name containing any of these as
// an underscore-delimited word, or as a prefix/suffix of one, is treated
// as credential-bearing.
//
// Matching is word-boundary aware so MONKEY_BUSINESS does not trip on
// "KEY" and TOKENIZER_MODE does not trip on "TOKEN".
var secretKeyWords = []string{
	"TOKEN", "SECRET", "PASSWORD", "PASSWD", "PASSPHRASE", "CREDENTIAL",
	"CREDENTIALS", "APIKEY", "KEY", "AUTH", "AUTHORIZATION", "COOKIE",
	"SESSION", "SIGNATURE", "PRIVATE", "ACCESS", "BEARER", "OTP",
}

// secretKeyExact covers names that are credential-bearing as a whole but
// whose words are too generic to blanket-match (e.g. "ACCESS" alone).
var secretKeyExact = map[string]bool{
	"AUTHORIZATION":         true,
	"PROXY_AUTHORIZATION":   true,
	"COOKIE":                true,
	"SET_COOKIE":            true,
	"AWS_ACCESS_KEY_ID":     true,
	"AWS_SECRET_ACCESS_KEY": true,
	"AWS_SESSION_TOKEN":     true,
}

// secretKeyPrefixes: whole namespaces that are credential-adjacent
// enough to redact wholesale.
var secretKeyPrefixes = []string{"AWS_"}

// benignKeys are never redacted by name, even though they contain a
// secret-ish word. These are well-known, non-credential variables whose
// values carry real debugging signal.
var benignKeys = map[string]bool{
	"XRR_MODE":             true,
	"XRR_CASSETTE_DIR":     true,
	"XRR_REDACT_ALLOW":     true,
	"XRR_REDACT_DENY":      true,
	"XRR_REDACT_DISABLE":   true,
	"AWS_REGION":           true,
	"AWS_DEFAULT_REGION":   true,
	"AWS_PROFILE":          true,
	"SSH_AUTH_SOCK":        true,
	"KEYBOARD_LAYOUT":      true,
	"ACCESS_LOG":           true,
	"ACCESS_LOG_FORMAT":    true,
	"PRIVATE_NETWORK":      true,
	"SESSION_MANAGER":      true,
	"GPG_TTY":              true,
	"AUTHORIZED_KEYS_FILE": true,
}

// secretValuePatterns are high-confidence, vendor-prefixed credential
// shapes. Deliberately narrow: a false positive silently corrupts a
// cassette, so only formats with a distinctive prefix or structure are
// listed. Generic "long random-looking string" heuristics are NOT used
// — they would redact commit SHAs, UUIDs, and base64 payloads.
var secretValuePatterns = []*regexp.Regexp{
	// GitHub: ghp_/gho_/ghu_/ghs_/ghr_ + 36+ chars, and fine-grained PATs.
	regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{20,}\b`),
	regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,}\b`),
	// AWS access key IDs.
	regexp.MustCompile(`\b(?:AKIA|ASIA|ABIA|ACCA)[A-Z0-9]{16}\b`),
	// OpenAI / Anthropic style.
	regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{20,}\b`),
	// Slack.
	regexp.MustCompile(`\bxox[abposr]-[A-Za-z0-9-]{10,}\b`),
	// Google API keys.
	regexp.MustCompile(`\bAIza[A-Za-z0-9_-]{35}\b`),
	// Stripe.
	regexp.MustCompile(`\b(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}\b`),
	// PEM private key blocks.
	regexp.MustCompile(`-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----`),
	// JWTs: three base64url segments, header starts with the standard
	// `{"alg"` prefix encoded as eyJhbGci.
	regexp.MustCompile(`\beyJhbGci[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+`),
	// Bearer/Basic credentials embedded in a header value.
	regexp.MustCompile(`(?i)\b(?:bearer|basic)\s+[A-Za-z0-9+/._~-]{16,}={0,2}`),
}

// RedactConfig configures a Redactor.
//
// Zero value = defaults on: name-based matching over the built-in word
// list plus value-pattern matching over the built-in vendor patterns.
type RedactConfig struct {
	// Disabled turns redaction off entirely.
	Disabled bool
	// Allow lists field names preserved verbatim (wins over everything).
	Allow []string
	// Deny lists extra field names to always redact.
	Deny []string
}

// RedactConfigFromEnv builds a RedactConfig from the XRR_REDACT_* env
// vars. Unset vars leave the secure defaults in place.
func RedactConfigFromEnv() RedactConfig {
	return RedactConfig{
		Disabled: truthy(os.Getenv(EnvRedactDisable)),
		Allow:    splitList(os.Getenv(EnvRedactAllow)),
		Deny:     splitList(os.Getenv(EnvRedactDeny)),
	}
}

func truthy(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "0", "false", "no":
		return false
	default:
		return true
	}
}

func splitList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// Redactor classifies field names and values as secret-bearing and
// produces deterministic placeholders for them.
//
// Redaction is applied at record time, before any bytes reach disk —
// see FileCassette.write. A secret is never written and then cleaned up.
type Redactor struct {
	disabled bool
	allow    map[string]bool
	deny     map[string]bool
}

// NewRedactor returns a Redactor for cfg. The zero RedactConfig yields
// the secure defaults.
func NewRedactor(cfg RedactConfig) *Redactor {
	r := &Redactor{
		disabled: cfg.Disabled,
		allow:    make(map[string]bool, len(cfg.Allow)),
		deny:     make(map[string]bool, len(cfg.Deny)),
	}
	for _, k := range cfg.Allow {
		r.allow[normalizeKey(k)] = true
	}
	for _, k := range cfg.Deny {
		r.deny[normalizeKey(k)] = true
	}
	return r
}

// normalizeKey uppercases and folds dashes to underscores so header
// names ("X-Api-Key") and env names ("X_API_KEY") classify identically.
func normalizeKey(k string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(k), "-", "_"))
}

// IsSecretKey reports whether a field name looks credential-bearing.
func (r *Redactor) IsSecretKey(name string) bool {
	if r.disabled {
		return false
	}
	n := normalizeKey(name)
	if r.allow[n] {
		return false
	}
	if r.deny[n] {
		return true
	}
	if benignKeys[n] {
		return false
	}
	if secretKeyExact[n] {
		return true
	}
	for _, p := range secretKeyPrefixes {
		if strings.HasPrefix(n, p) {
			return true
		}
	}
	// Word-boundary match over underscore-delimited segments.
	for _, word := range strings.Split(n, "_") {
		for _, sw := range secretKeyWords {
			if word == sw {
				return true
			}
		}
	}
	return false
}

// IsSecretValue reports whether a value matches a known credential
// pattern. Used to catch secrets in fields whose names give no hint.
func (r *Redactor) IsSecretValue(v string) bool {
	if r.disabled || v == "" {
		return false
	}
	for _, re := range secretValuePatterns {
		if re.MatchString(v) {
			return true
		}
	}
	return false
}

// Placeholder returns the deterministic replacement for a field.
//
// The placeholder depends only on the field name — never on the secret
// value, a counter, or a hash. Re-recording the same interaction
// therefore produces byte-identical cassettes, so committed cassettes
// do not churn and fingerprints stay stable.
func (r *Redactor) Placeholder(name string) string {
	return redactedPrefix + normalizeDisplayKey(name) + redactedSuffix
}

// normalizeDisplayKey uppercases but preserves dashes, so an HTTP header
// renders as <redacted:X-API-KEY> and an env var as <redacted:API_KEY>.
func normalizeDisplayKey(k string) string {
	return strings.ToUpper(strings.TrimSpace(k))
}

// RedactField returns the value to serialize for (name, value) and
// whether it was redacted.
//
// A field is redacted when its name looks credential-bearing OR its
// value matches a known credential pattern. Empty values are left
// alone — there is nothing to leak, and a placeholder would misleadingly
// imply a secret was present.
func (r *Redactor) RedactField(name, value string) (string, bool) {
	if r.disabled || value == "" {
		return value, false
	}
	if r.allow[normalizeKey(name)] {
		return value, false
	}
	if r.IsSecretKey(name) || r.IsSecretValue(value) {
		return r.Placeholder(name), true
	}
	return value, false
}

// redactNode walks a decoded YAML tree in place, replacing
// credential-bearing scalars with deterministic placeholders.
//
// key is the mapping key the node was reached under ("" at the root and
// inside sequences). Only scalar *values* are rewritten; mapping keys
// and non-string scalars (ints, bools, null) are left untouched so the
// YAML shape and types survive redaction intact.
//
// Envelope metadata (xrr, adapter, fingerprint, recorded_at, error) is
// never scrubbed: the fingerprint in particular must match the filename,
// and none of those fields ever carry adopter data.
func redactNode(n *yaml.Node, r *Redactor, key string) {
	if n == nil || r == nil || r.disabled {
		return
	}
	switch n.Kind {
	case yaml.DocumentNode:
		for _, c := range n.Content {
			redactNode(c, r, key)
		}
	case yaml.MappingNode:
		// Content alternates key, value, key, value...
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			if isEnvelopeMetaKey(key, k.Value) {
				continue
			}
			redactNode(v, r, k.Value)
		}
	case yaml.SequenceNode:
		// Sequence elements inherit the key of the sequence itself, so
		// `args: [--token, ghp_...]` still gets value-pattern coverage.
		for _, c := range n.Content {
			redactNode(c, r, key)
		}
	case yaml.ScalarNode:
		redactScalar(n, r, key)
	}
}

// envelopeMetaKeys are the top-level envelope fields that must never be
// rewritten. Scoped to the document root so a payload field genuinely
// named "error" or "adapter" is still eligible for redaction.
var envelopeMetaKeys = map[string]bool{
	"xrr": true, "adapter": true, "fingerprint": true,
	"recorded_at": true, "error": true,
}

func isEnvelopeMetaKey(parentKey, key string) bool {
	return parentKey == "" && envelopeMetaKeys[key]
}

// redactScalar rewrites a single scalar in place when it is
// credential-bearing.
func redactScalar(n *yaml.Node, r *Redactor, key string) {
	// Only touch string scalars. Ints/bools/null carry no credentials
	// and rewriting them would change the payload's types.
	if n.Tag != "" && n.Tag != "!!str" {
		return
	}
	redacted, changed := r.RedactField(key, n.Value)
	if !changed {
		return
	}
	n.SetString(redacted)
}
