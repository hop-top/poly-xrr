package grpctransport

import (
	"regexp"
	"strings"
)

// Header credential sanitization.
//
// Credentials travel in HTTP/2 HEADERS. On a live gRPC connection the
// request header block carries `authorization`, and adopters routinely add
// their own token headers — every one of them would land in a cassette
// verbatim if headers were recorded as observed.
//
// NOTE ON DUPLICATION: the core package gains an xrr.Redactor (field-name
// word lists, value patterns, deterministic <redacted:NAME> placeholders)
// on the in-flight secret-redaction branch, which is not yet merged to
// main. This file is the same policy, scoped to header name/value pairs, so
// this adapter does not depend on an unmerged branch. When redaction lands
// on main, headerRedactor should be deleted and its two methods delegated
// to xrr.Redactor.IsSecretKey / RedactField — the placeholder format and
// normalization rules here are deliberately identical so that swap is
// mechanical and cassettes do not churn.

const (
	redactedPrefix = "<redacted:"
	redactedSuffix = ">"
)

// secretHeaderWords are matched as underscore-delimited words against the
// normalized header name, so `monkey-business` does not trip on KEY.
var secretHeaderWords = []string{
	"TOKEN", "SECRET", "PASSWORD", "PASSWD", "PASSPHRASE", "CREDENTIAL",
	"CREDENTIALS", "APIKEY", "KEY", "AUTH", "AUTHORIZATION", "COOKIE",
	"SESSION", "SIGNATURE", "PRIVATE", "BEARER", "OTP",
}

// secretHeaderExact covers names credential-bearing as a whole.
var secretHeaderExact = map[string]bool{
	"AUTHORIZATION":       true,
	"PROXY_AUTHORIZATION": true,
	"COOKIE":              true,
	"SET_COOKIE":          true,
}

// benignHeaders are never redacted despite containing a secret-ish word:
// they are protocol headers carrying real debugging signal and no secret.
var benignHeaders = map[string]bool{
	"GRPC_ACCEPT_ENCODING": true,
	"ACCEPT_ENCODING":      true,
	"GRPC_ENCODING":        true,
	"CONTENT_TYPE":         true,
	"USER_AGENT":           true,
	"GRPC_TIMEOUT":         true,
	"GRPC_STATUS":          true,
	"GRPC_MESSAGE":         true,
	"TE":                   true,
}

// secretValuePatterns are high-confidence, vendor-prefixed credential
// shapes, for headers whose names give no hint. Deliberately narrow: a
// false positive silently corrupts a cassette, so no generic
// "long random string" heuristics.
var secretValuePatterns = []*regexp.Regexp{
	regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{20,}\b`),
	regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,}\b`),
	regexp.MustCompile(`\b(?:AKIA|ASIA|ABIA|ACCA)[A-Z0-9]{16}\b`),
	regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{20,}\b`),
	regexp.MustCompile(`\bxox[abposr]-[A-Za-z0-9-]{10,}\b`),
	regexp.MustCompile(`\bAIza[A-Za-z0-9_-]{35}\b`),
	regexp.MustCompile(`\b(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}\b`),
	regexp.MustCompile(`-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----`),
	regexp.MustCompile(`\beyJhbGci[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+`),
	regexp.MustCompile(`(?i)\b(?:bearer|basic)\s+[A-Za-z0-9+/._~-]{16,}={0,2}`),
}

// headerRedactor classifies header names and values as credential-bearing.
type headerRedactor struct{}

// normalizeHeaderKey uppercases and folds dashes to underscores, so
// "x-api-key" and "X_API_KEY" classify identically.
func normalizeHeaderKey(k string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(k), "-", "_"))
}

// isSecretName reports whether a header name looks credential-bearing.
func (headerRedactor) isSecretName(name string) bool {
	n := normalizeHeaderKey(name)
	if benignHeaders[n] {
		return false
	}
	if secretHeaderExact[n] {
		return true
	}
	for _, word := range strings.Split(n, "_") {
		for _, sw := range secretHeaderWords {
			if word == sw {
				return true
			}
		}
	}
	return false
}

// isSecretValue reports whether a value matches a known credential shape.
func (headerRedactor) isSecretValue(v string) bool {
	if v == "" {
		return false
	}
	for _, re := range secretValuePatterns {
		if re.MatchString(v) {
			return true
		}
	}
	return false
}

// redactField returns the value to record for (name, value) and whether it
// was redacted. The placeholder depends only on the header NAME — never on
// the secret, a counter, or a hash — so re-recording the same traffic
// yields byte-identical cassettes.
func (r headerRedactor) redactField(name, value string) (string, bool) {
	if value == "" {
		return value, false
	}
	if r.isSecretName(name) || r.isSecretValue(value) {
		return redactedPrefix + strings.ToUpper(strings.TrimSpace(name)) + redactedSuffix, true
	}
	return value, false
}
