package grpctransport

import (
	"testing"

	"golang.org/x/net/http2/hpack"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"hop.top/xrr"
)

// Header sanitization is defense in depth, and it is worth being explicit
// about why: the cassette format records no metadata at all (matching the
// unary gRPC adapter), so a decoded header value has no persistence path
// today even unsanitized. Sanitization exists so that the invariant holds
// at the seam rather than by luck — decoded headers are held in recorder
// state, appear in diagnostics, and would be the natural thing to persist
// if the format ever gained a metadata field. These tests therefore assert
// on the sanitizer directly rather than on cassette contents, which would
// pass whether or not the sanitizer ran.
//
// The policy under test is the core xrr.Redactor's, shared with every other
// port — this package holds no redaction policy of its own.

// testRedactor is the redactor the adapter builds at record time, with the
// env-driven config left at its secure defaults.
func testRedactor() *xrr.Redactor { return xrr.NewRedactor(xrr.RedactConfig{}) }

func TestSanitizeHeadersRedactsCredentials(t *testing.T) {
	const secret = "s3cr3t-token-value-not-in-cassettes"
	fields := []hpack.HeaderField{
		{Name: ":path", Value: "/pkg.Service/Method"},
		{Name: ":method", Value: "POST"},
		{Name: "authorization", Value: "Bearer " + secret},
		{Name: "x-api-key", Value: secret},
		{Name: "cookie", Value: "session=" + secret},
		{Name: "content-type", Value: "application/grpc"},
		{Name: "user-agent", Value: "grpc-go/1.83.1"},
		{Name: "grpc-timeout", Value: "29998683u"},
	}

	got := sanitizeHeaders(testRedactor(), fields)
	require.Len(t, got, len(fields))

	byName := map[string]string{}
	for _, f := range got {
		byName[f.Name] = f.Value
	}

	// Credentials are replaced by name-derived placeholders.
	assert.Equal(t, "<redacted:AUTHORIZATION>", byName["authorization"])
	assert.Equal(t, "<redacted:X-API-KEY>", byName["x-api-key"])
	assert.Equal(t, "<redacted:COOKIE>", byName["cookie"])

	// The secret must not survive anywhere in the output.
	for _, f := range got {
		assert.NotContains(t, f.Value, secret, "header %q leaked the secret", f.Name)
	}

	// Pseudo-headers and benign protocol headers pass through untouched.
	// :path in particular is a fingerprint input — redacting it would
	// break cassette addressing.
	assert.Equal(t, "/pkg.Service/Method", byName[":path"])
	assert.Equal(t, "POST", byName[":method"])
	assert.Equal(t, "application/grpc", byName["content-type"])
	assert.Equal(t, "grpc-go/1.83.1", byName["user-agent"])
	assert.Equal(t, "29998683u", byName["grpc-timeout"])
}

// Values matching a known credential shape are redacted even when the
// header name gives no hint.
func TestSanitizeHeadersRedactsByValuePattern(t *testing.T) {
	fields := []hpack.HeaderField{
		{Name: "x-trace", Value: "ghp_abcdefghijklmnopqrstuvwxyz0123456789"},
		{Name: "x-note", Value: "nothing interesting here"},
	}
	got := sanitizeHeaders(testRedactor(), fields)
	assert.Equal(t, "<redacted:X-TRACE>", got[0].Value)
	assert.Equal(t, "nothing interesting here", got[1].Value)
}

// Placeholders depend only on the header name, never on the secret, so
// re-recording the same traffic yields byte-identical cassettes.
func TestSanitizeHeadersPlaceholderIsValueIndependent(t *testing.T) {
	a := sanitizeHeaders(testRedactor(), []hpack.HeaderField{
		{Name: "authorization", Value: "Bearer aaaaaaaaaaaaaaaaaaaaaaaa"},
	})
	b := sanitizeHeaders(testRedactor(), []hpack.HeaderField{
		{Name: "authorization", Value: "Bearer bbbbbbbbbbbbbbbbbbbbbbbb"},
	})
	assert.Equal(t, a[0].Value, b[0].Value)
}

// Word-boundary matching must not over-redact names that merely contain a
// secret-ish substring.
func TestSanitizeHeadersDoesNotOverRedact(t *testing.T) {
	fields := []hpack.HeaderField{
		{Name: "monkey-business", Value: "keep me"},
		{Name: "tokenizer-mode", Value: "keep me too"},
	}
	got := sanitizeHeaders(testRedactor(), fields)
	assert.Equal(t, "keep me", got[0].Value)
	assert.Equal(t, "keep me too", got[1].Value)
}

// The core redactor's word list includes ACCESS, which the retired
// adapter-local sanitizer omitted. Header names containing it as a word are
// therefore redacted now where they previously passed through. This is a
// deliberate widening — it errs toward redacting a non-credential, never
// toward leaking one — and it is pinned here so the change is visible
// rather than incidental.
func TestSanitizeHeadersRedactsAccessWord(t *testing.T) {
	got := sanitizeHeaders(testRedactor(), []hpack.HeaderField{
		{Name: "x-access-count", Value: "17"},
		{Name: "access-control-allow-origin", Value: "*"},
	})
	assert.Equal(t, "<redacted:X-ACCESS-COUNT>", got[0].Value)
	assert.Equal(t, "<redacted:ACCESS-CONTROL-ALLOW-ORIGIN>", got[1].Value)
}

// The core redactor's AWS_ exact/prefix rules reach header names too, which
// the adapter-local sanitizer never covered.
func TestSanitizeHeadersRedactsAWSNames(t *testing.T) {
	got := sanitizeHeaders(testRedactor(), []hpack.HeaderField{
		{Name: "aws-secret-access-key", Value: "wJalrXUtnFEMI/K7MDENG"},
		{Name: "aws-region", Value: "us-east-1"},
	})
	assert.Equal(t, "<redacted:AWS-SECRET-ACCESS-KEY>", got[0].Value)
	// AWS_REGION is on the core benign list: real debugging signal, no secret.
	assert.Equal(t, "us-east-1", got[1].Value)
}

// The adapter now honours the shared XRR_REDACT_* env contract, which the
// adapter-local sanitizer had no way to express.
func TestSanitizeHeadersHonoursAllowList(t *testing.T) {
	r := xrr.NewRedactor(xrr.RedactConfig{Allow: []string{"x-api-key"}})
	got := sanitizeHeaders(r, []hpack.HeaderField{
		{Name: "x-api-key", Value: "deliberately-fake-fixture-value"},
		{Name: "authorization", Value: "Bearer aaaaaaaaaaaaaaaaaaaaaaaa"},
	})
	assert.Equal(t, "deliberately-fake-fixture-value", got[0].Value)
	assert.Equal(t, "<redacted:AUTHORIZATION>", got[1].Value)
}
