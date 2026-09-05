package xrr

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hazardInput exercises every JSON string-escaping class that has forked
// fingerprints across ports: HTML-sensitive & < >, a slash, non-ASCII,
// U+2028/U+2029, the \b and \f short forms, a control byte and DEL.
var hazardInput = "a&b<c>/é" + string(rune(0x2028)) + string(rune(0x2029)) + "\b\f\x1f\x7f"

// {"k":"a&b<c>/é<U+2028><U+2029>\b\f<U+001F escaped><DEL>","stream":"server"}
// — the spec's canonical-JSON hazard vector, pinned byte-for-byte as hex.
const (
	hazardStreamCanonicalHex = "7b226b223a226126623c633e2fc3a9e280a8e280a95c625c665c75303031667f222c2273747265616d223a22736572766572227d"
	hazardStreamFingerprint  = "bcc2c6c3"
)

func TestCanonicalJSONHazardVector(t *testing.T) {
	inputs := map[string]any{"k": hazardInput, "stream": "server"}
	canonical, err := CanonicalJSON(inputs)
	require.NoError(t, err)
	assert.Equal(t, hazardStreamCanonicalHex, hex.EncodeToString(canonical))

	fp, err := CanonicalFingerprint(inputs)
	require.NoError(t, err)
	assert.Equal(t, hazardStreamFingerprint, fp)
}

func TestStreamFingerprintHazardVector(t *testing.T) {
	open := StreamOpen{AdapterID: "x", Type: StreamServer, Identity: map[string]any{"k": hazardInput}}
	fp, err := StreamFingerprint(open, -1)
	require.NoError(t, err)
	assert.Equal(t, hazardStreamFingerprint, fp)
}

func TestCanonicalJSONNoHTMLEscape(t *testing.T) {
	canonical, err := CanonicalJSON("a&b<c>")
	require.NoError(t, err)
	assert.Equal(t, `"a&b<c>"`, string(canonical))
}

func TestUnescapeLineTerminatorsRawSeparators(t *testing.T) {
	ls, ps := string(rune(0x2028)), string(rune(0x2029))
	canonical, err := CanonicalJSON("a" + ls + "b" + ps + "c")
	require.NoError(t, err)
	assert.Equal(t, `"a`+ls+`b`+ps+`c"`, string(canonical))
	assert.False(t, bytes.Contains(canonical, []byte(`\u`)), "encoder escape leaked through")
}

func TestUnescapeLineTerminatorsLeavesLiteralBackslashAlone(t *testing.T) {
	// A source string holding a literal backslash followed by the text
	// u2028 encodes as a backslash pair plus plain digits — the pair is one
	// escape and the digits are text, so the post-pass must not rewrite it.
	src := "\\" + "u2028" + "\\" + "u2029"
	canonical, err := CanonicalJSON(src)
	require.NoError(t, err)
	want := []byte{'"', '\\', '\\', 'u', '2', '0', '2', '8', '\\', '\\', 'u', '2', '0', '2', '9', '"'}
	assert.Equal(t, want, canonical)
}

func TestUnescapeLineTerminatorsIdentityWithoutEscapes(t *testing.T) {
	in := []byte(`{"a":"plain"}`)
	assert.Equal(t, in, unescapeLineTerminators(in))
}
