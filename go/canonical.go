package xrr

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// CanonicalJSON encodes v as the spec's canonical JSON: lexicographically
// sorted object keys, no insignificant whitespace, and RFC 8785 §3.2.2.2
// string serialization — only `"`, `\` and U+0000–U+001F are escaped
// (`\b \t \n \f \r` as their short forms, the rest as lowercase `\u00xx`);
// every other code point, including `& < > /`, non-ASCII, U+007F and
// U+2028/U+2029, is emitted as raw UTF-8. Every fingerprint in this module
// hashes CanonicalJSON output, which is what keeps the bytes — and so the
// cassette filenames — identical across the ports.
//
// encoding/json needs two corrections to reach that form: SetEscapeHTML
// (false) turns off the HTML-safe `& < >` escapes, and
// unescapeLineTerminators undoes the `\u`-escapes the encoder applies to U+2028/U+2029
// unconditionally (there is no option to disable them).
func CanonicalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return unescapeLineTerminators(bytes.TrimSuffix(buf.Bytes(), []byte("\n"))), nil
}

// CanonicalFingerprint returns the v1 fingerprint of v:
// sha256(CanonicalJSON(v))[:8] as lowercase hex.
func CanonicalFingerprint(v any) (string, error) {
	canonical, err := CanonicalJSON(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:4]), nil
}

var (
	rawLineSeparator      = []byte(string(rune(0x2028)))
	rawParagraphSeparator = []byte(string(rune(0x2029)))
)

// unescapeLineTerminators rewrites the six-byte `\u`-escapes that encoding/json
// emits for U+2028 and U+2029 back to the raw code points. It
// walks escape sequences left to right and copies every other escape as a
// two-byte unit, so a literal backslash in the source string (encoded as
// `\\`) followed by the text u2028 is left untouched.
func unescapeLineTerminators(b []byte) []byte {
	if bytes.IndexByte(b, '\\') < 0 {
		return b
	}
	out := make([]byte, 0, len(b))
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' || i+1 >= len(b) {
			out = append(out, b[i])
			continue
		}
		if i+6 <= len(b) && b[i+1] == 'u' && b[i+2] == '2' && b[i+3] == '0' && b[i+4] == '2' {
			switch b[i+5] {
			case '8':
				out = append(out, rawLineSeparator...)
				i += 5
				continue
			case '9':
				out = append(out, rawParagraphSeparator...)
				i += 5
				continue
			}
		}
		out = append(out, b[i], b[i+1])
		i++
	}
	return out
}
