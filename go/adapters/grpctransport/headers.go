package grpctransport

import (
	"strconv"
	"strings"

	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/codes"
)

// HTTP/2 header handling: sanitization and the gRPC semantics carried in
// pseudo-headers and trailers.
//
// Credentials travel in HEADERS. On a live gRPC connection the request
// header block carries `authorization`, and adopters routinely add their
// own token headers — every one of them would otherwise land in a cassette
// verbatim. Sanitization therefore happens at decode time, before a header
// value can reach any recorder state — see redact.go for the policy and for
// its relationship to the core xrr.Redactor.

// sanitizeHeaders returns fields with credential-bearing values replaced by
// the redactor's deterministic placeholders.
//
// Pseudo-headers (:path, :method, :status, ...) are never redacted: they
// carry protocol routing, never credentials, and :path in particular is a
// fingerprint input — redacting it would break cassette addressing.
//
// The redactor's placeholder depends only on the header NAME, never on the
// secret, so re-recording the same traffic yields byte-identical cassettes.
func sanitizeHeaders(r headerRedactor, fields []hpack.HeaderField) []hpack.HeaderField {
	out := make([]hpack.HeaderField, len(fields))
	for i, f := range fields {
		out[i] = f
		if strings.HasPrefix(f.Name, ":") {
			continue
		}
		if v, redacted := r.redactField(f.Name, f.Value); redacted {
			out[i].Value = v
		}
	}
	return out
}

// headerValue returns the first value for name, or "".
func headerValue(fields []hpack.HeaderField, name string) string {
	for _, f := range fields {
		if f.Name == name {
			return f.Value
		}
	}
	return ""
}

// hasHeader reports whether name is present at all.
func hasHeader(fields []hpack.HeaderField, name string) bool {
	for _, f := range fields {
		if f.Name == name {
			return true
		}
	}
	return false
}

// splitPath splits a gRPC :path pseudo-header ("/pkg.Service/Method") into
// its service and method identifiers. This is the only place on the wire
// where the RPC's identity appears, which is why HPACK decoding is
// mandatory rather than optional for this adapter.
func splitPath(path string) (service, method string, ok bool) {
	s := strings.TrimPrefix(path, "/")
	i := strings.LastIndex(s, "/")
	if i <= 0 || i == len(s)-1 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

// grpcStatus extracts the terminal status from a server header block
// (trailers, or a Trailers-Only response). Absent grpc-status means the
// block is the initial response HEADERS, not the terminal.
func grpcStatus(fields []hpack.HeaderField) (code codes.Code, msg string, ok bool) {
	raw := headerValue(fields, "grpc-status")
	if raw == "" && !hasHeader(fields, "grpc-status") {
		return codes.OK, "", false
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return codes.Unknown, headerValue(fields, "grpc-message"), true
	}
	return codes.Code(n), decodeGRPCMessage(headerValue(fields, "grpc-message")), true
}

// decodeGRPCMessage undoes gRPC's percent-encoding of the grpc-message
// trailer. gRPC restricts the header to ASCII and percent-escapes the rest,
// so the recorded error text matches what a client library would surface.
func decodeGRPCMessage(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+3], 16, 8); err == nil {
				b.WriteByte(byte(v))
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// isCompressed reports whether the header block negotiates per-message
// compression. Compressed messages are refused rather than recorded: the
// cassette format stores decoded message bytes, and the compressed form is
// codec- and level-dependent, so recording it would produce cassettes that
// only replay against the exact compressor that made them.
func isCompressed(fields []hpack.HeaderField) bool {
	enc := headerValue(fields, "grpc-encoding")
	return enc != "" && enc != "identity"
}
