// Package grpctransport records and replays streamed gRPC RPCs at the
// TRANSPORT layer, by wrapping the connection a gRPC client dials rather
// than by intercepting its calls.
//
// # Why this exists
//
// The interceptor-based adapter (hop.top/xrr/adapters/grpc) attaches via
// grpc.StreamClientInterceptor and is the right tool whenever a client
// library lets you pass one. Some do not. A library may build an internal
// connection with no interceptor options exposed at all, which puts the
// most interesting half of its API — the streamed calls — permanently out
// of reach of interceptor-based capture.
//
// This package attaches one level lower, at grpc.WithContextDialer, which
// is a plain func(context.Context, string) (net.Conn, error). Any client
// that dials at all can be given one. The connection is tapped passively,
// its HTTP/2 frames decoded, and the gRPC messages reconstructed.
//
// # What it produces
//
// Ordinary streaming cassettes. The reconstructed messages are the same
// bytes the interceptor adapter would record, filed under the same
// fingerprints via the same core stream session API, in the format defined
// by spec/cassette-format-streaming.md. A cassette recorded here replays
// through the interceptor adapter and vice versa.
//
// # Limits
//
//   - Streamed RPCs only, like the interceptor streaming adapter. Unary
//     RPCs keep the v1 unary format and are ignored by the tap.
//   - Cleartext HTTP/2 only. A TLS connection is opaque at the dialer seam:
//     the dialer returns the raw socket and gRPC negotiates TLS on top, so
//     the tap sees ciphertext. Record against a cleartext endpoint, or
//     terminate TLS below the tap.
//   - Uncompressed messages only. grpc-encoding other than identity is
//     refused rather than silently mis-recorded.
//   - The client must let you supply the dialer. This seam is far more
//     commonly available than an interceptor, but it is not universal: a
//     library that hardcodes its own grpc.NewClient options exposes
//     neither. Such a client needs an upstream change (accept a
//     grpc.DialOption, or honour one), a proxy in front of it, or capture
//     at a different layer entirely.
//
// # HTTP/2 version sensitivity
//
// Decoding is tied to HTTP/2 framing and to gRPC's length-prefixed message
// format, both of which are stable, versioned wire protocols — not to any
// grpc-go internals. Upgrading grpc-go does not affect this package.
// Frame types this adapter does not understand are skipped as noise rather
// than treated as errors, so protocol extensions degrade gracefully.
// A move to HTTP/3 (QUIC) would be a different transport and would need a
// separate decoder; nothing here would silently mis-record it, because the
// preface and framing would not parse.
package grpctransport

import (
	"crypto/sha256"
	"encoding/hex"
)

// adapterID matches the interceptor-based gRPC adapter's id, deliberately:
// both produce the same cassette files for the same traffic, so they share
// one adapter namespace on disk.
const adapterID = "grpc"

// msgHash is the v1 content-address building block:
// sha256(message_bytes)[:8], as lowercase hex.
func msgHash(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:4])
}
