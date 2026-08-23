package grpctransport

import (
	"encoding/binary"
	"testing"

	"golang.org/x/net/http2/hpack"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gRPC message reassembly is what makes DATA frame boundaries irrelevant to
// the recording. These tests pin the two cases that make transport capture
// stable across runs: one logical message split over several frames, and
// several messages packed into one frame. Both are legal and both happen —
// framing is chosen by flow control, not by the application.

// framed wraps payload in the gRPC 5-byte length prefix.
func framed(payload []byte) []byte {
	out := make([]byte, grpcMsgPrefixLen+len(payload))
	binary.BigEndian.PutUint32(out[1:5], uint32(len(payload)))
	copy(out[grpcMsgPrefixLen:], payload)
	return out
}

func TestStreamDecoderReassemblesSplitMessage(t *testing.T) {
	msg := []byte("a message that arrives in pieces")
	wire := framed(msg)

	var d streamDecoder
	// Feed one byte at a time: the worst-case fragmentation.
	for i := 0; i < len(wire)-1; i++ {
		got, err := d.feed(wire[i : i+1])
		require.NoError(t, err)
		assert.Empty(t, got, "no message may complete before its last byte")
	}
	got, err := d.feed(wire[len(wire)-1:])
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, msg, got[0])
}

func TestStreamDecoderSplitsPackedMessages(t *testing.T) {
	a, b, c := []byte("first"), []byte(""), []byte("third")
	wire := append(append(framed(a), framed(b)...), framed(c)...)

	var d streamDecoder
	got, err := d.feed(wire)
	require.NoError(t, err)
	require.Len(t, got, 3, "every message packed into one frame must emerge")
	assert.Equal(t, a, got[0])
	assert.Equal(t, b, got[1], "an empty message is a real message")
	assert.Equal(t, c, got[2])
}

// A trailing partial message must stay buffered rather than being emitted
// truncated — truncation would corrupt the cassette silently.
func TestStreamDecoderBuffersTrailingPartial(t *testing.T) {
	full := framed([]byte("complete"))
	partial := framed([]byte("incomplete"))[:4]

	var d streamDecoder
	got, err := d.feed(append(full, partial...))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []byte("complete"), got[0])
}

// An absurd length prefix must be rejected rather than driving a huge
// allocation.
func TestStreamDecoderRejectsOversizedLength(t *testing.T) {
	bad := make([]byte, grpcMsgPrefixLen)
	binary.BigEndian.PutUint32(bad[1:5], maxGRPCMessageSize+1)

	var d streamDecoder
	_, err := d.feed(bad)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds cap")
}

// splitPath is how service and method are recovered from the wire; a
// malformed :path must be rejected rather than producing a half-identity
// that would file a cassette under a wrong fingerprint.
func TestSplitPath(t *testing.T) {
	service, method, ok := splitPath("/files.FileService/Download")
	require.True(t, ok)
	assert.Equal(t, "files.FileService", service)
	assert.Equal(t, "Download", method)

	for _, bad := range []string{"", "/", "/onlyservice", "/svc/", "//method"} {
		_, _, ok := splitPath(bad)
		assert.False(t, ok, "%q must be rejected", bad)
	}
}

func TestIsCompressed(t *testing.T) {
	assert.False(t, isCompressed(nil))
	assert.False(t, isCompressed(hdr("grpc-encoding", "identity")))
	assert.True(t, isCompressed(hdr("grpc-encoding", "gzip")))
}

func TestGRPCStatusDecoding(t *testing.T) {
	// No grpc-status: this header block is the initial response, not the
	// terminal.
	_, _, ok := grpcStatus(hdr("content-type", "application/grpc"))
	assert.False(t, ok)

	code, msg, ok := grpcStatus(append(hdr("grpc-status", "10"),
		hdr("grpc-message", "exec failed: exit status 3")...))
	require.True(t, ok)
	assert.EqualValues(t, 10, code)
	assert.Equal(t, "exec failed: exit status 3", msg)
}

// grpc-message is percent-encoded on the wire; the recorded error text must
// match what a client library surfaces.
func TestGRPCMessagePercentDecoding(t *testing.T) {
	_, msg, ok := grpcStatus(append(hdr("grpc-status", "2"),
		hdr("grpc-message", "bad%20input%3A%20caf%C3%A9")...))
	require.True(t, ok)
	assert.Equal(t, "bad input: café", msg)
}

func TestEncodeDecodeGRPCMessageRoundTrip(t *testing.T) {
	for _, s := range []string{"plain", "with space", "café", "100% done", "tab\there"} {
		assert.Equal(t, s, decodeGRPCMessage(encodeGRPCMessage(s)), "round trip of %q", s)
	}
}

// hdr is a one-field header block helper.
func hdr(name, value string) []hpack.HeaderField {
	return []hpack.HeaderField{{Name: name, Value: value}}
}
