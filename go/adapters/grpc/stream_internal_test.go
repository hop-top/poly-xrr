package grpc

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	xrr "hop.top/xrr"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestMarshalMessageDeterministicMapOrdering — protobuf gives map entries no
// guaranteed wire order, and plain proto.Marshal follows Go's randomized map
// iteration, so a map-carrying message can marshal to different bytes on
// every call. That breaks the format's byte-level contracts: server-stream
// fingerprints (content-addressed by the open message) miss on replay, and
// client/bidi send validation mismatches spuriously. marshalMessage must
// therefore produce identical bytes across calls — pinned here by marshaling
// the same ≥3-entry map message repeatedly, which fails near-certainly under
// nondeterministic map iteration.
func TestMarshalMessageDeterministicMapOrdering(t *testing.T) {
	msg, err := structpb.NewStruct(map[string]any{
		"PATH": "/usr/local/bin:/usr/bin",
		"HOME": "/home/dev",
		"LANG": "C.UTF-8",
		"TERM": "xterm-256color",
	})
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}

	first, err := marshalMessage(msg)
	if err != nil {
		t.Fatalf("marshalMessage: %v", err)
	}
	for i := 0; i < 100; i++ {
		b, mErr := marshalMessage(msg)
		if mErr != nil {
			t.Fatalf("marshalMessage (call %d): %v", i+2, mErr)
		}
		if !bytes.Equal(first, b) {
			t.Fatalf("marshalMessage is nondeterministic: call %d produced different bytes for the same map-carrying message", i+2)
		}
	}

	// The stable form must be the deterministic (sorted-map-key) marshal, so
	// bytes also agree across processes — record and replay never share one.
	want, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		t.Fatalf("deterministic reference marshal: %v", err)
	}
	if !bytes.Equal(first, want) {
		t.Fatal("marshalMessage bytes differ from proto.MarshalOptions{Deterministic: true} output")
	}
}

// TestStreamOpenServerIdentityUsesScrubbedBytes — the server-stream msg_hash
// is the one identity input derived from message bytes, and it is computed
// adapter-side before the core's frame seam. It must therefore be derived
// from the session-scrubbed bytes, in record and replay mode alike, so a
// scrubbed recording and a scrubbed replay of the same live open message
// address the same cassette.
func TestStreamOpenServerIdentityUsesScrubbedBytes(t *testing.T) {
	secret := []byte("do:{token=FAKE-SECRET-0123456789}")
	scrub := func(_ xrr.StreamDirection, _ xrr.StreamScrubInfo, data []byte) []byte {
		return bytes.ReplaceAll(data, []byte("FAKE-SECRET-0123456789"), []byte("XXXXXXXXXXXXXXXXXXXXXX"))
	}
	hashOf := func(b []byte) string {
		sum := sha256.Sum256(b)
		return hex.EncodeToString(sum[:4])
	}

	rec := xrr.NewSessionWithStreamScrub(xrr.ModeRecord, nil, scrub)
	rep := xrr.NewSessionWithStreamScrub(xrr.ModeReplay, nil, scrub)
	scrubbed := scrub(xrr.StreamSend, xrr.StreamScrubInfo{}, secret)

	for name, session := range map[string]*xrr.FileSession{"record": rec, "replay": rep} {
		open := streamOpen(session, xrr.StreamServer, "ops.Deploy", "Run", secret)
		if got := open.Identity["msg_hash"]; got != hashOf(scrubbed) {
			t.Fatalf("%s: msg_hash = %v, want hash of scrubbed bytes %s", name, got, hashOf(scrubbed))
		}
		if got := open.Identity["msg_hash"]; got == hashOf(secret) {
			t.Fatalf("%s: msg_hash derived from raw secret-bearing bytes", name)
		}
	}

	// No hook installed: identity is the raw-bytes hash (default unchanged).
	plain := xrr.NewSession(xrr.ModeRecord, nil)
	open := streamOpen(plain, xrr.StreamServer, "ops.Deploy", "Run", secret)
	if got := open.Identity["msg_hash"]; got != hashOf(secret) {
		t.Fatalf("no hook: msg_hash = %v, want raw hash %s", got, hashOf(secret))
	}
}
