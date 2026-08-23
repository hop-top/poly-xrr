package grpc

import (
	"bytes"
	"testing"

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
