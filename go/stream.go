package xrr

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

// Streamed-interaction model + format-layer rules.
// See spec/cassette-format-streaming.md.

// StreamType is the direction shape of a streamed interaction.
type StreamType string

const (
	StreamServer StreamType = "server"
	StreamClient StreamType = "client"
	StreamBidi   StreamType = "bidi"
)

// ErrShapeMismatch is returned when a streamed request meets a unary
// cassette or a unary request meets a streamed cassette. Distinct from
// ErrCassetteMiss: the pair exists but has the wrong shape.
var ErrShapeMismatch = errors.New("xrr: cassette shape mismatch")

// ErrStreamMismatch is the errors.Is target for replay-time stream
// mismatches (byte-divergent sends, short half-close). The concrete error
// is always a *StreamMismatchError.
var ErrStreamMismatch = errors.New("xrr: stream mismatch")

// StreamMismatchError reports a replay-time divergence between the client's
// send-side behaviour and the recording. Mismatch is terminal: every
// subsequent operation on the stream returns the same error.
type StreamMismatchError struct {
	Op      string // "send" or "half_close"
	Ordinal int    // 0-based ordinal of the offending client operation
	Detail  string // expected vs actual (message content identified by sha256)
}

func (e *StreamMismatchError) Error() string {
	return fmt.Sprintf("xrr: stream mismatch: %s %d: %s", e.Op, e.Ordinal, e.Detail)
}

// Is makes errors.Is(err, ErrStreamMismatch) match.
func (e *StreamMismatchError) Is(target error) bool { return target == ErrStreamMismatch }

// StreamFrame is one message event in a stream direction. Message always
// holds the decoded bytes; Text records whether the source (or preferred)
// on-disk encoding is message_text rather than message_b64. Hashing and
// comparison operate on the decoded bytes, so the encodings are
// interchangeable and Text is advisory for writers only.
type StreamFrame struct {
	Seq     int
	Message []byte
	Text    bool
	AtMs    *int64 // milliseconds since stream open; nil when absent
}

// StreamEvent is a positioned scalar event (half_close, end).
type StreamEvent struct {
	Seq  int
	AtMs *int64
}

// ReqStream is the client→server half of a streamed interaction.
type ReqStream struct {
	Type      StreamType
	Frames    []StreamFrame
	HalfClose *StreamEvent // nil ⇒ the stream terminated before the client half-closed
}

// RespStream is the server→client half of a streamed interaction.
type RespStream struct {
	Frames []StreamFrame
	End    StreamEvent // the terminal event; every recorded stream has exactly one
}

// StreamPair is one fully loaded streamed interaction (req + resp files).
type StreamPair struct {
	Req         ReqStream
	Resp        RespStream
	ReqPayload  map[string]any // adapter-defined open request
	RespPayload map[string]any // adapter-defined terminal response
	RecordedAt  string         // RFC3339 UTC; preserved on re-emit, filled when empty
	RecordedErr string         // resp envelope error field; non-empty ⇔ error terminal
}

// ValidateStreamPair enforces the model-level validation rules: known type,
// per-file strictly ascending frames, no duplicate seq across the pair
// (frames, half_close, end), non-negative seq, end.seq maximal. Sparse
// numbering is accepted (readers MAY; writers never produce it).
// Parse-level rules (missing seq, message encoding, base64 strictness) are
// enforced by LoadStream, which cannot represent violations in this model.
func ValidateStreamPair(p *StreamPair) error {
	switch p.Req.Type {
	case StreamServer, StreamClient, StreamBidi:
	default:
		return fmt.Errorf("xrr: stream type %q invalid (want server|client|bidi)", p.Req.Type)
	}

	seen := make(map[int]struct{})
	maxSeq := -1
	add := func(seq int, what string) error {
		if seq < 0 {
			return fmt.Errorf("xrr: %s seq %d is negative", what, seq)
		}
		if _, dup := seen[seq]; dup {
			return fmt.Errorf("xrr: duplicate seq %d across pair (%s)", seq, what)
		}
		seen[seq] = struct{}{}
		if seq > maxSeq {
			maxSeq = seq
		}
		return nil
	}
	addFrames := func(frames []StreamFrame, side string) error {
		prev := -1
		for i, f := range frames {
			if err := add(f.Seq, fmt.Sprintf("%s frame %d", side, i)); err != nil {
				return err
			}
			if f.Seq <= prev {
				return fmt.Errorf("xrr: %s frames not strictly ascending at index %d", side, i)
			}
			prev = f.Seq
		}
		return nil
	}

	if err := addFrames(p.Req.Frames, "req"); err != nil {
		return err
	}
	if err := addFrames(p.Resp.Frames, "resp"); err != nil {
		return err
	}
	if p.Req.HalfClose != nil {
		if err := add(p.Req.HalfClose.Seq, "half_close"); err != nil {
			return err
		}
	}
	if err := add(p.Resp.End.Seq, "end"); err != nil {
		return err
	}
	if p.Resp.End.Seq != maxSeq {
		return fmt.Errorf("xrr: end.seq %d is not the maximum seq %d", p.Resp.End.Seq, maxSeq)
	}
	return nil
}

// StreamOpen identifies a streamed interaction at open time — everything a
// replay needs to locate the cassette before any frames exist. Message is
// the single request message for server streams (available at open,
// mirroring unary); client/bidi opens carry no message and are
// disambiguated by the session's occurrence counter instead.
type StreamOpen struct {
	AdapterID string
	Type      StreamType
	Service   string
	Method    string
	Message   []byte // server streams only
}

// StreamFingerprint computes the streaming fingerprint for an open:
// sha256(canonical_json)[:8] over inputs that always include a "stream"
// discriminator, keeping the streaming fingerprint space disjoint from the
// unary one. Server streams are content-addressed via
// msg_hash = sha256(message_bytes)[:8]; client/bidi include the 0-based
// occurrence ordinal n (ignored for server streams).
func StreamFingerprint(open StreamOpen, n int) (string, error) {
	inputs := map[string]any{
		"method":  open.Method,
		"service": open.Service,
		"stream":  string(open.Type),
	}
	switch open.Type {
	case StreamServer:
		sum := sha256.Sum256(open.Message)
		inputs["msg_hash"] = hex.EncodeToString(sum[:4])
	case StreamClient, StreamBidi:
		if n < 0 {
			return "", fmt.Errorf("xrr: stream occurrence n must be >= 0, got %d", n)
		}
		inputs["n"] = n
	default:
		return "", fmt.Errorf("xrr: stream type %q invalid (want server|client|bidi)", open.Type)
	}
	// json.Marshal sorts map keys lexicographically and emits no
	// insignificant whitespace — exactly the spec's canonical JSON.
	canonical, err := json.Marshal(inputs)
	if err != nil {
		return "", fmt.Errorf("xrr: stream fingerprint marshal: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:4]), nil
}

// decodeStrictB64 decodes standard base64 (RFC 4648, with padding),
// rejecting any character outside the base64 alphabet — including the
// whitespace that Go's decoder (like several other languages') silently
// ignores by default.
func decodeStrictB64(s string) ([]byte, error) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		valid := (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '+' || c == '/' || c == '='
		if !valid {
			return nil, fmt.Errorf("invalid base64 character %q at index %d", c, i)
		}
	}
	return base64.StdEncoding.Strict().DecodeString(s)
}
