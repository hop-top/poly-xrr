package xrr

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// Streamed cassette IO: parse/emit of the v1-additive `stream` envelope
// field. Unary cassettes are untouched — they keep the v1 write path
// byte-for-byte and are rejected here with ErrShapeMismatch.

// StreamCassette is implemented by cassettes that support streamed
// interactions. FileCassette implements it.
type StreamCassette interface {
	LoadStream(adapterID, fingerprint string) (*StreamPair, error)
	SaveStream(adapterID, fingerprint string, pair *StreamPair) error
}

// ── read side ──────────────────────────────────────────────────────────────

// wire structs capture the on-disk stream schema. Message scalars are kept
// as yaml.Node so they can be decoded resolution-blind: a hand-authored
// unquoted `on`, `12:30`, or `null` must still read as exactly that string.
// Unknown extra fields are ignored (forward compat, same policy as the
// envelope).
type wireStream struct {
	Type      *string     `yaml:"type"`
	Frames    []wireFrame `yaml:"frames"`
	HalfClose *wireEvent  `yaml:"half_close"`
	End       *wireEvent  `yaml:"end"`
}

type wireFrame struct {
	Seq         *int      `yaml:"seq"`
	MessageB64  yaml.Node `yaml:"message_b64"`
	MessageText yaml.Node `yaml:"message_text"`
	AtMs        *int64    `yaml:"at_ms"`
}

type wireEvent struct {
	Seq  *int   `yaml:"seq"`
	AtMs *int64 `yaml:"at_ms"`
}

// streamFile is one parsed envelope of a streamed pair.
type streamFile struct {
	payload     map[string]any
	recordedAt  string
	recordedErr string
	stream      *wireStream // nil when the file carries no stream field
}

// LoadStream reads a streamed req/resp pair, parses the stream halves, and
// enforces the spec's validation rules. Returns ErrCassetteMiss when no
// pair exists and ErrShapeMismatch when the pair is unary (no stream field
// on either file).
func (c *FileCassette) LoadStream(adapterID, fingerprint string) (*StreamPair, error) {
	reqFile, err := c.readStreamFile(adapterID, fingerprint, "req")
	if err != nil {
		return nil, err
	}
	respFile, err := c.readStreamFile(adapterID, fingerprint, "resp")
	if err != nil {
		return nil, err
	}

	if reqFile.stream == nil && respFile.stream == nil {
		return nil, fmt.Errorf("xrr: unary cassette pair in streaming code path: %w", ErrShapeMismatch)
	}
	if reqFile.stream == nil || respFile.stream == nil {
		return nil, fmt.Errorf("xrr: stream field present on one file of the pair but not the other")
	}

	pair := &StreamPair{
		ReqPayload:  reqFile.payload,
		RespPayload: respFile.payload,
		RecordedAt:  reqFile.recordedAt,
		RecordedErr: respFile.recordedErr,
	}

	if reqFile.stream.Type == nil {
		return nil, fmt.Errorf("xrr: req stream missing type")
	}
	pair.Req.Type = StreamType(*reqFile.stream.Type)
	if pair.Req.Frames, err = parseWireFrames(reqFile.stream.Frames, "req"); err != nil {
		return nil, err
	}
	if pair.Req.HalfClose, err = parseWireEvent(reqFile.stream.HalfClose, "half_close", false); err != nil {
		return nil, err
	}

	if pair.Resp.Frames, err = parseWireFrames(respFile.stream.Frames, "resp"); err != nil {
		return nil, err
	}
	end, err := parseWireEvent(respFile.stream.End, "end", true)
	if err != nil {
		return nil, err
	}
	pair.Resp.End = *end

	if err := ValidateStreamPair(pair); err != nil {
		return nil, err
	}
	return pair, nil
}

func (c *FileCassette) readStreamFile(adapterID, fingerprint, kind string) (*streamFile, error) {
	path := filepath.Join(c.dir, fmt.Sprintf("%s-%s.%s.yaml", adapterID, fingerprint, kind))
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrCassetteMiss
		}
		return nil, fmt.Errorf("xrr: read %s: %w", kind, err)
	}

	var raw map[string]yaml.Node
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("xrr: unmarshal envelope %s: %w", kind, err)
	}

	f := &streamFile{}
	payloadNode, ok := raw["payload"]
	if !ok {
		return nil, fmt.Errorf("xrr: missing payload in %s", kind)
	}
	if err := payloadNode.Decode(&f.payload); err != nil {
		return nil, fmt.Errorf("xrr: decode payload %s: %w", kind, err)
	}
	if f.payload == nil {
		// v1 requires payload to be a non-null object; tolerate a null on
		// read by normalizing, mirroring the write-side rule.
		f.payload = map[string]any{}
	}
	// Resolution-blind scalar reads: a hand-authored unquoted all-digit
	// fingerprint or timestamp must not corrupt the string value.
	if node, ok := raw["recorded_at"]; ok && node.Kind == yaml.ScalarNode {
		f.recordedAt = node.Value
	}
	if node, ok := raw["error"]; ok && node.Kind == yaml.ScalarNode && node.Tag != "!!null" {
		f.recordedErr = node.Value
	}
	if node, ok := raw["stream"]; ok {
		var ws wireStream
		if err := node.Decode(&ws); err != nil {
			return nil, fmt.Errorf("xrr: decode stream %s: %w", kind, err)
		}
		f.stream = &ws
	}
	return f, nil
}

func parseWireFrames(frames []wireFrame, side string) ([]StreamFrame, error) {
	if len(frames) == 0 {
		return nil, nil // absent key and [] both read as empty
	}
	out := make([]StreamFrame, 0, len(frames))
	for i, wf := range frames {
		if wf.Seq == nil {
			return nil, fmt.Errorf("xrr: %s frame %d missing seq", side, i)
		}
		b64Present := wf.MessageB64.Kind != 0
		textPresent := wf.MessageText.Kind != 0
		if b64Present == textPresent {
			return nil, fmt.Errorf("xrr: %s frame %d must have exactly one of message_b64/message_text", side, i)
		}
		frame := StreamFrame{Seq: *wf.Seq, AtMs: wf.AtMs}
		if b64Present {
			if wf.MessageB64.Kind != yaml.ScalarNode {
				return nil, fmt.Errorf("xrr: %s frame %d message_b64 is not a scalar", side, i)
			}
			msg, err := decodeStrictB64(wf.MessageB64.Value)
			if err != nil {
				return nil, fmt.Errorf("xrr: %s frame %d message_b64: %w", side, i, err)
			}
			frame.Message = msg
		} else {
			if wf.MessageText.Kind != yaml.ScalarNode {
				return nil, fmt.Errorf("xrr: %s frame %d message_text is not a scalar", side, i)
			}
			// Node.Value is the scalar's string content regardless of tag
			// resolution — `on` stays "on", `12:30` stays "12:30".
			frame.Message = []byte(wf.MessageText.Value)
			frame.Text = true
		}
		out = append(out, frame)
	}
	return out, nil
}

func parseWireEvent(we *wireEvent, what string, required bool) (*StreamEvent, error) {
	if we == nil {
		if required {
			return nil, fmt.Errorf("xrr: stream missing %s", what)
		}
		return nil, nil
	}
	if we.Seq == nil {
		return nil, fmt.Errorf("xrr: %s missing seq", what)
	}
	return &StreamEvent{Seq: *we.Seq, AtMs: we.AtMs}, nil
}

// ── write side ─────────────────────────────────────────────────────────────

// quotedScalar marshals as a double-quoted YAML scalar. The spec mandates
// quoting for fingerprint (all-digit forms otherwise parse as integers) and
// message_text (unquoted `on`/`12:30`/`null` corrupt under YAML 1.1
// readers).
type quotedScalar string

// MarshalYAML implements yaml.Marshaler.
func (q quotedScalar) MarshalYAML() (any, error) {
	return &yaml.Node{Kind: yaml.ScalarNode, Style: yaml.DoubleQuotedStyle, Value: string(q)}, nil
}

// streamEnvelope is the on-disk wrapper for streamed req and resp files.
// Field order mirrors the unary envelope with stream appended.
type streamEnvelope struct {
	XRR         string       `yaml:"xrr"`
	Adapter     string       `yaml:"adapter"`
	Fingerprint quotedScalar `yaml:"fingerprint"`
	RecordedAt  quotedScalar `yaml:"recorded_at"`
	Error       string       `yaml:"error,omitempty"`
	Payload     any          `yaml:"payload"`
	Stream      any          `yaml:"stream"`
}

type wireReqStreamOut struct {
	Type      StreamType     `yaml:"type"`
	Frames    []wireFrameOut `yaml:"frames"`
	HalfClose *wireEventOut  `yaml:"half_close,omitempty"`
}

type wireRespStreamOut struct {
	Frames []wireFrameOut `yaml:"frames"`
	End    wireEventOut   `yaml:"end"`
}

type wireFrameOut struct {
	Seq         int           `yaml:"seq"`
	MessageB64  *quotedScalar `yaml:"message_b64,omitempty"`
	MessageText *quotedScalar `yaml:"message_text,omitempty"`
	AtMs        *int64        `yaml:"at_ms,omitempty"`
}

type wireEventOut struct {
	Seq  int    `yaml:"seq"`
	AtMs *int64 `yaml:"at_ms,omitempty"`
}

// SaveStream validates and writes a streamed pair as two YAML files. The
// message-encoding choice follows each frame's Text flag: message_text for
// Text frames whose bytes are valid UTF-8 (emitted quoted), message_b64
// otherwise. RecordedAt is preserved when set, else stamped now (UTC).
func (c *FileCassette) SaveStream(adapterID, fingerprint string, pair *StreamPair) error {
	if pair == nil {
		return fmt.Errorf("xrr: nil stream pair")
	}
	if err := ValidateStreamPair(pair); err != nil {
		return err
	}
	recordedAt := pair.RecordedAt
	if recordedAt == "" {
		recordedAt = time.Now().UTC().Format(time.RFC3339)
	}

	reqEnv := streamEnvelope{
		XRR:         "1",
		Adapter:     adapterID,
		Fingerprint: quotedScalar(fingerprint),
		RecordedAt:  quotedScalar(recordedAt),
		Payload:     normalizePayload(pair.ReqPayload),
		Stream: wireReqStreamOut{
			Type:      pair.Req.Type,
			Frames:    framesOut(pair.Req.Frames),
			HalfClose: eventOutPtr(pair.Req.HalfClose),
		},
	}
	if err := c.writeStreamFile(adapterID, fingerprint, "req", reqEnv); err != nil {
		return err
	}

	respEnv := streamEnvelope{
		XRR:         "1",
		Adapter:     adapterID,
		Fingerprint: quotedScalar(fingerprint),
		RecordedAt:  quotedScalar(recordedAt),
		Error:       pair.RecordedErr,
		Payload:     normalizePayload(pair.RespPayload),
		Stream: wireRespStreamOut{
			Frames: framesOut(pair.Resp.Frames),
			End:    wireEventOut{Seq: pair.Resp.End.Seq, AtMs: pair.Resp.End.AtMs},
		},
	}
	return c.writeStreamFile(adapterID, fingerprint, "resp", respEnv)
}

func (c *FileCassette) writeStreamFile(adapterID, fingerprint, kind string, env streamEnvelope) error {
	data, err := yaml.Marshal(env)
	if err != nil {
		return fmt.Errorf("xrr: marshal %s: %w", kind, err)
	}
	path := filepath.Join(c.dir, fmt.Sprintf("%s-%s.%s.yaml", adapterID, fingerprint, kind))
	return os.WriteFile(path, data, 0o644)
}

// framesOut encodes frames for emit. A nil slice marshals as `frames: []`,
// satisfying the explicit-empty rule.
func framesOut(frames []StreamFrame) []wireFrameOut {
	if len(frames) == 0 {
		return nil
	}
	out := make([]wireFrameOut, 0, len(frames))
	for _, f := range frames {
		wf := wireFrameOut{Seq: f.Seq, AtMs: f.AtMs}
		if f.Text && utf8.Valid(f.Message) {
			text := quotedScalar(f.Message)
			wf.MessageText = &text
		} else {
			b64 := quotedScalar(base64.StdEncoding.EncodeToString(f.Message))
			wf.MessageB64 = &b64
		}
		out = append(out, wf)
	}
	return out
}

func eventOutPtr(e *StreamEvent) *wireEventOut {
	if e == nil {
		return nil
	}
	return &wireEventOut{Seq: e.Seq, AtMs: e.AtMs}
}
