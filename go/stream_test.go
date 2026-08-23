package xrr_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	xrr "hop.top/xrr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamFingerprintSpecVectors cross-checks the six embedded spec vectors
// from spec/cassette-format-streaming.md (msg_hash building blocks plus all
// four fingerprint rows of the test-vector table).
func TestStreamFingerprintSpecVectors(t *testing.T) {
	hosts := []byte(`{"path":"/etc/hosts"}`)
	biglog := []byte(`{"path":"/var/log/big.log"}`)

	// msg_hash building block: sha256(message_bytes)[:8].
	h1 := sha256.Sum256(hosts)
	assert.Equal(t, "f1e315a5", hex.EncodeToString(h1[:4]))
	h2 := sha256.Sum256(biglog)
	assert.Equal(t, "164658bd", hex.EncodeToString(h2[:4]))

	cases := []struct {
		name string
		open xrr.StreamOpen
		n    int
		want string
	}{
		{
			name: "server hosts",
			open: grpcStreamOpen(xrr.StreamServer, "files.FileService", "Download", hosts),
			want: "58a4bf3f",
		},
		{
			name: "server biglog",
			open: grpcStreamOpen(xrr.StreamServer, "files.FileService", "Download", biglog),
			want: "9e8c4d4c",
		},
		{
			name: "client upload n0",
			open: grpcStreamOpen(xrr.StreamClient, "files.FileService", "Upload", nil),
			n:    0,
			want: "2bebfd6f",
		},
		{
			name: "bidi converse n0",
			open: grpcStreamOpen(xrr.StreamBidi, "chat.ChatService", "Converse", nil),
			n:    0,
			want: "c6233d2e",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fp, err := xrr.StreamFingerprint(tc.open, tc.n)
			require.NoError(t, err)
			assert.Equal(t, tc.want, fp)
		})
	}
}

func TestStreamFingerprintInvalidInputs(t *testing.T) {
	_, err := xrr.StreamFingerprint(xrr.StreamOpen{Type: "duplex", Identity: map[string]any{"service": "s"}}, 0)
	assert.Error(t, err, "unknown stream type")
	_, err = xrr.StreamFingerprint(grpcStreamOpen(xrr.StreamClient, "s", "m", nil), -1)
	assert.Error(t, err, "counter-addressed open without an ordinal")
	for _, reserved := range []string{"stream", "n"} {
		_, err = xrr.StreamFingerprint(xrr.StreamOpen{
			Type: xrr.StreamServer, Identity: map[string]any{reserved: "x"},
		}, 0)
		assert.Error(t, err, "identity key %q is reserved for core injection", reserved)
	}
}

// TestStreamFingerprintSSEIdentity — the acceptance proof that the seam is
// adapter-neutral: an sse-shaped, url-keyed identity reproduces the
// sse-text-scalars fixture fingerprint
// sha256(canonical({"stream":"server","url":"https://example.test/events"}))[:8]
// with no core changes, both from the pure function and through a session
// open with an sse-shaped payload.
func TestStreamFingerprintSSEIdentity(t *testing.T) {
	open := xrr.StreamOpen{
		AdapterID: "sse",
		Type:      xrr.StreamServer,
		Identity:  map[string]any{"url": "https://example.test/events"},
		Payload:   map[string]any{"url": "https://example.test/events"},
	}
	fp, err := xrr.StreamFingerprint(open, -1)
	require.NoError(t, err)
	assert.Equal(t, "66ecc77a", fp)

	dir := t.TempDir()
	s := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	rec, err := s.OpenStreamRecord(open)
	require.NoError(t, err)
	assert.Equal(t, "66ecc77a", rec.Fingerprint())
	rec.RecordRecv([]byte("on"))
	require.NoError(t, rec.Finish(map[string]any{}, nil))

	pair, err := xrr.NewFileCassette(dir).LoadStream("sse", "66ecc77a")
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"url": "https://example.test/events"}, pair.ReqPayload)
	assert.NotContains(t, pair.ReqPayload, "n", "content-addressed opens carry no ordinal")
}

func TestStreamFingerprintNoHTMLEscaping(t *testing.T) {
	// url carries & < > — HTML-safe escaping would canonicalize them as
	// escape sequences and fork the fingerprint from the other ports.
	url := "https://example.test/events?a=1&cmp=<2>"
	open := xrr.StreamOpen{
		AdapterID: "sse",
		Type:      xrr.StreamServer,
		Identity:  map[string]any{"url": url},
	}
	fp, err := xrr.StreamFingerprint(open, -1)
	require.NoError(t, err)

	canonical := `{"stream":"server","url":"` + url + `"}`
	sum := sha256.Sum256([]byte(canonical))
	assert.Equal(t, hex.EncodeToString(sum[:4]), fp,
		"canonical JSON must use standard escaping only (no HTML-safe escaping)")
}

// writeStreamedPair writes raw req/resp YAML docs as a grpc-<fp> pair in a
// fresh temp dir and returns a cassette over it.
func writeStreamedPair(t *testing.T, fp, req, resp string) *xrr.FileCassette {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "grpc-"+fp+".req.yaml"), []byte(req), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "grpc-"+fp+".resp.yaml"), []byte(resp), 0o644))
	return xrr.NewFileCassette(dir)
}

const (
	testEnvHeader = "xrr: \"1\"\nadapter: grpc\nfingerprint: \"cafecafe\"\nrecorded_at: \"2026-08-23T12:00:00Z\"\n"

	testReqDoc = testEnvHeader + `payload:
  service: chat.ChatService
  method: Converse
  n: 0
stream:
  type: bidi
  frames:
    - seq: 0
      message_b64: "cGluZw=="
      at_ms: 0
  half_close:
    seq: 2
    at_ms: 5
`

	testRespDoc = testEnvHeader + `payload:
  status_code: 0
stream:
  frames:
    - seq: 1
      message_b64: "cG9uZw=="
      at_ms: 3
  end:
    seq: 3
    at_ms: 6
`
)

func TestLoadStreamValidPair(t *testing.T) {
	c := writeStreamedPair(t, "cafecafe", testReqDoc, testRespDoc)
	pair, err := c.LoadStream("grpc", "cafecafe")
	require.NoError(t, err)

	assert.Equal(t, xrr.StreamBidi, pair.Req.Type)
	require.Len(t, pair.Req.Frames, 1)
	assert.Equal(t, 0, pair.Req.Frames[0].Seq)
	assert.Equal(t, []byte("ping"), pair.Req.Frames[0].Message)
	require.NotNil(t, pair.Req.Frames[0].AtMs)
	assert.Equal(t, int64(0), *pair.Req.Frames[0].AtMs)
	require.NotNil(t, pair.Req.HalfClose)
	assert.Equal(t, 2, pair.Req.HalfClose.Seq)

	require.Len(t, pair.Resp.Frames, 1)
	assert.Equal(t, []byte("pong"), pair.Resp.Frames[0].Message)
	assert.Equal(t, 3, pair.Resp.End.Seq)

	assert.Equal(t, "chat.ChatService", pair.ReqPayload["service"])
	assert.Equal(t, 0, pair.ReqPayload["n"])
	assert.Equal(t, 0, pair.RespPayload["status_code"])
	assert.Empty(t, pair.RecordedErr)
	assert.Equal(t, "2026-08-23T12:00:00Z", pair.RecordedAt)
}

// TestLoadStreamValidationRejections drives every reader-side MUST-reject
// rule from the spec's Validation Rules section.
func TestLoadStreamValidationRejections(t *testing.T) {
	frames := func(body string) string {
		return testEnvHeader + "payload:\n  service: s\n  method: m\n  n: 0\n" + body
	}
	respWith := func(body string) string {
		return testEnvHeader + "payload:\n  status_code: 0\n" + body
	}
	cases := []struct {
		name string
		req  string
		resp string
	}{
		{
			name: "missing type",
			req:  frames("stream:\n  frames: []\n  half_close:\n    seq: 0\n"),
			resp: testRespDoc,
		},
		{
			name: "bad type",
			req:  frames("stream:\n  type: duplex\n  frames: []\n"),
			resp: testRespDoc,
		},
		{
			name: "frame with both encodings",
			req:  frames("stream:\n  type: bidi\n  frames:\n    - seq: 0\n      message_b64: \"cGluZw==\"\n      message_text: \"ping\"\n"),
			resp: testRespDoc,
		},
		{
			name: "frame with neither encoding",
			req:  frames("stream:\n  type: bidi\n  frames:\n    - seq: 0\n      at_ms: 0\n"),
			resp: testRespDoc,
		},
		{
			name: "frame missing seq",
			req:  frames("stream:\n  type: bidi\n  frames:\n    - message_b64: \"cGluZw==\"\n"),
			resp: testRespDoc,
		},
		{
			name: "negative seq",
			req:  frames("stream:\n  type: bidi\n  frames:\n    - seq: -1\n      message_b64: \"cGluZw==\"\n"),
			resp: testRespDoc,
		},
		{
			name: "non-ascending frames",
			req:  frames("stream:\n  type: bidi\n  frames:\n    - seq: 4\n      message_b64: \"cGluZw==\"\n    - seq: 0\n      message_b64: \"cGluZw==\"\n"),
			resp: respWith("stream:\n  frames: []\n  end:\n    seq: 5\n"),
		},
		{
			name: "duplicate seq across pair",
			req:  frames("stream:\n  type: bidi\n  frames:\n    - seq: 0\n      message_b64: \"cGluZw==\"\n"),
			resp: respWith("stream:\n  frames:\n    - seq: 0\n      message_b64: \"cG9uZw==\"\n  end:\n    seq: 3\n"),
		},
		{
			name: "missing end",
			req:  testReqDoc,
			resp: respWith("stream:\n  frames:\n    - seq: 1\n      message_b64: \"cG9uZw==\"\n"),
		},
		{
			name: "end not maximal",
			req:  frames("stream:\n  type: bidi\n  frames:\n    - seq: 0\n      message_b64: \"cGluZw==\"\n  half_close:\n    seq: 5\n"),
			resp: respWith("stream:\n  frames:\n    - seq: 1\n      message_b64: \"cG9uZw==\"\n  end:\n    seq: 2\n"),
		},
		{
			name: "b64 embedded whitespace",
			req:  frames("stream:\n  type: bidi\n  frames:\n    - seq: 0\n      message_b64: \"cGlu Zw==\"\n"),
			resp: testRespDoc,
		},
		{
			name: "b64 out-of-alphabet character",
			req:  frames("stream:\n  type: bidi\n  frames:\n    - seq: 0\n      message_b64: \"cGluZw!=\"\n"),
			resp: testRespDoc,
		},
		{
			name: "b64 embedded newline",
			req:  frames("stream:\n  type: bidi\n  frames:\n    - seq: 0\n      message_b64: \"cGlu\\nZw==\"\n"),
			resp: testRespDoc,
		},
		{
			name: "b64 truncated padding",
			req:  frames("stream:\n  type: bidi\n  frames:\n    - seq: 0\n      message_b64: \"YQ=\"\n"),
			resp: testRespDoc,
		},
		{
			name: "one-sided stream req only",
			req:  testReqDoc,
			resp: testEnvHeader + "payload:\n  status_code: 0\n",
		},
		{
			name: "one-sided stream resp only",
			req:  testEnvHeader + "payload:\n  service: s\n  method: m\n",
			resp: testRespDoc,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := writeStreamedPair(t, "cafecafe", tc.req, tc.resp)
			_, err := c.LoadStream("grpc", "cafecafe")
			require.Error(t, err)
			assert.NotErrorIs(t, err, xrr.ErrCassetteMiss)
		})
	}
}

// TestLoadStreamLenientAcceptance — reader MAY/MUST-tolerate rules: sparse
// seq numbering, absent frames key, absent at_ms, unquoted hazard scalars.
func TestLoadStreamLenientAcceptance(t *testing.T) {
	req := testEnvHeader + `payload:
  service: s
  method: m
  n: 0
stream:
  type: client
  half_close:
    seq: 0
`
	resp := testEnvHeader + `payload:
  status_code: 0
stream:
  frames:
    - seq: 3
      message_text: on
    - seq: 5
      message_text: 12:30
    - seq: 7
      message_text: null
  end:
    seq: 9
`
	c := writeStreamedPair(t, "cafecafe", req, resp)
	pair, err := c.LoadStream("grpc", "cafecafe")
	require.NoError(t, err)
	assert.Empty(t, pair.Req.Frames, "absent frames key reads as []")
	require.Len(t, pair.Resp.Frames, 3)
	assert.Equal(t, []byte("on"), pair.Resp.Frames[0].Message)
	assert.Equal(t, []byte("12:30"), pair.Resp.Frames[1].Message)
	assert.Equal(t, []byte("null"), pair.Resp.Frames[2].Message)
	assert.Nil(t, pair.Resp.Frames[0].AtMs, "absent at_ms tolerated")
	assert.Nil(t, pair.Resp.End.AtMs)
}

func TestLoadStreamShapeMismatch(t *testing.T) {
	dir := t.TempDir()
	c := xrr.NewFileCassette(dir)
	require.NoError(t, c.Save("grpc", "cafecafe",
		map[string]any{"service": "s", "method": "m"},
		map[string]any{"status_code": 0}, nil))

	// Unary pair through the streaming code path.
	_, err := c.LoadStream("grpc", "cafecafe")
	assert.ErrorIs(t, err, xrr.ErrShapeMismatch)

	// Streamed pair through the unary code path.
	sc := writeStreamedPair(t, "cafecafe", testReqDoc, testRespDoc)
	var reqPayload, respPayload map[string]any
	_, err = sc.Load("grpc", "cafecafe", &reqPayload, &respPayload)
	assert.ErrorIs(t, err, xrr.ErrShapeMismatch)

	// Missing pair is still a miss, not a shape mismatch.
	_, err = c.LoadStream("grpc", "deadbeef")
	assert.ErrorIs(t, err, xrr.ErrCassetteMiss)
}

func TestSaveStreamRoundTrip(t *testing.T) {
	c := writeStreamedPair(t, "cafecafe", testReqDoc, testRespDoc)
	pair, err := c.LoadStream("grpc", "cafecafe")
	require.NoError(t, err)

	out := xrr.NewFileCassette(t.TempDir())
	require.NoError(t, out.SaveStream("grpc", "cafecafe", pair))
	again, err := out.LoadStream("grpc", "cafecafe")
	require.NoError(t, err)
	assertStreamPairEqual(t, pair, again)
}

// TestSaveStreamQuotedScalars — fingerprint and message_text MUST be emitted
// as quoted scalars so YAML readers cannot misresolve them.
func TestSaveStreamQuotedScalars(t *testing.T) {
	dir := t.TempDir()
	c := xrr.NewFileCassette(dir)
	atMs := int64(1)
	pair := &xrr.StreamPair{
		Req: xrr.ReqStream{
			Type:      xrr.StreamServer,
			Frames:    []xrr.StreamFrame{{Seq: 0, Message: []byte("on"), Text: true, AtMs: &atMs}},
			HalfClose: &xrr.StreamEvent{Seq: 1},
		},
		Resp: xrr.RespStream{
			Frames: []xrr.StreamFrame{{Seq: 2, Message: []byte("12:30"), Text: true}},
			End:    xrr.StreamEvent{Seq: 3},
		},
		ReqPayload:  map[string]any{"url": "https://example.test/events"},
		RespPayload: map[string]any{},
	}
	// All-digit fingerprint: unquoted it would parse as an integer.
	require.NoError(t, c.SaveStream("sse", "12345678", pair))

	raw, err := os.ReadFile(filepath.Join(dir, "sse-12345678.req.yaml"))
	require.NoError(t, err)
	assert.Contains(t, string(raw), `fingerprint: "12345678"`)
	assert.Contains(t, string(raw), `message_text: "on"`)

	again, err := c.LoadStream("sse", "12345678")
	require.NoError(t, err)
	assert.Equal(t, []byte("on"), again.Req.Frames[0].Message)
	assert.Equal(t, []byte("12:30"), again.Resp.Frames[0].Message)
}

// TestSaveStreamBinaryFallsBackToB64 — a Text-flagged frame whose bytes are
// not valid UTF-8 must be written as message_b64, not message_text.
func TestSaveStreamBinaryFallsBackToB64(t *testing.T) {
	dir := t.TempDir()
	c := xrr.NewFileCassette(dir)
	binary := []byte{0xff, 0xfe, 0x00}
	pair := &xrr.StreamPair{
		Req: xrr.ReqStream{Type: xrr.StreamBidi, Frames: nil, HalfClose: &xrr.StreamEvent{Seq: 1}},
		Resp: xrr.RespStream{
			Frames: []xrr.StreamFrame{{Seq: 0, Message: binary, Text: true}},
			End:    xrr.StreamEvent{Seq: 2},
		},
		ReqPayload:  map[string]any{"service": "s", "method": "m", "n": 0},
		RespPayload: map[string]any{"status_code": 0},
	}
	require.NoError(t, c.SaveStream("grpc", "cafecafe", pair))

	raw, err := os.ReadFile(filepath.Join(dir, "grpc-cafecafe.resp.yaml"))
	require.NoError(t, err)
	assert.NotContains(t, string(raw), "message_text")

	again, err := c.LoadStream("grpc", "cafecafe")
	require.NoError(t, err)
	assert.Equal(t, binary, again.Resp.Frames[0].Message)
	assert.Empty(t, again.Req.Frames, "nil frames emit as [] and reload empty")
}

// assertStreamPairEqual compares pairs field-for-field with messages compared
// over decoded bytes. The message-encoding choice (Text flag) is free on
// re-emit and is deliberately not compared.
func assertStreamPairEqual(t *testing.T, want, got *xrr.StreamPair) {
	t.Helper()
	assert.Equal(t, want.Req.Type, got.Req.Type)
	assert.Equal(t, want.Req.HalfClose, got.Req.HalfClose)
	assert.Equal(t, want.Resp.End, got.Resp.End)
	assertFramesEqual(t, want.Req.Frames, got.Req.Frames)
	assertFramesEqual(t, want.Resp.Frames, got.Resp.Frames)
	assert.Equal(t, want.ReqPayload, got.ReqPayload)
	assert.Equal(t, want.RespPayload, got.RespPayload)
	assert.Equal(t, want.RecordedErr, got.RecordedErr)
	assert.Equal(t, want.RecordedAt, got.RecordedAt)
}

func assertFramesEqual(t *testing.T, want, got []xrr.StreamFrame) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		assert.Equal(t, want[i].Seq, got[i].Seq, "frame %d seq", i)
		assert.Equal(t, want[i].Message, got[i].Message, "frame %d bytes", i)
		assert.Equal(t, want[i].AtMs, got[i].AtMs, "frame %d at_ms", i)
	}
}
