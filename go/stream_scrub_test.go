package xrr_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	xrr "hop.top/xrr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Frame-level scrub hook: secrets are rewritten on the DECODED bytes,
// identically at record and replay time. Symmetry is the correctness
// invariant — a cassette recorded through a scrub only replays green when
// the same scrub is active on the replaying session.

const scrubSecret = "hunter2-FAKE-TOKEN-0123456789"
const scrubMask = "<scrubbed>"

// maskSecret is a deterministic scrub replacing the fake token wherever it
// appears, both directions.
func maskSecret(_ xrr.StreamDirection, _ xrr.StreamScrubInfo, data []byte) []byte {
	return bytes.ReplaceAll(data, []byte(scrubSecret), []byte(scrubMask))
}

// grpcScrubbedServerOpen mirrors the gRPC adapter's server-stream open under
// the scrub contract: content-derived identity (msg_hash) is computed over
// the SCRUBBED open-message bytes, so record and replay address the cassette
// by scrubbed content.
func grpcScrubbedServerOpen(s *xrr.FileSession, service, method string, msg []byte) xrr.StreamOpen {
	open := xrr.StreamOpen{
		AdapterID: "grpc",
		Type:      xrr.StreamServer,
		Identity:  map[string]any{"service": service, "method": method},
		Payload:   map[string]any{"service": service, "method": method},
	}
	scrubbed := s.ScrubStreamFrame(xrr.StreamSend,
		xrr.StreamScrubInfo{AdapterID: "grpc", Type: xrr.StreamServer}, msg)
	sum := sha256.Sum256(scrubbed)
	open.Identity["msg_hash"] = hex.EncodeToString(sum[:4])
	return open
}

// TestStreamScrubRecordScrubsFrames — record path: frame bytes are scrubbed
// before persistence, both directions, so the secret never reaches disk in
// any encoding.
func TestStreamScrubRecordScrubsFrames(t *testing.T) {
	dir := t.TempDir()
	s := xrr.NewSessionWithStreamScrub(xrr.ModeRecord, xrr.NewFileCassette(dir), maskSecret)

	rec, err := s.OpenStreamRecord(grpcStreamOpen(xrr.StreamBidi, "chat.ChatService", "Converse", nil))
	require.NoError(t, err)
	rec.RecordSend([]byte("ping " + scrubSecret))
	rec.RecordRecv([]byte("pong " + scrubSecret))
	rec.RecordHalfClose()
	require.NoError(t, rec.Finish(map[string]any{"status_code": 0}, nil))

	pair, err := xrr.NewFileCassette(dir).LoadStream("grpc", rec.Fingerprint())
	require.NoError(t, err)
	require.Len(t, pair.Req.Frames, 1)
	require.Len(t, pair.Resp.Frames, 1)
	assert.Equal(t, []byte("ping "+scrubMask), pair.Req.Frames[0].Message)
	assert.Equal(t, []byte("pong "+scrubMask), pair.Resp.Frames[0].Message)

	// The decoded frame check above is the real gate (base64 hides the
	// secret from text scans); the raw-text check guards the payload side.
	for _, kind := range []string{"req", "resp"} {
		raw, err := os.ReadFile(filepath.Join(dir, "grpc-"+rec.Fingerprint()+"."+kind+".yaml"))
		require.NoError(t, err)
		assert.NotContains(t, string(raw), scrubSecret)
	}
}

// TestStreamScrubServerStreamIdentity — a server stream whose open message
// carries a secret: msg_hash is derived from scrubbed bytes on BOTH sides,
// so the scrubbed recording and the scrubbed replay compute one stable
// fingerprint, and the recorded open frame hashes to its own identity.
func TestStreamScrubServerStreamIdentity(t *testing.T) {
	dir := t.TempDir()
	msg := []byte(`{"cmd":"deploy","token":"` + scrubSecret + `"}`)

	recS := xrr.NewSessionWithStreamScrub(xrr.ModeRecord, xrr.NewFileCassette(dir), maskSecret)
	rec, err := recS.OpenStreamRecord(grpcScrubbedServerOpen(recS, "ops.Deploy", "Run", msg))
	require.NoError(t, err)
	rec.RecordSend(msg)
	rec.RecordHalfClose()
	rec.RecordRecv([]byte("deployed"))
	require.NoError(t, rec.Finish(map[string]any{"status_code": 0}, nil))

	// Self-consistency: the persisted (scrubbed) open frame hashes to the
	// msg_hash identity the fingerprint was computed from — recomputing the
	// fingerprint from the frame on disk reproduces the filename.
	pair, err := xrr.NewFileCassette(dir).LoadStream("grpc", rec.Fingerprint())
	require.NoError(t, err)
	require.Len(t, pair.Req.Frames, 1)
	assert.NotContains(t, string(pair.Req.Frames[0].Message), scrubSecret)
	fromDisk, err := xrr.StreamFingerprint(
		grpcStreamOpen(xrr.StreamServer, "ops.Deploy", "Run", pair.Req.Frames[0].Message), -1)
	require.NoError(t, err)
	assert.Equal(t, rec.Fingerprint(), fromDisk)

	repS := xrr.NewSessionWithStreamScrub(xrr.ModeReplay, xrr.NewFileCassette(dir), maskSecret)
	rep, err := repS.OpenStreamReplay(grpcScrubbedServerOpen(repS, "ops.Deploy", "Run", msg))
	require.NoError(t, err, "scrubbed identity must locate the scrubbed recording")
	assert.Equal(t, rec.Fingerprint(), rep.Fingerprint())

	require.NoError(t, rep.Send(msg), "live secret-bearing open must match after symmetric scrub")
	require.NoError(t, rep.HalfClose())
	got, err := rep.Recv()
	require.NoError(t, err)
	assert.Equal(t, []byte("deployed"), got)
	_, err = rep.Recv()
	assert.ErrorIs(t, err, io.EOF)
}

// TestStreamScrubReplaySymmetry — the invariant that makes scrubbing
// correct: replaying the same live traffic through the same scrub is green;
// replaying WITHOUT the scrub mismatches, because the cassette holds
// scrubbed bytes and the live sends do not.
func TestStreamScrubReplaySymmetry(t *testing.T) {
	dir := t.TempDir()
	open := grpcStreamOpen(xrr.StreamClient, "vault.Vault", "Put", nil)

	recS := xrr.NewSessionWithStreamScrub(xrr.ModeRecord, xrr.NewFileCassette(dir), maskSecret)
	rec, err := recS.OpenStreamRecord(open)
	require.NoError(t, err)
	rec.RecordSend([]byte("key=" + scrubSecret))
	rec.RecordHalfClose()
	rec.RecordRecv([]byte("stored"))
	require.NoError(t, rec.Finish(map[string]any{"status_code": 0}, nil))

	t.Run("same scrub replays green", func(t *testing.T) {
		s := xrr.NewSessionWithStreamScrub(xrr.ModeReplay, xrr.NewFileCassette(dir), maskSecret)
		rep, err := s.OpenStreamReplay(open)
		require.NoError(t, err)
		require.NoError(t, rep.Send([]byte("key="+scrubSecret)))
		require.NoError(t, rep.HalfClose())
		got, err := rep.Recv()
		require.NoError(t, err)
		assert.Equal(t, []byte("stored"), got)
		_, err = rep.Recv()
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("replay without scrub mismatches", func(t *testing.T) {
		s := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(dir))
		rep, err := s.OpenStreamReplay(open)
		require.NoError(t, err)
		err = rep.Send([]byte("key=" + scrubSecret))
		assert.ErrorIs(t, err, xrr.ErrStreamMismatch,
			"unscrubbed live send vs scrubbed recording must mismatch")
	})
}

// TestStreamScrubAppliedExactlyOnce — a deliberately non-idempotent scrub
// (appends a marker) pins single application per frame per phase: recorded
// frames are scrubbed once at record time and delivered verbatim on replay
// (never re-scrubbed), and live sends are scrubbed once before comparison.
func TestStreamScrubAppliedExactlyOnce(t *testing.T) {
	appendMarker := func(_ xrr.StreamDirection, _ xrr.StreamScrubInfo, data []byte) []byte {
		return append(append([]byte{}, data...), '#')
	}
	dir := t.TempDir()
	open := grpcStreamOpen(xrr.StreamBidi, "chat.ChatService", "Converse", nil)

	recS := xrr.NewSessionWithStreamScrub(xrr.ModeRecord, xrr.NewFileCassette(dir), appendMarker)
	rec, err := recS.OpenStreamRecord(open)
	require.NoError(t, err)
	rec.RecordSend([]byte("ping"))
	rec.RecordRecv([]byte("pong"))
	rec.RecordHalfClose()
	require.NoError(t, rec.Finish(map[string]any{"status_code": 0}, nil))

	pair, err := xrr.NewFileCassette(dir).LoadStream("grpc", rec.Fingerprint())
	require.NoError(t, err)
	assert.Equal(t, []byte("ping#"), pair.Req.Frames[0].Message, "record scrubs once")
	assert.Equal(t, []byte("pong#"), pair.Resp.Frames[0].Message, "record scrubs once")

	repS := xrr.NewSessionWithStreamScrub(xrr.ModeReplay, xrr.NewFileCassette(dir), appendMarker)
	rep, err := repS.OpenStreamReplay(open)
	require.NoError(t, err)
	require.NoError(t, rep.Send([]byte("ping")), "live send scrubbed once, matching the once-scrubbed frame")
	got, err := rep.Recv()
	require.NoError(t, err)
	assert.Equal(t, []byte("pong#"), got, "recorded frames are delivered verbatim, never re-scrubbed")
}
