package xrr_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"path/filepath"
	"testing"

	xrr "hop.top/xrr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fixtureSession(t *testing.T, dir string) *xrr.FileSession {
	t.Helper()
	path := filepath.Join("..", "spec", "fixtures", dir)
	return xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(path))
}

// grpcStreamOpen mirrors the gRPC adapter's open definition: canonical
// inputs service + method (+ msg_hash for content-addressed server
// streams), counter-addressed client/bidi, req payload {service, method}.
func grpcStreamOpen(typ xrr.StreamType, service, method string, msg []byte) xrr.StreamOpen {
	open := xrr.StreamOpen{
		AdapterID: "grpc",
		Type:      typ,
		Identity:  map[string]any{"service": service, "method": method},
		Payload:   map[string]any{"service": service, "method": method},
	}
	if typ == xrr.StreamServer {
		sum := sha256.Sum256(msg)
		open.Identity["msg_hash"] = hex.EncodeToString(sum[:4])
	} else {
		open.Counter = true
	}
	return open
}

// ── record path ────────────────────────────────────────────────────────────

func TestOpenStreamRecordServer(t *testing.T) {
	dir := t.TempDir()
	s := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	msg := []byte(`{"path":"/etc/hosts"}`)
	rec, err := s.OpenStreamRecord(grpcStreamOpen(xrr.StreamServer, "files.FileService", "Download", msg))
	require.NoError(t, err)
	assert.Equal(t, "58a4bf3f", rec.Fingerprint())

	rec.RecordSend(msg)
	rec.RecordHalfClose()
	rec.RecordRecv([]byte("chunk-one\n"))
	rec.RecordRecv([]byte("chunk-two\n"))
	require.NoError(t, rec.Finish(map[string]any{"status_code": 0}, nil))

	pair, err := xrr.NewFileCassette(dir).LoadStream("grpc", "58a4bf3f")
	require.NoError(t, err)
	assert.Equal(t, xrr.StreamServer, pair.Req.Type)

	// Dense seq 0..N-1 counting all events in arrival order.
	require.Len(t, pair.Req.Frames, 1)
	assert.Equal(t, 0, pair.Req.Frames[0].Seq)
	require.NotNil(t, pair.Req.HalfClose)
	assert.Equal(t, 1, pair.Req.HalfClose.Seq)
	require.Len(t, pair.Resp.Frames, 2)
	assert.Equal(t, 2, pair.Resp.Frames[0].Seq)
	assert.Equal(t, 3, pair.Resp.Frames[1].Seq)
	assert.Equal(t, 4, pair.Resp.End.Seq)

	assert.Equal(t, []byte("chunk-one\n"), pair.Resp.Frames[0].Message)

	// at_ms stamped on every event, ≥ 0 and non-decreasing.
	prev := int64(0)
	for _, f := range append(append([]xrr.StreamFrame{}, pair.Req.Frames...), pair.Resp.Frames...) {
		require.NotNil(t, f.AtMs)
		assert.GreaterOrEqual(t, *f.AtMs, prev)
		prev = *f.AtMs
	}
	require.NotNil(t, pair.Req.HalfClose.AtMs)
	require.NotNil(t, pair.Resp.End.AtMs)

	// Server-stream payload carries no occurrence ordinal.
	assert.Equal(t, "files.FileService", pair.ReqPayload["service"])
	assert.Equal(t, "Download", pair.ReqPayload["method"])
	assert.NotContains(t, pair.ReqPayload, "n")
}

// TestOpenStreamRecordCounterN — one Session object is one counter domain:
// two opens of the same (service, method, type) tuple record n=0 then n=1,
// matching the grpc-client-stream-repeat fixture fingerprints.
func TestOpenStreamRecordCounterN(t *testing.T) {
	dir := t.TempDir()
	s := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	open := grpcStreamOpen(xrr.StreamClient, "files.FileService", "Upload", nil)

	rec1, err := s.OpenStreamRecord(open)
	require.NoError(t, err)
	assert.Equal(t, "2bebfd6f", rec1.Fingerprint())
	rec1.RecordSend([]byte("alpha\n"))
	rec1.RecordHalfClose()
	rec1.RecordRecv([]byte(`{"received_bytes":6}`))
	require.NoError(t, rec1.Finish(map[string]any{"status_code": 0}, nil))

	rec2, err := s.OpenStreamRecord(open)
	require.NoError(t, err)
	assert.Equal(t, "b27b5fe1", rec2.Fingerprint())
	rec2.RecordHalfClose()
	require.NoError(t, rec2.Finish(map[string]any{"status_code": 0}, nil))

	c := xrr.NewFileCassette(dir)
	p1, err := c.LoadStream("grpc", "2bebfd6f")
	require.NoError(t, err)
	assert.Equal(t, 0, p1.ReqPayload["n"])
	p2, err := c.LoadStream("grpc", "b27b5fe1")
	require.NoError(t, err)
	assert.Equal(t, 1, p2.ReqPayload["n"])

	// A different tuple starts its own count.
	rec3, err := s.OpenStreamRecord(grpcStreamOpen(xrr.StreamBidi, "chat.ChatService", "Converse", nil))
	require.NoError(t, err)
	assert.Equal(t, "c6233d2e", rec3.Fingerprint())
}

// TestStreamRecordingTerminalIsFinal — no events are recorded after the
// terminal; a second Finish is an error.
func TestStreamRecordingTerminalIsFinal(t *testing.T) {
	dir := t.TempDir()
	s := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	rec, err := s.OpenStreamRecord(grpcStreamOpen(xrr.StreamBidi, "chat.ChatService", "Converse", nil))
	require.NoError(t, err)
	rec.RecordSend([]byte("ping-1"))
	rec.RecordRecv([]byte("pong-1"))
	require.NoError(t, rec.Finish(map[string]any{"status_code": 0}, nil))

	// Dropped, matching the real-world no-op.
	rec.RecordSend([]byte("late"))
	rec.RecordRecv([]byte("late"))
	rec.RecordHalfClose()
	assert.Error(t, rec.Finish(map[string]any{"status_code": 0}, nil))

	pair, err := xrr.NewFileCassette(dir).LoadStream("grpc", rec.Fingerprint())
	require.NoError(t, err)
	assert.Len(t, pair.Req.Frames, 1)
	assert.Len(t, pair.Resp.Frames, 1)
	assert.Nil(t, pair.Req.HalfClose)
	assert.Equal(t, 2, pair.Resp.End.Seq)
}

func TestStreamRecordingErrorTerminal(t *testing.T) {
	dir := t.TempDir()
	s := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	rec, err := s.OpenStreamRecord(grpcStreamOpen(
		xrr.StreamServer, "files.FileService", "Download", []byte(`{"path":"/var/log/big.log"}`)))
	require.NoError(t, err)
	rec.RecordSend([]byte(`{"path":"/var/log/big.log"}`))
	rec.RecordHalfClose()
	rec.RecordRecv([]byte("log-chunk-1\n"))
	require.NoError(t, rec.Finish(
		map[string]any{"status_code": 14},
		errors.New("rpc error: code = Unavailable desc = connection reset"),
	))

	pair, err := xrr.NewFileCassette(dir).LoadStream("grpc", "9e8c4d4c")
	require.NoError(t, err)
	assert.Equal(t, "rpc error: code = Unavailable desc = connection reset", pair.RecordedErr)
	assert.Equal(t, 14, pair.RespPayload["status_code"])
}

// ── replay path ────────────────────────────────────────────────────────────

func TestOpenStreamReplayBidi(t *testing.T) {
	s := fixtureSession(t, "grpc-bidi-stream")
	rep, err := s.OpenStreamReplay(grpcStreamOpen(xrr.StreamBidi, "chat.ChatService", "Converse", nil))
	require.NoError(t, err)
	assert.Equal(t, "c6233d2e", rep.Fingerprint())
	assert.Equal(t, xrr.StreamBidi, rep.Type())

	// Reads never gate on send progress: drain both pongs first.
	msg, err := rep.Recv()
	require.NoError(t, err)
	assert.Equal(t, []byte("pong-1"), msg)
	msg, err = rep.Recv()
	require.NoError(t, err)
	assert.Equal(t, []byte("pong-2"), msg)

	// Sends validated in order and bytes afterwards.
	require.NoError(t, rep.Send([]byte("ping-1")))
	require.NoError(t, rep.Send([]byte("ping-2")))
	require.NoError(t, rep.HalfClose())

	_, err = rep.Recv()
	assert.ErrorIs(t, err, io.EOF)
	_, err = rep.Recv()
	assert.ErrorIs(t, err, io.EOF, "terminal repeats for j > R")
}

func TestStreamReplaySendMismatchIsTerminal(t *testing.T) {
	s := fixtureSession(t, "grpc-bidi-stream")
	rep, err := s.OpenStreamReplay(grpcStreamOpen(xrr.StreamBidi, "chat.ChatService", "Converse", nil))
	require.NoError(t, err)

	require.NoError(t, rep.Send([]byte("ping-1")))
	err = rep.Send([]byte("ping-DIVERGED"))
	require.Error(t, err)
	assert.ErrorIs(t, err, xrr.ErrStreamMismatch)
	var mErr *xrr.StreamMismatchError
	require.ErrorAs(t, err, &mErr)
	assert.Equal(t, 1, mErr.Ordinal)

	// Mismatch poisons every subsequent operation.
	_, err = rep.Recv()
	assert.ErrorIs(t, err, xrr.ErrStreamMismatch)
	assert.ErrorIs(t, rep.HalfClose(), xrr.ErrStreamMismatch)
	assert.ErrorIs(t, rep.Send([]byte("ping-2")), xrr.ErrStreamMismatch)
}

func TestStreamReplayShortHalfCloseIsMismatch(t *testing.T) {
	s := fixtureSession(t, "grpc-client-stream")
	rep, err := s.OpenStreamReplay(grpcStreamOpen(xrr.StreamClient, "files.FileService", "Upload", nil))
	require.NoError(t, err)

	require.NoError(t, rep.Send([]byte("part-one\n")))
	err = rep.HalfClose()
	assert.ErrorIs(t, err, xrr.ErrStreamMismatch, "half-close after 1 of 2 sends")
}

// TestStreamReplayPostCompletionSend — send at i ≥ S with an OK terminal is
// the non-poisoning stream-done signal; the recv side is unaffected.
func TestStreamReplayPostCompletionSend(t *testing.T) {
	s := fixtureSession(t, "grpc-client-stream")
	rep, err := s.OpenStreamReplay(grpcStreamOpen(xrr.StreamClient, "files.FileService", "Upload", nil))
	require.NoError(t, err)

	require.NoError(t, rep.Send([]byte("part-one\n")))
	require.NoError(t, rep.Send([]byte("part-two\n")))
	assert.ErrorIs(t, rep.Send([]byte("part-three\n")), io.EOF)
	require.NoError(t, rep.HalfClose(), "half-close after all recorded sends is always accepted")

	msg, err := rep.Recv()
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"received_bytes":18}`), msg)
	_, err = rep.Recv()
	assert.ErrorIs(t, err, io.EOF)
}

// TestStreamReplayMidStreamError — recorded frames delivered, then the
// recorded error in place of end-of-stream; post-completion sends surface
// the same recorded error.
func TestStreamReplayMidStreamError(t *testing.T) {
	s := fixtureSession(t, "grpc-stream-error")
	rep, err := s.OpenStreamReplay(grpcStreamOpen(
		xrr.StreamServer, "files.FileService", "Download", []byte(`{"path":"/var/log/big.log"}`)))
	require.NoError(t, err)
	assert.Equal(t, "9e8c4d4c", rep.Fingerprint())
	assert.Equal(t, 14, rep.RespPayload()["status_code"])

	require.NoError(t, rep.Send([]byte(`{"path":"/var/log/big.log"}`)))
	require.NoError(t, rep.HalfClose())

	msg, err := rep.Recv()
	require.NoError(t, err)
	assert.Equal(t, []byte("log-chunk-1\n"), msg)
	msg, err = rep.Recv()
	require.NoError(t, err)
	assert.Equal(t, []byte("log-chunk-2\n"), msg)

	wantErr := "rpc error: code = Unavailable desc = connection reset"
	_, err = rep.Recv()
	require.EqualError(t, err, wantErr)
	assert.NotErrorIs(t, err, xrr.ErrStreamMismatch)
	_, err = rep.Recv()
	require.EqualError(t, err, wantErr, "recorded error repeats for j > R")

	// The recorded stream was already dead: post-completion send returns it.
	assert.EqualError(t, rep.Send([]byte("extra")), wantErr)
}

func TestStreamReplayEmptyStreams(t *testing.T) {
	t.Run("server empty resp", func(t *testing.T) {
		s := fixtureSession(t, "grpc-stream-empty")
		rep, err := s.OpenStreamReplay(grpcStreamOpen(
			xrr.StreamServer, "files.FileService", "Download", []byte(`{"path":"/etc/empty"}`)))
		require.NoError(t, err)
		_, err = rep.Recv()
		assert.ErrorIs(t, err, io.EOF, "first read yields end-of-stream")
	})

	t.Run("client immediate half-close", func(t *testing.T) {
		s := fixtureSession(t, "grpc-stream-empty")
		rep, err := s.OpenStreamReplay(grpcStreamOpen(xrr.StreamClient, "telemetry.MetricsService", "Push", nil))
		require.NoError(t, err)
		require.NoError(t, rep.HalfClose(), "S=0: immediate half-close accepted")
		msg, err := rep.Recv()
		require.NoError(t, err)
		assert.Equal(t, []byte(`{"count":0}`), msg)
		_, err = rep.Recv()
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("bidi no traffic", func(t *testing.T) {
		s := fixtureSession(t, "grpc-stream-empty")
		rep, err := s.OpenStreamReplay(grpcStreamOpen(xrr.StreamBidi, "chat.ChatService", "Ping", nil))
		require.NoError(t, err)
		require.NoError(t, rep.HalfClose())
		_, err = rep.Recv()
		assert.ErrorIs(t, err, io.EOF)
	})
}

func TestStreamReplayMissAndShapeMismatch(t *testing.T) {
	dir := t.TempDir()
	c := xrr.NewFileCassette(dir)
	s := xrr.NewSession(xrr.ModeReplay, c)

	// No pair on disk ⇒ cassette miss.
	_, err := s.OpenStreamReplay(grpcStreamOpen(xrr.StreamBidi, "s", "m", nil))
	assert.ErrorIs(t, err, xrr.ErrCassetteMiss)

	// A unary pair at the streamed fingerprint ⇒ shape mismatch, not a miss.
	fp, err := xrr.StreamFingerprint(grpcStreamOpen(xrr.StreamBidi, "s", "m", nil), 1)
	require.NoError(t, err)
	require.NoError(t, c.Save("grpc", fp,
		map[string]any{"service": "s", "method": "m"},
		map[string]any{"status_code": 0}, nil))
	_, err = s.OpenStreamReplay(grpcStreamOpen(xrr.StreamBidi, "s", "m", nil))
	assert.ErrorIs(t, err, xrr.ErrShapeMismatch)
	assert.NotErrorIs(t, err, xrr.ErrCassetteMiss)
}

func TestOpenStreamModeEnforcement(t *testing.T) {
	dir := t.TempDir()
	open := grpcStreamOpen(xrr.StreamBidi, "s", "m", nil)

	replaySession := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(dir))
	assert.Equal(t, xrr.ModeReplay, replaySession.Mode())
	_, err := replaySession.OpenStreamRecord(open)
	assert.Error(t, err)

	recordSession := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	_, err = recordSession.OpenStreamReplay(open)
	assert.Error(t, err)
}
