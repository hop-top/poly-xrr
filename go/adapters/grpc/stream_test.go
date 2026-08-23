package grpc_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	xrr "hop.top/xrr"
	xgrpc "hop.top/xrr/adapters/grpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ── helpers ────────────────────────────────────────────────────────────────

var (
	serverDesc = &grpc.StreamDesc{StreamName: "S", ServerStreams: true}
	clientDesc = &grpc.StreamDesc{StreamName: "C", ClientStreams: true}
	bidiDesc   = &grpc.StreamDesc{StreamName: "B", ClientStreams: true, ServerStreams: true}
	unaryDesc  = &grpc.StreamDesc{StreamName: "U"}
)

func fixtureDir(dir string) string {
	return filepath.Join("..", "..", "..", "spec", "fixtures", dir)
}

func replayFixtureSession(t *testing.T, dir string) *xrr.FileSession {
	t.Helper()
	return xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(fixtureDir(dir)))
}

// failStreamer fails the test if replay ever reaches the network.
func failStreamer(t *testing.T) grpc.Streamer {
	return func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, ...grpc.CallOption) (grpc.ClientStream, error) {
		t.Fatal("streamer must not be called in replay mode")
		return nil, nil
	}
}

func openStream(t *testing.T, s *xrr.FileSession, desc *grpc.StreamDesc, full string, streamer grpc.Streamer) grpc.ClientStream {
	t.Helper()
	cs, err := xgrpc.StreamClientInterceptor(s)(context.Background(), desc, nil, full, streamer)
	require.NoError(t, err)
	return cs
}

// recvAll drains the stream into strings until a non-nil error.
func recvAll(t *testing.T, cs grpc.ClientStream) ([]string, error) {
	t.Helper()
	var got []string
	for {
		var m []byte
		if err := cs.RecvMsg(&m); err != nil {
			return got, err
		}
		got = append(got, string(m))
	}
}

func sendRaw(t *testing.T, cs grpc.ClientStream, s string) error {
	t.Helper()
	b := []byte(s)
	return cs.SendMsg(&b)
}

// fakeStream is an in-memory grpc.ClientStream standing in for a live one in
// record-mode tests. It speaks both raw *[]byte and proto.Message payloads,
// mirroring a real stream's codec boundary.
type fakeStream struct {
	mu        sync.Mutex
	sent      [][]byte
	recvQueue [][]byte
	terminal  error
	closeSent bool
	sendErr   error
}

func (f *fakeStream) Header() (metadata.MD, error) { return nil, nil }
func (f *fakeStream) Trailer() metadata.MD         { return nil }
func (f *fakeStream) Context() context.Context     { return context.Background() }

func (f *fakeStream) CloseSend() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeSent = true
	return nil
}

func (f *fakeStream) SendMsg(m any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendErr != nil {
		return f.sendErr
	}
	b, err := testWire(m)
	if err != nil {
		return err
	}
	f.sent = append(f.sent, b)
	return nil
}

func (f *fakeStream) RecvMsg(m any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.recvQueue) == 0 {
		return f.terminal
	}
	b := f.recvQueue[0]
	f.recvQueue = f.recvQueue[1:]
	return testUnwire(b, m)
}

func (f *fakeStream) streamer() grpc.Streamer {
	return func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, ...grpc.CallOption) (grpc.ClientStream, error) {
		return f, nil
	}
}

func testWire(m any) ([]byte, error) {
	switch v := m.(type) {
	case *[]byte:
		return append([]byte(nil), *v...), nil
	case proto.Message:
		return proto.Marshal(v)
	}
	return nil, errors.New("fake: unsupported message type")
}

func testUnwire(b []byte, m any) error {
	switch v := m.(type) {
	case *[]byte:
		*v = append([]byte(nil), b...)
		return nil
	case proto.Message:
		return proto.Unmarshal(b, v)
	}
	return errors.New("fake: unsupported message type")
}

func recordSession(t *testing.T) (*xrr.FileSession, string) {
	t.Helper()
	dir := t.TempDir()
	return xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir)), dir
}

// ── replay: conformance fixtures through the interceptor path ──────────────

func TestStreamReplayServerStreamFixture(t *testing.T) {
	s := replayFixtureSession(t, "grpc-server-stream")
	cs := openStream(t, s, serverDesc, "/files.FileService/Download", failStreamer(t))

	require.NoError(t, sendRaw(t, cs, `{"path":"/etc/hosts"}`))
	require.NoError(t, cs.CloseSend())

	got, err := recvAll(t, cs)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, []string{"chunk-one\n", "chunk-two\n", "chunk-three\n"}, got)

	// terminal repeats; post-completion send signals stream done.
	var m []byte
	assert.ErrorIs(t, cs.RecvMsg(&m), io.EOF)
	assert.ErrorIs(t, sendRaw(t, cs, `{"path":"/etc/hosts"}`), io.EOF)
}

func TestStreamReplayClientStreamFixture(t *testing.T) {
	s := replayFixtureSession(t, "grpc-client-stream")
	cs := openStream(t, s, clientDesc, "/files.FileService/Upload", failStreamer(t))

	require.NoError(t, sendRaw(t, cs, "part-one\n"))
	require.NoError(t, sendRaw(t, cs, "part-two\n"))
	require.NoError(t, cs.CloseSend())

	got, err := recvAll(t, cs)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, []string{`{"received_bytes":18}`}, got)
}

func TestStreamReplayBidiFixture(t *testing.T) {
	s := replayFixtureSession(t, "grpc-bidi-stream")
	cs := openStream(t, s, bidiDesc, "/chat.ChatService/Converse", failStreamer(t))

	require.NoError(t, sendRaw(t, cs, "ping-1"))
	var m []byte
	require.NoError(t, cs.RecvMsg(&m))
	assert.Equal(t, "pong-1", string(m))
	require.NoError(t, sendRaw(t, cs, "ping-2"))
	require.NoError(t, cs.RecvMsg(&m))
	assert.Equal(t, "pong-2", string(m))
	require.NoError(t, cs.CloseSend())
	assert.ErrorIs(t, cs.RecvMsg(&m), io.EOF)
}

func TestStreamReplayMidStreamErrorFixture(t *testing.T) {
	s := replayFixtureSession(t, "grpc-stream-error")
	cs := openStream(t, s, serverDesc, "/files.FileService/Download", failStreamer(t))

	require.NoError(t, sendRaw(t, cs, `{"path":"/var/log/big.log"}`))
	require.NoError(t, cs.CloseSend())

	got, err := recvAll(t, cs)
	assert.Equal(t, []string{"log-chunk-1\n", "log-chunk-2\n"}, got)
	require.Error(t, err)
	assert.NotErrorIs(t, err, io.EOF)
	// Status reconstructed from status_code; rendering matches the recording.
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Equal(t, "rpc error: code = Unavailable desc = connection reset", err.Error())

	// Post-terminal send returns the recorded error, not EOF.
	sendErr := sendRaw(t, cs, "extra")
	require.Error(t, sendErr)
	assert.Equal(t, codes.Unavailable, status.Code(sendErr))
	assert.Equal(t, err.Error(), sendErr.Error())
}

func TestStreamReplayEmptyFixtures(t *testing.T) {
	s := replayFixtureSession(t, "grpc-stream-empty")

	t.Run("server-empty", func(t *testing.T) {
		cs := openStream(t, s, serverDesc, "/files.FileService/Download", failStreamer(t))
		require.NoError(t, sendRaw(t, cs, `{"path":"/etc/empty"}`))
		require.NoError(t, cs.CloseSend())
		got, err := recvAll(t, cs)
		assert.ErrorIs(t, err, io.EOF)
		assert.Empty(t, got)
	})

	t.Run("client-no-sends", func(t *testing.T) {
		cs := openStream(t, s, clientDesc, "/telemetry.MetricsService/Push", failStreamer(t))
		require.NoError(t, cs.CloseSend())
		got, err := recvAll(t, cs)
		assert.ErrorIs(t, err, io.EOF)
		assert.Equal(t, []string{`{"count":0}`}, got)
	})

	t.Run("bidi-no-traffic", func(t *testing.T) {
		cs := openStream(t, s, bidiDesc, "/chat.ChatService/Ping", failStreamer(t))
		require.NoError(t, cs.CloseSend())
		got, err := recvAll(t, cs)
		assert.ErrorIs(t, err, io.EOF)
		assert.Empty(t, got)
	})
}

func TestStreamReplayClientStreamRepeatFixture(t *testing.T) {
	// The spec's scripted n=1 obligation, driven through the interceptor:
	// two sequential opens of the same tuple in one session.
	s := replayFixtureSession(t, "grpc-client-stream-repeat")

	cs1 := openStream(t, s, clientDesc, "/files.FileService/Upload", failStreamer(t))
	require.NoError(t, sendRaw(t, cs1, "alpha\n"))
	require.NoError(t, cs1.CloseSend())
	got, err := recvAll(t, cs1)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, []string{`{"received_bytes":6}`}, got)

	cs2 := openStream(t, s, clientDesc, "/files.FileService/Upload", failStreamer(t))
	require.NoError(t, sendRaw(t, cs2, "beta-1\n"))
	require.NoError(t, sendRaw(t, cs2, "beta-2\n"))
	require.NoError(t, cs2.CloseSend())
	got, err = recvAll(t, cs2)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, []string{`{"received_bytes":14}`}, got)
}

// ── replay: mismatches, misses, shape errors ───────────────────────────────

func TestStreamReplaySendMismatchIsTerminal(t *testing.T) {
	s := replayFixtureSession(t, "grpc-client-stream")
	cs := openStream(t, s, clientDesc, "/files.FileService/Upload", failStreamer(t))

	require.NoError(t, sendRaw(t, cs, "part-one\n"))
	err := sendRaw(t, cs, "divergent")
	require.ErrorIs(t, err, xrr.ErrStreamMismatch)

	// Mismatch is terminal: every subsequent operation fails the same way.
	var m []byte
	assert.ErrorIs(t, cs.RecvMsg(&m), xrr.ErrStreamMismatch)
	assert.ErrorIs(t, cs.CloseSend(), xrr.ErrStreamMismatch)
}

func TestStreamReplayShortHalfCloseIsMismatch(t *testing.T) {
	s := replayFixtureSession(t, "grpc-client-stream")
	cs := openStream(t, s, clientDesc, "/files.FileService/Upload", failStreamer(t))

	require.NoError(t, sendRaw(t, cs, "part-one\n"))
	assert.ErrorIs(t, cs.CloseSend(), xrr.ErrStreamMismatch)
}

func TestStreamReplayCassetteMiss(t *testing.T) {
	dir := t.TempDir()
	s := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(dir))
	ic := xgrpc.StreamClientInterceptor(s)

	// client/bidi opens miss at the interceptor call.
	_, err := ic(context.Background(), clientDesc, nil, "/files.FileService/Upload", failStreamer(t))
	assert.ErrorIs(t, err, xrr.ErrCassetteMiss)

	// server opens defer to the first send (fingerprint needs the message);
	// the miss is sticky for later operations.
	cs, err := ic(context.Background(), serverDesc, nil, "/files.FileService/Download", failStreamer(t))
	require.NoError(t, err)
	assert.ErrorIs(t, sendRaw(t, cs, "anything"), xrr.ErrCassetteMiss)
	var m []byte
	assert.ErrorIs(t, cs.RecvMsg(&m), xrr.ErrCassetteMiss)
}

func TestStreamReplayHeaderTrailerEmpty(t *testing.T) {
	// Metadata is not recorded (spec, matching the unary adapter).
	s := replayFixtureSession(t, "grpc-bidi-stream")
	cs := openStream(t, s, bidiDesc, "/chat.ChatService/Converse", failStreamer(t))
	md, err := cs.Header()
	require.NoError(t, err)
	assert.Empty(t, md)
	assert.Empty(t, cs.Trailer())
	require.NotNil(t, cs.Context())
}

func TestStreamInterceptorRejectsUnaryDesc(t *testing.T) {
	s := replayFixtureSession(t, "grpc-server-stream")
	_, err := xgrpc.StreamClientInterceptor(s)(context.Background(), unaryDesc, nil, "/x.Y/Z", failStreamer(t))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unary")
}

func TestUnaryPathSurfacesShapeMismatch(t *testing.T) {
	// A streamed pair sitting at a unary fingerprint must surface
	// ErrShapeMismatch through the unary adapter path, not be swallowed.
	adapter := xgrpc.NewAdapter()
	req := &xgrpc.Request{Service: "files.FileService", Method: "Download", Message: []byte(`{"path":"/etc/hosts"}`)}
	fp, err := adapter.Fingerprint(req)
	require.NoError(t, err)

	dir := t.TempDir()
	for _, kind := range []string{"req", "resp"} {
		src := filepath.Join(fixtureDir("grpc-server-stream"), "grpc-58a4bf3f."+kind+".yaml")
		data, err := os.ReadFile(src)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "grpc-"+fp+"."+kind+".yaml"), data, 0o644))
	}

	s := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(dir))
	_, err = s.Record(context.Background(), adapter, req, func() (xrr.Response, error) {
		t.Fatal("do() must not run in replay")
		return nil, nil
	})
	assert.ErrorIs(t, err, xrr.ErrShapeMismatch)
}

// ── record: fake live stream ───────────────────────────────────────────────

func TestStreamRecordServerStream(t *testing.T) {
	s, dir := recordSession(t)
	fake := &fakeStream{recvQueue: [][]byte{[]byte("chunk-one\n"), []byte("chunk-two\n"), []byte("chunk-three\n")}, terminal: io.EOF}
	cs := openStream(t, s, serverDesc, "/files.FileService/Download", fake.streamer())

	require.NoError(t, sendRaw(t, cs, `{"path":"/etc/hosts"}`))
	require.NoError(t, cs.CloseSend())
	got, err := recvAll(t, cs)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, []string{"chunk-one\n", "chunk-two\n", "chunk-three\n"}, got)
	assert.True(t, fake.closeSent)

	// Cassette lands at the spec's server fingerprint for these open inputs.
	pair, err := xrr.NewFileCassette(dir).LoadStream("grpc", "58a4bf3f")
	require.NoError(t, err)
	assert.Equal(t, xrr.StreamServer, pair.Req.Type)
	require.Len(t, pair.Req.Frames, 1)
	assert.Equal(t, []byte(`{"path":"/etc/hosts"}`), pair.Req.Frames[0].Message)
	require.NotNil(t, pair.Req.HalfClose)
	assert.Equal(t, 1, pair.Req.HalfClose.Seq)
	require.Len(t, pair.Resp.Frames, 3)
	assert.Equal(t, 5, pair.Resp.End.Seq)
	assert.Equal(t, "files.FileService", pair.ReqPayload["service"])
	assert.Equal(t, "Download", pair.ReqPayload["method"])
	assert.NotContains(t, pair.ReqPayload, "n")
	assert.EqualValues(t, 0, pair.RespPayload["status_code"])
	assert.Empty(t, pair.RecordedErr)
}

func TestStreamRecordClientStream(t *testing.T) {
	s, dir := recordSession(t)
	fake := &fakeStream{recvQueue: [][]byte{[]byte(`{"received_bytes":18}`)}, terminal: io.EOF}
	cs := openStream(t, s, clientDesc, "/files.FileService/Upload", fake.streamer())

	require.NoError(t, sendRaw(t, cs, "part-one\n"))
	require.NoError(t, sendRaw(t, cs, "part-two\n"))
	require.NoError(t, cs.CloseSend())
	var m []byte
	require.NoError(t, cs.RecvMsg(&m))
	assert.Equal(t, `{"received_bytes":18}`, string(m))

	// A generated client never reads again after the single response: the
	// pair must already be persisted (recv of the response is the terminal
	// on a non-server-streaming RPC).
	pair, err := xrr.NewFileCassette(dir).LoadStream("grpc", "2bebfd6f")
	require.NoError(t, err)
	assert.Equal(t, xrr.StreamClient, pair.Req.Type)
	require.Len(t, pair.Req.Frames, 2)
	require.NotNil(t, pair.Req.HalfClose)
	assert.Equal(t, 2, pair.Req.HalfClose.Seq)
	require.Len(t, pair.Resp.Frames, 1)
	assert.Equal(t, 3, pair.Resp.Frames[0].Seq)
	assert.Equal(t, 4, pair.Resp.End.Seq)
	assert.EqualValues(t, 0, pair.ReqPayload["n"])
	assert.EqualValues(t, 0, pair.RespPayload["status_code"])

	// An extra read after the terminal must not disturb the recording.
	assert.ErrorIs(t, cs.RecvMsg(&m), io.EOF)
	again, err := xrr.NewFileCassette(dir).LoadStream("grpc", "2bebfd6f")
	require.NoError(t, err)
	assert.Equal(t, 4, again.Resp.End.Seq)
}

func TestStreamRecordBidi(t *testing.T) {
	s, dir := recordSession(t)
	fake := &fakeStream{recvQueue: [][]byte{[]byte("pong-1"), []byte("pong-2")}, terminal: io.EOF}
	cs := openStream(t, s, bidiDesc, "/chat.ChatService/Converse", fake.streamer())

	require.NoError(t, sendRaw(t, cs, "ping-1"))
	var m []byte
	require.NoError(t, cs.RecvMsg(&m))
	require.NoError(t, sendRaw(t, cs, "ping-2"))
	require.NoError(t, cs.RecvMsg(&m))
	require.NoError(t, cs.CloseSend())
	assert.ErrorIs(t, cs.RecvMsg(&m), io.EOF)

	pair, err := xrr.NewFileCassette(dir).LoadStream("grpc", "c6233d2e")
	require.NoError(t, err)
	assert.Equal(t, xrr.StreamBidi, pair.Req.Type)
	// Interleaving recorded in arrival order: send 0, recv 1, send 2,
	// recv 3, half_close 4, end 5 — the spec's worked bidi example.
	require.Len(t, pair.Req.Frames, 2)
	assert.Equal(t, 0, pair.Req.Frames[0].Seq)
	assert.Equal(t, 2, pair.Req.Frames[1].Seq)
	require.Len(t, pair.Resp.Frames, 2)
	assert.Equal(t, 1, pair.Resp.Frames[0].Seq)
	assert.Equal(t, 3, pair.Resp.Frames[1].Seq)
	require.NotNil(t, pair.Req.HalfClose)
	assert.Equal(t, 4, pair.Req.HalfClose.Seq)
	assert.Equal(t, 5, pair.Resp.End.Seq)
}

func TestStreamRecordErrorTerminalRoundTrip(t *testing.T) {
	s, dir := recordSession(t)
	liveErr := status.Error(codes.Unavailable, "connection reset")
	fake := &fakeStream{recvQueue: [][]byte{[]byte("log-chunk-1\n"), []byte("log-chunk-2\n")}, terminal: liveErr}
	cs := openStream(t, s, serverDesc, "/files.FileService/Download", fake.streamer())

	require.NoError(t, sendRaw(t, cs, `{"path":"/var/log/big.log"}`))
	require.NoError(t, cs.CloseSend())
	got, err := recvAll(t, cs)
	assert.Equal(t, []string{"log-chunk-1\n", "log-chunk-2\n"}, got)
	require.ErrorIs(t, err, liveErr)

	pair, err := xrr.NewFileCassette(dir).LoadStream("grpc", "9e8c4d4c")
	require.NoError(t, err)
	assert.EqualValues(t, 14, pair.RespPayload["status_code"])
	assert.Equal(t, "rpc error: code = Unavailable desc = connection reset", pair.RecordedErr)

	// Replaying the just-recorded cassette reproduces the client-observed
	// behaviour byte-for-byte, error rendering included.
	rs := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(dir))
	rcs := openStream(t, rs, serverDesc, "/files.FileService/Download", failStreamer(t))
	require.NoError(t, sendRaw(t, rcs, `{"path":"/var/log/big.log"}`))
	require.NoError(t, rcs.CloseSend())
	rgot, rerr := recvAll(t, rcs)
	assert.Equal(t, got, rgot)
	require.Error(t, rerr)
	assert.Equal(t, codes.Unavailable, status.Code(rerr))
	assert.Equal(t, liveErr.Error(), rerr.Error())
}

func TestStreamRecordStreamerFailureWritesNothing(t *testing.T) {
	s, dir := recordSession(t)
	boom := errors.New("dial refused")
	streamer := func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, ...grpc.CallOption) (grpc.ClientStream, error) {
		return nil, boom
	}
	_, err := xgrpc.StreamClientInterceptor(s)(context.Background(), bidiDesc, nil, "/chat.ChatService/Converse", streamer)
	require.ErrorIs(t, err, boom)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries, "no terminal observed ⇒ no cassette")
}

func TestStreamRecordTypedProtoMessages(t *testing.T) {
	// The typed-message half of the replay gotcha: recorded frames are wire
	// bytes; replay must unmarshal them into the caller's proto message, and
	// send validation must compare marshaled wire bytes.
	s, dir := recordSession(t)
	pong, err := proto.Marshal(wrapperspb.String("pong-1"))
	require.NoError(t, err)
	fake := &fakeStream{recvQueue: [][]byte{pong}, terminal: io.EOF}
	cs := openStream(t, s, bidiDesc, "/chat.ChatService/Converse", fake.streamer())

	require.NoError(t, cs.SendMsg(wrapperspb.String("ping-1")))
	reply := new(wrapperspb.StringValue)
	require.NoError(t, cs.RecvMsg(reply))
	assert.Equal(t, "pong-1", reply.GetValue())
	require.NoError(t, cs.CloseSend())
	require.ErrorIs(t, cs.RecvMsg(new(wrapperspb.StringValue)), io.EOF)

	// Replay with typed messages end-to-end.
	rs := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(dir))
	rcs := openStream(t, rs, bidiDesc, "/chat.ChatService/Converse", failStreamer(t))
	require.NoError(t, rcs.SendMsg(wrapperspb.String("ping-1")))
	rreply := new(wrapperspb.StringValue)
	require.NoError(t, rcs.RecvMsg(rreply))
	assert.Equal(t, "pong-1", rreply.GetValue())
	require.NoError(t, rcs.CloseSend())
	assert.ErrorIs(t, rcs.RecvMsg(new(wrapperspb.StringValue)), io.EOF)

	// Divergent typed send mismatches on wire bytes.
	rs2 := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(dir))
	rcs2 := openStream(t, rs2, bidiDesc, "/chat.ChatService/Converse", failStreamer(t))
	assert.ErrorIs(t, rcs2.SendMsg(wrapperspb.String("ping-other")), xrr.ErrStreamMismatch)
}

// ── passthrough ────────────────────────────────────────────────────────────

func TestStreamPassthroughReturnsLiveStream(t *testing.T) {
	dir := t.TempDir()
	s := xrr.NewSession(xrr.ModePassthrough, xrr.NewFileCassette(dir))
	fake := &fakeStream{terminal: io.EOF}
	cs, err := xgrpc.StreamClientInterceptor(s)(context.Background(), bidiDesc, nil, "/chat.ChatService/Converse", fake.streamer())
	require.NoError(t, err)
	assert.Same(t, grpc.ClientStream(fake), cs, "passthrough must hand back the live stream untouched")

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}
