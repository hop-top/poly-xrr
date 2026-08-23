// Package xrr_test — gRPC streaming e2e: record against a live in-process
// server, stop the server, replay from cassettes only, and assert identical
// client-observed behaviour (messages, EOF, status errors) for all three
// streaming modes plus mid-stream-error and empty-stream cases.
package xrr_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"

	xrr "hop.top/xrr"
	xgrpc "hop.top/xrr/adapters/grpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ── hand-rolled test service (no protoc; wrapperspb carries the payloads) ──

const streamServiceName = "xrrtest.StreamService"

var streamServiceDesc = grpc.ServiceDesc{
	ServiceName: streamServiceName,
	Streams: []grpc.StreamDesc{
		{StreamName: "Download", Handler: downloadHandler, ServerStreams: true},
		{StreamName: "Upload", Handler: uploadHandler, ClientStreams: true},
		{StreamName: "Converse", Handler: converseHandler, ClientStreams: true, ServerStreams: true},
	},
}

// downloadHandler streams chunks for a named file; "empty" streams nothing,
// "boom" fails mid-stream after two chunks.
func downloadHandler(_ any, stream grpc.ServerStream) error {
	req := new(wrapperspb.StringValue)
	if err := stream.RecvMsg(req); err != nil {
		return err
	}
	switch req.GetValue() {
	case "empty":
		return nil
	case "boom":
		for _, c := range []string{"log-chunk-1", "log-chunk-2"} {
			if err := stream.SendMsg(wrapperspb.String(c)); err != nil {
				return err
			}
		}
		return status.Error(codes.Unavailable, "connection reset")
	default:
		for i := 1; i <= 3; i++ {
			if err := stream.SendMsg(wrapperspb.String(fmt.Sprintf("%s-chunk-%d", req.GetValue(), i))); err != nil {
				return err
			}
		}
		return nil
	}
}

// uploadHandler consumes the client stream and answers with a byte total.
func uploadHandler(_ any, stream grpc.ServerStream) error {
	total := 0
	for {
		part := new(wrapperspb.StringValue)
		if err := stream.RecvMsg(part); err != nil {
			if err == io.EOF {
				return stream.SendMsg(wrapperspb.String(fmt.Sprintf("received:%d", total)))
			}
			return err
		}
		total += len(part.GetValue())
	}
}

// converseHandler pongs every ping until the client half-closes.
func converseHandler(_ any, stream grpc.ServerStream) error {
	for {
		ping := new(wrapperspb.StringValue)
		if err := stream.RecvMsg(ping); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := stream.SendMsg(wrapperspb.String("pong:" + ping.GetValue())); err != nil {
			return err
		}
	}
}

// ── client-side drivers (shared verbatim between record and replay runs) ───

var (
	downloadDesc = &grpc.StreamDesc{StreamName: "Download", ServerStreams: true}
	uploadDesc   = &grpc.StreamDesc{StreamName: "Upload", ClientStreams: true}
	converseDesc = &grpc.StreamDesc{StreamName: "Converse", ClientStreams: true, ServerStreams: true}
)

// transcript is everything a client observes on one stream.
type transcript struct {
	msgs []string
	errs []string // non-nil operation results, in order (io.EOF included)
}

func (tr *transcript) observe(err error) {
	if err != nil {
		tr.errs = append(tr.errs, err.Error())
	}
}

func (tr *transcript) recv(t *testing.T, cs grpc.ClientStream) bool {
	t.Helper()
	m := new(wrapperspb.StringValue)
	if err := cs.RecvMsg(m); err != nil {
		tr.observe(err)
		return false
	}
	tr.msgs = append(tr.msgs, m.GetValue())
	return true
}

type driver func(t *testing.T, conn *grpc.ClientConn) transcript

func downloadDriver(file string, extraReads int) driver {
	return func(t *testing.T, conn *grpc.ClientConn) transcript {
		t.Helper()
		var tr transcript
		cs, err := conn.NewStream(context.Background(), downloadDesc, "/"+streamServiceName+"/Download")
		require.NoError(t, err)
		tr.observe(cs.SendMsg(wrapperspb.String(file)))
		tr.observe(cs.CloseSend())
		for tr.recv(t, cs) {
		}
		for i := 0; i < extraReads; i++ {
			tr.recv(t, cs) // terminal must repeat identically
		}
		return tr
	}
}

func uploadDriver(parts ...string) driver {
	return func(t *testing.T, conn *grpc.ClientConn) transcript {
		t.Helper()
		var tr transcript
		cs, err := conn.NewStream(context.Background(), uploadDesc, "/"+streamServiceName+"/Upload")
		require.NoError(t, err)
		for _, p := range parts {
			tr.observe(cs.SendMsg(wrapperspb.String(p)))
		}
		tr.observe(cs.CloseSend())
		tr.recv(t, cs) // the single response (CloseAndRecv shape)
		tr.recv(t, cs) // one read past the response
		return tr
	}
}

func converseDriver(pings ...string) driver {
	return func(t *testing.T, conn *grpc.ClientConn) transcript {
		t.Helper()
		var tr transcript
		cs, err := conn.NewStream(context.Background(), converseDesc, "/"+streamServiceName+"/Converse")
		require.NoError(t, err)
		for _, p := range pings {
			tr.observe(cs.SendMsg(wrapperspb.String(p)))
			tr.recv(t, cs)
		}
		tr.observe(cs.CloseSend())
		tr.recv(t, cs) // end-of-stream
		return tr
	}
}

// scenarios run in a fixed order so the occurrence counters advance
// identically in the record and replay sessions.
var streamScenarios = []struct {
	name string
	run  driver
}{
	{"server-stream", downloadDriver("file", 1)},
	{"server-stream-empty", downloadDriver("empty", 0)},
	{"server-stream-error", downloadDriver("boom", 1)},
	{"client-stream", uploadDriver("part-one", "part-two", "part-three")},
	{"bidi", converseDriver("ping-1", "ping-2")},
}

// ── wiring ─────────────────────────────────────────────────────────────────

func startStreamServer(t *testing.T) (*grpc.Server, *bufconn.Listener) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	srv.RegisterService(&streamServiceDesc, nil)
	go func() { _ = srv.Serve(lis) }()
	return srv, lis
}

func streamClientConn(t *testing.T, session *xrr.FileSession, dialer func(context.Context, string) (net.Conn, error)) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(xgrpc.StreamClientInterceptor(session)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestE2EGRPCStream_RecordThenReplayServerStopped — records all streaming
// modes against a live in-process gRPC server, stops the server, then
// replays the same drivers from cassettes only and asserts the transcripts
// are identical.
func TestE2EGRPCStream_RecordThenReplayServerStopped(t *testing.T) {
	dir := t.TempDir()

	// ── phase 1: record against the live server
	srv, lis := startStreamServer(t)
	recSession := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	recConn := streamClientConn(t, recSession, func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})

	recorded := make(map[string]transcript, len(streamScenarios))
	for _, sc := range streamScenarios {
		recorded[sc.name] = sc.run(t, recConn)
	}
	require.NoError(t, recConn.Close())

	// Live sanity: the interesting shapes actually happened.
	assert.Equal(t, []string{"file-chunk-1", "file-chunk-2", "file-chunk-3"}, recorded["server-stream"].msgs)
	assert.Empty(t, recorded["server-stream-empty"].msgs)
	assert.Equal(t, []string{"log-chunk-1", "log-chunk-2"}, recorded["server-stream-error"].msgs)
	assert.Equal(t, []string{"received:26"}, recorded["client-stream"].msgs)
	assert.Equal(t, []string{"pong:ping-1", "pong:ping-2"}, recorded["bidi"].msgs)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 2*len(streamScenarios), "one req/resp pair per scenario")

	// ── phase 2: server STOPPED; replay must never touch the network
	srv.Stop()
	require.NoError(t, lis.Close())

	var dialAttempts atomic.Int32
	repSession := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(dir))
	repConn := streamClientConn(t, repSession, func(context.Context, string) (net.Conn, error) {
		dialAttempts.Add(1)
		return nil, fmt.Errorf("server is down")
	})

	for _, sc := range streamScenarios {
		t.Run(sc.name, func(t *testing.T) {
			replayed := sc.run(t, repConn)
			assert.Equal(t, recorded[sc.name].msgs, replayed.msgs, "replayed messages must match live run")
			assert.Equal(t, recorded[sc.name].errs, replayed.errs, "replayed errors must match live run")
		})
	}
	assert.Zero(t, dialAttempts.Load(), "replay must not dial")

	// The mid-stream error replays as a real gRPC status, code preserved.
	repSession2 := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(dir))
	repConn2 := streamClientConn(t, repSession2, func(context.Context, string) (net.Conn, error) {
		return nil, fmt.Errorf("server is down")
	})
	cs, err := repConn2.NewStream(context.Background(), downloadDesc, "/"+streamServiceName+"/Download")
	require.NoError(t, err)
	require.NoError(t, cs.SendMsg(wrapperspb.String("boom")))
	require.NoError(t, cs.CloseSend())
	m := new(wrapperspb.StringValue)
	require.NoError(t, cs.RecvMsg(m))
	require.NoError(t, cs.RecvMsg(m))
	err = cs.RecvMsg(m)
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Equal(t, "rpc error: code = Unavailable desc = connection reset", err.Error())
}

// TestE2EGRPCStream_ReplayCassetteMissServerStopped — replaying a stream
// that was never recorded fails loudly with ErrCassetteMiss, not a hang or
// a dial.
func TestE2EGRPCStream_ReplayCassetteMissServerStopped(t *testing.T) {
	session := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(t.TempDir()))
	conn := streamClientConn(t, session, func(context.Context, string) (net.Conn, error) {
		return nil, fmt.Errorf("server is down")
	})

	// client/bidi miss at open.
	_, err := conn.NewStream(context.Background(), uploadDesc, "/"+streamServiceName+"/Upload")
	require.ErrorIs(t, err, xrr.ErrCassetteMiss)

	// server-stream miss at the first send (fingerprint needs the message).
	cs, err := conn.NewStream(context.Background(), downloadDesc, "/"+streamServiceName+"/Download")
	require.NoError(t, err)
	require.ErrorIs(t, cs.SendMsg(wrapperspb.String("file")), xrr.ErrCassetteMiss)
}

// TestE2EGRPCStream_Passthrough — passthrough mode is transparent: live
// calls work, no cassette files are written.
func TestE2EGRPCStream_Passthrough(t *testing.T) {
	dir := t.TempDir()
	srv, lis := startStreamServer(t)
	defer srv.Stop()

	session := xrr.NewSession(xrr.ModePassthrough, xrr.NewFileCassette(dir))
	conn := streamClientConn(t, session, func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})

	tr := downloadDriver("file", 0)(t, conn)
	assert.Equal(t, []string{"file-chunk-1", "file-chunk-2", "file-chunk-3"}, tr.msgs)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries, "passthrough must not touch the cassette dir")
}
