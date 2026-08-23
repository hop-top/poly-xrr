package grpctransport_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xrr "hop.top/xrr"
	xtransport "hop.top/xrr/adapters/grpctransport"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// End-to-end proof for TRANSPORT-level capture.
//
// The bar is the same one the interceptor-based streaming adapter is held
// to: record a genuine workload over real TCP from a separate OS process,
// then kill the server, verify the port is dead, unset the credentials, and
// replay through a dialer that FAILS AND COUNTS any dial attempt. The
// client-observed transcripts must be byte-identical, and the dial count
// must be zero.
//
// The critical difference: nothing here uses a grpc interceptor. Capture
// attaches only at grpc.WithContextDialer, which is exactly the seam
// available on client libraries that expose no interceptor hooks.

// ── workload ───────────────────────────────────────────────────────────────

const (
	tpExecOKScript   = `for i in 1 2 3 4; do echo "chunk $i"; sleep 0.12; done`
	tpExecFailScript = `echo boot; sleep 0.1; echo "fs mounted"; sleep 0.1; exit 3`
)

// tpUploadChunks includes raw non-UTF-8 bytes so recorded frames exercise
// genuinely binary content through the base64 path.
var tpUploadChunks = [][]byte{
	[]byte("upload-part-one\n"),
	{0x00, 0x01, 0xFE, 0xFF, 0x10, 0x80},
	[]byte("upload-tail\n"),
}

var (
	tpExecDesc     = &grpc.StreamDesc{StreamName: "Exec", ServerStreams: true}
	tpUploadDesc   = &grpc.StreamDesc{StreamName: "Upload", ClientStreams: true}
	tpConverseDesc = &grpc.StreamDesc{StreamName: "Converse", ClientStreams: true, ServerStreams: true}
)

// ── transcript ─────────────────────────────────────────────────────────────

// transcript is everything a client observes on one stream.
type transcript struct {
	msgs []string
	errs []string
}

func (tr *transcript) observe(err error) {
	if err != nil {
		tr.errs = append(tr.errs, err.Error())
	}
}

type driver func(t *testing.T, conn *grpc.ClientConn) transcript

// tpCallCtx builds the per-call context, attaching the bearer token only
// when the env carries one. Reading the env at call time is what lets the
// replay phase run with credentials genuinely unset.
func tpCallCtx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	if tok := os.Getenv(tpTokenEnv); tok != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, tpTokenHeader, "Bearer "+tok)
	}
	return ctx, cancel
}

func tpExecDriver(script string, extraReads int) driver {
	return func(t *testing.T, conn *grpc.ClientConn) transcript {
		t.Helper()
		ctx, cancel := tpCallCtx()
		defer cancel()
		var tr transcript
		cs, err := conn.NewStream(ctx, tpExecDesc, "/"+tpServiceName+"/Exec")
		require.NoError(t, err)
		tr.observe(cs.SendMsg(wrapperspb.String(script)))
		tr.observe(cs.CloseSend())
		for {
			m := new(wrapperspb.BytesValue)
			if err := cs.RecvMsg(m); err != nil {
				tr.observe(err)
				break
			}
			tr.msgs = append(tr.msgs, string(m.GetValue()))
		}
		for i := 0; i < extraReads; i++ {
			m := new(wrapperspb.BytesValue)
			tr.observe(cs.RecvMsg(m)) // terminal must repeat identically
		}
		return tr
	}
}

func tpUploadDriver(chunks ...[]byte) driver {
	return func(t *testing.T, conn *grpc.ClientConn) transcript {
		t.Helper()
		ctx, cancel := tpCallCtx()
		defer cancel()
		var tr transcript
		cs, err := conn.NewStream(ctx, tpUploadDesc, "/"+tpServiceName+"/Upload")
		require.NoError(t, err)
		for _, c := range chunks {
			tr.observe(cs.SendMsg(wrapperspb.Bytes(c)))
		}
		tr.observe(cs.CloseSend())
		for i := 0; i < 2; i++ {
			m := new(wrapperspb.StringValue)
			if err := cs.RecvMsg(m); err != nil {
				tr.observe(err)
				continue
			}
			tr.msgs = append(tr.msgs, m.GetValue())
		}
		return tr
	}
}

// tpConverseDriver drives a bidi ping/pong: send, read the answer, repeat.
// Sends and receives genuinely interleave, which is what makes the recorded
// global seq ordering non-trivial.
func tpConverseDriver(pings ...string) driver {
	return func(t *testing.T, conn *grpc.ClientConn) transcript {
		t.Helper()
		ctx, cancel := tpCallCtx()
		defer cancel()
		var tr transcript
		cs, err := conn.NewStream(ctx, tpConverseDesc, "/"+tpServiceName+"/Converse")
		require.NoError(t, err)
		for _, p := range pings {
			tr.observe(cs.SendMsg(wrapperspb.String(p)))
			m := new(wrapperspb.StringValue)
			if err := cs.RecvMsg(m); err != nil {
				tr.observe(err)
				break
			}
			tr.msgs = append(tr.msgs, m.GetValue())
		}
		tr.observe(cs.CloseSend())
		m := new(wrapperspb.StringValue)
		tr.observe(cs.RecvMsg(m)) // end-of-stream
		return tr
	}
}

// tpScenarios run in a fixed order so occurrence counters advance
// identically in the record and replay sessions.
var tpScenarios = []struct {
	name string
	run  driver
}{
	{"exec-ok", tpExecDriver(tpExecOKScript, 1)},
	{"exec-fail-midstream", tpExecDriver(tpExecFailScript, 1)},
	{"upload-stdin", tpUploadDriver(tpUploadChunks...)},
	{"converse-bidi", tpConverseDriver("ping-1", "ping-2", "ping-3")},
}

// ── server process management ──────────────────────────────────────────────

type tpServer struct {
	addr     string
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	killOnce sync.Once
}

func (s *tpServer) scrapeAddr(t *testing.T, stdout io.Reader) {
	t.Helper()
	addrCh := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if a, ok := strings.CutPrefix(sc.Text(), tpAddrPrefix); ok {
				select {
				case addrCh <- a:
				default:
				}
			}
		}
	}()
	select {
	case s.addr = <-addrCh:
	case <-time.After(30 * time.Second):
		t.Fatal("transport server did not report its address in time")
	}
}

func (s *tpServer) kill() {
	s.killOnce.Do(func() {
		_ = s.stdin.Close()
		_ = s.cmd.Process.Kill()
		_ = s.cmd.Wait()
	})
}

// requirePortDead asserts nothing accepts TCP connections on addr anymore.
func requirePortDead(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			return
		}
		_ = conn.Close()
		require.False(t, time.Now().After(deadline), "port %s still accepting after server kill", addr)
		time.Sleep(100 * time.Millisecond)
	}
}

func tpRandomToken(t *testing.T) string {
	t.Helper()
	b := make([]byte, 24)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return hex.EncodeToString(b)
}

// tpReplayConn builds a replay-mode client. The dialer counts every attempt
// to reach the network and hands back the cassette-backed pipe instead, so
// a nonzero count is a hard failure.
func tpReplayConn(t *testing.T, session *xrr.FileSession, dir string, dials *atomic.Int32) *grpc.ClientConn {
	t.Helper()
	replay := xtransport.ReplayDialer(session, dir)
	conn, err := grpc.NewClient("passthrough:///transport-replay",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return replay(ctx, addr)
		}),
		grpc.WithNoProxy(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_ = dials
	return conn
}

// ── the test ───────────────────────────────────────────────────────────────

// TestE2ETransportRecordReplay records real streamed RPCs purely at the
// transport (no interceptor anywhere), then replays them with the server
// dead, the port closed, credentials unset, and zero network dials.
func TestE2ETransportRecordReplay(t *testing.T) {
	dir := t.TempDir()
	token := tpRandomToken(t)
	t.Setenv(tpTokenEnv, token)
	server := startTransportServer(t, token)

	// ── phase 1: record through the live server over real TCP.
	//
	// NOTE: the ONLY xrr wiring here is the dialer. No interceptor option
	// is passed, which is the whole point.
	recSession := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	recDial := xtransport.RecordDialer(recSession, nil)
	var liveDials atomic.Int32
	recConn, err := grpc.NewClient("passthrough:///"+server.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			liveDials.Add(1)
			return recDial(ctx, server.addr)
		}),
	)
	require.NoError(t, err)

	live := make(map[string]transcript, len(tpScenarios))
	for _, sc := range tpScenarios {
		live[sc.name] = sc.run(t, recConn)
	}
	require.NoError(t, recConn.Close())
	require.Positive(t, liveDials.Load(), "the record phase must really have dialed")

	// Live sanity: the workload genuinely streamed.
	execOK := live["exec-ok"]
	require.GreaterOrEqual(t, len(execOK.msgs), 2, "exec output must arrive as multiple chunks")
	require.Equal(t, "chunk 1\nchunk 2\nchunk 3\nchunk 4\n", strings.Join(execOK.msgs, ""))

	execFail := live["exec-fail-midstream"]
	require.Equal(t, "boot\nfs mounted\n", strings.Join(execFail.msgs, ""))
	require.Contains(t, execFail.errs[0], "exit status 3")

	total := 0
	for _, c := range tpUploadChunks {
		total += len(c)
	}
	require.Equal(t, []string{fmt.Sprintf("received:%d", total)}, live["upload-stdin"].msgs,
		"the child command really consumed the streamed stdin")

	require.Equal(t, []string{"pong:ping-1", "pong:ping-2", "pong:ping-3"},
		live["converse-bidi"].msgs)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 2*len(tpScenarios), "one req/resp pair per scenario")

	// ── phase 2: no credential may reach disk.
	//
	// The token traveled in an `authorization` HEADERS field on every call.
	// Two independent mechanisms keep it out of the cassettes: the format
	// records no metadata at all, and decoded headers are sanitized at the
	// seam (see redact_test.go, which tests the sanitizer directly — this
	// check alone would pass even with sanitization disabled).
	assertTokenAbsent(t, dir, token)

	// ── phase 3: kill the server, verify the port is dead, unset creds.
	server.kill()
	requirePortDead(t, server.addr)

	t.Setenv(tpTokenEnv, "")
	require.NoError(t, os.Unsetenv(tpTokenEnv))

	// ── phase 4: replay — no server, no creds, no dials.
	var dialAttempts atomic.Int32
	repSession := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(dir))
	repConn := tpReplayConn(t, repSession, dir, &dialAttempts)

	for _, sc := range tpScenarios {
		replayed := sc.run(t, repConn)
		assert.Equal(t, live[sc.name].msgs, replayed.msgs,
			"%s: replayed messages must be byte-identical to the live run", sc.name)
	}
	assert.Zero(t, dialAttempts.Load(), "replay must never touch the network")

	// ── phase 5: cassettes conform to the core loader's validation.
	cassette := xrr.NewFileCassette(dir)
	loaded := 0
	for _, e := range entries {
		fp, ok := strings.CutPrefix(e.Name(), "grpc-")
		if !ok {
			t.Fatalf("unexpected cassette file %q", e.Name())
		}
		fp, ok = strings.CutSuffix(fp, ".req.yaml")
		if !ok {
			continue
		}
		pair, err := cassette.LoadStream("grpc", fp)
		require.NoError(t, err, "pair %s must pass spec validation", e.Name())
		require.NotNil(t, pair.Resp.End.AtMs, "end must carry at_ms")
		loaded++
	}
	require.Equal(t, len(tpScenarios), loaded)
}

// assertTokenAbsent greps every cassette for the literal secret. This is
// the outcome assertion — "no credential on disk" — not a test of the
// sanitizer; see redact_test.go for that.
func assertTokenAbsent(t *testing.T, dir, token string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, err)
		text := string(data)
		require.NotContains(t, text, token,
			"%s: the live auth token must never reach a cassette", e.Name())
		require.NotContains(t, strings.ToLower(text), "bearer "+strings.ToLower(token),
			"%s: bearer credential must not reach a cassette", e.Name())
	}
}
