// Package xrr_test — end-to-end validation of streamed recording against a
// realistic workload. The sandbox-shaped gRPC server (see
// e2e_stream_validation_server_test.go) runs in a SEPARATE OS PROCESS on a
// real localhost TCP port, and its handlers run REAL child commands, so the
// recorded streams carry genuine chunk boundaries and timing.
//
// Proof structure: record through the authenticated live server → kill the
// server process and verify the port is dead → scrub the auth token from
// the environment (and plant fake creds nothing should read) → replay with
// a dialer that counts and fails → assert the client-observed transcripts
// are byte-identical to the live run, the cassettes pass the core loader's
// validation, at_ms timing was recorded but not reproduced, and a corrupted
// cassette breaks transcript equality.
package xrr_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
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
	xgrpc "hop.top/xrr/adapters/grpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ── workload (real child commands; sleeps force multi-chunk streaming) ─────

const (
	valExecOKScript   = `for i in 1 2 3 4; do echo "chunk $i"; sleep 0.12; done`
	valExecFailScript = `echo boot; sleep 0.1; echo "fs mounted"; sleep 0.1; exit 3`
)

// valUploadChunks includes raw non-UTF-8 bytes so the recorded frames
// exercise the message_b64 encoding path with genuinely binary content.
var valUploadChunks = [][]byte{
	[]byte("upload-part-one\n"),
	{0x00, 0x01, 0xFE, 0xFF, 0x10, 0x80},
	[]byte("upload-tail\n"),
}

// ── drivers (shared verbatim between the record and replay phases) ─────────

var (
	valExecStreamDesc   = &grpc.StreamDesc{StreamName: "Exec", ServerStreams: true}
	valUploadStreamDesc = &grpc.StreamDesc{StreamName: "Upload", ClientStreams: true}
)

// valCallCtx builds the per-call context: a generous CI-safe timeout, plus
// the auth token metadata when the env carries one — reading the env at
// call time is what lets the replay phase run with credentials unset.
func valCallCtx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	if tok := os.Getenv(valTokenEnv); tok != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, valTokenHeader, tok)
	}
	return ctx, cancel
}

func valRecvBytes(tr *transcript, cs grpc.ClientStream) bool {
	m := new(wrapperspb.BytesValue)
	if err := cs.RecvMsg(m); err != nil {
		tr.observe(err)
		return false
	}
	tr.msgs = append(tr.msgs, string(m.GetValue()))
	return true
}

func valRecvString(tr *transcript, cs grpc.ClientStream) {
	m := new(wrapperspb.StringValue)
	if err := cs.RecvMsg(m); err != nil {
		tr.observe(err)
		return
	}
	tr.msgs = append(tr.msgs, m.GetValue())
}

func valExecDriver(script string, extraReads int) driver {
	return func(t *testing.T, conn *grpc.ClientConn) transcript {
		t.Helper()
		ctx, cancel := valCallCtx()
		defer cancel()
		var tr transcript
		cs, err := conn.NewStream(ctx, valExecStreamDesc, "/"+valServiceName+"/Exec")
		require.NoError(t, err)
		tr.observe(cs.SendMsg(wrapperspb.String(script)))
		tr.observe(cs.CloseSend())
		for valRecvBytes(&tr, cs) {
		}
		for i := 0; i < extraReads; i++ {
			valRecvBytes(&tr, cs) // terminal must repeat identically
		}
		return tr
	}
}

func valUploadDriver(chunks ...[]byte) driver {
	return func(t *testing.T, conn *grpc.ClientConn) transcript {
		t.Helper()
		ctx, cancel := valCallCtx()
		defer cancel()
		var tr transcript
		cs, err := conn.NewStream(ctx, valUploadStreamDesc, "/"+valServiceName+"/Upload")
		require.NoError(t, err)
		for _, c := range chunks {
			tr.observe(cs.SendMsg(wrapperspb.Bytes(c)))
		}
		tr.observe(cs.CloseSend())
		valRecvString(&tr, cs) // the single response (CloseAndRecv shape)
		valRecvString(&tr, cs) // one read past it: end-of-stream
		return tr
	}
}

// valScenarios run in a fixed order so occurrence counters advance
// identically in the record and replay sessions.
var valScenarios = []struct {
	name string
	run  driver
}{
	{"exec-ok", valExecDriver(valExecOKScript, 1)},
	{"exec-fail-midstream", valExecDriver(valExecFailScript, 1)},
	{"upload-stdin", valUploadDriver(valUploadChunks...)},
}

// ── server process management ──────────────────────────────────────────────

type valServer struct {
	addr     string
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	killOnce sync.Once
}

// startValidationServerProcess re-execs the test binary as the validation
// server (a separate OS process), passing the auth token through its env,
// and scrapes the ephemeral listen address off its stdout.
func startValidationServerProcess(t *testing.T, token string) *valServer {
	t.Helper()
	bin, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), valServerEnv+"=1", valTokenEnv+"="+token)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	addrCh := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if a, ok := strings.CutPrefix(sc.Text(), valAddrPrefix); ok {
				select {
				case addrCh <- a:
				default:
				}
			}
		}
	}()

	vs := &valServer{cmd: cmd, stdin: stdin}
	t.Cleanup(vs.kill) // safety net; the test kills it explicitly mid-flow
	select {
	case vs.addr = <-addrCh:
	case <-time.After(30 * time.Second):
		t.Fatal("validation server did not report its address in time")
	}
	return vs
}

// kill terminates the server process and reaps it. Idempotent.
func (s *valServer) kill() {
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

// ── helpers ────────────────────────────────────────────────────────────────

func valRandomToken(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return hex.EncodeToString(b)
}

func valCopyDir(t *testing.T, src, dst string) {
	t.Helper()
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dst, e.Name()), data, 0o644))
	}
}

// valReplayConn builds a replay-mode client whose dialer never succeeds —
// any network attempt is counted and fails.
func valReplayConn(t *testing.T, session *xrr.FileSession, dials *atomic.Int32, target string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+target,
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			if dials != nil {
				dials.Add(1)
			}
			return nil, fmt.Errorf("network is dead")
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(xgrpc.StreamClientInterceptor(session)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// ── the validation test ────────────────────────────────────────────────────

// TestE2EStreamValidation_RealProcessRecordReplay is the honest end-to-end
// proof for streamed recording: a genuine server-streaming exec workload is
// recorded over real TCP from a separate OS process, then replayed with the
// server dead, the port closed, credentials scrubbed, and zero dials — and
// the client-observed transcripts must be byte-identical.
func TestE2EStreamValidation_RealProcessRecordReplay(t *testing.T) {
	dir := t.TempDir()

	// ── phase 1: record against the live server (separate process, real TCP)
	token := valRandomToken(t)
	t.Setenv(valTokenEnv, token)
	server := startValidationServerProcess(t, token)

	// Auth sanity: without the token the live server refuses to serve, so
	// the recording made below demonstrably required real credentials.
	authCtx, authCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer authCancel()
	rawConn, err := grpc.NewClient("passthrough:///"+server.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	cs, err := rawConn.NewStream(authCtx, valExecStreamDesc, "/"+valServiceName+"/Exec")
	require.NoError(t, err)
	require.NoError(t, cs.SendMsg(wrapperspb.String("echo hi")))
	require.NoError(t, cs.CloseSend())
	err = cs.RecvMsg(new(wrapperspb.BytesValue))
	require.Error(t, err)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.NoError(t, rawConn.Close())

	recSession := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	recConn, err := grpc.NewClient("passthrough:///"+server.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(xgrpc.StreamClientInterceptor(recSession)),
	)
	require.NoError(t, err)

	live := make(map[string]transcript, len(valScenarios))
	for _, sc := range valScenarios {
		live[sc.name] = sc.run(t, recConn)
	}
	require.NoError(t, recConn.Close())

	// Live sanity: exec output genuinely streamed as multiple chunks.
	execOK := live["exec-ok"]
	require.GreaterOrEqual(t, len(execOK.msgs), 2, "exec output must arrive as multiple chunks over time")
	require.Equal(t, "chunk 1\nchunk 2\nchunk 3\nchunk 4\n", strings.Join(execOK.msgs, ""))
	require.Equal(t, []string{"EOF", "EOF"}, execOK.errs)

	execFail := live["exec-fail-midstream"]
	require.Equal(t, "boot\nfs mounted\n", strings.Join(execFail.msgs, ""))
	require.Len(t, execFail.errs, 2)
	require.Equal(t, "rpc error: code = Aborted desc = exec failed: exit status 3", execFail.errs[0])
	require.Equal(t, execFail.errs[0], execFail.errs[1], "terminal error must repeat on the extra read")

	upload := live["upload-stdin"]
	total := 0
	for _, c := range valUploadChunks {
		total += len(c)
	}
	require.Equal(t, []string{fmt.Sprintf("received:%d", total)}, upload.msgs,
		"the child command really consumed the streamed stdin")
	require.Equal(t, []string{"EOF"}, upload.errs)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 2*len(valScenarios), "one req/resp pair per scenario")

	// ── phase 2: kill the server, verify the port is dead, scrub env
	server.kill()
	requirePortDead(t, server.addr)

	t.Setenv(valTokenEnv, "") // registers restoration; then truly unset
	require.NoError(t, os.Unsetenv(valTokenEnv))
	t.Setenv("SANDBOX_TOKEN_ID", "fake-token-id-never-read")
	t.Setenv("SANDBOX_TOKEN_SECRET", "fake-token-secret-never-read")

	// ── phase 3: replay — no server, no creds, zero dials
	var dialAttempts atomic.Int32
	repSession := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(dir))
	repConn := valReplayConn(t, repSession, &dialAttempts, "validation-replay")

	replayStart := time.Now()
	for _, sc := range valScenarios {
		replayed := sc.run(t, repConn)
		assert.Equal(t, live[sc.name].msgs, replayed.msgs,
			"%s: replayed messages must be byte-identical to the live run", sc.name)
		assert.Equal(t, live[sc.name].errs, replayed.errs,
			"%s: replayed errors must render identically to the live run", sc.name)
	}
	replayElapsed := time.Since(replayStart)
	assert.Zero(t, dialAttempts.Load(), "replay must never touch the network")

	// ── phase 4: cassettes on disk conform; timing recorded, not reproduced
	cassette := xrr.NewFileCassette(dir)
	loaded := 0
	for _, e := range entries {
		fp, ok := strings.CutPrefix(e.Name(), "grpc-")
		if !ok {
			t.Fatalf("unexpected cassette file %q", e.Name())
		}
		fp, ok = strings.CutSuffix(fp, ".req.yaml")
		if !ok {
			continue // .resp.yaml half of a pair
		}
		_, err := cassette.LoadStream("grpc", fp)
		require.NoError(t, err, "pair %s must pass the core loader's spec validation", e.Name())
		loaded++
	}
	require.Equal(t, len(valScenarios), loaded)

	execOKWire, err := proto.Marshal(wrapperspb.String(valExecOKScript))
	require.NoError(t, err)
	fpExecOK, err := xrr.StreamFingerprint(xrr.StreamOpen{
		AdapterID: "grpc", Type: xrr.StreamServer,
		Service: valServiceName, Method: "Exec", Message: execOKWire,
	}, -1)
	require.NoError(t, err)
	pair, err := cassette.LoadStream("grpc", fpExecOK)
	require.NoError(t, err)
	require.Equal(t, xrr.StreamServer, pair.Req.Type)
	require.GreaterOrEqual(t, len(pair.Resp.Frames), 2)
	for i, f := range pair.Resp.Frames {
		require.NotNil(t, f.AtMs, "resp frame %d must carry at_ms", i)
	}
	require.NotNil(t, pair.Resp.End.AtMs, "end must carry at_ms")
	require.GreaterOrEqual(t, *pair.Resp.End.AtMs, int64(300),
		"the live exec genuinely took wall-clock time (4 x sleep 0.12)")
	require.Less(t, replayElapsed, time.Duration(*pair.Resp.End.AtMs)*time.Millisecond,
		"replay must not reproduce recorded timing")

	// ── phase 5: a corrupted cassette must break transcript equality
	t.Run("corrupted-cassette-detected", func(t *testing.T) {
		mutDir := t.TempDir()
		valCopyDir(t, dir, mutDir)

		orig := base64.StdEncoding.EncodeToString(pair.Resp.Frames[0].Message)
		tamperedWire, err := proto.Marshal(wrapperspb.Bytes([]byte("tampered-chunk\n")))
		require.NoError(t, err)
		tampered := base64.StdEncoding.EncodeToString(tamperedWire)
		require.NotEqual(t, orig, tampered)

		respPath := filepath.Join(mutDir, "grpc-"+fpExecOK+".resp.yaml")
		data, err := os.ReadFile(respPath)
		require.NoError(t, err)
		require.Contains(t, string(data), orig, "recorded frame must be present to corrupt")
		require.NoError(t, os.WriteFile(respPath,
			[]byte(strings.Replace(string(data), orig, tampered, 1)), 0o644))

		mutSession := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(mutDir))
		mutConn := valReplayConn(t, mutSession, nil, "validation-mutated")
		mutated := valExecDriver(valExecOKScript, 1)(t, mutConn)
		assert.NotEqual(t, live["exec-ok"].msgs, mutated.msgs,
			"a corrupted cassette must not reproduce the live transcript")
	})
}
