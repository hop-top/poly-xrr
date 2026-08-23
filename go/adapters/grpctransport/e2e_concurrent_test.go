package grpctransport_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	xrr "hop.top/xrr"
	xtransport "hop.top/xrr/adapters/grpctransport"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Concurrent multiplexing is the main correctness risk of transport-level
// capture: ONE TCP connection carries many simultaneous RPCs, their HTTP/2
// frames arbitrarily interleaved and distinguished only by stream ID. A
// demultiplexing bug shows up here as messages attributed to the wrong RPC
// — which is exactly the kind of corruption a single-stream test cannot
// see, because with one stream every frame belongs to it by default.
//
// The workload makes misattribution detectable rather than merely possible:
// every concurrent RPC carries a DISTINCT payload, and each response echoes
// its own input, so any cross-talk produces a wrong string rather than a
// merely reordered one.

const tpConcurrency = 8

// TestE2ETransportConcurrentStreams records many interleaved RPCs over one
// connection and replays them, asserting each stream keeps its own messages.
func TestE2ETransportConcurrentStreams(t *testing.T) {
	dir := t.TempDir()
	token := tpRandomToken(t)
	t.Setenv(tpTokenEnv, token)
	server := startTransportServer(t, token)

	// ── record: fire N server-streaming execs at once on ONE conn.
	recSession := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	recDial := xtransport.RecordDialer(recSession, nil)
	recConn, err := grpc.NewClient("passthrough:///"+server.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return recDial(ctx, server.addr)
		}),
	)
	require.NoError(t, err)

	live := runConcurrent(t, recConn)
	require.NoError(t, recConn.Close())

	// Each stream must have received exactly its own payload back.
	for i, got := range live {
		require.Equal(t, expectedConcurrentOutput(i), got,
			"stream %d got another stream's data during RECORD (demux bug)", i)
	}

	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, files, 2*tpConcurrency,
		"each concurrent RPC must produce its own cassette pair")

	// ── replay: same concurrency, no server, no dials.
	server.kill()
	requirePortDead(t, server.addr)

	var servedFromCassette atomic.Int32
	repSession := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(dir))
	repConn := tpReplayConn(t, repSession, dir, &servedFromCassette)

	replayed := runConcurrent(t, repConn)
	for i := range replayed {
		assert.Equal(t, live[i], replayed[i],
			"stream %d replayed another stream's data (demux bug)", i)
	}
	assert.Positive(t, servedFromCassette.Load(),
		"the replay path must actually have been exercised")
}

// expectedConcurrentOutput is the exact stdout the i-th script produces.
func expectedConcurrentOutput(i int) string {
	return fmt.Sprintf("stream-%d-a\nstream-%d-b\n", i, i)
}

// concurrentScript emits two distinct chunks with a gap, so the RPCs really
// overlap on the wire instead of completing one after another.
func concurrentScript(i int) string {
	return fmt.Sprintf(`echo "stream-%d-a"; sleep 0.15; echo "stream-%d-b"`, i, i)
}

// runConcurrent drives tpConcurrency Exec streams simultaneously on one
// connection and returns each stream's concatenated output, indexed by
// stream number.
func runConcurrent(t *testing.T, conn *grpc.ClientConn) []string {
	t.Helper()
	out := make([]string, tpConcurrency)
	var wg sync.WaitGroup
	for i := 0; i < tpConcurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := tpCallCtx()
			defer cancel()
			cs, err := conn.NewStream(ctx, tpExecDesc, "/"+tpServiceName+"/Exec")
			if !assert.NoError(t, err) {
				return
			}
			if !assert.NoError(t, cs.SendMsg(wrapperspb.String(concurrentScript(i)))) {
				return
			}
			assert.NoError(t, cs.CloseSend())
			var sb strings.Builder
			for {
				m := new(wrapperspb.BytesValue)
				if err := cs.RecvMsg(m); err != nil {
					break
				}
				sb.Write(m.GetValue())
			}
			out[i] = sb.String()
		}(i)
	}
	wg.Wait()
	return out
}
