package xrr_test

import (
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

// Identity-hook conformance — spec "Scrub Hook Obligations — Identity-Hook
// Matrix" (M1..M7).
//
// The scrub hook's contract is WHEN it runs and WHAT it receives, never
// what it computes; xrr defines no scrub algorithm. Two byte-neutral hooks
// generate the whole matrix:
//
//   - identity: returns its input. Installed and invoked but byte-neutral,
//     so any divergence from a no-hook session is a mechanics defect —
//     clause 7 fixes no-hook behaviour as the reference.
//   - counting: identity plus a call log. Reveals invocation points,
//     multiplicity, and — the part fixtures cannot see — non-invocation.

// identityScrub is the hook of clause 6's "MAY return the input unchanged"
// case: observable, byte-neutral.
func identityScrub(_ xrr.StreamDirection, _ xrr.StreamScrubInfo, data []byte) []byte {
	return data
}

// scrubCall is one observed invocation: the direction and the exact bytes
// the hook was handed.
type scrubCall struct {
	Dir  xrr.StreamDirection
	Data string
}

// countingScrub returns an identity hook that appends every invocation to
// log. Its bookkeeping is test scaffolding, not scrub state — the bytes it
// returns are its input, so clause 4's determinism holds.
func countingScrub(log *[]scrubCall) xrr.StreamScrubFunc {
	return func(dir xrr.StreamDirection, _ xrr.StreamScrubInfo, data []byte) []byte {
		*log = append(*log, scrubCall{Dir: dir, Data: string(data)})
		return data
	}
}

// recordFixedStream drives one identical scripted stream of the given type
// through a record session, returning the fingerprint. The script is fixed
// so two sessions differing only in hook installation are comparable
// byte-for-byte.
func recordFixedStream(t *testing.T, dir string, typ xrr.StreamType, scrub xrr.StreamScrubFunc) string {
	t.Helper()
	var s *xrr.FileSession
	if scrub == nil {
		s = xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette(dir))
	} else {
		s = xrr.NewSessionWithStreamScrub(xrr.ModeRecord, xrr.NewFileCassette(dir), scrub)
	}
	open := grpcStreamOpen(typ, "chat.ChatService", "Converse", fixedOpenMsg)
	rec, err := s.OpenStreamRecord(open)
	require.NoError(t, err)
	for _, f := range fixedSends(typ) {
		rec.RecordSend(f)
	}
	rec.RecordHalfClose()
	for _, f := range fixedRecvs(typ) {
		rec.RecordRecv(f)
	}
	fp := rec.Fingerprint()
	require.NoError(t, rec.Finish(map[string]any{"status_code": 0}, nil))
	return fp
}

var fixedOpenMsg = []byte(`{"room":"ops"}`)

// fixedSends / fixedRecvs honour the gRPC mapping constraints per type:
// server streams record exactly one send, client streams at most one recv.
func fixedSends(typ xrr.StreamType) [][]byte {
	if typ == xrr.StreamServer {
		return [][]byte{fixedOpenMsg}
	}
	return [][]byte{[]byte("alpha"), []byte("beta")}
}

func fixedRecvs(typ xrr.StreamType) [][]byte {
	if typ == xrr.StreamClient {
		return [][]byte{[]byte("ack")}
	}
	return [][]byte{[]byte("one"), []byte("two")}
}

func readPairBytes(t *testing.T, dir, fp string) (string, string) {
	t.Helper()
	req, err := os.ReadFile(filepath.Join(dir, "grpc-"+fp+".req.yaml"))
	require.NoError(t, err)
	resp, err := os.ReadFile(filepath.Join(dir, "grpc-"+fp+".resp.yaml"))
	require.NoError(t, err)
	return string(req), string(resp)
}

// TestScrubIdentityMatchesNoHook — M1: an installed identity hook is
// byte-indistinguishable from no hook. Same cassette bytes, same
// fingerprint, all three stream types. Any divergence here is a mechanics
// defect: an extra scrub site, a missed one, or an identity input derived
// from the wrong bytes.
func TestScrubIdentityMatchesNoHook(t *testing.T) {
	for _, typ := range []xrr.StreamType{xrr.StreamServer, xrr.StreamClient, xrr.StreamBidi} {
		t.Run(string(typ), func(t *testing.T) {
			bare := t.TempDir()
			hooked := t.TempDir()

			bareFP := recordFixedStream(t, bare, typ, nil)
			hookedFP := recordFixedStream(t, hooked, typ, identityScrub)
			assert.Equal(t, bareFP, hookedFP, "identity hook must not move the fingerprint")

			bareReq, bareResp := readPairBytes(t, bare, bareFP)
			hookReq, hookResp := readPairBytes(t, hooked, hookedFP)
			assert.Equal(t, bareReq, hookReq, "req.yaml must be byte-identical")
			assert.Equal(t, bareResp, hookResp, "resp.yaml must be byte-identical")
		})
	}
}

// TestScrubIdentityReplayCrossHook — M2: because the identity hook changes
// no bytes, a cassette crosses the hook boundary in both directions. This
// is the one legitimate exception to clause 5's "same hook both sides",
// and it holds precisely because the two hooks agree byte-for-byte.
func TestScrubIdentityReplayCrossHook(t *testing.T) {
	replayFixed := func(t *testing.T, dir string, typ xrr.StreamType, scrub xrr.StreamScrubFunc) {
		t.Helper()
		var s *xrr.FileSession
		if scrub == nil {
			s = xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette(dir))
		} else {
			s = xrr.NewSessionWithStreamScrub(xrr.ModeReplay, xrr.NewFileCassette(dir), scrub)
		}
		rep, err := s.OpenStreamReplay(grpcStreamOpen(typ, "chat.ChatService", "Converse", fixedOpenMsg))
		require.NoError(t, err)
		for _, f := range fixedSends(typ) {
			require.NoError(t, rep.Send(f))
		}
		require.NoError(t, rep.HalfClose())
		for _, want := range fixedRecvs(typ) {
			got, err := rep.Recv()
			require.NoError(t, err)
			assert.Equal(t, want, got)
		}
		_, err = rep.Recv()
		assert.ErrorIs(t, err, io.EOF)
	}

	for _, typ := range []xrr.StreamType{xrr.StreamServer, xrr.StreamClient, xrr.StreamBidi} {
		t.Run(string(typ)+"/recorded with hook, replayed without", func(t *testing.T) {
			dir := t.TempDir()
			recordFixedStream(t, dir, typ, identityScrub)
			replayFixed(t, dir, typ, nil)
		})
		t.Run(string(typ)+"/recorded without hook, replayed with", func(t *testing.T) {
			dir := t.TempDir()
			recordFixedStream(t, dir, typ, nil)
			replayFixed(t, dir, typ, identityScrub)
		})
	}
}

// TestScrubIdentityDerivedIdentity — M3: clause 3 routes content-derived
// identity through the hook. Under identity that derivation must land on
// the same msg_hash as the raw bytes, in record and replay mode alike —
// otherwise the hook is being applied to the wrong buffer, or applied
// twice, at the identity site.
func TestScrubIdentityDerivedIdentity(t *testing.T) {
	msg := []byte(`{"cmd":"deploy"}`)
	rawSum := sha256.Sum256(msg)
	rawHash := hex.EncodeToString(rawSum[:4])

	for _, mode := range []xrr.Mode{xrr.ModeRecord, xrr.ModeReplay} {
		s := xrr.NewSessionWithStreamScrub(mode, xrr.NewFileCassette(t.TempDir()), identityScrub)
		scrubbed := s.ScrubStreamFrame(xrr.StreamSend,
			xrr.StreamScrubInfo{AdapterID: "grpc", Type: xrr.StreamServer}, msg)
		sum := sha256.Sum256(scrubbed)
		assert.Equal(t, rawHash, hex.EncodeToString(sum[:4]),
			"identity-derived msg_hash must equal the raw one in mode %v", mode)
	}
}

// TestScrubCountingRecordInvocations — M4: exactly one call per frame per
// direction, in frame order, carrying that frame's bytes. Half-close and
// the terminal carry no payload and contribute no call.
func TestScrubCountingRecordInvocations(t *testing.T) {
	var log []scrubCall
	dir := t.TempDir()
	recordFixedStream(t, dir, xrr.StreamBidi, countingScrub(&log))

	assert.Equal(t, []scrubCall{
		{xrr.StreamSend, "alpha"},
		{xrr.StreamSend, "beta"},
		{xrr.StreamRecv, "one"},
		{xrr.StreamRecv, "two"},
	}, log, "record: one call per frame per direction, no call for half-close or terminal")
}

// TestScrubCountingReplayInvocations — M5: replay scrubs live sends only,
// exactly once each, and never touches recorded frames. The trailing case
// is the one that caught a real cross-port divergence: two ports ran the
// hook BEFORE the bound check that rejects a send past the end of the
// recording, two ran it after, so an uncompared frame was scrubbed in some
// ports and not others. Only a counting hook can see that.
func TestScrubCountingReplayInvocations(t *testing.T) {
	dir := t.TempDir()
	recordFixedStream(t, dir, xrr.StreamBidi, identityScrub)

	var log []scrubCall
	s := xrr.NewSessionWithStreamScrub(xrr.ModeReplay, xrr.NewFileCassette(dir), countingScrub(&log))
	rep, err := s.OpenStreamReplay(grpcStreamOpen(xrr.StreamBidi, "chat.ChatService", "Converse", fixedOpenMsg))
	require.NoError(t, err)

	require.NoError(t, rep.Send([]byte("alpha")))
	require.NoError(t, rep.Send([]byte("beta")))
	require.NoError(t, rep.HalfClose())
	for _, want := range []string{"one", "two"} {
		got, err := rep.Recv()
		require.NoError(t, err)
		assert.Equal(t, want, string(got))
	}
	assert.Equal(t, []scrubCall{
		{xrr.StreamSend, "alpha"},
		{xrr.StreamSend, "beta"},
	}, log, "replay: live sends only — recorded recv frames are delivered verbatim")

	log = nil
	_ = rep.Send([]byte("overrun"))
	assert.Empty(t, log, "a send past the last recorded frame is never compared, so never scrubbed")
}

// TestScrubCountingNoDoubleScrub — M6: clause 3's no-pre-scrub rule. The
// gRPC server-stream open message is both an identity input and a
// persisted frame — two distinct invocation points, one call each. An
// adapter that pre-scrubbed the message it also hands the core would show
// two calls for the persist point.
func TestScrubCountingNoDoubleScrub(t *testing.T) {
	var log []scrubCall
	dir := t.TempDir()
	msg := []byte(`{"cmd":"deploy"}`)
	s := xrr.NewSessionWithStreamScrub(xrr.ModeRecord, xrr.NewFileCassette(dir), countingScrub(&log))

	// Identity point: the adapter derives msg_hash over the scrubbed bytes.
	scrubbed := s.ScrubStreamFrame(xrr.StreamSend,
		xrr.StreamScrubInfo{AdapterID: "grpc", Type: xrr.StreamServer}, msg)
	require.Len(t, log, 1, "identity derivation is exactly one call")

	sum := sha256.Sum256(scrubbed)
	open := xrr.StreamOpen{
		AdapterID: "grpc",
		Type:      xrr.StreamServer,
		Identity: map[string]any{
			"service": "ops.Deploy", "method": "Run",
			"msg_hash": hex.EncodeToString(sum[:4]),
		},
		Payload: map[string]any{"service": "ops.Deploy", "method": "Run"},
	}
	rec, err := s.OpenStreamRecord(open)
	require.NoError(t, err)

	// Persist point: the adapter passes the message RAW. The core scrubs.
	rec.RecordSend(msg)
	rec.RecordHalfClose()
	rec.RecordRecv([]byte("deployed"))
	require.NoError(t, rec.Finish(map[string]any{"status_code": 0}, nil))

	assert.Equal(t, []scrubCall{
		{xrr.StreamSend, string(msg)}, // identity derivation
		{xrr.StreamSend, string(msg)}, // persist — one call, not two
		{xrr.StreamRecv, "deployed"},
	}, log, "each invocation point fires exactly once; the core, not the adapter, scrubs persisted frames")
}

// TestScrubLengthChangeRoundTrips — M7: clause 6 permits a length change;
// neither the record nor the replay path may assume byte-count
// preservation.
func TestScrubLengthChangeRoundTrips(t *testing.T) {
	grow := func(_ xrr.StreamDirection, _ xrr.StreamScrubInfo, data []byte) []byte {
		return append(append([]byte{}, data...), []byte("-PADDED-LONGER")...)
	}
	dir := t.TempDir()
	open := grpcStreamOpen(xrr.StreamBidi, "chat.ChatService", "Converse", nil)

	recS := xrr.NewSessionWithStreamScrub(xrr.ModeRecord, xrr.NewFileCassette(dir), grow)
	rec, err := recS.OpenStreamRecord(open)
	require.NoError(t, err)
	rec.RecordSend([]byte("alpha"))
	rec.RecordHalfClose()
	rec.RecordRecv([]byte("one"))
	require.NoError(t, rec.Finish(map[string]any{"status_code": 0}, nil))

	pair, err := xrr.NewFileCassette(dir).LoadStream("grpc", rec.Fingerprint())
	require.NoError(t, err)
	assert.Equal(t, []byte("alpha-PADDED-LONGER"), pair.Req.Frames[0].Message)

	repS := xrr.NewSessionWithStreamScrub(xrr.ModeReplay, xrr.NewFileCassette(dir), grow)
	rep, err := repS.OpenStreamReplay(open)
	require.NoError(t, err)
	require.NoError(t, rep.Send([]byte("alpha")), "green despite the length change")
	require.NoError(t, rep.HalfClose())
	got, err := rep.Recv()
	require.NoError(t, err)
	assert.Equal(t, []byte("one-PADDED-LONGER"), got)
}
