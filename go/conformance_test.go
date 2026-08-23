package xrr_test

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
	xrr "hop.top/xrr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type manifest struct {
	Interactions []struct {
		Adapter     string `yaml:"adapter"`
		Fingerprint string `yaml:"fingerprint"`
		Streamed    bool   `yaml:"streamed"`
	} `yaml:"interactions"`
}

// TestConformanceFixtures replays spec/fixtures cassettes — proves Go can read
// cassettes produced by any other language port. Entries marked streamed are
// routed through the streaming code path per the manifest extension:
// load+validate, lossless re-emit round-trip, and (for grpc) fingerprint
// recomputation against the filename.
func TestConformanceFixtures(t *testing.T) {
	fixtures := filepath.Join("..", "spec", "fixtures")
	entries, err := os.ReadDir(fixtures)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "no fixture dirs found")

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		e := e
		t.Run(e.Name(), func(t *testing.T) {
			dir := filepath.Join(fixtures, e.Name())
			manifestPath := filepath.Join(dir, "manifest.yaml")
			data, err := os.ReadFile(manifestPath)
			require.NoError(t, err, "missing manifest.yaml in %s", e.Name())

			var m manifest
			require.NoError(t, yaml.Unmarshal(data, &m))

			c := xrr.NewFileCassette(dir)
			for _, interaction := range m.Interactions {
				if interaction.Streamed {
					conformStreamedPair(t, c, interaction.Adapter, interaction.Fingerprint)
					continue
				}
				var reqPayload, respPayload map[string]any
				_, err := c.Load(interaction.Adapter, interaction.Fingerprint, &reqPayload, &respPayload)
				assert.NoError(t, err,
					"cassette miss: adapter=%s fp=%s", interaction.Adapter, interaction.Fingerprint)
			}
		})
	}
}

// conformStreamedPair exercises the format-layer obligations on one streamed
// pair: parse into the stream model, re-emit losslessly (encoding choice
// free; equality over decoded bytes), and recompute the fingerprint from
// open-time inputs for adapters whose algorithm the spec defines (grpc).
func conformStreamedPair(t *testing.T, c *xrr.FileCassette, adapter, fingerprint string) {
	t.Helper()
	pair, err := c.LoadStream(adapter, fingerprint)
	require.NoError(t, err, "streamed load: adapter=%s fp=%s", adapter, fingerprint)

	out := xrr.NewFileCassette(t.TempDir())
	require.NoError(t, out.SaveStream(adapter, fingerprint, pair))
	again, err := out.LoadStream(adapter, fingerprint)
	require.NoError(t, err, "re-emitted pair must load: fp=%s", fingerprint)
	assertStreamPairEqual(t, pair, again)

	if adapter == "grpc" {
		assert.Equal(t, fingerprint, recomputeGRPCStreamFingerprint(t, pair),
			"fingerprint recomputation must match filename")
	}
}

// recomputeGRPCStreamFingerprint recomputes a grpc streaming fingerprint from
// the pair's open-time inputs: the single send frame for server streams, the
// recorded occurrence ordinal for client/bidi. Reading the informational
// payload n is a conformance-only device — replay recomputes its own counter.
func recomputeGRPCStreamFingerprint(t *testing.T, pair *xrr.StreamPair) string {
	t.Helper()
	service, _ := pair.ReqPayload["service"].(string)
	method, _ := pair.ReqPayload["method"].(string)
	var msg []byte
	n := 0
	if pair.Req.Type == xrr.StreamServer {
		require.Len(t, pair.Req.Frames, 1, "server stream records exactly one send frame")
		msg = pair.Req.Frames[0].Message
	} else {
		recorded, ok := pair.ReqPayload["n"].(int)
		require.True(t, ok, "client/bidi req payload must record n")
		n = recorded
	}
	open := grpcStreamOpen(pair.Req.Type, service, method, msg)
	fp, err := xrr.StreamFingerprint(open, n)
	require.NoError(t, err)
	return fp
}

// TestConformanceMalformedB64Rejected — the grpc-stream-malformed-b64 pair is
// deliberately absent from its manifest (interactions enumerate replayable
// pairs); harnesses target it by path and assert strict loading fails rather
// than silently discarding the bad characters.
func TestConformanceMalformedB64Rejected(t *testing.T) {
	dir := filepath.Join("..", "spec", "fixtures", "grpc-stream-malformed-b64")
	c := xrr.NewFileCassette(dir)
	_, err := c.LoadStream("grpc", "8dbfb222")
	require.Error(t, err)
	assert.NotErrorIs(t, err, xrr.ErrCassetteMiss)
}

// TestConformanceScalarHazards — sse-text-scalars message_text payloads must
// decode as exactly those characters regardless of YAML scalar resolution.
func TestConformanceScalarHazards(t *testing.T) {
	dir := filepath.Join("..", "spec", "fixtures", "sse-text-scalars")
	pair, err := xrr.NewFileCassette(dir).LoadStream("sse", "66ecc77a")
	require.NoError(t, err)

	want := []string{"on", "12:30", "null", " leading", "trailing ", "  padded  "}
	require.Len(t, pair.Resp.Frames, len(want))
	for i, w := range want {
		assert.Equal(t, []byte(w), pair.Resp.Frames[i].Message, "frame %d", i)
	}
}

// TestConformanceClientStreamRepeat — the spec's scripted n=1 obligation: one
// session, two sequential opens of (files.FileService, Upload, client)
// against grpc-client-stream-repeat as the session dir, expecting the n=0
// then n=1 fingerprints and both conversations replayed.
func TestConformanceClientStreamRepeat(t *testing.T) {
	s := fixtureSession(t, "grpc-client-stream-repeat")
	open := grpcStreamOpen(xrr.StreamClient, "files.FileService", "Upload", nil)

	rep1, err := s.OpenStreamReplay(open)
	require.NoError(t, err)
	assert.Equal(t, "2bebfd6f", rep1.Fingerprint())
	require.NoError(t, rep1.Send([]byte("alpha\n")))
	require.NoError(t, rep1.HalfClose())
	msg, err := rep1.Recv()
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"received_bytes":6}`), msg)
	_, err = rep1.Recv()
	assert.ErrorIs(t, err, io.EOF)

	rep2, err := s.OpenStreamReplay(open)
	require.NoError(t, err)
	assert.Equal(t, "b27b5fe1", rep2.Fingerprint())
	require.NoError(t, rep2.Send([]byte("beta-1\n")))
	require.NoError(t, rep2.Send([]byte("beta-2\n")))
	require.NoError(t, rep2.HalfClose())
	msg, err = rep2.Recv()
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"received_bytes":14}`), msg)
	_, err = rep2.Recv()
	assert.ErrorIs(t, err, io.EOF)
}
