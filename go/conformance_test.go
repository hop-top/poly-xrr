package xrr_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
	xrr "hop.top/xrr"
	execadapter "hop.top/xrr/adapters/exec"
	fsadapter "hop.top/xrr/adapters/fs"
	httpadapter "hop.top/xrr/adapters/http"
	redisadapter "hop.top/xrr/adapters/redis"
	sqladapter "hop.top/xrr/adapters/sql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type manifest struct {
	Interactions []struct {
		Adapter     string `yaml:"adapter"`
		Fingerprint string `yaml:"fingerprint"`
		Streamed    bool   `yaml:"streamed"`
		// VerifyFingerprint marks a unary entry whose fingerprint is a
		// computed value: the walker rebuilds the adapter's request from
		// the req payload and recomputes it with the adapter's algorithm.
		VerifyFingerprint bool `yaml:"verify_fingerprint"`
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
				conformUnaryPair(t, c, interaction.Adapter, interaction.Fingerprint, interaction.VerifyFingerprint)
			}
		})
	}
}

// conformUnaryPair loads one unary pair through the v1 code path and, when
// the manifest pins the fingerprint, recomputes it from the loaded request
// with the adapter's own algorithm. Loading alone cannot expose a
// canonical-JSON escaping fork — the files load in every port; the derived
// key is what differs.
func conformUnaryPair(t *testing.T, c *xrr.FileCassette, adapter, fingerprint string, verify bool) {
	t.Helper()
	var reqPayload, respPayload map[string]any
	_, err := c.Load(adapter, fingerprint, &reqPayload, &respPayload)
	if !assert.NoError(t, err, "cassette miss: adapter=%s fp=%s", adapter, fingerprint) || !verify {
		return
	}
	got, err := recomputeUnaryFingerprint(c, adapter, fingerprint)
	require.NoError(t, err)
	assert.Equal(t, fingerprint, got,
		"unary fingerprint recomputation must match manifest: adapter=%s", adapter)
}

// recomputeUnaryFingerprint decodes the req payload into the adapter's typed
// request and runs the adapter's Fingerprint over it.
func recomputeUnaryFingerprint(c *xrr.FileCassette, adapter, fingerprint string) (string, error) {
	var resp map[string]any
	switch adapter {
	case "exec":
		var req execadapter.Request
		if _, err := c.Load(adapter, fingerprint, &req, &resp); err != nil {
			return "", err
		}
		return execadapter.NewAdapter().Fingerprint(&req)
	case "http":
		var req httpadapter.Request
		if _, err := c.Load(adapter, fingerprint, &req, &resp); err != nil {
			return "", err
		}
		return httpadapter.NewAdapter().Fingerprint(&req)
	case "sql":
		var req sqladapter.Request
		if _, err := c.Load(adapter, fingerprint, &req, &resp); err != nil {
			return "", err
		}
		return sqladapter.NewAdapter().Fingerprint(&req)
	case "fs":
		var req fsadapter.Request
		if _, err := c.Load(adapter, fingerprint, &req, &resp); err != nil {
			return "", err
		}
		return fsadapter.NewAdapter().Fingerprint(&req)
	case "redis":
		var req redisadapter.Request
		if _, err := c.Load(adapter, fingerprint, &req, &resp); err != nil {
			return "", err
		}
		return redisadapter.NewAdapter().Fingerprint(&req)
	}
	return "", fmt.Errorf("verify_fingerprint: no unary fingerprint model for adapter %q", adapter)
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
//
// This derives n from each pair's own payload rather than from a counter
// shared across the dir, so the loop above needs no ordering: the manifest's
// order cannot affect any result. That satisfies the spec's ordering rule
// (cassette-format-streaming.md, Manifest Extension) vacuously — there is no
// shared counter for a wrong order to desynchronise.
// TestConformanceManifestOrderIrrelevant pins that property so it cannot
// regress into an order dependence silently.
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

// TestConformanceManifestOrderIrrelevant pins the property that makes the
// manifest ordering rule (cassette-format-streaming.md, Manifest Extension)
// hold here: `interactions` is an unordered set, so reversing a manifest's
// entries must not change any conformance result. Go recomputes each pair's
// occurrence n from that pair's own payload rather than from a counter shared
// across the dir, so it is order-independent by construction — this test keeps
// that construction from silently regressing into a shared-counter loop, where
// a wrong order would assign entries each other's n and miss.
func TestConformanceManifestOrderIrrelevant(t *testing.T) {
	fixtures := filepath.Join("..", "spec", "fixtures")
	entries, err := os.ReadDir(fixtures)
	require.NoError(t, err)

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		e := e
		t.Run(e.Name(), func(t *testing.T) {
			dir := filepath.Join(fixtures, e.Name())
			data, err := os.ReadFile(filepath.Join(dir, "manifest.yaml"))
			require.NoError(t, err)

			var m manifest
			require.NoError(t, yaml.Unmarshal(data, &m))

			// Reverse the manifest: a legal edit under the ordering rule.
			reversed := m.Interactions
			for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
				reversed[i], reversed[j] = reversed[j], reversed[i]
			}

			c := xrr.NewFileCassette(dir)
			for _, interaction := range reversed {
				if interaction.Streamed {
					conformStreamedPair(t, c, interaction.Adapter, interaction.Fingerprint)
					continue
				}
				conformUnaryPair(t, c, interaction.Adapter, interaction.Fingerprint, interaction.VerifyFingerprint)
			}
		})
	}
}
