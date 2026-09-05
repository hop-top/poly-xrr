package redis_test

import (
	"testing"

	xredis "hop.top/xrr/adapters/redis"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisAdapterFingerprint(t *testing.T) {
	a := xredis.NewAdapter()
	req := &xredis.Request{Command: "GET", Args: []string{"user:42"}}
	fp, err := a.Fingerprint(req)
	require.NoError(t, err)
	assert.Len(t, fp, 8)
	fp2, _ := a.Fingerprint(req)
	assert.Equal(t, fp, fp2)
	req2 := &xredis.Request{Command: "GET", Args: []string{"user:99"}}
	fp3, _ := a.Fingerprint(req2)
	assert.NotEqual(t, fp, fp3)
}

func TestRedisAdapterRoundtrip(t *testing.T) {
	a := xredis.NewAdapter()
	req := &xredis.Request{Command: "SET", Args: []string{"key", "value"}}
	data, err := a.Serialize(req)
	require.NoError(t, err)
	var got xredis.Request
	require.NoError(t, a.Deserialize(data, &got))
	assert.Equal(t, req.Command, got.Command)
	assert.Equal(t, req.Args, got.Args)
}

// "GET a&b<c>/é" under RFC 8785 escaping — no HTML-safe escapes, é raw.
func TestFingerprintNoHTMLEscape(t *testing.T) {
	fp, err := xredis.NewAdapter().Fingerprint(&xredis.Request{Command: "get", Args: []string{"a&b<c>/é"}})
	require.NoError(t, err)
	assert.Equal(t, "25f2532e", fp)
}
