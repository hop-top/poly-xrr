package http_test

import (
	"testing"

	xhttp "hop.top/xrr/adapters/http"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPAdapterFingerprint(t *testing.T) {
	a := xhttp.NewAdapter()
	req := &xhttp.Request{Method: "GET", URL: "https://api.example.com/users?page=1"}
	fp, err := a.Fingerprint(req)
	require.NoError(t, err)
	assert.Len(t, fp, 8)
	// deterministic
	fp2, _ := a.Fingerprint(req)
	assert.Equal(t, fp, fp2)
	// different path → different fp
	req2 := &xhttp.Request{Method: "GET", URL: "https://api.example.com/users?page=2"}
	fp3, _ := a.Fingerprint(req2)
	assert.NotEqual(t, fp, fp3)
	// host difference ignored (same path)
	req3 := &xhttp.Request{Method: "GET", URL: "https://other.example.com/users?page=1"}
	fp4, _ := a.Fingerprint(req3)
	assert.Equal(t, fp, fp4)
}

func TestHTTPAdapterRoundtrip(t *testing.T) {
	a := xhttp.NewAdapter()
	req := &xhttp.Request{Method: "POST", URL: "https://api.example.com/items", Body: `{"name":"x"}`}
	data, err := a.Serialize(req)
	require.NoError(t, err)
	var got xhttp.Request
	require.NoError(t, a.Deserialize(data, &got))
	assert.Equal(t, req.Method, got.Method)
	assert.Equal(t, req.Body, got.Body)
}

// {"body_hash":"e3b0c442","method":"GET","path":"/p?q=a&b<c>"} under RFC
// 8785 escaping — no HTML-safe escapes in the query string.
func TestFingerprintNoHTMLEscape(t *testing.T) {
	fp, err := xhttp.NewAdapter().Fingerprint(&xhttp.Request{Method: "GET", URL: "https://h/p?q=a&b<c>"})
	require.NoError(t, err)
	assert.Equal(t, "54e90fb9", fp)
}
