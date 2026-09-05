package grpc_test

import (
	"testing"

	xgrpc "hop.top/xrr/adapters/grpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGRPCAdapterFingerprint(t *testing.T) {
	a := xgrpc.NewAdapter()
	req := &xgrpc.Request{Service: "UserService", Method: "GetUser", Message: []byte(`{"id":1}`)}
	fp, err := a.Fingerprint(req)
	require.NoError(t, err)
	assert.Len(t, fp, 8)
	fp2, _ := a.Fingerprint(req)
	assert.Equal(t, fp, fp2)
	// different message → different fp
	req2 := &xgrpc.Request{Service: "UserService", Method: "GetUser", Message: []byte(`{"id":2}`)}
	fp3, _ := a.Fingerprint(req2)
	assert.NotEqual(t, fp, fp3)
}

func TestGRPCAdapterRoundtrip(t *testing.T) {
	a := xgrpc.NewAdapter()
	req := &xgrpc.Request{Service: "UserService", Method: "GetUser", Message: []byte(`{"id":1}`)}
	data, err := a.Serialize(req)
	require.NoError(t, err)
	var got xgrpc.Request
	require.NoError(t, a.Deserialize(data, &got))
	assert.Equal(t, req.Service, got.Service)
	assert.Equal(t, req.Method, got.Method)
}

// {"method":"Get<x>","msg_hash":"62c66a7a","service":"a&b.Svc"} under RFC
// 8785 escaping — no HTML-safe escapes.
func TestFingerprintNoHTMLEscape(t *testing.T) {
	fp, err := xgrpc.NewAdapter().Fingerprint(&xgrpc.Request{Service: "a&b.Svc", Method: "Get<x>", Message: []byte("m")})
	require.NoError(t, err)
	assert.Equal(t, "19c17430", fp)
}
