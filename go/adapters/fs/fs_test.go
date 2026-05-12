package fs_test

import (
	"testing"

	"hop.top/xrr/adapters/fs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdapterID(t *testing.T) {
	a := fs.NewAdapter()
	assert.Equal(t, "fs", a.ID())
}

func TestRequestAdapterID(t *testing.T) {
	r := &fs.Request{Op: fs.OpWrite, Path: "/x"}
	assert.Equal(t, "fs", r.AdapterID())
}

func TestResponseAdapterID(t *testing.T) {
	r := &fs.Response{}
	assert.Equal(t, "fs", r.AdapterID())
}

func TestFingerprintDeterministic(t *testing.T) {
	a := fs.NewAdapter()
	req := &fs.Request{Op: fs.OpWrite, Path: "/etc/hosts", Data: []byte("127.0.0.1 localhost\n")}
	fp1, err := a.Fingerprint(req)
	require.NoError(t, err)
	assert.Len(t, fp1, 8, "fingerprint must be 8 hex chars")
	fp2, err := a.Fingerprint(req)
	require.NoError(t, err)
	assert.Equal(t, fp1, fp2, "same request must hash identically")
}

func TestFingerprintDiscriminatesPath(t *testing.T) {
	a := fs.NewAdapter()
	fpA, _ := a.Fingerprint(&fs.Request{Op: fs.OpWrite, Path: "/a", Data: []byte("x")})
	fpB, _ := a.Fingerprint(&fs.Request{Op: fs.OpWrite, Path: "/b", Data: []byte("x")})
	assert.NotEqual(t, fpA, fpB)
}

func TestFingerprintDiscriminatesData(t *testing.T) {
	a := fs.NewAdapter()
	fpA, _ := a.Fingerprint(&fs.Request{Op: fs.OpWrite, Path: "/x", Data: []byte("foo")})
	fpB, _ := a.Fingerprint(&fs.Request{Op: fs.OpWrite, Path: "/x", Data: []byte("bar")})
	assert.NotEqual(t, fpA, fpB)
}

func TestFingerprintDiscriminatesOp(t *testing.T) {
	a := fs.NewAdapter()
	fpW, _ := a.Fingerprint(&fs.Request{Op: fs.OpWrite, Path: "/x"})
	fpR, _ := a.Fingerprint(&fs.Request{Op: fs.OpRemove, Path: "/x"})
	assert.NotEqual(t, fpW, fpR)
}

func TestFingerprintOmitsZeroFields(t *testing.T) {
	a := fs.NewAdapter()
	// Request with Mode unset must hash identically to a "minimal" write.
	bare := &fs.Request{Op: fs.OpWrite, Path: "/x", Data: []byte("y")}
	withNilMode := &fs.Request{Op: fs.OpWrite, Path: "/x", Data: []byte("y"), Mode: nil}
	fpA, _ := a.Fingerprint(bare)
	fpB, _ := a.Fingerprint(withNilMode)
	assert.Equal(t, fpA, fpB, "Mode: nil must omit `mode` from fingerprint")
}

func TestFingerprintPointerToZeroIncludesField(t *testing.T) {
	a := fs.NewAdapter()
	zero := uint32(0)
	bare := &fs.Request{Op: fs.OpWrite, Path: "/x", Data: []byte("y")}
	withZeroMode := &fs.Request{Op: fs.OpWrite, Path: "/x", Data: []byte("y"), Mode: &zero}
	fpA, _ := a.Fingerprint(bare)
	fpB, _ := a.Fingerprint(withZeroMode)
	assert.NotEqual(t, fpA, fpB,
		"Mode: &0 must include `mode: 0` in fingerprint; differs from Mode: nil")
}

func TestFingerprintRejectsWrongType(t *testing.T) {
	a := fs.NewAdapter()
	type bogus struct{}
	type bogusReq struct{ bogus }
	// Use any other AdapterID-implementing type — the exec adapter's
	// Request would do, but we don't want an import cycle. Define a
	// minimal stand-in.
	_, err := a.Fingerprint(notFsRequest{})
	assert.Error(t, err)
}

type notFsRequest struct{}

func (notFsRequest) AdapterID() string { return "not-fs" }
