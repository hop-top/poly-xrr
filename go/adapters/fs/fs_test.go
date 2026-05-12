package fs_test

import (
	"testing"

	"hop.top/xrr/adapters/fs"

	"github.com/stretchr/testify/assert"
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
