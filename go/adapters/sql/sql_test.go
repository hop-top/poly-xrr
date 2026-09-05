package sql_test

import (
	"testing"

	xsql "hop.top/xrr/adapters/sql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLAdapterFingerprint(t *testing.T) {
	a := xsql.NewAdapter()
	req := &xsql.Request{Query: "SELECT * FROM users WHERE id = ?", Args: []any{42}}
	fp, err := a.Fingerprint(req)
	require.NoError(t, err)
	assert.Len(t, fp, 8)
	fp2, _ := a.Fingerprint(req)
	assert.Equal(t, fp, fp2)
	// whitespace normalization — same query
	req2 := &xsql.Request{Query: "SELECT  *  FROM  users  WHERE  id = ?", Args: []any{42}}
	fp3, _ := a.Fingerprint(req2)
	assert.Equal(t, fp, fp3)
	// different args → different fp
	req3 := &xsql.Request{Query: "SELECT * FROM users WHERE id = ?", Args: []any{99}}
	fp4, _ := a.Fingerprint(req3)
	assert.NotEqual(t, fp, fp4)
}

func TestSQLAdapterRoundtrip(t *testing.T) {
	a := xsql.NewAdapter()
	req := &xsql.Request{Query: "SELECT 1", Args: []any{"a", 1}}
	data, err := a.Serialize(req)
	require.NoError(t, err)
	var got xsql.Request
	require.NoError(t, a.Deserialize(data, &got))
	assert.Equal(t, req.Query, got.Query)
}

// {"args":null,"query":"select a&b<c>/é"} under RFC 8785 escaping — no
// HTML-safe & < >, é raw.
func TestFingerprintNoHTMLEscape(t *testing.T) {
	fp, err := xsql.NewAdapter().Fingerprint(&xsql.Request{Query: "SELECT a&b<c>/é"})
	require.NoError(t, err)
	assert.Equal(t, "200c6828", fp)
}
