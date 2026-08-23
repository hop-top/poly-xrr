package xrr_test

import (
	"testing"

	xrr "hop.top/xrr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- key-name classification -------------------------------------------

func TestDefaultRedactor_SecretEnvNames(t *testing.T) {
	r := xrr.NewRedactor(xrr.RedactConfig{})
	secret := []string{
		"GITHUB_TOKEN", "AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID",
		"API_KEY", "DB_PASSWORD", "GOOGLE_CREDENTIALS", "MY_SECRET",
		"npm_token", "Stripe_Api_Key", "SESSION_COOKIE", "PRIVATE_KEY",
		"AUTH_TOKEN", "PASSPHRASE", "SOME_AUTH", "CLIENT_SECRET",
	}
	for _, k := range secret {
		assert.True(t, r.IsSecretKey(k), "expected %q to be classified secret", k)
	}
}

func TestDefaultRedactor_BenignEnvNames(t *testing.T) {
	r := xrr.NewRedactor(xrr.RedactConfig{})
	benign := []string{
		"PATH", "HOME", "LANG", "PWD", "SHELL", "TERM", "GOPATH",
		"CI", "NODE_ENV", "XRR_MODE", "XRR_CASSETTE_DIR",
		// "key"/"token" as a substring of a non-credential word must not trip.
		"MONKEY_BUSINESS", "TOKENIZER_MODE", "KEYBOARD_LAYOUT",
	}
	for _, k := range benign {
		assert.False(t, r.IsSecretKey(k), "expected %q to be benign", k)
	}
}

func TestDefaultRedactor_SecretHeaderNames(t *testing.T) {
	r := xrr.NewRedactor(xrr.RedactConfig{})
	// Header matching must be case-insensitive and dash/underscore agnostic.
	secret := []string{
		"Authorization", "authorization", "Proxy-Authorization",
		"Cookie", "Set-Cookie", "X-Api-Key", "x-api-key", "X_API_KEY",
		"X-Auth-Token", "X-Amz-Security-Token", "X-CSRF-Token",
	}
	for _, k := range secret {
		assert.True(t, r.IsSecretKey(k), "expected header %q to be classified secret", k)
	}
	benign := []string{"Content-Type", "Accept", "User-Agent", "Content-Length"}
	for _, k := range benign {
		assert.False(t, r.IsSecretKey(k), "expected header %q to be benign", k)
	}
}

// --- placeholder shape --------------------------------------------------

func TestRedactor_PlaceholderIsDeterministicAndNamed(t *testing.T) {
	r := xrr.NewRedactor(xrr.RedactConfig{})
	got := r.Placeholder("Authorization")
	assert.Equal(t, "<redacted:AUTHORIZATION>", got)
	// Stable across calls — no counters, no randomness, no hashing of value.
	assert.Equal(t, got, r.Placeholder("Authorization"))
	assert.Equal(t, "<redacted:X-API-KEY>", r.Placeholder("X-Api-Key"))
	assert.Equal(t, "<redacted:GITHUB_TOKEN>", r.Placeholder("GITHUB_TOKEN"))
}

// --- value-pattern matching ---------------------------------------------

func TestDefaultRedactor_SecretValuePatterns(t *testing.T) {
	r := xrr.NewRedactor(xrr.RedactConfig{})
	// High-confidence, vendor-prefixed tokens. Name gives no hint here —
	// only the value shape does.
	secret := []string{
		"ghp_0123456789abcdefghijklmnopqrstuvwxyz",
		"github_pat_11ABCDEFG0123456789_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQR",
		"AKIAIOSFODNN7EXAMPLE",
		"sk-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij0123456789",
		"xoxb-EXAMPLE-NOT-A-REAL-TOKEN-000",
		"-----BEGIN RSA PRIVATE KEY-----",
		"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIn0.abcdefghijklmnop",
	}
	for _, v := range secret {
		assert.True(t, r.IsSecretValue(v), "expected value %q to match a secret pattern", v)
	}
}

func TestDefaultRedactor_BenignValuesNotMatched(t *testing.T) {
	r := xrr.NewRedactor(xrr.RedactConfig{})
	benign := []string{
		"", "/usr/local/bin:/usr/bin", "en_US.UTF-8", "true", "1",
		"https://api.example.com/v1/things?page=2",
		"application/json", "Mozilla/5.0 (Macintosh)",
		"a3f9c1b2", "hello world",
	}
	for _, v := range benign {
		assert.False(t, r.IsSecretValue(v), "expected value %q NOT to match a secret pattern", v)
	}
}

// --- escape hatch + custom config ---------------------------------------

func TestRedactor_AllowListPreservesValue(t *testing.T) {
	r := xrr.NewRedactor(xrr.RedactConfig{Allow: []string{"GITHUB_TOKEN"}})
	assert.False(t, r.IsSecretKey("GITHUB_TOKEN"), "allow-list must win over default deny")
	// Sibling secrets still redacted.
	assert.True(t, r.IsSecretKey("AWS_SECRET_ACCESS_KEY"))
}

func TestRedactor_CustomDenyKeys(t *testing.T) {
	r := xrr.NewRedactor(xrr.RedactConfig{Deny: []string{"MY_CUSTOM_FIELD"}})
	assert.True(t, r.IsSecretKey("MY_CUSTOM_FIELD"))
	assert.True(t, r.IsSecretKey("my_custom_field"), "custom deny must be case-insensitive")
}

func TestRedactor_Disabled(t *testing.T) {
	r := xrr.NewRedactor(xrr.RedactConfig{Disabled: true})
	assert.False(t, r.IsSecretKey("GITHUB_TOKEN"))
	assert.False(t, r.IsSecretValue("ghp_0123456789abcdefghijklmnopqrstuvwxyz"))
}

func TestRedactor_AllowListPreservesValuePatternMatch(t *testing.T) {
	// Allow-listing a key must also suppress value-pattern redaction for
	// that key, otherwise the escape hatch is useless for a var whose
	// value happens to look like a token.
	r := xrr.NewRedactor(xrr.RedactConfig{Allow: []string{"FIXTURE_TOKEN"}})
	v, redacted := r.RedactField("FIXTURE_TOKEN", "ghp_0123456789abcdefghijklmnopqrstuvwxyz")
	assert.False(t, redacted)
	assert.Equal(t, "ghp_0123456789abcdefghijklmnopqrstuvwxyz", v)
}

// --- RedactField composition --------------------------------------------

func TestRedactor_RedactField(t *testing.T) {
	r := xrr.NewRedactor(xrr.RedactConfig{})

	// Secret by key name.
	v, redacted := r.RedactField("GITHUB_TOKEN", "anything")
	assert.True(t, redacted)
	assert.Equal(t, "<redacted:GITHUB_TOKEN>", v)

	// Secret by value shape, benign key.
	v, redacted = r.RedactField("MY_VAR", "ghp_0123456789abcdefghijklmnopqrstuvwxyz")
	assert.True(t, redacted)
	assert.Equal(t, "<redacted:MY_VAR>", v)

	// Benign key + benign value passes through untouched.
	v, redacted = r.RedactField("PATH", "/usr/bin")
	assert.False(t, redacted)
	assert.Equal(t, "/usr/bin", v)

	// Empty value on a secret key: nothing to leak, leave it alone so
	// `env: {FOO_TOKEN: ""}` doesn't gain a misleading placeholder.
	v, redacted = r.RedactField("GITHUB_TOKEN", "")
	assert.False(t, redacted)
	assert.Equal(t, "", v)
}

func TestRedactor_ConfigFromEnv(t *testing.T) {
	t.Setenv(xrr.EnvRedactDisable, "1")
	cfg := xrr.RedactConfigFromEnv()
	require.True(t, cfg.Disabled)

	t.Setenv(xrr.EnvRedactDisable, "")
	t.Setenv(xrr.EnvRedactAllow, "FOO_TOKEN, BAR_KEY")
	t.Setenv(xrr.EnvRedactDeny, "CUSTOM_A,CUSTOM_B")
	cfg = xrr.RedactConfigFromEnv()
	assert.False(t, cfg.Disabled)
	assert.Equal(t, []string{"FOO_TOKEN", "BAR_KEY"}, cfg.Allow)
	assert.Equal(t, []string{"CUSTOM_A", "CUSTOM_B"}, cfg.Deny)
}
