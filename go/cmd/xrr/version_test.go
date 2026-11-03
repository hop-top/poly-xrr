package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestVersionFlags(t *testing.T) {
	for _, arg := range []string{"--version", "-v", "version"} {
		var out, errb bytes.Buffer
		if code := dispatch([]string{arg}, &out, &errb); code != exitOK {
			t.Fatalf("%s: exit %d, stderr %q", arg, code, errb.String())
		}
		if !strings.HasPrefix(out.String(), "xrr ") || strings.Count(out.String(), "\n") != 1 {
			t.Fatalf("%s: want one 'xrr <version>' line, got %q", arg, out.String())
		}
	}
}

func TestVersionLdflagWins(t *testing.T) {
	old := version
	version = "1.2.3-test"
	defer func() { version = old }()
	if got := resolveVersion(); got != "1.2.3-test" {
		t.Fatalf("resolveVersion() = %q, want ldflag value", got)
	}
}

func TestVersionFallbackNeverEmpty(t *testing.T) {
	old := version
	version = ""
	defer func() { version = old }()
	if got := resolveVersion(); got == "" {
		t.Fatal("resolveVersion() empty without ldflag")
	}
}
