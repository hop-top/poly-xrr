package main

import (
	"regexp"
	"sort"
	"strings"

	xrr "hop.top/xrr"
)

var assignRe = regexp.MustCompile(`^(-{0,2}[A-Za-z][A-Za-z0-9_.-]*)=(.*)$`)

// scrubArgv returns a copy of argv with credential values replaced by the
// redactor's deterministic placeholders. It covers what the YAML value
// patterns cannot see: a secret whose only hint is the flag or variable
// name in front of it.
//
//	--password X   ->  --password <redacted:PASSWORD>
//	--api-key=X    ->  --api-key=<redacted:API-KEY>
//	TOKEN=X        ->  TOKEN=<redacted:TOKEN>
//
// The result is for what is written to disk or printed. Fingerprints are
// always computed over the original argv, so scrubbing never shifts a
// cassette key (spec: redaction MUST NOT change any fingerprint).
func scrubArgv(r *xrr.Redactor, argv []string) []string {
	out := make([]string, len(argv))
	copy(out, argv)
	for i := 1; i < len(out); i++ { // argv[0] is the command name
		if r.IsSecretValue(out[i]) {
			out[i] = r.Placeholder("argv")
			continue
		}
		if m := assignRe.FindStringSubmatch(out[i]); m != nil {
			name := strings.TrimLeft(m[1], "-")
			if red, ok := r.RedactField(name, m[2]); ok {
				out[i] = m[1] + "=" + red
			}
			continue
		}
		if strings.HasPrefix(out[i], "-") && i+1 < len(out) && !strings.HasPrefix(out[i+1], "-") {
			name := strings.TrimLeft(out[i], "-")
			if red, ok := r.RedactField(name, out[i+1]); ok {
				out[i+1] = red
				i++
			}
		}
	}
	return out
}

// minSecretLen guards against corrupting output by replacing short env
// values ("1", "on") that merely sit under a secret-looking name.
const minSecretLen = 8

// envSecrets maps each credential-bearing env var's value (by name, per
// the redactor) to its placeholder, so the same secret is also caught when
// a command echoes it into stdout, stderr or argv in a shape no value
// pattern recognises.
type envSecrets []struct{ value, placeholder string }

func collectEnvSecrets(r *xrr.Redactor, environ []string) envSecrets {
	var out envSecrets
	for _, e := range environ {
		name, val, ok := strings.Cut(e, "=")
		if !ok || len(val) < minSecretLen || !r.IsSecretKey(name) {
			continue
		}
		out = append(out, struct{ value, placeholder string }{val, r.Placeholder(name)})
	}
	// Longest first so a value that contains another is replaced whole.
	sort.SliceStable(out, func(i, j int) bool { return len(out[i].value) > len(out[j].value) })
	return out
}

func (es envSecrets) scrub(s string) string {
	for _, e := range es {
		s = strings.ReplaceAll(s, e.value, e.placeholder)
	}
	return s
}

func (es envSecrets) scrubAll(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = es.scrub(s)
	}
	return out
}
