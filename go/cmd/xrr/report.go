package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Call outcomes logged by shims.
const (
	outRecorded    = "recorded"
	outReplayed    = "replayed"
	outPassthrough = "passthrough"
	outDenied      = "denied"
	outMiss        = "miss"
	outError       = "error"
)

// callEntry is one shim invocation. Argv is already scrubbed.
type callEntry struct {
	Cmd         string   `json:"cmd"`
	Argv        []string `json:"argv"`
	Outcome     string   `json:"outcome"`
	Fingerprint string   `json:"fingerprint,omitempty"`
	ExitCode    int      `json:"exit_code"`
	Detail      string   `json:"detail,omitempty"`
}

func (e callEntry) failing() bool {
	return e.Outcome == outDenied || e.Outcome == outMiss || e.Outcome == outError
}

// appendCall adds one JSON line to the run's call log. Lines are small and
// written with O_APPEND in a single write, so concurrent shims interleave
// whole lines.
func appendCall(runDir string, e callEntry) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(runDir, callsFile), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(b, '\n'))
	return err
}

func readCalls(runDir string) ([]callEntry, error) {
	b, err := os.ReadFile(filepath.Join(runDir, callsFile))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []callEntry
	for _, line := range strings.Split(string(b), "\n") {
		if line == "" {
			continue
		}
		var e callEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			return nil, fmt.Errorf("call log: %w", err)
		}
		out = append(out, e)
	}
	return out, nil
}

func anyFailing(calls []callEntry) bool {
	for _, c := range calls {
		if c.failing() {
			return true
		}
	}
	return false
}

// renderReport prints the human summary. Failing calls are listed with
// their (scrubbed) argv; passing calls are only counted.
func renderReport(w io.Writer, mode string, calls []callEntry) {
	counts := map[string]int{}
	for _, c := range calls {
		counts[c.Outcome]++
	}
	fmt.Fprintf(w, "xrr run: %s, %d shimmed call(s)\n", mode, len(calls))
	for _, o := range []string{outRecorded, outReplayed, outPassthrough, outDenied, outMiss, outError} {
		if counts[o] > 0 {
			fmt.Fprintf(w, "  %-12s %d\n", o, counts[o])
		}
	}
	for _, c := range calls {
		if !c.failing() {
			continue
		}
		line := fmt.Sprintf("  %s: %s", c.Outcome, strings.Join(c.Argv, " "))
		if c.Fingerprint != "" {
			line += " (fingerprint " + c.Fingerprint + ")"
		}
		if c.Detail != "" {
			line += " - " + c.Detail
		}
		fmt.Fprintln(w, line)
	}
}

type reportJSON struct {
	Mode   string         `json:"mode"`
	Failed bool           `json:"failed"`
	Counts map[string]int `json:"counts"`
	Calls  []callEntry    `json:"calls"`
}

func writeReportFile(path, mode string, calls []callEntry) error {
	r := reportJSON{Mode: mode, Failed: anyFailing(calls), Counts: map[string]int{}, Calls: calls}
	if r.Calls == nil {
		r.Calls = []callEntry{}
	}
	for _, c := range calls {
		r.Counts[c.Outcome]++
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
