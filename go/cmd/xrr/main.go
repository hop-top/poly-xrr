// Command xrr records and replays external CLI calls for test suites
// written in any language.
//
//	xrr run [flags] -- <cmd> [args...]
//
// run prepends a temp dir of shims to PATH, runs <cmd>, and answers every
// shimmed command from (or records it to) exec cassettes in the existing
// xrr exec format, so the other language ports replay them too. See
// docs/xrr-run.md.
package main

import (
	"fmt"
	"io"
	"os"
)

// Exit codes. A child's own non-zero exit code always wins over exitViolation.
const (
	exitOK        = 0
	exitUsage     = 2   // bad flags / configuration
	exitViolation = 3   // run finished but had deny hits, misses or shim errors
	exitMiss      = 125 // shim: strict cassette miss
	exitDenied    = 126 // shim: denied command
	exitNotFound  = 127 // shim: real binary not found
)

const usageText = `usage: xrr <command> [flags]

commands:
  run    run a command with PATH shims that record/replay external CLIs
  shim   internal: entrypoint of the generated shim scripts

run 'xrr run -h' for flags.
`

func main() { os.Exit(dispatch(os.Args[1:], os.Stdout, os.Stderr)) }

func dispatch(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usageText)
		return exitUsage
	}
	switch args[0] {
	case "run":
		return runCmd(args[1:], stdout, stderr)
	case "shim":
		return shimCmd(args[1:], stderr)
	case "-h", "--help", "help":
		fmt.Fprint(stdout, usageText)
		return exitOK
	default:
		fmt.Fprintf(stderr, "xrr: unknown command %q\n\n%s", args[0], usageText)
		return exitUsage
	}
}
