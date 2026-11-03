// Command fakecli is a stand-in for a real CLI and for a test suite, so the
// xrr tests can exercise shims without sh or bash (works on Windows). Its
// behaviour is picked by the name it was started under, minus any extension:
//
//	az     prints "az:<args>" to stdout, "az-err" to stderr; first arg "fail" exits 3
//	suite  calls `az account show`, `az fail`, and with arg "deny" also `open x`;
//	       arg "noenv" calls az with an empty environment
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	name := strings.ToLower(filepath.Base(os.Args[0]))
	for _, e := range []string{".exe", ".cmd", ".com", ".bat"} {
		if strings.HasSuffix(name, e) {
			name = strings.TrimSuffix(name, e)
			break
		}
	}
	switch name {
	case "az":
		fmt.Printf("az:%s\n", strings.Join(os.Args[1:], " "))
		fmt.Fprintln(os.Stderr, "az-err")
		if len(os.Args) > 1 && os.Args[1] == "fail" {
			os.Exit(3)
		}
	case "suite":
		suite(os.Args[1:])
	default:
		fmt.Fprintf(os.Stderr, "fakecli: unknown name %q\n", name)
		os.Exit(99)
	}
}

func call(env []string, bin string, args ...string) {
	cmd := exec.Command(bin, args...)
	if env != nil {
		cmd.Env = env
	}
	out, err := cmd.Output()
	rc := 0
	if ee, ok := err.(*exec.ExitError); ok {
		rc = ee.ExitCode()
	} else if err != nil {
		fmt.Printf("%s: %v\n", bin, err)
		rc = 100
	}
	fmt.Printf("%s%s rc=%d\n", out, args[0], rc)
}

func suite(args []string) {
	// "bin=<name>" makes the suite call <name> instead of az.
	az := "az"
	if len(args) > 0 && strings.HasPrefix(args[0], "bin=") {
		az, args = strings.TrimPrefix(args[0], "bin="), args[1:]
	}
	if len(args) > 0 && args[0] == "noenv" {
		bin, err := exec.LookPath(az)
		if err != nil {
			fmt.Println("lookpath:", err)
			os.Exit(1)
		}
		env := []string{}
		if sr := os.Getenv("SystemRoot"); sr != "" {
			env = append(env, "SystemRoot="+sr)
		}
		call(env, bin, "account", "show")
		return
	}
	if len(args) > 0 && args[0] == "rundir" {
		fmt.Printf("XRR_RUN_DIR=%s\n", os.Getenv("XRR_RUN_DIR"))
		return
	}
	call(nil, az, "account", "show")
	call(nil, az, "fail")
	if len(args) > 0 && args[0] == "deny" {
		call(nil, "open", "x")
	}
}
