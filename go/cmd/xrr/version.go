package main

import "runtime/debug"

// version is stamped at release time:
//
//	-ldflags "-X main.version=<semver>"
//
// Empty for plain `go build` / `go install`; resolveVersion then falls back
// to the module version recorded in the build info.
var version string

func resolveVersion() string {
	if version != "" {
		return version
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return "dev"
}
