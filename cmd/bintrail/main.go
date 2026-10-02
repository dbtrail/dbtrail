// Command bintrail is the OSS binary. All CLI behavior lives in the
// importable cliapp package; this main exists only to receive the
// -ldflags-injected build metadata (which must target main.* so the
// Makefile and build/.goreleaser.yaml ldflags keep working unchanged) and to
// own the process exit.
package main

import (
	"os"

	"github.com/dbtrail/dbtrail/cliapp"
)

// Build-time variables injected via -ldflags.
var (
	Version   = "dev"
	CommitSHA = "none"
	BuildDate = "unknown"
)

func main() {
	os.Exit(cliapp.Main(Version, CommitSHA, BuildDate))
}
