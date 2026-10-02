// Command bintrail-console serves the read-only Bintrail web console as a
// standalone binary, decoupled from the core `bintrail` CLI. All command
// behavior lives in the importable consoleapp package (the console sibling
// of cliapp); this main exists only to receive the -ldflags-injected build
// metadata (which must target main.* so the Makefile and build/.goreleaser.yaml
// ldflags keep working unchanged) and to own the process exit.
package main

import (
	"os"

	"github.com/dbtrail/dbtrail/consoleapp"
)

// Build-time variables injected via -ldflags. The names are deliberately the
// same as the bintrail binary's (main.Version/CommitSHA/BuildDate) so the
// Makefile's BINTRAIL_LDFLAGS applies to this binary verbatim.
var (
	Version   = "dev"
	CommitSHA = "none"
	BuildDate = "unknown"
)

func main() {
	// The SQL sandbox worker is this same binary re-executed; answer that
	// before anything else so no per-process setup runs for every query.
	consoleapp.RunSQLWorkerIfInvoked()
	os.Exit(consoleapp.Main(Version, CommitSHA, BuildDate))
}
