package consoleapp

import (
	"context"
	"os"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// TestMain widens mydumperlock.ProbeTimeout for every test in this package.
//
// The product bound stays 10s (pinned in internal/mydumperlock): a mydumper
// that never answers --version must not hold the console up. The tests here
// run fake mydumper scripts that answer at once, but under a full
// `go test ./...` on a loaded machine starting one of those scripts has taken
// longer than 10s, and the test failed with "did not answer within 10s" while
// the code under test was fine. No test in this package is about that bound,
// so a slow process start gets room here instead.
//
// Set once, before any test runs: nothing in this package mutates it while a
// baseline job goroutine may be probing in the background, and a new
// fake-mydumper test cannot forget it. A test that needs the timeout to fire
// sets its own short value and restores this one with t.Cleanup.
func TestMain(m *testing.M) {
	// Re-executed as the SQL sandbox worker (sqlsandbox.New with a zero
	// Config spawns THIS binary with the worker command): run the real root
	// command, pre-run hook included, exactly as the shipped binary does one
	// cobra dispatch in.
	if sqlsandbox.IsWorkerInvocation(os.Args) {
		rootCmd.SetArgs(os.Args[1:])
		os.Exit(Main("test", "none", "unknown"))
	}
	mydumperlock.ProbeTimeout = fakeMydumperBound
	// Password tests here hash through the console package: at the shipped
	// cost each one costs seconds under -race. The cost itself is pinned by
	// the console package's own tests.
	console.SetBcryptCostForTest(bcrypt.MinCost)
	// A full read under the default TLS mode (preferred) asks the source
	// whether it encrypts before mydumper runs (#1996). The tests here run
	// fake mydumpers against sources that do not exist, so the question gets
	// a fixed answer: no TLS, the read goes on in cleartext as it always did
	// for them. A test about that decision sets its own answer
	// (stubSourceEncrypted); the integration tests restore the real probe.
	sourceEncrypted = func(context.Context, string, config.SSL) (bool, error) { return false, nil }
	// Same for the mandatory-TLS check before a Connector/C mydumper.
	sourceTLSPin = func(context.Context, string, dumpTLS) (string, error) { return "00:11", nil }
	os.Exit(m.Run())
}

// realSourceEncrypted is the production probe, captured before TestMain
// replaces it.
var realSourceEncrypted = sourceEncrypted

// realSourceTLSPin is the production check, captured before TestMain
// replaces it.
var realSourceTLSPin = sourceTLSPin

// fakeMydumperBound is how long a test here gives a fake mydumper: the
// --version probe's bound, and the deadline of any wait for a job that runs
// the probe. A wait shorter than the probe fails the same way under load, one
// step later: the probe answers after 12s and the test gave up at 10s.
const fakeMydumperBound = 2 * time.Minute
