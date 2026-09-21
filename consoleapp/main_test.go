package consoleapp

import (
	"os"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/mydumperlock"
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
	mydumperlock.ProbeTimeout = fakeMydumperBound
	os.Exit(m.Run())
}

// fakeMydumperBound is how long a test here gives a fake mydumper: the
// --version probe's bound, and the deadline of any wait for a job that runs
// the probe. A wait shorter than the probe fails the same way under load, one
// step later: the probe answers after 12s and the test gave up at 10s.
const fakeMydumperBound = 2 * time.Minute
