package cliapp

import (
	"os"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// TestMain widens mydumperlock.ProbeTimeout for every test in this package.
//
// The product bound stays 10s (pinned in internal/mydumperlock): a mydumper
// that never answers --version must not hang `bintrail dump`. The tests here
// run fake mydumper scripts that answer at once, but under a full
// `go test ./...` on a loaded machine starting one of those scripts has taken
// longer than 10s, and the test failed with "did not answer within 10s" while
// the code under test was fine. No test in this package is about that bound,
// so a slow process start gets room here instead.
//
// Set once, before any test runs, so a new fake-mydumper test cannot forget
// it. A test that needs the timeout to fire sets its own short value and
// restores this one with t.Cleanup.
func TestMain(m *testing.M) {
	mydumperlock.ProbeTimeout = 2 * time.Minute
	os.Exit(m.Run())
}
