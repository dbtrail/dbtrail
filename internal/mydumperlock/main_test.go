package mydumperlock

import (
	"os"
	"testing"
	"time"
)

// productProbeTimeout is ProbeTimeout as the package initialises it, read by
// TestMain before it widens the variable for the tests.
var productProbeTimeout time.Duration

// TestMain widens ProbeTimeout for every test in this package, after keeping
// the product value for TestProbeTimeoutProductDefault.
//
// The tests here run fake mydumper scripts that answer at once, but under a
// full `go test ./...` on a loaded machine starting one of those scripts has
// taken longer than the 10s product bound, and the test failed with "did not
// answer within 10s" while ProbeVersion was fine. The one test about the bound
// itself, TestProbeVersionGivesUpOnABinaryThatNeverAnswers, sets its own short
// value and restores this one.
func TestMain(m *testing.M) {
	productProbeTimeout = ProbeTimeout
	ProbeTimeout = 2 * time.Minute
	os.Exit(m.Run())
}

// TestProbeTimeoutProductDefault pins the bound the product ships with: a
// mydumper that never answers --version gives up after 10s, so it cannot hold
// the console's startup line or `bintrail dump` for longer. The value is read
// before TestMain widens it, so the tests' own room does not leak into this
// assertion.
func TestProbeTimeoutProductDefault(t *testing.T) {
	if productProbeTimeout != 10*time.Second {
		t.Fatalf("ProbeTimeout ships as %s, want 10s", productProbeTimeout)
	}
}
