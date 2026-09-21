package consoleapp

import (
	"errors"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// TestWatchPreflightRefusalClassifies mirrors cliapp's test for the daemon
// that ships in the compose stack: watch's own upPreflightOutcome plus the
// shared doctor.BootRefusal must hand the telemetry hook the refusal's class,
// not unknown (#1503).
func TestWatchPreflightRefusalClassifies(t *testing.T) {
	r := &doctor.Report{}
	r.Add(doctor.CheckResult{Name: doctor.SourceConnectionCheckName, Status: doctor.StatusFail, Detail: "dial tcp: connection refused"})
	r.Add(doctor.CheckResult{Name: doctor.CapacityCheckName, Status: doctor.StatusFail})

	fatal, warn := upPreflightOutcome(r)
	if fatal == nil || warn {
		t.Fatalf("a source-connection failure must refuse boot (fatal=%v warn=%v)", fatal, warn)
	}
	var pe *doctor.PreflightError
	if !errors.As(fatal, &pe) {
		t.Fatalf("fatal is %T, want *doctor.PreflightError", fatal)
	}
	if got := telemetry.ClassifyError(doctor.BootRefusal(fatal)); got != telemetry.ClassDBConnection {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassDBConnection)
	}

	// Capacity alone is advisory: boot proceeds with a warning, no error.
	only := &doctor.Report{}
	only.Add(doctor.CheckResult{Name: doctor.CapacityCheckName, Status: doctor.StatusFail})
	if fatal, warn := upPreflightOutcome(only); fatal != nil || !warn {
		t.Errorf("capacity-only must warn, not refuse (fatal=%v warn=%v)", fatal, warn)
	}

	// A missing primary key before the first snapshot refuses boot (#1766):
	// the main stream would refuse on it a moment later, and that ends this
	// daemon too, so holding it advisory would only swap the fix for a stream
	// error.
	pk := &doctor.Report{}
	pk.Add(doctor.CheckResult{Name: doctor.PrimaryKeyCheckName, Status: doctor.StatusFail})
	if fatal, _ := upPreflightOutcome(pk); fatal == nil {
		t.Error("a missing primary key before the first snapshot must refuse watch")
	}
}

// tallyCheck counts a failed extra check as a failure (#1767): Test connection
// on a new server answers Failed == 0, so a failure counted as a pass would
// read as ready.
func TestTallyCheckCountsAFailure(t *testing.T) {
	out := &console.DoctorReport{}
	for _, s := range []string{"fail", "warn", "skip", "pass"} {
		tallyCheck(out, s)
	}
	if out.Failed != 1 || out.Warnings != 1 || out.Skipped != 1 || out.Passed != 1 {
		t.Errorf("tally = %+v, want one of each", *out)
	}
}
