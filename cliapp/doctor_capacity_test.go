package cliapp

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/doctor"
)

func TestParseDocRetain(t *testing.T) {
	for _, s := range []string{"off", "0", ""} {
		d, err := parseDocRetain(s)
		if err != nil || d != 0 {
			t.Errorf("parseDocRetain(%q) = (%v, %v), want (0, nil)", s, d, err)
		}
	}
	if d, err := parseDocRetain("7d"); err != nil || d != 7*24*time.Hour {
		t.Errorf("parseDocRetain(7d) = (%v, %v), want 168h", d, err)
	}
	if _, err := parseDocRetain("banana"); err == nil {
		t.Error("parseDocRetain(banana) must error")
	}
}

// TestUpPreflightOutcome pins up's advisory semantics: a capacity-only FAIL
// must not block boot but MUST surface the warning — losing either half would
// silently change what blocks `up` or swallow the operator's only disk-full
// signal. Reports are built from doctor's exported fields (Checks, which Err
// reads, plus the counters) rather than the package-private add().
func TestUpPreflightOutcome(t *testing.T) {
	clean := &doctor.Report{
		Checks: []doctor.CheckResult{{Name: "log_bin enabled", Status: doctor.StatusPass}},
		Passed: 1,
	}
	if fatal, warn := upPreflightOutcome(clean); fatal != nil || warn {
		t.Errorf("clean report: (fatal=%v, warn=%v), want (nil, false)", fatal, warn)
	}

	capOnly := &doctor.Report{
		Checks: []doctor.CheckResult{
			{Name: doctor.CapacityCheckName, Status: doctor.StatusFail},
			{Name: "log_bin enabled", Status: doctor.StatusPass},
		},
		Passed: 1,
		Failed: 1,
	}
	fatal, warn := upPreflightOutcome(capOnly)
	if fatal != nil {
		t.Errorf("capacity-only FAIL must not block boot, got fatal=%v", fatal)
	}
	if !warn {
		t.Error("capacity-only FAIL must surface the warning — the operator's only disk-full signal")
	}

	mixed := &doctor.Report{
		Checks: []doctor.CheckResult{
			{Name: doctor.CapacityCheckName, Status: doctor.StatusFail},
			{Name: "binlog_format=ROW", Status: doctor.StatusFail},
		},
		Failed: 2,
	}
	if fatal, _ := upPreflightOutcome(mixed); fatal == nil {
		t.Error("a non-advisory FAIL must block boot regardless of the capacity check")
	}

	// A missing primary key refuses `up` (#1766): it fails only while the
	// first snapshot is pending, when the stream would refuse a moment later
	// anyway.
	pk := &doctor.Report{
		Checks: []doctor.CheckResult{{Name: doctor.PrimaryKeyCheckName, Status: doctor.StatusFail}},
		Failed: 1,
	}
	if fatal, _ := upPreflightOutcome(pk); fatal == nil {
		t.Error("a missing primary key before the first snapshot must refuse up")
	}
}
