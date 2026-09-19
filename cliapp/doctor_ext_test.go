package cliapp

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/doctor"
)

func TestExtCheckResultKnownStatusesPassThrough(t *testing.T) {
	for _, status := range []string{"pass", "fail", "warn", "skip"} {
		got := extCheckResult(ext.DoctorCheck{Name: "n", Status: status, Detail: "d", Remediation: "r"})
		if got.Status != doctor.CheckStatus(status) || got.Name != "n" || got.Detail != "d" || got.Remediation != "r" {
			t.Errorf("status %q: got %+v", status, got)
		}
	}
}

// TestExtCheckResultStatusCaseInsensitive: the incoming status is normalized
// (trimmed, lowercased) before matching, so any casing/padding of the four
// canonical statuses passes through instead of degrading to WARN.
func TestExtCheckResultStatusCaseInsensitive(t *testing.T) {
	cases := map[string]doctor.CheckStatus{
		"PASS":   doctor.StatusPass,
		"Fail":   doctor.StatusFail,
		" warn ": doctor.StatusWarn,
		"SKIP":   doctor.StatusSkip,
	}
	for in, want := range cases {
		got := extCheckResult(ext.DoctorCheck{Name: "n", Status: in, Detail: "d"})
		if got.Status != want {
			t.Errorf("status %q: got %q, want %q", in, got.Status, want)
		}
		if got.Detail != "d" {
			t.Errorf("status %q: detail = %q, want it untouched", in, got.Detail)
		}
	}
}

func TestExtCheckResultUnknownStatusBecomesWarn(t *testing.T) {
	// "flaky" is not a canonical status in any casing — it must degrade to a
	// counted WARN rather than an uncounted malformed entry.
	got := extCheckResult(ext.DoctorCheck{Name: "n", Status: "flaky", Detail: "d"})
	if got.Status != doctor.StatusWarn {
		t.Fatalf("status = %q, want %q", got.Status, doctor.StatusWarn)
	}
	if !strings.Contains(got.Detail, `unknown status "flaky"`) || !strings.HasPrefix(got.Detail, "d") {
		t.Errorf("detail = %q, want original detail plus an unknown-status note", got.Detail)
	}
}

// TestAppendExtDoctorChecksDefaultNoop pins the stock-binary behavior: with
// no registered checks the report is untouched. ResetForTest first so the pin
// holds regardless of what a sibling test registered before this one ran.
func TestAppendExtDoctorChecksDefaultNoop(t *testing.T) {
	ext.ResetForTest()
	report := &doctor.Report{}
	appendExtDoctorChecks(context.Background(), report, "src", "idx")
	if len(report.Checks) != 0 || report.Warnings != 0 {
		t.Fatalf("report changed with nothing registered: %+v", report)
	}
}

// TestAppendExtDoctorChecksPanicBecomesFail: a panicking registered check
// must not lose the already-computed report — it degrades to a FAIL entry
// naming the extension battery, with the panic text in the detail.
func TestAppendExtDoctorChecksPanicBecomesFail(t *testing.T) {
	t.Cleanup(ext.ResetForTest)
	ext.RegisterDoctorCheck(func(context.Context, string, string) []ext.DoctorCheck {
		panic("check exploded")
	})

	report := &doctor.Report{}
	report.Add(doctor.CheckResult{Name: "builtin", Status: doctor.StatusPass})
	appendExtDoctorChecks(context.Background(), report, "src", "idx")

	if len(report.Checks) != 2 {
		t.Fatalf("report has %d checks, want the builtin plus the FAIL entry: %+v", len(report.Checks), report.Checks)
	}
	var found bool
	for _, c := range report.Checks {
		if c.Name != "extension doctor checks" {
			continue
		}
		found = true
		if c.Status != doctor.StatusFail {
			t.Errorf("status = %q, want %q", c.Status, doctor.StatusFail)
		}
		if !strings.Contains(c.Detail, "check exploded") {
			t.Errorf("detail = %q, want the panic text", c.Detail)
		}
	}
	if !found {
		t.Fatalf("no FAIL entry for the panicking battery; checks = %+v", report.Checks)
	}
}

// TestRunDoctorToRendersRegisteredCheck drives the real doctor entry point
// (runDoctorTo) with a registered extension check and asserts the check name
// appears in BOTH rendered formats — pinning the appendExtDoctorChecks call
// site inside runDoctorTo. The DSN points at a closed local port so the
// built-in checks fail fast; only the presence of the registered check's
// name is asserted, so built-in failures are irrelevant here.
func TestRunDoctorToRendersRegisteredCheck(t *testing.T) {
	t.Cleanup(ext.ResetForTest)
	const name = "ext-e2e-preflight-check-93b1"
	ext.RegisterDoctorCheck(func(context.Context, string, string) []ext.DoctorCheck {
		return []ext.DoctorCheck{{Name: name, Status: "pass", Detail: "registered check ran"}}
	})

	// Port 1 on loopback refuses connections immediately — unreachable is
	// fine (and fast) for this pin.
	const badDSN = "root:x@tcp(127.0.0.1:1)/"
	for _, format := range []string{"text", "json"} {
		var buf bytes.Buffer
		// The built-in checks fail against the unreachable DSN, so the
		// returned error is expected; the rendered report is what matters.
		_ = runDoctorTo(context.Background(), &buf, format, badDSN, "", "", 0, "", "", "", "")
		if !strings.Contains(buf.String(), name) {
			t.Errorf("%s output does not contain the registered check %q:\n%s", format, name, buf.String())
		}
	}
}
