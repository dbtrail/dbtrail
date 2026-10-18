package pgstreamrun

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

func TestSlotHealthResult(t *testing.T) {
	tests := []struct {
		name              string
		h                 pgcapture.SlotHealth
		wantStatus        doctor.CheckStatus
		wantInDetailOrRem string
	}{
		{"absent", pgcapture.SlotHealth{Exists: false}, doctor.StatusSkip, "does not exist"},
		{"reserved", pgcapture.SlotHealth{Exists: true, WalStatus: "reserved", Active: true}, doctor.StatusPass, "wal_status=reserved"},
		{"empty-status-passes", pgcapture.SlotHealth{Exists: true, WalStatus: ""}, doctor.StatusPass, "wal_status="},
		{"extended", pgcapture.SlotHealth{Exists: true, WalStatus: "extended"}, doctor.StatusWarn, "approaching"},
		{"unreserved", pgcapture.SlotHealth{Exists: true, WalStatus: "unreserved"}, doctor.StatusWarn, "approaching"},
		{"lost", pgcapture.SlotHealth{Exists: true, WalStatus: "lost"}, doctor.StatusFail, "re-baseline"},
		{"unknown-status-warns", pgcapture.SlotHealth{Exists: true, WalStatus: "weird"}, doctor.StatusWarn, "Unrecognized wal_status"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := slotHealthResult(tt.h, "myslot")
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q", got.Status, tt.wantStatus)
			}
			hay := got.Detail + " " + got.Remediation
			if !strings.Contains(hay, tt.wantInDetailOrRem) {
				t.Errorf("detail/remediation %q missing %q", hay, tt.wantInDetailOrRem)
			}
		})
	}
}

// TestSlotHealthResult_lostHasRecoveryPath pins the load-bearing #532 acceptance: a
// lost slot is a loud FAIL whose remediation names the manual re-baseline steps and
// reassures that recovery (which is index-only) is unaffected.
func TestSlotHealthResult_lostHasRecoveryPath(t *testing.T) {
	got := slotHealthResult(pgcapture.SlotHealth{Exists: true, WalStatus: "lost"}, "s1")
	if got.Status != doctor.StatusFail {
		t.Fatalf("lost slot must FAIL, got %q", got.Status)
	}
	for _, want := range []string{"bintrail-pg reset", "--slot s1", "recovery never needs the slot"} {
		if !strings.Contains(got.Remediation, want) {
			t.Errorf("lost remediation missing %q:\n%s", want, got.Remediation)
		}
	}
}

func TestSlotHealthResult_safeWalSizeRendering(t *testing.T) {
	withSafe := slotHealthResult(pgcapture.SlotHealth{
		Exists: true, WalStatus: "reserved", RetainedBytes: 2048,
		SafeWalSize: sql.NullInt64{Int64: 1048576, Valid: true},
	}, "s")
	if !strings.Contains(withSafe.Detail, "retained_wal=2.0 KiB") {
		t.Errorf("missing humanized retained_wal: %q", withSafe.Detail)
	}
	if !strings.Contains(withSafe.Detail, "safe_wal=1.0 MiB") {
		t.Errorf("missing safe_wal when valid: %q", withSafe.Detail)
	}
	// NULL safe_wal_size (unlimited retention) → safe_wal omitted entirely.
	noSafe := slotHealthResult(pgcapture.SlotHealth{Exists: true, WalStatus: "reserved"}, "s")
	if strings.Contains(noSafe.Detail, "safe_wal=") {
		t.Errorf("safe_wal must be omitted when NULL: %q", noSafe.Detail)
	}
}

// TestWalLevelResult pins the distinction the silent-failure review required: a
// genuine wal_level!=logical config error FAILs and SKIPs dependents, while a query
// failure FAILs but does NOT skip (so a transient blip never suppresses slot-health).
func TestWalLevelResult(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		res, skip := walLevelResult(nil)
		if res.Status != doctor.StatusPass || skip {
			t.Errorf("nil err → got (%s, skip=%t), want (pass, false)", res.Status, skip)
		}
	})
	t.Run("not-logical-skips", func(t *testing.T) {
		err := fmt.Errorf("wrap: %w", pgcapture.ErrWALLevelNotLogical)
		res, skip := walLevelResult(err)
		if res.Status != doctor.StatusFail || !skip {
			t.Errorf("not-logical → got (%s, skip=%t), want (fail, true)", res.Status, skip)
		}
		if !strings.Contains(res.Remediation, "restart the server") {
			t.Errorf("not-logical remediation should mention the restart: %q", res.Remediation)
		}
	})
	t.Run("query-error-does-not-skip", func(t *testing.T) {
		res, skip := walLevelResult(errors.New("connection refused"))
		if res.Status != doctor.StatusFail || skip {
			t.Errorf("query error → got (%s, skip=%t), want (fail, false — must not suppress dependents)", res.Status, skip)
		}
		if !strings.Contains(res.Remediation, "Retry") {
			t.Errorf("query-error remediation should suggest retry, not a server restart: %q", res.Remediation)
		}
	})
}

func TestKeepSizeResult(t *testing.T) {
	t.Run("unlimited-warns", func(t *testing.T) {
		res := keepSizeResult("-1")
		if res.Status != doctor.StatusWarn {
			t.Errorf("'-1' → %s, want warn (the production red line)", res.Status)
		}
		if !strings.Contains(res.Remediation, "disk fills") {
			t.Errorf("unlimited remediation should name the disk-fill risk: %q", res.Remediation)
		}
	})
	t.Run("bounded-passes", func(t *testing.T) {
		res := keepSizeResult("10GB")
		if res.Status != doctor.StatusPass || res.Detail != "10GB" {
			t.Errorf("'10GB' → (%s, %q), want (pass, 10GB)", res.Status, res.Detail)
		}
	})
}

func TestFormatBytes(t *testing.T) {
	cases := map[int64]string{
		0:          "0 B",
		512:        "512 B",
		1024:       "1.0 KiB",
		1536:       "1.5 KiB",
		1048576:    "1.0 MiB",
		1073741824: "1.0 GiB",
	}
	for in, want := range cases {
		if got := formatBytes(in); got != want {
			t.Errorf("formatBytes(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestUnloggedResult(t *testing.T) {
	// #555: no UNLOGGED tables → PASS.
	if got := unloggedResult(nil); got.Status != doctor.StatusPass {
		t.Errorf("no unlogged → %s, want pass", got.Status)
	}
	// UNLOGGED tables present → WARN naming them (a silent capture hole made visible).
	got := unloggedResult([]string{"public.cache", "public.tmp"})
	if got.Status != doctor.StatusWarn {
		t.Errorf("unlogged present → %s, want warn", got.Status)
	}
	if !strings.Contains(got.Detail, "public.cache") || !strings.Contains(got.Detail, "public.tmp") {
		t.Errorf("detail should name the UNLOGGED tables, got %q", got.Detail)
	}
	if !strings.Contains(got.Remediation, "SET LOGGED") {
		t.Errorf("remediation should mention SET LOGGED, got %q", got.Remediation)
	}
}

func TestCascadeCoverageResult(t *testing.T) {
	// #556: every cascade child covered → PASS.
	if got := cascadeCoverageResult(nil); got.Status != doctor.StatusPass {
		t.Errorf("no uncovered children → %s, want pass", got.Status)
	}
	// An uncovered cascade child → WARN naming the child, action, and parent.
	got := cascadeCoverageResult([]pgcapture.CascadeChild{
		{Child: "public.line_items", Parent: "public.orders", Action: "ON DELETE CASCADE"},
	})
	if got.Status != doctor.StatusWarn {
		t.Errorf("uncovered child → %s, want warn", got.Status)
	}
	if !strings.Contains(got.Detail, "public.line_items") ||
		!strings.Contains(got.Detail, "ON DELETE CASCADE") ||
		!strings.Contains(got.Detail, "public.orders") {
		t.Errorf("detail should name child + action + parent, got %q", got.Detail)
	}
}

// track_commit_timestamp is informational (#2441): it passes either way and
// says what each value means for inferred schema changes.
func TestCommitTimestampResult(t *testing.T) {
	on := commitTimestampResult("on")
	off := commitTimestampResult("off")
	if on.Status != doctor.StatusPass || off.Status != doctor.StatusPass {
		t.Fatalf("statuses %s / %s, want pass both", on.Status, off.Status)
	}
	if !strings.Contains(on.Detail, "exact commit time") || !strings.Contains(off.Detail, "at or before") || !strings.Contains(off.Detail, "track_commit_timestamp = on") {
		t.Fatalf("details: %q / %q", on.Detail, off.Detail)
	}
}
