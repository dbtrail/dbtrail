package doctor

import (
	"strings"
	"testing"
	"time"
)

func pausedProbe(now time.Time, hours int, free uint64, freeKnown bool) CapacityProbe {
	var parts []CapacityPartition
	for i := 1; i <= hours; i++ {
		parts = append(parts, CapacityPartition{Hour: now.Add(-time.Duration(i) * time.Hour), Rows: 1000, Bytes: 1_000_000})
	}
	return CapacityProbe{Partitions: parts, TableVisible: true, FreeBytes: free, FreeKnown: freeKnown}
}

// TestEvaluateCapacity_dropsPaused_2323: an index the rotation loop refuses to
// drop on its window is not bounded by that window. It is graded as growing
// unbounded, with its own reason, because the advice for no_retention ("turn
// rotation on") is wrong for a loop that is on and waiting for a choice.
func TestEvaluateCapacity_dropsPaused_2323(t *testing.T) {
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	oldest := time.Date(2026, 1, 15, 3, 0, 0, 0, time.UTC)
	paused := &DropsPaused{Oldest: oldest}
	window := 30 * 24 * time.Hour

	t.Run("paused: warn, no projection, a forecast, and the window it would keep", func(t *testing.T) {
		m := EvaluateCapacity(pausedProbe(now, 6, 2_000_000_000, true), window, true, paused, now)
		if m.Status != StatusWarn || m.Reason != CapacityRotationPaused {
			t.Fatalf("status/reason = %s/%s, want warn/rotation_paused", m.Status, m.Reason)
		}
		if !m.DropsPaused || !m.PausedOldest.Equal(oldest) || m.Retain != window {
			t.Fatalf("measurement = %+v, want paused over the 30d window since %v", m, oldest)
		}
		if m.ProjectedBytes != 0 || m.RemainingBytes != 0 {
			t.Fatalf("projected/remaining = %v/%v, want none", m.ProjectedBytes, m.RemainingBytes)
		}
		if d, ok := m.DaysUntilFull(); !ok || d < 83 || d > 84 {
			t.Fatalf("DaysUntilFull = %v/%v, want ≈83", d, ok)
		}
		r := capacityCheckResult(m, "binlog_index", "")
		for _, want := range []string{"2026-01-15 03:00 UTC", "more than twice the 30d", "a daemon running without a retention of its own", "grows unbounded", "days until the volume fills"} {
			if !strings.Contains(r.Detail, want) {
				t.Errorf("detail lacks %q:\n%s", want, r.Detail)
			}
		}
		for _, want := range []string{"--rotate-retain", "BINTRAIL_ROTATE_RETAIN", "Edit rotation", "--retain <that window>"} {
			if !strings.Contains(r.Remediation, want) {
				t.Errorf("remediation lacks %q:\n%s", want, r.Remediation)
			}
		}
		if strings.Contains(r.Remediation, "rotates by default") {
			t.Errorf("remediation is the no_retention one, which tells an operator to turn on a loop that is on:\n%s", r.Remediation)
		}
		t.Logf("doctor line:\n%s\n%s", r.Detail, r.Remediation)
	})

	t.Run("not paused: the same probe over the same window projects and passes", func(t *testing.T) {
		m := EvaluateCapacity(pausedProbe(now, 6, 2_000_000_000, true), window, true, nil, now)
		if m.Reason != CapacityOK || m.DropsPaused || m.ProjectedBytes != 720_000_000 {
			t.Fatalf("m = %+v, want ok over 720 MB", m)
		}
		if _, ok := m.DaysUntilFull(); ok {
			t.Fatal("a bounded index has no date the disk fills")
		}
	})

	t.Run("paused, free space unknown: still said, without a forecast", func(t *testing.T) {
		m := EvaluateCapacity(pausedProbe(now, 6, 0, false), window, true, paused, now)
		if m.Reason != CapacityRotationPaused {
			t.Fatalf("reason = %s, want rotation_paused", m.Reason)
		}
		if _, ok := m.DaysUntilFull(); ok {
			t.Fatal("no free space, no forecast")
		}
	})

	t.Run("paused, too little history: the measuring skip stands", func(t *testing.T) {
		m := EvaluateCapacity(pausedProbe(now, 1, 2_000_000_000, true), window, true, paused, now)
		if m.Status != StatusSkip || m.Reason != CapacityNotEnoughHistory {
			t.Fatalf("status/reason = %s/%s, want skip/not_enough_history", m.Status, m.Reason)
		}
		// A restored index: the skip must still say nothing will be dropped.
		d := capacityCheckResult(m, "binlog_index", "").Detail
		for _, want := range []string{"not enough recent history", "2026-01-15 03:00 UTC", "drops none of it", "--rotate-retain"} {
			if !strings.Contains(d, want) {
				t.Errorf("detail lacks %q:\n%s", want, d)
			}
		}
		t.Logf("doctor skip line:\n%s", d)
		plain := capacityCheckResult(EvaluateCapacity(pausedProbe(now, 1, 2_000_000_000, true), window, true, nil, now), "binlog_index", "").Detail
		if strings.Contains(plain, "drops none") {
			t.Errorf("an index that is not paused was told it is:\n%s", plain)
		}
	})

	t.Run("a pause is meaningless without a known window or with rotation off", func(t *testing.T) {
		if m := EvaluateCapacity(pausedProbe(now, 6, 2_000_000_000, true), window, false, paused, now); m.Reason != CapacityRetentionUnknown || m.DropsPaused {
			t.Fatalf("unknown window: %s paused=%v, want retention_unknown", m.Reason, m.DropsPaused)
		}
		if m := EvaluateCapacity(pausedProbe(now, 6, 2_000_000_000, true), 0, true, paused, now); m.Reason != CapacityNoRetention || m.DropsPaused {
			t.Fatalf("rotation off: %s paused=%v, want no_retention", m.Reason, m.DropsPaused)
		}
	})
}

func TestCliRetain(t *testing.T) {
	for d, want := range map[time.Duration]string{30 * 24 * time.Hour: "30d", 48 * time.Hour: "48h", 7 * 24 * time.Hour: "7d", 6 * time.Hour: "6h", 36 * time.Hour: "36h"} {
		if got := cliRetain(d); got != want {
			t.Errorf("cliRetain(%v) = %q, want %q", d, got, want)
		}
	}
}

// The option is the only way the doctor command's guard verdict reaches the
// capacity check; without it Build grades the assumed window as in force.
func TestWithDropsPaused_reachesTheBuildConfig(t *testing.T) {
	oldest := time.Date(2026, 1, 15, 3, 0, 0, 0, time.UTC)
	var cfg buildConfig
	WithDropsPaused(oldest)(&cfg)
	if cfg.dropsPaused == nil || !cfg.dropsPaused.Oldest.Equal(oldest) {
		t.Fatalf("dropsPaused = %+v, want the guard's oldest partition", cfg.dropsPaused)
	}
	var plain buildConfig
	WithRetainNote("x")(&plain)
	if plain.dropsPaused != nil {
		t.Fatal("a build with no guard verdict must not be paused")
	}
}
