package rotation

import (
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
)

// ─── archive→drop TOCTOU guard decision (issue #779) ─────────────────────────

// TestArchivedPartitionUnchanged pins the pure decision behind the guard that
// refuses to drop a partition whose live row count has drifted from what was
// archived. binlog_events is append-only, so a mismatch means rows landed after
// the archive SELECT (e.g. a backfilled gap replayed with original timestamps
// into the oldest RANGE partition during the upload window) and dropping would
// destroy them. An unknown archived count is never safe to drop on.
func TestArchivedPartitionUnchanged(t *testing.T) {
	cases := []struct {
		name     string
		archived sql.NullInt64
		live     int64
		want     bool
	}{
		{"unchanged → safe to drop", sql.NullInt64{Int64: 100, Valid: true}, 100, true},
		{"grew during upload → defer", sql.NullInt64{Int64: 100, Valid: true}, 150, false},
		{"shrank (tampered/impossible) → defer", sql.NullInt64{Int64: 100, Valid: true}, 50, false},
		{"empty partition unchanged → safe", sql.NullInt64{Int64: 0, Valid: true}, 0, true},
		{"unknown archived count → defer", sql.NullInt64{Valid: false}, 100, false},
		{"unknown archived, zero live → defer", sql.NullInt64{Valid: false}, 0, false},
	}
	for _, c := range cases {
		if got := archivedPartitionUnchanged(c.archived, c.live); got != c.want {
			t.Errorf("%s: archivedPartitionUnchanged(%+v, %d) = %v, want %v",
				c.name, c.archived, c.live, got, c.want)
		}
	}
}

// ─── nextPartitionStart ─────────────────────────────────────────────────────────────

func TestNextPartitionStart_withNamedPartitions(t *testing.T) {
	partitions := []partitionInfo{
		{Name: "p_2026021700"},
		{Name: "p_2026021800"},
		{Name: "p_2026021900"},
		{Name: "p_future", Description: "MAXVALUE"},
	}
	next := nextPartitionStart(partitions)
	// Latest is p_2026021900 (hour 00 on 2026-02-19), next should be hour 01.
	if next.Year() != 2026 || next.Month() != 2 || next.Day() != 19 || next.Hour() != 1 {
		t.Errorf("expected 2026-02-19 01:00, got %v", next)
	}
}

func TestNextPartitionStart_onlyFuture(t *testing.T) {
	// No named partitions — should return the current hour (UTC, truncated to hour).
	partitions := []partitionInfo{
		{Name: "p_future", Description: "MAXVALUE"},
	}
	thisHour := time.Now().UTC().Truncate(time.Hour)
	next := nextPartitionStart(partitions)
	if !next.Equal(thisHour) {
		t.Errorf("expected current hour %v, got %v", thisHour, next)
	}
}

func TestNextPartitionStart_empty(t *testing.T) {
	thisHour := time.Now().UTC().Truncate(time.Hour)
	next := nextPartitionStart(nil)
	if !next.Equal(thisHour) {
		t.Errorf("expected current hour %v, got %v", thisHour, next)
	}
}

func TestNextPartitionStart_unsortedOrder(t *testing.T) {
	// Deliberately out of order — must find the maximum date, not the last element.
	partitions := []partitionInfo{
		{Name: "p_2026021700"},
		{Name: "p_2026021900"}, // latest
		{Name: "p_2026021800"},
		{Name: "p_future", Description: "MAXVALUE"},
	}
	next := nextPartitionStart(partitions)
	if next.Year() != 2026 || next.Month() != 2 || next.Day() != 19 || next.Hour() != 1 {
		t.Errorf("expected 2026-02-19 01:00 (max+1h), got %v", next)
	}
}

// ─── cutoff logic (hourly retain) ────────────────────────────────────────────

// TestCutoffDropsHourlyPartition verifies that --retain 1h correctly selects
// the previous-hour partition for dropping. This is the exact scenario from
// GitHub issue #96: at 00:05 UTC, "p_2026030223" (hour 23) should be eligible
// for dropping with --retain 1h, because its start time (23:00) is before the
// cutoff (23:05).
func TestCutoffDropsHourlyPartition(t *testing.T) {
	// Simulate: current time = 2026-03-03 00:05:15 UTC, --retain 1h.
	now := time.Date(2026, 3, 3, 0, 5, 15, 0, time.UTC)
	retainDur := time.Hour
	cutoff := now.Add(-retainDur) // 2026-03-02 23:05:15 UTC (no Truncate!)

	partitions := []struct {
		name     string
		wantDrop bool
	}{
		{"p_2026030222", true},  // hour 22 → 22:00 < 23:05 → drop
		{"p_2026030223", true},  // hour 23 → 23:00 < 23:05 → drop (the bug was here)
		{"p_2026030300", false}, // hour 00 → 00:00 is NOT < 23:05 → keep
		{"p_2026030301", false}, // hour 01 → keep
	}

	for _, p := range partitions {
		d, ok := indexer.PartitionDate(p.name)
		if !ok {
			t.Fatalf("indexer.PartitionDate(%q) returned false", p.name)
		}
		dropped := d.Before(cutoff)
		if dropped != p.wantDrop {
			t.Errorf("partition %s (date %v): Before(cutoff %v) = %v, want %v",
				p.name, d, cutoff, dropped, p.wantDrop)
		}
	}
}

// ─── addFuturePartitions SQL shape ────────────────────────────────────────────

// TestPartitionSpec_shape verifies the generated DDL looks correct without
// needing a live database.
func TestPartitionSpec_shape(t *testing.T) {
	start := time.Date(2026, 2, 20, 14, 0, 0, 0, time.UTC)
	// Build the partition spec strings the same way addFuturePartitions does.
	parts := make([]string, 0, 3+1)
	for i := range 3 {
		d := start.Add(time.Duration(i) * time.Hour)
		nextHour := d.Add(time.Hour)
		parts = append(parts, "PARTITION "+indexer.PartitionName(d)+
			" VALUES LESS THAN (TO_SECONDS('"+
			nextHour.UTC().Format("2006-01-02 15:04:05")+"'))")
	}
	parts = append(parts, "PARTITION p_future VALUES LESS THAN MAXVALUE")

	if len(parts) != 4 {
		t.Fatalf("expected 4 parts, got %d", len(parts))
	}
	if !strings.Contains(parts[0], "p_2026022014") {
		t.Errorf("expected p_2026022014 in first spec: %s", parts[0])
	}
	if !strings.Contains(parts[0], "2026-02-20 15:00:00") {
		t.Errorf("expected 2026-02-20 15:00:00 (next hour boundary) in first spec: %s", parts[0])
	}
	if !strings.Contains(parts[1], "p_2026022015") {
		t.Errorf("expected p_2026022015 in second spec: %s", parts[1])
	}
	if parts[3] != "PARTITION p_future VALUES LESS THAN MAXVALUE" {
		t.Errorf("expected p_future catch-all last, got: %s", parts[3])
	}
}

// ─── countFuturePartitions ───────────────────────────────────────────────────

func TestCountFuturePartitions(t *testing.T) {
	ref := time.Date(2026, 2, 19, 12, 0, 0, 0, time.UTC)
	partitions := []partitionInfo{
		{Name: "p_2026021910"}, // past
		{Name: "p_2026021911"}, // past
		{Name: "p_2026021912"}, // equal to ref → not strictly after
		{Name: "p_2026021913"}, // future
		{Name: "p_2026021914"}, // future
		{Name: "p_future", Description: "MAXVALUE"},
		{Name: "garbage"},
	}
	if got := countFuturePartitions(partitions, ref); got != 2 {
		t.Errorf("expected 2 future partitions, got %d", got)
	}
	if got := countFuturePartitions(nil, ref); got != 0 {
		t.Errorf("expected 0 for nil slice, got %d", got)
	}
}

// ─── autoAdd calculation (declarative --add-future) ─────────────────────────

// TestAutoAddDeclarative verifies the new declarative toAdd formula: --add-future
// is a target headroom (top-up only), plus auto-replacements for drops.
func TestAutoAddDeclarative(t *testing.T) {
	cases := []struct {
		name        string
		addFuture   int
		futureCount int
		dropped     int
		noReplace   bool
		wantToAdd   int
	}{
		{"steady state", 1, 1, 0, false, 0},
		{"cold start", 3, 0, 0, false, 3},
		{"drops with replace", 1, 1, 1, false, 1},        // 0 top-up + 1 replacement
		{"drops no replace", 1, 1, 1, true, 0},           // 0 top-up, no replacement
		{"over target never shrinks", 1, 5, 0, false, 0}, // futureCount > target
		{"under target top-up", 3, 1, 0, false, 2},
		{"under target plus drops", 3, 1, 2, false, 4}, // 2 top-up + 2 replacements
		{"no-replace ignores drops", 3, 1, 2, true, 2}, // 2 top-up only
		{"no flags", 0, 0, 0, false, 0},
		{"no-replace over target ignores drops", 1, 5, 3, true, 0},
	}
	for _, c := range cases {
		toAdd := computeToAdd(c.addFuture, c.futureCount, c.dropped, c.noReplace)
		if toAdd != c.wantToAdd {
			t.Errorf("%s: expected toAdd=%d, got %d", c.name, c.wantToAdd, toAdd)
		}
	}
}

// TestAutoAddDeclarative_48CycleSimulation replays the daemon rotation loop
// for 48 cycles to confirm the declarative formula keeps the partition count
// stable and does not leak (regression test for issue #199).
func TestAutoAddDeclarative_48CycleSimulation(t *testing.T) {
	// Start: 12h retention window already populated + 1 future partition,
	// plus p_future catch-all. Total: 13 named + 1 MAXVALUE = 14.
	now := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	parts := []partitionInfo{}
	for i := -11; i <= 1; i++ { // 12 past/current hours + 1 future
		parts = append(parts, partitionInfo{Name: indexer.PartitionName(now.Add(time.Duration(i) * time.Hour))})
	}
	parts = append(parts, partitionInfo{Name: "p_future", Description: "MAXVALUE"})

	retainDur := 12 * time.Hour
	addFuture := 1

	for cycle := 0; cycle < 48; cycle++ {
		cycleNow := now.Add(time.Duration(cycle) * time.Hour)
		cutoff := cycleNow.Add(-retainDur)

		// Drop expired.
		dropped := 0
		kept := parts[:0]
		for _, p := range parts {
			d, ok := indexer.PartitionDate(p.Name)
			if ok && d.Before(cutoff) {
				dropped++
				continue
			}
			kept = append(kept, p)
		}
		parts = kept

		// Compute toAdd with the production helper.
		nowHour := cycleNow.Truncate(time.Hour)
		futureCount := countFuturePartitions(parts, nowHour)
		toAdd := computeToAdd(addFuture, futureCount, dropped, false)

		// Append new future partitions after the latest existing named one.
		startDate := nextPartitionStart(parts)
		// Strip p_future, re-append later.
		withoutFuture := parts[:0]
		for _, p := range parts {
			if p.Name != "p_future" {
				withoutFuture = append(withoutFuture, p)
			}
		}
		parts = withoutFuture
		for i := 0; i < toAdd; i++ {
			parts = append(parts, partitionInfo{Name: indexer.PartitionName(startDate.Add(time.Duration(i) * time.Hour))})
		}
		parts = append(parts, partitionInfo{Name: "p_future", Description: "MAXVALUE"})
	}

	// Expected stable count: ~15 named partitions + p_future. The important
	// property is that the count does NOT grow linearly with cycle count
	// (the pre-fix bug leaked +1 partition per cycle → 48+ after 48 cycles).
	// Allow a small window around the steady state for boundary transitions.
	if len(parts) > 17 {
		t.Errorf("partition count leaked: got %d after 48 cycles, want ≤17 (steady state ~16)", len(parts))
	}
	if len(parts) < 14 {
		t.Errorf("partition count shrank unexpectedly: got %d after 48 cycles, want ≥14", len(parts))
	}

	// Second 48-cycle run from the same state must return the same count
	// (true steady state, no drift).
	startCount := len(parts)
	for cycle := 48; cycle < 96; cycle++ {
		cycleNow := now.Add(time.Duration(cycle) * time.Hour)
		cutoff := cycleNow.Add(-retainDur)
		dropped := 0
		kept := parts[:0]
		for _, p := range parts {
			d, ok := indexer.PartitionDate(p.Name)
			if ok && d.Before(cutoff) {
				dropped++
				continue
			}
			kept = append(kept, p)
		}
		parts = kept
		nowHour := cycleNow.Truncate(time.Hour)
		futureCount := countFuturePartitions(parts, nowHour)
		toAdd := computeToAdd(addFuture, futureCount, dropped, false)
		startDate := nextPartitionStart(parts)
		withoutFuture := parts[:0]
		for _, p := range parts {
			if p.Name != "p_future" {
				withoutFuture = append(withoutFuture, p)
			}
		}
		parts = withoutFuture
		for i := 0; i < toAdd; i++ {
			parts = append(parts, partitionInfo{Name: indexer.PartitionName(startDate.Add(time.Duration(i) * time.Hour))})
		}
		parts = append(parts, partitionInfo{Name: "p_future", Description: "MAXVALUE"})
	}
	if len(parts) != startCount {
		t.Errorf("steady-state drift: cycles 48→96 changed count from %d to %d", startCount, len(parts))
	}
}

// ─── fileExists ──────────────────────────────────────────────────────────────

func TestFileExists(t *testing.T) {
	dir := t.TempDir()

	// Non-existent file.
	if fileExists(dir + "/nope.parquet") {
		t.Error("expected false for non-existent file")
	}

	// Empty file (0 bytes).
	empty := dir + "/empty.parquet"
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if fileExists(empty) {
		t.Error("expected false for empty file")
	}

	// Non-empty file.
	valid := dir + "/valid.parquet"
	if err := os.WriteFile(valid, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !fileExists(valid) {
		t.Error("expected true for non-empty file")
	}
}

// ─── HiveArchivePath ────────────────────────────────────────────────────────────

func TestHiveArchivePath(t *testing.T) {
	path, err := HiveArchivePath("/data/archives", "d7bfcfe5-651e-42df-bcab-ffb9149abecb", "p_2026030214")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "/data/archives/bintrail_id=d7bfcfe5-651e-42df-bcab-ffb9149abecb/event_date=2026-03-02/event_hour=14/events.parquet"
	if path != want {
		t.Errorf("HiveArchivePath mismatch\n got: %s\nwant: %s", path, want)
	}
}

func TestHiveArchivePath_midnightHour(t *testing.T) {
	path, err := HiveArchivePath("/data", "abc", "p_2026010100")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "/data/bintrail_id=abc/event_date=2026-01-01/event_hour=00/events.parquet"
	if path != want {
		t.Errorf("HiveArchivePath mismatch\n got: %s\nwant: %s", path, want)
	}
}

func TestHiveArchivePath_invalidPartition(t *testing.T) {
	_, err := HiveArchivePath("/data", "abc", "p_future")
	if err == nil {
		t.Fatal("expected error for p_future, got nil")
	}
}

// ─── content range vs hour label (#1037) ─────────────────────────────────────

// TestEscapesLabelHour pins the trigger for the backfill warning and the
// content-range recording: a partition whose archived rows' [min, max]
// event_timestamp range extends outside its p_YYYYMMDDHH label hour holds
// backfilled events, which label-based archive pruning would lose.
func TestEscapesLabelHour(t *testing.T) {
	label := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
	name := "p_" + label.Format("2006010215")

	cases := []struct {
		name     string
		min, max time.Time
		want     bool
	}{
		{"content inside label hour", label.Add(time.Minute), label.Add(59 * time.Minute), false},
		{"backfilled: min two days early", label.Add(-48 * time.Hour), label.Add(30 * time.Minute), true},
		{"max spills into next hour", label, label.Add(time.Hour), true},
		{"empty partition (zero range)", time.Time{}, time.Time{}, false},
		{"exact hour boundaries", label, label.Add(59*time.Minute + 59*time.Second), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := escapesLabelHour(name, tc.min, tc.max); got != tc.want {
				t.Errorf("escapesLabelHour(%s, %v, %v) = %v, want %v", name, tc.min, tc.max, got, tc.want)
			}
		})
	}

	if escapesLabelHour("p_future", label.Add(-time.Hour), label) {
		t.Error("p_future has no hour label; must never report an escape")
	}
}

// TestHoursBehind: the partitions missing between the latest named one and
// the current hour, the current hour included (#2409).
func TestHoursBehind(t *testing.T) {
	now := time.Date(2026, 10, 15, 14, 0, 0, 0, time.UTC)
	named := func(hours ...int) []partitionInfo {
		ps := []partitionInfo{}
		for _, h := range hours {
			ps = append(ps, partitionInfo{Name: indexer.PartitionName(now.Add(time.Duration(h) * time.Hour))})
		}
		return append(ps, partitionInfo{Name: "p_future", Description: "MAXVALUE"})
	}
	cases := []struct {
		name  string
		parts []partitionInfo
		want  int
	}{
		{"one hour behind", named(-3, -2, -1), 1},
		{"current hour exists", named(-1, 0), 0},
		{"ahead", named(0, 1, 2), 0},
		{"eight hours behind", named(-9, -8), 8},
		{"only p_future", named(), 0},
		{"nil", nil, 0},
		{"unsorted", named(-1, -5, -2), 1},
	}
	for _, c := range cases {
		if got := hoursBehind(c.parts, now); got != c.want {
			t.Errorf("%s: hoursBehind = %d, want %d", c.name, got, c.want)
		}
	}
}

// TestRotationCatchesUpWhenBehind replays the built-in loop (NoReplace, as
// loopOptions sets it) from a table whose latest partition is an hour behind
// the clock. Before #2409 every cycle added only the current hour, so each
// new hour began with no partition and its rows in p_future; after the first
// cycle there must be a partition ahead at every cycle.
func TestRotationCatchesUpWhenBehind(t *testing.T) {
	start := time.Date(2026, 10, 15, 0, 41, 0, 0, time.UTC)
	parts := []partitionInfo{}
	for i := -12; i <= -1; i++ {
		parts = append(parts, partitionInfo{Name: indexer.PartitionName(start.Truncate(time.Hour).Add(time.Duration(i) * time.Hour))})
	}
	parts = append(parts, partitionInfo{Name: "p_future", Description: "MAXVALUE"})
	for _, target := range []int{1, 3} {
		ps := append([]partitionInfo(nil), parts...)
		for cycle := range 24 {
			nowHour := start.Add(time.Duration(cycle) * time.Hour).Truncate(time.Hour)
			toAdd, _ := partitionsToAdd(target, ps, nowHour, 0, true)
			startDate := nextPartitionStart(ps)
			ps = ps[:len(ps)-1]
			for i := range toAdd {
				ps = append(ps, partitionInfo{Name: indexer.PartitionName(startDate.Add(time.Duration(i) * time.Hour))})
			}
			ps = append(ps, partitionInfo{Name: "p_future", Description: "MAXVALUE"})
			if got := countFuturePartitions(ps, nowHour); got < target {
				t.Fatalf("target %d, cycle %d: %d partitions ahead of %s, want %d", target, cycle, got, nowHour, target)
			}
			if hoursBehind(ps, nowHour.Add(time.Hour)) != 0 {
				t.Fatalf("target %d, cycle %d: the next hour has no partition", target, cycle)
			}
		}
	}
}

// TestPartitionsToAddBehindOnlyWithTarget: without a target (the CLI's
// rotate with no --add-future), replacements for drops add no catch-up, so
// a deferred add's suggested --add-future (futureCount+toAdd-behind) stays
// the replacements owed, never zero or negative (#2409).
func TestPartitionsToAddBehindOnlyWithTarget(t *testing.T) {
	now := time.Date(2026, 10, 15, 14, 0, 0, 0, time.UTC)
	ps := []partitionInfo{{Name: indexer.PartitionName(now.Add(-5 * time.Hour))}, {Name: "p_future", Description: "MAXVALUE"}}
	cases := []struct {
		target, dropped     int
		noReplace           bool
		wantToAdd, wantBack int
	}{
		{0, 2, false, 2, 0},
		{0, 0, false, 0, 0},
		{3, 0, true, 8, 5},
		{3, 2, false, 10, 5},
	}
	for _, c := range cases {
		toAdd, behind := partitionsToAdd(c.target, ps, now, c.dropped, c.noReplace)
		if toAdd != c.wantToAdd || behind != c.wantBack {
			t.Errorf("target %d dropped %d: got (%d, %d), want (%d, %d)", c.target, c.dropped, toAdd, behind, c.wantToAdd, c.wantBack)
		}
		if toAdd > 0 && toAdd-behind <= 0 {
			t.Errorf("target %d dropped %d: deferred target %d", c.target, c.dropped, toAdd-behind)
		}
	}
}
