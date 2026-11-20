package query

import (
	"testing"
	"time"
)

func TestParsePartitionName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantOK  bool
		wantUTC time.Time
	}{
		{"valid", "p_2026021914", true, time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)},
		{"valid midnight", "p_2026010100", true, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"p_future", "p_future", false, time.Time{}},
		{"too short", "p_20260219", false, time.Time{}},
		{"no prefix", "x_2026021914", false, time.Time{}},
		{"empty", "", false, time.Time{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParsePartitionName(tc.input)
			if ok != tc.wantOK {
				t.Fatalf("ParsePartitionName(%q) ok=%v, want %v", tc.input, ok, tc.wantOK)
			}
			if ok && !got.Equal(tc.wantUTC) {
				t.Errorf("ParsePartitionName(%q) = %v, want %v", tc.input, got, tc.wantUTC)
			}
		})
	}
}

func TestBuildContiguousRanges(t *testing.T) {
	h := func(day, hour int) time.Time {
		return time.Date(2026, 1, day, hour, 0, 0, 0, time.UTC)
	}

	hours := []time.Time{h(5, 10), h(5, 11), h(5, 12), h(5, 15), h(5, 16)}
	start := h(5, 10)
	end := h(5, 17)

	ranges := buildContiguousRanges(hours, start, end)
	if len(ranges) != 2 {
		t.Fatalf("expected 2 contiguous ranges, got %d: %+v", len(ranges), ranges)
	}
	if !ranges[0].Start.Equal(h(5, 10)) || !ranges[0].End.Equal(h(5, 13)) {
		t.Errorf("range[0] = %v – %v, want 10:00 – 13:00", ranges[0].Start, ranges[0].End)
	}
	if !ranges[1].Start.Equal(h(5, 15)) || !ranges[1].End.Equal(h(5, 17)) {
		t.Errorf("range[1] = %v – %v, want 15:00 – 17:00", ranges[1].Start, ranges[1].End)
	}
}

func TestBuildContiguousRanges_filtersOutOfRange(t *testing.T) {
	h := func(hour int) time.Time {
		return time.Date(2026, 1, 5, hour, 0, 0, 0, time.UTC)
	}

	hours := []time.Time{h(8), h(9), h(10), h(11), h(12)}
	start := h(10)
	end := h(12)

	ranges := buildContiguousRanges(hours, start, end)
	if len(ranges) != 1 {
		t.Fatalf("expected 1 range, got %d", len(ranges))
	}
	if !ranges[0].Start.Equal(h(10)) || !ranges[0].End.Equal(h(12)) {
		t.Errorf("range = %v – %v, want 10:00 – 12:00", ranges[0].Start, ranges[0].End)
	}
}

func TestBuildContiguousRanges_singleHour(t *testing.T) {
	h := time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)
	ranges := buildContiguousRanges([]time.Time{h}, h, h.Add(time.Hour))
	if len(ranges) != 1 {
		t.Fatalf("expected 1 range, got %d", len(ranges))
	}
	if !ranges[0].Start.Equal(h) || !ranges[0].End.Equal(h.Add(time.Hour)) {
		t.Errorf("range = %v – %v, want single hour", ranges[0].Start, ranges[0].End)
	}
}

func TestBuildContiguousRanges_empty(t *testing.T) {
	start := time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	ranges := buildContiguousRanges(nil, start, end)
	if len(ranges) != 0 {
		t.Errorf("expected empty ranges for nil hours, got %+v", ranges)
	}
}

func TestFormatGapWarning_noGaps(t *testing.T) {
	if w := FormatGapWarning(nil); w != "" {
		t.Errorf("expected empty warning for no gaps, got %q", w)
	}
}

func TestFormatGapWarning_withGaps(t *testing.T) {
	gaps := []time.Time{
		time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 5, 1, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 9, 23, 0, 0, 0, time.UTC),
	}
	w := FormatGapWarning(gaps)
	want := "query covers hours with no data (rotated and not archived): 2026-01-05 00:00 – 2026-01-09 23:00"
	if w != want {
		t.Errorf("got:\n  %q\nwant:\n  %q", w, want)
	}
}

func TestFormatGapWarning_singleHour(t *testing.T) {
	gaps := []time.Time{
		time.Date(2026, 3, 1, 14, 0, 0, 0, time.UTC),
	}
	w := FormatGapWarning(gaps)
	want := "query covers hours with no data (rotated and not archived): 2026-03-01 14:00 – 2026-03-01 14:00"
	if w != want {
		t.Errorf("got:\n  %q\nwant:\n  %q", w, want)
	}
}

func TestQueryPlan_SkipMySQL(t *testing.T) {
	// A nil plan (fallback) should NOT skip MySQL — safety default.
	if (*QueryPlan)(nil).SkipMySQL() {
		t.Error("expected SkipMySQL=false for nil plan")
	}

	// A plan with no MySQL ranges and no gaps means MySQL can be skipped.
	p := &QueryPlan{}
	if !p.SkipMySQL() {
		t.Error("expected SkipMySQL=true for empty (fully-archived) plan")
	}

	// If there are MySQL ranges, don't skip.
	p2 := &QueryPlan{
		MySQLRanges: []TimeRange{{
			Start: time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC),
			End:   time.Date(2026, 1, 5, 11, 0, 0, 0, time.UTC),
		}},
	}
	if p2.SkipMySQL() {
		t.Error("expected SkipMySQL=false when MySQL ranges exist")
	}

	// If there are gaps, don't skip (data might be there but not rotated yet).
	p3 := &QueryPlan{
		GapHours: []time.Time{time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)},
	}
	if p3.SkipMySQL() {
		t.Error("expected SkipMySQL=false when gaps exist")
	}
}

func TestPlan_nilDB(t *testing.T) {
	since := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	p, err := Plan(t.Context(), nil, "testdb", &since, nil, false, AllArchives())
	if err != nil {
		t.Fatal(err)
	}
	if p != nil {
		t.Errorf("expected nil plan for nil DB, got %+v", p)
	}
}

func TestPlan_noTimeRange(t *testing.T) {
	// Nil since and until should produce nil plan (no routing possible).
	p, err := Plan(t.Context(), nil, "testdb", nil, nil, false, AllArchives())
	if err != nil {
		t.Fatal(err)
	}
	if p != nil {
		t.Errorf("expected nil plan for no time range, got %+v", p)
	}
}

// ─── buildPlan tests (core routing logic) ─────────────────────────────────────

// OldestKnownHour lets GapError consumers tell a rotated hour from one that
// predates the index's existence (#1126). It must reflect exactly the hours
// the plan honours: under noArchive, archived history is excluded from the
// fetch, so the oldest LIVE hour is the oldest this plan can vouch for.
func TestBuildPlan_StampsOldestKnownHour(t *testing.T) {
	live := []time.Time{h(6, 10), h(6, 11)}
	archived := []time.Time{h(5, 3), h(5, 4)}

	if plan := buildPlan(live, archived, h(5, 0), h(6, 12), false); !plan.OldestKnownHour.Equal(h(5, 3)) {
		t.Errorf("archives honored: OldestKnownHour = %v, want %v", plan.OldestKnownHour, h(5, 3))
	}
	if plan := buildPlan(live, archived, h(5, 0), h(6, 12), true); !plan.OldestKnownHour.Equal(h(6, 10)) {
		t.Errorf("noArchive: OldestKnownHour = %v, want oldest LIVE hour %v", plan.OldestKnownHour, h(6, 10))
	}
	if plan := buildPlan(nil, nil, h(5, 0), h(5, 2), false); !plan.OldestKnownHour.IsZero() {
		t.Errorf("no partitions at all: OldestKnownHour must stay zero (unknown), got %v", plan.OldestKnownHour)
	}
}

func h(day, hour int) time.Time {
	return time.Date(2026, 1, day, hour, 0, 0, 0, time.UTC)
}

func TestBuildPlan_allLiveNoneArchived(t *testing.T) {
	live := []time.Time{h(5, 10), h(5, 11), h(5, 12)}
	plan := buildPlan(live, nil, h(5, 10), h(5, 13), false)

	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if plan.SkipMySQL() {
		t.Error("should not skip MySQL when all hours are live")
	}
	if len(plan.GapHours) != 0 {
		t.Errorf("expected no gaps, got %d", len(plan.GapHours))
	}
	if len(plan.MySQLRanges) != 1 {
		t.Fatalf("expected 1 MySQL range, got %d", len(plan.MySQLRanges))
	}
}

func TestBuildPlan_allArchivedNoneLive(t *testing.T) {
	archived := []time.Time{h(5, 10), h(5, 11), h(5, 12)}
	plan := buildPlan(nil, archived, h(5, 10), h(5, 13), false)

	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if !plan.SkipMySQL() {
		t.Error("should skip MySQL when all hours are archived")
	}
	if len(plan.GapHours) != 0 {
		t.Errorf("expected no gaps, got %d", len(plan.GapHours))
	}
	if len(plan.MySQLRanges) != 0 {
		t.Errorf("expected no MySQL ranges, got %d", len(plan.MySQLRanges))
	}
}

func TestBuildPlan_mixedLiveAndArchived(t *testing.T) {
	live := []time.Time{h(5, 12), h(5, 13)}
	archived := []time.Time{h(5, 10), h(5, 11)}
	plan := buildPlan(live, archived, h(5, 10), h(5, 14), false)

	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if plan.SkipMySQL() {
		t.Error("should not skip MySQL in mixed scenario")
	}
	if len(plan.GapHours) != 0 {
		t.Errorf("expected no gaps, got %d", len(plan.GapHours))
	}
	if len(plan.MySQLRanges) != 1 {
		t.Fatalf("expected 1 MySQL range, got %d", len(plan.MySQLRanges))
	}
	if !plan.MySQLRanges[0].Start.Equal(h(5, 12)) || !plan.MySQLRanges[0].End.Equal(h(5, 14)) {
		t.Errorf("MySQL range = %v – %v, want 12:00 – 14:00",
			plan.MySQLRanges[0].Start, plan.MySQLRanges[0].End)
	}
}

func TestBuildPlan_withGaps(t *testing.T) {
	live := []time.Time{h(5, 10)}
	archived := []time.Time{h(5, 13)}
	// Range 10-14, live at 10, gap at 11-12, archived at 13
	plan := buildPlan(live, archived, h(5, 10), h(5, 14), false)

	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if plan.SkipMySQL() {
		t.Error("should not skip MySQL when there are gaps")
	}
	if len(plan.GapHours) != 2 {
		t.Fatalf("expected 2 gap hours, got %d: %v", len(plan.GapHours), plan.GapHours)
	}
	if !plan.GapHours[0].Equal(h(5, 11)) || !plan.GapHours[1].Equal(h(5, 12)) {
		t.Errorf("gaps = %v, want [11:00, 12:00]", plan.GapHours)
	}
}

func TestBuildPlan_overlappingLiveAndArchived(t *testing.T) {
	// An hour exists in both live and archive — should not be a gap.
	live := []time.Time{h(5, 10), h(5, 11)}
	archived := []time.Time{h(5, 10), h(5, 11)}
	plan := buildPlan(live, archived, h(5, 10), h(5, 12), false)

	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if len(plan.GapHours) != 0 {
		t.Errorf("expected no gaps for overlapping hours, got %d", len(plan.GapHours))
	}
	// MySQL is still needed because hours are live.
	if plan.SkipMySQL() {
		t.Error("should not skip MySQL when live hours exist")
	}
}

func TestBuildPlan_noArchiveArchivedHoursAreGaps(t *testing.T) {
	// #837: under --no-archive / an active profile the archives are excluded
	// from the fetch, so hours present ONLY in archive_state must be classified
	// as gaps — not silently counted as covered. This is what populates
	// GapHours (→ *GapError under AllowGaps=false, → slog.Warn otherwise) in
	// FetchMerged for reconstruct/recover.
	archived := []time.Time{h(5, 10), h(5, 11), h(5, 12)}

	// Baseline: with archives honored these are covered, no gaps.
	honored := buildPlan(nil, archived, h(5, 10), h(5, 13), false)
	if len(honored.GapHours) != 0 {
		t.Fatalf("noArchive=false: expected archived hours covered, got %d gaps", len(honored.GapHours))
	}

	// noArchive=true: the same archived-only hours become gaps.
	plan := buildPlan(nil, archived, h(5, 10), h(5, 13), true)
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if len(plan.GapHours) != 3 {
		t.Fatalf("noArchive=true: expected 3 gap hours, got %d: %v", len(plan.GapHours), plan.GapHours)
	}
	for i, want := range archived {
		if !plan.GapHours[i].Equal(want) {
			t.Errorf("gap[%d] = %v, want %v", i, plan.GapHours[i], want)
		}
	}
	if plan.SkipMySQL() {
		t.Error("should not skip MySQL when gaps exist")
	}
}

func TestBuildPlan_noArchiveKeepsLiveHours(t *testing.T) {
	// Mixed range: live at 10-11, archived-only at 12-13. Under noArchive the
	// live hours stay live (MySQL is queried) and only the archived-only hours
	// become gaps.
	live := []time.Time{h(5, 10), h(5, 11)}
	archived := []time.Time{h(5, 12), h(5, 13)}
	plan := buildPlan(live, archived, h(5, 10), h(5, 14), true)

	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if len(plan.GapHours) != 2 {
		t.Fatalf("expected 2 gap hours, got %d: %v", len(plan.GapHours), plan.GapHours)
	}
	if !plan.GapHours[0].Equal(h(5, 12)) || !plan.GapHours[1].Equal(h(5, 13)) {
		t.Errorf("gaps = %v, want [12:00, 13:00]", plan.GapHours)
	}
	if len(plan.MySQLRanges) != 1 {
		t.Fatalf("expected 1 MySQL range for live hours, got %d", len(plan.MySQLRanges))
	}
	if !plan.MySQLRanges[0].Start.Equal(h(5, 10)) || !plan.MySQLRanges[0].End.Equal(h(5, 12)) {
		t.Errorf("MySQL range = %v – %v, want 10:00 – 12:00",
			plan.MySQLRanges[0].Start, plan.MySQLRanges[0].End)
	}
}

func TestBuildPlan_allGaps(t *testing.T) {
	// No live, no archived — everything is a gap.
	plan := buildPlan(nil, nil, h(5, 10), h(5, 13), false)

	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if len(plan.GapHours) != 3 {
		t.Fatalf("expected 3 gap hours, got %d", len(plan.GapHours))
	}
	// SkipMySQL should be false because there are gaps.
	if plan.SkipMySQL() {
		t.Error("should not skip MySQL when there are gaps")
	}
}

func TestBuildPlan_noBoundsReturnsNil(t *testing.T) {
	plan := buildPlan(nil, nil, time.Time{}, time.Time{}, false)
	if plan != nil {
		t.Errorf("expected nil for zero-value bounds, got %+v", plan)
	}
}

// ─── archive content coverage (#1037) ───────────────────────────────────────

func TestExpandArchiveHours_contentRangeWidensCoverage(t *testing.T) {
	label := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
	// Backfilled partition: content starts 3 hours before the label hour.
	cov := []archiveCoverage{{
		Label: label,
		Min:   label.Add(-3 * time.Hour).Add(10 * time.Minute),
		Max:   label.Add(45 * time.Minute),
	}}
	got := expandArchiveHours(cov)
	want := []time.Time{
		label.Add(-3 * time.Hour),
		label.Add(-2 * time.Hour),
		label.Add(-time.Hour),
		label,
	}
	if len(got) != len(want) {
		t.Fatalf("expandArchiveHours = %v, want %v", got, want)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Errorf("hour[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestExpandArchiveHours_labelOnlyFallbacks(t *testing.T) {
	label := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		cov  archiveCoverage
	}{
		{"zero min/max (pre-#1037 row)", archiveCoverage{Label: label}},
		{"inverted range", archiveCoverage{Label: label, Min: label.Add(time.Hour), Max: label}},
		{"implausibly wide range", archiveCoverage{
			Label: label,
			Min:   label.Add(-maxCoverageSpan - time.Hour),
			Max:   label,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := expandArchiveHours([]archiveCoverage{tc.cov})
			if len(got) != 1 || !got[0].Equal(label) {
				t.Errorf("expandArchiveHours = %v, want just the label hour %v", got, label)
			}
		})
	}
}

func TestExpandArchiveHours_dedupesAcrossRows(t *testing.T) {
	l1 := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
	l2 := l1.Add(time.Hour)
	cov := []archiveCoverage{
		{Label: l1, Min: l1, Max: l2.Add(30 * time.Minute)}, // spills into l2
		{Label: l2, Min: l2, Max: l2.Add(59 * time.Minute)},
	}
	got := expandArchiveHours(cov)
	if len(got) != 2 || !got[0].Equal(l1) || !got[1].Equal(l2) {
		t.Errorf("expandArchiveHours = %v, want [%v %v]", got, l1, l2)
	}
}

func TestMisfiledHours(t *testing.T) {
	label := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
	// Misfiled: content spans two days before the label (the #1037 backfill).
	misfiled := archiveCoverage{
		Label: label,
		Min:   label.Add(-48 * time.Hour),
		Max:   label.Add(30 * time.Minute),
	}
	// Well-filed: content inside its own label hour.
	wellFiled := archiveCoverage{
		Label: label.Add(time.Hour),
		Min:   label.Add(time.Hour),
		Max:   label.Add(time.Hour + 59*time.Minute),
	}
	// Unknown content (pre-#1037 row): never reported misfiled.
	unknown := archiveCoverage{Label: label.Add(2 * time.Hour)}
	cov := []archiveCoverage{misfiled, wellFiled, unknown}

	t.Run("window over backfilled hours returns the misfiled label", func(t *testing.T) {
		start := label.Add(-40 * time.Hour)
		end := label.Add(-39 * time.Hour)
		got := misfiledHours(cov, start, end)
		if len(got) != 1 || !got[0].Equal(label) {
			t.Fatalf("misfiledHours = %v, want [%v]", got, label)
		}
	})

	t.Run("window not overlapping content returns nothing", func(t *testing.T) {
		start := label.Add(-80 * time.Hour)
		end := label.Add(-72 * time.Hour)
		if got := misfiledHours(cov, start, end); len(got) != 0 {
			t.Fatalf("misfiledHours = %v, want empty", got)
		}
	})

	t.Run("open start bound", func(t *testing.T) {
		got := misfiledHours(cov, time.Time{}, label.Add(-39*time.Hour))
		if len(got) != 1 || !got[0].Equal(label) {
			t.Fatalf("misfiledHours = %v, want [%v]", got, label)
		}
	})

	t.Run("open end bound", func(t *testing.T) {
		got := misfiledHours(cov, label.Add(-40*time.Hour), time.Time{})
		if len(got) != 1 || !got[0].Equal(label) {
			t.Fatalf("misfiledHours = %v, want [%v]", got, label)
		}
	})

	t.Run("well-filed archives never reported", func(t *testing.T) {
		// Window covering exactly the well-filed hour — past the misfiled
		// archive's content max, so only wellFiled/unknown are candidates,
		// and neither may be reported.
		start := wellFiled.Min
		end := wellFiled.Min.Add(time.Hour)
		if got := misfiledHours(cov, start, end); len(got) != 0 {
			t.Fatalf("misfiledHours = %v, want empty (content within label hour)", got)
		}
	})
}

// The POSITIVE direction of ArchivesBelowLive, which the short-circuit tests
// cannot cover: every one of those hands the predicate a plan literal with the
// flag it chose, so hardwiring buildPlan to `false` left the entire repo green
// while quietly disabling the optimization the whole feature exists for. A
// silently absent speed-up is a worse failure than a noisy one — nothing tells
// the operator the 0.3s path became 27.6s.
//
// The interleaved case is here for a different mutation: swapping the
// reference point from the OLDEST live hour to the newest also survived
// everything else. Newest would be unsound exactly on the layout the feature
// refuses — an archived-but-not-dropped hour whose live partition was later
// truncated — so the doc's "oldest live partition hour" needs a test that
// distinguishes them, and only an archive sitting BETWEEN live hours does.
func TestBuildPlanComputesArchivesBelowLive(t *testing.T) {
	var live []time.Time
	for i := 10; i <= 17; i++ {
		live = append(live, h(5, i))
	}
	tests := []struct {
		name     string
		archived []time.Time
		want     bool
		why      string
	}{
		{"strictly below live", []time.Time{h(5, 8), h(5, 9)}, true,
			"the ordinary rotated index — this is the layout the short-circuit exists for, " +
				"and a flag stuck at false disables it everywhere with no signal"},
		{"no archives at all", nil, true,
			"nothing to skip, so the premise holds vacuously"},
		{"one archived hour above the live range", []time.Time{h(5, 18)}, false,
			"a restored index or a rotate that archived without dropping — the archive can hold " +
				"rows newer than anything live"},
		{"archived hour interleaved between live hours", []time.Time{h(5, 13)}, false,
			"below the NEWEST live hour but not below the oldest; a reference point of newest " +
				"would call this safe and it is not"},
		{"archived hour equal to the oldest live hour", []time.Time{h(5, 10)}, false,
			"strictly below, not at-or-below: the same hour on both sides means the partition " +
				"was archived and not dropped"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan := buildPlan(live, tc.archived, h(5, 8), h(5, 20), false)
			if plan == nil {
				t.Fatal("buildPlan returned nil")
			}
			if plan.ArchivesBelowLive != tc.want {
				t.Errorf("ArchivesBelowLive = %v, want %v — %s", plan.ArchivesBelowLive, tc.want, tc.why)
			}
		})
	}
}

// A range given in another zone finds the same hours as in UTC: the hours it
// is looked up among are UTC, and a time.Time is a map key by its zone too.
// A --since written with an offset read every hour of its range as a gap,
// which made recover refuse a recovery its index covered.
func TestBuildPlan_aRangeInAnotherZone(t *testing.T) {
	live := []time.Time{h(5, 10), h(5, 11), h(5, 12)}
	archived := []time.Time{h(5, 8), h(5, 9)}
	want := buildPlan(live, archived, h(5, 8), h(5, 13), false)
	if len(want.GapHours) != 0 || len(want.MySQLRanges) != 1 {
		t.Fatalf("in UTC: %d gaps, %d MySQL ranges; this test needs a range with none and one", len(want.GapHours), len(want.MySQLRanges))
	}
	for name, loc := range map[string]*time.Location{
		"minus five":         time.FixedZone("", -5*3600),
		"plus five and half": time.FixedZone("", 5*3600+1800),
		"UTC by another name": time.FixedZone("UTC", 0),
	} {
		got := buildPlan(live, archived, h(5, 8).In(loc), h(5, 13).In(loc), false)
		if len(got.GapHours) != 0 || len(got.MySQLRanges) != len(want.MySQLRanges) || got.SkipMySQL() != want.SkipMySQL() {
			t.Errorf("%s: %d gaps (%v), %d MySQL ranges; in UTC none and %d", name, len(got.GapHours), got.GapHours, len(got.MySQLRanges), len(want.MySQLRanges))
		}
	}
}
