package query

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// pkRowsAt builds n rows for one pk_values, newest-first, a minute apart,
// with the oldest at `oldest`.
func pkRowsAt(pk string, n int, oldest time.Time) []ResultRow {
	out := make([]ResultRow, n)
	for i := range out {
		out[i] = ResultRow{
			EventID:        uint64(n - i),
			PKValues:       pk,
			EventTimestamp: oldest.Add(time.Duration(n-1-i) * time.Minute),
		}
	}
	return out
}

func TestPerPKSatisfiedLive(t *testing.T) {
	now := time.Now().UTC()
	live := hourRange(24*time.Hour, 0, now)
	inside := now.Add(-2 * time.Hour)
	below := now.Add(-30 * time.Hour) // older than the live range's start

	tests := []struct {
		name string
		opts Options
		rows []ResultRow
		plan *QueryPlan
		want bool
		why  string
	}{
		{
			name: "one named PK with its latest N already live",
			opts: Options{LimitPerPK: 1, PKValues: "42"},
			rows: pkRowsAt("42", 1, inside),
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: true,
			why:  "the trim keeps this row and discards every older archived one",
		},
		{
			// #2156: the latest N in binary log order can be an archived
			// change whose statement started before the live floor.
			name: "the latest N in binary log order (LatestPerPKCandidates)",
			opts: Options{LimitPerPK: 1, PKValues: "42", LatestPerPKCandidates: true},
			rows: pkRowsAt("42", 1, inside),
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "an archived change can be later in the binary log than every live one",
		},
		{
			// #2007: a system-versioned lookup adds spellings (key plus
			// each current marker). The trim is per stored spelling, and
			// an added spelling with no live row (the other server
			// version's marker, or the plain key from before versioning)
			// can still hold archived rows the full answer keeps.
			name: "spellings added by a system-versioned lookup",
			opts: Options{LimitPerPK: 1, PKValuesIn: []string{"2|m"},
				PKAliases: map[string]string{"2|m": "2", "2|n": "2"}},
			rows: pkRowsAt("2|m", 1, inside),
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "an added spelling's archived rows survive the per-spelling trim",
		},
		{
			name: "several named PKs, all satisfied",
			opts: Options{LimitPerPK: 2, PKValuesIn: []string{"1", "2"}},
			rows: append(pkRowsAt("1", 2, inside), pkRowsAt("2", 2, inside)...),
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: true,
		},
		{
			name: "one PK short of its N",
			opts: Options{LimitPerPK: 2, PKValuesIn: []string{"1", "2"}},
			rows: append(pkRowsAt("1", 2, inside), pkRowsAt("2", 1, inside)...),
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "the archives can still extend PK 2 — one short PK invalidates the whole skip",
		},
		{
			name: "a named PK absent from the live result",
			opts: Options{LimitPerPK: 1, PKValuesIn: []string{"1", "2"}},
			rows: pkRowsAt("1", 1, inside),
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "PK 2's entire history may be archived; skipping would drop the row, not trim it",
		},
		{
			name: "the empty PK name",
			opts: Options{LimitPerPK: 1, PKValuesIn: []string{""}},
			rows: pkRowsAt("", 1, inside),
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "merge.go buckets every empty-PK row under its own synthetic key, so the trim discards nothing there and the proof does not hold — see the #318 drift carve-out in LimitPerPK",
		},
		{
			name: "an alternate PK encoding is in play",
			opts: Options{LimitPerPK: 1, PKValues: "42", PKValuesAlt: "0x2A"},
			rows: pkRowsAt("42", 1, inside),
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "the same logical row can be stored under either spelling, and the trim partitions by the stored one — so its N is not well defined",
		},
		{
			name: "a different PK than the one named",
			opts: Options{LimitPerPK: 1, PKValues: "42"},
			rows: pkRowsAt("99", 3, inside),
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "PK 42 is absent; counting distinct keys instead of walking the named ones would let 99 stand in for it",
		},
		{
			name: "no PK named",
			opts: Options{LimitPerPK: 1},
			rows: pkRowsAt("42", 1, inside),
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "this is the unsound case: an archive-only pk_values is a legitimate result row, and nothing here names the set that would have to be complete",
		},
		{
			name: "no per-PK trim",
			opts: Options{PKValues: "42"},
			rows: pkRowsAt("42", 5, inside),
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "without the trim the archives are not discarded, they are the older half of the answer",
		},
		{
			name: "oldest kept row sits below the live range",
			opts: Options{LimitPerPK: 2, PKValues: "42"},
			rows: pkRowsAt("42", 2, below),
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "the span above the kept rows is not provably live-covered",
		},
		{
			name: "two live ranges",
			opts: Options{LimitPerPK: 1, PKValues: "42"},
			rows: pkRowsAt("42", 1, inside),
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live, hourRange(72*time.Hour, 48*time.Hour, now)}},
			want: false,
			why:  "two live ranges mean the planner saw a hole in the live hours; what fills it is not\n\t\t\t\t\t\tknowable from here, so no live result proves anything about the span above it",
		},
		{
			// Plan sets this when the archive_state read fails for a reason
			// other than a missing table, and then hands buildPlan an EMPTY
			// hour list — over which archivesBelowLive is vacuously true. The
			// plan would say "coverage was never evaluated" and "every
			// archived hour is below live" at the same time. Sources come
			// from a SEPARATE query, so they can still be resolved and
			// waiting to be skipped.
			name: "archive coverage could not be read",
			opts: Options{LimitPerPK: 1, PKValues: "42"},
			rows: pkRowsAt("42", 1, inside),
			plan: &QueryPlan{ArchivesBelowLive: true, ArchiveCoverageUnavailable: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "a vacuous premise over an unread archive_state is not a premise",
		},
		{
			name: "no plan",
			opts: Options{LimitPerPK: 1, PKValues: "42"},
			rows: pkRowsAt("42", 1, inside),
			plan: nil,
			want: false,
			why:  "no plan, no proof",
		},
		{
			name: "no live rows at all",
			opts: Options{LimitPerPK: 1, PKValues: "42"},
			rows: nil,
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "the whole history may be archived",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := perPKSatisfiedLive(tc.opts, tc.rows, tc.plan); got != tc.want {
				t.Errorf("perPKSatisfiedLive = %v, want %v — %s", got, tc.want, tc.why)
			}
		})
	}
}

// The predicate above is pure; this drives the real fetchPage, because a
// predicate that is never consulted passes every test it has. Mirrors
// TestFetchPageSkipsArchivesOnFilledTopN for the per-PK proof.
func TestFetchPageSkipsArchivesWhenEveryNamedPKIsSatisfiedLive(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	live := sqlmock.NewRows([]string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	})
	live.AddRow(uint64(7), "mysql-bin.000001", 100, 200, now.Add(-2*time.Hour),
		nil, nil, "shop", "orders", 3, "93921",
		nil, nil, nil, 1, nil, nil, nil)
	mock.ExpectQuery("SELECT").WillReturnRows(live)

	var archiveCalls int
	src := mergeSources{
		archSources: []string{"s3://bucket/bintrail_id=x"},
		plan:        &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{{Start: now.Add(-24 * time.Hour), End: now.Add(time.Hour)}}},
	}
	o := FetchMergedOptions{
		// The shape the console sends for a scoped reversal: one PK, latest
		// one per row, no lower bound.
		Opts:      Options{LimitPerPK: 1, PKValues: "93921", Limit: 1000, Order: "DESC"},
		AllowGaps: true,
		ArchiveFetcher: func(context.Context, Options, string) ([]ResultRow, error) {
			archiveCalls++
			return []ResultRow{{EventID: 999, PKValues: "93921", EventTimestamp: now.Add(-72 * time.Hour)}}, nil
		},
	}

	rows, _, _, _, elided, err := fetchPage(context.Background(), New(db), o, src)
	if err != nil {
		t.Fatalf("fetchPage: %v", err)
	}
	if archiveCalls != 0 {
		t.Errorf("archive fetcher called %d time(s); the named PK already had its latest event live, "+
			"so the archives could only contribute rows the per-PK trim discards", archiveCalls)
	}
	// Reported, not just skipped: a reversal that silently dropped registered
	// archives is the audit failure archivesElided exists to prevent (#1353).
	if !elided {
		t.Error("archivesElided is false after skipping the archive sources — the surface rendering " +
			"this has no way to say the archives went unread")
	}
	if len(rows) != 1 || rows[0].EventID != 7 {
		t.Errorf("rows = %+v, want just the live event", rows)
	}
}

// The other direction, and the one that must never be traded for speed: a
// named PK with nothing live has its whole history in the archives, so the
// skip is refused and the fetcher runs.
func TestFetchPageReadsArchivesWhenANamedPKHasNothingLive(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	empty := sqlmock.NewRows([]string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	})
	mock.ExpectQuery("SELECT").WillReturnRows(empty)

	var archiveCalls int
	src := mergeSources{
		archSources: []string{"s3://bucket/bintrail_id=x"},
		plan:        &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{{Start: now.Add(-24 * time.Hour), End: now.Add(time.Hour)}}},
	}
	o := FetchMergedOptions{
		Opts:      Options{LimitPerPK: 1, PKValues: "93921", Limit: 1000, Order: "DESC"},
		AllowGaps: true,
		ArchiveFetcher: func(context.Context, Options, string) ([]ResultRow, error) {
			archiveCalls++
			return []ResultRow{{EventID: 999, PKValues: "93921", EventTimestamp: now.Add(-72 * time.Hour)}}, nil
		},
	}

	rows, _, _, _, elided, err := fetchPage(context.Background(), New(db), o, src)
	if err != nil {
		t.Fatalf("fetchPage: %v", err)
	}
	if archiveCalls != 1 {
		t.Errorf("archive fetcher called %d time(s), want 1 — the row's entire history is archived", archiveCalls)
	}
	if elided {
		t.Error("archivesElided is true on a path that read the archives")
	}
	if len(rows) != 1 || rows[0].EventID != 999 {
		t.Errorf("rows = %+v, want the archived event", rows)
	}
}

// The premise behind the empty-name refusal, asserted against the real trim
// rather than taken from its comment. If LimitPerPK ever stops carving empty
// PKs out, the refusal becomes needlessly conservative and this says so.
func TestLimitPerPKDoesNotTrimEmptyPKs(t *testing.T) {
	now := time.Now().UTC()
	rows := []ResultRow{
		{EventID: 1, PKValues: "", EventTimestamp: now.Add(-72 * time.Hour)},
		{EventID: 2, PKValues: "", EventTimestamp: now.Add(-71 * time.Hour)},
		{EventID: 3, PKValues: "", EventTimestamp: now.Add(-2 * time.Hour)},
	}
	if got := LimitPerPK(rows, 1); len(got) != 3 {
		t.Fatalf("LimitPerPK(empty PKs, 1) kept %d rows, want all 3 — the drift carve-out is what "+
			"makes perPKSatisfiedLive refuse the empty name; if it is gone, revisit that refusal", len(got))
	}
	// And the contrast, so the test is about the carve-out and not about
	// LimitPerPK being a no-op.
	named := []ResultRow{
		{EventID: 1, PKValues: "42", EventTimestamp: now.Add(-72 * time.Hour)},
		{EventID: 2, PKValues: "42", EventTimestamp: now.Add(-2 * time.Hour)},
	}
	if got := LimitPerPK(named, 1); len(got) != 1 {
		t.Fatalf("LimitPerPK(named PK, 1) kept %d rows, want 1", len(got))
	}
}

// The negative control #1403 asked for by name — "an index whose archived hour
// sits above the live range" — and the one the first version of this file
// substituted a different layout for, because the predicate as written could
// not pass it.
//
// This is the layout that makes the short-circuit dangerous rather than merely
// wrong: live partitions 10:00–17:00, an archived hour at 18:00 ABOVE them
// (a restored index, or a rotate that archived without dropping), and the
// archive holding an event NEWER than anything live. The named PK has its one
// live row, so a per-PK count alone says "satisfied" — and the reversal would
// be built from the 15:00 event while the 18:05 one that limit_per_pk=1 should
// have kept is never read. Short reversal script, reported complete.
//
// The plan comes from the REAL buildPlan rather than a literal. That is the
// point: every other case here hands the predicate an ArchivesBelowLive it
// chose, so without this one the field could be computed wrongly — or not at
// all — and the suite would not notice.
func TestFetchPageReadsArchivesWhenAnArchivedHourSitsAboveLive(t *testing.T) {
	base := time.Date(2026, 3, 4, 0, 0, 0, 0, time.UTC)
	hr := func(h int) time.Time { return base.Add(time.Duration(h) * time.Hour) }

	var liveHours []time.Time
	for h := 10; h <= 17; h++ {
		liveHours = append(liveHours, hr(h))
	}
	plan := buildPlan(liveHours, []time.Time{hr(18)}, hr(10), hr(20), false)
	if plan == nil {
		t.Fatal("buildPlan returned nil for a live+archived layout")
	}
	if len(plan.MySQLRanges) != 1 {
		t.Fatalf("MySQLRanges = %v, want one contiguous range — the fixture is meant to have no "+
			"interior hole, so that condition cannot be what refuses the skip", plan.MySQLRanges)
	}
	if plan.ArchivesBelowLive {
		t.Fatal("buildPlan says ArchivesBelowLive with an archived hour at 18:00 above a live " +
			"range topping out at 17:00 — the flag is not being computed from the hours")
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	live := sqlmock.NewRows([]string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	})
	live.AddRow(uint64(1), "mysql-bin.000001", 100, 200, hr(15),
		nil, nil, "shop", "orders", 3, "A",
		nil, nil, nil, 1, nil, nil, nil)
	mock.ExpectQuery("SELECT").WillReturnRows(live)

	var archiveCalls int
	until := hr(20)
	o := FetchMergedOptions{
		Opts:      Options{LimitPerPK: 1, PKValues: "A", Limit: 1000, Order: "DESC", Until: &until},
		AllowGaps: true,
		ArchiveFetcher: func(context.Context, Options, string) ([]ResultRow, error) {
			archiveCalls++
			// NEWER than the live row, which is the whole hazard.
			return []ResultRow{{EventID: 99, PKValues: "A", EventTimestamp: hr(18).Add(5 * time.Minute)}}, nil
		},
	}
	rows, _, _, _, elided, err := fetchPage(context.Background(), New(db), o,
		mergeSources{archSources: []string{"s3://bucket/bintrail_id=x"}, plan: plan})
	if err != nil {
		t.Fatalf("fetchPage: %v", err)
	}
	if archiveCalls != 1 {
		t.Fatalf("archive fetcher called %d time(s); with an archived hour above the live range the "+
			"skip is unsound — the archive holds the newest event for this PK", archiveCalls)
	}
	if elided {
		t.Error("archivesElided is true on a page that DID read the archives; the console renders " +
			"that as \"nothing is missing here\", which would be a false assurance on a short reversal")
	}
	// The trim keeps the newest per PK: the archived event, not the live one.
	if len(rows) != 1 || rows[0].EventID != 99 {
		t.Errorf("rows = %+v, want the archived event 99 — it is newer than the live one and "+
			"limit_per_pk=1 keeps the newest", rows)
	}
}
