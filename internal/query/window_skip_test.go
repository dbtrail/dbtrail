package query

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// The fourth short-circuit (#1414): a Since bound inside contiguous live
// coverage makes every archive source redundant, whatever the page looks
// like. It exists because the sharpest measured shape — a sparse table
// reached from a live-retention widget — can never fill a page, so
// topNSatisfiedLive structurally cannot help it.
func TestWindowSatisfiedLive(t *testing.T) {
	now := time.Now().UTC()
	live := TimeRange{Start: now.Add(-24 * time.Hour), End: now.Add(time.Hour)}
	inside := now.Add(-12 * time.Hour)
	atStart := live.Start
	below := now.Add(-48 * time.Hour)

	tests := []struct {
		name string
		opts Options
		plan *QueryPlan
		want bool
		why  string
	}{
		{
			name: "since inside one live range",
			opts: Options{Since: &inside},
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: true,
			why:  "every label-accurate archived row sits below the window's floor",
		},
		{
			name: "since exactly at the range start",
			opts: Options{Since: &atStart},
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: true,
			why:  "archived hours are strictly below the oldest live hour, so the boundary is safe",
		},
		{
			name: "SincePos drops the exact Since filter — decline",
			opts: func() Options {
				pos := BinlogPos{File: "mysql-bin.000007", Pos: 4711}
				return Options{Since: &inside, SincePos: &pos}
			}(),
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why: "with SincePos the fetch reads an hour BELOW Since on both legs and relies on " +
				"the position comparison — a window this proof never modeled; verify's false " +
				"mismatch is the failure mode",
		},
		{
			name: "no since bound",
			opts: Options{},
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "an unbounded window reaches back to where the archives live",
		},
		{
			name: "since below the live range",
			opts: Options{Since: &below},
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "the window reaches under live coverage — that is what the archives hold",
		},
		{
			name: "archives not below live",
			opts: Options{Since: &inside},
			plan: &QueryPlan{ArchivesBelowLive: false, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "an archived hour above the oldest live hour can hold rows inside the window",
		},
		{
			name: "archive coverage could not be read",
			opts: Options{Since: &inside},
			plan: &QueryPlan{ArchivesBelowLive: true, ArchiveCoverageUnavailable: true, MySQLRanges: []TimeRange{live}},
			want: false,
			why:  "a vacuous premise over an unread archive_state is not a premise",
		},
		{
			name: "misfiled archive overlaps the window",
			opts: Options{Since: &inside},
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live},
				MisfiledArchiveHours: []time.Time{now.Add(-30 * time.Hour)}},
			want: false,
			why:  "a #1037 backfill puts in-window rows into an archive labeled below it",
		},
		{
			name: "two live ranges",
			opts: Options{Since: &inside},
			plan: &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{
				{Start: now.Add(-24 * time.Hour), End: now.Add(-20 * time.Hour)}, live}},
			want: false,
			why:  "the sibling predicates' single-range premise, kept for the same reason they keep it",
		},
		{
			name: "no plan",
			opts: Options{Since: &inside},
			plan: nil,
			want: false,
			why:  "no plan, no proof",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := windowSatisfiedLive(tt.opts, tt.plan); got != tt.want {
				t.Errorf("windowSatisfiedLive = %v, want %v — %s", got, tt.want, tt.why)
			}
		})
	}
}

// The wiring, not just the predicate: a Since-bounded window inside live
// coverage must leave the archive fetcher UNCALLED and report the elision.
// Deleting the skip from fetchPage has to fail here. The page is deliberately
// SPARSE — fewer rows than the limit — because that is the shape (#1414) no
// other short-circuit can take: topNSatisfiedLive requires a filled page.
func TestFetchPageSkipsArchivesOnLiveCoveredWindow(t *testing.T) {
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
	// Six events against a 100-row limit: the sparse widget-clicked table.
	for i := range 6 {
		live.AddRow(uint64(6-i), "mysql-bin.000001", 100, 200, now.Add(-time.Duration(i)*time.Minute),
			nil, nil, "wordpress", "dbt_actionscheduler_logs", 2, "1",
			nil, nil, nil, 1, nil, nil,
			nil)
	}
	expectNoEventPartitions(mock)
	mock.ExpectQuery("SELECT").WillReturnRows(live)

	since := now.Add(-12 * time.Hour)
	var archiveCalls int
	src := mergeSources{
		archSources: []string{"s3://bucket/bintrail_id=x"},
		plan: &QueryPlan{ArchivesBelowLive: true,
			MySQLRanges: []TimeRange{{Start: now.Add(-24 * time.Hour), End: now.Add(time.Hour)}}},
	}
	o := FetchMergedOptions{
		Opts:      Options{Limit: 100, Order: "DESC", Since: &since},
		AllowGaps: true,
		ArchiveFetcher: func(context.Context, Options, string) ([]ResultRow, error) {
			archiveCalls++
			return nil, nil
		},
	}

	rows, _, _, _, elided, err := fetchPage(context.Background(), New(db), o, src)
	if err != nil {
		t.Fatal(err)
	}
	if archiveCalls != 0 {
		t.Errorf("archive fetcher called %d time(s); the window is live-covered, so the archives "+
			"could only contribute rows the Since filter discards", archiveCalls)
	}
	if len(rows) != 6 {
		t.Errorf("rows = %d, want 6", len(rows))
	}
	if !elided {
		t.Errorf("archivesElided is false after skipping the archive sources — the surface " +
			"rendering this has no way to say the archives went unread")
	}
}
