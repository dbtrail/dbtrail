package query

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// TestArchivesNotNeeded pins the exported decision the MCP tools consult
// before their own archive loop (#1410). It must answer exactly what fetchPage
// answers, so each proof is driven once, plus the shapes that must decline.
func TestArchivesNotNeeded(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	live := TimeRange{Start: now.Add(-24 * time.Hour), End: now.Add(time.Hour)}
	plan := &QueryPlan{ArchivesBelowLive: true, MySQLRanges: []TimeRange{live}}
	inside := now.Add(-2 * time.Hour)
	atFloor := live.Start
	belowFloor := live.Start.Add(-time.Nanosecond)

	anchorRow := ResultRow{EventID: 7, EventTimestamp: inside}

	tests := []struct {
		name     string
		opts     Options
		rows     []ResultRow
		plan     *QueryPlan
		want     bool
		wantKept int
	}{
		{
			name: "anchor live, no plan needed",
			opts: Options{EventAnchor: &EventCursor{EventID: 7, Timestamp: inside}},
			rows: []ResultRow{anchorRow}, plan: nil, want: true, wantKept: 1,
		},
		{
			name: "since inside live coverage, empty result still skips",
			opts: Options{Since: &inside},
			rows: nil, plan: plan, want: true, wantKept: 0,
		},
		{
			name: "since exactly at the live floor",
			opts: Options{Since: &atFloor},
			rows: nil, plan: plan, want: true, wantKept: 0,
		},
		{
			name: "since one nanosecond below the live floor",
			opts: Options{Since: &belowFloor},
			rows: nil, plan: plan, want: false, wantKept: 0,
		},
		{
			name: "filled newest-first page is cut to the limit",
			opts: Options{Limit: 3, Order: "DESC"},
			rows: rowsAt(4, now.Add(-time.Hour)), plan: plan, want: true, wantKept: 3,
		},
		{
			name: "every named PK has its N live",
			opts: Options{PKValues: "42", LimitPerPK: 2},
			rows: pkRowsAt("42", 2, now.Add(-time.Hour)), plan: plan, want: true, wantKept: 2,
		},
		{
			name: "named PK short of its N: partly live, archives may extend it",
			opts: Options{PKValues: "42", LimitPerPK: 3},
			rows: pkRowsAt("42", 1, now.Add(-time.Hour)), plan: plan, want: false, wantKept: 1,
		},
		{
			name: "named PK absent from the live index",
			opts: Options{PKValues: "42", LimitPerPK: 1},
			rows: nil, plan: plan, want: false, wantKept: 0,
		},
		{
			name: "no plan and no anchor",
			opts: Options{PKValues: "42", LimitPerPK: 1},
			rows: pkRowsAt("42", 1, now.Add(-time.Hour)), plan: nil, want: false, wantKept: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kept, ok := ArchivesNotNeeded(tt.opts, tt.rows, tt.plan)
			if ok != tt.want {
				t.Fatalf("ArchivesNotNeeded ok = %v, want %v", ok, tt.want)
			}
			if len(kept) != tt.wantKept {
				t.Errorf("kept %d rows, want %d", len(kept), tt.wantKept)
			}
		})
	}
}

// TestArchiveSkipNeedsPlan pins the cheap pre-gate: a plan round trip is paid
// only when a plan-consuming proof could fire.
func TestArchiveSkipNeedsPlan(t *testing.T) {
	now := time.Now().UTC()
	pos := BinlogPos{File: "mysql-bin.000001", Pos: 4}
	tests := []struct {
		name string
		opts Options
		rows []ResultRow
		want bool
	}{
		{"since bound", Options{Since: &now}, nil, true},
		{"since with SincePos", Options{Since: &now, SincePos: &pos}, nil, false},
		{"until only, ascending", Options{Until: &now, Limit: 10}, rowsAt(10, now), false},
		{"filled DESC page", Options{Limit: 2, Order: "DESC"}, rowsAt(2, now), true},
		{"short DESC page", Options{Limit: 3, Order: "DESC"}, rowsAt(2, now), false},
		{"filled ASC page", Options{Limit: 2, Order: "ASC"}, rowsAt(2, now), false},
		{"per-PK over a named PK", Options{PKValues: "1", LimitPerPK: 1}, nil, true},
		{"per-PK over named PKs", Options{PKValuesIn: []string{"1", "2"}, LimitPerPK: 1}, nil, true},
		{"per-PK with no named PK", Options{LimitPerPK: 1}, nil, false},
		{"per-PK with an alternate spelling", Options{PKValues: "1", PKValuesAlt: "01", LimitPerPK: 1}, nil, false},
		{"named PK, no per-PK limit", Options{PKValues: "1"}, nil, false},
		{"nothing", Options{}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ArchiveSkipNeedsPlan(tt.opts, tt.rows); got != tt.want {
				t.Errorf("ArchiveSkipNeedsPlan = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestPlanArchiveSkipPicksThePlanner pins the branch PlanArchiveSkip mirrors
// from resolveMergeSources: a bounded window goes to Plan (whose range is
// clamped to the Since hour), an unbounded one to PlanBrowse (whose range
// spans the whole live tier). Both must scope archive_state to the resolved
// source's bintrail_id.
func TestPlanArchiveSkipPicksThePlanner(t *testing.T) {
	liveRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"PARTITION_NAME"}).
			AddRow("p_2026061010").AddRow("p_2026061011").AddRow("p_2026061012")
	}
	covRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"partition_name", "min_event_ts", "max_event_ts"})
	}
	src := []string{"/archives/bintrail_id=m1"}
	floor := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)

	t.Run("bounded window uses Plan", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("information_schema.PARTITIONS").WithArgs("idx").WillReturnRows(liveRows())
		mock.ExpectQuery("FROM archive_state").WithArgs("m1").WillReturnRows(covRows())

		since := time.Date(2026, 6, 10, 11, 30, 0, 0, time.UTC)
		p, err := PlanArchiveSkip(context.Background(), db, "idx", Options{Since: &since}, src)
		if err != nil || p == nil {
			t.Fatalf("PlanArchiveSkip = (%v, %v), want a plan", p, err)
		}
		if len(p.MySQLRanges) != 1 || !p.MySQLRanges[0].Start.Equal(floor.Add(time.Hour)) {
			t.Errorf("bounded plan ranges = %+v, want one range from the Since hour", p.MySQLRanges)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("no bounds uses PlanBrowse", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("information_schema.PARTITIONS").WithArgs("idx").WillReturnRows(liveRows())
		mock.ExpectQuery("FROM archive_state").WithArgs("m1").WillReturnRows(covRows())

		p, err := PlanArchiveSkip(context.Background(), db, "idx", Options{}, src)
		if err != nil || p == nil {
			t.Fatalf("PlanArchiveSkip = (%v, %v), want a plan", p, err)
		}
		if len(p.MySQLRanges) != 1 || !p.MySQLRanges[0].Start.Equal(floor) {
			t.Errorf("browse plan ranges = %+v, want one range from the live floor", p.MySQLRanges)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}
