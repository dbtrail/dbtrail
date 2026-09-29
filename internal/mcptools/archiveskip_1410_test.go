package mcptools

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The #1410 fixtures. Every archive source here is a real directory holding
// one file that is not Parquet, so READING it fails: the query tool then warns
// archive_source_skipped and the recover tool refuses. That makes each test
// discriminate between "the archives were read" and "they were skipped"
// without a hook inside the loop.

// skipFloor is the start of the one live range the fake plans report.
var skipFloor = time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)

// fakePlanner replaces planArchiveSkip for one test and records each call.
type fakePlanner struct {
	plan    *query.QueryPlan
	err     error
	calls   int
	dbNames []string
	sources [][]string
}

func installFakePlanner(t *testing.T, f *fakePlanner) {
	t.Helper()
	orig := planArchiveSkip
	planArchiveSkip = func(_ context.Context, _ *sql.DB, dbName string, _ query.Options, srcs []string) (*query.QueryPlan, error) {
		f.calls++
		f.dbNames = append(f.dbNames, dbName)
		f.sources = append(f.sources, slices.Clone(srcs))
		return f.plan, f.err
	}
	t.Cleanup(func() { planArchiveSkip = orig })
}

// livePlan is a plan whose single live range starts at skipFloor with every
// archive below it: the premise all three plan-consuming proofs need.
func livePlan() *query.QueryPlan {
	return &query.QueryPlan{
		ArchivesBelowLive: true,
		MySQLRanges:       []query.TimeRange{{Start: skipFloor, End: skipFloor.Add(48 * time.Hour)}},
	}
}

// brokenArchive puts a non-Parquet file under base so a read of it fails.
func brokenArchive(t *testing.T, base string) {
	t.Helper()
	dir := filepath.Join(base, "event_date=2026-06-01", "event_hour=12")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "events.parquet"), []byte("not parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// liveInserts mocks the live fetch returning one INSERT per pk, all inside the
// live range.
func liveInserts(mock sqlmock.Sqlmock, pks ...string) {
	rows := sqlmock.NewRows(recoverToolMockCols)
	for i, pk := range pks {
		ts := skipFloor.Add(time.Duration(i+1) * time.Hour)
		rows.AddRow(int64(i+1), "bin.000001", int64(4), int64(40), ts,
			nil, nil, "app", "users", int64(parser.EventInsert), pk,
			nil, nil, []byte(`{"id":`+pk+`}`), int64(0), nil, nil, nil)
	}
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(rows)
}

// skipTarget is a routed target with a database name, so the planner can run.
func skipTarget(db *sql.DB, dbName string) Config {
	return Config{Resolve: func(context.Context, string) (*Target, error) {
		return &Target{DB: db, DBName: dbName, ResolverLoaded: true}, nil
	}}
}

func newSkipMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, mock
}

// The row the client asked about is live, with its latest event: the archives
// cannot change the reversal, so they are not read, and the script says so.
// Before #1410 this read the (broken) archive and refused.
func TestRecoverTool1410_liveSatisfiesSkipsArchives(t *testing.T) {
	db, mock := newSkipMock(t)
	base := mockDiscoverableSource(t, mock)
	brokenArchive(t, base)
	liveInserts(mock, "42")
	fp := &fakePlanner{plan: livePlan()}
	installFakePlanner(t, fp)

	res, _, err := MakeRecoverTool(skipTarget(db, "idx"))(context.Background(), &mcp.CallToolRequest{},
		RecoverArgs{Schema: "app", Table: "users", PK: "42", LimitPerPK: 1})
	if err != nil {
		t.Fatal(err)
	}
	txt := resultText(res)
	if res.IsError {
		t.Fatalf("a reversal the live index answers must not read (and fail on) the archives, got: %s", txt)
	}
	if !strings.Contains(txt, "DELETE FROM") {
		t.Errorf("want the reversal script, got: %s", txt)
	}
	if !strings.Contains(txt, "-- Note: "+recoverArchivesSkippedNote()) {
		t.Errorf("a skip must be recorded in the script, got: %s", txt)
	}
	if fp.calls != 1 || fp.dbNames[0] != "idx" || !slices.Equal(fp.sources[0], []string{base}) {
		t.Errorf("planner calls = %d, dbNames %v, sources %v; want one call for idx over [%s]", fp.calls, fp.dbNames, fp.sources, base)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The envelope path (summary_only, chunks) carries no script text, so the
// record rides its own field there.
func TestRecoverTool1410_skipNoteInEnvelope(t *testing.T) {
	db, mock := newSkipMock(t)
	base := mockDiscoverableSource(t, mock)
	brokenArchive(t, base)
	liveInserts(mock, "42")
	installFakePlanner(t, &fakePlanner{plan: livePlan()})

	res, _, err := MakeRecoverTool(skipTarget(db, "idx"))(context.Background(), &mcp.CallToolRequest{},
		RecoverArgs{Schema: "app", Table: "users", PK: "42", LimitPerPK: 1, SummaryOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("summary must succeed, got: %s", resultText(res))
	}
	var env recoverResult
	if err := json.Unmarshal([]byte(resultText(res)), &env); err != nil {
		t.Fatalf("summary is not the JSON envelope: %v: %s", err, resultText(res))
	}
	if !slices.Contains(env.Notes, recoverArchivesSkippedNote()) {
		t.Errorf("envelope notes = %v, want the archives_skipped record", env.Notes)
	}
	if slices.Contains(env.Warnings, recoverArchivesSkippedNote()) {
		t.Errorf("the skip record is a note, not a warning: %v", env.Warnings)
	}
}

// Recover refusals that must survive the skip: whenever no proof holds, the
// archive is read, and a failed read still refuses.
func TestRecoverTool1410_refusesWhenArchivesAreNeeded(t *testing.T) {
	below := skipFloor.Add(-time.Second).Format("2006-01-02 15:04:05")
	tests := []struct {
		name      string
		args      RecoverArgs
		live      []string
		planner   *fakePlanner
		dbName    string
		since     bool
		wantCalls int
	}{
		{
			name:    "partly live: the row has fewer live events than asked",
			args:    RecoverArgs{Schema: "app", Table: "users", PK: "42", LimitPerPK: 3},
			live:    []string{"42"},
			planner: &fakePlanner{plan: livePlan()}, dbName: "idx", wantCalls: 1,
		},
		{
			name:    "empty: the row is not in the live index",
			args:    RecoverArgs{Schema: "app", Table: "users", PK: "42", LimitPerPK: 1},
			planner: &fakePlanner{plan: livePlan()}, dbName: "idx", wantCalls: 1,
		},
		{
			name:    "since one second below the live floor",
			args:    RecoverArgs{Schema: "app", Table: "users", Since: below},
			planner: &fakePlanner{plan: livePlan()}, dbName: "idx", since: true, wantCalls: 1,
		},
		{
			name:    "no per-row limit and no since: an unbounded reversal needs the archives",
			args:    RecoverArgs{Schema: "app", Table: "users", PK: "42"},
			live:    []string{"42"},
			planner: &fakePlanner{plan: livePlan()}, dbName: "idx", wantCalls: 0,
		},
		{
			name:    "the planner failed",
			args:    RecoverArgs{Schema: "app", Table: "users", PK: "42", LimitPerPK: 1},
			live:    []string{"42"},
			planner: &fakePlanner{err: errors.New("simulated planner failure")}, dbName: "idx", wantCalls: 1,
		},
		{
			name:    "no plan (nothing provable)",
			args:    RecoverArgs{Schema: "app", Table: "users", PK: "42", LimitPerPK: 1},
			live:    []string{"42"},
			planner: &fakePlanner{}, dbName: "idx", wantCalls: 1,
		},
		{
			name:    "the target has no database name",
			args:    RecoverArgs{Schema: "app", Table: "users", PK: "42", LimitPerPK: 1},
			live:    []string{"42"},
			planner: &fakePlanner{plan: livePlan()}, dbName: "", wantCalls: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock := newSkipMock(t)
			base := mockDiscoverableSource(t, mock)
			brokenArchive(t, base)
			if tt.since {
				mock.ExpectQuery("archive_state").WillReturnRows(
					sqlmock.NewRows([]string{"partition_name", "min_event_ts", "max_event_ts"}))
			}
			liveInserts(mock, tt.live...)
			installFakePlanner(t, tt.planner)

			res, _, err := MakeRecoverTool(skipTarget(db, tt.dbName))(context.Background(), &mcp.CallToolRequest{}, tt.args)
			if err != nil {
				t.Fatal(err)
			}
			txt := resultText(res)
			if !res.IsError {
				t.Fatalf("recover must read the archive and refuse on its failure, got a script: %s", txt)
			}
			if !strings.Contains(txt, base) || !strings.Contains(txt, "no_archive") {
				t.Errorf("refusal must name the failed source and no_archive, got: %s", txt)
			}
			if strings.Contains(txt, "--") {
				t.Errorf("an MCP error must carry no flag-shaped token, got: %s", txt)
			}
			if tt.planner.calls != tt.wantCalls {
				t.Errorf("planner calls = %d, want %d", tt.planner.calls, tt.wantCalls)
			}
		})
	}
}

// since exactly at the live floor: every archived row is below the window,
// so the archives are skipped even though the result is empty.
func TestRecoverTool1410_sinceAtLiveFloorSkipsWithEmptyResult(t *testing.T) {
	db, mock := newSkipMock(t)
	base := mockDiscoverableSource(t, mock)
	brokenArchive(t, base)
	mock.ExpectQuery("archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"partition_name", "min_event_ts", "max_event_ts"}))
	liveInserts(mock)
	installFakePlanner(t, &fakePlanner{plan: livePlan()})

	res, _, err := MakeRecoverTool(skipTarget(db, "idx"))(context.Background(), &mcp.CallToolRequest{},
		RecoverArgs{Schema: "app", Table: "users", Since: skipFloor.Format("2006-01-02 15:04:05")})
	if err != nil {
		t.Fatal(err)
	}
	txt := resultText(res)
	if res.IsError {
		t.Fatalf("since at the live floor must skip the archives, got: %s", txt)
	}
	if !strings.Contains(txt, recoverArchivesSkippedNote()) {
		t.Errorf("an empty result must still record the skip, got: %q", txt)
	}
}

// The env pair (BINTRAIL_ARCHIVE_S3 + BINTRAIL_ID) may name an archive with
// no archive_state rows, which would make the proofs' premise vacuous. Such
// sources are always read: the planner is not even asked.
func TestRecoverTool1410_envPairSourcesAreAlwaysRead(t *testing.T) {
	db, mock := newSkipMock(t)
	parent := t.TempDir()
	base := filepath.Join(parent, "bintrail_id=env1")
	brokenArchive(t, base)
	t.Setenv("BINTRAIL_ARCHIVE_S3", parent)
	t.Setenv("BINTRAIL_ID", "env1")
	liveInserts(mock, "42")
	fp := &fakePlanner{plan: livePlan()}
	installFakePlanner(t, fp)

	cfg := Config{Resolve: func(context.Context, string) (*Target, error) {
		return &Target{DB: db, DBName: "idx", ResolverLoaded: true, EnvArchiveDiscovery: true}, nil
	}}
	res, _, err := MakeRecoverTool(cfg)(context.Background(), &mcp.CallToolRequest{},
		RecoverArgs{Schema: "app", Table: "users", PK: "42", LimitPerPK: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(resultText(res), base) {
		t.Fatalf("an env-pair archive must be read (and refuse on failure), got: %s", resultText(res))
	}
	if fp.calls != 0 {
		t.Errorf("planner calls = %d, want 0 for env-pair sources", fp.calls)
	}
}

// Query tool, satisfied: a filled newest-first page from live coverage. The
// archive is not read, so no archive_source_skipped warning, and a Note line
// records the skip.
func TestQueryTool1410_liveSatisfiesSkipsArchives(t *testing.T) {
	db, mock := newSkipMock(t)
	base := mockDiscoverableSource(t, mock)
	brokenArchive(t, base)
	liveInserts(mock, "42")
	fp := &fakePlanner{plan: livePlan()}
	installFakePlanner(t, fp)

	res, _, err := MakeQueryTool(skipTarget(db, "idx"))(context.Background(), &mcp.CallToolRequest{},
		QueryArgs{Schema: "app", Table: "users", Limit: 1, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	txt := resultText(res)
	if res.IsError {
		t.Fatalf("query errored: %s", txt)
	}
	if strings.Contains(txt, "archive_source_skipped") {
		t.Errorf("the archive must not be read, got: %s", txt)
	}
	if !strings.Contains(txt, "\nNote: "+queryArchivesSkippedNote()+"\n") {
		t.Errorf("a skip must be recorded, got: %s", txt)
	}
	if !strings.Contains(txt, `"pk_values": "42"`) && !strings.Contains(txt, `"pk_values":"42"`) {
		t.Errorf("want the live row, got: %s", txt)
	}
	if fp.calls != 1 {
		t.Errorf("planner calls = %d, want 1", fp.calls)
	}
}

// Query tool, not satisfied: the archive is read, its failure is a warning as
// before, and nothing claims a skip. The ascending page never asks the planner.
func TestQueryTool1410_readsArchivesWhenNeeded(t *testing.T) {
	tests := []struct {
		name      string
		args      QueryArgs
		live      []string
		wantCalls int
	}{
		{"partly live: short newest-first page", QueryArgs{Schema: "app", Table: "users", Limit: 5, Order: "DESC"}, []string{"42"}, 0},
		{"empty result", QueryArgs{Schema: "app", Table: "users", PK: "42", LimitPerPK: 1}, nil, 1},
		{"ascending page: no proof can fire, no plan round trip", QueryArgs{Schema: "app", Table: "users", Limit: 1}, []string{"42"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock := newSkipMock(t)
			base := mockDiscoverableSource(t, mock)
			brokenArchive(t, base)
			liveInserts(mock, tt.live...)
			fp := &fakePlanner{plan: livePlan()}
			installFakePlanner(t, fp)

			res, _, err := MakeQueryTool(skipTarget(db, "idx"))(context.Background(), &mcp.CallToolRequest{}, tt.args)
			if err != nil {
				t.Fatal(err)
			}
			txt := resultText(res)
			if res.IsError {
				t.Fatalf("query must degrade, not error: %s", txt)
			}
			if !strings.Contains(txt, "archive_source_skipped") || !strings.Contains(txt, base) {
				t.Errorf("the archive must be read and its failure warned, got: %s", txt)
			}
			if strings.Contains(txt, "archives_skipped") {
				t.Errorf("no skip happened, so none may be claimed, got: %s", txt)
			}
			if fp.calls != tt.wantCalls {
				t.Errorf("planner calls = %d, want %d", fp.calls, tt.wantCalls)
			}
		})
	}
}

// The skip records carry no flag-shaped token and no em dash.
func TestArchivesSkippedNotesWording(t *testing.T) {
	for _, n := range []string{queryArchivesSkippedNote(), recoverArchivesSkippedNote()} {
		if !strings.HasPrefix(n, "archives_skipped: ") {
			t.Errorf("note must start with its stable key, got %q", n)
		}
		// The newest-first proof fires on a full page, next to a truncation
		// warning; the note may only claim the archives could not add rows.
		if !strings.HasSuffix(n, "they could not add rows to this answer") {
			t.Errorf("note must claim only that the archives could not add rows, got %q", n)
		}
		if strings.Contains(n, "nothing is missing") {
			t.Errorf("note must not claim nothing is missing (it can sit next to a truncation warning): %q", n)
		}
		if strings.Contains(n, "--") || strings.Contains(n, "\u2014") {
			t.Errorf("note carries a flag-shaped token or an em dash: %q", n)
		}
	}
}

// A failed misfiled-archive registry scan means the registry could not be
// read, and no skip may rest on it: the query tool reads the archives (and
// warns as before), without asking the planner.
func TestQueryTool1410_misfiledScanFailureNeverSkips(t *testing.T) {
	db, mock := newSkipMock(t)
	base := mockDiscoverableSource(t, mock)
	brokenArchive(t, base)
	mock.ExpectQuery("archive_state").WillReturnError(errors.New("simulated registry failure"))
	liveInserts(mock, "42")
	fp := &fakePlanner{plan: livePlan()}
	installFakePlanner(t, fp)

	res, _, err := MakeQueryTool(skipTarget(db, "idx"))(context.Background(), &mcp.CallToolRequest{},
		QueryArgs{Schema: "app", Table: "users", Since: skipFloor.Format("2006-01-02 15:04:05"), Limit: 1, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	txt := resultText(res)
	if !strings.Contains(txt, "archive_scan_incomplete") || !strings.Contains(txt, "archive_source_skipped") {
		t.Errorf("want the scan warning and the read-failure warning, got: %s", txt)
	}
	if strings.Contains(txt, "archives_skipped") || fp.calls != 0 {
		t.Errorf("no skip may rest on an unreadable registry (planner calls %d): %s", fp.calls, txt)
	}
}

// A half-set env pair makes discovery unreliable (archive_discovery_failed).
// The query tool must not then claim a skip whose premise it could not check:
// the two lines would contradict each other.
func TestQueryTool1410_discoveryFailureNeverSkips(t *testing.T) {
	t.Setenv("BINTRAIL_ARCHIVE_S3", t.TempDir())
	t.Setenv("BINTRAIL_ID", "")
	db, mock := newSkipMock(t)
	base := mockDiscoverableSource(t, mock)
	brokenArchive(t, base)
	liveInserts(mock, "42")
	fp := &fakePlanner{plan: livePlan()}
	installFakePlanner(t, fp)

	cfg := Config{Resolve: func(context.Context, string) (*Target, error) {
		return &Target{DB: db, DBName: "idx", ResolverLoaded: true, EnvArchiveDiscovery: true}, nil
	}}
	res, _, err := MakeQueryTool(cfg)(context.Background(), &mcp.CallToolRequest{},
		QueryArgs{Schema: "app", Table: "users", Limit: 1, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	txt := resultText(res)
	if !strings.Contains(txt, "archive_discovery_failed") {
		t.Fatalf("want the discovery warning, got: %s", txt)
	}
	if strings.Contains(txt, "archives_skipped") || fp.calls != 0 {
		t.Errorf("no skip may be claimed after a discovery failure (planner calls %d): %s", fp.calls, txt)
	}
}
