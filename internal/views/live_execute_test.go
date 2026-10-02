package views

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// The fixture archive holds ONE event, id 1, at this instant. The hot table
// below holds that same event (the archived-but-not-yet-dropped overlap) plus
// one the archives do not have yet.
const (
	coldEpoch = 1777604400 // 2026-05-01 03:00:00Z, what writeFixtureArchive writes
	hotEpoch  = 1777626900 // 2026-05-01 09:15:00Z
)

// liveStandIn creates the catalog the generated ATTACH would create, with the
// column TYPES DuckDB's mysql extension produces for the index's own DDL
// (docs/migrations/001_create_tables.sql), so the union this test executes is the
// one an operator's DuckDB builds:
//
//	event_id        BIGINT UNSIGNED  -> UBIGINT
//	start/end_pos   BIGINT UNSIGNED  -> UBIGINT
//	event_timestamp DATETIME         -> TIMESTAMP   (NAIVE — this is finding A)
//	connection_id   INT UNSIGNED     -> UINTEGER
//	event_type      TINYINT UNSIGNED -> UTINYINT
//	commit_ts_us    BIGINT UNSIGNED  -> UBIGINT
//	JSON/TEXT                        -> VARCHAR
//
// pk_hash is present here on purpose: it is a real column of the live table and
// the archives do not carry it, so a leg that selected it would not line up.
func liveStandIn(t *testing.T, db *sql.DB, omit ...string) {
	t.Helper()
	liveStandInAs(t, db, liveAttachAlias, omit...)
}

// liveStandInAs is liveStandIn under the alias a file generated with a schema
// attaches its index as, so two servers' stand-ins can sit in one session.
func liveStandInAs(t *testing.T, db *sql.DB, alias string, omit ...string) {
	t.Helper()
	cols := []struct{ name, typ string }{
		{"event_id", "UBIGINT"}, {"binlog_file", "VARCHAR"}, {"start_pos", "UBIGINT"},
		{"end_pos", "UBIGINT"}, {"event_timestamp", "TIMESTAMP"}, {"gtid", "VARCHAR"},
		{"connection_id", "UINTEGER"}, {"schema_name", "VARCHAR"}, {"table_name", "VARCHAR"},
		{"event_type", "UTINYINT"}, {"pk_values", "VARCHAR"}, {"pk_hash", "VARCHAR"},
		{"changed_columns", "VARCHAR"}, {"row_before", "VARCHAR"}, {"row_after", "VARCHAR"},
		{"schema_version", "INTEGER"}, {"query_text", "VARCHAR"}, {"query_hash", "VARCHAR"},
		{"commit_ts_us", "UBIGINT"},
	}
	var defs, names []string
	for _, c := range cols {
		if slicesContains(omit, c.name) {
			continue
		}
		defs = append(defs, `"`+c.name+`" `+c.typ)
		names = append(names, `"`+c.name+`"`)
	}
	if _, err := db.Exec(`ATTACH ':memory:' AS ` + quoteIdent(alias)); err != nil {
		t.Fatalf("attach stand-in catalog: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE ` + quoteIdent(alias) + `."binlog_events" (` + strings.Join(defs, ", ") + `)`); err != nil {
		t.Fatalf("create stand-in table: %v", err)
	}

	// Row 1 is the archived event's twin: same event_id, and the index copy
	// knows no bintrail_id. Row 2 is only in the index. event_type 200 is a
	// code no build defines, kept here because the decoded CASE promises such a
	// code shows up as an unfamiliar VALUE rather than failing the query.
	rows := [][]any{
		{1, "binlog.000001", 100, 200, "2026-05-01 03:00:00", nil, nil, "shop", "orders",
			2, "1", "hash1", `["status"]`, `{"id":1}`, `{"id":1}`, 1, nil, nil, 1777000000000000},
		{2, "binlog.000001", 300, 400, "2026-05-01 09:15:00", nil, nil, "shop", "orders",
			200, "2", "hash2", nil, nil, `{"id":2}`, 1, nil, nil, nil},
	}
	for _, r := range rows {
		var vals []any
		for i, c := range cols {
			if slicesContains(omit, c.name) {
				continue
			}
			vals = append(vals, r[i])
		}
		ph := strings.TrimSuffix(strings.Repeat("?,", len(vals)), ",")
		stmt := `INSERT INTO ` + quoteIdent(alias) + `."binlog_events" (` + strings.Join(names, ",") + `) VALUES (` + ph + `)`
		if _, err := db.Exec(stmt, vals...); err != nil {
			t.Fatalf("insert stand-in row: %v", err)
		}
	}
}

func slicesContains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// viewsHalf strips the preamble. INSTALL mysql / CREATE SECRET / ATTACH cannot
// run here — there is no MySQL and no extension download — so the catalog is
// stood up by liveStandIn and this executes the half that describes the data.
func viewsHalf(t *testing.T, out string) string {
	t.Helper()
	// The live preamble is CUT OUT rather than skipped past. Since #1536 it no
	// longer sits at the top of the file: the Parquet-only views are emitted
	// first and the ATTACH comes between them and the two-leg events view. A
	// suffix from the first "-- events:" would therefore carry the ATTACH into
	// the session and try to dial a real MySQL, which is exactly what
	// liveStandIn exists to avoid.
	out = stripLivePreamble(out)
	i := strings.Index(out, "-- events:")
	if i < 0 {
		t.Fatalf("no events view in the generated file:\n%s", out)
	}
	return out[i:]
}

// stripLivePreamble removes the INSTALL/SECRET/ATTACH block, wherever in the
// file it sits, leaving every view definition around it intact.
func stripLivePreamble(out string) string {
	start := strings.Index(out, "-- Live index setup")
	if start < 0 {
		return out
	}
	att := strings.Index(out[start:], "\nATTACH ")
	if att < 0 {
		return out
	}
	end := strings.Index(out[start+att+1:], "\n")
	if end < 0 {
		return out
	}
	return out[:start] + out[start+att+1+end+1:]
}

func twoLegInput(t *testing.T, id string) Input {
	t.Helper()
	root := t.TempDir()
	writeFixtureArchive(t, root, id)
	return Input{
		GeneratedAt:    time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:        "test",
		ArchiveSources: []string{filepath.Join(root, "bintrail_id="+id)},
		LiveIndex: &LiveIndex{
			Host: "db.internal", Port: 3306, Database: "idx", User: "reader",
			// The multi-source shape: the index leg cannot attribute its rows,
			// which is what makes the overlap's winner observable.
			Attribution: AttributionMultiSource,
		},
	}
}

// TestTwoLegs_executeInDuckDB runs the two-leg view through a real DuckDB, in a
// session whose timezone is NOT UTC.
//
// That last part is the whole point. On a UTC box every assertion here passes
// with or without the fix, which is how a timestamp bug ships: the archives'
// Parquet column reads back as TIMESTAMP WITH TIME ZONE, the index's DATETIME
// arrives naive, and UNION reconciles the pair by reading the naive value in
// the READER's timezone. The index stores UTC, so every hot row lands off by
// the reader's offset — the same event answering with two different instants
// depending on which leg served it.
func TestTwoLegs_executeInDuckDB(t *testing.T) {
	const id = "11111111-2222-3333-4444-555555555555"
	sqlText := Generate(twoLegInput(t, id))

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	// Bogota is UTC-5 year round: an offset with no DST to hide behind.
	if _, err := db.Exec(`SET TimeZone='America/Bogota'`); err != nil {
		t.Fatalf("set session timezone: %v", err)
	}
	liveStandIn(t, db)
	if _, err := db.Exec(viewsHalf(t, sqlText)); err != nil {
		t.Fatalf("DuckDB rejected the generated views:\n%v\n\n--- generated ---\n%s", err, sqlText)
	}

	// Finding A: the same instant, whichever leg served it.
	for _, tc := range []struct {
		eventID int
		want    int64
		leg     string
	}{
		{1, coldEpoch, "the archives (the event is in both)"},
		{2, hotEpoch, "the index (the event is only there)"},
	} {
		var got float64
		if err := db.QueryRow(`SELECT epoch("event_timestamp") FROM events WHERE "event_id" = ?`, tc.eventID).Scan(&got); err != nil {
			t.Fatalf("query event %d: %v", tc.eventID, err)
		}
		if int64(got) != tc.want {
			t.Errorf("event %d from %s is at epoch %d, want %d (off by %ds — the reader's UTC offset)",
				tc.eventID, tc.leg, int64(got), tc.want, int64(got)-tc.want)
		}
	}

	// And the filter form an operator actually writes: a UTC range that
	// contains the event must return it.
	var n int
	err = db.QueryRow(`SELECT COUNT(*) FROM events
	                   WHERE "event_timestamp" >= TIMESTAMPTZ '2026-05-01 09:00:00+00'
	                     AND "event_timestamp" <  TIMESTAMPTZ '2026-05-01 09:30:00+00'`).Scan(&n)
	if err != nil {
		t.Fatalf("range query: %v", err)
	}
	if n != 1 {
		t.Errorf("a UTC range containing the event returned %d rows, want 1", n)
	}

	// Finding D: the overlap is deduplicated, and the winner is the leg that
	// knows the source. If the index wins, event 1's bintrail_id becomes NULL
	// and this filter loses a row the archives hold.
	if err := db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 2 {
		t.Errorf("events has %d rows, want 2: the overlap is not deduplicated", n)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE "bintrail_id" = ?`, id).Scan(&n); err != nil {
		t.Fatalf("count attributed events: %v", err)
	}
	if n != 1 {
		t.Errorf("WHERE bintrail_id = <the archive's id> returned %d rows, want 1: "+
			"the archived row is being hidden by its unattributed twin in the index", n)
	}

	// Finding E: event_date must stay a DATE. The union widens it to VARCHAR
	// if the hot leg's strftime is left uncast, and every date expression on
	// the SAME file regenerated with this flag then breaks.
	var typ string
	if err := db.QueryRow(`SELECT typeof("event_date") FROM events LIMIT 1`).Scan(&typ); err != nil {
		t.Fatalf("typeof event_date: %v", err)
	}
	if typ != "DATE" {
		t.Errorf("event_date is %s, want DATE", typ)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE date_trunc('month', "event_date") = DATE '2026-05-01'`).Scan(&n); err != nil {
		t.Fatalf("date_trunc on event_date: %v", err)
	}
	if n != 2 {
		t.Errorf("date_trunc matched %d of 2 rows", n)
	}

	// Finding F: an event type this build does not know must render as its own
	// number. A narrowing cast to the index's TINYINT would fail the query
	// instead, which is the opposite of what the CASE's ELSE promises.
	var label string
	var code int
	if err := db.QueryRow(`SELECT "event_type", "event_type_code" FROM events WHERE "event_id" = 2`).Scan(&label, &code); err != nil {
		t.Fatalf("query unknown event type: %v", err)
	}
	if label != "200" || code != 200 {
		t.Errorf("unknown event type rendered as %q/%d, want \"200\"/200", label, code)
	}

	// The two legs must line up on hour as well as date, or a filter on one
	// silently selects a single leg.
	var hours int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE "event_hour" IN ('03', '09')`).Scan(&hours); err != nil {
		t.Fatalf("event_hour filter: %v", err)
	}
	if hours != 2 {
		t.Errorf("event_hour matched %d of 2 rows: the derived hour does not agree with the path's", hours)
	}
}

// TestTwoLegs_indexMissingColumnsStillBinds covers an index migrated to an
// earlier point than this build's schema: the console sets EnsureSchema: false
// and never migrates registry servers.
//
// The failure it prevents is not a degraded view. It is DuckDB refusing the
// statement outright — no events view created at all, and an error naming an
// internal column that means nothing to the operator reading it.
func TestTwoLegs_indexMissingColumnsStillBinds(t *testing.T) {
	const id = "11111111-2222-3333-4444-555555555555"
	missing := []string{"connection_id", "query_text", "query_hash", "commit_ts_us"}

	// First: prove the hazard is real on this exact table, so the guard below
	// is not asserting against a problem that does not exist.
	naive := twoLegInput(t, id)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	liveStandIn(t, db, missing...)
	if _, err := db.Exec(viewsHalf(t, Generate(naive))); err == nil {
		t.Fatal("naming a column the index does not have was accepted; " +
			"this test can no longer tell whether the observed column set matters")
	}

	// Now with the column set the command observes.
	observed := twoLegInput(t, id)
	for _, c := range []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp", "gtid",
		"schema_name", "table_name", "event_type", "pk_values", "pk_hash",
		"changed_columns", "row_before", "row_after", "schema_version",
	} {
		observed.LiveIndex.TableColumns = append(observed.LiveIndex.TableColumns, c)
	}
	db2, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db2.Close()
	liveStandIn(t, db2, missing...)
	if _, err := db2.Exec(viewsHalf(t, Generate(observed))); err != nil {
		t.Fatalf("the view still does not bind against an index missing columns: %v", err)
	}

	var n int
	if err := db2.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 2 {
		t.Errorf("events has %d rows, want 2", n)
	}
	// The archived row keeps the values the archives DO have: the index's gap
	// must not blank the other leg.
	var qt sql.NullString
	if err := db2.QueryRow(`SELECT "query_text" FROM events WHERE "event_id" = 2`).Scan(&qt); err != nil {
		t.Fatalf("query_text on the index-only row: %v", err)
	}
	if qt.Valid {
		t.Errorf("query_text = %q on a row from an index that has no such column", qt.String)
	}
}

// TestLiveOnly_executesInDuckDB: with nothing archived, the file is the index
// leg alone. It has to be a view that BINDS and answers, not a preamble
// followed by nothing (#1485).
func TestLiveOnly_executesInDuckDB(t *testing.T) {
	in := Input{
		GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:     "test",
		LiveIndex: &LiveIndex{
			Host: "db.internal", Port: 3306, Database: "idx", User: "reader",
			BintrailID: "single-source",
		},
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`SET TimeZone='America/Bogota'`); err != nil {
		t.Fatalf("set session timezone: %v", err)
	}
	liveStandIn(t, db)
	if _, err := db.Exec(viewsHalf(t, Generate(in))); err != nil {
		t.Fatalf("DuckDB rejected the index-only view: %v", err)
	}

	var n int
	var epoch float64
	if err := db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 2 {
		t.Errorf("events has %d rows, want 2", n)
	}
	if err := db.QueryRow(`SELECT epoch("event_timestamp") FROM events WHERE "event_id" = 2`).Scan(&epoch); err != nil {
		t.Fatalf("query event 2: %v", err)
	}
	if int64(epoch) != hotEpoch {
		t.Errorf("index-only leg is at epoch %d, want %d: the UTC reading is not the union's doing",
			int64(epoch), hotEpoch)
	}
	var bid string
	if err := db.QueryRow(`SELECT "bintrail_id" FROM events LIMIT 1`).Scan(&bid); err != nil {
		t.Fatalf("query bintrail_id: %v", err)
	}
	if bid != "single-source" {
		t.Errorf("bintrail_id = %q, want the attributed id", bid)
	}
}
