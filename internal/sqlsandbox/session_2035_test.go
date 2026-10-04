package sqlsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// The session time zone (#2035) reaches the engine: set after the views
// script, which pins UTC, and before the lock. The proof is a value whose
// answer depends on the zone: a zone-less timestamp read as an instant.
func TestRun_sessionTimeZone(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	const q = "SELECT current_setting('TimeZone') AS z, (TIMESTAMP '2026-01-15 12:00:00')::TIMESTAMPTZ AS at, " +
		"current_setting('lock_configuration') AS locked"

	for _, tc := range []struct {
		zone, wantZone, wantAt string
	}{
		{"", "UTC", "2026-01-15T12:00:00Z"},
		{"Asia/Tokyo", "Asia/Tokyo", "2026-01-15T03:00:00Z"},
		// MySQL's '+03:00' as the port maps it: POSIX inverts the sign.
		{"Etc/GMT-3", "Etc/GMT-3", "2026-01-15T09:00:00Z"},
		{"America/Argentina/Buenos_Aires", "America/Argentina/Buenos_Aires", "2026-01-15T15:00:00Z"},
	} {
		job := f.job(q)
		job.Session.TimeZone = tc.zone
		res, err := r.Run(context.Background(), job)
		if err != nil {
			t.Fatalf("zone %q: %v", tc.zone, err)
		}
		if got := res.Rows[0]; got[0] != tc.wantZone || got[1] != tc.wantAt {
			t.Errorf("zone %q: (TimeZone, noon as an instant) = (%v, %v), want (%s, %s)", tc.zone, got[0], got[1], tc.wantZone, tc.wantAt)
		}
		// The zone was set before the lock, and the lock still happened.
		if got := res.Rows[0][2]; got != "true" && got != true {
			t.Errorf("zone %q: lock_configuration = %v, want it on", tc.zone, got)
		}
	}
}

// A zone the engine does not know refuses the statement and names the zone;
// it never runs under UTC instead. A quote in the name stays inside the
// literal.
func TestRun_sessionTimeZoneRefusedByName(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	for _, zone := range []string{"Nope/Zone", "+03:00", "UTC'; SET lock_configuration = false; --"} {
		job := f.job("SELECT 1")
		job.Session.TimeZone = zone
		_, err := r.Run(context.Background(), job)
		var refused *RefusedError
		if !errors.As(err, &refused) {
			t.Fatalf("zone %q: err = %v (%T), want *RefusedError", zone, err, err)
		}
		if !strings.Contains(refused.Reason, "time_zone") || !strings.Contains(refused.Reason, zone[:4]) {
			t.Errorf("zone %q: reason %q does not name the setting and the zone", zone, refused.Reason)
		}
		if strings.Contains(refused.Reason, "\n") {
			t.Errorf("zone %q: reason carries the engine's candidate list: %q", zone, refused.Reason)
		}
	}
}

// sql_select_limit (#2035): the cut the client asked for is silent; the cap
// still rules above it; the statement's own LIMIT wins; SHOW is not a SELECT.
func TestRun_sessionSelectLimit(t *testing.T) {
	f := newCopyFixture(t)
	l := testLimits()
	l.MaxRows = 5
	r := newTestRunner(t, l)
	for _, tc := range []struct {
		name, sql string
		limit     int
		wantRows  int
		wantTrunc bool
	}{
		{"no limit set: the cap cuts, and says so", "SELECT * FROM range(9)", 0, 5, true},
		{"limit under the cap: cut there, silently", "SELECT * FROM range(9)", 3, 3, false},
		{"limit equal to the cap: the client's cut", "SELECT * FROM range(9)", 5, 5, false},
		{"limit above the cap: the cap rules", "SELECT * FROM range(9)", 7, 5, true},
		{"limit above the result: everything", "SELECT * FROM range(2)", 3, 2, false},
		{"limit equal to the result: nothing was cut", "SELECT * FROM range(3)", 3, 3, false},
		{"the statement's own LIMIT wins over a smaller limit", "SELECT * FROM range(9) LIMIT 4", 2, 4, false},
		{"the statement's own LIMIT is still under the cap", "SELECT * FROM range(9) LIMIT 8", 2, 5, true},
		{"an OFFSET alone is not a LIMIT", "SELECT * FROM range(9) OFFSET 1", 2, 2, false},
		{"a LIMIT inside a subquery is not the statement's", "SELECT * FROM (SELECT * FROM range(9) LIMIT 4)", 2, 2, false},
		{"a LIMIT inside a WITH is not the statement's", "WITH a AS (SELECT * FROM range(9) LIMIT 4) SELECT * FROM a", 2, 2, false},
		{"a set operation is limited whole", "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3", 2, 2, false},
		{"a set operation's own LIMIT wins", "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 LIMIT 3", 2, 3, false},
		{"a percent LIMIT is a LIMIT", "SELECT * FROM range(8) LIMIT 50%", 2, 4, false},
		{"VALUES is a SELECT", "VALUES (1), (2), (3)", 2, 2, false},
		{"DESCRIBE is not a SELECT", "DESCRIBE SELECT 1 AS a, 2 AS b, 3 AS c", 2, 3, false},
	} {
		job := f.job(tc.sql)
		job.Session.SelectLimit = tc.limit
		res, err := r.Run(context.Background(), job)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if len(res.Rows) != tc.wantRows || res.Truncated != tc.wantTrunc {
			t.Errorf("%s: rows=%d truncated=%v, want %d/%v", tc.name, len(res.Rows), res.Truncated, tc.wantRows, tc.wantTrunc)
		}
	}
	// The first rows, in the statement's order.
	job := f.job("SELECT range AS i FROM range(9) ORDER BY i DESC")
	job.Session.SelectLimit = 2
	res, err := r.Run(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 || res.Rows[0][0] != json.Number("8") || res.Rows[1][0] != json.Number("7") {
		t.Errorf("limit 2 over a descending order = %v, want 8 then 7", res.Rows)
	}
}

// selectLimitApplies reads DuckDB's own tree. A shape it does not know is
// reported as unknown, never guessed either way.
func TestSelectLimitApplies_unknownShapeIsNotGuessed(t *testing.T) {
	for _, tc := range []struct {
		name string
		stmt any
	}{
		{"not an object", "x"},
		{"no node", map[string]any{}},
		{"a node type this build does not know", map[string]any{"node": map[string]any{"type": "CTE_NODE"}}},
		{"modifiers that are not a list", map[string]any{"node": map[string]any{"type": "SELECT_NODE", "modifiers": "x"}}},
		{"a modifier that is not an object", map[string]any{"node": map[string]any{"type": "SELECT_NODE", "modifiers": []any{"x"}}}},
	} {
		if applies, known := selectLimitApplies(tc.stmt); known || applies {
			t.Errorf("%s: (applies, known) = (%v, %v), want (false, false)", tc.name, applies, known)
		}
	}
	// The pinned engine's own parse of a plain SELECT is known and applies.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stmt, reason := parseStatement(context.Background(), conn, "SELECT 1")
	if reason != "" {
		t.Fatal(reason)
	}
	if applies, known := selectLimitApplies(stmt); !applies || !known {
		t.Errorf("SELECT 1: (applies, known) = (%v, %v), want (true, true)", applies, known)
	}
}
