//go:build unix

package sqlsandbox

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// #2786: Session.PostgresRules. The statements are the ones internal/pgcompare
// measured against PostgreSQL 16; "pg" is PostgreSQL's answer, "mysql" what
// the session written for MySQL gives and must go on giving.
var pgRulesCases = []struct {
	sql, pg, mysql string
}{
	{"SELECT x FROM (VALUES (1),(NULL),(3)) t(x) ORDER BY x", "[[1],[3],[null]]", "[[null],[1],[3]]"},
	{"SELECT x FROM (VALUES (1),(NULL),(3)) t(x) ORDER BY x DESC", "[[null],[3],[1]]", "[[3],[1],[null]]"},
	// A NULLS clause of the statement's own wins either way.
	{"SELECT x FROM (VALUES (1),(NULL),(3)) t(x) ORDER BY x NULLS FIRST", "[[null],[1],[3]]", "[[null],[1],[3]]"},
	{"SELECT 9/2, -9/2, 9::BIGINT/2::SMALLINT", "[[4,-4,4]]", "[[4.5,-4.5,4.5]]"},
	// A float or a decimal on either side keeps the fraction.
	{"SELECT 9.0/2, 9/2::DOUBLE, 9::DECIMAL(10,2)/2", "[[4.5,4.5,4.5]]", "[[4.5,4.5,4.5]]"},
	{"SELECT sum(x)::BIGINT/count(*) FROM (VALUES (1),(2),(4)) t(x)", "[[2]]", "[[2.3333333333333335]]"},
	{"SELECT 'a' = 'A', 'a' LIKE 'A', 'a' < 'B'", "[[false,false,false]]", "[[true,false,true]]"},
	// Not PostgreSQL's answer, which is an error: the engine has no setting
	// for it. Pinned so that a change of engine that starts failing here, or
	// one that stops, is seen.
	{"SELECT 9/0, 9 % 0, 9::DOUBLE/0", "[[null,null,null]]", "[[null,null,null]]"},
}

func TestPostgresRules_changeWhatTheSessionDecides(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	for _, c := range pgRulesCases {
		job := f.job(c.sql)
		res, err := r.Run(context.Background(), job)
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		if got := rowsJSON(t, res); got != c.mysql {
			t.Errorf("without PostgresRules, %s\n got %s\nwant %s", c.sql, got, c.mysql)
		}
		job.Session.PostgresRules = true
		res, err = r.Run(context.Background(), job)
		if err != nil {
			t.Fatalf("PostgresRules, %s: %v", c.sql, err)
		}
		if got := rowsJSON(t, res); got != c.pg {
			t.Errorf("with PostgresRules, %s\n got %s\nwant %s", c.sql, got, c.pg)
		}
	}
}

// The rules are locked with the rest of the session: a statement cannot turn
// them back, and the views still read (they were installed under them).
func TestPostgresRules_lockedAndTheViewsStillRead(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	job := f.job(`SELECT current_setting('integer_division')::VARCHAR, current_setting('default_collation')::VARCHAR,
		lower(current_setting('default_null_order')::VARCHAR), current_setting('lock_configuration')::VARCHAR`)
	job.Session = Session{PostgresRules: true, MySQLClock: true, TimeZone: "America/Bogota"}
	res, err := r.Run(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := rowsJSON(t, res), `[["true","","nulls_last_on_asc_first_on_desc","true"]]`; got != want {
		t.Errorf("settings = %s, want %s", got, want)
	}
	job.SQL = "SELECT id, status FROM shop.orders ORDER BY id"
	res, err = r.Run(context.Background(), job)
	if err != nil {
		t.Fatalf("the views under PostgresRules: %v", err)
	}
	if got, want := rowsJSON(t, res), `[[1,"new"],[2,"paid"]]`; got != want {
		t.Errorf("rows = %s, want %s", got, want)
	}
	job.SQL = "SET integer_division = false"
	if _, err := r.Run(context.Background(), job); err == nil {
		t.Error("a SET was admitted")
	}
}

// One worker, statement after statement (#2084): the rules are the job's, not
// the worker's. A job without them after one with them gets the MySQL
// session, and the other way round.
func TestPostgresRules_doNotOutliveTheirJobOnAKeptWorker(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	const q = "SELECT 9/2, 'a' = 'A'"
	_, first := mustRun(t, r, f.job("SELECT 1"))
	for i, pg := range []bool{true, false, true, false, false, true} {
		job := f.job(q)
		job.Session.PostgresRules = pg
		res, pid := mustRun(t, r, job)
		if pid != first {
			t.Fatalf("statement %d ran in worker %d, want the kept worker %d", i, pid, first)
		}
		want := "[[4.5,true]]"
		if pg {
			want = "[[4,false]]"
		}
		if got := rowsJSON(t, res); got != want {
			t.Errorf("statement %d (PostgresRules %v): %s, want %s", i, pg, got, want)
		}
	}
}

// Each setting exists on the pinned engine and can be set where the worker
// sets it: after the restrictions, before the lock. And the session written
// for MySQL names none of the values.
func TestPostgresSettingsRunOnPinnedEngine(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	lock := lockdownStatements([]string{t.TempDir()}, spillSpec{})
	for _, s := range lock[:len(lock)-1] {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	for _, s := range postgresSettings {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	if _, err := db.Exec(lock[len(lock)-1]); err != nil {
		t.Fatal(err)
	}
	for _, s := range postgresSettings {
		if _, err := db.Exec(s); err == nil || !strings.Contains(err.Error(), "locked") {
			t.Errorf("after the lock, %s: err = %v, want the lock's refusal", s, err)
		}
	}
	script := strings.Join(lock, "\n")
	for _, s := range postgresSettings {
		if strings.Contains(script, s) {
			t.Errorf("the session for every job already has %q", s)
		}
	}
	if strings.Contains(script, "integer_division") {
		t.Error("the session for every job sets integer_division")
	}
}

// The field reaches the worker: it is on the wire only when set.
func TestPostgresRules_onTheWire(t *testing.T) {
	for _, pg := range []bool{false, true} {
		b, err := marshalJob(Job{SQL: "SELECT 1", Session: Session{PostgresRules: pg}}, testLimits(), spillSpec{}, "", false)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Contains(string(b), `"postgres_rules":true`); got != pg {
			t.Errorf("PostgresRules %v: wire job %s", pg, b)
		}
	}
}
