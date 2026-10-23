package oracle

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func rowsOf(table string, rows ...[]string) TableRows {
	return TableRows{Table: table, Columns: []string{"id", "v", "note"}, PK: []string{"id"}, Rows: rows}
}

func snapshotDirWithFiles(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "2026-10-22T10-00-00Z")
	if err := os.MkdirAll(filepath.Join(dir, "sch"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, size := range map[string]int{"sch/t_int.parquet": 1234, "sch/t_int.000001.posdel": 56, "sch/t_int.000001.upserts": 789, "_SUCCESS": 0} {
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func checkFails(t *testing.T, cx Context, source, copy TableRows, want ...string) *Failure {
	t.Helper()
	err := Check(cx, source, copy)
	if err == nil {
		t.Fatalf("no failure; want one saying %q", want[0])
	}
	var f *Failure
	if !errors.As(err, &f) {
		t.Fatalf("error is a %T, want *Failure", err)
	}
	msg := err.Error()
	for _, w := range want {
		if !strings.Contains(msg, w) {
			t.Errorf("failure message lacks %q:\n%s", w, msg)
		}
	}
	return f
}

// TestCheck_messageCarriesEverything generates the failure message from real
// data and checks it carries the seed, the step, the table, the key, both
// rows, the snapshot's files with their sizes and the reproduction command.
func TestCheck_messageCarriesEverything(t *testing.T) {
	cx := Context{Seed: 7, Step: 3, Steps: 10, StepName: "refresh", Session: "America/Bogota", SnapshotDir: snapshotDirWithFiles(t)}
	source := rowsOf("t_int", []string{"1", "10", "a"}, []string{"2", "20", "NULL"})
	copy := rowsOf("t_int", []string{"1", "10", "a"}, []string{"2", "21", "NULL"})
	f := checkFails(t, cx, source, copy,
		"seed 7", "step 3 of 10", "(refresh, session America/Bogota)", "table t_int", "key 2", "column v",
		"source: id=2, v=20, note=NULL", "copy:   id=2, v=21, note=NULL",
		"sch/t_int.parquet 1234 bytes", "sch/t_int.000001.posdel 56 bytes", "sch/t_int.000001.upserts 789 bytes", "_SUCCESS 0 bytes",
		"BINTRAIL_ORACLE_SEED=7 BINTRAIL_ORACLE_STEPS=10 go test -tags integration -run TestOracle ./internal/oracle/")
	if f.Check != CheckRows || f.Key != "2" || f.Column != "v" {
		t.Fatalf("failure = check %q key %q column %q", f.Check, f.Key, f.Column)
	}
}

// TestCheck_order: the three checks run in order. A duplicated key fails the
// first even when the counts also differ; a count mismatch without
// duplicates fails the second; equal counts with a key swapped fail the
// third naming the key missing from the copy and the one it has instead.
// TestFailure_saysAsOf: a table answered as of the published cut is
// compared with that cut's rows, and the message says so, or a mismatch
// would read as the current cut's.
func TestFailure_saysAsOf(t *testing.T) {
	f := &Failure{Context: Context{Seed: 1, Step: 4, Steps: 9, StepName: "tail", AsOf: "the published cut (step 2)"}, Table: "t_int", Detail: "row count differs"}
	msg := f.Error()
	if !strings.Contains(msg, "the copy answered as of the published cut (step 2); the rows compared are that cut's") {
		t.Fatalf("message does not say the cut compared:\n%s", msg)
	}
	if strings.Contains((&Failure{Context: Context{Seed: 1}, Table: "t_int", Detail: "x"}).Error(), "answered as of") {
		t.Fatal("a check against the cut itself must not claim an earlier one")
	}
}

func TestCheck_order(t *testing.T) {
	cx := Context{Seed: 1, Step: 1, Steps: 1, StepName: "snapshot"}
	source := rowsOf("t_int", []string{"1", "10", "a"})
	dup := rowsOf("t_int", []string{"1", "10", "a"}, []string{"1", "10", "a"}, []string{"3", "30", "c"})
	f := checkFails(t, cx, source, dup, "key 1 appears 2 times in the copy", "every primary key must appear once")
	if f.Check != CheckOnePerKey {
		t.Fatalf("check = %q, want %q", f.Check, CheckOnePerKey)
	}

	more := rowsOf("t_int", []string{"1", "10", "a"}, []string{"3", "30", "c"})
	f = checkFails(t, cx, source, more, "the copy has 2 rows, the source 1", "key 3 is in the copy and not in the source", "copy:   id=3, v=30, note=c")
	if f.Check != CheckCount {
		t.Fatalf("check = %q, want %q", f.Check, CheckCount)
	}

	swapped := rowsOf("t_int", []string{"3", "30", "c"})
	f = checkFails(t, cx, source, swapped, "key 1 is in the source and not in the copy", "copy has key 3 instead", "source: id=1, v=10, note=a")
	if f.Check != CheckRows || f.Key != "1" {
		t.Fatalf("check = %q key %q", f.Check, f.Key)
	}

	if err := Check(cx, source, rowsOf("t_int", []string{"1", "10", "a"})); err != nil {
		t.Fatalf("equal tables fail: %v", err)
	}
	if err := Check(cx, rowsOf("t_int"), rowsOf("t_int")); err != nil {
		t.Fatalf("two empty tables fail: %v", err)
	}
	checkFails(t, cx, rowsOf("t_int"), rowsOf("t_int", []string{"9", "0", "NULL"}), "the copy has 1 rows, the source 0")
}

// TestCheck_compositeKeyAndNullText: a composite key is reported joined, and
// NULL against the text NULL compares equal on purpose (both sides render a
// NULL that way; the generator never writes the text).
func TestCheck_compositeKeyAndNullText(t *testing.T) {
	cx := Context{Seed: 2, Step: 1, Steps: 1}
	src := TableRows{Table: "t_comp", Columns: []string{"a", "b", "v"}, PK: []string{"a", "b"}, Rows: [][]string{{"1", "2", "5"}}}
	cp := TableRows{Table: "t_comp", Columns: []string{"a", "b", "v"}, PK: []string{"a", "b"}, Rows: [][]string{{"1", "2", "6"}}}
	checkFails(t, cx, src, cp, "key 1|2", "column v")
}

// TestReadView_datetimeText runs ReadView against a real DuckDB session, the
// two ways the run reads the copy: a DATETIME column read as TIMESTAMPTZ
// under a UTC session prints a zone suffix unless the read names it as a
// datetime and formats the wall clock; NULL reads as NULL either way.
func TestReadView_datetimeText(t *testing.T) {
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	ctx := context.Background()
	for _, q := range []string{
		"SET TimeZone = 'UTC'",
		"CREATE SCHEMA sch",
		"CREATE VIEW sch.t_types AS SELECT 1 AS id, TIMESTAMPTZ '2027-01-02 03:04:05+00' AS seen, NULL::TIMESTAMPTZ AS seen2, 12.30::DECIMAL(12,2) AS amount",
	} {
		if _, err := ddb.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	cols := []string{"id", "seen", "seen2", "amount"}
	got, err := ReadView(ctx, ddb, "sch", "t_types", cols, []string{"id"}, []string{"seen", "seen2"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"1", "2027-01-02 03:04:05", "NULL", "12.30"}; strings.Join(got.Rows[0], ",") != strings.Join(want, ",") {
		t.Fatalf("normalized read = %v, want %v", got.Rows[0], want)
	}
	raw, err := ReadView(ctx, ddb, "sch", "t_types", cols, []string{"id"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if raw.Rows[0][1] != "2027-01-02 03:04:05+00" {
		t.Fatalf("raw read of a TIMESTAMPTZ = %q; the normalization above exists because this is not the source's text", raw.Rows[0][1])
	}
}

func TestFilesUnder_missingDir(t *testing.T) {
	cx := Context{Seed: 3, Step: 1, Steps: 1, SnapshotDir: "/nonexistent/oracle"}
	checkFails(t, cx, rowsOf("t_int"), rowsOf("t_int", []string{"1", "1", "x"}), "/nonexistent/oracle", "cannot list")
}
