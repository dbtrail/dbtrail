//go:build integration

package pgdrill

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// scratchDB makes a database of its own on the test server and returns its
// URL; it is dropped when the test ends.
func scratchDB(t *testing.T) string {
	t.Helper()
	base := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("pgdrill_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		admin.Close(context.Background())
	})
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

// psqlFile writes a file laid out as the psql output is and the report that
// locates its rows.
func psqlFile(t *testing.T, dir, name, before, stmt string, rows []string, after string) *reconstruct.TableReport {
	t.Helper()
	data := ""
	for _, r := range rows {
		data += r + "\n"
	}
	head := before + stmt + ";\n"
	body := head + data + "\\.\n" + after
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return &reconstruct.TableReport{Files: []string{name}, RowsWritten: int64(len(rows)),
		PsqlCopy: &reconstruct.PsqlCopy{Statement: stmt, DataStart: int64(len(head)), DataEnd: int64(len(head) + len(data))}}
}

func open(t *testing.T, dsn string) *Target {
	t.Helper()
	tg, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tg.Close() })
	return tg
}

// A default's text holds lines that look like the COPY and its end; the rows
// are found by position, so they load anyway, and so do the awkward values.
func TestLoad_findsTheRowsByPosition(t *testing.T) {
	dsn := scratchDB(t)
	dir := t.TempDir()
	ctx := context.Background()
	before := "-- a comment\nBEGIN;\nSET client_encoding = 'UTF8';\nCREATE SCHEMA IF NOT EXISTS \"Shop Ü\";\n" +
		"CREATE SEQUENCE \"Shop Ü\".\"Ord ers_id_seq\" AS integer;\n" +
		"CREATE TABLE \"Shop Ü\".\"Ord ers\" (\n  \"id\" integer NOT NULL DEFAULT nextval('\"Shop Ü\".\"Ord ers_id_seq\"'::regclass),\n" +
		"  \"note\" text DEFAULT 'x\nCOPY \"Shop Ü\".\"Ord ers\" (\"id\") FROM stdin;\n\\.\n',\n" +
		"  \"twice\" integer GENERATED ALWAYS AS (id * 2) STORED,\n  PRIMARY KEY (\"id\")\n);\n" +
		"ALTER SEQUENCE \"Shop Ü\".\"Ord ers_id_seq\" OWNED BY \"Shop Ü\".\"Ord ers\".\"id\";\n"
	stmt := `COPY "Shop Ü"."Ord ers" ("id", "note") FROM stdin`
	rep := psqlFile(t, dir, "f.sql", before, stmt,
		[]string{`1	\\.`, `2	\N`, `3	tab\there`, `7	`},
		"SELECT setval('\"Shop Ü\".\"Ord ers_id_seq\"', 7, true);\nCOMMIT;\n")
	rep.Schema, rep.Table = "Shop Ü", "Ord ers"
	tg := open(t, dsn)
	if err := tg.Empty(ctx, []string{"Shop Ü"}); err != nil {
		t.Fatal(err)
	}
	if err := tg.Load(ctx, dir, rep); err != nil {
		t.Fatal(err)
	}
	n, err := tg.Count(ctx, "Shop Ü", "Ord ers")
	if err != nil || n != 4 {
		t.Fatalf("count = %d, %v", n, err)
	}
	var got string
	if err := tg.conn.QueryRow(ctx, `SELECT string_agg(id || '=' || coalesce(note, 'NULL') || '/' || twice, ',' ORDER BY id)
		FROM "Shop Ü"."Ord ers"`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if want := "1=\\./2,2=NULL/4,3=tab\there/6,7=/14"; got != want {
		t.Errorf("rows = %q, want %q", got, want)
	}
	var next int64
	if err := tg.conn.QueryRow(ctx, `SELECT nextval('"Shop Ü"."Ord ers_id_seq"')`).Scan(&next); err != nil || next != 8 {
		t.Errorf("the sequence goes on from %d (%v), want 8", next, err)
	}
	if err := tg.Empty(ctx, []string{"Shop Ü"}); err == nil || !strings.Contains(err.Error(), "drill-postgres") {
		t.Errorf("a loaded schema is not a scratch: %v", err)
	}
}

func TestLoad_emptyTable(t *testing.T) {
	dsn := scratchDB(t)
	dir := t.TempDir()
	ctx := context.Background()
	rep := psqlFile(t, dir, "e.sql", "BEGIN;\nCREATE TABLE public.e (id int PRIMARY KEY);\n",
		`COPY "public"."e" ("id") FROM stdin`, nil, "COMMIT;\n")
	if rep.PsqlCopy.DataStart != rep.PsqlCopy.DataEnd {
		t.Fatalf("an empty table's rows are an empty range: %+v", rep.PsqlCopy)
	}
	tg := open(t, dsn)
	if err := tg.Load(ctx, dir, rep); err != nil {
		t.Fatal(err)
	}
	if n, err := tg.Count(ctx, "public", "e"); err != nil || n != 0 {
		t.Fatalf("count = %d, %v", n, err)
	}
}

// A load that fails anywhere leaves nothing behind: the table is created
// inside the file's transaction, which dies with the load's connection.
func TestLoad_failureLeavesNoTable(t *testing.T) {
	dsn := scratchDB(t)
	ctx := context.Background()
	for name, c := range map[string]struct {
		rows  []string
		after string
		want  string
	}{
		"a bad row":    {[]string{"1", "not a number"}, "COMMIT;\n", "the rows"},
		"no COMMIT":    {[]string{"1"}, "", "left its transaction open"},
		"a bad ending": {[]string{"1"}, "SELECT no_such_function();\nCOMMIT;\n", "no_such_function"},
		// Two lines in the file, one written row in the report: a row the
		// writer did not count.
		"a row too many": {[]string{"1", "2"}, "COMMIT;\n", "COPY took 2 rows"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			rep := psqlFile(t, dir, "x.sql", "BEGIN;\nCREATE TABLE public.x (id int);\n",
				`COPY "public"."x" ("id") FROM stdin`, c.rows, c.after)
			if name == "a row too many" {
				rep.RowsWritten = 1
			}
			tg := open(t, dsn)
			err := tg.Load(ctx, dir, rep)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want it to say %q", err, c.want)
			}
			if err := tg.Empty(ctx, []string{"public"}); err != nil {
				t.Fatalf("the failed load left something: %v", err)
			}
		})
	}
}

const fakeEnd = "BEGIN;\nCREATE TABLE public.x (id int, n text DEFAULT '\n\\.\n');\n"

// A file that does not match its report is refused before anything runs.
func TestLoad_refusesAFileThatDoesNotMatchItsReport(t *testing.T) {
	dsn := scratchDB(t)
	ctx := context.Background()
	tg := open(t, dsn)
	for name, mutate := range map[string]func(*reconstruct.TableReport){
		"no rows located": func(r *reconstruct.TableReport) { r.PsqlCopy = nil },
		"two files":       func(r *reconstruct.TableReport) { r.Files = append(r.Files, "y.sql") },
		"start moved":     func(r *reconstruct.TableReport) { r.PsqlCopy.DataStart-- },
		"end moved":       func(r *reconstruct.TableReport) { r.PsqlCopy.DataEnd-- },
		"past the end":    func(r *reconstruct.TableReport) { r.PsqlCopy.DataEnd += 100 },
		// The same length and loadable: only the bytes tell it apart.
		"other statement": func(r *reconstruct.TableReport) { r.PsqlCopy.Statement = `COPY "public"."x" ("id") FROM STDIN` },
		// Points the end at the end-of-data line inside the default's text,
		// ahead of the start.
		"end before start": func(r *reconstruct.TableReport) { r.PsqlCopy.DataEnd = int64(strings.Index(fakeEnd, "\\.\n")) },
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			rep := psqlFile(t, dir, "x.sql", fakeEnd,
				`COPY "public"."x" ("id") FROM stdin`, []string{"1", "2"}, "COMMIT;\n")
			mutate(rep)
			// Refused by the check, not by a statement failing later.
			if err := tg.Load(ctx, dir, rep); err == nil || !strings.Contains(err.Error(), "did not report") && !strings.Contains(err.Error(), "does not match its report") {
				t.Fatalf("err = %v", err)
			}
			if err := tg.Empty(ctx, []string{"public"}); err != nil {
				t.Fatalf("a refused file ran: %v", err)
			}
		})
	}
}

// Anything in a drilled schema makes the target not a scratch, not only a
// table: the files create their sequences without IF NOT EXISTS.
func TestEmpty_anyRelation(t *testing.T) {
	dsn := scratchDB(t)
	ctx := context.Background()
	tg := open(t, dsn)
	if err := tg.Empty(ctx, []string{"public", "no_such_schema"}); err != nil {
		t.Fatalf("a fresh database is a scratch: %v", err)
	}
	for _, q := range []string{"CREATE SCHEMA s1", "CREATE SEQUENCE s1.only_seq", "CREATE SCHEMA s2", "CREATE VIEW s2.only_view AS SELECT 1 AS one"} {
		if _, err := tg.conn.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	for s, want := range map[string]string{"s1": "s1.only_seq", "s2": "s2.only_view"} {
		if err := tg.Empty(ctx, []string{"public", s}); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("schema %s: %v", s, err)
		}
	}
}
