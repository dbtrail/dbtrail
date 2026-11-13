//go:build integration

package pgcompare

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	itCustomers = "pgcmp_customers"
	itOrders    = "pgcmp_orders"
	itPub       = "pgcmp_pub"
	itSlot      = "pgcmp_slot"
)

// expectation is what one statement gives today. A statement listed as
// anything but EQUAL names the issue that makes it so: when that issue is
// fixed this test fails on that line, and the line moves to EQUAL.
type expectation struct {
	stmt    string
	verdict sqlcompare.Verdict
	kind    string
	why     string
}

// TestIntegrationPGCompare takes a snapshot of a PostgreSQL source the way
// the product does (pgbaseline.Run), opens it as the copy, and runs a list of
// statements on both. Every verdict is pinned, the wrong ones included: the
// list is the measured state of "same statement, same answer" for a
// PostgreSQL source.
func TestIntegrationPGCompare(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	dropAll := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+itPub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+itOrders+", "+itCustomers)
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", itSlot)
	}
	dropAll()
	t.Cleanup(dropAll)
	for _, s := range seedSQL() {
		if _, err := setup.Exec(ctx, s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	outDir := t.TempDir()
	repl := baseDSN + "?replication=database"
	if strings.Contains(baseDSN, "?") {
		repl = baseDSN + "&replication=database"
	}
	if _, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN: baseDSN, ReplDSN: repl, SlotName: itSlot, Publication: itPub,
		OutputDir: outDir, Compression: "none", Parallelism: 2,
	}); err != nil {
		t.Fatalf("pgbaseline.Run: %v", err)
	}
	snapshot := ""
	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if _, ok := snapshotdir.ParseTime(e.Name()); ok && e.IsDir() {
			snapshot = filepath.Join(outDir, e.Name())
		}
	}
	if snapshot == "" {
		t.Fatalf("no snapshot directory in %s: %v", outDir, entries)
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}})
	cp, err := OpenSnapshotCopy(ctx, runner, snapshot, "public")
	if err != nil {
		t.Fatalf("open the copy: %v", err)
	}
	src, err := OpenSource(ctx, baseDSN, "public")
	if err != nil {
		t.Fatalf("open the source: %v", err)
	}
	t.Cleanup(func() { src.Close(context.Background()) })

	want := expectations()
	equal := 0
	for _, e := range want {
		if e.verdict == sqlcompare.Equal {
			equal++
		}
	}
	// 26 of the 54 statements answering the same is the measured starting
	// point; the count only goes up from here.
	if equal < 26 {
		t.Fatalf("%d statements are expected to be EQUAL, fewer than the 26 this list started with", equal)
	}
	stmts := make([]string, len(want))
	for i, e := range want {
		stmts[i] = e.stmt
	}
	// The source is quiet: nothing writes to these tables after the seed.
	rep, err := Run(ctx, src, cp, Options{NoRerun: true}, stmts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for i, r := range rep.Results {
		e := want[i]
		line := fmt.Sprintf("%-13s %-10s %s\n    detail: %s", r.Verdict, r.Kind, r.Statement, r.Detail)
		if r.Verdict != e.verdict || r.Kind != e.kind {
			t.Errorf("want %s/%q (%s), got:\n  %s", e.verdict, e.kind, e.why, line)
			continue
		}
		t.Log(line)
	}

	// The read-only session is the second lock behind IsQuery: a WITH that
	// writes reaches the source and must be refused there.
	writes := "WITH gone AS (DELETE FROM " + itOrders + " RETURNING id) SELECT count(*) FROM gone"
	rep, err = Run(ctx, src, cp, Options{NoRerun: true}, []string{writes, "SELECT 1; SELECT 2"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, r := range rep.Results {
		if r.Verdict != sqlcompare.SourceError {
			t.Errorf("%s: want SOURCE_ERROR, got %s (%s)", r.Statement, r.Verdict, r.Detail)
		}
	}
	var left int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM "+itOrders).Scan(&left); err != nil || left != 12 {
		t.Fatalf("orders after the refused write: %d rows, err %v; want 12", left, err)
	}
}

func seedSQL() []string {
	return []string{
		`CREATE TABLE ` + itCustomers + ` (
			id int PRIMARY KEY, name text NOT NULL, city text, active boolean, signup date)`,
		`INSERT INTO ` + itCustomers + ` VALUES
			(1, 'Ana',   'Lima',   true,  '2025-11-30'),
			(2, 'bruno', 'Quito',  false, '2026-01-15'),
			(3, 'Carla', 'lima',   true,  '2026-02-01'),
			(4, 'Dario', NULL,     NULL,  NULL)`,
		`CREATE TABLE ` + itOrders + ` (
			id bigint PRIMARY KEY, customer_id int, amount numeric(12,2), qty int,
			ratio double precision, status varchar(16), note text,
			placed_at timestamp, seen_at timestamptz, tag uuid, attrs jsonb,
			payload bytea, labels text[])`,
		`INSERT INTO ` + itOrders + ` VALUES
			(1,  1, 9.50,     2,    0.1,   'new',  'first',  '2026-01-02 03:04:05',     '2026-01-02 03:04:05+00',    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"k": "v", "n": 1}',  '\x00ff', '{red,blue}'),
			(2,  1, 10.00,    9,    0.25,  'NEW',  'second', '2026-01-02 03:04:05.5',   '2026-01-02 08:04:05.5+05',  'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', '{"k": "w", "n": 20}', '\x',     '{}'),
			(3,  2, 100.25,   10,   1.5,   'paid', 'third',  '2026-01-31 23:59:59',     '2026-01-31 23:59:59+00',    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13', '{"k": "v"}',          NULL,     '{red}'),
			(4,  2, 1234.56,  100,  -2.75, 'Paid', NULL,     '2026-02-01 00:00:00',     '2026-02-01 00:00:00+00',    NULL,                                   NULL,                  NULL,     NULL),
			(5,  3, -5.00,    -5,   1e20,  'paid', '',       '2026-02-14 12:00:00',     '2026-02-14 12:00:00+00',    NULL,                                   '{}',                  NULL,     NULL),
			(6,  3, 0.00,     0,    0,     'void', 'x',      '2026-03-01 00:00:00',     '2026-03-01 00:00:00+00',    NULL,                                   NULL,                  NULL,     NULL),
			(7,  3, NULL,     NULL, NULL,  NULL,   NULL,     NULL,                      NULL,                        NULL,                                   NULL,                  NULL,     NULL),
			(8,  4, 20.10,    11,   2.5,   'new',  'y',      '2026-03-02 10:00:00',     '2026-03-02 10:00:00+00',    NULL,                                   NULL,                  NULL,     NULL),
			(9,  4, 3.30,     3,    3.3,   'new',  'z',      '2026-03-03 10:00:00',     '2026-03-03 10:00:00+00',    NULL,                                   NULL,                  NULL,     NULL),
			(10, 9, 7.70,     7,    7.7,   'paid', 'w',      '2026-03-04 10:00:00',     '2026-03-04 10:00:00+00',    NULL,                                   NULL,                  NULL,     NULL),
			(11, 1, 8.80,     8,    8.8,   'paid', 'v',      '2026-03-05 10:00:00',     '2026-03-05 10:00:00+00',    NULL,                                   NULL,                  NULL,     NULL),
			(12, 1, 9.90,     20,   9.9,   'paid', 'u',      '2026-03-06 10:00:00',     '2026-03-06 10:00:00+00',    NULL,                                   NULL,                  NULL,     NULL)`,
		`CREATE PUBLICATION ` + itPub + ` FOR TABLE ` + itCustomers + `, ` + itOrders,
	}
}

func expectations() []expectation {
	o, c := itOrders, itCustomers
	eq := func(stmt string) expectation { return expectation{stmt: stmt, verdict: sqlcompare.Equal} }
	// Both of these are #2755: the copy of a PostgreSQL source holds every
	// column as text, compares text without regard to case, and returns the
	// columns of a star in alphabetical order. diff is a statement the copy
	// answers wrongly, refused one it cannot run.
	const allText = "#2755: every column of the copy is text"
	diff := func(kind, stmt string) expectation {
		return expectation{stmt: stmt, verdict: sqlcompare.Different, kind: kind, why: allText}
	}
	refused := func(stmt string) expectation {
		return expectation{stmt: stmt, verdict: sqlcompare.NotOnCopy, why: allText}
	}
	return []expectation{
		// No tables.
		eq(`SELECT 1`),
		eq(`SELECT 1 + 1 AS two, 'a' || 'b' AS ab`),
		eq(`SELECT true AS yes, false AS no, NULL AS nothing`),
		// Plain reads.
		eq(`SELECT count(*) FROM ` + o),
		eq(`SELECT id, note FROM ` + o),
		eq(`SELECT id, amount, qty, ratio FROM ` + o),
		eq(`SELECT id, placed_at, seen_at FROM ` + o),
		eq(`SELECT id, tag, attrs, payload, labels FROM ` + o),
		eq(`SELECT id, name, city, active, signup FROM ` + c),
		eq(`SELECT name FROM ` + c + ` WHERE id = 3`),
		diff("rows", `SELECT name FROM ` + c + ` WHERE city = 'Lima'`),
		eq(`SELECT name FROM ` + c + ` WHERE city IS NULL`),
		eq(`SELECT o.id, c.name FROM ` + o + ` o JOIN ` + c + ` c ON c.id = o.customer_id`),
		eq(`SELECT c.name, count(o.id) FROM ` + c + ` c LEFT JOIN ` + o + ` o ON o.customer_id = c.id GROUP BY c.name`),
		// Order.
		diff("order", `SELECT id FROM ` + o + ` ORDER BY id`),
		diff("order", `SELECT id, qty FROM ` + o + ` ORDER BY qty, id`),
		diff("text", `SELECT id FROM ` + o + ` ORDER BY amount DESC NULLS LAST, id LIMIT 3`),
		eq(`SELECT name FROM ` + c + ` ORDER BY name`),
		// Numbers.
		refused(`SELECT sum(qty), min(qty), max(qty) FROM ` + o),
		refused(`SELECT sum(amount), avg(amount) FROM ` + o),
		diff("rows", `SELECT max(amount) FROM ` + o),
		refused(`SELECT id FROM ` + o + ` WHERE qty > 9`),
		refused(`SELECT id FROM ` + o + ` WHERE amount BETWEEN 5 AND 50`),
		refused(`SELECT id, qty * 2 AS twice, amount * qty AS total FROM ` + o),
		refused(`SELECT customer_id, sum(amount) FROM ` + o + ` GROUP BY customer_id`),
		refused(`SELECT id, qty / 2 AS half FROM ` + o),
		refused(`SELECT id, round(ratio::numeric, 1) FROM ` + o),
		// Text.
		diff("rows", `SELECT status, count(*) FROM ` + o + ` GROUP BY status`),
		diff("rows", `SELECT DISTINCT status FROM ` + o),
		diff("rows", `SELECT id FROM ` + o + ` WHERE status = 'new'`),
		eq(`SELECT id FROM ` + o + ` WHERE status LIKE 'p%'`),
		eq(`SELECT id FROM ` + o + ` WHERE status ILIKE 'p%'`),
		eq(`SELECT upper(name), length(name) FROM ` + c),
		eq(`SELECT id, coalesce(note, '-') FROM ` + o),
		// Booleans.
		eq(`SELECT name FROM ` + c + ` WHERE active`),
		eq(`SELECT name FROM ` + c + ` WHERE NOT active`),
		eq(`SELECT active, count(*) FROM ` + c + ` GROUP BY active`),
		// Dates and times.
		refused(`SELECT name FROM ` + c + ` WHERE signup >= DATE '2026-01-01'`),
		refused(`SELECT name, signup + 1 FROM ` + c),
		refused(`SELECT id FROM ` + o + ` WHERE placed_at < TIMESTAMP '2026-02-01 00:00:00'`),
		refused(`SELECT date_trunc('month', placed_at) AS m, count(*) FROM ` + o + ` GROUP BY 1`),
		refused(`SELECT id, extract(year FROM placed_at) FROM ` + o),
		eq(`SELECT id, placed_at::date FROM ` + o),
		eq(`SELECT min(seen_at), max(seen_at) FROM ` + o),
		// JSON, arrays, bytes, uuid.
		eq(`SELECT id, attrs->>'k' FROM ` + o),
		refused(`SELECT id FROM ` + o + ` WHERE attrs @> '{"k": "v"}'`),
		diff("rows", `SELECT id, labels[1] FROM ` + o),
		refused(`SELECT id FROM ` + o + ` WHERE 'red' = ANY(labels)`),
		diff("rows", `SELECT id, length(payload) FROM ` + o),
		eq(`SELECT id FROM ` + o + ` WHERE tag = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'`),
		// Shapes.
		refused(`WITH big AS (SELECT * FROM ` + o + ` WHERE qty >= 10) SELECT count(*) FROM big`),
		diff("rows", `SELECT id, row_number() OVER (ORDER BY id) FROM ` + o),
		eq(`SELECT id FROM ` + o + ` WHERE customer_id IN (SELECT id FROM ` + c + ` WHERE active)`),
		diff("columns", `SELECT * FROM ` + c),
		// Not compared.
		{stmt: `SELECT * FROM pgcmp_missing`, verdict: sqlcompare.SourceError},
		{stmt: `DELETE FROM ` + o, verdict: sqlcompare.Skipped, kind: "not_a_query"},
	}
}
