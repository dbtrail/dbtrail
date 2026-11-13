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
	itOdd       = "pgcmp_odd"
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
	// says is a piece of the copy's refusal, for NOT_ON_COPY: a statement
	// refused for another reason is another finding.
	says string
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
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+itOrders+", "+itCustomers+", "+itOdd)
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
		if r.Verdict != e.verdict || r.Kind != e.kind || !strings.Contains(r.Detail, e.says) {
			if e.says != "" {
				e.why += "; the copy's refusal should say " + e.says
			}
			t.Errorf("want %s/%q (%s), got:\n  %s", e.verdict, e.kind, e.why, line)
			continue
		}
		t.Log(line)
	}

	// 26 statements answered the same when every column of the copy was
	// text; 50 do with the columns typed (#2755). The count only goes up.
	if n := rep.Counts[sqlcompare.Equal]; n < 50 {
		t.Errorf("%d statements were EQUAL, fewer than 50", n)
	}

	// The read-only transaction is the lock behind IsQuery and ReadOnlyVeto.
	// Straight to the source, past both: a statement that turns the session
	// read-write changes nothing for the next one, and a WITH that writes is
	// refused.
	for _, unlock := range []string{
		"SELECT set_config('default_transaction_read_only', 'off', false)",
		"SELECT set_config('transaction_read_only', 'off', false)",
	} {
		_, _, _ = src.Query(ctx, unlock, 10)
		if _, _, err := src.Query(ctx, "WITH gone AS (DELETE FROM "+itOrders+" RETURNING id) SELECT count(*) FROM gone", 10); err == nil || !strings.Contains(err.Error(), "read-only") {
			t.Fatalf("after %s the source ran a DELETE, or refused it for another reason: %v", unlock, err)
		}
	}

	// Through Run: a WITH that writes reaches the source and is refused
	// there, and a second statement after a `;` is refused by the protocol.
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
		`CREATE TABLE ` + itOdd + ` (id int PRIMARY KEY, free numeric, fixed numeric(10,2))`,
		`INSERT INTO ` + itOdd + ` VALUES
			(1, 'NaN', 1.10),
			(2, 1.1234567890123456789012, 2.20),
			(3, 12345.678, 3.30),
			(4, 99999999999999999999, 4.40),
			(5, NULL, NULL)`,
		`CREATE PUBLICATION ` + itPub + ` FOR TABLE ` + itCustomers + `, ` + itOrders + `, ` + itOdd,
	}
}

func expectations() []expectation {
	o, c, odd := itOrders, itCustomers, itOdd
	eq := func(stmt string) expectation { return expectation{stmt: stmt, verdict: sqlcompare.Equal} }
	// What still differs, each with the issue it belongs to. still is a
	// statement the copy answers differently, refused one it cannot run.
	const (
		nullsAndDivision = "#2786: the copy sorts NULL and divides integers by MySQL's rules"
		otherTypes       = "#2787: arrays and json are still read as text"
		dialect          = "#2770: the copy's dialect answers this differently; it belongs to the source"
	)
	still := func(kind, says, why, stmt string) expectation {
		return expectation{stmt: stmt, verdict: sqlcompare.Different, kind: kind, why: why, says: says}
	}
	refused := func(says, why, stmt string) expectation {
		return expectation{stmt: stmt, verdict: sqlcompare.NotOnCopy, why: why, says: says}
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
		eq(`SELECT name FROM ` + c + ` WHERE city = 'Lima'`),
		eq(`SELECT name FROM ` + c + ` WHERE city IS NULL`),
		eq(`SELECT o.id, c.name FROM ` + o + ` o JOIN ` + c + ` c ON c.id = o.customer_id`),
		eq(`SELECT c.name, count(o.id) FROM ` + c + ` c LEFT JOIN ` + o + ` o ON o.customer_id = c.id GROUP BY c.name`),
		// Order.
		eq(`SELECT id FROM ` + o + ` ORDER BY id`),
		still("order", "", nullsAndDivision, `SELECT id, qty FROM `+o+` ORDER BY qty, id`),
		eq(`SELECT id FROM ` + o + ` ORDER BY amount DESC NULLS LAST, id LIMIT 3`),
		eq(`SELECT name FROM ` + c + ` ORDER BY signup NULLS LAST`),
		eq(`(SELECT id FROM ` + o + ` ORDER BY id)`),
		// Numbers.
		eq(`SELECT sum(qty), min(qty), max(qty) FROM ` + o),
		still("precision", "", dialect, `SELECT sum(amount), avg(amount) FROM `+o),
		eq(`SELECT max(amount) FROM ` + o),
		eq(`SELECT id FROM ` + o + ` WHERE qty > 9`),
		eq(`SELECT id FROM ` + o + ` WHERE amount BETWEEN 5 AND 50`),
		eq(`SELECT id, qty * 2 AS twice, amount * qty AS total FROM ` + o),
		eq(`SELECT customer_id, sum(amount) FROM ` + o + ` GROUP BY customer_id`),
		still("rows", "", nullsAndDivision, `SELECT id, qty / 2 AS half FROM `+o),
		refused("to DECIMAL(18,3)", dialect, `SELECT id, round(ratio::numeric, 1) FROM `+o),
		// Text.
		eq(`SELECT status, count(*) FROM ` + o + ` GROUP BY status`),
		eq(`SELECT DISTINCT status FROM ` + o),
		eq(`SELECT id FROM ` + o + ` WHERE status = 'new'`),
		eq(`SELECT id FROM ` + o + ` WHERE status LIKE 'p%'`),
		eq(`SELECT id FROM ` + o + ` WHERE status ILIKE 'p%'`),
		eq(`SELECT upper(name), length(name) FROM ` + c),
		eq(`SELECT id, coalesce(note, '-') FROM ` + o),
		// Booleans.
		eq(`SELECT name FROM ` + c + ` WHERE active`),
		eq(`SELECT name FROM ` + c + ` WHERE NOT active`),
		eq(`SELECT active, count(*) FROM ` + c + ` GROUP BY active`),
		// Dates and times.
		eq(`SELECT name FROM ` + c + ` WHERE signup >= DATE '2026-01-01'`),
		eq(`SELECT name, signup + 1 FROM ` + c),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at < TIMESTAMP '2026-02-01 00:00:00'`),
		eq(`SELECT date_trunc('month', placed_at) AS m, count(*) FROM ` + o + ` GROUP BY 1`),
		eq(`SELECT id, extract(year FROM placed_at) FROM ` + o),
		eq(`SELECT id, placed_at::date FROM ` + o),
		eq(`SELECT min(seen_at), max(seen_at) FROM ` + o),
		// JSON, arrays, bytes, uuid.
		eq(`SELECT id, attrs->>'k' FROM ` + o),
		refused("@>(VARCHAR, STRING_LITERAL)", otherTypes, `SELECT id FROM `+o+` WHERE attrs @> '{"k": "v"}'`),
		still("rows", "", otherTypes, `SELECT id, labels[1] FROM `+o),
		refused("UNNEST not supported here", otherTypes, `SELECT id FROM `+o+` WHERE 'red' = ANY(labels)`),
		refused("length(BLOB)", dialect, `SELECT id, length(payload) FROM `+o),
		eq(`SELECT id FROM ` + o + ` WHERE tag = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'`),
		// Shapes.
		eq(`WITH big AS (SELECT * FROM ` + o + ` WHERE qty >= 10) SELECT count(*) FROM big`),
		eq(`SELECT id, row_number() OVER (ORDER BY id) FROM ` + o),
		eq(`SELECT id FROM ` + o + ` WHERE customer_id IN (SELECT id FROM ` + c + ` WHERE active)`),
		eq(`SELECT * FROM ` + c),
		// A numeric declared without precision is an exact DECIMAL(38,18) on
		// the copy: it sorts, sums and compares as on the source.
		eq(`SELECT id FROM ` + odd + ` WHERE fixed > 2 ORDER BY fixed`),
		eq(`SELECT sum(fixed) FROM ` + odd),
		eq(`SELECT id, fixed FROM ` + odd),
		// It prints with 18 decimals, where the source prints the digits
		// the value was written with.
		still("precision", "", "#2768: a numeric without precision prints with the copy's 18 decimals", `SELECT free FROM `+odd+` WHERE id = 3`),
		// A value the copy's type cannot hold fails the statement that reads
		// it, and only that one.
		refused("NaN", "by design: NaN is not a DECIMAL", `SELECT free FROM `+odd+` WHERE id = 1`),
		refused("more than 18 decimals", "by design: never rounded", `SELECT free FROM `+odd+` WHERE id = 2`),
		// Which of the two values stops it first is the engine's choice.
		refused("", "by design: a statement that reads the whole column meets those values", `SELECT id FROM `+odd+` WHERE free > 12345`),
		// Not compared.
		{stmt: `SELECT * FROM pgcmp_missing`, verdict: sqlcompare.SourceError},
		{stmt: `DELETE FROM ` + o, verdict: sqlcompare.Skipped, kind: "not_a_query"},
		{stmt: `SELECT pg_advisory_lock(2772)`, verdict: sqlcompare.Skipped, kind: "not_read_only"},
	}
}
