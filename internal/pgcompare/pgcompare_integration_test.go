//go:build integration

package pgcompare

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	itCustomers = "pgcmp_customers"
	itOrders    = "pgcmp_orders"
	itOdd       = "pgcmp_odd"
	itArrays    = "pgcmp_arrays"
	itTimes     = "pgcmp_times"
	itRound     = "pgcmp_round"
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
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+itOrders+", "+itCustomers+", "+itOdd+", "+itArrays+", "+itTimes+", "+itRound)
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
	// Measured as the PostgreSQL port runs it.
	cp.Rewrite = pgshim.RewriteForCopy
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

	checkRouting(t, ctx, cp, rep, want)

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
		`CREATE TABLE ` + itArrays + ` (id int PRIMARY KEY, tags text[], nums integer[], days date[], flags boolean[], grid integer[])`,
		`INSERT INTO ` + itArrays + ` VALUES
			(1, ARRAY['red', 'two words', 'a,b', 'q"x', 'back\\slash', '', 'NULL', 'null', NULL, 'it''s', '{x}'],
			    ARRAY[1, NULL, 7], ARRAY[DATE '2026-01-31', NULL], ARRAY[true, false, NULL], NULL),
			(2, '{}', '{}', '{}', '{}', NULL),
			(3, NULL, NULL, NULL, NULL, '{{1,2},{3,4}}')`,
		`CREATE TABLE ` + itTimes + ` (id int PRIMARY KEY, starts time, startz timetz, span interval, late time, latez timetz)`,
		`INSERT INTO ` + itTimes + ` VALUES
			(1, '10:00:00',        '10:00:00+00',    '1 year 2 mons 3 days 04:05:06.789',   NULL,       NULL),
			(2, '23:59:59.999999', '08:30:00-05:30', '-1 years -2 mons +3 days -04:05:06', NULL,       NULL),
			(3, '00:00:00',        '00:00:00+14',    '00:00:00',                            NULL,       NULL),
			(4, '12:00:00.5',      '12:00:00+00',    '-3 days +01:00:00',                   NULL,       NULL),
			(5, NULL,              NULL,             '100:00:00',                           '24:00:00', '24:00:00+00'),
			(6, '06:00:00',        '06:00:00+02',    '-1 mons',                             NULL,       NULL)`,
		`CREATE TABLE ` + itRound + ` (id int PRIMARY KEY, g int, n numeric(20,6), d numeric(12,3), i int)`,
		`INSERT INTO ` + itRound + ` VALUES ` + roundRows(),
		`CREATE PUBLICATION ` + itPub + ` FOR TABLE ` + itCustomers + `, ` + itOrders + `, ` + itOdd + `, ` + itArrays + `, ` + itTimes + `, ` + itRound,
	}
}

// roundRows are the rows round(a / b, k) and round(avg(x), k) are measured
// over (pgshim.RewriteForCopy): random numerators and divisors from a fixed
// seed, both signs, and quotients that lie exactly on a half at each k from
// 0 to 4, where rounding half away from zero and half to even part.
func roundRows() string {
	rng := rand.New(rand.NewSource(2961))
	var rows []string
	for id := 1; id <= 1500; id++ {
		n := rng.Int63n(2_000_000_000_000_00) - 1_000_000_000_000_00 // +/- 1e8, 6 decimals
		d := rng.Int63n(100_000_000) + 1                            // up to 1e5, 3 decimals
		if rng.Intn(2) == 0 {
			d = -d
		}
		i := rng.Intn(2000) - 1000
		if i == 0 {
			i = 7
		}
		rows = append(rows, fmt.Sprintf("(%d, %d, %s, %s, %d)", id, id%37, fixed(n, 6), fixed(d, 3), i))
	}
	id := 2000
	for k := 0; k <= 4; k++ {
		for _, m := range []int64{0, 1, 2, 7, 12, 99, 12345} {
			for _, div := range []int64{1, 3, 7, 12} {
				for _, sign := range []int64{1, -1} {
					// q = m + 5 / 10^(k+1), n = q * div: k+1 decimals.
					unit := int64(1)
					for range k + 1 {
						unit *= 10
					}
					q := sign * (m*unit + 5)
					id++
					rows = append(rows, fmt.Sprintf("(%d, %d, %s, %s, %d)", id, 100+k, fixed(q*div*1_000_000/unit, 6), fixed(div*1000, 3), div))
				}
			}
		}
	}
	return strings.Join(rows, ",\n")
}

// fixed writes v / 10^scale with exactly scale decimals.
func fixed(v int64, scale int) string {
	neg := v < 0
	if neg {
		v = -v
	}
	s := fmt.Sprintf("%0*d", scale+1, v)
	out := s[:len(s)-scale] + "." + s[len(s)-scale:]
	if neg {
		out = "-" + out
	}
	return out
}

func expectations() []expectation {
	o, c, odd, arr, tm := itOrders, itCustomers, itOdd, itArrays, itTimes
	eq := func(stmt string) expectation { return expectation{stmt: stmt, verdict: sqlcompare.Equal} }
	// What still differs, each with the issue it belongs to. still is a
	// statement the copy answers differently, refused one it cannot run.
	const (
		otherTypes = "#2770: json's operators belong to the source's dialect"
		dialect    = "#2770: the copy's dialect answers this differently; it belongs to the source"
		route      = "#2770: the PostgreSQL port sends this to the source"
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
		// Text with no column behind it compares byte by byte (#2786).
		eq(`SELECT 'a' = 'A' AS same, 'abc' LIKE 'ABC' AS alike, 'a' IN ('A', 'b') AS listed`),
		eq(`SELECT 7 / 2 AS cut, -7 / 2 AS cut_negative`),
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
		// NULL is the largest value, as on the source (#2786).
		eq(`SELECT id, qty FROM ` + o + ` ORDER BY qty, id`),
		eq(`SELECT id, qty FROM ` + o + ` ORDER BY qty DESC, id`),
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
		// Two integers divide to an integer, cut toward zero (#2786); with
		// a float on a side the fraction stays.
		eq(`SELECT id, qty / 2 AS half FROM ` + o),
		eq(`SELECT id, (0 - qty) / 2 AS half FROM ` + o),
		eq(`SELECT customer_id, sum(qty) / count(*) FROM ` + o + ` GROUP BY customer_id`),
		eq(`SELECT id, ratio / 2 FROM ` + o),
		// The source fails a division by zero. The copy would answer NULL
		// (sqlsandbox pins that); here the statement stops at the source.
		{stmt: `SELECT id, qty / 0 FROM ` + o, verdict: sqlcompare.SourceError},
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
		// An array is a list on the copy (#2787): an element by its
		// position, ANY, and the array itself, written as the source does.
		eq(`SELECT id, labels[1] FROM ` + o),
		eq(`SELECT id, labels[2] FROM ` + o),
		eq(`SELECT id FROM ` + o + ` WHERE 'red' = ANY(labels)`),
		eq(`SELECT id, labels FROM ` + o + ` WHERE labels IS NOT NULL`),
		eq(`SELECT id FROM ` + o + ` WHERE labels IS NULL`),
		eq(`SELECT id, tags, nums, days, flags FROM ` + arr),
		eq(`SELECT id, nums[1], tags[3], flags[2] FROM ` + arr),
		eq(`SELECT id FROM ` + arr + ` WHERE 7 = ANY(nums)`),
		eq(`SELECT id FROM ` + arr + ` WHERE 'two words' = ANY(tags)`),
		// Times and intervals (#2787).
		eq(`SELECT id, starts, startz, span FROM ` + tm),
		eq(`SELECT id FROM ` + tm + ` WHERE starts < TIME '12:00:00'`),
		eq(`SELECT id FROM ` + tm + ` ORDER BY starts NULLS LAST, id`),
		eq(`SELECT id FROM ` + tm + ` WHERE span > INTERVAL '1 day'`),
		eq(`SELECT id FROM ` + tm + ` WHERE span < INTERVAL '0 seconds'`),
		eq(`SELECT id FROM ` + tm + ` ORDER BY span, id`),
		eq(`SELECT id, DATE '2026-01-31' + span FROM ` + tm + ` WHERE id IN (1, 6)`),
		eq(`SELECT min(starts), max(starts) FROM ` + tm),
		// 24:00:00 is a time on the source and not on the copy.
		refused("24:00:00", "by design: never read as midnight", `SELECT late FROM `+tm),
		eq(`SELECT id, latez FROM ` + tm),
		// More than one dimension does not fit a list: the statement that
		// reads the column fails, and one that does not is answered.
		refused("more than one dimension", "by design: never read as something else", `SELECT grid FROM `+arr),
		eq(`SELECT id FROM ` + arr + ` ORDER BY id`),
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
		// the value was written with: the copy does not keep the scale each
		// value was written with, and trimming the zeros would print 1.50 as
		// 1.5. The PostgreSQL port writes it so too (#2768).
		still("precision", "", "by design: a numeric without precision prints with the copy's 18 decimals", `SELECT free FROM `+odd+` WHERE id = 3`),
		// A value the copy's type cannot hold fails the statement that reads
		// it, and only that one.
		refused("NaN", "by design: NaN is not a DECIMAL", `SELECT free FROM `+odd+` WHERE id = 1`),
		refused("more than 18 decimals", "by design: never rounded", `SELECT free FROM `+odd+` WHERE id = 2`),
		// Which of the two values stops it first is the engine's choice.
		refused("", "by design: a statement that reads the whole column meets those values", `SELECT id FROM `+odd+` WHERE free > 12345`),
		// Measured for the PostgreSQL port's per-statement routing (#2770):
		// what the port lets the copy answer is built from these lines, and
		// every line that differs here is one it sends to the source
		// (TestIntegrationPGCompare checks that below).
		//
		// Text by order: the source orders it by its collation (en_US here),
		// the copy by bytes. name > 'B' happens to agree on these rows.
		still("order", "", route, `SELECT name FROM `+c+` ORDER BY name`),
		still("rows", "", route, `SELECT min(name), max(name) FROM `+c),
		eq(`SELECT name FROM ` + c + ` WHERE name > 'B'`),
		still("rows", "", route, `SELECT 'a' < 'B' AS lt`),
		eq(`SELECT upper('é'), lower('É')`),
		eq(`SELECT lower(name) FROM ` + c),
		{stmt: `SELECT id, amount / qty FROM ` + o + ` WHERE id = 6`, verdict: sqlcompare.SourceError},
		still("precision", "", route, `SELECT avg(qty) FROM `+o),
		still("rows", "", route, `SELECT id FROM `+o+` WHERE status ~ '^p'`),
		still("rows", "", route, `SELECT id FROM `+o+` WHERE status SIMILAR TO 'p%'`),
		refused("syntax error", route, `SELECT id FROM `+o+` WHERE attrs ? 'n'`),
		still("rows", "", route, `SELECT id, attrs->'k' FROM `+o),
		refused("syntax error", route, `SELECT id, attrs #>> '{k}' FROM `+o),
		eq(`SELECT id, qty % 3 FROM ` + o),
		eq(`SELECT id, -qty % 3 FROM ` + o),
		eq(`SELECT id, abs(qty) FROM ` + o),
		still("rows", "", route, `SELECT id, round(ratio) FROM `+o),
		eq(`SELECT id, round(amount, 1) FROM ` + o),
		// round(a / b, k) and round(avg(x), k), as the port writes them for
		// the copy (pgshim.RewriteForCopy): PostgreSQL's numeric quotient,
		// rounded half away from zero to exactly k decimals.
		eq(`SELECT id, round(amount / 3, 2) FROM ` + o + ` ORDER BY id`),
		eq(`SELECT customer_id, round(100.0 * sum(qty) / count(*), 2) AS pct FROM ` + o + ` GROUP BY customer_id ORDER BY customer_id`),
		eq(`SELECT customer_id, round(avg(amount), 2), round(avg(qty), 3), round(100 * avg(amount), 1) FROM ` + o + ` GROUP BY customer_id ORDER BY customer_id`),
		eq(`SELECT round(sum(amount) / nullif(sum(qty), 0), 4) FROM ` + o),
		eq(`SELECT id, round(100.0 * amount / nullif(sum(amount) OVER (), 0), 2) FROM ` + o + ` ORDER BY id`),
		eq(`SELECT round(avg(amount), 2), round(sum(amount) / nullif(count(*), 0), 2) FROM ` + o + ` WHERE id < 0`),
		eq(`SELECT round(1.0 * count(*) / 7, 0), round(-1.0 * count(*) / 8, 1) FROM ` + o),
		// Over 1,500 random quotients of both signs, and quotients exactly
		// on a half at each k.
		eq(`SELECT id, round(n / d, 0), round(n / d, 1), round(n / d, 2), round(n / d, 3) FROM ` + itRound + ` ORDER BY id`),
		eq(`SELECT id, round(n / d, 4), round(n / d, 5), round(n / d, 6), round(n / i, 2) FROM ` + itRound + ` ORDER BY id`),
		eq(`SELECT id, round(n / i, 0), round(n / i, 4), round(100.0 * d / i, 3) FROM ` + itRound + ` ORDER BY id`),
		eq(`SELECT g, round(avg(n), 2), round(avg(d), 4), round(avg(i), 3), round(100 * avg(d), 2), round(1000.0 * avg(i), 1) FROM ` + itRound + ` GROUP BY g ORDER BY g`),
		// What the copy cannot round as PostgreSQL does it refuses, and
		// never answers otherwise.
		{stmt: `SELECT id, round(amount / qty, 2) FROM ` + o, verdict: sqlcompare.SourceError},
		{stmt: `SELECT id, round(ratio / 2, 1) FROM ` + o, verdict: sqlcompare.SourceError},
		refused("both sides of the division are whole numbers", "by design: PostgreSQL divides them as whole numbers", `SELECT id, round(qty / 2, 1) FROM `+o),
		refused("too close to a half", "by design: PostgreSQL may round it twice", `SELECT round(0.004999999999999999999 / 1, 2)`),
		{stmt: `SELECT id, ratio::int FROM ` + o, verdict: sqlcompare.SourceError},
		eq(`SELECT id, amount::int FROM ` + o),
		eq(`SELECT id, qty::bigint FROM ` + o),
		eq(`SELECT id, qty::text FROM ` + o),
		still("precision", "", route, `SELECT id, ratio::text FROM `+o),
		eq(`SELECT id, placed_at::text FROM ` + o),
		eq(`SELECT id, CAST(qty AS bigint) FROM ` + o),
		eq(`SELECT id, CAST(placed_at AS date) FROM ` + o),
		eq(`SELECT id, nullif(qty, 0) FROM ` + o),
		eq(`SELECT id, CASE WHEN qty > 5 THEN 'big' ELSE 'small' END FROM ` + o),
		eq(`SELECT id FROM ` + o + ` WHERE qty IN (2, 9, 10)`),
		eq(`SELECT id FROM ` + o + ` WHERE customer_id NOT IN (1, 2)`),
		eq(`SELECT id FROM ` + o + ` WHERE note IS DISTINCT FROM 'x'`),
		eq(`SELECT id FROM ` + o + ` ORDER BY id LIMIT 3 OFFSET 2`),
		eq(`SELECT customer_id, count(*) FROM ` + o + ` GROUP BY customer_id HAVING count(*) > 2`),
		eq(`SELECT id FROM ` + o + ` WHERE id < 3 UNION SELECT id FROM ` + c),
		eq(`SELECT id FROM ` + o + ` WHERE id < 3 UNION ALL SELECT id FROM ` + c),
		eq(`SELECT id FROM ` + o + ` INTERSECT SELECT id FROM ` + c),
		eq(`SELECT id FROM ` + o + ` EXCEPT SELECT id FROM ` + c),
		eq(`SELECT c.id FROM ` + c + ` c WHERE EXISTS (SELECT 1 FROM ` + o + ` o WHERE o.customer_id = c.id)`),
		eq(`SELECT count(DISTINCT status) FROM ` + o),
		eq(`SELECT count(*) FILTER (WHERE qty > 5) FROM ` + o),
		eq(`SELECT DISTINCT ON (customer_id) customer_id, id FROM ` + o + ` ORDER BY customer_id, id`),
		eq(`SELECT id, substring(note, 1, 2) FROM ` + o),
		eq(`SELECT id, trim(note) FROM ` + o),
		eq(`SELECT id, coalesce(qty, 0) + 1 FROM ` + o),
		eq(`SELECT id, greatest(qty, 5), least(qty, 5) FROM ` + o),
		eq(`SELECT id, date_part('year', placed_at) FROM ` + o),
		eq(`SELECT id, seen_at AT TIME ZONE 'America/Bogota' FROM ` + o),
		refused("to_char", route, `SELECT id, to_char(placed_at, 'YYYY-MM') FROM `+o),
		eq(`SELECT id, concat(status, '-', qty) FROM ` + o),
		eq(`SELECT id, status || qty FROM ` + o),
		eq(`SELECT string_agg(status, ',' ORDER BY id) FROM ` + o),
		eq(`SELECT id, sum(qty) OVER (PARTITION BY customer_id ORDER BY id) FROM ` + o),
		eq(`SELECT id, rank() OVER (ORDER BY qty) FROM ` + o),
		eq(`SELECT id, lag(qty) OVER (ORDER BY id) FROM ` + o),
		eq(`SELECT id, ratio * 2.5 FROM ` + o),
		eq(`SELECT id, amount * 1.5 FROM ` + o),
		still("precision", "", route, `SELECT id, amount / 2 FROM `+o),
		still("precision", "", route, `SELECT id, qty / 2.0 FROM `+o),
		eq(`SELECT sum(ratio) FROM ` + o),
		still("precision", "", route, `SELECT sum(qty)::numeric / 3 FROM `+o),
		still("precision", "", route, `SELECT stddev(qty) FROM `+o),
		eq(`SELECT bool_and(active) FROM ` + c),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at > now() - interval '100 years'`),
		eq(`SELECT current_date > DATE '2000-01-01'`),
		eq(`SELECT id, labels FROM ` + o + ` ORDER BY labels NULLS LAST, id`),
		eq(`SELECT id FROM ` + o + ` ORDER BY tag NULLS LAST, id`),
		eq(`SELECT id, tag FROM ` + o + ` WHERE tag > 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'`),
		eq(`SELECT id, note COLLATE "C" FROM ` + o + ` ORDER BY note COLLATE "C" NULLS LAST, id`),
		eq(`SELECT id FROM ` + o + ` WHERE note ILIKE 'F%'`),
		eq(`SELECT id, length(note) FROM ` + o),
		eq(`SELECT id, position('i' in note) FROM ` + o),
		still("rows", "", route, `SELECT generate_series(1, 3)`),
		eq(`SELECT id, payload FROM ` + o + ` ORDER BY payload NULLS LAST, id`),
		still("precision", "", route, `SELECT id, fixed / 3 FROM `+odd),
		// Not compared.
		{stmt: `SELECT * FROM pgcmp_missing`, verdict: sqlcompare.SourceError},
		{stmt: `DELETE FROM ` + o, verdict: sqlcompare.Skipped, kind: "not_a_query"},
		{stmt: `SELECT pg_advisory_lock(2772)`, verdict: sqlcompare.Skipped, kind: "not_read_only"},
	}
}
