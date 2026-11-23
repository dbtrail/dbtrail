//go:build integration

package pgcompare

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #3073: the type the PostgreSQL port announces for each column of an
// answer, and the bytes it sends for each cell, against PostgreSQL itself.
// The other lists of this package compare values as text read from the
// copy's engine; this one connects to the port as a client does and compares
// what travels: each column's name, type and size in RowDescription and each
// cell's bytes, on the simple protocol, on the extended protocol in text and
// in binary, and for a prepared statement described before it runs.

const (
	tyVals = "pgty_vals"
	tyPub  = "pgty_pub"
	tySlot = "pgty_slot"
	tyPass = "pgty-port-password"
)

func tySeed() []string {
	return []string{
		`CREATE TABLE ` + tyVals + ` (id int PRIMARY KEY, s smallint, i int, b bigint, n numeric(12,2),
			r real, f double precision, d date, e date, ts timestamp, tz timestamptz, t text, by bytea)`,
		// i sums past an integer, b past nothing; no s, i or b overflows its
		// own type with the small literals the statements below add.
		`INSERT INTO ` + tyVals + ` VALUES
			(1, 1,     10,         100,                 1.50,  1.5,   2.5,  '2026-01-31', '2025-12-25', '2026-01-02 03:04:05.5',      '2026-01-02 03:04:05.5+00',      'año nuevo', '\x00ff'),
			(2, 32000, 2000000000, 9000000000000000000, -2.25, 0.1,   0.1,  '2024-02-29', '2024-03-01', '2024-02-29 23:59:59.999999', '2024-02-29 23:59:59.999999+00', '',          '\x'),
			(3, -5,    2000000000, 5,                   0.00,  -3.25, 1e20, '1999-12-31', '2000-01-01', '1999-12-31 00:00:00',        '1999-12-31 00:00:00+00',        'banana',    NULL),
			(4, NULL,  NULL,       NULL,                NULL,  NULL,  NULL, NULL,         NULL,         NULL,                         NULL,                            NULL,        NULL)`,
		`CREATE PUBLICATION ` + tyPub + ` FOR TABLE ` + tyVals,
	}
}

// tyList is a list of expressions read in one statement, each a column of
// its own: SELECT <exprs> FROM pgty_vals <tail>.
type tyList struct {
	tail  string
	exprs []string
}

// tySame are the expressions the port announces and sends as PostgreSQL
// does. The comments name what the port announced before #3073. Arithmetic
// and sum() over a real are not here: their value differs too (#3069).
func tySame() []tyList {
	return []tyList{
		{"", []string{
			// sum of a smallint or an integer is a bigint, of a bigint a
			// numeric: the port said numeric for all three.
			"sum(s)", "sum(i)", "sum(b)", "sum(n)", "sum(f)",
			"SUM(i)", "sum(DISTINCT i)", "sum(i) FILTER (WHERE id > 1)", "sum(i) FILTER (WHERE id > 99)", "sum(" + tyVals + ".i)", "sum(i) AS total",
			"sum(i) + 1", "coalesce(sum(i), 0)", "sum(i + 1)", "sum(i - s)", "sum(-i)", "sum(abs(s))",
			"sum(CASE WHEN id > 1 THEN 1 ELSE 0 END)", "sum(i::bigint)", "sum(s::int)", "sum(length(t))", "sum(2)",
			"count(*)", "count(i)", "min(s)", "max(i)", "max(b)", "min(n)", "max(f)", "min(d)", "max(t)", "bool_and(id > 0)",
		}},
		{"ORDER BY id", []string{
			// length, position and ntile are integers: the port said bigint.
			"length(t)", "LENGTH(t)", "char_length(t)", "character_length(t)", "position('a' in t)", "strpos(t, 'a')",
			"length(t) + 1", "length(t) AS n", "coalesce(length(t), 0)", "length(t) * b",
			"ntile(2) OVER (ORDER BY id)", "ntile(3) OVER (PARTITION BY id % 2 ORDER BY id) AS bucket",
			"row_number() OVER (ORDER BY id)", "rank() OVER (ORDER BY id)", "dense_rank() OVER (ORDER BY id)",
			"count(*) OVER ()", "lag(i) OVER (ORDER BY id)",
			"sum(s) OVER (ORDER BY id)", "sum(i) OVER (ORDER BY id)", "sum(b) OVER (ORDER BY id)", "sum(i) OVER ()",
			"sum(n) OVER (ORDER BY id)", "abs(i)", "abs(s)", "abs(n)",
		}},
		{"ORDER BY id", []string{
			// extract is a numeric and date_part a double precision: the port
			// said bigint for both.
			"extract(year FROM ts)", "extract(month FROM d)", "extract(day FROM tz)", "EXTRACT(YEAR FROM d)",
			"extract(hour FROM ts)", "extract(minute FROM ts)", "extract(quarter FROM d)", "extract(dow FROM d)",
			"extract(doy FROM d)", "extract(week FROM d)", "extract(isodow FROM d)", "extract(isoyear FROM d)",
			"extract(decade FROM d)", "extract(century FROM d)", "extract(millennium FROM d)",
			"extract(year FROM d) AS y",
			"date_part('year', ts)", "date_part('month', d)", "date_part('day', tz)", "DATE_PART('hour', ts)",
			"date_part('minute', ts)", "date_part('quarter', d)", "date_part('dow', d)", "date_part('doy', d)",
			"date_part('week', d)", "date_part('month', d)::text", "extract(month FROM d)::text", "lpad(date_part('month', d)::text, 2, '0')",
			"date_trunc('month', ts)", "date_trunc('day', ts)",
		}},
		{"ORDER BY id", []string{
			"id", "s", "i", "b", "n", "r", "f", "d", "ts", "tz", "t", "by",
			"i + 1", "i - s", "s - s", "b + 1", "i + b", "-i", "n + 1", "n * 2", "f + 1", "f * r", "i + 1.5",
			"d + 1", "d - 1", "id > 2", "t || 'x'", "coalesce(t, 'none')", "nullif(i, 10)",
			"CASE WHEN id > 2 THEN i ELSE 0 END", "i::bigint", "s::int", "i::text", "n::int",
			"1", "1.5", "'a'", "true", "NULL::int", "1 + 1", "3000000000", "DATE '2026-01-01'",
		}},
	}
}

// tyStatements are whole statements: the same expressions under an alias,
// in a subquery, in a WITH, behind a star, and a lone NULL.
func tyStatements() []string {
	v := tyVals
	return []string{
		`SELECT * FROM (SELECT sum(i) AS total, sum(b) AS wide, count(*) AS n FROM ` + v + `) q`,
		`SELECT total + 1, total FROM (SELECT sum(i) AS total FROM ` + v + `) q`,
		`WITH c AS (SELECT id % 2 AS g, sum(s) AS small, sum(i) AS total FROM ` + v + ` GROUP BY 1) SELECT g, small, total FROM c ORDER BY g`,
		`SELECT * FROM (SELECT id, length(t) AS l, position('a' in t) AS p, ntile(2) OVER (ORDER BY id) AS q FROM ` + v + `) x ORDER BY id`,
		`SELECT l + 1, m FROM (SELECT id, length(t) AS l, position('a' in t) m FROM ` + v + `) x ORDER BY id`,
		// The headings: beside a star, and of a subquery's columns read
		// through one, an item with no name is headed by its function.
		`SELECT *, length(t), sum(i) OVER (), extract(year FROM d), date_part('month', d), NULL FROM ` + v + ` ORDER BY id`,
		`SELECT * FROM (SELECT id, length(t), ntile(2) OVER (ORDER BY id), sum(i) OVER (ORDER BY id), strpos(t, 'a') FROM ` + v + `) q ORDER BY id`,
		`SELECT * FROM (SELECT length(t), count(*), sum(s) FROM ` + v + ` GROUP BY length(t)) q ORDER BY 1`,
		// What the engine reads again stays as it was: these answered before.
		`SELECT DISTINCT date_part('year', d) FROM ` + v + ` ORDER BY date_part('year', d)`,
		`SELECT d - extract(dow FROM d) * interval '1 day' FROM ` + v + ` ORDER BY id`,
		`SELECT max(l), min(l) + 1 FROM (SELECT length(t) AS l FROM ` + v + `) x`,
		`SELECT id FROM ` + v + ` WHERE length(t) > 2 AND extract(year FROM d) > 2000 ORDER BY length(t), id`,
		`SELECT length(t), count(*), sum(i) FROM ` + v + ` GROUP BY length(t) HAVING sum(i) > 0 ORDER BY 1`,
		// A lone NULL is text.
		`SELECT NULL`,
		`SELECT NULL, 1, NULL AS nothing, NULL AS more`,
		`SELECT id, NULL FROM ` + v + ` ORDER BY id`,
		// Beside another SELECT it takes the other side's type.
		`SELECT NULL UNION ALL SELECT 1`,
		`SELECT 1 UNION ALL SELECT NULL`,
	}
}

// tyOther is a statement one of whose columns the port still announces with
// another type than PostgreSQL's, with the same value: docs/postgres.md
// lists them. col counts from 0.
type tyOther struct {
	sql      string
	col      int
	pg, port uint32
	why      string
}

// tyForwarded are statements RewriteForCopy writes macros into, for a
// connection that can reach its source: the casts go around what it wrote,
// and the values stay PostgreSQL's.
func tyForwarded() []string {
	v := tyVals
	return []string{
		`SELECT round(sum(n), 2), round(avg(n), 2), sum(i), sum(s), length(max(t)) FROM ` + v,
		`SELECT id, extract(epoch from ts), extract(year from ts), floor(extract(epoch from ts) / 600), date_part('month', d), length(t) FROM ` + v + ` ORDER BY id`,
		`SELECT id % 2, coalesce(round(avg(n), 2), 0.00), sum(i), count(*) FROM ` + v + ` GROUP BY 1 ORDER BY 1`,
		`SELECT id, round(n, 1), sum(i) OVER (ORDER BY id), ntile(2) OVER (ORDER BY id), NULL FROM ` + v + ` ORDER BY id`,
	}
}

// tyForwardedPlain is a statement that calls a function of the table inside
// one of those macros and names it in GROUP BY: it is cast in neither place,
// so the two stay one expression.
func tyForwardedPlain() []string {
	return []string{
		`SELECT length(t), round(100.0 * count(*) / length(t), 2) FROM ` + tyVals + ` WHERE length(t) > 0 GROUP BY length(t) ORDER BY 1`,
	}
}

func tyDifferent() []tyOther {
	v := tyVals
	return []tyOther{
		{`SELECT d - e FROM ` + v + ` ORDER BY id`, 0, 23, 20, "a date less a date is a bigint on the copy"},
		{`SELECT s + 1 FROM ` + v + ` ORDER BY id`, 0, 23, 21, "a smallint with a literal stays a smallint on the copy"},
		{`SELECT sum(l) FROM (SELECT length(t) AS l FROM ` + v + `) x`, 0, 20, 1700, "the sum of a name the statement defines: its type is not read"},
		{`SELECT sum(id) FROM (SELECT i AS id FROM ` + v + `) x`, 0, 20, 1700, "the same, under the name of a column"},
		{`SELECT x FROM (SELECT NULL AS x) q`, 0, 25, 23, "a NULL named in a subquery"},
		{`SELECT extract(year FROM d) + 1 FROM ` + v + ` ORDER BY id`, 0, 1700, 20, "extract inside an expression: cast, a function that takes a bigint would refuse it"},
		{`SELECT y FROM (SELECT id, extract(year FROM d) AS y FROM ` + v + `) q ORDER BY id`, 0, 1700, 20, "extract in a subquery, for the same reason"},
		{`SELECT a FROM (SELECT id, length(t) AS a, length(t) + 1 FROM ` + v + `) q ORDER BY id`, 0, 23, 20, "a subquery calls the function in an item with no name, whose heading must stay"},
		{`SELECT date_part('year', d) + 1 FROM ` + v + ` ORDER BY id`, 0, 701, 20, "date_part inside an expression: as a double precision the engine would write it 2026.0 where it is read as text"},
		{`SELECT y FROM (SELECT id, date_part('year', d) AS y FROM ` + v + `) q ORDER BY id`, 0, 701, 20, "date_part in a subquery, for the same reason"},
		{`SELECT NULL, NULL`, 1, 25, 23, "a second NULL with no name"},
	}
}

var tyTypeNames = map[uint32]string{
	16: "boolean", 17: "bytea", 20: "bigint", 21: "smallint", 23: "integer", 25: "text", 700: "real",
	701: "double precision", 1082: "date", 1114: "timestamp", 1184: "timestamptz", 1700: "numeric", 705: "unknown",
}

func tyName(oid uint32) string {
	if n, ok := tyTypeNames[oid]; ok {
		return n
	}
	return "type " + strconv.FormatUint(uint64(oid), 10)
}

// tyAnswer is what one side sent for one statement in one mode.
type tyAnswer struct {
	fields []pgconn.FieldDescription
	rows   [][][]byte
	// described are the columns a Describe of the prepared statement
	// announced before it ran; nil in the modes that do not prepare.
	described []pgconn.FieldDescription
	err       error
}

var tyModes = []string{"simple", "extended text", "extended binary", "prepared binary"}

func tyAsk(ctx context.Context, pc *pgconn.PgConn, mode, sql string) tyAnswer {
	read := func(r *pgconn.Result) tyAnswer {
		return tyAnswer{fields: r.FieldDescriptions, rows: r.Rows, err: r.Err}
	}
	switch mode {
	case "simple":
		rs, err := pc.Exec(ctx, sql).ReadAll()
		if err != nil {
			return tyAnswer{err: err}
		}
		if len(rs) != 1 {
			return tyAnswer{err: fmt.Errorf("%d results", len(rs))}
		}
		return read(rs[0])
	case "extended text":
		return read(pc.ExecParams(ctx, sql, nil, nil, nil, []int16{0}).Read())
	case "extended binary":
		return read(pc.ExecParams(ctx, sql, nil, nil, nil, []int16{1}).Read())
	}
	sd, err := pc.Prepare(ctx, "", sql, nil)
	if err != nil {
		return tyAnswer{err: err}
	}
	a := read(pc.ExecPrepared(ctx, "", nil, nil, []int16{1}).Read())
	a.described = sd.Fields
	return a
}

// tyDiff is every way the port's answer is not PostgreSQL's, one line each.
func tyDiff(mode string, pg, port tyAnswer) []string {
	if pg.err != nil || port.err != nil {
		if pg.err != nil && port.err != nil {
			return nil
		}
		return []string{fmt.Sprintf("%s: PostgreSQL: %v; the port: %v", mode, pg.err, port.err)}
	}
	var out []string
	if len(pg.fields) != len(port.fields) || len(pg.rows) != len(port.rows) {
		return []string{fmt.Sprintf("%s: PostgreSQL %d columns, %d rows; the port %d columns, %d rows", mode, len(pg.fields), len(pg.rows), len(port.fields), len(port.rows))}
	}
	for i, f := range pg.fields {
		g := port.fields[i]
		// The type modifier too (numeric(18,0) is not numeric). But for a
		// numeric with decimals: an expression over one keeps its precision
		// and scale on the port where PostgreSQL announces none, which
		// docs/postgres.md says.
		modifier := f.TypeModifier != g.TypeModifier && !(f.DataTypeOID == 1700 && f.TypeModifier == -1 && (g.TypeModifier-4)&0xFFFF > 0)
		if modifier {
			out = append(out, fmt.Sprintf("%s: column %d (%s): PostgreSQL announces type modifier %d, the port %d", mode, i+1, f.Name, f.TypeModifier, g.TypeModifier))
		}
		if f.DataTypeOID != g.DataTypeOID || f.DataTypeSize != g.DataTypeSize || f.Format != g.Format {
			out = append(out, fmt.Sprintf("%s: column %d (%s): PostgreSQL announces %s (size %d, format %d), the port %s (size %d, format %d)",
				mode, i+1, f.Name, tyName(f.DataTypeOID), f.DataTypeSize, f.Format, tyName(g.DataTypeOID), g.DataTypeSize, g.Format))
		}
		if f.Name != g.Name {
			out = append(out, fmt.Sprintf("%s: column %d: PostgreSQL heads it %q, the port %q", mode, i+1, f.Name, g.Name))
		}
		if port.described != nil {
			if i >= len(port.described) || port.described[i].DataTypeOID != g.DataTypeOID {
				out = append(out, fmt.Sprintf("%s: column %d (%s): the port's Describe and its answer announce different types", mode, i+1, f.Name))
			}
			if i >= len(pg.described) || i < len(port.described) && pg.described[i].DataTypeOID != port.described[i].DataTypeOID {
				out = append(out, fmt.Sprintf("%s: column %d (%s): described as another type than PostgreSQL describes it", mode, i+1, f.Name))
			}
		}
		for r := range pg.rows {
			if a, b := pg.rows[r][i], port.rows[r][i]; !bytes.Equal(a, b) || (a == nil) != (b == nil) {
				out = append(out, fmt.Sprintf("%s: column %d (%s), row %d: PostgreSQL sends %q, the port %q", mode, i+1, f.Name, r+1, a, b))
			}
		}
	}
	return out
}

func TestIntegrationPGCompareAnnouncedTypes3073(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	dropAll := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+tyPub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+tyVals)
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", tySlot)
	}
	dropAll()
	t.Cleanup(dropAll)
	for _, s := range tySeed() {
		if _, err := setup.Exec(ctx, s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
	// The port answers in UTC; so does the source here.
	if _, err := setup.Exec(ctx, "SET TimeZone = 'UTC'"); err != nil {
		t.Fatal(err)
	}

	outDir := t.TempDir()
	repl := baseDSN + "?replication=database"
	if strings.Contains(baseDSN, "?") {
		repl = baseDSN + "&replication=database"
	}
	if _, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN: baseDSN, ReplDSN: repl, SlotName: tySlot, Publication: tyPub,
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

	// The port's catalog of the table, as the product reads it: the copy's
	// type of each column and the type the source declares for it.
	declared, err := setup.Query(ctx, `SELECT a.attname, a.atttypid::oid::int8, a.atttypmod FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		WHERE c.relname = '`+tyVals+`' AND c.relnamespace = 'public'::regnamespace AND a.attnum > 0 AND NOT a.attisdropped`)
	if err != nil {
		t.Fatalf("the source's column types: %v", err)
	}
	onSource := map[string][2]int64{}
	for declared.Next() {
		var name string
		var oid int64
		var mod int32
		if err := declared.Scan(&name, &oid, &mod); err != nil {
			t.Fatal(err)
		}
		onSource[name] = [2]int64{oid, int64(mod)}
	}
	if err := declared.Err(); err != nil {
		t.Fatal(err)
	}
	held, _, err := cp.Query(ctx, "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '"+tyVals+"' ORDER BY ordinal_position", 1000)
	if err != nil {
		t.Fatalf("the copy's column types: %v", err)
	}
	catalog := []pgshim.CatalogTable{{Schema: "public", Name: tyVals}}
	for _, r := range held.Rows {
		st, ok := onSource[*r[0]]
		if !ok {
			t.Fatalf("the copy holds column %s, which the source does not declare", *r[0])
		}
		catalog[0].Columns = append(catalog[0].Columns, pgshim.CatalogColumn{Name: *r[0], Type: *r[1], SourceType: uint32(st[0]), SourceTypeMod: int32(st[1])})
	}
	if len(catalog[0].Columns) != 13 {
		t.Fatalf("the copy's catalog has %d columns, want 13", len(catalog[0].Columns))
	}

	// The port, with the copy behind it and no forwarding account: the copy
	// answers every statement, so nothing here is PostgreSQL answering itself.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, stop := context.WithCancel(context.Background())
	served := make(chan struct{})
	login := func(_ context.Context, _, _ string, password *pgshim.Password, _ func()) (*pgshim.Binding, *pgshim.Error) {
		if !password.Matches([]string{tyPass}) {
			return nil, &pgshim.Error{Code: "28P01", Message: "no"}
		}
		return &pgshim.Binding{
			Actor: "server:pgty", Schema: "public",
			Run: func(ctx context.Context, statement string, opts pgshim.RunOptions) (sqlsandbox.Result, error) {
				schema := opts.Schema
				if schema == "" {
					schema = "public"
				}
				return runner.Run(ctx, sqlsandbox.Job{
					CopyDirs: []string{cp.dir}, ViewsSQL: cp.viewsSQL, SQL: statement, Schema: schema,
					Session: sqlsandbox.Session{PostgresRules: true, PostgresNames: true, TimeZone: opts.TimeZone},
					Limits:  sqlsandbox.Limits{MaxRows: 1000},
				})
			},
			Catalog: func(context.Context) ([]pgshim.CatalogTable, error) { return catalog, nil },
		}, nil
	}
	go func() {
		defer close(served)
		_ = pgshim.Serve(serveCtx, ln, pgshim.Config{Login: login, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	}()
	t.Cleanup(func() { stop(); _ = ln.Close(); <-served })
	port, err := pgx.Connect(ctx, fmt.Sprintf("postgres://app:%s@%s/app?sslmode=disable", tyPass, ln.Addr()))
	if err != nil {
		t.Fatalf("connect to the port: %v", err)
	}
	t.Cleanup(func() { port.Close(context.Background()) })

	// compare runs one statement on both sides in every mode.
	compare := func(sql string) []string {
		var out []string
		for _, mode := range tyModes {
			out = append(out, tyDiff(mode, tyAsk(ctx, setup.PgConn(), mode, sql), tyAsk(ctx, port.PgConn(), mode, sql))...)
		}
		return out
	}
	// answered fails the test when PostgreSQL itself refuses a statement of
	// the list: a statement both sides refuse compares nothing.
	answered := func(sql string) {
		t.Helper()
		if a := tyAsk(ctx, setup.PgConn(), "extended binary", sql); a.err != nil || len(a.rows) == 0 {
			t.Errorf("PostgreSQL does not answer %s with rows: %v", sql, a.err)
		}
	}

	n := 0
	for _, l := range tySame() {
		sql := "SELECT " + strings.Join(l.exprs, ", ") + " FROM " + tyVals + " " + l.tail
		answered(sql)
		n += len(l.exprs)
		if len(compare(sql)) == 0 {
			continue
		}
		// One at a time, to name the expression.
		for _, e := range l.exprs {
			one := "SELECT " + e + " FROM " + tyVals + " " + l.tail
			if diffs := compare(one); len(diffs) > 0 {
				t.Errorf("%s\n    %s", e, strings.Join(diffs, "\n    "))
			}
		}
	}
	for _, sql := range tyStatements() {
		answered(sql)
		n++
		if diffs := compare(sql); len(diffs) > 0 {
			t.Errorf("%s\n    %s", sql, strings.Join(diffs, "\n    "))
		}
	}
	for _, o := range tyDifferent() {
		answered(o.sql)
		n++
		for _, mode := range tyModes {
			pg, pt := tyAsk(ctx, setup.PgConn(), mode, o.sql), tyAsk(ctx, port.PgConn(), mode, o.sql)
			if pg.err != nil || pt.err != nil || len(pg.fields) <= o.col || len(pt.fields) != len(pg.fields) || len(pt.rows) != len(pg.rows) {
				t.Errorf("%s, %s: PostgreSQL: %d columns, %d rows, %v; the port: %d columns, %d rows, %v", o.sql, mode, len(pg.fields), len(pg.rows), pg.err, len(pt.fields), len(pt.rows), pt.err)
				continue
			}
			if got, is := pt.fields[o.col].DataTypeOID, pg.fields[o.col].DataTypeOID; is != o.pg || got != o.port {
				t.Errorf("%s, %s: PostgreSQL announces %s and the port %s; listed as %s and %s (%s). If the port now agrees, move it to tyStatements and take it out of docs/postgres.md",
					o.sql, mode, tyName(is), tyName(got), tyName(o.pg), tyName(o.port), o.why)
			}
			if pt.described != nil && pt.described[o.col].DataTypeOID != pt.fields[o.col].DataTypeOID {
				t.Errorf("%s: the port's Describe and its answer announce different types", o.sql)
			}
			if mode != "simple" {
				continue
			}
			// The value is the same: as text, cell by cell.
			for r := range pg.rows {
				if a, b := pg.rows[r][o.col], pt.rows[r][o.col]; !bytes.Equal(a, b) || (a == nil) != (b == nil) {
					t.Errorf("%s, row %d: PostgreSQL writes %q and the port %q", o.sql, r+1, a, b)
				}
			}
		}
	}
	// As a connection that can reach its source hands them to the copy.
	types := map[string][]string{}
	for _, c := range catalog[0].Columns {
		types[strings.ToLower(c.Name)] = []string{strings.ToUpper(c.Type)}
	}
	cp.Rewrite = func(s string) string { return pgshim.AsPostgresTypes(pgshim.RewriteForCopy(s), types) }
	src, err := OpenSource(ctx, baseDSN, "public")
	if err != nil {
		t.Fatalf("open the source: %v", err)
	}
	t.Cleanup(func() { src.Close(context.Background()) })
	rep, err := Run(ctx, src, cp, Options{NoRerun: true, MaxRows: 100}, append(tyForwarded(), tyForwardedPlain()...))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for i, r := range rep.Results {
		if written := cp.Rewrite(r.Statement); r.Verdict != sqlcompare.Equal || strings.Contains(written, "CAST(") != (i < len(tyForwarded())) || !strings.Contains(written, "_bintrail_pg_") {
			t.Errorf("want EQUAL with a macro written in (and a cast, but for the last), got %s %s: %s\n    detail: %s\n    written: %s", r.Verdict, r.Kind, r.Statement, r.Detail, written)
		}
	}

	t.Logf("%d expressions and statements compared in %d modes", n, len(tyModes))
}
