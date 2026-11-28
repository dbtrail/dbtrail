//go:build integration

package pgbaseline_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/views"
)

const (
	typedSlot   = "bintrail_pgbaseline_it_typed_3315"
	typedPub    = "bintrail_pgbaseline_it_pub_typed_3315"
	typedTbl    = "pgbaseline_it_typed_3315"
	typedTblBC  = "pgbaseline_it_typed_bc_3315"
	typedTblNoK = "pgbaseline_it_typed_nokey_3315"
)

// A snapshot stores a table's whole numbers, oid, date, uuid and numeric
// columns as their type (#3315), and its views read exactly what they read
// from the same table written as text: the same columns, the same types,
// the same rows. A table holding a value its type does not store with the
// same text (a date before the common era) is written as text, and so is a
// table with no primary key, and every table with BINTRAIL_PG_TYPED_COPY=0.
func TestPGBaseline_TypedColumns(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	dropAll := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+typedPub)
		for _, tbl := range []string{typedTbl, typedTblBC, typedTblNoK} {
			_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		}
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", typedSlot)
	}
	dropAll()
	t.Cleanup(dropAll)
	exec := func(q string) {
		t.Helper()
		if _, err := setup.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(`CREATE TABLE ` + typedTbl + ` (id bigint, k text, a int2, b int4, c int8, o oid, d date, u uuid,
		n numeric(12,2), n0 numeric(15,0), w numeric(38,4), p numeric(2,2), nf numeric, "select" int4, "Mixed Case" date, t text,
		PRIMARY KEY (k, id))`)
	exec(`INSERT INTO ` + typedTbl + ` VALUES
		(1, 'a|b', 0, 0, 0, 0, '1970-01-01', '00000000-0000-0000-0000-000000000000', 0, 0, 0, 0.5, 1.5, 0, '2024-02-29', 'plain'),
		(2, 'back\slash', 32767, 2147483647, 9223372036854775807, 4294967295, '9999-12-31', 'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF', 9999999999.99, 999999999999999, 1234567890123456789012345678901234.5678, 0.99, 1e20, 7, '10000-01-01', 'x'),
		(3, '', -32768, -2147483648, -9223372036854775808, 16384, '0001-01-01', '12345678-9abc-def0-1234-56789abcdef0', -9999999999.99, -15, -0.0001, -0.01, -0.5, -7, 'infinity', ''),
		(4, 'n', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
		(5, 'inf', 1, 1, 1, 1, 'infinity', 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', 0.05, 1, 1.5, 0.01, 'NaN', 1, '-infinity', 'y'),
		(6, 'ninf', -1, -1, -1, 2, '-infinity', 'aaaaaaaa-bbbb-cccc-dddd-000000000000', -0.05, -1, 0, -0.01, 0, -1, '5874897-12-31', 'z')`)
	exec(`CREATE TABLE ` + typedTblBC + ` (id int PRIMARY KEY, d date, b int4)`)
	exec(`INSERT INTO ` + typedTblBC + ` VALUES (1, '2024-01-01', 1), (2, '0044-03-15 BC', 2)`)
	exec(`CREATE TABLE ` + typedTblNoK + ` (a int4, d date)`)
	exec(`ALTER TABLE ` + typedTblNoK + ` REPLICA IDENTITY FULL`)
	exec(`INSERT INTO ` + typedTblNoK + ` VALUES (1, '2024-01-01')`)
	exec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s, %s, %s", typedPub, typedTbl, typedTblBC, typedTblNoK))

	run := func(off bool) string {
		t.Helper()
		if off {
			t.Setenv(baseline.PGTypedDisableEnv, "0")
		} else {
			t.Setenv(baseline.PGTypedDisableEnv, "")
		}
		out := t.TempDir()
		if _, err := pgbaseline.Run(ctx, pgbaseline.Config{
			QueryDSN: baseDSN, ReplDSN: replDSN(baseDSN), SlotName: typedSlot, Publication: typedPub,
			OutputDir: out, Compression: "zstd", Parallelism: 2, RowGroupSize: 2,
		}); err != nil {
			t.Fatalf("pgbaseline.Run (typed off %v): %v", off, err)
		}
		return singleSnapshotDir(t, out)
	}
	typedDir, textDir := run(false), run(true)

	path := func(dir, tbl string) string { return filepath.Join(dir, "public", tbl+".parquet") }
	enc := readParquetMetadata(t, path(typedDir, typedTbl))[baseline.MetaKeyPGTypedColumns]
	got, err := baseline.ParsePGTypedColumns(enc)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"a": "SMALLINT", "b": "INTEGER", "c": "BIGINT", "o": "UINTEGER", "d": "DATE", "u": "UUID",
		"n": "DECIMAL(12,2)", "n0": "DECIMAL(15,0)", "w": "DECIMAL(38,4)", "select": "INTEGER", "Mixed Case": "DATE"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stored typed: %v, want %v", got, want)
	}
	for _, p := range []string{path(typedDir, typedTblBC), path(typedDir, typedTblNoK), path(textDir, typedTbl), path(textDir, typedTblBC)} {
		if v, ok := readParquetMetadata(t, p)[baseline.MetaKeyPGTypedColumns]; ok {
			t.Errorf("%s stores columns typed (%s), want every column as text", p, v)
		}
	}

	// The views of both snapshots, as the copy writes them for its own
	// session: the same columns, types and rows.
	for _, tbl := range []string{typedTbl, typedTblBC, typedTblNoK} {
		var shapes, rows [2][]string
		for i, dir := range []string{textDir, typedDir} {
			p := path(dir, tbl)
			footers, err := baseline.TableFootersFor(ctx, []string{p})
			if err != nil {
				t.Fatal(err)
			}
			in := views.Input{
				GeneratedAt: time.Now(), Version: "test", BaselineSource: dir,
				Baselines: []views.BaselineTable{{Schema: "public", Table: tbl, Path: p, Rel: "public/" + tbl + ".parquet"}},
			}
			in.ApplyFooters(footers)
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			sqlText := views.Generate(in)
			if _, err := db.Exec(sqlText); err != nil {
				t.Fatalf("%s: %v\n%s", tbl, err, sqlText)
			}
			// A NaN and a date before the common era fail the read of the
			// column that holds them, in both: those columns are left out.
			skip := map[string]bool{"nf": tbl == typedTbl, "d": tbl == typedTblBC}
			shapes[i], rows[i] = describeAndRead(t, db, tbl, skip)
			if tbl == typedTblBC {
				if _, err := db.Query(`SELECT d FROM public."` + tbl + `"`); err == nil || !strings.Contains(err.Error(), "before the common era") {
					t.Errorf("%s: reading a date before the common era: %v", tbl, err)
				}
			}
		}
		if !reflect.DeepEqual(shapes[0], shapes[1]) {
			t.Errorf("%s: the view's columns differ:\ntext  %v\ntyped %v", tbl, shapes[0], shapes[1])
		}
		if !reflect.DeepEqual(rows[0], rows[1]) {
			t.Errorf("%s: the view's rows differ:\ntext  %v\ntyped %v", tbl, rows[0], rows[1])
		}
	}
}

// describeAndRead is the view's columns with their types, and its rows,
// each value as its Go value and type, sorted, but the columns of skip.
func describeAndRead(t *testing.T, db *sql.DB, tbl string, skip map[string]bool) (shape, out []string) {
	t.Helper()
	q := `public."` + tbl + `"`
	rs, err := db.Query(`SELECT column_name, column_type FROM (DESCRIBE ` + q + `)`)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rs.Next() {
		var n, ty string
		if err := rs.Scan(&n, &ty); err != nil {
			t.Fatal(err)
		}
		shape = append(shape, n+" "+ty)
		if !skip[n] {
			names = append(names, `"`+strings.ReplaceAll(n, `"`, `""`)+`"`)
		}
	}
	rs.Close()
	rows, err := db.Query(`SELECT ` + strings.Join(names, ", ") + ` FROM ` + q)
	if err != nil {
		t.Fatalf("%s: %v", tbl, err)
	}
	defer rows.Close()
	for rows.Next() {
		vals := make([]any, len(names))
		ptrs := make([]any, len(names))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%#v", vals))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", tbl, err)
	}
	sort.Strings(out)
	return shape, out
}
