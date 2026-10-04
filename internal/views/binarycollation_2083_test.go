package views

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// The REPLACE list gives a _bin column byte comparison, next to the decimal
// casts, and nothing when the table's schema was not read.
func TestReplaceClause_binaryCollation(t *testing.T) {
	got := replaceClause(BaselineTable{SchemaKnown: true,
		Decimals:   []DecimalColumn{{Name: "amount", Precision: 10, Scale: 2}},
		BinaryText: []string{"code", `we"ird`}})
	want := `CAST("amount" AS DECIMAL(10,2)) AS "amount", "code" COLLATE C AS "code", "we""ird" COLLATE C AS "we""ird"`
	if got != want {
		t.Errorf("replaceClause =\n  %s\nwant\n  %s", got, want)
	}
	if got := replaceClause(BaselineTable{BinaryText: []string{"code"}}); got != "" {
		t.Errorf("replaceClause without a known schema = %q, want nothing", got)
	}
	if got := replaceClause(BaselineTable{SchemaKnown: true}); got != "" {
		t.Errorf("replaceClause with nothing to re-type = %q, want nothing", got)
	}
}

// ApplyFooters carries the footer's _bin columns to the table.
func TestApplyFooters_binaryText(t *testing.T) {
	in := Input{Baselines: []BaselineTable{{Path: "a.parquet"}, {Path: "b.parquet"}}}
	in.ApplyFooters(map[string]baseline.TableFooter{"a.parquet": {BinaryText: []string{"code"}}})
	if got := in.Baselines[0].BinaryText; len(got) != 1 || got[0] != "code" || !in.Baselines[0].SchemaKnown {
		t.Errorf("table a: BinaryText = %q, SchemaKnown = %v", got, in.Baselines[0].SchemaKnown)
	}
	if in.Baselines[1].BinaryText != nil || in.Baselines[1].SchemaKnown {
		t.Errorf("table b had no footer and got %+v", in.Baselines[1])
	}
}

// A column MySQL declares _bin compares byte by byte in its state view, under
// a session that folds case and accents everywhere else (#2083). The copy's
// SQL session folds case and accents to match MySQL's default collation,
// and that made `code = 'ab'` match 'AB' and 'Ab' on a column where MySQL
// matches one row.
//
// This has to run the SQL through the real footer: what is under test is that
// the collation written in the CREATE TABLE reaches the comparison.
func TestStateView_binaryCollationColumnComparesBytes(t *testing.T) {
	root := t.TempDir()
	createSQL := "CREATE TABLE `codes` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `code` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n" +
		"  `label` varchar(16) DEFAULT NULL,\n" +
		"  `amount` decimal(10,2) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "2026-04-30T03-00-00Z", "shop", "codes.parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][]string{
		{"1", "AB", "ab", "1.00"}, {"2", "ab", "AB", "2.00"}, {"3", "Ab", "x", "3.00"},
		{"4", "zz", "X", "4.00"}, {"5", "é", "e", "5.00"}, {"6", "", "", ""},
	} {
		nulls := make([]bool, len(r))
		if r[0] == "6" {
			nulls[1], nulls[2], nulls[3] = true, true, true
		}
		if err := w.WriteRow(r, nulls); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	in := Input{
		GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
		BaselineSource: root, BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Baselines: []BaselineTable{{Schema: "shop", Table: "codes", Path: path}},
	}
	footers, err := baseline.TableFootersFor(context.Background(), in.BaselinePaths())
	if err != nil {
		t.Fatal(err)
	}
	in.ApplyFooters(footers)
	sqlText := Generate(in)

	if !strings.Contains(sqlText, `"code" COLLATE C AS "code"`) || !strings.Contains(sqlText, "-- Text columns MySQL declares under a _bin collation") {
		t.Errorf("the generated file must carry the collation and say why:\n%s", sqlText)
	}
	plain := Generate(Input{GeneratedAt: in.GeneratedAt, Version: "test", BaselineSource: root, BaselineSnapshot: in.BaselineSnapshot,
		Baselines: []BaselineTable{{Schema: "shop", Table: "codes", Path: path, SchemaKnown: true}}})
	if strings.Contains(plain, "COLLATE C AS") || strings.Contains(plain, "_bin collation") {
		t.Errorf("a table with no _bin column got a collation or its note:\n%s", plain)
	}

	// The copy's session defaults (sqlsandbox lock-down), and the built-in
	// folding collation it used before: COLLATE C on the column outranks
	// both.
	for _, collation := range []string{"nocase.icu_noaccent", "nocase.noaccent"} {
		t.Run(collation, func(t *testing.T) { binaryCollationCases(t, sqlText, collation) })
	}
}

func binaryCollationCases(t *testing.T, sqlText, collation string) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, s := range []string{"SET default_collation = '" + collation + "'", "SET default_null_order = 'nulls_first_on_asc_last_on_desc'"} {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(sqlText); err != nil {
		t.Fatalf("DuckDB rejected the generated views:\n%v\n\n--- generated ---\n%s", err, sqlText)
	}
	cases := []struct{ name, query, want string }{
		{"= matches the exact bytes", `SELECT id FROM shop.codes WHERE code = 'ab' ORDER BY id`, "2"},
		{"= on the folded column still folds", `SELECT id FROM shop.codes WHERE label = 'ab' ORDER BY id`, "1,2"},
		{"accents are bytes too", `SELECT id FROM shop.codes WHERE code = 'e' ORDER BY id`, ""},
		{"the folded column still folds accents", `SELECT id FROM shop.codes WHERE label = 'é' ORDER BY id`, "5"},
		{"IN", `SELECT id FROM shop.codes WHERE code IN ('ab', 'AB') ORDER BY id`, "1,2"},
		{"<>", `SELECT id FROM shop.codes WHERE code <> 'ab' ORDER BY id`, "1,3,4,5"},
		{"GROUP BY keeps three spellings apart", `SELECT count(*) FROM (SELECT code FROM shop.codes GROUP BY code)`, "6"},
		{"DISTINCT", `SELECT count(*) FROM (SELECT DISTINCT code FROM shop.codes)`, "6"},
		{"count(DISTINCT)", `SELECT count(DISTINCT code) FROM shop.codes`, "5"},
		{"ORDER BY is code point order, NULL first", `SELECT coalesce(code, 'NULL') FROM shop.codes ORDER BY code`, "NULL,AB,Ab,ab,zz,é"},
		{"ORDER BY DESC, NULL last", `SELECT coalesce(code, 'NULL') FROM shop.codes ORDER BY code DESC`, "é,zz,ab,Ab,AB,NULL"},
		{"min and max", `SELECT min(code) || ',' || max(code) FROM shop.codes`, "AB,é"},
		{"joined to a folded column, bytes win", `SELECT a.id || '-' || b.id FROM shop.codes a JOIN shop.codes b ON a.code = b.label ORDER BY 1`, "1-2,2-1"},
		{"joined to itself", `SELECT count(*) FROM shop.codes a JOIN shop.codes b ON a.code = b.code`, "5"},
		{"through a CTE and a function", `WITH c AS (SELECT upper(code) AS u FROM shop.codes) SELECT count(*) FROM c WHERE u = 'ab'`, "0"},
		{"through a subquery", `SELECT count(*) FROM (SELECT code AS k FROM shop.codes) WHERE k = 'AB'`, "1"},
		{"UNION ALL with a folded column, then compared: bytes", `SELECT count(*) FROM (SELECT code AS k FROM shop.codes UNION ALL SELECT label FROM shop.codes) WHERE k = 'ab'`, "2"},
		{"COALESCE of a byte column and a folded one: bytes", `SELECT count(*) FROM shop.codes WHERE coalesce(code, label) = 'ab'`, "1"},
		{"CASE over both: bytes", `SELECT count(*) FROM shop.codes WHERE CASE WHEN id > 0 THEN code ELSE label END = 'AB'`, "1"},
		{"a folded column beside it is untouched by the byte column", `SELECT count(*) FROM shop.codes WHERE code = 'ab' AND label = 'ab'`, "1"},
		{"the decimal cast is still there", `SELECT CAST(sum(amount) AS VARCHAR) FROM shop.codes`, "15.00"},
		{"the column is still a VARCHAR", `SELECT lower(column_type) FROM (DESCRIBE SELECT code FROM shop.codes)`, "varchar"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rows, err := db.Query(c.query)
			if err != nil {
				t.Fatalf("%s: %v", c.query, err)
			}
			defer rows.Close()
			var got []string
			for rows.Next() {
				var v sql.NullString
				if err := rows.Scan(&v); err != nil {
					t.Fatal(err)
				}
				got = append(got, v.String)
			}
			if j := strings.Join(got, ","); j != c.want {
				t.Errorf("%s\n  got  %q\n  want %q", c.query, j, c.want)
			}
		})
	}
}

// A view that FOLLOWS later snapshots carries no column collation. DuckDB
// binds the REPLACE list against whichever file the view reads at query time,
// so a _bin column dropped or retyped at the source would fail every query on
// that table until the file was regenerated; and a following file is read by
// a DuckDB of the reader's own, whose default collation already compares
// bytes. The pinned views (what the copy's own SQL session runs) carry it.
func TestStateView_followingViewsCarryNoCollation(t *testing.T) {
	table := BaselineTable{Schema: "shop", Table: "codes", Path: "/snap/2026-04-30T03-00-00Z/shop/codes.parquet",
		Rel: "shop/codes.parquet", SchemaKnown: true, BinaryText: []string{"code"},
		Decimals: []DecimalColumn{{Name: "amount", Precision: 10, Scale: 2}}}
	for _, mode := range []FollowMode{FollowNone, FollowPointer, FollowNewest} {
		in := Input{GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
			BaselineSource: "/snap", BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
			Follow: mode, Baselines: []BaselineTable{table}}
		sqlText := Generate(in)
		has := strings.Contains(sqlText, `"code" COLLATE C AS "code"`) || strings.Contains(sqlText, "_bin collation")
		if want := mode == FollowNone; has != want {
			t.Errorf("follow mode %v: collation in the file = %v, want %v", mode, has, want)
		}
		if !strings.Contains(sqlText, `CAST("amount" AS DECIMAL(10,2))`) {
			t.Errorf("follow mode %v lost the decimal cast", mode)
		}
		if in.Baselines[0].BinaryText == nil {
			t.Errorf("follow mode %v: Generate changed the caller's tables", mode)
		}
	}
}
