package views

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// After a refresh a table's view is no longer `SELECT * FROM read_parquet`:
// it is the base file minus its dead rows plus the newest image of every
// changed row (baseline.TableDeltaStateSQL). The _bin column's collation has
// to hold on that shape too, for rows that come from the base file and rows
// that come from the chain alike (#2083 review). This runs the generated SQL;
// the string alone would not show which rows compare how.
func TestStateView_binaryCollationThroughADeltaChain(t *testing.T) {
	f := newFx1918(t, "codes", "")
	const stamp = "2026-04-30T01-00-00Z"
	base := f.publish(stamp, row1918{1, "AB"}, row1918{2, "ab"}, row1918{3, "zz"})
	// The chain: row number 2 (id 3) is dead, ids 4 and 5 are new.
	f.pair(base, 0, nil, nil)
	f.pair(base, 1, []int64{2}, []row1918{{4, "Ab"}, {5, "AB"}})

	tables := []BaselineTable{{
		Schema: "shop", Table: f.table, Path: base, Rel: "shop/" + f.table + ".parquet",
		SchemaKnown: true, Decimals: []DecimalColumn{{Name: "amount", Precision: 10, Scale: 2}},
		BinaryText: []string{"status"},
	}}
	if err := MarkTableDeltas(context.Background(), tables); err != nil {
		t.Fatal(err)
	}
	if !tables[0].Delta {
		t.Fatal("the fixture's table has no delta: this test would run the plain view")
	}
	sqlText := Generate(Input{
		GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
		BaselineSource: f.root, BaselineSnapshot: time.Date(2026, 4, 30, 1, 0, 0, 0, time.UTC),
		Baselines: tables,
	})
	if !strings.Contains(sqlText, "bintrail_latest") || !strings.Contains(sqlText, `"status" COLLATE C AS "status"`) {
		t.Fatalf("the view is not the delta shape with the collation:\n%s", sqlText)
	}
	for _, collation := range []string{"nocase.icu_noaccent", "nocase.noaccent"} {
		t.Run(collation, func(t *testing.T) {
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			for _, s := range []string{"SET default_collation = '" + collation + "'", "SET default_null_order = 'nulls_first_on_asc_last_on_desc'", sqlText} {
				if _, err := db.Exec(s); err != nil {
					t.Fatalf("%v\n%s", err, s)
				}
			}
			for _, c := range []struct{ name, query, want string }{
				{"every live row, dead row gone", `SELECT id FROM shop.codes ORDER BY id`, "1,2,4,5"},
				{"= on a base row", `SELECT id FROM shop.codes WHERE status = 'ab' ORDER BY id`, "2"},
				{"= across base and chain", `SELECT id FROM shop.codes WHERE status = 'AB' ORDER BY id`, "1,5"},
				{"= on a chain row", `SELECT id FROM shop.codes WHERE status = 'Ab' ORDER BY id`, "4"},
				{"GROUP BY", `SELECT status || ':' || count(*) FROM shop.codes GROUP BY status ORDER BY status`, "AB:2,Ab:1,ab:1"},
				{"ORDER BY", `SELECT id FROM shop.codes ORDER BY status, id`, "1,5,4,2"},
				{"the decimal cast beside it", `SELECT CAST(sum(amount) AS VARCHAR) FROM shop.codes`, "14.00"},
			} {
				rows, err := db.Query(c.query)
				if err != nil {
					t.Fatalf("%s: %v", c.query, err)
				}
				var got []string
				for rows.Next() {
					var v sql.NullString
					if err := rows.Scan(&v); err != nil {
						t.Fatal(err)
					}
					got = append(got, v.String)
				}
				rows.Close()
				if j := strings.Join(got, ","); j != c.want {
					t.Errorf("%s: %s\n  got  %q\n  want %q", c.name, c.query, j, c.want)
				}
			}
		})
	}
}
