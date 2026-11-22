package pgdelta

import (
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// The patterns a following view reads a PostgreSQL table's chain through
// admit the table's own file (so they never match nothing), every link of
// the chain, one or many, and nothing else a snapshot folder holds: not the
// other half of a pair, not a MySQL delta file, not another table's files,
// also one whose name holds a pattern character.
//
// Matched by DuckDB's own glob over real files, not by a Go imitation of it.
func TestFollowGlobs(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"orders.parquet",
		"orders.000000.pgdead", "orders.000000.pgnew",
		"orders.000001.pgdead", "orders.000001.pgnew",
		"orders.000002.pgdead", "orders.000002.pgnew",
		"orders.000003.pgnew.tmp-12345",
		"orders.000000.posdel", "orders.000000.upserts",
		"orders_old.parquet", "orders_old.000000.pgnew", "orders_old.000000.pgdead",
		"orders.eu.parquet", "orders.eu.000000.pgnew", "orders.eu.000000.pgdead",
		"or*ders.parquet", "or*ders.000000.pgnew", "or*ders.000000.pgdead",
		"xorders.000000.pgnew",
	}
	for _, n := range names {
		if err := os.WriteFile(filepath.Join(dir, n), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	match := func(pattern string) []string {
		t.Helper()
		rows, err := db.Query("SELECT file FROM glob(" + quote(pattern) + ")")
		if err != nil {
			t.Fatalf("glob %s: %v", pattern, err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var f string
			if err := rows.Scan(&f); err != nil {
				t.Fatal(err)
			}
			out = append(out, filepath.Base(f))
		}
		sort.Strings(out)
		return out
	}
	cases := map[string]struct{ dead, rows []string }{
		"orders": {
			dead: []string{"orders.000000.pgdead", "orders.000001.pgdead", "orders.000002.pgdead", "orders.parquet"},
			rows: []string{"orders.000000.pgnew", "orders.000001.pgnew", "orders.000002.pgnew", "orders.parquet"},
		},
		"or*ders": {
			dead: []string{"or*ders.000000.pgdead", "or*ders.parquet"},
			rows: []string{"or*ders.000000.pgnew", "or*ders.parquet"},
		},
		"orders.eu": {
			dead: []string{"orders.eu.000000.pgdead", "orders.eu.parquet"},
			rows: []string{"orders.eu.000000.pgnew", "orders.eu.parquet"},
		},
		"orders_old": {
			dead: []string{"orders_old.000000.pgdead", "orders_old.parquet"},
			rows: []string{"orders_old.000000.pgnew", "orders_old.parquet"},
		},
	}
	for table, want := range cases {
		dead, rows := FollowGlobs(filepath.Join(dir, table+".parquet"))
		if got := match(dead); !reflect.DeepEqual(got, want.dead) {
			t.Errorf("%s: the dead pattern %s matches %v, want %v", table, dead, got, want.dead)
		}
		if got := match(rows); !reflect.DeepEqual(got, want.rows) {
			t.Errorf("%s: the rows pattern %s matches %v, want %v", table, rows, got, want.rows)
		}
	}
	// A table with no pair: each pattern still matches its file, so a read
	// through it binds.
	if err := os.WriteFile(filepath.Join(dir, "items.parquet"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	dead, rows := FollowGlobs(filepath.Join(dir, "items.parquet"))
	for _, p := range []string{dead, rows} {
		if got := match(p); !reflect.DeepEqual(got, []string{"items.parquet"}) {
			t.Errorf("a table with no pair: %s matches %v, want its file alone", p, got)
		}
	}
}

// The name filter keeps a link of this table's chain, whatever folder or
// spelling of the path is in front of it, and drops the base, the other
// half, a sibling table's links (also one whose name is this one's and a
// six-digit part), and a file still being written.
func TestFollowNameFilter(t *testing.T) {
	expr := followNameFilter("s3://b/snap/public/or.ders.parquet", NewSuffix)
	m := regexp.MustCompile(`'(.*)'\)$`).FindStringSubmatch(expr)
	if m == nil {
		t.Fatalf("filter %s", expr)
	}
	re := regexp.MustCompile(strings.ReplaceAll(m[1], "''", "'"))
	for name, want := range map[string]bool{
		"s3://b/snap/public/or.ders.000000.pgnew":        true,
		"public/or.ders.000017.pgnew":                    true,
		"or.ders.999999.pgnew":                           true,
		"s3://b/snap/public/or.ders.parquet":             false,
		"s3://b/snap/public/or.ders.000000.pgdead":       false,
		"s3://b/snap/public/orxders.000000.pgnew":        false,
		"s3://b/snap/public/xor.ders.000000.pgnew":       false,
		"s3://b/snap/public/or.ders.000003.000000.pgnew": false,
		"s3://b/snap/public/or.ders.0000001.pgnew":       false,
		"s3://b/snap/public/or.ders.000000.pgnew.tmp-1":  false,
	} {
		if got := re.MatchString(name); got != want {
			t.Errorf("%s: kept %v, want %v", name, got, want)
		}
	}
}
