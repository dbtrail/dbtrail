package console

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// #2235: a view names its table's file so that the path matches itself as a
// glob, which rewrites a directory named "sh[o]p" as "sh[[]o]p". The SQL
// sandbox allows a statement its directories by their text, so the list the
// console hands it has to hold the directory the way the view spells it too:
// without that, every table under a directory with a glob character was
// refused, where it was read before the paths were escaped. And the second
// spelling is that directory, not a pattern: a sibling it would match as one
// stays out of reach.
func TestSQLCopyDirs_allowWhatTheViewsRead(t *testing.T) {
	for _, schema := range []string{"shop", "sh[o]p", "sh{o}p", "s?op", "sh*p"} {
		t.Run(schema, func(t *testing.T) {
			root := t.TempDir()
			plain := writeSQLBaselineFixture(t, root) // <root>/<snapshot>/shop
			dir := filepath.Join(filepath.Dir(plain), schema)
			if schema != "shop" {
				// The schema with the odd name, and "shop" left beside it as
				// the sibling its name would match as a pattern.
				if err := os.Mkdir(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				raw, err := os.ReadFile(filepath.Join(plain, "orders.parquet"))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "orders.parquet"), raw, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			in := views.Input{
				GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
				BaselineSource: root, Follow: views.FollowNone, OmitEvents: true,
				Baselines: []views.BaselineTable{{Schema: schema, Table: "orders", Path: filepath.Join(dir, "orders.parquet"), SchemaKnown: true}},
			}
			ddb, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatal(err)
			}
			defer ddb.Close()
			ddb.SetMaxOpenConns(1)
			if _, err := ddb.Exec(views.Generate(in)); err != nil {
				t.Fatalf("the views do not load: %v", err)
			}
			allowed := "["
			for i, d := range sqlCopyDirs(in) {
				if i > 0 {
					allowed += ", "
				}
				allowed += "'" + d + "'"
			}
			allowed += "]"
			for _, stmt := range []string{"SET allowed_directories = " + allowed, "SET enable_external_access = false"} {
				if _, err := ddb.Exec(stmt); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}
			var n int
			if err := ddb.QueryRow(`SELECT count(*) FROM "` + schema + `".orders`).Scan(&n); err != nil || n != 2 {
				t.Fatalf("the table under %q is read as %d rows (err=%v), want its 2", schema, n, err)
			}
			if schema == "shop" {
				return
			}
			var m int
			err = ddb.QueryRow("SELECT count(*) FROM read_parquet('" + filepath.Join(plain, "orders.parquet") + "')").Scan(&m)
			if err == nil {
				t.Fatalf("the sibling directory %s is readable: the list %s allows more than the views read", plain, allowed)
			}
		})
	}
}
