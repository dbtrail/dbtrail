package baseline

import (
	"context"
	"path/filepath"
	"testing"
)

// #2235: the footer read hands DuckDB a list of paths, and each entry of a
// list is taken as a glob. A table named "order[st]" beside one named
// "orders" had its footer read from the other's file: it got no entry, and
// was reported as a file that carries no CREATE TABLE, which a caller keeps
// as a fact about the file and never asks again. Every table gets its own.
func TestReadTableFooters_aTableNamedLikeAGlob(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "snap", "shop")
	var paths []string
	for _, name := range []string{"order[st]", "orders", "or?ers", "or*s", "{orders,x}", "other"} {
		p := filepath.Join(dir, name+".parquet")
		writeFixtureTable(t, p, footerTestSQL, [][]string{{"1", "AB"}})
		paths = append(paths, p)
	}
	for _, list := range [][]string{paths, paths[:1]} {
		got, err := ReadTableFooters(context.Background(), list)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.NoSchema) != 0 || len(got.Unread) != 0 {
			t.Errorf("%d files: no schema = %v, unread = %v, want every file read", len(list), got.NoSchema, got.Unread)
		}
		for _, p := range list {
			if _, ok := got.Footers[p]; !ok {
				t.Errorf("%d files: no footer for %s", len(list), filepath.Base(p))
			}
		}
	}
}
