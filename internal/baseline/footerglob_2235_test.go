package baseline

import (
	"context"
	"path/filepath"
	"slices"
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

// A backslash is the one character no class stands for: DuckDB's glob splits
// a pattern on it. A table named `\..\hr\*` in "shop" must not have the
// footers of the schema "hr" read as its own, and the tables beside it keep
// theirs.
func TestReadTableFooters_aBackslashNameReadsNoOtherSchema(t *testing.T) {
	root := filepath.Join(t.TempDir(), "snap")
	hr := filepath.Join(root, "hr", "salaries.parquet")
	writeFixtureTable(t, hr, footerTestSQL, [][]string{{"1", "AB"}})
	orders := filepath.Join(root, "shop", "orders.parquet")
	writeFixtureTable(t, orders, footerTestSQL, [][]string{{"1", "AB"}})
	for _, name := range []string{`\..\hr\*`, `\..\hr\salaries`, `a\b`, `a\*b`} {
		t.Run(name, func(t *testing.T) {
			own := filepath.Join(root, "shop", name+".parquet")
			writeFixtureTable(t, own, footerTestSQL, [][]string{{"1", "AB"}})
			got, err := ReadTableFooters(context.Background(), []string{orders, own})
			if err != nil {
				t.Fatal(err)
			}
			// Its own footer, or said to be unread: never another file's,
			// and never "carries no CREATE TABLE", which a caller keeps.
			if _, ok := got.Footers[own]; !ok && !slices.Contains(got.Unread, own) {
				t.Errorf("%s: no footer and not reported unread (no schema = %v)", name, got.NoSchema)
			}
			if _, ok := got.Footers[hr]; ok {
				t.Errorf("the footer of %s was read: nobody asked for it", hr)
			}
			if _, ok := got.Footers[orders]; !ok {
				t.Errorf("no footer for shop.orders, the table beside it")
			}
		})
	}
}
