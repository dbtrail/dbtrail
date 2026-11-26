package pgbaseline

import (
	"slices"
	"testing"
)

// The key index is built on the key in the table's order, the order a key
// is joined in: PRIMARY KEY (b, a) on a table (a, b, c) is the key "a|b".
// A key that names a column the file does not hold (a generated column is
// not copied) has no index.
func TestKeyInTableOrder(t *testing.T) {
	for _, c := range []struct {
		name string
		t    tableInfo
		want []string
	}{
		{"declared in another order", tableInfo{Columns: []string{"a", "b", "c"}, PrimaryKey: []string{"b", "a"}}, []string{"a", "b"}},
		{"one column", tableInfo{Columns: []string{"id", "v"}, PrimaryKey: []string{"id"}}, []string{"id"}},
		{"no key", tableInfo{Columns: []string{"id", "v"}}, nil},
		{"a column the file does not hold", tableInfo{Columns: []string{"a", "v"}, PrimaryKey: []string{"a", "gen"}}, nil},
		{"a column named twice", tableInfo{Columns: []string{"a", "v"}, PrimaryKey: []string{"a", "a"}}, nil},
	} {
		if got := keyInTableOrder(c.t); !slices.Equal(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}
