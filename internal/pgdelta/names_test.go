package pgdelta

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestPaths(t *testing.T) {
	dead, rows, err := Paths("/snap/2026-11-15T10-00-00Z/public/orders.parquet", 3)
	if err != nil || dead != "/snap/2026-11-15T10-00-00Z/public/orders.000003.pgdead" || rows != "/snap/2026-11-15T10-00-00Z/public/orders.000003.pgnew" {
		t.Fatalf("Paths = %q, %q, %v", dead, rows, err)
	}
	if dead, rows, err = Paths("s3://b/k/public/a.b.parquet", 0); err != nil || dead != "s3://b/k/public/a.b.000000.pgdead" || rows != "s3://b/k/public/a.b.000000.pgnew" {
		t.Fatalf("Paths over S3, a table with a dot = %q, %q, %v", dead, rows, err)
	}
	if _, rows, err = Paths("orders.parquet", MaxSeq); err != nil || rows != "orders.999999.pgnew" {
		t.Fatalf("the last sequence = %q, %v", rows, err)
	}
	for _, bad := range []struct {
		base string
		seq  int
	}{{"orders.parquet", -1}, {"orders.parquet", MaxSeq + 1}, {"orders", 0}, {"orders.000000.pgnew", 0}, {"", 0}} {
		if _, _, err := Paths(bad.base, bad.seq); err == nil {
			t.Errorf("Paths(%q, %d) gave no error", bad.base, bad.seq)
		}
	}
	// What Paths writes, ParseName reads back.
	dead, rows, _ = Paths("/x/My.Table.parquet", 42)
	for path, wantDead := range map[string]bool{dead: true, rows: false} {
		n, ok := ParseName(path[strings.LastIndexByte(path, '/')+1:])
		if !ok || n.Table != "My.Table" || n.Seq != 42 || n.Dead != wantDead {
			t.Errorf("ParseName of %s = %+v, %v", path, n, ok)
		}
	}
}

func TestParseName(t *testing.T) {
	good := map[string]Name{
		"orders.000000.pgnew":        {Table: "orders", Seq: 0},
		"orders.000003.pgdead":       {Table: "orders", Seq: 3, Dead: true},
		"orders.999999.pgnew":        {Table: "orders", Seq: 999999},
		"a.b.000001.pgnew":           {Table: "a.b", Seq: 1},
		"orders.000003.000004.pgnew": {Table: "orders.000003", Seq: 4},
		"Órdenes ñ.000002.pgdead":    {Table: "Órdenes ñ", Seq: 2, Dead: true},
		".x.000002.pgnew":            {Table: ".x", Seq: 2},
	}
	for name, want := range good {
		if got, ok := ParseName(name); !ok || got != want {
			t.Errorf("ParseName(%q) = %+v, %v; want %+v", name, got, ok, want)
		}
	}
	for _, name := range []string{
		"", "orders.parquet", "orders.pgnew", "orders.000003", ".000003.pgnew", "000003.pgnew",
		// The MySQL delta's files, and its range pair.
		"orders.000003.upserts", "orders.000003.posdel", "orders.000001-000004.upserts",
		// Not six digits, a sign, a letter, a range.
		"orders.00003.pgnew", "orders.0000003.pgnew", "orders.-00003.pgnew", "orders.+00003.pgnew",
		"orders.00000a.pgnew", "orders.000001-000004.pgnew", "orders.٠٠٠٠٠٣.pgnew",
		// A longer or different suffix.
		"orders.000003.pgnew.tmp", "orders.000003.pgnewx", "orders.000003.PGNEW", "orders.000003.xpgnew", "orders.000003.pgdeads",
	} {
		if got, ok := ParseName(name); ok {
			t.Errorf("ParseName(%q) = %+v, want it refused", name, got)
		}
	}
}

func TestChainOf(t *testing.T) {
	pair := func(s string) []string { return []string{"orders." + s + ".pgdead", "orders." + s + ".pgnew"} }
	join := func(parts ...[]string) (out []string) {
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	others := []string{"orders.parquet", "orders.keyidx", "orders_old.000000.pgnew", "orders_old.000000.pgdead",
		"orders.eu.000000.pgnew", "orders.eu.000000.pgdead", "orders.000000.upserts", "orders.000000.posdel", "items.parquet", "_SUCCESS"}

	c, err := ChainOf("orders", others)
	if err != nil || c.Table != "orders" || len(c.Pairs) != 0 {
		t.Fatalf("no delta file: %+v, %v", c, err)
	}
	// Whatever order the folder lists them in.
	c, err = ChainOf("orders", join(pair("000002"), others, pair("000000"), pair("000001")))
	want := []Pair{
		{Seq: 0, Dead: "orders.000000.pgdead", New: "orders.000000.pgnew"},
		{Seq: 1, Dead: "orders.000001.pgdead", New: "orders.000001.pgnew"},
		{Seq: 2, Dead: "orders.000002.pgdead", New: "orders.000002.pgnew"},
	}
	if err != nil || !reflect.DeepEqual(c.Pairs, want) {
		t.Fatalf("three pairs: %+v, %v", c.Pairs, err)
	}
	if c, err = ChainOf("orders.eu", others); err != nil || len(c.Pairs) != 1 || c.Pairs[0].New != "orders.eu.000000.pgnew" {
		t.Fatalf("the table with a dot in its name: %+v, %v", c, err)
	}

	// A path or a bucket key in place of a file's name: an error, never a
	// chain that reads as empty.
	for _, withFolder := range []string{"public/orders.000000.pgnew", `public\orders.000000.pgdead`, "s3://b/k/orders.000000.pgnew"} {
		if c, err := ChainOf("orders", append(pair("000000"), withFolder)); err == nil || errors.Is(err, ErrBrokenChain) {
			t.Errorf("a name with a folder (%s): %+v, %v; want an error of its own", withFolder, c, err)
		}
	}

	for name, names := range map[string][]string{
		"half a pair, the rows":           {"orders.000000.pgnew"},
		"half a pair, the dead rows":      {"orders.000000.pgdead"},
		"half of a later pair":            join(pair("000000"), []string{"orders.000001.pgnew"}),
		"does not start at 0":             pair("000001"),
		"skips a number":                  join(pair("000000"), pair("000002")),
		"the same file of one pair twice": {"orders.000000.pgnew", "orders.000000.pgdead", "orders.000000.pgnew"},
	} {
		if c, err := ChainOf("orders", names); !errors.Is(err, ErrBrokenChain) {
			t.Errorf("%s: %+v, %v; want ErrBrokenChain", name, c, err)
		}
	}
}

func TestAnyNewGlob(t *testing.T) {
	for base, want := range map[string]string{
		"/snap/public/orders.parquet":         "/snap/public/orders.[0-9][0-9][0-9][0-9][0-9][0-9].pgnew",
		"s3://b/p/public/or*ders.parquet":     "s3://b/p/public/or[*]ders.[0-9][0-9][0-9][0-9][0-9][0-9].pgnew",
		"public/a[1]{x}?.parquet":             "public/a[[]1][{]x}[?].[0-9][0-9][0-9][0-9][0-9][0-9].pgnew",
	} {
		if got := AnyNewGlob(base); got != want {
			t.Errorf("AnyNewGlob(%q) = %q, want %q", base, got, want)
		}
	}
}
