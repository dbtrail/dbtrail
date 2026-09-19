package baseline

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// #1723: a MINOR compaction merges pairs lo..hi of a chain into one RANGE
// pair named "<stem>.<lo>-<hi>.<suffix>". The layout rules below are what
// every listing, the state SQL and the refresh's carry-forward rely on.

func TestParseTableDeltaRange(t *testing.T) {
	cases := []struct {
		name       string
		wantStem   string
		lo, hi     int
		wantSuffix string
		ok         bool
	}{
		{"orders.000001-000006.posdel", "orders", 1, 6, TableDeltaPosdelSuffix, true},
		{"orders.000001-000006.upserts", "orders", 1, 6, TableDeltaUpsertsSuffix, true},
		{"orders.000000-000006.upserts", "orders", 0, 6, TableDeltaUpsertsSuffix, true}, // the empty seq 0 absorbed
		{"orders.000007.posdel", "orders", 7, 7, TableDeltaPosdelSuffix, true},          // a plain pair: lo == hi
		{"orders.posdel", "orders", TableDeltaLegacySeq, TableDeltaLegacySeq, TableDeltaPosdelSuffix, true},
		{"t_202601-202602.000003-000004.posdel", "t_202601-202602", 3, 4, TableDeltaPosdelSuffix, true},
		// Not a range: lo == hi spelled as a range, lo > hi, a width that is
		// not six, a missing half. These are table names, like any other
		// unparseable middle segment (the v0.83.0 shape).
		{"orders.000003-000003.posdel", "orders.000003-000003", TableDeltaLegacySeq, TableDeltaLegacySeq, TableDeltaPosdelSuffix, true},
		{"orders.000006-000001.posdel", "orders.000006-000001", TableDeltaLegacySeq, TableDeltaLegacySeq, TableDeltaPosdelSuffix, true},
		{"orders.00001-000006.posdel", "orders.00001-000006", TableDeltaLegacySeq, TableDeltaLegacySeq, TableDeltaPosdelSuffix, true},
		{"orders.000001-.posdel", "orders.000001-", TableDeltaLegacySeq, TableDeltaLegacySeq, TableDeltaPosdelSuffix, true},
		{"orders.parquet", "", 0, 0, "", false},
	}
	for _, c := range cases {
		stem, lo, hi, suffix, ok := ParseTableDeltaRange(c.name)
		if stem != c.wantStem || lo != c.lo || hi != c.hi || suffix != c.wantSuffix || ok != c.ok {
			t.Errorf("ParseTableDeltaRange(%q) = %q,%d,%d,%q,%v want %q,%d,%d,%q,%v", c.name, stem, lo, hi, suffix, ok, c.wantStem, c.lo, c.hi, c.wantSuffix, c.ok)
		}
		// The single-sequence parser reports the range's HIGH end: that is
		// the sequence the chain has reached through it.
		if _, seq, _, ok2 := ParseTableDeltaName(c.name); ok2 != c.ok || (ok && seq != c.hi) {
			t.Errorf("ParseTableDeltaName(%q) = seq %d ok %v, want %d %v", c.name, seq, ok2, c.hi, c.ok)
		}
	}
}

func TestTableDeltaRangePaths(t *testing.T) {
	posdel, upserts := TableDeltaRangePaths("/snap/shop/orders.parquet", 1, 6)
	if posdel != "/snap/shop/orders.000001-000006.posdel" || upserts != "/snap/shop/orders.000001-000006.upserts" {
		t.Fatalf("range paths = %q %q", posdel, upserts)
	}
	// A file's own paths, plain or range, through one helper.
	f := TableDeltaFile{SeqLo: 1, Seq: 6}
	if p, _ := f.PathsUnder("/new/shop/orders.parquet"); p != "/new/shop/orders.000001-000006.posdel" {
		t.Fatalf("PathsUnder(range) = %q", p)
	}
	f = TableDeltaFile{SeqLo: 7, Seq: 7}
	if p, _ := f.PathsUnder("/new/shop/orders.parquet"); p != "/new/shop/orders.000007.posdel" {
		t.Fatalf("PathsUnder(plain) = %q", p)
	}
}

// The glob admits every "<stem>.<6 digits>..." file; the name filter keeps
// exactly the plain and range pairs of THIS table, and the state SQL orders
// a range by its low end among plain names. Executed against DuckDB: a
// wrong filter would read another table's pairs into this one's state
// without an error.
func TestTableDeltaNameFilter(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	write := func(name string, pos int) {
		if _, err := db.Exec(fmt.Sprintf("COPY (SELECT %d AS pos) TO '%s' (FORMAT PARQUET)", pos, filepath.Join(dir, name))); err != nil {
			t.Fatal(err)
		}
	}
	write("orders.000000.posdel", 0)
	write("orders.000001-000006.posdel", 16)
	write("orders.000007.posdel", 7)
	write("orders.000001x.posdel", 99)        // a table named orders.000001x
	write("orders.000001x.000000.posdel", 98) // its own chain
	write("orders.000001-000006x.posdel", 97) // a table named orders.000001-000006x
	write("orders_archive.000001-000002.posdel", 96)
	posdel, _ := TableDeltaGlobs(filepath.Join(dir, "orders.parquet"))
	lit := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	q := "SELECT filename, pos FROM read_parquet(" + lit(posdel) + ", filename=true) WHERE " +
		TableDeltaNameFilter(filepath.Join(dir, "orders.parquet"), TableDeltaPosdelSuffix) + " ORDER BY filename DESC"
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var f string
		var pos int
		if err := rows.Scan(&f, &pos); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%s=%d", filepath.Base(f), pos))
	}
	want := []string{"orders.000007.posdel=7", "orders.000001-000006.posdel=16", "orders.000000.posdel=0"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("filtered read = %v, want exactly the chain, newest first", got)
	}
	// A relative glob from inside the directory gives bare file names: the
	// filter must still keep the chain (a "/" anchor would drop every pair
	// and read the base alone, with no error).
	wd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })
	rel, _ := TableDeltaGlobs("orders.parquet")
	var n int
	if err := db.QueryRow("SELECT count(*) FROM read_parquet(" + lit(rel) + ", filename=true) WHERE " +
		TableDeltaNameFilter("orders.parquet", TableDeltaPosdelSuffix)).Scan(&n); err != nil || n != 3 {
		t.Fatalf("relative read = %d rows err=%v, want the three pairs", n, err)
	}
	// A stem with regexp metacharacters is quoted, and the filter is on the
	// file NAME only (the directory is the glob's business).
	f := TableDeltaNameFilter("/snap/dir/a.b+c.parquet", TableDeltaUpsertsSuffix)
	if !strings.Contains(f, `(^|[/\\])a\.b\+c\.`) || strings.Contains(f, "snap") {
		t.Fatalf("filter = %s", f)
	}
	if f := TableDeltaNameFilter("/snap/it's.parquet", TableDeltaPosdelSuffix); !strings.Contains(f, "it''s") {
		t.Fatalf("a quote in the stem is not doubled for the SQL literal: %s", f)
	}
}

// Ranges tile the chain: the first starts at 0, each starts right after the
// previous one ends, no gap and no overlap; a range may absorb sequence 0.
func TestMarkTableDeltaFiles_ranges(t *testing.T) {
	pair := func(stem string) []string { return []string{stem + ".posdel", stem + ".upserts"} }
	files := func(stems ...string) []string {
		out := []string{"orders.parquet"}
		for _, s := range stems {
			out = append(out, pair("orders."+s)...)
		}
		return out
	}
	good := map[string][]string{
		"range then plain":       files("000000", "000001-000006", "000007"),
		"range absorbing seq 0":  files("000000-000006", "000007", "000008"),
		"two ranges":             files("000000", "000001-000003", "000004-000006"),
		"range alone":            files("000000-000003"),
		"plain chain, unchanged": files("000000", "000001", "000002"),
	}
	for name, list := range good {
		chains, err := MarkTableDeltaFiles("/snap/shop", list)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		c := chains["/snap/shop/orders.parquet"]
		if c == nil {
			t.Fatalf("%s: no chain", name)
		}
		for i, f := range c.Files {
			if i == 0 && f.SeqLo != 0 {
				t.Fatalf("%s: first file starts at %d", name, f.SeqLo)
			}
			if i > 0 && f.SeqLo != c.Files[i-1].Seq+1 {
				t.Fatalf("%s: file %d = %+v does not follow %+v", name, i, f, c.Files[i-1])
			}
		}
	}
	bad := map[string][]string{
		"gap after a range":            files("000000", "000001-000006", "000008"),
		"gap before a range":           files("000000", "000002-000006"),
		"overlapping ranges":           files("000000", "000001-000006", "000005-000008"),
		"plain inside a range":         files("000000", "000001-000006", "000006"),
		"plain inside a range, listed": files("000000", "000003", "000001-000006", "000007"),
		"range without seq 0":          files("000001-000006", "000007"),
		"range missing its upserts":    append(files("000000", "000007"), "orders.000001-000006.posdel"),
	}
	for name, list := range bad {
		_, err := MarkTableDeltaFiles("/snap/shop", list)
		if !errors.Is(err, ErrHalfTableDelta) {
			t.Fatalf("%s: err = %v, want the chain refused", name, err)
		}
	}
	// The last file of a chain that ends in a range reports the range's high
	// end as the sequence to resume from.
	chains, _ := MarkTableDeltaFiles("/snap/shop", files("000000", "000001-000006"))
	if last := chains["/snap/shop/orders.parquet"].Last(); last.Seq != 6 || last.SeqLo != 1 {
		t.Fatalf("Last = %+v", last)
	}
}

func TestWithDeltaSeqRange(t *testing.T) {
	md := WithDeltaSeqRange(map[string]string{"k": "v"}, 1, 6)
	if md[MetaKeyDeltaSeq] != "6" || md[MetaKeyDeltaSeqLo] != "1" || md["k"] != "v" {
		t.Fatalf("md = %v", md)
	}
	if plain := WithDeltaSeq(nil, 7); plain[MetaKeyDeltaSeq] != "7" || plain[MetaKeyDeltaSeqLo] != "" {
		t.Fatalf("plain md = %v, want no low end", plain)
	}
}
