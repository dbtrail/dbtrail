package reconstruct

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/keyidx"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// Review round on #2496 slice 2 at the reconstruct site. The snapshot's
// schema lists the key in TABLE ordinal order (metadata.PKColumnMetas),
// the dump's PRIMARY KEY line in key order, and the Parquet file the
// baseline writer produces holds its columns in ALPHABETICAL order
// (baseline.sortColumnsForParquet) — three orders. The index takes the
// file's, whatever it is handed, so both sites write the same bytes.
func TestKeyIndex_ordinalOrderIsNotTheFilesOrderAndTheWriterConverges_2496(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n  `w` smallint NOT NULL,\n  `d` bigint unsigned NOT NULL,\n  `v` varchar(8) DEFAULT NULL,\n  PRIMARY KEY (`w`,`d`)\n) ENGINE=InnoDB;\n"
	base, pkCols := writeKeyedBaseline(t, ddl, []string{"w", "d"}, [][]string{{"2", "1", "a"}, {"1", "2", "b"}, {"1", "1", "c"}})
	tm := metadata.TableMeta{Columns: []metadata.ColumnMeta{
		{Name: "w", OrdinalPosition: 1, IsPK: true}, {Name: "d", OrdinalPosition: 2, IsPK: true}, {Name: "v", OrdinalPosition: 3}}}
	var ordinal []string
	for _, c := range tm.PKColumnMetas() {
		ordinal = append(ordinal, c.Name)
	}
	if !slices.Equal(ordinal, []string{"w", "d"}) || pkCols[0].Name != "w" {
		t.Fatalf("PKColumnMetas gives %q, the fixture's pkCols start with %q; want table order w, d", ordinal, pkCols[0].Name)
	}
	cols, err := baseline.ParseSchemaText(ddl)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := baseline.BinaryColumnsStoredAsText(base, cols); err != nil {
		t.Fatal(err)
	}
	build := func(keys []string) []byte {
		t.Helper()
		if _, err := keyidx.Build(context.Background(), base, keys, duckdbutil.DefaultTuning()); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(keyidx.PathFor(base))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	fromOrdinal := build(ordinal)
	fromKeyLine := build(keyidx.PrimaryKeyColumns(ddl))
	if !bytes.Equal(fromOrdinal, fromKeyLine) {
		t.Fatal("the schema's order and the PRIMARY KEY line's order gave different index files")
	}
	// The file's order is alphabetical, so d before w: not the ordinal order.
	n := int(fromOrdinal[10])
	off := int(keyidx.HeaderSize(nil)) + n // the fixed header, then the type codes
	var names []string
	for range n {
		l := int(binary.BigEndian.Uint16(fromOrdinal[off:]))
		names = append(names, string(fromOrdinal[off+2:off+2+l]))
		off += 2 + l
	}
	if !slices.Equal(names, []string{"d", "w"}) {
		t.Fatalf("header names %q; want the file's alphabetical order d, w (which is NOT the ordinal order %q)", names, ordinal)
	}
}

// A PostgreSQL baseline (text keys) and a table with no key open no DuckDB
// session for an index they cannot have.
func TestWantKeyIndex_2496(t *testing.T) {
	pk := []metadata.ColumnMeta{{Name: "id", IsPK: true}}
	for _, tc := range []struct {
		name string
		in   mergeInput
		want bool
	}{
		{"MySQL table with a key", mergeInput{PKCols: pk}, true},
		{"PostgreSQL table", mergeInput{PKCols: pk, PG: true}, false},
		{"no key columns", mergeInput{}, false},
	} {
		if got := wantKeyIndex(tc.in); got != tc.want {
			t.Errorf("%s: %v", tc.name, got)
		}
	}
}
