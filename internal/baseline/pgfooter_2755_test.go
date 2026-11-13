package baseline

import (
	"context"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

// writePGFixture writes a file the way a PostgreSQL snapshot does: every
// column as text, under the names given, with the footer value given for the
// recorded table shape ("" writes none).
func writePGFixture(t *testing.T, path string, names []string, relation string, row []string) {
	t.Helper()
	cols := make([]Column, len(names))
	for i, n := range names {
		cols[i] = Column{Name: n, RawText: true}
	}
	meta := map[string]string{}
	if relation != "" {
		meta[MetaKeyPGRelation] = relation
	}
	w, err := NewWriter(path, cols, WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: meta})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow(row, make([]bool, len(row))); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// A PostgreSQL snapshot file's recorded shape is read as its footer entry:
// the table's own column order and how each column's text is read (#2755).
// What cannot be trusted reads as text: a shape that does not parse, one that
// names a column the file does not hold, one with two columns that differ only
// in case, and a file that records none.
func TestReadTableFooters_postgresShape(t *testing.T) {
	dir := t.TempDir()
	encode := func(cols ...PGColumn) string {
		s, err := PGRelation{OID: 1, Columns: cols}.Encode()
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	shape := encode(
		PGColumn{Name: "id", TypeOID: 20, TypeMod: -1, Ordinal: 1},
		PGColumn{Name: "note", TypeOID: 25, TypeMod: -1, Ordinal: 2},
		PGColumn{Name: "amount", TypeOID: 1700, TypeMod: ((12 << 16) | 2) + 4, Ordinal: 3},
	)
	good := filepath.Join(dir, "snap", "public", "orders.parquet")
	writePGFixture(t, good, []string{"id", "note", "amount"}, shape, []string{"1", "x", "9.50"})
	other := filepath.Join(dir, "snap", "public", "other.parquet")
	writePGFixture(t, other, []string{"id", "note"}, shape, []string{"1", "x"})
	broken := filepath.Join(dir, "snap", "public", "broken.parquet")
	writePGFixture(t, broken, []string{"id"}, "{not json", []string{"1"})
	old := filepath.Join(dir, "snap", "public", "old.parquet")
	writePGFixture(t, old, []string{"id"}, "", []string{"1"})

	twins := filepath.Join(dir, "snap", "public", "twins.parquet")
	writePGFixture(t, twins, []string{"Id", "id"}, encode(
		PGColumn{Name: "Id", TypeOID: 20, TypeMod: -1, Ordinal: 1},
		PGColumn{Name: "id", TypeOID: 25, TypeMod: -1, Ordinal: 2},
	), []string{"1", "x"})

	logs := captureLogs(t)
	read, err := ReadTableFooters(context.Background(), []string{good, other, twins, broken, old})
	if err != nil {
		t.Fatal(err)
	}
	f, ok := read.Footers[good]
	if !ok {
		t.Fatalf("no footer entry for the PostgreSQL file; read %+v\n%s", read, logs.String())
	}
	if want := []string{"id", "note", "amount"}; !reflect.DeepEqual(f.Columns, want) {
		t.Errorf("Columns = %v, want the table's order %v", f.Columns, want)
	}
	if want := []PGCast{{"id", "BIGINT"}, {"note", PGAsText}, {"amount", "DECIMAL(12,2)"}}; !reflect.DeepEqual(f.PG, want) {
		t.Errorf("PG = %v, want %v", f.PG, want)
	}
	if f.Decimals != nil || f.Text != nil || f.Temporal != nil {
		t.Errorf("a PostgreSQL entry carries MySQL fields: %+v", f)
	}
	// other: the shape names a column the file does not hold, so a view that
	// named it would not bind. twins: two columns that differ only in case,
	// which the engine reading the file cannot tell apart. Neither keeps an
	// entry: an empty one would read as a MySQL table with nothing to cast.
	for _, p := range []string{other, twins, broken, old} {
		if _, ok := read.Footers[p]; ok {
			t.Errorf("%s has a footer entry; want none", filepath.Base(p))
		}
		if !slices.Contains(read.NoSchema, p) {
			t.Errorf("%s is not reported as carrying no usable shape: %v", filepath.Base(p), read.NoSchema)
		}
	}
	if len(read.Unread) != 0 {
		t.Errorf("Unread = %v, want none", read.Unread)
	}
}
