package reconstruct

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// writePGTypedPair writes the same rows twice, every column as text and with
// the columns of typed stored as their type (#3315).
func writePGTypedPair(t *testing.T, cols []string, typed map[string]string, rows [][]string) (text, stored string) {
	t.Helper()
	write := func(asTyped bool) string {
		path := filepath.Join(t.TempDir(), "t.parquet")
		var w interface {
			WriteRow([]string, []bool) error
			Close() error
		}
		var err error
		if asTyped {
			enc, _ := baseline.EncodePGTypedColumns(typed)
			w, err = baseline.NewPGWriter(path, cols, typed, baseline.WriterConfig{Compression: "none", RowGroupSize: 2,
				Metadata: map[string]string{baseline.MetaKeyPGTypedColumns: enc}})
		} else {
			bc := make([]baseline.Column, len(cols))
			for i, c := range cols {
				bc[i] = baseline.Column{Name: c, RawText: true}
			}
			w, err = baseline.NewWriter(path, bc, baseline.WriterConfig{Compression: "none", RowGroupSize: 2})
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			if err := w.WriteRow(r, make([]bool, len(r))); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return path
	}
	return write(false), write(true)
}

// A file whose list of stored columns cannot be read is refused by the
// readers of whole rows and by a fold, never read through the cast.
func TestPGTypedUnusableListRefusedByReaders(t *testing.T) {
	_, stored := writePGTypedPair(t, []string{"id", "n"}, map[string]string{"n": "INTEGER"}, [][]string{{"1", "2"}})
	_ = stored
	path := filepath.Join(t.TempDir(), "t.parquet")
	w, err := baseline.NewWriter(path, []baseline.Column{{Name: "id", RawText: true}}, baseline.WriterConfig{Compression: "none", RowGroupSize: 2,
		Metadata: map[string]string{baseline.MetaKeyPGTypedColumns: `{"v":2,"columns":{"n":"TIMESTAMP"}}`}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBaselineRows(context.Background(), path, map[string]string{"id": "1"}, 1); err == nil {
		t.Fatal("ReadBaselineRows read a list of another version")
	}
	rel := baseline.PGRelation{OID: 1, PrimaryKey: []string{"id"}, Columns: []baseline.PGColumn{{Name: "id", TypeOID: 23, TypeMod: -1}, {Name: "n", TypeOID: 23, TypeMod: -1}}}
	shape, _ := rel.Encode()
	md := baseline.DumpMetadata{LSN: 1, PGRelation: shape, RenderGUCs: baseline.RenderGUCsPinned}
	if _, err := checkPGFoldable("public", "t", md); err != nil {
		t.Fatalf("a file of text: %v", err)
	}
	for _, v := range []string{`{"v":1,"columns":{"n":"INTEGER"}}`} {
		md.PGTypedColumns = v
		if _, err := checkPGFoldable("public", "t", md); err != nil {
			t.Fatalf("%s: %v", v, err)
		}
	}
	for _, v := range []string{`{"v":2,"columns":{"n":"INTEGER"}}`, `{"v":1,"columns":{"id":"INTEGER"}}`, `{"v":1,"columns":{"n":"BIGINT"}}`} {
		md.PGTypedColumns = v
		if _, err := checkPGFoldable("public", "t", md); err == nil {
			t.Fatalf("%s: a fold starts from it", v)
		}
	}
}

// The row a single-row reconstruct starts from is the same text from a file
// that stores columns typed as from the text file of the same rows.
func TestReadBaselineRowsTypedPG(t *testing.T) {
	cols := []string{"id", "n", "d", "u"}
	typed := map[string]string{"n": "DECIMAL(12,2)", "d": "DATE", "u": "UUID"}
	rows := [][]string{{"1", "1.50", "-infinity", "12345678-9abc-def0-1234-56789abcdef0"}, {"2", "-0.05", "10000-01-01", "00000000-0000-0000-0000-000000000000"}}
	text, stored := writePGTypedPair(t, cols, typed, rows)
	for _, id := range []string{"1", "2"} {
		a, err := ReadBaselineRows(context.Background(), text, map[string]string{"id": id}, 1)
		if err != nil {
			t.Fatal(err)
		}
		b, err := ReadBaselineRows(context.Background(), stored, map[string]string{"id": id}, 1)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(a, b) || len(a) != 1 {
			t.Fatalf("id %s: text file %s, typed file %s", id, fmt.Sprintf("%#v", a), fmt.Sprintf("%#v", b))
		}
	}
}
