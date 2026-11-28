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
