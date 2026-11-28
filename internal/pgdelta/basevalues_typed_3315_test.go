package pgdelta

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// A column the base stores as its type (#3315) gives the text it was
// written from, the same as the text file of the same rows: what the
// changes beside the file take an unchanged TOAST value from.
func TestBaseValuesOfTypedColumns(t *testing.T) {
	db := duck(t)
	ctx := context.Background()
	cols := []string{"id", "n", "d", "u", "big"}
	typed := map[string]string{"n": "DECIMAL(38,4)", "d": "DATE", "u": "UUID", "big": "BIGINT"}
	rows := [][]string{
		{"1", "-9999999999999999999999999999999999.9999", "infinity", "12345678-9abc-def0-1234-56789abcdef0", "-9223372036854775808"},
		{"2", "0.0001", "10000-01-01", "00000000-0000-0000-0000-000000000000", "0"},
		{"3", "", "", "", ""},
	}
	var got [2]map[string]map[string]*string
	for i, asTyped := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "t.parquet")
		var w interface {
			WriteRow([]string, []bool) error
			Close() error
		}
		var err error
		if asTyped {
			w, err = baseline.NewPGWriter(path, cols, typed, baseline.WriterConfig{Compression: "none", RowGroupSize: 2})
		} else {
			bc := make([]baseline.Column, len(cols))
			for j, c := range cols {
				bc[j] = baseline.Column{Name: c, RawText: true}
			}
			w, err = baseline.NewWriter(path, bc, baseline.WriterConfig{Compression: "none", RowGroupSize: 2})
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			nulls := make([]bool, len(r))
			for j := range r {
				nulls[j] = j > 0 && r[j] == ""
			}
			if err := w.WriteRow(r, nulls); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		got[i], err = BaseValuesOf(ctx, db, path, []string{"id"}, []string{"1", "2", "3"}, []string{"n", "d", "u", "big"}, 0)
		if err != nil {
			t.Fatalf("typed %v: %v", asTyped, err)
		}
	}
	if !reflect.DeepEqual(got[0], got[1]) {
		t.Fatalf("the typed file gives other text:\n%v\n%v", got[0], got[1])
	}
	if d := got[1]["1"]["d"]; d == nil || *d != "infinity" {
		t.Fatalf("d = %v", d)
	}
}
