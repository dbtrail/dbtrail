package reconstruct

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// #2007: a versioned table stores each change under the declared key plus
// ROW END, keyed by the before image, so an operator's `--pk 2` must also
// match "2|<current marker>" for each marker a server writes.
func TestExpandSysVersionedKeys_2007(t *testing.T) {
	const m4, m11 = "2038-01-19 03:14:07.999999", "2106-02-07 06:28:15.999999"
	id := metadata.ColumnMeta{Name: "id", IsPK: true, DataType: "int"}
	a := metadata.ColumnMeta{Name: "a", IsPK: true, DataType: "varchar"}
	end := metadata.ColumnMeta{Name: "row_end", IsPK: true, DataType: "timestamp", IsGenerated: true}
	for _, c := range []struct {
		name   string
		metas  []metadata.ColumnMeta
		values []string
		want   []string // nil = unchanged
	}{
		{"declared key, end last", []metadata.ColumnMeta{id, end}, []string{"2"},
			[]string{"2", "2|" + m4, "2|" + m11}},
		{"two values (--pks)", []metadata.ColumnMeta{id, end}, []string{"2", "3"},
			[]string{"2", "2|" + m4, "2|" + m11, "3", "3|" + m4, "3|" + m11}},
		{"composite declared key, end between", []metadata.ColumnMeta{a, end, id}, []string{"x|5"},
			[]string{"x|5", "x|" + m4 + "|5", "x|" + m11 + "|5"}},
		{"value already holds the end", []metadata.ColumnMeta{id, end}, []string{"2|" + m11}, nil},
		{"plain table", []metadata.ColumnMeta{id}, []string{"2"}, nil},
		{"transaction-precise end: no marker to spell", []metadata.ColumnMeta{id,
			{Name: "re", IsPK: true, DataType: "bigint", IsGenerated: true}}, []string{"2"}, nil},
		{"two generated key members", []metadata.ColumnMeta{id, end,
			{Name: "g", IsPK: true, DataType: "timestamp", IsGenerated: true}}, []string{"2|x"}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, ok := expandSysVersionedKeys(c.metas, c.values)
			if c.want == nil {
				if ok {
					t.Fatalf("expanded to %q, want unchanged", got)
				}
				return
			}
			if !ok || strings.Join(got, ",") != strings.Join(c.want, ",") {
				t.Fatalf("expandSysVersionedKeys = %q, %v; want %q", got, ok, c.want)
			}
		})
	}
}
