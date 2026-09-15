package reconstruct

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// TestCheckBaselineSchema_namesSkippedWithoutASnapshotForThem is #1667: when no
// snapshot can describe the table at the target (the one in effect predates the
// baseline and a newer one postdates the target), the caller passes nil names;
// types are still compared when it has them.
func TestCheckBaselineSchema_namesSkippedWithoutASnapshotForThem(t *testing.T) {
	createSQL := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `c` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	meta := func(cols ...string) *metadata.TableMeta {
		tm := &metadata.TableMeta{Schema: "s", Table: "t"}
		for _, c := range cols {
			name, ctype, _ := strings.Cut(c, " ")
			tm.Columns = append(tm.Columns, metadata.ColumnMeta{Name: name, DataType: ctype, ColumnType: ctype})
		}
		return tm
	}
	if err := checkBaselineSchemaCurrent(createSQL, nil, nil, "s", "t"); err != nil {
		t.Errorf("nothing to compare: %v", err)
	}
	if err := checkBaselineSchemaCurrent(createSQL, nil, meta("id int", "c int", "extra int"), "s", "t"); err != nil {
		t.Errorf("names skipped, types unchanged: refused on a column set: %v", err)
	}
	if err := checkBaselineSchemaCurrent(createSQL, nil, meta("id int", "c bigint"), "s", "t"); err == nil || !strings.Contains(err.Error(), "c (int -> bigint)") {
		t.Errorf("names skipped, a type changed: err = %v, want the type refusal", err)
	}
}
