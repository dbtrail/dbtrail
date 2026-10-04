package reconstruct

import (
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// A known limit, pinned so that closing it is a visible change (#2083): a
// refresh carries the table's CREATE TABLE forward, and the check that
// refuses when the definition moved (columnDefinitionChanged) compares each
// column's type and NULL-ness with the schema snapshot. It does not compare
// collations, because a schema snapshot does not record them.
//
// So an ALTER that changes only a column's collation passes the refresh, and
// the new snapshot's footer keeps the OLD collation. The state views read
// collations from that footer (baseline.BinaryCollationColumns):
//
//   - _ci at the full snapshot, _bin now: the copy keeps folding case. That is
//     what it did for every column before collations were read at all.
//   - _bin at the full snapshot, _ci now: the copy keeps comparing bytes, and
//     returns fewer rows than MySQL until the next full snapshot.
//
// Both directions are asserted here on purpose. When schema snapshots record
// COLLATION_NAME and this check compares it, both must flip to "changed".
func TestColumnDefinitionChanged_doesNotSeeACollationChange(t *testing.T) {
	const ci = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	carried := func(column string) (baseline.Column, []string) {
		sql := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n" + column + "  PRIMARY KEY (`id`)\n) " + ci + ";\n"
		cols, err := baseline.ParseSchemaText(sql)
		if err != nil {
			t.Fatal(err)
		}
		return cols[1], baseline.BinaryCollationColumns(sql, cols)
	}
	// What the schema snapshot holds for the column after EITHER alter: the
	// same type, the same NULL-ness, the same character set.
	now := metadata.ColumnMeta{Name: "code", DataType: "varchar", ColumnType: "varchar(16)", CharacterSet: "utf8mb4", IsNullable: "YES"}

	wasBin, binary := carried("  `code` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n")
	if !slices.Equal(binary, []string{"code"}) {
		t.Fatalf("the carried definition should name code as _bin: %v", binary)
	}
	if diff, changed := columnDefinitionChanged(wasBin, now); changed {
		t.Errorf("_bin to _ci is now seen by the refresh (%s): update this test and the documented limit", diff)
	}

	wasCI, binary := carried("  `code` varchar(16) DEFAULT NULL,\n")
	if len(binary) != 0 {
		t.Fatalf("the carried definition should not name code as _bin: %v", binary)
	}
	if diff, changed := columnDefinitionChanged(wasCI, now); changed {
		t.Errorf("_ci to _bin is now seen by the refresh (%s): update this test and the documented limit", diff)
	}
}
