package reconstruct

import (
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// TestIntegerPKDataType_isTheCanonicalizersPassThrough: integerPKDataType,
// which the key index path stands aside on, says yes for exactly the
// DataTypes canonicalizePKValue hands back untouched (the integer family),
// so the index never answers a key the join would refuse by type.
func TestIntegerPKDataType_isTheCanonicalizersPassThrough(t *testing.T) {
	for _, dt := range []string{
		"int", "integer", "smallint", "tinyint", "mediumint", "bigint", "year", "INT", " bigint ",
		"char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum", "set", "decimal", "numeric",
		"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "datetime", "timestamp", "date",
		"uuid", "inet4", "inet6", "float", "double", "time", "bit", "json", "", "int unsigned", "bigint unsigned",
	} {
		col := metadata.ColumnMeta{Name: "k", DataType: dt, ColumnType: dt}
		got, err := canonicalizePKValue(int32(7), col)
		passes := err == nil && got == int32(7)
		if passes != integerPKDataType(dt) {
			t.Errorf("DataType %q: canonicalizePKValue passes an int32 through = %v (err %v), integerPKDataType = %v", dt, passes, err, integerPKDataType(dt))
		}
		// The set itself, pinned: the two agree by construction (one list),
		// so a name added to or dropped from it must show here.
		want := slices.Contains([]string{"int", "integer", "smallint", "tinyint", "mediumint", "bigint", "year"}, strings.ToLower(strings.TrimSpace(dt)))
		if integerPKDataType(dt) != want {
			t.Errorf("DataType %q: integerPKDataType = %v, want %v", dt, integerPKDataType(dt), want)
		}
	}
}
