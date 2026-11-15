package baseline

import (
	"slices"
	"testing"
)

// #2848: the UNSIGNED whole-number columns are listed, whatever their width
// and spelling; an UNSIGNED DECIMAL, DOUBLE or FLOAT is not, nor a signed
// column, nor a text column whose name says unsigned.
func TestUnsignedWholeColumns(t *testing.T) {
	cols, err := ParseSchemaText("CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `ti` tinyint unsigned DEFAULT NULL,\n" +
		"  `si` smallint unsigned DEFAULT NULL,\n" +
		"  `mi` mediumint UNSIGNED DEFAULT NULL,\n" +
		"  `ui` int unsigned NOT NULL,\n" +
		"  `iw` int(10) unsigned DEFAULT NULL,\n" +
		"  `u` bigint unsigned DEFAULT NULL,\n" +
		"  `Big_U` BIGINT(20) UNSIGNED NOT NULL,\n" +
		"  `zf` int(5) unsigned zerofill DEFAULT NULL,\n" +
		"  `b` bigint DEFAULT NULL,\n" +
		"  `dcu` decimal(10,2) unsigned DEFAULT NULL,\n" +
		"  `dbu` double unsigned DEFAULT NULL,\n" +
		"  `fu` float unsigned DEFAULT NULL,\n" +
		"  `is_unsigned` varchar(16) DEFAULT NULL COMMENT 'unsigned',\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n")
	if err != nil {
		t.Fatal(err)
	}
	got := UnsignedWholeColumns(cols)
	if want := []string{"ti", "si", "mi", "ui", "iw", "u", "Big_U", "zf"}; !slices.Equal(got, want) {
		t.Errorf("UnsignedWholeColumns = %q, want %q", got, want)
	}
	if got := UnsignedWholeColumns(nil); got != nil {
		t.Errorf("UnsignedWholeColumns(nil) = %q, want nil", got)
	}
	// A value kept verbatim as text is still an UNSIGNED number on MySQL, and
	// ZEROFILL alone implies UNSIGNED.
	if got := UnsignedWholeColumns([]Column{{Name: "r", MySQLType: "int", Unsigned: true, RawText: true}, {Name: "z", MySQLType: "smallint", DeclaredType: "smallint(4) ZEROFILL"}}); !slices.Equal(got, []string{"r", "z"}) {
		t.Errorf("kept as text, ZEROFILL alone: %q, want [r z]", got)
	}
}
