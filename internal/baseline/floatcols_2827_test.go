package baseline

import (
	"slices"
	"testing"
)

// #2827: the FLOAT columns are listed for read routing, whatever their
// declaration (a FLOAT(M,D), an UNSIGNED one), and nothing else is: not a
// DOUBLE, a DECIMAL or an integer.
func TestFloatColumns(t *testing.T) {
	cols, err := ParseSchemaText("CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `f` float DEFAULT NULL,\n" +
		"  `f72` float(7,2) DEFAULT NULL,\n" +
		"  `fu` float unsigned DEFAULT NULL,\n" +
		"  `F_Upper` FLOAT NOT NULL,\n" +
		"  `d` double DEFAULT NULL,\n" +
		"  `d72` double(7,2) DEFAULT NULL,\n" +
		"  `m` decimal(5,2) DEFAULT NULL,\n" +
		"  `name` varchar(16) DEFAULT NULL,\n" +
		"  `floaty` varchar(16) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n")
	if err != nil {
		t.Fatal(err)
	}
	got := FloatColumns(cols)
	if want := []string{"f", "f72", "fu", "F_Upper"}; !slices.Equal(got, want) {
		t.Errorf("FloatColumns = %q, want %q", got, want)
	}
	if got := FloatColumns(nil); got != nil {
		t.Errorf("FloatColumns(nil) = %q, want nil", got)
	}
	// A value kept verbatim as text is still a FLOAT on MySQL.
	if got := FloatColumns([]Column{{Name: "r", MySQLType: "float", RawText: true}}); !slices.Equal(got, []string{"r"}) {
		t.Errorf("a FLOAT kept as text: %q, want [r]", got)
	}
}
