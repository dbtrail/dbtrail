package baseline

import (
	"slices"
	"testing"
)

// #2829: a FLOAT or DOUBLE declared with its digits, or ZEROFILL, is printed
// by MySQL with fixed decimals or zero padding, which the copy cannot do; a
// plain FLOAT or DOUBLE is not listed, and neither is any other type.
func TestFixedPrintColumns(t *testing.T) {
	cols, err := ParseSchemaText("CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `f` float DEFAULT NULL,\n" +
		"  `f72` float(7,2) DEFAULT NULL,\n" +
		"  `f50` float(5,0) DEFAULT NULL,\n" +
		"  `fu` float unsigned DEFAULT NULL,\n" +
		"  `fz` float unsigned zerofill DEFAULT NULL,\n" +
		"  `d` double DEFAULT NULL,\n" +
		"  `d102` double(10,2) DEFAULT NULL,\n" +
		"  `D_Upper` DOUBLE( 10 , 2 ) UNSIGNED NOT NULL,\n" +
		"  `dz` double unsigned zerofill DEFAULT NULL,\n" +
		"  `du` double unsigned DEFAULT NULL,\n" +
		"  `m` decimal(5,2) DEFAULT NULL,\n" +
		"  `iz` int unsigned zerofill DEFAULT NULL,\n" +
		"  `name` varchar(16) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := FixedPrintColumns(cols), []string{"f72", "f50", "fz", "d102", "D_Upper", "dz"}; !slices.Equal(got, want) {
		t.Errorf("FixedPrintColumns = %q, want %q", got, want)
	}
	if got := FixedPrintColumns(nil); got != nil {
		t.Errorf("FixedPrintColumns(nil) = %q, want nil", got)
	}
	// A declaration that was not recorded is not known to print as the
	// value needs.
	if got := FixedPrintColumns([]Column{{Name: "r", MySQLType: "double"}, {Name: "i", MySQLType: "int"}}); !slices.Equal(got, []string{"r"}) {
		t.Errorf("no declaration: %q, want [r]", got)
	}
}
