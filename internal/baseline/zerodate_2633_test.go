package baseline

import (
	"slices"
	"testing"
)

// TestZeroDateColumns: the DATE, DATETIME and TIMESTAMP columns whose NULL on
// the copy can only be a zero date (#2633). The copy holds 0000-00-00 as NULL,
// so a NULL in a NOT NULL column, or in one whose default is the zero date,
// may stand for one; a nullable column's NULL may be the source's own.
func TestZeroDateColumns(t *testing.T) {
	const create = "CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `made` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',\n" +
		"  `born` date DEFAULT '0000-00-00',\n" +
		"  `seen` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
		"  `upper` DATETIME(6) default '0000-00-00 00:00:00.000000',\n" +
		"  `shipped` datetime DEFAULT NULL,\n" +
		"  `noted` datetime DEFAULT NULL COMMENT 'was DEFAULT ''0000-00-00''',\n" +
		"  `named_not_null` date DEFAULT NULL COMMENT 'NOT NULL some day',\n" +
		"  `real_default` date DEFAULT '2020-01-01',\n" +
		"  `count` int NOT NULL DEFAULT '0',\n" +
		"  `label` varchar(20) NOT NULL DEFAULT '0000-00-00',\n" +
		"  `at` time NOT NULL,\n" +
		"  `y` year NOT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n"
	cols, err := ParseSchemaText(create)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ZeroDateColumns(cols), []string{"made", "born", "seen", "upper"}; !slices.Equal(got, want) {
		t.Errorf("ZeroDateColumns = %q, want %q", got, want)
	}
}
