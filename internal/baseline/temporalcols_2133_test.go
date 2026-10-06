package baseline

import (
	"context"
	"reflect"
	"testing"
)

// #2133: the columns read routing must know the type of. A type this package
// does not know is listed with no type, never left out: not known is not
// "not a date".
func TestTemporalColumns(t *testing.T) {
	ddl := "CREATE TABLE `ev` (\n  `id` int NOT NULL,\n  `created_on` date DEFAULT NULL,\n  `dt` datetime(6) DEFAULT NULL,\n" +
		"  `ts` timestamp NULL DEFAULT NULL,\n  `tm` time DEFAULT NULL,\n  `yr` year DEFAULT NULL,\n  `n` bigint unsigned DEFAULT NULL,\n" +
		"  `amount` decimal(10,2) DEFAULT NULL,\n  `name` varchar(8) DEFAULT NULL,\n  `flags` bit(8) DEFAULT NULL,\n  `doc` json DEFAULT NULL,\n" +
		"  `blob1` mediumblob,\n  `kind` enum('a','b') DEFAULT NULL,\n  `geo` point DEFAULT NULL,\n  `odd` datemultirange DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	cols, err := ParseSchemaText(ddl)
	if err != nil {
		t.Fatal(err)
	}
	want := []TemporalColumn{{"created_on", "date"}, {"dt", "datetime"}, {"ts", "timestamp"}, {"tm", "time"}, {"yr", "year"}, {"odd", ""}}
	if got := TemporalColumns(cols); !reflect.DeepEqual(got, want) {
		t.Errorf("TemporalColumns = %v, want %v", got, want)
	}
	// Every type the Parquet mapping knows by name is known here too, so a
	// type added there without a thought for this list shows up as unknown
	// and not as "not a date".
	for _, typ := range []string{"int", "integer", "tinyint", "smallint", "mediumint", "bigint", "float", "double", "real", "decimal", "numeric",
		"char", "varchar", "tinytext", "text", "mediumtext", "longtext", "enum", "set", "json", "binary", "varbinary", "blob", "geometry"} {
		if got := TemporalColumns([]Column{{Name: "c", MySQLType: typ}}); got != nil {
			t.Errorf("a %s column is listed: %v", typ, got)
		}
	}
	if got := TemporalColumns([]Column{{Name: "id", MySQLType: "int"}}); got != nil {
		t.Errorf("a table with no such column: %v, want none", got)
	}
}

// The footer read carries them beside the column order.
func TestReadTableFooters_temporal(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `created_on` date DEFAULT NULL,\n  `tm` time DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	path := writeOrderFile(t, "t", ddl, ddl)
	footers, err := TableFootersFor(context.Background(), []string{path})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := footers[path].Temporal, []TemporalColumn{{"created_on", "date"}, {"tm", "time"}}; !reflect.DeepEqual(got, want) {
		t.Errorf("Temporal = %v, want %v", got, want)
	}
}
