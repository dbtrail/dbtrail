package baseline

import (
	"context"
	"slices"
	"testing"
)

func TestDistinctColumns(t *testing.T) {
	cols := []Column{
		{Name: "id", MySQLType: "bigint", Unsigned: true},
		{Name: "n", MySQLType: "int"},
		{Name: "flag", MySQLType: "tinyint"},
		{Name: "ok", MySQLType: "boolean"},
		{Name: "amount", MySQLType: "decimal", DecimalPrecision: 10, DecimalScale: 2},
		{Name: "huge", MySQLType: "decimal", DecimalPrecision: 65, DecimalScale: 2},
		{Name: "noprec", MySQLType: "decimal"},
		{Name: "created_on", MySQLType: "date"},
		{Name: "seen_at", MySQLType: "datetime"},
		{Name: "ts", MySQLType: "timestamp"},
		{Name: "yr", MySQLType: "year"},
		{Name: "tm", MySQLType: "time"},
		{Name: "ratio", MySQLType: "float"},
		{Name: "score", MySQLType: "double"},
		{Name: "status", MySQLType: "varchar"},
		{Name: "kind", MySQLType: "enum"},
		{Name: "tags", MySQLType: "set"},
		{Name: "doc", MySQLType: "json"},
		{Name: "raw", MySQLType: "varbinary"},
		{Name: "bits", MySQLType: "bit"},
		{Name: "pg", MySQLType: "int", RawText: true},
		{Name: "odd", MySQLType: "somethingnew"},
	}
	safe, other := DistinctColumns(cols)
	wantSafe := []string{"id", "n", "flag", "ok", "amount"}
	if !slices.Equal(safe, wantSafe) {
		t.Errorf("safe = %v, want %v", safe, wantSafe)
	}
	if len(safe)+len(other) != len(cols) {
		t.Errorf("safe %v + other %v do not cover the %d columns", safe, other, len(cols))
	}
	for _, name := range []string{"created_on", "seen_at", "ts", "yr", "huge", "noprec", "tm", "ratio", "score", "status", "kind", "tags", "doc", "raw", "bits", "pg", "odd"} {
		if !slices.Contains(other, name) {
			t.Errorf("%s is not among the others: %v", name, other)
		}
	}
}

// The footer read carries them, parsed from the CREATE TABLE in the file.
func TestReadTableFooters_distinct(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `amount` decimal(10,2) DEFAULT NULL,\n  `status` varchar(20) DEFAULT NULL,\n  `created_on` date DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	path := writeOrderFile(t, "t", ddl, ddl)
	footers, err := TableFootersFor(context.Background(), []string{path})
	if err != nil {
		t.Fatal(err)
	}
	f := footers[path]
	if !slices.Equal(f.DistinctSafe, []string{"id", "amount"}) || !slices.Equal(f.DistinctOther, []string{"status", "created_on"}) {
		t.Errorf("DistinctSafe = %v, DistinctOther = %v", f.DistinctSafe, f.DistinctOther)
	}
}
