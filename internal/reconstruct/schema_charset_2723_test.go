package reconstruct

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// TestCheckBaselineSchemaCurrent_charsetChange is #2723 at the guard: a text
// column whose character set in the schema snapshot is not the one the
// baseline's CREATE TABLE gives it refuses like a type change. A narrowing
// ALTER the server let through rewrote values with no row event, and a
// widening one leaves the carried CREATE TABLE declaring a column that cannot
// hold what was written since.
func TestCheckBaselineSchemaCurrent_charsetChange(t *testing.T) {
	create := func(options string, name string) string {
		return "CREATE TABLE `t` (\n" +
			"  `id` int NOT NULL,\n" +
			"  " + name + ",\n" +
			"  `n` int DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n" +
			")" + options + ";\n"
	}
	const utf8mb4Table = " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	const latin1Table = " ENGINE=InnoDB DEFAULT CHARSET=latin1"
	col := func(columnType, charset, nullable string) metadata.ColumnMeta {
		dataType := columnType
		if n := strings.IndexAny(dataType, "( "); n >= 0 {
			dataType = dataType[:n]
		}
		return metadata.ColumnMeta{Name: "name", DataType: dataType, ColumnType: columnType, CharacterSet: charset, IsNullable: nullable}
	}
	snapshot := func(name metadata.ColumnMeta) *metadata.TableMeta {
		return &metadata.TableMeta{Schema: "s", Table: "t", Columns: []metadata.ColumnMeta{
			{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO"},
			name,
			{Name: "n", DataType: "int", ColumnType: "int", IsNullable: "YES"},
		}}
	}
	for _, tc := range []struct {
		name      string
		createSQL string
		now       metadata.ColumnMeta
		wantErr   string // "" = must not refuse
	}{
		{"same character set, from the table's default", create(utf8mb4Table, "`name` varchar(20) DEFAULT NULL"),
			col("varchar(20)", "utf8mb4", "YES"), ""},
		{"same character set, written on the column", create(latin1Table, "`name` varchar(20) CHARACTER SET utf8mb4 DEFAULT NULL"),
			col("varchar(20)", "utf8mb4", "YES"), ""},
		{"narrowed: the issue's first case", create(utf8mb4Table, "`name` varchar(20) DEFAULT NULL"),
			col("varchar(20)", "latin1", "YES"), "type changed since: name (varchar(20) CHARACTER SET utf8mb4 -> varchar(20) CHARACTER SET latin1)"},
		{"widened: CONVERT TO utf8mb4 on a latin1 table", create(latin1Table, "`name` varchar(20) DEFAULT NULL"),
			col("varchar(20)", "utf8mb4", "YES"), "name (varchar(20) CHARACTER SET latin1 -> varchar(20) CHARACTER SET utf8mb4)"},
		{"the column's own character set changed", create(utf8mb4Table, "`name` varchar(20) CHARACTER SET latin1 DEFAULT NULL"),
			col("varchar(20)", "utf8mb4", "YES"), "name (varchar(20) CHARACTER SET latin1 -> varchar(20) CHARACTER SET utf8mb4)"},
		{"an ENUM's character set changed", create(utf8mb4Table, "`name` enum('a','b') DEFAULT NULL"),
			col("enum('a','b')", "latin1", "YES"), "name (enum('a','b') CHARACTER SET utf8mb4 -> enum('a','b') CHARACTER SET latin1)"},
		{"type, character set and NULL-ness are one entry", create(latin1Table, "`name` text NOT NULL"),
			col("mediumtext", "utf8mb4", "YES"), "name (text CHARACTER SET latin1 NOT NULL -> mediumtext CHARACTER SET utf8mb4 NULL)"},
		{"utf8 in the dump, utf8mb3 in the snapshot", create(" DEFAULT CHARSET=utf8", "`name` varchar(20) DEFAULT NULL"),
			col("varchar(20)", "utf8mb3", "YES"), ""},
		{"upper case in the snapshot", create(utf8mb4Table, "`name` varchar(20) DEFAULT NULL"),
			col("varchar(20)", "UTF8MB4", "YES"), ""},
		{"a collation change within the character set is not seen (#2083)", create(utf8mb4Table, "`name` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL"),
			col("varchar(20)", "utf8mb4", "YES"), ""},
		{"a snapshot with no character set (before #756) compares nothing", create(utf8mb4Table, "`name` varchar(20) DEFAULT NULL"),
			col("varchar(20)", "", "YES"), ""},
		{"a CREATE TABLE that names no character set compares nothing", create(" ENGINE=InnoDB", "`name` varchar(20) DEFAULT NULL"),
			col("varchar(20)", "latin1", "YES"), ""},
		{"a column made binary is a type change, not a character set one", create(utf8mb4Table, "`name` varchar(20) DEFAULT NULL"),
			col("varbinary(20)", "", "YES"), "name (varchar(20) -> varbinary(20))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm := snapshot(tc.now)
			err := checkBaselineSchemaCurrent(tc.createSQL, tm, tm, "s", "t")
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected refusal: %v", err)
			case tc.wantErr == "":
				return
			case err == nil:
				t.Fatalf("expected a refusal containing %q", tc.wantErr)
			case !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
			if !isSchemaChanged(err) {
				t.Errorf("refusal is not tagged ErrSchemaChanged: %v", err)
			}
			if n := strings.Count(err.Error(), " -> "); n != 1 {
				t.Errorf("the refusal lists %d changes, want 1: %v", n, err)
			}
		})
	}

	narrowed := create(utf8mb4Table, "`name` varchar(20) DEFAULT NULL")
	tm := snapshot(col("varchar(20)", "latin1", "YES"))
	t.Run("compared only against the snapshot in effect after the baseline", func(t *testing.T) {
		if err := checkBaselineSchemaCurrent(narrowed, tm, nil, "s", "t"); err != nil {
			t.Errorf("no snapshot in effect: refused on the latest snapshot's character set: %v", err)
		}
	})
	t.Run("the Iceberg export does not compare it", func(t *testing.T) {
		if err := CheckBaselineSchemaCurrent(narrowed, tm, "s", "t"); err != nil {
			t.Errorf("the export refused a character set change through the refresh's check: %v", err)
		}
	})
}
