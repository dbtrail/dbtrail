package baseline

import (
	"maps"
	"testing"
)

// TestColumnCharsets_resolvesAsTheServerPrints is #2723's reading of a carried
// CREATE TABLE: SHOW CREATE TABLE leaves out a column's character set when it
// is the table's, so the table's options fill it in, and where nothing names
// one the column is left out rather than guessed.
func TestColumnCharsets_resolvesAsTheServerPrints(t *testing.T) {
	table := func(options string, columns ...string) string {
		sql := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n"
		for _, c := range columns {
			sql += "  " + c + ",\n"
		}
		return sql + "  PRIMARY KEY (`id`)\n)" + options + ";\n"
	}
	const mysql80 = " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	for _, tc := range []struct {
		name string
		sql  string
		want map[string]string
	}{
		{"no attribute: the table's", table(mysql80, "`a` varchar(20) DEFAULT NULL"), map[string]string{"a": "utf8mb4"}},
		{"its own character set", table(mysql80, "`a` varchar(20) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL"), map[string]string{"a": "latin1"}},
		{"CHARSET spelling", table(mysql80, "`a` varchar(20) CHARSET latin1 DEFAULT NULL"), map[string]string{"a": "latin1"}},
		{"its own collation alone", table(mysql80, "`a` varchar(20) COLLATE latin1_bin DEFAULT NULL"), map[string]string{"a": "latin1"}},
		{"a collation of the table's character set", table(mysql80, "`a` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL"), map[string]string{"a": "utf8mb4"}},
		{"table names only a collation", table(" ENGINE=InnoDB COLLATE=latin1_swedish_ci", "`a` text"), map[string]string{"a": "latin1"}},
		{"MySQL 5.7 table options", table(" ENGINE=InnoDB DEFAULT CHARSET=latin1", "`a` char(3) NOT NULL"), map[string]string{"a": "latin1"}},
		{"nothing names one: left out", table(" ENGINE=InnoDB", "`a` varchar(20) DEFAULT NULL"), map[string]string{}},
		{"no table options at all", table("", "`a` varchar(20) DEFAULT NULL"), map[string]string{}},
		{"a collation whose name does not start with a character set", table(" ENGINE=InnoDB", "`a` varchar(20) COLLATE uca1400_ai_ci"), map[string]string{}},
		{"utf8 is utf8mb3", table(" DEFAULT CHARSET=utf8", "`a` varchar(20)", "`b` varchar(5) CHARACTER SET utf8mb3"), map[string]string{"a": "utf8mb3", "b": "utf8mb3"}},
		{"upper case", table(" DEFAULT CHARSET=UTF8MB4", "`a` VARCHAR(20) CHARACTER SET LATIN1"), map[string]string{"a": "latin1"}},
		{"every text type, and nothing else", table(mysql80,
			"`c` char(1)", "`v` varchar(2)", "`tt` tinytext", "`x` text", "`m` mediumtext", "`l` longtext",
			"`e` enum('a','b') CHARACTER SET latin1", "`s` set('x')",
			"`n` int", "`vb` varbinary(4)", "`bl` blob", "`j` json", "`d` datetime"),
			map[string]string{"c": "utf8mb4", "v": "utf8mb4", "tt": "utf8mb4", "x": "utf8mb4", "m": "utf8mb4", "l": "utf8mb4", "e": "latin1", "s": "utf8mb4"}},
		{"the words in a comment or default are not the attribute", table(mysql80,
			"`a` varchar(20) DEFAULT 'CHARACTER SET latin1' COMMENT 'COLLATE latin1_bin'"), map[string]string{"a": "utf8mb4"}},
		{"a column named charset", table(mysql80, "`charset` varchar(8) DEFAULT NULL"), map[string]string{"charset": "utf8mb4"}},
		{"MariaDB's JSON alias", table(" ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci",
			"`j` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`j`))"), map[string]string{"j": "utf8mb4"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cols, err := ParseSchemaText(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			if got := ColumnCharsets(tc.sql, cols); !maps.Equal(got, tc.want) {
				t.Errorf("ColumnCharsets = %v, want %v\n%s", got, tc.want, tc.sql)
			}
		})
	}

	t.Run("a value kept as text has no character set", func(t *testing.T) {
		cols := []Column{{Name: "a", MySQLType: "varchar", RawText: true}}
		if got := ColumnCharsets("CREATE TABLE t (\n  a varchar\n) DEFAULT CHARSET=utf8mb4;", cols); len(got) != 0 {
			t.Errorf("a RawText column got a character set: %v", got)
		}
	})
}

func TestComparableCharset(t *testing.T) {
	for in, want := range map[string]string{
		"utf8": "utf8mb3", "UTF8": "utf8mb3", " utf8mb3 ": "utf8mb3", "utf8mb4": "utf8mb4", "Latin1": "latin1", "": "",
	} {
		if got := ComparableCharset(in); got != want {
			t.Errorf("ComparableCharset(%q) = %q, want %q", in, got, want)
		}
	}
}
