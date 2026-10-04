package baseline

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
)

// Which text columns MySQL compares byte by byte (#2083): a column under a
// _bin collation, its own or the table's. Everything this reads is the
// CREATE TABLE as SHOW CREATE TABLE prints it, which names a column's
// collation only when it differs from the table's.
func TestBinaryCollationColumns(t *testing.T) {
	table := func(cols, options string) string {
		return "CREATE TABLE `t` (\n  `id` int NOT NULL,\n" + cols + "  PRIMARY KEY (`id`)\n) " + options + ";\n"
	}
	const ci = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	cases := []struct {
		name string
		sql  string
		want []string
	}{
		{"column COLLATE _bin",
			table("  `code` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `note` varchar(16) DEFAULT NULL,\n", ci),
			[]string{"code"}},
		{"COLLATE without CHARACTER SET",
			table("  `code` varchar(16) COLLATE utf8mb4_bin NOT NULL,\n", ci), []string{"code"}},
		{"upper case", table("  `code` VARCHAR(16) COLLATE UTF8MB4_BIN,\n", ci), []string{"code"}},
		{"0900 bin, MariaDB nopad bin, latin1 bin",
			table("  `a` char(4) COLLATE utf8mb4_0900_bin,\n  `b` text COLLATE utf8mb4_nopad_bin,\n  `c` varchar(8) CHARACTER SET latin1 COLLATE latin1_bin,\n", ci),
			[]string{"a", "b", "c"}},
		{"every text type, enum and set",
			table("  `a` tinytext COLLATE utf8mb4_bin,\n  `b` mediumtext COLLATE utf8mb4_bin,\n  `c` longtext COLLATE utf8mb4_bin,\n"+
				"  `d` enum('x','y') COLLATE utf8mb4_bin,\n  `e` set('x','y') COLLATE utf8mb4_bin,\n", ci),
			[]string{"a", "b", "c", "d", "e"}},
		{"table default _bin reaches the columns that name none",
			table("  `code` varchar(16) DEFAULT NULL,\n  `n` int DEFAULT NULL,\n  `d` decimal(10,2) DEFAULT NULL,\n  `at` datetime DEFAULT NULL,\n  `j` json DEFAULT NULL,\n",
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"),
			[]string{"code"}},
		{"a column's own collation wins over a _bin table",
			table("  `code` varchar(16) COLLATE utf8mb4_general_ci DEFAULT NULL,\n  `raw` varchar(16) DEFAULT NULL,\n",
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"),
			[]string{"raw"}},
		{"CHARACTER SET alone is that character set's default, never the table's _bin",
			table("  `code` varchar(16) CHARACTER SET latin1 DEFAULT NULL,\n",
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"),
			nil},
		{"table with a character set and no collation", table("  `code` varchar(16) DEFAULT NULL,\n", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"), nil},
		{"no table options at all", "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `code` varchar(16) DEFAULT NULL\n);\n", nil},
		{"case-insensitive collations", table("  `a` varchar(8) COLLATE utf8mb4_general_ci,\n  `b` varchar(8) COLLATE utf8mb4_unicode_ci,\n", ci), nil},
		// _cs is not _bin: it orders letters alphabetically, bytes do not.
		{"_cs is left alone", table("  `tag` varchar(8) COLLATE utf8mb4_0900_as_cs,\n  `old` varchar(8) CHARACTER SET latin1 COLLATE latin1_general_cs,\n", ci), nil},
		{"the word inside a DEFAULT or a COMMENT is not the attribute",
			table("  `a` varchar(40) DEFAULT 'COLLATE utf8mb4_bin',\n  `b` varchar(40) DEFAULT NULL COMMENT 'was COLLATE utf8mb4_bin',\n"+
				"  `c` varchar(40) DEFAULT 'it''s COLLATE utf8mb4_bin' COMMENT 'x',\n", ci),
			nil},
		{"a column named collate, an enum label named after a collation",
			table("  `collate` varchar(8) DEFAULT NULL,\n  `e` enum('COLLATE utf8mb4_bin','b') DEFAULT NULL,\n", ci), nil},
		{"a table COMMENT that mentions a collation",
			table("  `code` varchar(16) DEFAULT NULL,\n", ci+" COMMENT='COLLATE=utf8mb4_bin'"), nil},
		{"table collation after other options, lower-case keyword",
			table("  `code` varchar(16) DEFAULT NULL,\n", "ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=latin1 collate=latin1_bin ROW_FORMAT=DYNAMIC COMMENT='x'"),
			[]string{"code"}},
		{"numbers and dates under a COLLATE never count",
			table("  `n` int COLLATE utf8mb4_bin,\n  `b` varbinary(8) DEFAULT NULL,\n  `bl` blob,\n", ci), nil},
		{"a name with a quote in it", table("  `we\"ird` varchar(8) COLLATE utf8mb4_bin,\n", ci), []string{`we"ird`}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cols, err := ParseSchemaText(c.sql)
			if err != nil {
				t.Fatal(err)
			}
			if got := BinaryCollationColumns(c.sql, cols); !slices.Equal(got, c.want) {
				t.Errorf("got %q, want %q\n%s", got, c.want, c.sql)
			}
		})
	}
}

// The round trip the views depend on: the collation survives into the Parquet
// footer and comes back out of TableFootersFor.
func TestTableFootersFor_readsBinaryCollations(t *testing.T) {
	dir := t.TempDir()
	createSQL := "CREATE TABLE `codes` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `code` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n" +
		"  `label` varchar(16) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
	path := filepath.Join(dir, "codes.parquet")
	writeFixtureTable(t, path, createSQL, [][]string{{"1", "AB", "x"}})
	got, err := TableFootersFor(context.Background(), []string{path})
	if err != nil {
		t.Fatal(err)
	}
	if b := got[path].BinaryText; !slices.Equal(b, []string{"code"}) {
		t.Errorf("BinaryText = %q, want [code]", b)
	}
}
