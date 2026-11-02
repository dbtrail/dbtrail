package baseline

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
)

// How each text column compares on MySQL against the copy (#2533), from the
// CREATE TABLE as SHOW CREATE TABLE prints it: the column's own collation, or
// its own character set's default, or the table's collation, or the table's
// character set's default.
func TestTextColumns(t *testing.T) {
	table := func(cols, options string) string {
		return "CREATE TABLE `t` (\n  `id` int NOT NULL,\n" + cols + "  PRIMARY KEY (`id`)\n) " + options + ";\n"
	}
	const ai = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	tc := func(name string, kind TextKind, positional bool) TextColumn {
		return TextColumn{Name: name, Kind: kind, Positional: positional}
	}
	cases := []struct {
		name string
		sql  string
		want []TextColumn
	}{
		{"the MySQL 8 default and a numeric column beside it",
			table("  `name` varchar(40) DEFAULT NULL,\n  `n` int DEFAULT NULL,\n", ai),
			[]TextColumn{tc("name", TextFolded, false)}},
		{"every text type, enum and set (positional)",
			table("  `a` char(4),\n  `b` tinytext,\n  `c` text,\n  `d` mediumtext,\n  `e` longtext,\n  `f` enum('x','y'),\n  `g` set('x','y'),\n", ai),
			[]TextColumn{tc("a", TextFolded, false), tc("b", TextFolded, false), tc("c", TextFolded, false), tc("d", TextFolded, false),
				tc("e", TextFolded, false), tc("f", TextFolded, true), tc("g", TextFolded, true)}},
		{"not text: numbers, dates, JSON, binary",
			table("  `n` int COLLATE utf8mb4_bin,\n  `at` datetime,\n  `j` json,\n  `b` varbinary(8),\n  `bl` blob,\n", ai), nil},
		{"the measured case-insensitive collations without language rules",
			table("  `a` varchar(8) COLLATE utf8mb4_general_ci,\n  `b` varchar(8) COLLATE utf8mb4_unicode_ci,\n"+
				"  `c` varchar(8) COLLATE utf8mb4_unicode_520_ci,\n  `d` varchar(8) COLLATE utf8mb4_0900_as_ci,\n"+
				"  `e` varchar(8) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,\n  `f` varchar(8) CHARACTER SET latin1 COLLATE latin1_general_ci,\n"+
				"  `g` varchar(8) CHARACTER SET ascii COLLATE ascii_general_ci,\n  `h` varchar(8) COLLATE utf8mb4_uca1400_ai_ci,\n", ai),
			[]TextColumn{tc("a", TextFolded, false), tc("b", TextFolded, false), tc("c", TextFolded, false), tc("d", TextFolded, false),
				tc("e", TextFolded, false), tc("f", TextFolded, false), tc("g", TextFolded, false), tc("h", TextFolded, false)}},
		{"upper case names",
			table("  `a` VARCHAR(8) COLLATE UTF8MB4_GENERAL_CI,\n  `b` VARCHAR(8) COLLATE UTF8MB4_BIN,\n", ai),
			[]TextColumn{tc("a", TextFolded, false), tc("b", TextBytes, false)}},
		{"_bin: utf8 ones by code point, any other character set's by its own byte order",
			table("  `a` varchar(8) COLLATE utf8mb4_bin,\n  `b` char(4) COLLATE utf8mb4_0900_bin,\n  `c` text COLLATE utf8mb4_nopad_bin,\n"+
				"  `d` varchar(8) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin,\n  `e` varchar(8) CHARACTER SET latin1 COLLATE latin1_bin,\n", ai),
			[]TextColumn{tc("a", TextBytes, false), tc("b", TextBytes, false), tc("c", TextBytes, false),
				tc("d", TextBytes, false), tc("e", TextBytesASCII, false)}},
		{"case-sensitive and language collations compare another way whatever the text",
			table("  `a` varchar(8) COLLATE utf8mb4_0900_as_cs,\n  `b` varchar(8) CHARACTER SET latin1 COLLATE latin1_general_cs,\n"+
				"  `c` varchar(8) COLLATE utf8mb4_tr_0900_ai_ci,\n  `d` varchar(8) COLLATE utf8mb4_turkish_ci,\n"+
				"  `e` varchar(8) COLLATE utf8mb4_da_0900_ai_ci,\n  `f` varchar(8) COLLATE utf8mb4_spanish2_ci,\n"+
				"  `g` varchar(8) CHARACTER SET latin1 COLLATE latin1_german2_ci,\n  `h` varchar(8) COLLATE utf8mb4_swedish_ci,\n", ai),
			[]TextColumn{tc("a", TextUnlike, false), tc("b", TextUnlike, false), tc("c", TextUnlike, false), tc("d", TextUnlike, false),
				tc("e", TextUnlike, false), tc("f", TextUnlike, false), tc("g", TextUnlike, false), tc("h", TextUnlike, false)}},
		{"the table's collation reaches the columns that name none",
			table("  `a` varchar(8) DEFAULT NULL,\n  `b` varchar(8) COLLATE utf8mb4_general_ci,\n", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs"),
			[]TextColumn{tc("a", TextUnlike, false), tc("b", TextFolded, false)}},
		{"a character set alone is its default collation, never the table's",
			table("  `a` varchar(8) CHARACTER SET latin1 DEFAULT NULL,\n  `b` varchar(8) CHARSET utf8mb4,\n  `c` varchar(8) CHARACTER SET sjis,\n",
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"),
			[]TextColumn{tc("a", TextFolded, false), tc("b", TextFolded, false), tc("c", TextUnlike, false)}},
		{"a table with a character set and no collation (MySQL 5.7 prints it so)",
			table("  `a` varchar(8) DEFAULT NULL,\n", "ENGINE=InnoDB DEFAULT CHARSET=latin1"),
			[]TextColumn{tc("a", TextFolded, false)}},
		{"a table character set with no folding default",
			table("  `a` varchar(8) DEFAULT NULL,\n", "ENGINE=InnoDB DEFAULT CHARSET=cp1251"),
			[]TextColumn{tc("a", TextUnlike, false)}},
		{"no table options at all (no server prints that): the server's default character set",
			"CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `code` varchar(16) DEFAULT NULL\n);\n",
			[]TextColumn{tc("code", TextFolded, false)}},
		{"the words inside a DEFAULT or a COMMENT are not the attribute",
			table("  `a` varchar(40) DEFAULT 'COLLATE utf8mb4_0900_as_cs',\n  `b` varchar(40) DEFAULT NULL COMMENT 'CHARACTER SET sjis',\n", ai),
			[]TextColumn{tc("a", TextFolded, false), tc("b", TextFolded, false)}},
		{"a table COMMENT that names a character set",
			table("  `a` varchar(8) DEFAULT NULL,\n", ai+" COMMENT='CHARSET=sjis'"),
			[]TextColumn{tc("a", TextFolded, false)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cols, err := ParseSchemaText(c.sql)
			if err != nil {
				t.Fatal(err)
			}
			got := TextColumns(c.sql, cols)
			for i := range got {
				got[i].Padded = false // TestTextColumns_padded has it
			}
			if !slices.Equal(got, c.want) {
				t.Errorf("got %+v\nwant %+v\n%s", got, c.want, c.sql)
			}
			// The guard of a byte-compared kind is right only where the
			// copy compares the column by its bytes (BinaryText gives it
			// COLLATE C): the two readings of the collation must agree.
			binary := map[string]bool{}
			for _, n := range BinaryCollationColumns(c.sql, cols) {
				binary[n] = true
			}
			for _, tc := range TextColumns(c.sql, cols) {
				if bytes := tc.Kind == TextBytes || tc.Kind == TextBytesASCII; bytes != binary[tc.Name] {
					t.Errorf("%s: kind %s, but in BinaryText = %v", tc.Name, tc.Kind, binary[tc.Name])
				}
			}
		})
	}
}

// A value kept verbatim as text (a PostgreSQL source) has no MySQL collation:
// nothing says the copy compares it as the source does.
func TestTextColumns_rawText(t *testing.T) {
	cols := []Column{{Name: "a", MySQLType: "varchar", RawText: true}, {Name: "n", MySQLType: "int", RawText: true}}
	got := TextColumns("", cols)
	want := []TextColumn{{Name: "a", Kind: TextUnlike, Padded: true}, {Name: "n", Kind: TextUnlike, Padded: true}}
	if !slices.Equal(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// The round trip the routed check depends on: the kinds come back out of
// TableFootersFor.
func TestTableFootersFor_readsTextColumns(t *testing.T) {
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
	want := []TextColumn{{Name: "code", Kind: TextBytes, Padded: true}, {Name: "label", Kind: TextFolded}}
	if g := got[path].Text; !slices.Equal(g, want) {
		t.Errorf("Text = %+v, want %+v", g, want)
	}
}

// Which collations ignore a trailing space (PAD SPACE), as information_schema
// .COLLATIONS.PAD_ATTRIBUTE says on MySQL 8.0: every one but the 0900 ones,
// and on MariaDB every one but the _nopad_ ones. A character set's default
// depends on the server, so it is taken to pad.
func TestTextColumns_padded(t *testing.T) {
	sql := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n" +
		"  `a` varchar(8) COLLATE utf8mb4_0900_ai_ci,\n  `b` varchar(8) COLLATE utf8mb4_0900_as_ci,\n  `c` varchar(8) COLLATE utf8mb4_0900_bin,\n" +
		"  `d` varchar(8) COLLATE utf8mb4_general_ci,\n  `e` varchar(8) COLLATE utf8mb4_bin,\n  `f` varchar(8) COLLATE utf8mb4_uca1400_ai_ci,\n" +
		"  `g` varchar(8) COLLATE utf8mb4_nopad_bin,\n  `h` varchar(8) CHARACTER SET latin1,\n  `i` varchar(8) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
	cols, err := ParseSchemaText(sql)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"a": false, "b": false, "c": false, "d": true, "e": true, "f": true, "g": false, "h": true, "i": true}
	for _, tc := range TextColumns(sql, cols) {
		if tc.Padded != want[tc.Name] {
			t.Errorf("%s: Padded %v, want %v", tc.Name, tc.Padded, want[tc.Name])
		}
	}
}
