package baseline

import (
	"slices"
	"testing"
)

// Which columns LENGTH, OCTET_LENGTH, BIT_LENGTH, CHAR_LENGTH and
// CHARACTER_LENGTH count the same on the copy as on MySQL (#2918): text in a
// character set whose bytes are the UTF-8 the copy holds, resolved from the
// CREATE TABLE as MySQL resolves it (the column's character set, else its
// collation's, else the table's collation's, else the table's character set).
func TestLengthColumns(t *testing.T) {
	table := func(cols, options string) string {
		return "CREATE TABLE `t` (\n  `id` int NOT NULL,\n" + cols + "  PRIMARY KEY (`id`)\n) " + options + ";\n"
	}
	const ai = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	cases := []struct {
		name        string
		sql         string
		safe, other []string
	}{
		{"every text type under the table's utf8mb4",
			table("  `a` char(4),\n  `b` varchar(8),\n  `c` tinytext,\n  `d` text,\n  `e` mediumtext,\n  `f` longtext,\n", ai),
			[]string{"a", "b", "c", "d", "e", "f"}, []string{"id"}},
		{"a UTF-8 character set or ascii named on the column, a _bin collation too",
			table("  `a` varchar(8) CHARACTER SET utf8mb3,\n  `b` varchar(8) CHARACTER SET utf8,\n  `c` varchar(8) CHARACTER SET ascii,\n"+
				"  `d` varchar(8) COLLATE utf8mb4_bin,\n  `e` char(4) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin,\n  `f` varchar(8) COLLATE utf8mb4_general_ci,\n", ai),
			[]string{"a", "b", "c", "d", "e", "f"}, []string{"id"}},
		{"latin1 counts a letter as one byte where the copy's UTF-8 has two",
			table("  `a` varchar(8) CHARACTER SET latin1,\n  `b` varchar(8) CHARACTER SET latin1 COLLATE latin1_bin,\n  `c` varchar(8) COLLATE latin1_swedish_ci,\n", ai),
			nil, []string{"id", "a", "b", "c"}},
		{"other character sets",
			table("  `a` varchar(8) CHARACTER SET utf16,\n  `b` varchar(8) CHARACTER SET gbk,\n  `c` varchar(8) CHARACTER SET binary,\n", ai),
			nil, []string{"id", "a", "b", "c"}},
		{"not text: numbers, dates, a TIME the copy holds as text, JSON, binary, ENUM and SET",
			table("  `n` int,\n  `dc` decimal(65,2),\n  `at` datetime,\n  `tm` time,\n  `j` json,\n  `b` binary(4),\n  `vb` varbinary(8),\n  `bl` blob,\n"+
				"  `e` enum('x','y'),\n  `s` set('x','y'),\n", ai),
			nil, []string{"id", "n", "dc", "at", "tm", "j", "b", "vb", "bl", "e", "s"}},
		{"a latin1 table: its columns are latin1 unless they say otherwise",
			table("  `a` varchar(8) DEFAULT NULL,\n  `b` varchar(8) CHARACTER SET utf8mb4 DEFAULT NULL,\n  `c` varchar(8) COLLATE utf8mb4_bin,\n", "ENGINE=InnoDB DEFAULT CHARSET=latin1"),
			[]string{"b", "c"}, []string{"id", "a"}},
		{"the table's collation alone names its character set",
			table("  `a` varchar(8) DEFAULT NULL,\n", "ENGINE=InnoDB COLLATE=utf8mb3_general_ci"),
			[]string{"a"}, []string{"id"}},
		{"MariaDB prints utf8mb3, and a JSON column is LONGTEXT there",
			table("  `a` varchar(8) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,\n"+
				"  `j` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`j`)),\n",
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"),
			[]string{"a", "j"}, []string{"id"}},
		{"upper case",
			table("  `a` VARCHAR(8) CHARACTER SET UTF8MB4,\n  `b` VARCHAR(8) CHARACTER SET LATIN1,\n", ai),
			[]string{"a"}, []string{"id", "b"}},
		{"no character set anywhere (a hand-written definition): not known",
			table("  `a` varchar(8) DEFAULT NULL,\n", "ENGINE=InnoDB"),
			nil, []string{"id", "a"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cols, err := ParseSchemaText(c.sql)
			if err != nil {
				t.Fatal(err)
			}
			safe, other := LengthColumns(c.sql, cols)
			if !slices.Equal(safe, c.safe) || !slices.Equal(other, c.other) {
				t.Errorf("LengthColumns = %q, %q; want %q, %q", safe, other, c.safe, c.other)
			}
		})
	}
}

// A value kept verbatim as text (a PostgreSQL source) is not MySQL text.
func TestLengthColumns_rawTextIsNotCounted(t *testing.T) {
	safe, other := LengthColumns("", []Column{{Name: "a", MySQLType: "varchar", Charset: "utf8mb4", RawText: true}})
	if len(safe) != 0 || !slices.Equal(other, []string{"a"}) {
		t.Errorf("LengthColumns = %q, %q; want none safe", safe, other)
	}
}
