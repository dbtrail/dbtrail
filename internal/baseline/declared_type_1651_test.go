package baseline

import "testing"

// TestComparableColumnType pins the spelling both sides of the fold's type
// check go through (#1651): the CREATE TABLE a baseline carries and the
// COLUMN_TYPE the schema snapshot holds. A pair that reads as different here
// refuses a refresh; a pair that reads as equal publishes the old type.
func TestComparableColumnType(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		same bool
	}{
		// Spelling only.
		{"int(11)", "int", true},
		{"INT(11)", "int", true},
		{"int(10) unsigned", "int unsigned", true},
		{"bigint(20) unsigned zerofill", "bigint unsigned zerofill", true},
		{"bigint(20) unsigned zerofill", "bigint unsigned", true},
		{"tinyint(1)", "tinyint(4)", true},
		{"year(4)", "year", true},
		{"integer", "int", true},
		{"decimal", "decimal(10,0)", true},
		{"decimal(12)", "decimal(12,0)", true},
		{"numeric(12,4)", "decimal(12,4)", true},
		{"decimal(12, 4)", "decimal(12,4)", true},
		{"enum('a)','b')", "enum('a)','b')", true},
		{"enum('it''s')", "enum('it''s')", true},
		// A real change, including every widening a restore would trip on.
		{"int", "bigint", false},
		{"tinyint", "int", false},
		{"int unsigned", "int", false},
		{"int unsigned", "bigint", false},
		{"decimal(10,2)", "decimal(12,4)", false},
		{"varchar(32)", "int", false},
		{"varchar(32)", "varchar(64)", false},
		{"varchar(32)", "text", false},
		{"datetime", "datetime(6)", false},
		{"bit(1)", "bit(8)", false},
		{"text", "mediumtext", false},
		{"enum('a','b')", "enum('a','b','c')", false},
		{"enum('a','b')", "enum('b','a')", false},
		{"enum('A')", "enum('a')", false},
	} {
		got := ComparableColumnType(tc.a) == ComparableColumnType(tc.b)
		if got != tc.same {
			t.Errorf("ComparableColumnType(%q)=%q vs (%q)=%q: same=%v, want %v",
				tc.a, ComparableColumnType(tc.a), tc.b, ComparableColumnType(tc.b), got, tc.same)
		}
	}
}

// TestParseSchemaText_declaredType reads DeclaredType off real mydumper-shaped
// lines: only the type is kept, never DEFAULT/COLLATE/COMMENT, and an enum
// label holding ')' does not cut the argument list short the way colRe's
// group 3 does.
func TestParseSchemaText_declaredType(t *testing.T) {
	const createSQL = "CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL AUTO_INCREMENT,\n" +
		"  `u` bigint(20) unsigned zerofill DEFAULT NULL,\n" +
		"  `amount` decimal(12,4) NOT NULL DEFAULT '0.0000',\n" +
		"  `name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT 'unsigned (sic)',\n" +
		"  `state` enum('open','cl)osed','it''s') NOT NULL,\n" +
		"  `is_unsigned` tinyint(1) DEFAULT NULL,\n" +
		"  `at` datetime(6) DEFAULT CURRENT_TIMESTAMP(6),\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n"
	cols, err := ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"int",
		"bigint(20) unsigned zerofill",
		"decimal(12,4)",
		"varchar(64)",
		"enum('open','cl)osed','it''s')",
		"tinyint(1)",
		"datetime(6)",
	}
	if len(cols) != len(want) {
		t.Fatalf("got %d columns, want %d: %+v", len(cols), len(want), cols)
	}
	for i, w := range want {
		if cols[i].DeclaredType != w {
			t.Errorf("%s: DeclaredType = %q, want %q", cols[i].Name, cols[i].DeclaredType, w)
		}
	}
}
