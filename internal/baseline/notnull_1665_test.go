package baseline

import "testing"

// TestParseSchemaText_notNull reads whether a column of a backup's CREATE TABLE
// is declared NOT NULL (#1665): the words themselves, after the type, and not a
// string that happens to contain them.
func TestParseSchemaText_notNull(t *testing.T) {
	for _, tc := range []struct {
		name string
		line string
		want bool
	}{
		{"not null", "`c` int NOT NULL,", true},
		{"default null", "`c` int DEFAULT NULL,", false},
		{"no attribute at all", "`c` int,", false},
		{"explicit NULL", "`c` timestamp NULL DEFAULT NULL,", false},
		{"after a collation, before a default", "`c` varchar(10) COLLATE utf8mb4_bin NOT NULL DEFAULT 'x',", true},
		{"after unsigned, before auto_increment", "`c` int unsigned NOT NULL AUTO_INCREMENT,", true},
		{"lowercase and two spaces", "`c` int not  null,", true},
		{"a tab between the words", "`c` int NOT\tNULL,", true},
		{"before a version comment", "`c` json NOT NULL /*!80023 INVISIBLE */,", true},
		{"inside a default string", "`c` varchar(10) DEFAULT 'NOT NULL',", false},
		{"inside a default string with an escaped quote", "`c` varchar(20) DEFAULT 'it''s NOT NULL',", false},
		{"inside a comment", "`c` int DEFAULT NULL COMMENT 'kept NOT NULL by the app',", false},
		{"inside an enum label", "`c` enum('NOT NULL','x') DEFAULT NULL,", false},
		{"NOT NULL after a string that holds a quote", "`c` varchar(20) DEFAULT 'it''s' NOT NULL,", true},
		{"a longer word starting with NULL", "`c` int NOT NULLS,", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cols, err := ParseSchemaText("CREATE TABLE `t` (\n  `id` int NOT NULL,\n  " + tc.line + "\n  PRIMARY KEY (`id`)\n);\n")
			if err != nil {
				t.Fatal(err)
			}
			if len(cols) != 2 {
				t.Fatalf("parsed %d columns, want 2", len(cols))
			}
			if !cols[0].NotNull {
				t.Error("the id column, declared NOT NULL, reads as nullable")
			}
			if cols[1].NotNull != tc.want {
				t.Errorf("NotNull = %v, want %v for %q", cols[1].NotNull, tc.want, tc.line)
			}
		})
	}
}
