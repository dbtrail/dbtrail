package ddltext

import (
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
)

// TestRowsChangedWithoutEvents_ignoreFound: MariaDB's ALTER IGNORE TABLE
// keeps the first row of each duplicate of a unique key and deletes the
// others, and the deletes are not written as row events (#2640). It counts
// when the statement adds a unique key (UNIQUE, PRIMARY KEY, a column's bare
// KEY, SERIAL), or changes values a unique key already compares (MODIFY,
// CHANGE, a character set or collation), and when the stored text was cut
// short, since the clause may have been in the part cut off.
func TestRowsChangedWithoutEvents_ignoreFound(t *testing.T) {
	tb := func(schema, table string) event.DDLTable { return event.DDLTable{Schema: schema, Table: table} }
	for _, tc := range []struct {
		name, query string
		wantOp      string
		wantTables  []event.DDLTable
	}{
		{"add unique", "ALTER IGNORE TABLE t ADD UNIQUE (c)", "IGNORE ... UNIQUE", []event.DDLTable{tb("", "t")}},
		{"add unique with no space before the list", "ALTER IGNORE TABLE t ADD UNIQUE(c)", "IGNORE ... UNIQUE", []event.DDLTable{tb("", "t")}},
		{"add unique key", "ALTER IGNORE TABLE t ADD UNIQUE KEY uk (c)", "IGNORE ... UNIQUE", []event.DDLTable{tb("", "t")}},
		{"add unique index", "ALTER IGNORE TABLE t ADD UNIQUE INDEX uk (c)", "IGNORE ... UNIQUE", []event.DDLTable{tb("", "t")}},
		{"add constraint unique", "ALTER IGNORE TABLE t ADD CONSTRAINT uk UNIQUE (c)", "IGNORE ... UNIQUE", []event.DDLTable{tb("", "t")}},
		{"add primary key", "ALTER IGNORE TABLE t ADD PRIMARY KEY (id)", "IGNORE ... PRIMARY KEY", []event.DDLTable{tb("", "t")}},
		{"add primary key with no space", "ALTER IGNORE TABLE t ADD PRIMARY KEY(id)", "IGNORE ... PRIMARY KEY", []event.DDLTable{tb("", "t")}},
		{"unique in a new column", "ALTER IGNORE TABLE t ADD COLUMN c INT NOT NULL DEFAULT 0 UNIQUE", "IGNORE ... UNIQUE", []event.DDLTable{tb("", "t")}},
		{"unique in a modified column", "ALTER IGNORE TABLE t MODIFY c INT UNIQUE", "IGNORE ... MODIFY", []event.DDLTable{tb("", "t")}},
		{"primary key in a changed column", "ALTER IGNORE TABLE t CHANGE c id INT PRIMARY KEY", "IGNORE ... CHANGE", []event.DDLTable{tb("", "t")}},
		// In a column definition a bare KEY is PRIMARY KEY.
		{"a column's bare KEY", "ALTER IGNORE TABLE t ADD COLUMN k INT NOT NULL KEY", "IGNORE ... PRIMARY KEY", []event.DDLTable{tb("", "t")}},
		// SERIAL DEFAULT VALUE is NOT NULL AUTO_INCREMENT UNIQUE, and the
		// SERIAL type is BIGINT UNSIGNED with the same attributes.
		{"SERIAL DEFAULT VALUE", "ALTER IGNORE TABLE t ADD COLUMN s INT SERIAL DEFAULT VALUE", "IGNORE ... SERIAL", []event.DDLTable{tb("", "t")}},
		{"the SERIAL type", "ALTER IGNORE TABLE t ADD COLUMN s SERIAL", "IGNORE ... SERIAL", []event.DDLTable{tb("", "t")}},
		// A narrower type, or a collation that compares more values equal,
		// makes duplicates of an existing unique key, which IGNORE deletes.
		// The text does not say whether the column is in one.
		{"a narrower type", "ALTER IGNORE TABLE t MODIFY c TINYINT NOT NULL", "IGNORE ... MODIFY", []event.DDLTable{tb("", "t")}},
		{"a changed column", "ALTER IGNORE TABLE t CHANGE COLUMN c c INT", "IGNORE ... CHANGE", []event.DDLTable{tb("", "t")}},
		{"convert to another character set", "ALTER IGNORE TABLE t CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci", "IGNORE ... CONVERT TO", []event.DDLTable{tb("", "t")}},
		{"a table collation", "ALTER IGNORE TABLE t COLLATE utf8mb4_general_ci", "IGNORE ... COLLATE", []event.DDLTable{tb("", "t")}},
		{"a table character set", "ALTER IGNORE TABLE t DEFAULT CHARSET=latin1", "IGNORE ... CHARACTER SET", []event.DDLTable{tb("", "t")}},
		{"a column character set", "ALTER IGNORE TABLE t ALTER COLUMN c SET DEFAULT 'x', ADD COLUMN d VARCHAR(9) CHARACTER SET latin1", "IGNORE ... CHARACTER SET", []event.DDLTable{tb("", "t")}},
		// Stored cut short (capDDLQuery): the UNIQUE may be past the cut.
		{"IGNORE with the text cut short", "ALTER IGNORE TABLE t ADD COLUMN c1 INT COMMENT 'long" + event.QueryTextTruncationMarker, "IGNORE (statement text cut short)", []event.DDLTable{tb("", "t")}},
		{"lower case, lines and spaces", "alter\n  ignore\ttable t\n add   unique (c)", "IGNORE ... UNIQUE", []event.DDLTable{tb("", "t")}},
		{"a comment between the words", "ALTER /* dedupe */ IGNORE TABLE t ADD /* x */ UNIQUE (c)", "IGNORE ... UNIQUE", []event.DDLTable{tb("", "t")}},
		{"ONLINE IGNORE", "ALTER ONLINE IGNORE TABLE t ADD UNIQUE (c)", "IGNORE ... UNIQUE", []event.DDLTable{tb("", "t")}},
		{"qualified and backticked", "ALTER IGNORE TABLE `shop`.`orders` ADD UNIQUE KEY `uk` (`email`)", "IGNORE ... UNIQUE", []event.DDLTable{tb("shop", "orders")}},
		{"MariaDB IF EXISTS on the table", "ALTER IGNORE TABLE IF EXISTS shop.t ADD UNIQUE (c)", "IGNORE ... UNIQUE", []event.DDLTable{tb("shop", "t")}},
		{"executable comment", "/*!50100 ALTER IGNORE TABLE t ADD UNIQUE (c) */", "IGNORE ... UNIQUE", []event.DDLTable{tb("", "t")}},
		{"MariaDB executable comment", "/*M!100100 ALTER IGNORE TABLE t ADD UNIQUE (c) */", "IGNORE ... UNIQUE", []event.DDLTable{tb("", "t")}},
		{"after a clause that keeps the rows", "ALTER IGNORE TABLE t ADD COLUMN d INT, ADD UNIQUE (c)", "IGNORE ... UNIQUE", []event.DDLTable{tb("", "t")}},
		{"after a dropped primary key", "ALTER IGNORE TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (a, b)", "IGNORE ... PRIMARY KEY", []event.DDLTable{tb("", "t")}},
		// The first clause found names the statement; a partition clause
		// before it keeps its own name.
		{"a dropped partition first", "ALTER IGNORE TABLE t DROP PARTITION p0, ADD UNIQUE (c)", "DROP PARTITION", []event.DDLTable{tb("", "t")}},
		// Read a second time with the backslash a plain character, as for
		// the partition clauses: there the quote ends at the backslash and
		// the UNIQUE is real.
		{"a backslash before a quote", `ALTER IGNORE TABLE t COMMENT 'C:\', ADD UNIQUE (c)`, "IGNORE ... UNIQUE", []event.DDLTable{tb("", "t")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op, tables := RowsChangedWithoutEvents(tc.query)
			if op != tc.wantOp {
				t.Errorf("op = %q, want %q", op, tc.wantOp)
			}
			if !slices.Equal(tables, tc.wantTables) {
				t.Errorf("tables = %q, want %q", tables, tc.wantTables)
			}
		})
	}
}

// TestRowsChangedWithoutEvents_ignoreNotFound: without IGNORE a duplicate
// fails the statement (and MySQL has had no ALTER IGNORE since 5.7), and
// under IGNORE a statement that adds no unique key and changes no existing
// value deletes no duplicate. Each one counted here would cost a full read
// of the source.
func TestRowsChangedWithoutEvents_ignoreNotFound(t *testing.T) {
	for _, tc := range []struct{ name, query string }{
		{"unique without IGNORE", "ALTER TABLE t ADD UNIQUE (c)"},
		{"primary key without IGNORE", "ALTER TABLE t ADD PRIMARY KEY (id)"},
		{"ONLINE without IGNORE", "ALTER ONLINE TABLE t ADD UNIQUE (c)"},
		{"IGNORE in a comment, not the statement", "ALTER /* ignore */ TABLE t ADD UNIQUE (c)"},
		{"a table called ignore", "ALTER TABLE `ignore` ADD UNIQUE (c)"},
		{"a table whose name starts with ignore", "ALTER TABLE ignore_log ADD UNIQUE (c)"},
		{"IGNORE adding a column", "ALTER IGNORE TABLE t ADD COLUMN c INT"},
		{"IGNORE adding a plain index", "ALTER IGNORE TABLE t ADD INDEX i (c), ADD KEY k (d), ADD FULLTEXT f (e)"},
		{"IGNORE dropping the primary key", "ALTER IGNORE TABLE t DROP PRIMARY KEY"},
		{"IGNORE dropping a unique index", "ALTER IGNORE TABLE t DROP INDEX uk"},
		{"IGNORE with keys that are not unique", "ALTER IGNORE TABLE t ADD FULLTEXT KEY f (e), ADD SPATIAL KEY s (g), ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES p (id)"},
		{"IGNORE dropping and renaming keys", "ALTER IGNORE TABLE t DROP FOREIGN KEY fk, DROP KEY k2, RENAME KEY a TO b"},
		{"IGNORE partitioning by key", "ALTER IGNORE TABLE t PARTITION BY KEY (id) PARTITIONS 4"},
		{"IGNORE partitioning by linear key", "ALTER IGNORE TABLE t PARTITION BY LINEAR KEY (id) PARTITIONS 4"},
		{"IGNORE with a key block size", "ALTER IGNORE TABLE t KEY_BLOCK_SIZE=8, ENGINE=InnoDB"},
		{"a text cut short without IGNORE", "ALTER TABLE t ADD COLUMN c INT COMMENT 'long" + event.QueryTextTruncationMarker},
		{"columns named like the keywords", "ALTER IGNORE TABLE t ADD COLUMN unique_code INT, ADD COLUMN `unique` INT, ADD COLUMN `primary` INT"},
		{"the word in a comment", "ALTER IGNORE TABLE t ADD COLUMN c INT COMMENT 'unique per customer'"},
		{"the whole phrase in a comment", "ALTER TABLE t COMMENT 'alter ignore table t add unique (c)'"},
		{"not an alter", "CREATE UNIQUE INDEX uk ON t (c)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if op, tables := RowsChangedWithoutEvents(tc.query); op != "" || tables != nil {
				t.Errorf("RowsChangedWithoutEvents(%q) = %q, %q, want nothing", tc.query, op, tables)
			}
		})
	}
}
