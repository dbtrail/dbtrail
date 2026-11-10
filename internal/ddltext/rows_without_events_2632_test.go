package ddltext

import (
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
)

// TestRowsChangedWithoutEvents_found: the ALTER TABLE clauses that change a
// table's rows with no row events (#2632), and the tables each one names.
// An unqualified name has an empty schema: the caller resolves it.
func TestRowsChangedWithoutEvents_found(t *testing.T) {
	tb := func(schema, table string) event.DDLTable { return event.DDLTable{Schema: schema, Table: table} }
	for _, tc := range []struct {
		name, query string
		wantOp      string
		wantTables  []event.DDLTable
	}{
		{"drop partition", "ALTER TABLE t DROP PARTITION p0", "DROP PARTITION", []event.DDLTable{tb("", "t")}},
		{"lower case and a list", "alter table t drop partition p0, p1", "DROP PARTITION", []event.DDLTable{tb("", "t")}},
		{"extra spaces and lines", "ALTER  TABLE\n t\n\tDROP   PARTITION\n p0 ;  ", "DROP PARTITION", []event.DDLTable{tb("", "t")}},
		{"block comments between the words", "/* app */ ALTER TABLE t /* x */ DROP /* y */ PARTITION p0", "DROP PARTITION", []event.DDLTable{tb("", "t")}},
		{"line comment before it", "ALTER TABLE t -- rotate\nDROP PARTITION p0", "DROP PARTITION", []event.DDLTable{tb("", "t")}},
		{"qualified and backticked", "ALTER TABLE `shop`.`orders` DROP PARTITION `p 2020`", "DROP PARTITION", []event.DDLTable{tb("shop", "orders")}},
		{"MariaDB IF EXISTS", "ALTER TABLE t DROP PARTITION IF EXISTS p0", "DROP PARTITION", []event.DDLTable{tb("", "t")}},
		{"after another clause", "ALTER TABLE t ALGORITHM=INPLACE, DROP PARTITION p0", "DROP PARTITION", []event.DDLTable{tb("", "t")}},
		{"ONLINE IGNORE", "ALTER ONLINE IGNORE TABLE t DROP PARTITION p0", "DROP PARTITION", []event.DDLTable{tb("", "t")}},
		{"MariaDB IF EXISTS on the table", "ALTER TABLE IF EXISTS shop.t EXCHANGE PARTITION p0 WITH TABLE t2", "EXCHANGE PARTITION", []event.DDLTable{tb("shop", "t"), tb("", "t2")}},
		{"a table called if", "ALTER TABLE `if` DROP PARTITION p0", "DROP PARTITION", []event.DDLTable{tb("", "if")}},
		{"executable comment", "/*!50100 ALTER TABLE t DROP PARTITION p0 */", "DROP PARTITION", []event.DDLTable{tb("", "t")}},
		{"truncate partition all", "ALTER TABLE t TRUNCATE PARTITION ALL", "TRUNCATE PARTITION", []event.DDLTable{tb("", "t")}},
		{"exchange names the other table", "ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE t2", "EXCHANGE PARTITION", []event.DDLTable{tb("", "t"), tb("", "t2")}},
		{"exchange with a qualified other table", "ALTER TABLE shop.t EXCHANGE PARTITION p0 WITH TABLE archive.t2 WITHOUT VALIDATION", "EXCHANGE PARTITION", []event.DDLTable{tb("shop", "t"), tb("archive", "t2")}},
		{"exchange with a backticked other table", "ALTER TABLE t EXCHANGE PARTITION `p0` WITH TABLE `my t2`", "EXCHANGE PARTITION", []event.DDLTable{tb("", "t"), tb("", "my t2")}},
		{"MariaDB convert partition to table", "ALTER TABLE t CONVERT PARTITION p0 TO TABLE t2", "CONVERT PARTITION", []event.DDLTable{tb("", "t"), tb("", "t2")}},
		{"MariaDB convert table to partition", "ALTER TABLE t CONVERT TABLE t2 TO PARTITION p9 VALUES LESS THAN (100)", "CONVERT TABLE", []event.DDLTable{tb("", "t"), tb("", "t2")}},
		{"discard tablespace", "ALTER TABLE t DISCARD TABLESPACE", "DISCARD TABLESPACE", []event.DDLTable{tb("", "t")}},
		{"import tablespace", "ALTER TABLE t IMPORT TABLESPACE", "IMPORT TABLESPACE", []event.DDLTable{tb("", "t")}},
		{"discard some partitions", "ALTER TABLE t DISCARD PARTITION p0, p1 TABLESPACE", "DISCARD TABLESPACE", []event.DDLTable{tb("", "t")}},
		{"import all partitions", "ALTER TABLE t IMPORT PARTITION ALL TABLESPACE", "IMPORT TABLESPACE", []event.DDLTable{tb("", "t")}},
		// Whether a backslash escapes a quote depends on the session's
		// sql_mode, which the text does not say: with NO_BACKSLASH_ESCAPES
		// this string ends at the backslash and the DROP is real. Read both
		// ways, and either one finding it counts.
		{"a backslash before a quote", `ALTER TABLE t COMMENT 'C:\', DROP PARTITION p0`, "DROP PARTITION", []event.DDLTable{tb("", "t")}},
		// The cost of that reading: an escaped quote in a comment that holds
		// the words counts too. One full read, not a wrong copy.
		{"a backslash makes a comment count", `ALTER TABLE t COMMENT 'it\'s, drop partition later'`, "DROP PARTITION", []event.DDLTable{tb("", "t")}},
		// Under NO_BACKSLASH_ESCAPES the backslash ends nothing, the second
		// quote closes the string, and the # is inside the next one: read
		// with the backslash as an escape, the # starts a comment that hides
		// the clause.
		{"a backslash that shifts the quotes before a #", `ALTER TABLE t COMMENT 'a\' COMMENT 'b#' DROP PARTITION p0`, "DROP PARTITION", []event.DDLTable{tb("", "t")}},
		{"a backslash that shifts the quotes before --", `ALTER TABLE t COMMENT 'a\' COMMENT 'b-- ' DROP PARTITION p0`, "DROP PARTITION", []event.DDLTable{tb("", "t")}},
		{"ANSI_QUOTES names with a backslash keep the other table", `ALTER TABLE "a\" EXCHANGE PARTITION "p#" WITH TABLE t2`, "EXCHANGE PARTITION", []event.DDLTable{tb("", `a\`), tb("", "t2")}},
		// A quote left open hides nothing: what follows it is read as words.
		{"a quote left open", "ALTER TABLE t COMMENT 'x, DROP PARTITION p0", "DROP PARTITION", []event.DDLTable{tb("", "t")}},
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

// TestRowsChangedWithoutEvents_notFound: statements that keep every row, or
// write their changes as row events, or are not an ALTER TABLE at all. Each
// one counted here would cost a full read of the source.
func TestRowsChangedWithoutEvents_notFound(t *testing.T) {
	for _, tc := range []struct{ name, query string }{
		{"add partition", "ALTER TABLE t ADD PARTITION (PARTITION p3 VALUES LESS THAN (2030))"},
		{"reorganize", "ALTER TABLE t REORGANIZE PARTITION p0 INTO (PARTITION a VALUES LESS THAN (5), PARTITION b VALUES LESS THAN (10))"},
		{"coalesce", "ALTER TABLE t COALESCE PARTITION 2"},
		{"rebuild", "ALTER TABLE t REBUILD PARTITION p0, p1"},
		{"optimize analyze check repair", "ALTER TABLE t OPTIMIZE PARTITION p0, ANALYZE PARTITION p0, CHECK PARTITION p0, REPAIR PARTITION p0"},
		{"remove partitioning", "ALTER TABLE t REMOVE PARTITIONING"},
		{"partition by", "ALTER TABLE t PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)"},
		{"a column called partition", "ALTER TABLE t ADD COLUMN `partition` INT"},
		{"a column called drop_partition", "ALTER TABLE t ADD COLUMN drop_partition INT"},
		{"the words in a comment", "ALTER TABLE t ADD COLUMN c INT COMMENT 'drop partition p0 later'"},
		{"the words in a partition value", "ALTER TABLE t ADD PARTITION (PARTITION p VALUES IN ('drop partition'))"},
		{"columns named like the keywords", "ALTER TABLE t ADD COLUMN import INT, ADD COLUMN discard INT, ADD COLUMN exchange INT"},
		{"convert to a character set", "ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4"},
		{"drops that write no partition", "ALTER TABLE t DROP COLUMN c, DROP INDEX i, DROP PRIMARY KEY"},
		{"move to a general tablespace", "ALTER TABLE t TABLESPACE ts1"},
		{"not an alter", "TRUNCATE TABLE t"},
		{"a create with partitions", "CREATE TABLE t (id INT) PARTITION BY HASH (id) PARTITIONS 4"},
		{"empty", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if op, tables := RowsChangedWithoutEvents(tc.query); op != "" || tables != nil {
				t.Errorf("RowsChangedWithoutEvents(%q) = %q, %q, want nothing", tc.query, op, tables)
			}
		})
	}
}
