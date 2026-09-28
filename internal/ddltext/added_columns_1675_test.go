package ddltext

import (
	"slices"
	"strings"
	"testing"
)

// TestAddedColumns_readable: the statements AddedColumns reads, and the
// columns it names for each.
func TestAddedColumns_readable(t *testing.T) {
	for _, tc := range []struct {
		name, query, defaultSchema string
		wantSchema, wantTable      string
		wantCols                   []string
	}{
		{"plain", "ALTER TABLE t ADD COLUMN c INT", "shop", "shop", "t", []string{"c"}},
		{"without the COLUMN word", "ALTER TABLE t ADD c INT", "shop", "shop", "t", []string{"c"}},
		{"qualified wins over the default schema", "ALTER TABLE other.t ADD COLUMN c INT", "shop", "other", "t", []string{"c"}},
		{"backticks", "ALTER TABLE `shop`.`t` ADD COLUMN `my col` INT", "", "shop", "t", []string{"my col"}},
		{"a doubled backtick is part of the name", "ALTER TABLE t ADD COLUMN `a``b` INT", "shop", "shop", "t", []string{"a`b"}},
		{"upper case name is kept as written", "ALTER TABLE t ADD COLUMN EXTRA INT", "shop", "shop", "t", []string{"EXTRA"}},
		{"lower case keywords", "alter table t add column c int not null default 0", "shop", "shop", "t", []string{"c"}},
		{"extra spaces and lines", "ALTER   TABLE\n\tt\n  ADD   COLUMN\n  c\n  INT ;  ", "shop", "shop", "t", []string{"c"}},
		{"block comments", "/* app */ ALTER TABLE t /* why */ ADD COLUMN c INT /* tail */", "shop", "shop", "t", []string{"c"}},
		{"ONLINE IGNORE", "ALTER ONLINE IGNORE TABLE t ADD COLUMN c INT", "shop", "shop", "t", []string{"c"}},
		{"two columns", "ALTER TABLE t ADD COLUMN c INT, ADD d VARCHAR(10) AFTER c", "shop", "shop", "t", []string{"c", "d"}},
		{"a comma inside a type", "ALTER TABLE t ADD COLUMN c DECIMAL(10,2), ADD COLUMN e ENUM('a,b','c')", "shop", "shop", "t", []string{"c", "e"}},
		{"a clause inside a comment string", "ALTER TABLE t ADD COLUMN c INT COMMENT 'x, DROP COLUMN y'", "shop", "shop", "t", []string{"c"}},
		{"a doubled quote inside a string", "ALTER TABLE t ADD COLUMN c VARCHAR(9) DEFAULT 'it''s, ok', ADD COLUMN d INT", "shop", "shop", "t", []string{"c", "d"}},
		{"ALGORITHM and LOCK", "ALTER TABLE t ADD COLUMN c INT, ALGORITHM=INSTANT, LOCK = NONE", "shop", "shop", "t", []string{"c"}},
		{"a quoted name that is a keyword", "ALTER TABLE t ADD COLUMN `key` INT", "shop", "shop", "t", []string{"key"}},
		{"generated", "ALTER TABLE t ADD COLUMN g INT GENERATED ALWAYS AS (a + 1) STORED", "shop", "shop", "t", []string{"g"}},
		{"a default that is a clause word", "ALTER TABLE t ADD COLUMN c VARCHAR(9) NOT NULL DEFAULT 'drop'", "shop", "shop", "t", []string{"c"}},
		{"a comment with clause words", "ALTER TABLE t ADD COLUMN c INT COMMENT 'rename me, then DROP COLUMN d'", "shop", "shop", "t", []string{"c"}},
		{"a column called partition", "ALTER TABLE t ADD COLUMN `partition` INT", "shop", "shop", "t", []string{"partition"}},
		{"on update", "ALTER TABLE t ADD COLUMN c TIMESTAMP(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)", "shop", "shop", "t", []string{"c"}},
		{"references", "ALTER TABLE t ADD COLUMN c BIGINT UNSIGNED REFERENCES otra(col) ON DELETE SET NULL ON UPDATE CASCADE", "shop", "shop", "t", []string{"c"}},
		{"references a quoted table", "ALTER TABLE t ADD COLUMN c INT REFERENCES `otra tabla` (`col`) ON DELETE NO ACTION", "shop", "shop", "t", []string{"c"}},
		{"check", "ALTER TABLE t ADD COLUMN c INT CHECK (c > 0) NOT ENFORCED", "shop", "shop", "t", []string{"c"}},
		{"character set and collation", "ALTER TABLE t ADD COLUMN c VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL", "shop", "shop", "t", []string{"c"}},
		{"numbers", "ALTER TABLE t ADD COLUMN c DECIMAL(10,2) UNSIGNED NOT NULL DEFAULT 0.50", "shop", "shop", "t", []string{"c"}},
		{"enum and set", "ALTER TABLE t ADD COLUMN c SET('a','b') DEFAULT 'a', ADD COLUMN d ENUM('x') INVISIBLE", "shop", "shop", "t", []string{"c", "d"}},
		{"first", "ALTER TABLE t ADD COLUMN c INT FIRST", "shop", "shop", "t", []string{"c"}},
		{"after a quoted column", "ALTER TABLE t ADD COLUMN c INT AFTER `drop`", "shop", "shop", "t", []string{"c"}},
		{"auto increment and unique", "ALTER TABLE t ADD COLUMN c SERIAL, ADD COLUMN d INT UNIQUE", "shop", "shop", "t", []string{"c", "d"}},
		{"a name that starts like a keyword", "ALTER TABLE t ADD indexed INT", "shop", "shop", "t", []string{"indexed"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl, cols, ok := AddedColumns(tc.query, tc.defaultSchema)
			if !ok {
				t.Fatalf("not read: %q", tc.query)
			}
			if tbl.Schema != tc.wantSchema || tbl.Table != tc.wantTable {
				t.Errorf("table = %q.%q, want %q.%q", tbl.Schema, tbl.Table, tc.wantSchema, tc.wantTable)
			}
			if !slices.Equal(cols, tc.wantCols) {
				t.Errorf("columns = %q, want %q", cols, tc.wantCols)
			}
		})
	}
}

// TestAddedColumns_notReadable: a statement that is not, with certainty, an
// ALTER TABLE that only adds columns is not read at all. A partial answer
// ("it adds c, and something else") would be taken for the whole statement.
func TestAddedColumns_notReadable(t *testing.T) {
	for _, tc := range []struct{ name, query string }{
		{"empty", ""},
		{"spaces", "  \n "},
		{"not DDL", "INSERT INTO t VALUES (1)"},
		{"create", "CREATE TABLE t (id INT)"},
		{"create like", "CREATE TABLE t LIKE u"},
		{"ADD clauses under a verb that is not ALTER", "CREATE TABLE t ADD COLUMN c INT"},
		{"ADD clauses under DROP", "DROP TABLE t ADD COLUMN c INT"},
		{"rename table", "RENAME TABLE u TO t"},
		{"drop table", "DROP TABLE t"},
		{"truncate", "TRUNCATE TABLE t"},
		{"no table", "ALTER TABLE"},
		{"no clause", "ALTER TABLE t"},
		{"drop column", "ALTER TABLE t DROP COLUMN c"},
		{"add and drop", "ALTER TABLE t ADD COLUMN c INT, DROP COLUMN d"},
		{"drop and add the same name", "ALTER TABLE t DROP COLUMN c, ADD COLUMN c INT"},
		{"rename column", "ALTER TABLE t RENAME COLUMN a TO c"},
		{"change", "ALTER TABLE t CHANGE a c INT"},
		{"modify", "ALTER TABLE t MODIFY c BIGINT"},
		{"add and modify", "ALTER TABLE t ADD COLUMN c INT, MODIFY d BIGINT"},
		{"rename the table", "ALTER TABLE t ADD COLUMN c INT, RENAME TO u"},
		{"add index", "ALTER TABLE t ADD INDEX i (c)"},
		{"add column and index", "ALTER TABLE t ADD COLUMN c INT, ADD INDEX i (c)"},
		{"add key", "ALTER TABLE t ADD KEY i (c)"},
		{"add primary key", "ALTER TABLE t ADD PRIMARY KEY (c)"},
		{"add unique", "ALTER TABLE t ADD UNIQUE (c)"},
		{"add constraint", "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (c) REFERENCES u (id)"},
		{"add foreign key", "ALTER TABLE t ADD FOREIGN KEY (c) REFERENCES u (id)"},
		{"add check", "ALTER TABLE t ADD CHECK (c > 0)"},
		{"add partition", "ALTER TABLE t ADD PARTITION (PARTITION p1 VALUES LESS THAN (10))"},
		{"add system versioning", "ALTER TABLE t ADD SYSTEM VERSIONING"},
		{"add period", "ALTER TABLE t ADD PERIOD FOR SYSTEM_TIME (a, b)"},
		{"column made the primary key", "ALTER TABLE t ADD COLUMN c INT PRIMARY KEY"},
		{"column made a key", "ALTER TABLE t ADD COLUMN c INT KEY"},
		{"column made unique key", "ALTER TABLE t ADD COLUMN c INT UNIQUE KEY"},
		{"if not exists", "ALTER TABLE t ADD COLUMN IF NOT EXISTS c INT"},
		{"if not exists without COLUMN", "ALTER TABLE t ADD IF NOT EXISTS c INT"},
		{"alter table if exists", "ALTER TABLE IF EXISTS t ADD COLUMN c INT"},
		{"list form", "ALTER TABLE t ADD COLUMN (c INT, d INT)"},
		{"no definition", "ALTER TABLE t ADD COLUMN c"},
		{"no name", "ALTER TABLE t ADD COLUMN"},
		{"only ALGORITHM", "ALTER TABLE t ALGORITHM=INSTANT"},
		{"the same column twice", "ALTER TABLE t ADD COLUMN c INT, ADD COLUMN C INT"},
		{"executable comment", "ALTER TABLE t ADD COLUMN c INT /*!80000 , ADD COLUMN d INT */"},
		{"MariaDB executable comment", "ALTER TABLE t /*M!100000 ADD COLUMN c INT */"},
		{"open block comment", "ALTER TABLE t ADD COLUMN c INT /* a"},
		{"a block comment closed before it opens", "ALTER TABLE t ADD COLUMN c INT */ /* a"},
		{"line comment with a hash", "ALTER TABLE t ADD COLUMN c INT # a"},
		{"line comment with a hash and more lines", "ALTER TABLE t # a\n ADD COLUMN c INT"},
		{"line comment with dashes", "ALTER TABLE t ADD COLUMN c INT -- a"},
		{"bytes that are not UTF-8", "ALTER TABLE t ADD COLUMN c INT COMMENT 'a\xf1o'"},
		{"another clause with no comma: rename", "ALTER TABLE t ADD COLUMN c INT RENAME TO u"},
		{"another clause with no comma: drop", "ALTER TABLE t ADD COLUMN c INT DROP COLUMN d"},
		{"another clause with no comma: change", "ALTER TABLE t ADD COLUMN c INT CHANGE a b INT"},
		{"another clause with no comma: modify", "ALTER TABLE t ADD COLUMN c INT MODIFY a INT"},
		{"another clause with no comma: add", "ALTER TABLE t ADD COLUMN c INT ADD COLUMN d INT"},
		{"another clause with no comma: alter", "ALTER TABLE t ADD COLUMN c INT ALTER COLUMN d SET DEFAULT 1"},
		{"another clause with no comma: convert", "ALTER TABLE t ADD COLUMN c INT CONVERT TO CHARACTER SET utf8mb4"},
		{"another clause with no comma: order", "ALTER TABLE t ADD COLUMN c INT ORDER BY id"},
		{"another clause with no comma: discard", "ALTER TABLE t ADD COLUMN c INT DISCARD TABLESPACE"},
		{"another clause with no comma: import", "ALTER TABLE t ADD COLUMN c INT IMPORT TABLESPACE"},
		{"another clause with no comma: force", "ALTER TABLE t ADD COLUMN c INT FORCE"},
		{"another clause with no comma: engine", "ALTER TABLE t ADD COLUMN c INT ENGINE=InnoDB"},
		{"partition by", "ALTER TABLE t ADD COLUMN c INT PARTITION BY HASH(id) PARTITIONS 4"},
		{"remove partitioning", "ALTER TABLE t ADD COLUMN c INT REMOVE PARTITIONING"},
		{"system versioning", "ALTER TABLE t ADD COLUMN c INT WITH SYSTEM VERSIONING"},
		{"after with no name", "ALTER TABLE t ADD COLUMN c INT AFTER "},
		{"after with no name, then a clause", "ALTER TABLE t ADD COLUMN c INT AFTER, ADD COLUMN d INT"},
		{"after with two names", "ALTER TABLE t ADD COLUMN c INT AFTER a b"},
		{"after a string", "ALTER TABLE t ADD COLUMN c INT AFTER 'a'"},
		{"after, then more", "ALTER TABLE t ADD COLUMN c INT AFTER a NOT NULL"},
		{"first with a name", "ALTER TABLE t ADD COLUMN c INT FIRST a"},
		{"collate with no name", "ALTER TABLE t ADD COLUMN c VARCHAR(9) COLLATE"},
		{"references with no table", "ALTER TABLE t ADD COLUMN c INT REFERENCES"},
		{"a word that is no part of a definition", "ALTER TABLE t ADD COLUMN c INT WHATEVER"},
		{"a type that is no type", "ALTER TABLE t ADD COLUMN c WHATEVER"},
		{"a quoted name where none belongs", "ALTER TABLE t ADD COLUMN c INT `d`"},
		{"a bare INDEX as the name", "ALTER TABLE t ADD INDEX INT"},
		{"a bare KEY as the name", "ALTER TABLE t ADD KEY INT"},
		{"a bare PARTITION as the name", "ALTER TABLE t ADD COLUMN PARTITION INT"},
		{"a semicolon inside a group", "ALTER TABLE t ADD COLUMN c INT CHECK (c > 0; d)"},
		{"a table option after the column", "ALTER TABLE t ADD COLUMN c INT AUTO_INCREMENT=5"},
		{"a table option after the column, spaced", "ALTER TABLE t ADD COLUMN c INT COMMENT = 'x'"},
		{"backslash", `ALTER TABLE t ADD COLUMN c VARCHAR(9) DEFAULT 'a\', ADD COLUMN d INT COMMENT 'x'`},
		{"open quote", "ALTER TABLE t ADD COLUMN c VARCHAR(9) DEFAULT 'a"},
		{"open backtick", "ALTER TABLE t ADD COLUMN `c INT"},
		{"open parenthesis", "ALTER TABLE t ADD COLUMN c DECIMAL(10,2"},
		{"closing parenthesis first", "ALTER TABLE t ADD COLUMN c INT), ADD COLUMN d INT"},
		{"cut in a clause", "ALTER TABLE t ADD COLUMN c INT, ADD COL"},
		{"cut after a comma", "ALTER TABLE t ADD COLUMN c INT,"},
		{"truncation marker", "ALTER TABLE t ADD COLUMN c INT /* bintrail:truncated */"},
		{"two statements", "ALTER TABLE t ADD COLUMN c INT; DROP TABLE t"},
		{"temporary", "ALTER TEMPORARY TABLE t ADD COLUMN c INT"},
		{"pointer text of a multi table row", "(same DROP TABLE statement as the row for shop.t)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tbl, cols, ok := AddedColumns(tc.query, "shop"); ok {
				t.Fatalf("%q was read as %s.%s adding %q", tc.query, tbl.Schema, tbl.Table, cols)
			}
		})
	}
}

// TestAddedColumns_longStatement: a statement is read whole, past the head
// parseDDL looks at.
func TestAddedColumns_longStatement(t *testing.T) {
	q := "ALTER TABLE t ADD COLUMN c INT COMMENT '" + strings.Repeat("x", 4*HeadLimit) + "', DROP COLUMN d"
	if _, cols, ok := AddedColumns(q, "shop"); ok {
		t.Fatalf("a DROP past the head was not seen: read as adding %q", cols)
	}
	q = "ALTER TABLE t ADD COLUMN c INT COMMENT '" + strings.Repeat("x", 4*HeadLimit) + "', ADD COLUMN d INT"
	if _, cols, ok := AddedColumns(q, "shop"); !ok || !slices.Equal(cols, []string{"c", "d"}) {
		t.Fatalf("columns = %q, ok = %v", cols, ok)
	}
}
