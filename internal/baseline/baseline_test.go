package baseline

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
)

// ─── ParseMetadata ────────────────────────────────────────────────────────────

const sampleMetadata = `Started dump at: 2025-02-28 00:00:00
Finished dump at: 2025-02-28 00:01:23
SHOW MASTER STATUS:
	Log: binlog.000042
	Pos: 12345
	GTID: 3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100
`

func TestParseMetadata(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := ParseMetadata(dir)
	if err != nil {
		t.Fatalf("ParseMetadata: %v", err)
	}
	want := time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
	if !m.StartedAt.Equal(want) {
		t.Errorf("StartedAt = %v, want %v", m.StartedAt, want)
	}
}

func TestParseMetadataMissing(t *testing.T) {
	_, err := ParseMetadata(t.TempDir())
	if err == nil {
		t.Fatal("expected error for missing metadata file")
	}
}

func TestParseMetadataFields(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := ParseMetadata(dir)
	if err != nil {
		t.Fatalf("ParseMetadata: %v", err)
	}
	if m.BinlogFile != "binlog.000042" {
		t.Errorf("BinlogFile = %q, want %q", m.BinlogFile, "binlog.000042")
	}
	if m.GTIDSet != "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100" {
		t.Errorf("GTIDSet = %q, want %q", m.GTIDSet, "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100")
	}
	if m.BinlogPos != 12345 {
		t.Errorf("BinlogPos = %d, want 12345", m.BinlogPos)
	}
}

// sampleMetadataNew is the format produced by mydumper 0.16+ (TOML-like with
// # prefixes and KEY = "value" assignments).
const sampleMetadataNew = `# Started dump at: 2026-03-02 23:45:20
[config]
quote-character = BACKTICK

[source]
# executed_gtid_set = "55512139-1432-11f1-8d8d-0693b428a89b:1-11490596"
# SOURCE_LOG_FILE = "mysql-bin-changelog.000879"
# SOURCE_LOG_POS = 4504702
# Finished dump at: 2026-03-02 23:45:21
`

func TestParseMetadataNewFormat(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "metadata"), []byte(sampleMetadataNew), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := ParseMetadata(dir)
	if err != nil {
		t.Fatalf("ParseMetadata: %v", err)
	}
	wantTime := time.Date(2026, 3, 2, 23, 45, 20, 0, time.UTC)
	if !m.StartedAt.Equal(wantTime) {
		t.Errorf("StartedAt = %v, want %v", m.StartedAt, wantTime)
	}
	if m.BinlogFile != "mysql-bin-changelog.000879" {
		t.Errorf("BinlogFile = %q, want %q", m.BinlogFile, "mysql-bin-changelog.000879")
	}
	if m.BinlogPos != 4504702 {
		t.Errorf("BinlogPos = %d, want 4504702", m.BinlogPos)
	}
	if m.GTIDSet != "55512139-1432-11f1-8d8d-0693b428a89b:1-11490596" {
		t.Errorf("GTIDSet = %q, want %q", m.GTIDSet, "55512139-1432-11f1-8d8d-0693b428a89b:1-11490596")
	}
}

// TestParseMetadataPrefersStartedAtMarker verifies the #768 fix: when
// `bintrail dump` has written its own process-captured UTC start-time
// marker, ParseMetadata uses it instead of re-parsing mydumper's
// "Started dump at" line as if it were UTC. Here the mydumper line encodes a
// dump-host-local timestamp (e.g. a UTC+2 host writing "02:00:00" for an
// event that actually happened at 00:00:00 UTC) — parsing it verbatim as UTC
// would anchor the baseline 2 hours in the future and silently exclude the
// intervening deltas from replay. The marker sidesteps that ambiguity.
func TestParseMetadataPrefersStartedAtMarker(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	trueUTC := time.Date(2025, 2, 27, 22, 0, 0, 0, time.UTC) // 2h earlier than the (host-local) mydumper line
	if err := WriteStartedAtMarker(dir, trueUTC); err != nil {
		t.Fatalf("WriteStartedAtMarker: %v", err)
	}

	m, err := ParseMetadata(dir)
	if err != nil {
		t.Fatalf("ParseMetadata: %v", err)
	}
	if !m.StartedAt.Equal(trueUTC) {
		t.Errorf("StartedAt = %v, want marker time %v (mydumper's ambiguous local-time line should have been overridden)", m.StartedAt, trueUTC)
	}
	// Other fields still come from mydumper's own metadata file.
	if m.BinlogFile != "binlog.000042" {
		t.Errorf("BinlogFile = %q, want %q", m.BinlogFile, "binlog.000042")
	}
}

// TestParseMetadataFallsBackWithoutMarker pins the pre-#768 behavior for
// mydumper dumps produced outside `bintrail dump` (no marker file): the
// ambiguous "Started dump at" line is still parsed as UTC verbatim.
func TestParseMetadataFallsBackWithoutMarker(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := ParseMetadata(dir)
	if err != nil {
		t.Fatalf("ParseMetadata: %v", err)
	}
	want := time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
	if !m.StartedAt.Equal(want) {
		t.Errorf("StartedAt = %v, want %v", m.StartedAt, want)
	}
}

// TestParseMetadataCorruptMarkerFallsBack verifies that an unparseable
// dump-start marker (e.g. truncated by a crash mid-write) is a no-op fallback,
// not a hard failure: ParseMetadata still succeeds using mydumper's own line.
func TestParseMetadataCorruptMarkerFallsBack(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, StartedAtMarkerFile), []byte("not-a-timestamp"), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := ParseMetadata(dir)
	if err != nil {
		t.Fatalf("ParseMetadata: %v", err)
	}
	want := time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
	if !m.StartedAt.Equal(want) {
		t.Errorf("StartedAt = %v, want %v (fallback to mydumper's line)", m.StartedAt, want)
	}
}

func TestParseMetadataMissingTimestamp(t *testing.T) {
	const content = "SHOW MASTER STATUS:\n\tLog: binlog.000001\n\tPos: 100\n"
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "metadata"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := ParseMetadata(dir)
	if err == nil {
		t.Error("expected error for missing 'Started dump at:' line, got nil")
	}
}

// ─── ParseSchema ──────────────────────────────────────────────────────────────

const sampleSchema = `CREATE TABLE ` + "`orders`" + ` (
  ` + "`id`" + ` int NOT NULL AUTO_INCREMENT,
  ` + "`user_id`" + ` bigint NOT NULL,
  ` + "`amount`" + ` decimal(10,2) NOT NULL,
  ` + "`note`" + ` varchar(255) DEFAULT NULL,
  ` + "`created_at`" + ` datetime NOT NULL,
  ` + "`paid_on`" + ` date DEFAULT NULL,
  PRIMARY KEY (` + "`id`" + `)
) ENGINE=InnoDB;
`

func TestParseSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.orders-schema.sql")
	if err := os.WriteFile(path, []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := ParseSchema(path)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	if len(cols) != 6 {
		t.Fatalf("got %d columns, want 6", len(cols))
	}
	wantNames := []string{"id", "user_id", "amount", "note", "created_at", "paid_on"}
	for i, name := range wantNames {
		if cols[i].Name != name {
			t.Errorf("col[%d].Name = %q, want %q", i, cols[i].Name, name)
		}
	}
	wantTypes := []string{"int", "bigint", "decimal", "varchar", "datetime", "date"}
	for i, typ := range wantTypes {
		if cols[i].MySQLType != typ {
			t.Errorf("col[%d].MySQLType = %q, want %q", i, cols[i].MySQLType, typ)
		}
	}
}

func TestParseSchemaEmpty(t *testing.T) {
	// A CREATE TABLE whose first line inside the parens is PRIMARY KEY — colRe
	// never matches before the stop condition, so no columns are found.
	const emptySchema = "CREATE TABLE `empty` (\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.empty-schema.sql")
	if err := os.WriteFile(path, []byte(emptySchema), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := ParseSchema(path)
	if err == nil {
		t.Error("expected error for schema with no columns, got nil")
	}
}

func TestParseSchemaStopAtUniqueKey(t *testing.T) {
	const schema = "CREATE TABLE `users` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `email` varchar(255) NOT NULL,\n" +
		"  UNIQUE KEY `email_unique` (`email`),\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.users-schema.sql")
	if err := os.WriteFile(path, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := ParseSchema(path)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	// id and email only; UNIQUE KEY line triggers the stop condition.
	if len(cols) != 2 {
		t.Errorf("got %d columns, want 2: %+v", len(cols), cols)
	}
}

func TestBuildParquetSchema(t *testing.T) {
	cols := []Column{
		{Name: "id", MySQLType: "int", ParquetType: MysqlToParquetNode("int")},
		{Name: "name", MySQLType: "varchar", ParquetType: MysqlToParquetNode("varchar")},
	}
	schema := BuildParquetSchema(cols)
	if schema == nil {
		t.Error("BuildParquetSchema returned nil")
	}
}

// ─── MySQLToParquetType ───────────────────────────────────────────────────────

func TestMySQLToParquetType(t *testing.T) {
	cases := []struct {
		typ  string
		want string // substring of the parquet node string representation
	}{
		{"int", "INT32"},
		{"bigint", "INT64"},
		{"float", "FLOAT"},
		{"double", "DOUBLE"},
		{"decimal", "STRING"},
		{"varchar", "STRING"},
		{"datetime", "INT64"},
		{"date", "INT32"},
		{"blob", "BYTE_ARRAY"},
		{"json", "STRING"},
	}
	for _, tc := range cases {
		node := MysqlToParquetNode(tc.typ)
		if node == nil {
			t.Errorf("MysqlToParquetNode(%q) = nil", tc.typ)
		}
		// Just check it doesn't panic; the actual type mapping is validated
		// end-to-end in TestWriteAndReadParquet.
	}
}

// ─── ReadTabRow ───────────────────────────────────────────────────────────────

func TestReadTabRow(t *testing.T) {
	cases := []struct {
		line  string
		want  []string
		nulls []bool
	}{
		{
			// Three fields separated by real tab characters.
			line:  "1\tAlice\talice@example.com",
			want:  []string{"1", "Alice", "alice@example.com"},
			nulls: []bool{false, false, false},
		},
		{
			// \N in a field (the literal two chars backslash + N) = NULL.
			// Tab is the real tab character as field separator.
			line:  "1\t\\N",
			want:  []string{"1", ""},
			nulls: []bool{false, true},
		},
		{
			// \\ in a field = single backslash; \N = NULL.
			line:  "hello\\\\world\t\\N",
			want:  []string{`hello\world`, ""},
			nulls: []bool{false, true},
		},
	}
	for _, tc := range cases {
		values, nulls, err := parseTabRow(tc.line, len(tc.want))
		if err != nil {
			t.Errorf("parseTabRow(%q): %v", tc.line, err)
			continue
		}
		if len(values) != len(tc.want) {
			t.Errorf("parseTabRow(%q): got %d values, want %d", tc.line, len(values), len(tc.want))
			continue
		}
		for i, w := range tc.want {
			if values[i] != w {
				t.Errorf("parseTabRow(%q)[%d] = %q, want %q", tc.line, i, values[i], w)
			}
			if nulls[i] != tc.nulls[i] {
				t.Errorf("parseTabRow(%q) nulls[%d] = %v, want %v", tc.line, i, nulls[i], tc.nulls[i])
			}
		}
	}
}

func TestReadTabFile(t *testing.T) {
	// 3 rows: normal, NULL in col 2, NULL in col 3.
	const tabData = "1\tAlice\t100\n2\t\\N\t200\n3\tCharlie\t\\N\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.users.00000.dat")
	if err := os.WriteFile(path, []byte(tabData), 0o644); err != nil {
		t.Fatal(err)
	}

	var rows [][]string
	var allNulls [][]bool
	if err := ReadTabFile(path, 3, func(values []string, nulls []bool) error {
		rows = append(rows, append([]string(nil), values...))
		allNulls = append(allNulls, append([]bool(nil), nulls...))
		return nil
	}); err != nil {
		t.Fatalf("ReadTabFile: %v", err)
	}

	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
	if rows[0][0] != "1" || rows[0][1] != "Alice" || rows[0][2] != "100" {
		t.Errorf("row 0 = %v, want [1 Alice 100]", rows[0])
	}
	if !allNulls[1][1] || rows[1][2] != "200" {
		t.Errorf("row 1 = %v nulls %v, want [2 <NULL> 200]", rows[1], allNulls[1])
	}
	if rows[2][1] != "Charlie" || !allNulls[2][2] {
		t.Errorf("row 2 = %v nulls %v, want [3 Charlie <NULL>]", rows[2], allNulls[2])
	}
}

func TestReadTabRowEscapes(t *testing.T) {
	cases := []struct {
		line string
		want string
	}{
		{`hello\tworld`, "hello\tworld"},
		{`line1\nline2`, "line1\nline2"},
		{`cr\rhere`, "cr\rhere"},
		{`back\\slash`, `back\slash`},
	}
	for _, tc := range cases {
		values, _, err := parseTabRow(tc.line, 1)
		if err != nil {
			t.Errorf("parseTabRow(%q): %v", tc.line, err)
			continue
		}
		if len(values) != 1 || values[0] != tc.want {
			t.Errorf("parseTabRow(%q) = %v, want %q", tc.line, values, tc.want)
		}
	}
}

// ─── ReadSQLRow ───────────────────────────────────────────────────────────────

func TestReadSQLRow(t *testing.T) {
	const sqlData = "INSERT INTO `orders` VALUES(1,'Alice',NULL),(2,'Bob',42);\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.orders.00000.sql")
	if err := os.WriteFile(path, []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}

	var rows [][]string
	var allNulls [][]bool
	if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		rows = append(rows, append([]string(nil), values...))
		allNulls = append(allNulls, append([]bool(nil), nulls...))
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	// Row 0: (1, 'Alice', NULL)
	if rows[0][0] != "1" || rows[0][1] != "Alice" || !allNulls[0][2] {
		t.Errorf("row 0 = %v nulls %v, want [1 Alice <NULL>]", rows[0], allNulls[0])
	}
	// Row 1: (2, 'Bob', 42)
	if rows[1][0] != "2" || rows[1][1] != "Bob" || rows[1][2] != "42" {
		t.Errorf("row 1 = %v, want [2 Bob 42]", rows[1])
	}
}

// TestReadSQLRowIdentifierEmbeddedSpace pins #502: a backtick-quoted table or
// column identifier with an embedded space (`Allowed Values`, `sensor values`)
// contains the substring " VALUES" before the real keyword. The pre-fix naive
// substring search sliced mid-identifier — silently dropping the first row of
// a multi-line INSERT, or rejecting a single-line file as truncated. The
// VALUES finder must skip quoted identifiers and match only the keyword.
func TestReadSQLRowIdentifierEmbeddedSpace(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want [][]string
	}{
		{
			// Multi-line (mydumper native layout): pre-fix this returned rows
			// (2,b),(3,c) — the first row lost with a clean exit.
			name: "column with embedded space, multi-line",
			sql:  "INSERT INTO `settings` (`id`,`Allowed Values`) VALUES(1,'a')\n,(2,'b')\n,(3,'c');\n",
			want: [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}},
		},
		{
			name: "table with embedded space, multi-line",
			sql:  "INSERT INTO `sensor values` VALUES(1)\n,(2)\n,(3);\n",
			want: [][]string{{"1"}, {"2"}, {"3"}},
		},
		{
			// Single-line (mysqldump extended-insert): pre-fix this errored as
			// "unterminated INSERT statement" and produced zero rows.
			name: "column with embedded space, single-line",
			sql:  "INSERT INTO `config` (`k`,`Default Values`) VALUES (1,'x'),(2,'y'),(3,'z');\n",
			want: [][]string{{"1", "x"}, {"2", "y"}, {"3", "z"}},
		},
		{
			// A doubled backtick is MySQL's escape for a literal backtick INSIDE
			// a quoted identifier — the skip must not treat it as the closer.
			name: "escaped backtick inside identifier",
			sql:  "INSERT INTO `a``b values` VALUES(7,'q');\n",
			want: [][]string{{"7", "q"}},
		},
		{
			name: "REPLACE with embedded-space column",
			sql:  "REPLACE INTO `settings` (`id`,`Allowed Values`) VALUES(9,'r');\n",
			want: [][]string{{"9", "r"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "shop.t.00000.sql")
			if err := os.WriteFile(path, []byte(tc.sql), 0o644); err != nil {
				t.Fatal(err)
			}
			var rows [][]string
			if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
				rows = append(rows, append([]string(nil), values...))
				return nil
			}); err != nil {
				t.Fatalf("ReadSQLFile: %v", err)
			}
			if len(rows) != len(tc.want) {
				t.Fatalf("got %d rows %v, want %d", len(rows), rows, len(tc.want))
			}
			for i := range tc.want {
				for j := range tc.want[i] {
					if rows[i][j] != tc.want[i][j] {
						t.Errorf("row %d col %d = %q, want %q", i, j, rows[i][j], tc.want[i][j])
					}
				}
			}
		})
	}
}

func TestFindValuesKeyword(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int // -1 = not found; otherwise the offset of 'V'
	}{
		{"plain", "INSERT INTO `t` VALUES(1)", 16},
		{"lowercase keyword", "insert into `t` values(1)", 16},
		{"no space before paren", "INSERT INTO `t` (`a`) VALUES(1)", 22},
		{"embedded-space identifier skipped", "INSERT INTO `sensor values` VALUES(1)", 28},
		{"escaped backtick inside identifier", "INSERT INTO `a``b values` VALUES(1)", 26},
		{"suffix of longer word ignored", "INSERT INTO NVALUES VALUES(1)", 20},
		{"prefix of longer word ignored", "INSERT INTO VALUESX VALUES(1)", 20},
		{"dollar-joined word ignored", "INSERT INTO a$VALUES VALUES(1)", 21},
		{"no keyword", "INSERT INTO `t` (`a`,`b`)", -1},
		{"unterminated backtick reads to end", "INSERT INTO `sensor values", -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := findValuesKeyword(tc.in); got != tc.want {
				t.Errorf("findValuesKeyword(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestReadSQLRowEscaping(t *testing.T) {
	cases := []struct {
		name string
		// sql is the full INSERT statement written to the file.
		sql  string
		want string
	}{
		{
			// \'  →  '   (backslash-escaped single quote)
			name: "backslash-single-quote",
			sql:  "INSERT INTO t VALUES('it\\'s fine');\n",
			want: "it's fine",
		},
		{
			// ''  →  '   (doubled single-quote escape)
			name: "doubled-single-quote",
			sql:  "INSERT INTO t VALUES('it''s fine');\n",
			want: "it's fine",
		},
		{
			// \n  →  newline
			name: "newline",
			sql:  "INSERT INTO t VALUES('line1\\nline2');\n",
			want: "line1\nline2",
		},
		{
			// \t  →  tab
			name: "tab",
			sql:  "INSERT INTO t VALUES('col1\\tcol2');\n",
			want: "col1\tcol2",
		},
		{
			// \\  →  single backslash
			name: "backslash",
			sql:  "INSERT INTO t VALUES('path\\\\file');\n",
			want: `path\file`,
		},
		{
			// \0  →  null byte
			name: "null-byte",
			sql:  "INSERT INTO t VALUES('\\0');\n",
			want: "\x00",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "t.00000.sql")
			if err := os.WriteFile(path, []byte(tc.sql), 0o644); err != nil {
				t.Fatal(err)
			}
			var rows [][]string
			if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
				rows = append(rows, append([]string(nil), values...))
				return nil
			}); err != nil {
				t.Fatalf("ReadSQLFile: %v", err)
			}
			if len(rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rows))
			}
			if rows[0][0] != tc.want {
				t.Errorf("col 0 = %q, want %q", rows[0][0], tc.want)
			}
		})
	}
}

func TestReadSQLRowDoubleQuoted(t *testing.T) {
	// Double-quoted strings with \" and \n escapes.
	sql := `INSERT INTO t VALUES("hello \"world\"", "line1\nline2");` + "\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "t.00000.sql")
	if err := os.WriteFile(path, []byte(sql), 0o644); err != nil {
		t.Fatal(err)
	}
	var rows [][]string
	if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		rows = append(rows, append([]string(nil), values...))
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile: %v", err)
	}
	if len(rows) != 1 || len(rows[0]) != 2 {
		t.Fatalf("got rows=%v, want 1 row with 2 cols", rows)
	}
	if rows[0][0] != `hello "world"` {
		t.Errorf(`col 0 = %q, want %q`, rows[0][0], `hello "world"`)
	}
	if rows[0][1] != "line1\nline2" {
		t.Errorf("col 1 = %q, want embedded-newline string", rows[0][1])
	}
}

func TestReadSQLRowHexLiteral(t *testing.T) {
	sql := "INSERT INTO t VALUES(0x48656C6C6F, 42);\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "t.00000.sql")
	if err := os.WriteFile(path, []byte(sql), 0o644); err != nil {
		t.Fatal(err)
	}
	var rows [][]string
	if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		rows = append(rows, append([]string(nil), values...))
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile: %v", err)
	}
	if len(rows) != 1 || len(rows[0]) != 2 {
		t.Fatalf("got rows=%v, want 1 row with 2 cols", rows)
	}
	if rows[0][0] != "0x48656C6C6F" {
		t.Errorf("hex literal = %q, want %q", rows[0][0], "0x48656C6C6F")
	}
	if rows[0][1] != "42" {
		t.Errorf("col 1 = %q, want 42", rows[0][1])
	}
}

func TestReadSQLRowNonInsertLines(t *testing.T) {
	// Comments, SET statements, and blank lines before the INSERT should be skipped.
	const data = "-- mydumper SQL dump\nSET NAMES utf8;\nSET TIME_ZONE='+00:00';\n\n" +
		"INSERT INTO `orders` VALUES(1,'test');\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.orders.00000.sql")
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	var rows [][]string
	if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		rows = append(rows, append([]string(nil), values...))
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0][0] != "1" || rows[0][1] != "test" {
		t.Errorf("row = %v, want [1 test]", rows[0])
	}
}

func TestReadSQLRowEmpty(t *testing.T) {
	// File with no INSERT statements → 0 rows, no error.
	const data = "-- just comments\nSET NAMES utf8;\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.orders.00000.sql")
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile: %v", err)
	}
	if count != 0 {
		t.Errorf("got %d rows, want 0", count)
	}
}

func TestReadSQLRowUnterminated(t *testing.T) {
	// String literal that never closes should return an error.
	const data = "INSERT INTO t VALUES('unterminated);\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "t.00000.sql")
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		return nil
	})
	if err == nil {
		t.Error("expected error for unterminated string, got nil")
	}
}

func TestReadSQLRowMultiLineInsert(t *testing.T) {
	// mydumper >= 1.0 emits one tuple per physical line with a leading comma.
	// The old line-oriented parser captured only the first tuple of each
	// INSERT and silently dropped every continuation row — issue #495. This
	// asserts every row of a multi-line, multi-statement dump is returned.
	const sqlData = "INSERT INTO `orders` (`id`,`name`) VALUES(1,'a')\n" +
		",(2,'b')\n" +
		",(3,'c')\n" +
		",(4,'d');\n" +
		"INSERT INTO `orders` (`id`,`name`) VALUES(5,'e')\n" +
		",(6,'f');\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.orders.00000.sql")
	if err := os.WriteFile(path, []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}

	var rows [][]string
	if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		rows = append(rows, append([]string(nil), values...))
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile: %v", err)
	}
	// Two statements carrying 4 + 2 = 6 tuples total.
	if len(rows) != 6 {
		t.Fatalf("got %d rows, want 6 (multi-line continuation rows were dropped)", len(rows))
	}
	for i, want := range []string{"1", "2", "3", "4", "5", "6"} {
		if rows[i][0] != want {
			t.Errorf("row %d id = %q, want %q", i, rows[i][0], want)
		}
	}
	// Spot-check continuation-row values, not just ids.
	if rows[3][1] != "d" || rows[5][1] != "f" {
		t.Errorf("continuation values wrong: row3=%v row5=%v", rows[3], rows[5])
	}
}

func TestReadSQLRowMultiLineLayouts(t *testing.T) {
	// Every comma/terminator placement mydumper variants emit must yield the
	// same three rows.
	layouts := map[string]string{
		"leading-comma":      "INSERT INTO t VALUES(1)\n,(2)\n,(3);\n",
		"trailing-comma":     "INSERT INTO t VALUES(1),\n(2),\n(3);\n",
		"semicolon-own-line": "INSERT INTO t VALUES(1)\n,(2)\n,(3)\n;\n",
		"values-then-tuples": "INSERT INTO t VALUES\n(1),\n(2),\n(3);\n",
		"single-line":        "INSERT INTO t VALUES(1),(2),(3);\n",
		"two-statements":     "INSERT INTO t VALUES(1);\nINSERT INTO t VALUES(2)\n,(3);\n",
	}
	for name, sql := range layouts {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "shop.t.00000.sql")
			if err := os.WriteFile(path, []byte(sql), 0o644); err != nil {
				t.Fatal(err)
			}
			var ids []string
			if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
				ids = append(ids, values[0])
				return nil
			}); err != nil {
				t.Fatalf("ReadSQLFile: %v", err)
			}
			if got := strings.Join(ids, ","); got != "1,2,3" {
				t.Errorf("ids = %q, want \"1,2,3\"", got)
			}
		})
	}
}

func TestReadSQLRowOversizedLine(t *testing.T) {
	// A single tuple carrying a value larger than the old fixed 8 MB scanner
	// buffer must parse, not abort with bufio.ErrTooLong. mydumper emits one
	// tuple per physical line, so a LONGBLOB/JSON column produces exactly this
	// shape (#801). The growable bufio.Reader has no artificial per-line cap.
	big := strings.Repeat("A", 12<<20) // 12 MB, well over the old 8 MB limit
	sqlData := "INSERT INTO `docs` VALUES(1,'" + big + "');\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.docs.00000.sql")
	if err := os.WriteFile(path, []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}

	var rows [][]string
	if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		rows = append(rows, append([]string(nil), values...))
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile on oversized line: %v", err)
	}
	if len(rows) != 1 || len(rows[0]) != 2 {
		t.Fatalf("got rows=%d cols, want 1 row with 2 cols", len(rows))
	}
	if rows[0][0] != "1" {
		t.Errorf("col 0 = %q, want 1", rows[0][0])
	}
	if len(rows[0][1]) != len(big) {
		t.Errorf("col 1 length = %d, want %d (large value truncated/dropped)", len(rows[0][1]), len(big))
	}
}

func TestReadSQLRowUnexpectedToken(t *testing.T) {
	// A stray token where a tuple or ';' is expected must surface as a loud
	// error, not silently truncate the fragment. With cross-line state, a
	// lenient skip would consume the FOLLOWING INSERT as a bogus continuation
	// and drop its rows, while a later ';' clears inStatement so the EOF guard
	// never fires — the exact silent-loss class #495 closes. Both review repros:
	cases := map[string]string{
		// stray token then a bare ';' line that would have hidden the loss
		"junk-then-bare-semicolon": "INSERT INTO `t` VALUES(1) JUNK,(2)\n;\n",
		// stray token desyncs, swallowing the next INSERT statement's rows
		"junk-swallows-next-stmt": "INSERT INTO `t` VALUES(1) X\nINSERT INTO `t` VALUES(2)\n,(3);\n",
	}
	for name, sql := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "shop.t.00000.sql")
			if err := os.WriteFile(path, []byte(sql), 0o644); err != nil {
				t.Fatal(err)
			}
			var count int
			err := ReadSQLFile(path, func(values []string, nulls []bool) error {
				count++
				return nil
			})
			if err == nil {
				t.Fatalf("expected error for unexpected token, got nil (parsed %d rows)", count)
			}
			if !strings.Contains(err.Error(), "unexpected token") {
				t.Errorf("error = %v, want it to mention 'unexpected token'", err)
			}
		})
	}
}

func TestReadSQLRowTruncated(t *testing.T) {
	// A dump file that ends mid-statement (no terminating ';') is truncated.
	// Fail loudly rather than silently return a short row count.
	const sqlData = "INSERT INTO `orders` VALUES(1,'a')\n,(2,'b')\n,(3,'c')\n" // no ';'
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.orders.00000.sql")
	if err := os.WriteFile(path, []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}
	var count int
	err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		count++
		return nil
	})
	if err == nil {
		t.Fatalf("expected error for truncated (unterminated) INSERT, got nil (parsed %d rows)", count)
	}
	if !strings.Contains(err.Error(), "unterminated") {
		t.Errorf("error = %v, want it to mention 'unterminated'", err)
	}
}

func TestReadSQLRowTruncatedBeforeValues(t *testing.T) {
	// #468 shape 1 + the adjacent unsupported layout: the reader must fail
	// loudly when an INSERT/REPLACE opening line carries no VALUES clause —
	// whether because the dump was truncated mid-header (the header/first-stmt
	// cases) or because VALUES was wrapped onto a continuation line (the
	// values-on-next-line case, an unsupported layout, not truncation). Pre-fix
	// the reader `continue`d past it and exited cleanly with a short row count
	// (silent data loss → wrong Time-travel reconstructions). Cases with a
	// preceding complete INSERT leave it intact; truncated-as-first-stmt has no
	// preceding statement.
	cases := map[string]string{
		"insert-header-only":      "INSERT INTO `orders` VALUES(1,'a');\nINSERT INTO `orders` (`id`,`na",
		"replace-header-only":     "REPLACE INTO `orders` VALUES(1,'a');\nREPLACE INTO `orders` (`id`,`co",
		"values-on-next-line":     "INSERT INTO `orders` VALUES(1,'a');\nINSERT INTO `orders` (`id`,`name`)\nVALUES(2,'b');\n",
		"truncated-as-first-stmt": "INSERT INTO `orders` (`id`,`na",
	}
	for name, sqlData := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "shop.orders.00000.sql")
			if err := os.WriteFile(path, []byte(sqlData), 0o644); err != nil {
				t.Fatal(err)
			}
			var count int
			err := ReadSQLFile(path, func(values []string, nulls []bool) error {
				count++
				return nil
			})
			if err == nil {
				t.Fatalf("expected error for INSERT/REPLACE without VALUES, got nil (parsed %d rows)", count)
			}
			if !strings.Contains(err.Error(), "VALUES") {
				t.Errorf("error = %v, want it to mention the missing VALUES clause", err)
			}
		})
	}
}

func TestReadSQLRowValuesAtLineEndStillParses(t *testing.T) {
	// Guard against over-firing the shape-1 truncation check: the supported
	// mydumper layout where VALUES ends the header line (tuples follow on
	// continuation lines) keeps VALUES on the INSERT line, so it must NOT be
	// mistaken for a truncated header.
	const sqlData = "INSERT INTO `t` VALUES\n(1),\n(2),\n(3);\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.t.00000.sql")
	if err := os.WriteFile(path, []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}
	var ids []string
	if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		ids = append(ids, values[0])
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile must accept VALUES-at-line-end layout: %v", err)
	}
	if got := strings.Join(ids, ","); got != "1,2,3" {
		t.Errorf("ids = %q, want \"1,2,3\"", got)
	}
}

func TestReadSQLRowMydumperBinaryJSON(t *testing.T) {
	// Fixture is captured mydumper output (--complete-insert) for a table with
	// VARBINARY, BLOB, BIT, and JSON columns holding adversarial bytes (',' ')'
	// quotes, NUL, 0x1A). Generated with:
	//   docker run mydumper/mydumper:v1.0.3-1 mydumper --complete-insert \
	//     --database ptest --tables-list ptest.bins   (MySQL 8.0 source)
	// The pre-fix parser routed _binary "…" and
	// CONVERT("…" USING …) through the quote-blind reader, silently corrupting
	// values and column counts. Columns (MySQL order): id, vb, bl, bt, js, txt.
	path := filepath.Join("testdata", "mydumper_v1_binary_json.sql")
	var rows [][]string
	var nullRows [][]bool
	if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		rows = append(rows, append([]string(nil), values...))
		nullRows = append(nullRows, append([]bool(nil), nulls...))
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
	// Wrong column count was the silent-corruption symptom — every row is 6 cols.
	for i, r := range rows {
		if len(r) != 6 {
			t.Fatalf("row %d has %d cols, want 6: %q", i, len(r), r)
		}
	}
	// Row 0: vb=0x612c6229 ("a,b)"), bl=0x2722 ("'\""), bt=0xAA,
	//        js={"m": "done) here", "n": 5}, txt=plain
	want0 := []string{"1", "a,b)", "'\"", "\xaa", `{"m": "done) here", "n": 5}`, "plain"}
	for c, w := range want0 {
		if rows[0][c] != w {
			t.Errorf("row0 col%d = %q, want %q", c, rows[0][c], w)
		}
	}
	// Row 1: vb=NUL,0x1A,CR,LF,backslash; bl=Hello; bt=0x01; js=[1, 2, 3];
	//        txt contains both ',' and ')'
	want1 := []string{"2", "\x00\x1a\r\n\\", "Hello", "\x01", "[1, 2, 3]", "two,with)delims"}
	for c, w := range want1 {
		if rows[1][c] != w {
			t.Errorf("row1 col%d = %q, want %q", c, rows[1][c], w)
		}
	}
	// Row 2: id=3 is a real value; columns 1..5 are NULL.
	if rows[2][0] != "3" || nullRows[2][0] {
		t.Errorf("row2 col0 = %q null=%v, want \"3\" non-null", rows[2][0], nullRows[2][0])
	}
	for c := 1; c < 6; c++ {
		if !nullRows[2][c] {
			t.Errorf("row2 col%d should be NULL, got %q", c, rows[2][c])
		}
	}
}

func TestReadSQLRowReplaceInto(t *testing.T) {
	// REPLACE INTO (mysqldump --replace / mydumper --replace) carries rows
	// exactly like INSERT; the old INSERT-only dispatch dropped them all.
	cases := map[string]struct {
		sql  string
		rows int
	}{
		"single-line": {"REPLACE INTO `t` VALUES (1,'a'),(2,'b');\n", 2},
		"multi-line":  {"REPLACE INTO `t` (`id`,`c`) VALUES\n(1,'a')\n,(2,'b')\n,(3,'c');\n", 3},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "shop.t.00000.sql")
			if err := os.WriteFile(path, []byte(tc.sql), 0o644); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
				count++
				return nil
			}); err != nil {
				t.Fatalf("ReadSQLFile: %v", err)
			}
			if count != tc.rows {
				t.Errorf("got %d rows, want %d", count, tc.rows)
			}
		})
	}
}

func TestReadSQLRowCharsetIntroducer(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want []string // expected decoded values of the single row
	}{
		{
			// mysqldump default (no --hex-blob): _binary 'escaped' (single-quote)
			// with embedded ',' ')' and a \Z (0x1A).
			name: "single-quote",
			sql:  "INSERT INTO `t` VALUES (1,_binary 'a,b)\\Z',2);\n",
			want: []string{"1", "a,b)\x1a", "2"},
		},
		{
			// mydumper default: _binary "escaped" (double-quote) with \0.
			name: "double-quote-nul",
			sql:  "INSERT INTO `t` VALUES (1,_binary \"x\\0y\",2);\n",
			want: []string{"1", "x\x00y", "2"},
		},
		{
			// other introducer (text columns under some configs)
			name: "utf8mb4",
			sql:  "INSERT INTO `t` VALUES (1,_utf8mb4'héllo');\n",
			want: []string{"1", "héllo"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "shop.t.00000.sql")
			if err := os.WriteFile(path, []byte(tc.sql), 0o644); err != nil {
				t.Fatal(err)
			}
			var rows [][]string
			if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
				rows = append(rows, append([]string(nil), values...))
				return nil
			}); err != nil {
				t.Fatalf("ReadSQLFile: %v", err)
			}
			if len(rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rows))
			}
			if len(rows[0]) != len(tc.want) {
				t.Fatalf("got %d cols %q, want %d", len(rows[0]), rows[0], len(tc.want))
			}
			for c, w := range tc.want {
				if rows[0][c] != w {
					t.Errorf("col%d = %q, want %q", c, rows[0][c], w)
				}
			}
		})
	}
}

func TestReadSQLRowConvertJSON(t *testing.T) {
	// mydumper JSON encoding CONVERT("<json>" USING <charset>) must yield the
	// JSON document, not the wrapper text — even with ')' inside a JSON string.
	// Row 3 covers the single-quoted inner-literal branch of parseConvertExpr.
	sql := "INSERT INTO `j` (`id`,`doc`) VALUES" +
		"(1,CONVERT(\"{\\\"k\\\": \\\"a)b\\\"}\" USING UTF8MB4))\n" +
		",(2,CONVERT(\"[1, 2]\" USING UTF8MB4))\n" +
		",(3,CONVERT('{\"y\": 2}' USING utf8mb4));\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.j.00000.sql")
	if err := os.WriteFile(path, []byte(sql), 0o644); err != nil {
		t.Fatal(err)
	}
	var rows [][]string
	if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		rows = append(rows, append([]string(nil), values...))
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
	if len(rows[0]) != 2 || rows[0][1] != `{"k": "a)b"}` {
		t.Errorf("row0 = %q, want doc=%q", rows[0], `{"k": "a)b"}`)
	}
	if len(rows[1]) != 2 || rows[1][1] != "[1, 2]" {
		t.Errorf("row1 = %q, want doc=%q", rows[1], "[1, 2]")
	}
	if len(rows[2]) != 2 || rows[2][1] != `{"y": 2}` {
		t.Errorf("row2 (single-quoted CONVERT) = %q, want doc=%q", rows[2], `{"y": 2}`)
	}
}

// copyFixture copies a file from testdata/ to dst.
func copyFixture(t *testing.T, name, dst string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Fatalf("write fixture %s: %v", dst, err)
	}
}

// TestRunMydumperBinaryJSONRoundTrip is the committed end-to-end proof: the real
// mydumper v1.0.3 fixture (schema + data) flows through baseline.Run into Parquet
// and reads back byte-exact. Unlike the ReadSQLFile-level test it also exercises
// convertValue + MysqlToParquetNode (the schema-mapping hop), so a regression in
// either is caught here.
func TestRunMydumperBinaryJSONRoundTrip(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()
	// mydumper-named copies so DiscoverTables groups them as ptest.bins.
	copyFixture(t, "mydumper_v1_binary_json-schema.sql", filepath.Join(inputDir, "ptest.bins-schema.sql"))
	copyFixture(t, "mydumper_v1_binary_json.sql", filepath.Join(inputDir, "ptest.bins.00000.sql"))
	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}

	stats, err := Run(context.Background(), Config{
		InputDir: inputDir, OutputDir: outputDir, Compression: "none", RowGroupSize: 100,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.RowsWritten != 3 {
		t.Fatalf("RowsWritten = %d, want 3", stats.RowsWritten)
	}

	var parquetPath string
	_ = filepath.Walk(outputDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && filepath.Ext(path) == ".parquet" {
			parquetPath = path
		}
		return nil
	})
	if parquetPath == "" {
		t.Fatal("no .parquet output found")
	}

	rf, err := os.Open(parquetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()
	info, err := rf.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(rf, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	reader := parquet.NewReader(pf)
	defer reader.Close()
	rows := make([]parquet.Row, 3)
	n, _ := reader.ReadRows(rows)
	if n != 3 {
		t.Fatalf("read %d rows, want 3", n)
	}

	// Parquet columns are alphabetical: bl(0) bt(1) id(2) js(3) txt(4) vb(5).
	byID := map[int32]parquet.Row{}
	for _, r := range rows[:n] {
		byID[r[2].Int32()] = r
	}
	ba := func(v parquet.Value) string { return string(v.ByteArray()) }

	r1 := byID[1] // vb=a,b)  bl='"  bt=0xAA  js={"m": "done) here", "n": 5}
	if got := ba(r1[5]); got != "a,b)" {
		t.Errorf("id1 vb = %q, want %q", got, "a,b)")
	}
	if got := ba(r1[0]); got != "'\"" {
		t.Errorf("id1 bl = %q, want %q", got, "'\"")
	}
	if got := r1[1].ByteArray(); len(got) != 1 || got[0] != 0xAA {
		t.Errorf("id1 bt = % x, want AA", got)
	}
	if got := ba(r1[3]); got != `{"m": "done) here", "n": 5}` {
		t.Errorf("id1 js = %q", got)
	}

	r2 := byID[2] // vb=NUL,0x1A,CR,LF,backslash  bl=Hello  bt=0x01  js=[1, 2, 3]
	if got := ba(r2[5]); got != "\x00\x1a\r\n\\" {
		t.Errorf("id2 vb = %q, want NUL,0x1A,CR,LF,backslash", got)
	}
	if got := ba(r2[0]); got != "Hello" {
		t.Errorf("id2 bl = %q, want Hello", got)
	}
	if got := r2[1].ByteArray(); len(got) != 1 || got[0] != 0x01 {
		t.Errorf("id2 bt = % x, want 01", got)
	}
	if got := ba(r2[3]); got != "[1, 2, 3]" {
		t.Errorf("id2 js = %q, want [1, 2, 3]", got)
	}

	r3 := byID[3] // binary/json columns NULL
	if !r3[5].IsNull() || !r3[0].IsNull() || !r3[3].IsNull() || !r3[1].IsNull() {
		t.Errorf("id3 vb/bl/bt/js should be NULL, got %v", r3)
	}
}

// ─── DiscoverTables ───────────────────────────────────────────────────────────

func TestDiscoverTables(t *testing.T) {
	dir := t.TempDir()
	files := []string{
		"shop.orders-schema.sql",
		"shop.orders.00000.sql",
		"shop.orders.00001.sql",
		"shop.users-schema.sql",
		"shop.users.00000.dat",
		"metadata",
		"shop-schema-create.sql", // database-level schema — no table
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f), []byte(""), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	tables, err := DiscoverTables(dir)
	if err != nil {
		t.Fatalf("DiscoverTables: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("got %d tables, want 2: %+v", len(tables), tables)
	}
	// Sorted alphabetically: orders before users
	if tables[0].Table != "orders" {
		t.Errorf("tables[0] = %q, want orders", tables[0].Table)
	}
	if len(tables[0].DataFiles) != 2 {
		t.Errorf("orders has %d data files, want 2", len(tables[0].DataFiles))
	}
	if tables[0].Format != "sql" {
		t.Errorf("orders format = %q, want sql", tables[0].Format)
	}
	if tables[1].Table != "users" || tables[1].Format != "tab" {
		t.Errorf("tables[1] = %+v, want users/tab", tables[1])
	}
}

// TestDiscoverTables_noChunkSuffix verifies that mydumper 0.10.0's unchunked
// file naming (<db>.<table>.sql without the .<chunk> number) is recognized by
// DiscoverTables. Ubuntu 24.04's apt package ships mydumper 0.10.0 which uses
// this format; the chunked format (<db>.<table>.00000.sql) was introduced in
// 0.11.0. Both must work so the dump → baseline pipeline succeeds regardless
// of which mydumper version the operator has installed (#221).
func TestDiscoverTables_noChunkSuffix(t *testing.T) {
	dir := t.TempDir()
	files := []string{
		"e2e_source.orders-schema.sql",
		"e2e_source.orders.sql", // NO chunk number — mydumper 0.10.0 format
		"e2e_source.users-schema.sql",
		"e2e_source.users.sql", // same
		"e2e_source-schema-create.sql",
		"metadata",
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f), []byte(""), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	tables, err := DiscoverTables(dir)
	if err != nil {
		t.Fatalf("DiscoverTables: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("got %d tables, want 2: %+v", len(tables), tables)
	}
	// Sorted alphabetically: orders before users
	if tables[0].Database != "e2e_source" || tables[0].Table != "orders" {
		t.Errorf("tables[0] = %q.%q, want e2e_source.orders", tables[0].Database, tables[0].Table)
	}
	if len(tables[0].DataFiles) != 1 {
		t.Errorf("orders has %d data files, want 1", len(tables[0].DataFiles))
	}
	if tables[0].Format != "sql" {
		t.Errorf("orders format = %q, want sql", tables[0].Format)
	}
	if tables[1].Database != "e2e_source" || tables[1].Table != "users" {
		t.Errorf("tables[1] = %q.%q, want e2e_source.users", tables[1].Database, tables[1].Table)
	}
}

// TestDiscoverTables_mixedChunkAndNoChunk verifies that if a directory somehow
// contains both chunked and unchunked files for different tables, both are
// discovered. This isn't a realistic mydumper output but guards against the
// fallback path accidentally breaking the chunked path.
func TestDiscoverTables_mixedChunkAndNoChunk(t *testing.T) {
	dir := t.TempDir()
	files := []string{
		"shop.orders-schema.sql",
		"shop.orders.00000.sql", // chunked (0.11.0+)
		"shop.orders.00001.sql",
		"shop.users-schema.sql",
		"shop.users.sql", // unchunked (0.10.0)
		"metadata",
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f), []byte(""), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	tables, err := DiscoverTables(dir)
	if err != nil {
		t.Fatalf("DiscoverTables: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("got %d tables, want 2: %+v", len(tables), tables)
	}
	if tables[0].Table != "orders" || len(tables[0].DataFiles) != 2 {
		t.Errorf("orders: want 2 chunked files, got %d", len(tables[0].DataFiles))
	}
	if tables[1].Table != "users" || len(tables[1].DataFiles) != 1 {
		t.Errorf("users: want 1 unchunked file, got %d", len(tables[1].DataFiles))
	}
}

// ─── Writer: convertValue, resolveCodec, sortColumnsForParquet ────────────────

func TestConvertValueTypes(t *testing.T) {
	cases := []struct {
		mysqlType string
		raw       string
		check     func(t *testing.T, v parquet.Value)
	}{
		{"bigint", "9876543210", func(t *testing.T, v parquet.Value) {
			if v.Int64() != 9876543210 {
				t.Errorf("got %d, want 9876543210", v.Int64())
			}
		}},
		{"float", "3.14", func(t *testing.T, v parquet.Value) {
			if v.Float() == 0 {
				t.Error("expected non-zero float")
			}
		}},
		{"double", "2.718281828", func(t *testing.T, v parquet.Value) {
			if v.Double() == 0 {
				t.Error("expected non-zero double")
			}
		}},
		{"datetime", "2025-01-15 12:30:00", func(t *testing.T, v parquet.Value) {
			if v.Int64() == 0 {
				t.Error("expected non-zero datetime microseconds")
			}
		}},
		{"datetime", "2025-01-15 12:30:00.123456", func(t *testing.T, v parquet.Value) {
			if v.Int64() == 0 {
				t.Error("expected non-zero datetime microseconds (with fractional)")
			}
		}},
		{"timestamp", "2025-01-15 12:30:00", func(t *testing.T, v parquet.Value) {
			if v.Int64() == 0 {
				t.Error("expected non-zero timestamp microseconds")
			}
		}},
		{"year", "2025", func(t *testing.T, v parquet.Value) {
			if v.Int32() != 2025 {
				t.Errorf("year got %d, want 2025", v.Int32())
			}
		}},
		{"decimal", "123.45", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "123.45" {
				t.Errorf("decimal got %q, want %q", string(v.ByteArray()), "123.45")
			}
		}},
		{"blob", "binarydata", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "binarydata" {
				t.Errorf("blob got %q, want %q", string(v.ByteArray()), "binarydata")
			}
		}},
		{"unknowntype", "fallback", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "fallback" {
				t.Errorf("unknown type fallback got %q, want %q", string(v.ByteArray()), "fallback")
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.mysqlType+"/"+tc.raw, func(t *testing.T) {
			col := Column{Name: "c", MySQLType: tc.mysqlType}
			v, err := convertValue(col, tc.raw)
			if err != nil {
				t.Fatalf("convertValue(%q, %q): %v", tc.mysqlType, tc.raw, err)
			}
			tc.check(t, v)
		})
	}
}

func TestConvertValueError(t *testing.T) {
	col := Column{Name: "n", MySQLType: "int"}
	if _, err := convertValue(col, "not-a-number"); err == nil {
		t.Error("expected error for non-numeric int value, got nil")
	}
}

func TestResolveCodec(t *testing.T) {
	if resolveCodec("zstd") == nil {
		t.Error("zstd codec should not be nil")
	}
	// Empty string defaults to zstd.
	if resolveCodec("") == nil {
		t.Error("empty string should default to zstd (non-nil)")
	}
	if resolveCodec("snappy") == nil {
		t.Error("snappy codec should not be nil")
	}
	if resolveCodec("gzip") == nil {
		t.Error("gzip codec should not be nil")
	}
	if resolveCodec("none") != nil {
		t.Error("'none' codec should be nil (no compression)")
	}
}

func TestSortColumnsForParquet(t *testing.T) {
	cols := []Column{
		{Name: "zebra", MySQLType: "int"},
		{Name: "apple", MySQLType: "varchar"},
		{Name: "mango", MySQLType: "bigint"},
	}
	sorted, order := sortColumnsForParquet(cols)

	wantNames := []string{"apple", "mango", "zebra"}
	for i, want := range wantNames {
		if sorted[i].Name != want {
			t.Errorf("sorted[%d] = %q, want %q", i, sorted[i].Name, want)
		}
	}
	// apple was MySQL index 1, mango was 2, zebra was 0.
	wantOrder := []int{1, 2, 0}
	for i, want := range wantOrder {
		if order[i] != want {
			t.Errorf("order[%d] = %d, want %d", i, order[i], want)
		}
	}
}

// ─── WriteAndReadParquet (round-trip) ─────────────────────────────────────────

func TestWriteAndReadParquet(t *testing.T) {
	cols := []Column{
		{Name: "id", MySQLType: "int", ParquetType: MysqlToParquetNode("int")},
		{Name: "name", MySQLType: "varchar", ParquetType: MysqlToParquetNode("varchar")},
		{Name: "score", MySQLType: "double", ParquetType: MysqlToParquetNode("double")},
		{Name: "born", MySQLType: "date", ParquetType: MysqlToParquetNode("date")},
	}

	dir := t.TempDir()
	outPath := filepath.Join(dir, "test.parquet")

	cfg := WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			"bintrail.snapshot_timestamp": "2025-02-28T00:00:00Z",
			"bintrail.source_database":    "testdb",
			"bintrail.source_table":       "testtable",
			"bintrail.mydumper_format":    "sql",
			"bintrail.bintrail_version":   "test",
		},
	}

	w, err := NewWriter(outPath, cols, cfg)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// Row 1: id=1, name="Alice", score=9.5, born=2000-01-15
	if err := w.WriteRow(
		[]string{"1", "Alice", "9.5", "2000-01-15"},
		[]bool{false, false, false, false},
	); err != nil {
		t.Fatalf("WriteRow 1: %v", err)
	}
	// Row 2: id=2, name=NULL, score=NULL, born=NULL
	if err := w.WriteRow(
		[]string{"2", "", "", ""},
		[]bool{false, true, true, true},
	); err != nil {
		t.Fatalf("WriteRow 2: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read back and verify values + metadata.
	rf, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open for read: %v", err)
	}
	defer rf.Close()
	info, err := rf.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	pf, err := parquet.OpenFile(rf, info.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if pf.NumRows() != 2 {
		t.Errorf("NumRows = %d, want 2", pf.NumRows())
	}

	// Verify key-value metadata was written.
	for key, want := range map[string]string{
		"bintrail.source_database": "testdb",
		"bintrail.source_table":    "testtable",
		"bintrail.mydumper_format": "sql",
	} {
		got, ok := pf.Lookup(key)
		if !ok {
			t.Errorf("metadata key %q not found", key)
		} else if got != want {
			t.Errorf("metadata[%q] = %q, want %q", key, got, want)
		}
	}
}

// ─── Run (orchestrator) ───────────────────────────────────────────────────────

// TestRun_writesIntegrityManifest: a completed baseline writes the _MANIFEST
// sidecar (#636), its crc32c matches the file's bytes, and the file validates
// clean. End-to-end for the write-side hook (no DB — Run is mydumper-SQL→Parquet).
func TestRun_writesIntegrityManifest(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"),
		[]byte("INSERT INTO `orders` VALUES(1,10,'9.99','n','2025-01-01 00:00:00','2025-01-15');\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), Config{InputDir: inputDir, OutputDir: outputDir, Compression: "none", RowGroupSize: 100}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var parquetPath string
	_ = filepath.Walk(outputDir, func(p string, info os.FileInfo, err error) error {
		if err == nil && filepath.Ext(p) == ".parquet" {
			parquetPath = p
		}
		return nil
	})
	if parquetPath == "" {
		t.Fatal("no .parquet produced")
	}
	snap := filepath.Dir(filepath.Dir(parquetPath))

	m, ok, err := baselineintegrity.LoadManifest(snap)
	if err != nil || !ok {
		t.Fatalf("a completed baseline must write a _MANIFEST: ok=%v err=%v", ok, err)
	}
	rel, _ := filepath.Rel(snap, parquetPath)
	want, listed := m.Files[filepath.ToSlash(rel)]
	if !listed {
		t.Fatalf("manifest missing the table file %q: %v", rel, m.Files)
	}
	got, err := baselineintegrity.CRC32CFile(parquetPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("manifest crc %s != file crc %s", want, got)
	}
	if err := baselineintegrity.ValidateLocalFile(parquetPath); err != nil {
		t.Errorf("a freshly-written baseline must validate clean, got %v", err)
	}
}

// TestRun_fatalOnManifestWriteFailure locks the write-side safety contract (#636):
// when the manifest can't be written, Run must FAIL and withhold _SUCCESS, so a
// complete-but-manifestless snapshot — an undetectable downgrade at read time —
// is never published. A refactor that softened the fatal return, or reordered
// _SUCCESS before the manifest, would silently break this; this test catches both.
func TestRun_fatalOnManifestWriteFailure(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"),
		[]byte("INSERT INTO `orders` VALUES(1,10,'9.99','n','2025-01-01 00:00:00','2025-01-15');\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Fixed timestamp → deterministic snapshot dir; pre-create _MANIFEST as a
	// DIRECTORY so the final os.WriteFile hits EISDIR (fails even when run as root).
	ts := time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
	tsDir := strings.ReplaceAll(ts.Format(time.RFC3339), ":", "-")
	snapDir := filepath.Join(outputDir, tsDir)
	if err := os.MkdirAll(filepath.Join(snapDir, baselineintegrity.ManifestName), 0o755); err != nil {
		t.Fatal(err)
	}

	if _, err := Run(context.Background(), Config{InputDir: inputDir, OutputDir: outputDir, Timestamp: ts, Compression: "none", RowGroupSize: 100}); err == nil {
		t.Fatal("Run must fail when the integrity manifest cannot be written")
	}
	// _SUCCESS must be ABSENT — the snapshot stays _INCOMPLETE, excluded from discovery.
	if _, statErr := os.Stat(filepath.Join(snapDir, SuccessMarker)); !os.IsNotExist(statErr) {
		t.Errorf("_SUCCESS must not be written when the manifest write failed (stat err=%v)", statErr)
	}
}

func TestRun(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	// Write metadata
	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	// Write schema
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	// Write SQL data
	const sqlData = "INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15'),(2,11,'19.99',NULL,'2025-01-02 00:00:00',NULL);\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"), []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		InputDir:     inputDir,
		OutputDir:    outputDir,
		Compression:  "none",
		RowGroupSize: 100,
	}

	stats, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.TablesProcessed != 1 {
		t.Errorf("TablesProcessed = %d, want 1", stats.TablesProcessed)
	}
	if stats.RowsWritten != 2 {
		t.Errorf("RowsWritten = %d, want 2", stats.RowsWritten)
	}

	// Find the output .parquet file and verify metadata.
	var parquetPath string
	_ = filepath.Walk(outputDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && filepath.Ext(path) == ".parquet" {
			parquetPath = path
		}
		return nil
	})
	if parquetPath == "" {
		t.Fatal("no .parquet file found in output directory")
	}

	// Verify binlog position metadata was written.
	rf, err := os.Open(parquetPath)
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	defer rf.Close()
	info, err := rf.Stat()
	if err != nil {
		t.Fatalf("stat parquet: %v", err)
	}
	pf, err := parquet.OpenFile(rf, info.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	for key, want := range map[string]string{
		MetaKeyBinlogFile: "binlog.000042",
		MetaKeyBinlogPos:  "12345",
		MetaKeyGTIDSet:    "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100",
	} {
		got, ok := pf.Lookup(key)
		if !ok {
			t.Errorf("metadata key %q not found", key)
		} else if got != want {
			t.Errorf("metadata[%q] = %q, want %q", key, got, want)
		}
	}
}

func TestRunWithTimestampOverride(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	// No metadata file — timestamp override must bypass metadata parsing.
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	const sqlData = "INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15');\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"), []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}

	ts := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	cfg := Config{
		InputDir:     inputDir,
		OutputDir:    outputDir,
		Timestamp:    ts,
		Compression:  "none",
		RowGroupSize: 100,
	}

	stats, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.TablesProcessed != 1 {
		t.Errorf("TablesProcessed = %d, want 1", stats.TablesProcessed)
	}
}

func TestRunWithTableFilter(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	// Two tables: orders and users.
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	const ordersData = "INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15');\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"), []byte(ordersData), 0o644); err != nil {
		t.Fatal(err)
	}
	const usersSchema = "CREATE TABLE `users` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.users-schema.sql"), []byte(usersSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.users.00000.sql"), []byte("INSERT INTO `users` VALUES(1);\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		InputDir:     inputDir,
		OutputDir:    outputDir,
		Tables:       []string{"shop.orders"}, // filter to orders only
		Compression:  "none",
		RowGroupSize: 100,
	}

	stats, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.TablesProcessed != 1 {
		t.Errorf("TablesProcessed = %d, want 1 (only orders, not users)", stats.TablesProcessed)
	}
}

func TestRunTabFormat(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	const schema2 = "CREATE TABLE `users` (\n  `id` int NOT NULL,\n  `name` varchar(100) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.users-schema.sql"), []byte(schema2), 0o644); err != nil {
		t.Fatal(err)
	}
	// TSV format: id TAB name; second row has NULL name.
	const tabData = "1\tAlice\n2\t\\N\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.users.00000.dat"), []byte(tabData), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		InputDir:     inputDir,
		OutputDir:    outputDir,
		Compression:  "none",
		RowGroupSize: 100,
	}

	stats, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.TablesProcessed != 1 {
		t.Errorf("TablesProcessed = %d, want 1", stats.TablesProcessed)
	}
	if stats.RowsWritten != 2 {
		t.Errorf("RowsWritten = %d, want 2", stats.RowsWritten)
	}
}

func TestRunMultiChunk(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	// Two chunk files, one row each.
	const chunk0 = "INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15');\n"
	const chunk1 = "INSERT INTO `orders` VALUES(2,11,'19.99','note2','2025-01-02 00:00:00','2025-01-16');\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"), []byte(chunk0), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00001.sql"), []byte(chunk1), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		InputDir:     inputDir,
		OutputDir:    outputDir,
		Compression:  "none",
		RowGroupSize: 100,
	}

	stats, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.RowsWritten != 2 {
		t.Errorf("RowsWritten = %d, want 2 (one row per chunk file)", stats.RowsWritten)
	}
}

func TestFilterTablesNoMatch(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	const sqlData = "INSERT INTO `orders` VALUES(1,10,'9.99','n','2025-01-01 00:00:00','2025-01-15');\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"), []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		InputDir:     inputDir,
		OutputDir:    outputDir,
		Tables:       []string{"shop.nonexistent"}, // no match
		Compression:  "none",
		RowGroupSize: 100,
	}

	// Pre-#461 this returned success with 0 tables processed — a typo'd
	// filter silently produced no baseline. It is now an error.
	_, err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("Run with a filter matching nothing succeeded; want an error")
	}
	if !strings.Contains(err.Error(), "matched none") {
		t.Errorf("error = %v, want the filter-matched-nothing message", err)
	}
}

func TestRunRetrySkipsExistingFiles(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	// Write metadata + schema + data for one table.
	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	const sqlData = "INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15');\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"), []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		InputDir:     inputDir,
		OutputDir:    outputDir,
		Compression:  "none",
		RowGroupSize: 100,
	}

	// First run: creates the Parquet file.
	stats1, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if stats1.RowsWritten != 1 {
		t.Fatalf("first Run: RowsWritten = %d, want 1", stats1.RowsWritten)
	}

	// Second run with Retry=true: should skip the existing file.
	cfg.Retry = true
	stats2, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("retry Run: %v", err)
	}
	if stats2.TablesProcessed != 1 {
		t.Errorf("retry Run: TablesProcessed = %d, want 1", stats2.TablesProcessed)
	}
	if stats2.RowsWritten != 0 {
		t.Errorf("retry Run: RowsWritten = %d, want 0 (file was skipped)", stats2.RowsWritten)
	}
	if stats2.FilesWritten != 1 {
		t.Errorf("retry Run: FilesWritten = %d, want 1 (counted as existing)", stats2.FilesWritten)
	}
}

// TestRunRetryReconvertsTruncatedParquet is the #798 guard: on --retry a
// nonzero-but-truncated Parquet (no footer, e.g. from an OOM/SIGKILL mid
// processTable) must be RE-CONVERTED, not skipped. The pre-fix skip decision
// was size-only, so it blessed the corrupt bytes; WriteManifest then CRC'd them
// and _SUCCESS was published, exploding only at restore time. The skip must now
// honor ReadParquetMetadata (footer parse) — skip only a file that parses.
func TestRunRetryReconvertsTruncatedParquet(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	const sqlData = "INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15');\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"), []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		InputDir:     inputDir,
		OutputDir:    outputDir,
		Compression:  "none",
		RowGroupSize: 100,
	}

	// First run: creates a valid Parquet file.
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("first Run: %v", err)
	}

	outPath := filepath.Join(snapDir(t, outputDir), "shop", "orders.parquet")
	if _, err := ReadParquetMetadata(outPath); err != nil {
		t.Fatalf("sanity: first-run Parquet should parse, got %v", err)
	}

	// Simulate an OOM/SIGKILL that left a truncated (footerless) file: nonzero
	// size but not a parseable Parquet.
	if err := os.Truncate(outPath, 8); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if fi, err := os.Stat(outPath); err != nil || fi.Size() == 0 {
		t.Fatalf("truncated file must remain nonzero size (stat err=%v)", err)
	}
	if _, err := ReadParquetMetadata(outPath); err == nil {
		t.Fatal("sanity: truncated Parquet must fail to parse (the re-convert signal)")
	}

	// Retry run: must RE-CONVERT the corrupt file, not skip it. The skip path
	// leaves RowsWritten==0; a re-conversion writes the row again.
	cfg.Retry = true
	stats, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("retry Run: %v", err)
	}
	if stats.RowsWritten != 1 {
		t.Errorf("retry Run: RowsWritten = %d, want 1 (truncated file must be re-converted, not skipped)", stats.RowsWritten)
	}
	// The healed file must parse again.
	if _, err := ReadParquetMetadata(outPath); err != nil {
		t.Errorf("after retry the Parquet should parse, got %v", err)
	}
}

// ─── ReadParquetMetadata ─────────────────────────────────────────────────────

func TestReadParquetMetadata(t *testing.T) {
	// Create a Parquet file with binlog position metadata via NewWriter.
	outPath := filepath.Join(t.TempDir(), "test.parquet")
	cols := []Column{
		{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)},
	}
	wantCreateTableSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	cfg := WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			MetaKeyBinlogFile:     "binlog.000042",
			MetaKeyBinlogPos:      "12345",
			MetaKeyGTIDSet:        "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100",
			MetaKeyCreateTableSQL: wantCreateTableSQL,
		},
	}
	w, err := NewWriter(outPath, cols, cfg)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read metadata back.
	m, err := ReadParquetMetadata(outPath)
	if err != nil {
		t.Fatalf("ReadParquetMetadata: %v", err)
	}
	if m.BinlogFile != "binlog.000042" {
		t.Errorf("BinlogFile = %q, want %q", m.BinlogFile, "binlog.000042")
	}
	if m.BinlogPos != 12345 {
		t.Errorf("BinlogPos = %d, want 12345", m.BinlogPos)
	}
	if m.GTIDSet != "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100" {
		t.Errorf("GTIDSet = %q, want %q", m.GTIDSet, "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100")
	}
	if m.CreateTableSQL != wantCreateTableSQL {
		t.Errorf("CreateTableSQL = %q, want %q", m.CreateTableSQL, wantCreateTableSQL)
	}
}

func TestReadParquetMetadata_noPosition(t *testing.T) {
	// Create a Parquet file without binlog position metadata.
	outPath := filepath.Join(t.TempDir(), "test.parquet")
	cols := []Column{
		{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)},
	}
	cfg := WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			"bintrail.source_database": "testdb",
		},
	}
	w, err := NewWriter(outPath, cols, cfg)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m, err := ReadParquetMetadata(outPath)
	if err != nil {
		t.Fatalf("ReadParquetMetadata: %v", err)
	}
	if m.BinlogFile != "" {
		t.Errorf("BinlogFile = %q, want empty", m.BinlogFile)
	}
	if m.BinlogPos != 0 {
		t.Errorf("BinlogPos = %d, want 0", m.BinlogPos)
	}
	if m.GTIDSet != "" {
		t.Errorf("GTIDSet = %q, want empty", m.GTIDSet)
	}
	if m.LSN != 0 {
		t.Errorf("LSN = %d, want 0", m.LSN)
	}
}

// Round-trip of the PG WAL LSN anchor (#593 slice A): written as the decimal
// string of the uint64 LSN, read back numerically.
func TestReadParquetMetadata_LSN(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "test.parquet")
	cols := []Column{
		{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)},
	}
	// 1/6B37B4C8 — an LSN whose text form would be lexically treacherous;
	// the metadata stores the plain decimal uint64.
	const wantLSN uint64 = 6093515976
	w, err := NewWriter(outPath, cols, WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			MetaKeyLSN: strconv.FormatUint(wantLSN, 10),
		},
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m, err := ReadParquetMetadata(outPath)
	if err != nil {
		t.Fatalf("ReadParquetMetadata: %v", err)
	}
	if m.LSN != wantLSN {
		t.Errorf("LSN = %d, want %d", m.LSN, wantLSN)
	}
	if m.BinlogFile != "" || m.BinlogPos != 0 {
		t.Errorf("BinlogFile/BinlogPos = %q/%d, want empty/0 (PG baseline)", m.BinlogFile, m.BinlogPos)
	}
}

// A corrupt LSN value warns and leaves LSN zero (mirrors the BinlogPos branch);
// the read itself must not fail.
func TestReadParquetMetadata_corruptLSN(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "test.parquet")
	cols := []Column{
		{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)},
	}
	w, err := NewWriter(outPath, cols, WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			MetaKeyLSN:        "0/1A2B3C4", // LSN TEXT form — not the decimal contract
			MetaKeyBinlogFile: "binlog.000042",
		},
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m, err := ReadParquetMetadata(outPath)
	if err != nil {
		t.Fatalf("ReadParquetMetadata: %v", err)
	}
	if m.LSN != 0 {
		t.Errorf("LSN = %d, want 0 (corrupt value zeroed)", m.LSN)
	}
	if m.BinlogFile != "binlog.000042" {
		t.Errorf("BinlogFile = %q, want %q (other keys unaffected)", m.BinlogFile, "binlog.000042")
	}
}

// ─── parseBaselineDirTimestamp ────────────────────────────────────────────────

func TestParseBaselineDirTimestamp(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  time.Time
		ok    bool
	}{
		{"valid UTC", "2025-02-28T00-00-00Z", time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC), true},
		{"valid with time", "2026-03-15T14-30-00Z", time.Date(2026, 3, 15, 14, 30, 0, 0, time.UTC), true},
		{"no T separator", "2025-02-28", time.Time{}, false},
		{"empty", "", time.Time{}, false},
		{"malformed time", "2025-02-28Tnot-a-time", time.Time{}, false},
		{"random folder", "some-folder", time.Time{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseBaselineDirTimestamp(tc.input)
			if ok != tc.ok {
				t.Errorf("ok = %v, want %v", ok, tc.ok)
			}
			if ok && !got.Equal(tc.want) {
				t.Errorf("time = %v, want %v", got, tc.want)
			}
		})
	}
}

// ─── DiscoverBaselines ───────────────────────────────────────────────────────

func TestDiscoverBaselines(t *testing.T) {
	baseDir := t.TempDir()

	// Create a well-formed baseline directory structure with a Parquet file.
	snapshotDir := filepath.Join(baseDir, "2025-02-28T00-00-00Z", "mydb")
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write a minimal Parquet file with binlog metadata.
	parquetPath := filepath.Join(snapshotDir, "orders.parquet")
	cols := []Column{
		{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)},
	}
	w, err := NewWriter(parquetPath, cols, WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			MetaKeyBinlogFile: "binlog.000042",
			MetaKeyBinlogPos:  "12345",
			MetaKeyGTIDSet:    "abc:1-100",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Also create a non-timestamp directory (should be skipped).
	if err := os.MkdirAll(filepath.Join(baseDir, "not-a-timestamp"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Discover baselines.
	results, err := DiscoverBaselines(baseDir)
	if err != nil {
		t.Fatalf("DiscoverBaselines: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d baselines, want 1", len(results))
	}

	b := results[0]
	wantTime := time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
	if !b.SnapshotTime.Equal(wantTime) {
		t.Errorf("SnapshotTime = %v, want %v", b.SnapshotTime, wantTime)
	}
	if b.Database != "mydb" {
		t.Errorf("Database = %q, want %q", b.Database, "mydb")
	}
	if b.Table != "orders" {
		t.Errorf("Table = %q, want %q", b.Table, "orders")
	}
	if b.BinlogFile != "binlog.000042" {
		t.Errorf("BinlogFile = %q, want %q", b.BinlogFile, "binlog.000042")
	}
	if b.BinlogPos != 12345 {
		t.Errorf("BinlogPos = %d, want 12345", b.BinlogPos)
	}
	if b.GTIDSet != "abc:1-100" {
		t.Errorf("GTIDSet = %q, want %q", b.GTIDSet, "abc:1-100")
	}
	if b.LSN != 0 {
		t.Errorf("LSN = %d, want 0 (key absent on a MySQL baseline)", b.LSN)
	}
}

// A PG-source baseline (#593) carries its WAL LSN anchor through discovery.
func TestDiscoverBaselines_LSN(t *testing.T) {
	baseDir := t.TempDir()
	snapshotDir := filepath.Join(baseDir, "2026-06-01T00-00-00Z", "pgdb")
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		t.Fatal(err)
	}
	parquetPath := filepath.Join(snapshotDir, "orders.parquet")
	cols := []Column{
		{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)},
	}
	w, err := NewWriter(parquetPath, cols, WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			MetaKeyLSN: "6093515976", // 1/6B37B4C8
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	results, err := DiscoverBaselines(baseDir)
	if err != nil {
		t.Fatalf("DiscoverBaselines: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d baselines, want 1", len(results))
	}
	if results[0].LSN != 6093515976 {
		t.Errorf("LSN = %d, want 6093515976", results[0].LSN)
	}
	if results[0].BinlogFile != "" {
		t.Errorf("BinlogFile = %q, want empty (PG baseline has no binlog keys)", results[0].BinlogFile)
	}
}

func TestProcessTable_EmptyDataFiles(t *testing.T) {
	dir := t.TempDir()

	// Write a minimal schema file.
	schemaPath := filepath.Join(dir, "shop.empty-schema.sql")
	schema := "CREATE TABLE `empty` (\n  `id` int,\n  `name` varchar(100)\n) ENGINE=InnoDB;\n"
	if err := os.WriteFile(schemaPath, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}

	outPath := filepath.Join(dir, "output", "empty.parquet")

	cfg := WriterConfig{
		Compression:  "none",
		RowGroupSize: 1000,
		Metadata: map[string]string{
			"bintrail.snapshot_timestamp": "2026-04-13T00:00:00Z",
			MetaKeyCreateTableSQL:         schema,
		},
	}

	tf := TableFiles{
		Database:   "shop",
		Table:      "empty",
		SchemaFile: schemaPath,
		Format:     "sql",
		// DataFiles intentionally empty — this is the empty-table case.
	}

	n, err := processTable(context.Background(), tf, outPath, cfg)
	if err != nil {
		t.Fatalf("processTable: %v", err)
	}
	if n != 0 {
		t.Errorf("row count = %d, want 0", n)
	}

	// Verify the Parquet file exists and can be read.
	f, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}

	if got := pf.NumRows(); got != 0 {
		t.Errorf("parquet rows = %d, want 0", got)
	}

	// Verify columns are present (sorted alphabetically in Parquet).
	fields := pf.Schema().Fields()
	if len(fields) != 2 {
		t.Fatalf("schema fields = %d, want 2", len(fields))
	}
	// Alphabetical: id, name.
	if fields[0].Name() != "id" {
		t.Errorf("field[0] = %q, want %q", fields[0].Name(), "id")
	}
	if fields[1].Name() != "name" {
		t.Errorf("field[1] = %q, want %q", fields[1].Name(), "name")
	}

	// Verify metadata includes create_table_sql.
	var foundCreateSQL bool
	for _, kv := range pf.Metadata().KeyValueMetadata {
		if kv.Key == MetaKeyCreateTableSQL {
			foundCreateSQL = true
			break
		}
	}
	if !foundCreateSQL {
		t.Error("Parquet metadata missing create_table_sql key")
	}
}

func TestDiscoverBaselines_emptyDir(t *testing.T) {
	results, err := DiscoverBaselines(t.TempDir())
	if err != nil {
		t.Fatalf("DiscoverBaselines: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("got %d baselines, want 0", len(results))
	}
}

// ─── zero-table refusals (#461) ──────────────────────────────────────────────

// TestRun_zeroTablesIsError: a metadata-only dump (mydumper exits 0 for a
// no-match --regex or missing SELECT privileges) must NOT convert into a
// silent "success" with no baseline.
func TestRun_zeroTablesIsError(t *testing.T) {
	inputDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := Run(context.Background(), Config{
		InputDir:     inputDir,
		OutputDir:    t.TempDir(),
		Compression:  "none",
		RowGroupSize: 100,
	})
	if err == nil {
		t.Fatal("Run on a metadata-only dump succeeded; want an error")
	}
	if !strings.Contains(err.Error(), "no tables found") {
		t.Fatalf("error = %v, want the no-tables message", err)
	}
}

// TestRun_filterMatchesNothingIsError: a --tables filter that eliminates every
// discovered table is a caller mistake (typo'd schema.table), not a no-op.
func TestRun_filterMatchesNothingIsError(t *testing.T) {
	inputDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"),
		[]byte("INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15');\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := Run(context.Background(), Config{
		InputDir:     inputDir,
		OutputDir:    t.TempDir(),
		Tables:       []string{"shop.nope"},
		Compression:  "none",
		RowGroupSize: 100,
	})
	if err == nil {
		t.Fatal("Run with a filter matching nothing succeeded; want an error")
	}
	if !strings.Contains(err.Error(), "matched none") {
		t.Fatalf("error = %v, want the filter-matched-nothing message", err)
	}
}

// ─── completeness markers (#467) ─────────────────────────────────────────────

// snapDir returns the single <timestamp> snapshot directory under outputDir.
func snapDir(t *testing.T, outputDir string) string {
	t.Helper()
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			return filepath.Join(outputDir, e.Name())
		}
	}
	t.Fatal("no snapshot directory produced")
	return ""
}

// TestRun_writesSuccessMarker: a clean run marks the snapshot _SUCCESS and
// DiscoverBaselines treats it as complete.
func TestRun_writesSuccessMarker(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"),
		[]byte("INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15');\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Run(context.Background(), Config{
		InputDir: inputDir, OutputDir: outputDir, Compression: "none", RowGroupSize: 100,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	dir := snapDir(t, outputDir)
	if _, err := os.Stat(filepath.Join(dir, SuccessMarker)); err != nil {
		t.Fatalf("expected %s marker after a clean run: %v", SuccessMarker, err)
	}
	if _, err := os.Stat(filepath.Join(dir, IncompleteMarker)); !os.IsNotExist(err) {
		t.Fatalf("did not expect %s marker after a clean run (err=%v)", IncompleteMarker, err)
	}
	if !SnapshotComplete(dir) {
		t.Fatal("SnapshotComplete=false for a clean run")
	}
	got, err := DiscoverBaselines(outputDir)
	if err != nil {
		t.Fatalf("DiscoverBaselines: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("DiscoverBaselines returned %d, want 1", len(got))
	}
}

// TestRun_partialFailureMarksIncomplete: when one table fails mid-run, the
// snapshot is flagged _INCOMPLETE (no _SUCCESS) and DiscoverBaselines skips it,
// so a partial snapshot is never treated as the newest baseline (#467).
func TestRun_partialFailureMarksIncomplete(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	// Good table.
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"),
		[]byte("INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15');\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Broken table: a schema file with no parseable columns → ParseSchema fails
	// in processTable, failing this table while orders converts.
	if err := os.WriteFile(filepath.Join(inputDir, "shop.broken-schema.sql"), []byte("-- not a CREATE TABLE\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.broken.00000.sql"), []byte("INSERT INTO `broken` VALUES(1);\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Run(context.Background(), Config{
		InputDir: inputDir, OutputDir: outputDir, Compression: "none", RowGroupSize: 100,
	}); err == nil {
		t.Fatal("Run with a failing table succeeded; want an error")
	}

	dir := snapDir(t, outputDir)
	if _, err := os.Stat(filepath.Join(dir, SuccessMarker)); !os.IsNotExist(err) {
		t.Fatalf("did not expect %s marker after a partial run (err=%v)", SuccessMarker, err)
	}
	if _, err := os.Stat(filepath.Join(dir, IncompleteMarker)); err != nil {
		t.Fatalf("expected %s marker after a partial run: %v", IncompleteMarker, err)
	}
	if SnapshotComplete(dir) {
		t.Fatal("SnapshotComplete=true for a partial run; want false")
	}
	// Discovery must NOT surface the incomplete snapshot.
	got, err := DiscoverBaselines(outputDir)
	if err != nil {
		t.Fatalf("DiscoverBaselines: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("DiscoverBaselines returned %d for an incomplete snapshot; want 0", len(got))
	}
}

// TestRun_cancelledContextMarksIncomplete: a run cancelled before any table
// converts still leaves the snapshot dir flagged _INCOMPLETE. This pins the
// crash-safety fix — the marker is written BEFORE the workers launch, so an
// uncatchable kill mid-conversion can't leave a markerless partial that
// complete-by-default would serve as the newest baseline (#467). Pre-fix the
// snapshot dir was created lazily by the first worker, so a run that converted
// no table produced no dir and the post-wait marker write silently failed.
func TestRun_cancelledContextMarksIncomplete(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"),
		[]byte("INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15');\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Run launches workers — no table converts

	if _, err := Run(ctx, Config{
		InputDir: inputDir, OutputDir: outputDir, Compression: "none", RowGroupSize: 100,
	}); err == nil {
		t.Fatal("Run with a cancelled context succeeded; want an error")
	}

	dir := snapDir(t, outputDir)
	if _, err := os.Stat(filepath.Join(dir, IncompleteMarker)); err != nil {
		t.Fatalf("expected %s marker after a cancelled run: %v", IncompleteMarker, err)
	}
	if _, err := os.Stat(filepath.Join(dir, SuccessMarker)); !os.IsNotExist(err) {
		t.Fatalf("did not expect %s marker after a cancelled run (err=%v)", SuccessMarker, err)
	}
	if SnapshotComplete(dir) {
		t.Fatal("SnapshotComplete=true for a cancelled run; want false")
	}
	got, err := DiscoverBaselines(outputDir)
	if err != nil {
		t.Fatalf("DiscoverBaselines: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("DiscoverBaselines returned %d for a cancelled run; want 0", len(got))
	}
}

// TestSnapshotComplete_legacyMarkerless: a pre-marker snapshot (neither marker)
// stays complete-by-default so existing baselines keep working.
func TestSnapshotComplete_legacyMarkerless(t *testing.T) {
	dir := t.TempDir()
	if !SnapshotComplete(dir) {
		t.Fatal("a marker-absent (legacy) snapshot must be complete-by-default")
	}
}

// TestDiscoverBaselinesReport_datesTheFoldersItSkipped (#1639): the status
// grading needs WHEN a skipped folder is from, which its name still says.
func TestDiscoverBaselinesReport_datesTheFoldersItSkipped(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory read permissions")
	}
	root := t.TempDir()
	older, newer := "2026-09-01T06-00-00Z", "2026-09-02T06-00-00Z"
	for _, d := range []string{filepath.Join(root, older, "shop"), filepath.Join(root, newer, "shop"), filepath.Join(root, newer, "crm")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{filepath.Join(root, older), filepath.Join(root, newer, "crm")} {
		if err := os.Chmod(dir, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	}
	_, unreadable, err := DiscoverBaselinesReport(root)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"2026-09-01T06:00:00Z": true, "2026-09-02T06:00:00Z": true}
	if len(unreadable) != 2 {
		t.Fatalf("unreadable = %v, want both skipped folders", unreadable)
	}
	for _, u := range unreadable {
		if !want[u.UTC().Format(time.RFC3339)] {
			t.Fatalf("unreadable = %v", unreadable)
		}
	}
}
