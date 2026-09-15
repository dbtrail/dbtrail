package baseline

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// binaryTypeTokens is the single authority for "this MySQL type carries raw
// bytes, not text". Two independent decisions read it and MUST agree, which is
// why it is one map rather than two switch case-lists: the Parquet column node
// (a binary leaf, never the UTF-8 STRING default, #503 item 2) and the value
// conversion (decodeBinaryLiteral, which turns a --hex-blob 0x… literal back
// into bytes). A type present in one list and absent from the other would write
// non-UTF-8 bytes into a string column, or store the ASCII text "0x…" as the
// value.
//
// GEOMETRY and its subtypes carry WKB bytes. MySQL 8.0 canonicalizes
// GEOMETRYCOLLECTION to GEOMCOLLECTION; both spellings are listed because a
// schema file can come from either server generation.
var binaryTypeTokens = map[string]bool{
	"binary": true, "varbinary": true,
	"tinyblob": true, "blob": true, "mediumblob": true, "longblob": true,
	"bit":      true,
	"geometry": true, "point": true, "linestring": true, "polygon": true,
	"multipoint": true, "multilinestring": true, "multipolygon": true,
	"geometrycollection": true, "geomcollection": true,
}

// IsBinaryType reports whether a MySQL type token names a binary-family column
// — one whose Parquet representation is a byte array and whose text rendering
// is the --hex-blob 0x<hex> literal form. Exported for producers that build a
// baseline row's text values themselves rather than reading them out of a
// mydumper dump (full-table reconstruct's Parquet output, #1169).
func IsBinaryType(typeToken string) bool {
	return binaryTypeTokens[strings.ToLower(strings.TrimSpace(typeToken))]
}

// Column describes a single column parsed from a CREATE TABLE statement.
type Column struct {
	Name        string
	MySQLType   string // raw type token e.g. "int", "varchar", "datetime"
	Unsigned    bool   // true when the column carries the UNSIGNED attribute
	ParquetType parquet.Node
	// RawText marks a column whose values are stored VERBATIM as optional
	// Parquet strings, bypassing the MySQL type mapping entirely (both the
	// schema node and convertValue). Used by the PostgreSQL baseline producer
	// (#593): PG values arrive as pgoutput-style text and must round-trip
	// byte-identically so the PK join with the delta path stays an identity
	// string match — no type conversion, ever. MySQL callers never set it,
	// so the mydumper paths are untouched.
	RawText bool

	// DecimalPrecision and DecimalScale carry the declared (p,s) of a
	// decimal/numeric column, and are zero for every other type. They do NOT
	// change how the value is stored — a DECIMAL is still written as text, for
	// the reasons MysqlToParquetNode states — they exist so a consumer that
	// wants the number back can say which number it is. `bintrail views` uses
	// them to cast the column in the generated state views.
	//
	// MySQL's own defaults are applied here so the pair always describes a real
	// column: a bare `decimal` is (10,0) and `decimal(p)` is (p,0), the same as
	// the server's.
	DecimalPrecision int
	DecimalScale     int

	// DeclaredType is the column's type exactly as the CREATE TABLE declares
	// it: the type token, its parenthesized arguments and a trailing
	// UNSIGNED/ZEROFILL, e.g. "decimal(12,4)", "int(10) unsigned",
	// "enum('a)','b')". Nothing else from the line (DEFAULT, COLLATE,
	// COMMENT) is kept. It is the spelling information_schema reports as
	// COLUMN_TYPE, which is what lets a producer that carries this CREATE
	// TABLE forward notice that the source's column type moved since (#1651).
	// The arguments are read quote-aware, unlike colRe's group 3, so an enum
	// label holding ')' is not cut short.
	DeclaredType string

	// NotNull is true when the column's definition says NOT NULL (#1665): the
	// two words after the type, outside any quoted DEFAULT or COMMENT string.
	// A producer that carries this CREATE TABLE forward compares it with the
	// source's IS_NULLABLE, because a restore loads the carried definition.
	NotNull bool
}

// DecimalColumn names one decimal/numeric column of a baseline table and the
// precision and scale it was declared with.
type DecimalColumn struct {
	Name      string
	Precision int
	Scale     int
}

// colRe matches a column definition line from mydumper's schema SQL output.
// Groups: 1=name, 2=type token, 3=parenthesized type arguments, 4="unsigned"
// iff present.
// The unsigned attribute is matched only in the tail that immediately follows
// the type token (plus an optional display width like int(10)), so a column
// literally named `is_unsigned` or a COMMENT containing "unsigned" never trips
// it — the name lives in group 1 inside backticks, separate from group 4.
// The unsigned group is case-insensitive (`(?i:...)` inside a capture group, so
// group 4 is preserved): mydumper emits lowercase by contract, but an uppercase
// UNSIGNED from a hand-rolled schema must not silently fall through to signed.
// Group 3 holds a display width for int(10), a length for varchar(255), the
// value list for enum(...), and the precision and scale for decimal(6,2). It is
// captured rather than skipped only so the decimal family can report its
// declared (p,s); every other type ignores it, and the group is still OPTIONAL,
// so nothing about which lines match changes.
var colRe = regexp.MustCompile("^\\s+`([^`]+)`\\s+(\\w+)(?:\\s*\\(([^)]*)\\))?\\s*((?i:unsigned))?")

// generatedRe matches a STORED/VIRTUAL/PERSISTENT generated column's defining
// clause: MySQL's canonical "GENERATED ALWAYS AS (`price` * `qty`) STORED",
// and the shorter form MariaDB's SHOW CREATE TABLE may emit without the
// "GENERATED ALWAYS" prefix, e.g. "AS (`price` * `qty`) PERSISTENT"
// (PERSISTENT is MariaDB's legacy alias for STORED). mysqldump and mydumper
// both EXCLUDE generated columns from the INSERT column-list and VALUES
// tuples they emit (see internal/consistency/checksum.go's tableColumns,
// which drops them from the live-source fingerprint for the same reason) —
// so a schema that still lists them shifts every subsequent column's
// positional mapping in WriteRow (issue #767).
//
// Requiring the trailing VIRTUAL/STORED/PERSISTENT keyword (not just "AS (")
// is a deliberate, accepted trade-off: it is possible in principle for a
// COMMENT string to contain " as (...) stored" and false-trip this, but
// unlike the #506 UNSIGNED false-positive (which silently mis-typed a real
// column), the consequence here is the arity check in baseline.go failing
// loud on a column-count mismatch — never silent corruption.
var generatedRe = regexp.MustCompile(`(?i)\bAS\s*\(.*\)\s*(?:VIRTUAL|STORED|PERSISTENT)\b`)

// rowPeriodRe matches a MariaDB system-versioned table's explicit temporal
// period columns, whose defining clause is parenthesis-free and therefore
// invisible to generatedRe:
//
//	`row_start` timestamp(6) GENERATED ALWAYS AS ROW START,
//	`row_end`   timestamp(6) GENERATED ALWAYS AS ROW END,
//
// Verified against a real dump (issue #863, mydumper v1.0.3-1 vs MariaDB
// 11.4, same flags bintrail passes incl. --complete-insert): mydumper
// emits these columns in the -schema.sql exactly as above but EXCLUDES their
// values from the INSERT column-list and VALUES tuples, the same treatment
// STORED/VIRTUAL generated columns get — so ParseSchema must drop them from
// the positional column list too, or the #861 arity check refuses the whole
// table. IMPLICIT period columns (a table created WITH SYSTEM VERSIONING
// without declaring them) are invisible and appear in neither the schema nor
// the data files, so only the explicit form needs handling. The regex is
// anchored on the full "GENERATED ALWAYS AS ROW START|END" phrase and tolerates
// trailing attributes (e.g. INVISIBLE); a plain column merely NAMED row_start
// has no such clause and is kept. Same accepted COMMENT-false-positive
// trade-off as generatedRe: the failure mode is the loud arity check, never
// silent corruption. internal/consistency/checksum.go needs no sibling change —
// its tableColumns already excludes these via GENERATION_EXPRESSION, which
// MariaDB fills with the literal "ROW START"/"ROW END" (verified live).
//
// Known scope limit (#1266): MariaDB extends the table's PRIMARY KEY with the
// ROW END column, and the schema snapshot keeps it (PKColumnMetas selects on
// IsPK only), so FULL-TABLE reconstruct — and verify, which reconstructs —
// refuse such a table UP FRONT (reconstruct.GeneratedPKColumn gates both
// full-table paths; verify reports it inconclusive rather than error).
// Excluding the column from the join key instead would corrupt silently —
// the binlog carries history rows and versioned deletes under the same
// remaining key; see GeneratedPKColumn's comment for the MariaDB 11.4
// evidence. The baseline itself, single-row reconstruct with --pk-columns,
// and the query paths all work.
var rowPeriodRe = regexp.MustCompile(`(?i)\bGENERATED\s+ALWAYS\s+AS\s+ROW\s+(?:START|END)\b`)

// ParseSchema reads a mydumper <db>.<table>-schema.sql file and returns the
// ordered list of columns with their Parquet type mappings.
func ParseSchema(path string) ([]Column, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open schema file %s: %w", path, err)
	}
	defer f.Close()

	cols, err := parseSchemaFrom(f)
	if err != nil {
		return nil, fmt.Errorf("%w (schema file %s)", err, path)
	}
	return cols, nil
}

// ParseSchemaText parses the same mydumper schema SQL that ParseSchema reads
// from disk, but from an in-memory string.
//
// It exists for the consumers that already hold those exact bytes rather than a
// path: a baseline Parquet's MetaKeyCreateTableSQL metadata is the verbatim
// <db>.<table>-schema.sql that produced it (embedded by Run), so a producer
// deriving a NEW snapshot from an existing one — full-table reconstruct's
// Parquet output (#1169) — can recover the column list and its MySQL types
// without a dump directory on disk.
func ParseSchemaText(createSQL string) ([]Column, error) {
	return parseSchemaFrom(strings.NewReader(createSQL))
}

// parseSchemaFrom is the shared scanner both entry points above drive.
func parseSchemaFrom(r io.Reader) ([]Column, error) {
	var cols []Column
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		// Stop at PRIMARY KEY / KEY / UNIQUE or closing paren lines.
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "PRIMARY") ||
			strings.HasPrefix(trimmed, "UNIQUE") ||
			strings.HasPrefix(trimmed, "KEY") ||
			strings.HasPrefix(trimmed, "CONSTRAINT") ||
			trimmed == ");" || trimmed == ")" {
			break
		}
		loc := colRe.FindStringSubmatchIndex(line)
		if loc == nil {
			continue
		}
		m := colRe.FindStringSubmatch(line)
		if generatedRe.MatchString(line) || rowPeriodRe.MatchString(line) {
			// STORED/VIRTUAL generated column, or a system-versioning period
			// column (#863) — mydumper never dumps its value, so it must not
			// occupy a slot in the positional column list either.
			continue
		}
		name := m[1]
		typeToken := strings.ToLower(m[2])
		unsigned := strings.EqualFold(m[4], "unsigned")
		precision, scale := decimalPrecisionScale(typeToken, m[3])
		declared := declaredType(line[loc[4]:])
		cols = append(cols, Column{
			Name:             name,
			MySQLType:        typeToken,
			Unsigned:         unsigned,
			ParquetType:      mysqlToParquetNode(typeToken, unsigned),
			DecimalPrecision: precision,
			DecimalScale:     scale,
			DeclaredType:     declared,
			NotNull:          declaredNotNull(line[loc[4]+len(declared):]),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read schema: %w", err)
	}
	if len(cols) == 0 {
		return nil, errors.New("no columns found in schema SQL")
	}
	return cols, nil
}

// declaredNotNull reports whether a column's attributes, the text after its
// declared type, say NOT NULL. Words inside single-quoted strings are skipped
// (a doubled quote or a backslash escape stays inside the string), and a
// string counts as a word of its own, so DEFAULT 'NOT NULL' or a COMMENT
// holding the words is not read as the attribute.
func declaredNotNull(attrs string) bool {
	var words []string
	var w strings.Builder
	flush := func() {
		if w.Len() > 0 {
			words = append(words, strings.ToUpper(w.String()))
			w.Reset()
		}
	}
	for i := 0; i < len(attrs); i++ {
		ch := attrs[i]
		switch {
		case ch == '\'':
			flush()
			for i++; i < len(attrs); i++ {
				if attrs[i] == '\\' {
					i++
					continue
				}
				if attrs[i] == '\'' {
					if i+1 < len(attrs) && attrs[i+1] == '\'' {
						i++
						continue
					}
					break
				}
			}
			words = append(words, "")
		case ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9':
			w.WriteByte(ch)
		default:
			flush()
		}
	}
	flush()
	for i := 0; i+1 < len(words); i++ {
		if words[i] == "NOT" && words[i+1] == "NULL" {
			return true
		}
	}
	return false
}

// declaredType reads a column's declared type from the text that starts at its
// type token and returns exactly that span; see splitDeclaredType.
func declaredType(s string) string {
	_, _, _, end := splitDeclaredType(s)
	return s[:end]
}

// splitDeclaredType reads a type declaration from the start of s: the type
// token, a parenthesized argument list when one follows, and any run of
// UNSIGNED/ZEROFILL words after it. It returns the lowercased token, the raw
// arguments (without the parentheses), whether UNSIGNED or ZEROFILL was
// present (MySQL prints ZEROFILL only together with UNSIGNED), and the byte
// offset where the declaration ends.
//
// Single-quoted strings inside the arguments are skipped whole (a doubled
// single quote is how SHOW CREATE TABLE escapes one, and it reads as two adjacent strings here,
// which skips the same bytes), so an ENUM or SET label holding ')' does not end
// the list. An argument list that never closes runs to the end of s: callers
// compare spellings, and an unterminated one can only compare unequal.
func splitDeclaredType(s string) (token, args string, unsigned bool, end int) {
	i := 0
	for i < len(s) && (s[i] == '_' || s[i] >= 'a' && s[i] <= 'z' || s[i] >= 'A' && s[i] <= 'Z' || s[i] >= '0' && s[i] <= '9') {
		i++
	}
	token = strings.ToLower(s[:i])
	end = i
	j := i
	for j < len(s) && s[j] == ' ' {
		j++
	}
	if j < len(s) && s[j] == '(' {
		end = len(s)
		args = s[j+1:]
		inQuote := false
		for k := j + 1; k < len(s); k++ {
			if s[k] == '\'' {
				inQuote = !inQuote
			} else if s[k] == ')' && !inQuote {
				args, end = s[j+1:k], k+1
				break
			}
		}
	}
	for {
		rest := s[end:]
		trimmed := strings.TrimLeft(rest, " ")
		word := trimmed
		if n := strings.IndexAny(word, " ,"); n >= 0 {
			word = word[:n]
		}
		if !strings.EqualFold(word, "unsigned") && !strings.EqualFold(word, "zerofill") {
			break
		}
		unsigned = true
		end += len(rest) - len(trimmed) + len(word)
	}
	return token, args, unsigned, end
}

// integerDisplayWidthTypes are the types whose parenthesized argument is a
// display width, which changes nothing about the values stored. MySQL 8.0.19
// stopped printing it (int(11) became int), so a baseline dumped before a
// server upgrade and a schema read after it spell the same column differently.
var integerDisplayWidthTypes = map[string]bool{
	"tinyint": true, "smallint": true, "mediumint": true, "int": true, "integer": true, "bigint": true,
	"year": true,
}

// ComparableColumnType turns a type declaration into a spelling on which two
// declarations of the same column type compare equal: the one a CREATE TABLE
// carries (Column.DeclaredType) and the one information_schema reports as
// COLUMN_TYPE (#1651). Only what is spelling is folded:
//
//   - the token is lowercased, and the aliases MySQL canonicalizes are folded
//     (integer is int, numeric is decimal);
//   - an integer or YEAR display width is dropped;
//   - a decimal's arguments get MySQL's defaults (decimal is decimal(10,0));
//   - ZEROFILL is dropped (display only), UNSIGNED is kept;
//   - every other argument list is kept byte for byte: a length, a
//     fractional-seconds precision and an ENUM label list are part of the
//     type, and labels are case-sensitive, so they are not lowercased.
//
// Attributes after the type (DEFAULT, COLLATE, COMMENT) never reach the
// result, so a character set change is invisible here, as it is in
// COLUMN_TYPE.
func ComparableColumnType(declared string) string {
	token, args, unsigned, _ := splitDeclaredType(strings.TrimSpace(declared))
	switch token {
	case "integer":
		token = "int"
	case "numeric":
		token = "decimal"
	}
	switch {
	case integerDisplayWidthTypes[token]:
		args = ""
	case token == "decimal":
		if p, sc := decimalPrecisionScale(token, args); p > 0 {
			args = strconv.Itoa(p) + "," + strconv.Itoa(sc)
		}
	}
	out := token
	if args != "" {
		out += "(" + args + ")"
	}
	if unsigned {
		out += " unsigned"
	}
	return out
}

// decimalPrecisionScale reads the (p,s) out of a decimal or numeric column's
// type arguments, applying MySQL's own defaults for the spellings that OMIT
// them: `decimal` is (10,0) and `decimal(p)` is (p,0). Every other type gets
// (0,0) — the same parenthesized args carry a display width for int(10) and a
// length for varchar(255), and reporting either as a precision would invite a
// cast that means nothing.
//
// Arguments that are PRESENT but unparseable return (0,0), which callers read
// as "no usable precision" and which keeps the column stored and read as text.
// Falling back to the defaults there would be a guess, and not an inert one: a
// consumer casting to a narrower scale gets SILENT ROUNDING, not an error
// (DuckDB's CAST('1.239' AS DECIMAL(10,1)) is 1.2), so `decimal(10,2x)`
// answered as (10,0) would report every value in the column rounded to whole
// units. A refusal costs a cast; a guess corrupts the number.
//
// This never fails the schema parse. The column list is what callers came for,
// and a decimal whose precision could not be read is still a decimal.
//
// The MySQL defaults are only correct because the sole producer of the footer
// key these are read back out of is mydumper's canonical output. A future
// non-MySQL producer of an embedded CREATE TABLE must not inherit them: an
// unconstrained PostgreSQL `numeric`, for one, is not (10,0).
func decimalPrecisionScale(typeToken, args string) (precision, scale int) {
	if typeToken != "decimal" && typeToken != "numeric" {
		return 0, 0
	}
	if args == "" {
		return 10, 0
	}
	parts := strings.Split(args, ",")
	if len(parts) > 2 {
		return 0, 0
	}
	precision, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || precision <= 0 {
		return 0, 0
	}
	if len(parts) == 1 {
		return precision, 0
	}
	scale, err = strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || scale < 0 {
		return 0, 0
	}
	return precision, scale
}

// BuildParquetSchema converts a slice of Columns into a parquet.Schema.
func BuildParquetSchema(cols []Column) *parquet.Schema {
	group := make(parquet.Group, len(cols))
	for _, c := range cols {
		group[c.Name] = c.ParquetType
	}
	return parquet.NewSchema("row", group)
}

// MysqlToParquetNode maps a MySQL type token to the appropriate parquet-go node,
// treating integer types as signed. Callers with an UNSIGNED column should build
// the column via ParseSchema (which threads the attribute through) or via
// MysqlToParquetNode2 — this signed-only entry point is preserved for external
// callers that pass a bare type token (e.g. internal/archive, internal/byos).
func MysqlToParquetNode(typeToken string) parquet.Node {
	return mysqlToParquetNode(typeToken, false)
}

// MysqlToParquetNode2 is the unsigned-aware entry point for callers that hand-
// build a Column from a bare type token (internal/archive.BinlogEventColumns,
// internal/byos) rather than going through ParseSchema. It must be used for any
// column backing an UNSIGNED MySQL type (e.g. connection_id is INT UNSIGNED):
// the signed-only MysqlToParquetNode would emit an Int(32) column, and a value
// past int32 (a CONNECTION_ID() above 2147483647) would then fail conversion
// against that signed column. Passing unsigned=true widens it the same way
// ParseSchema does (INT UNSIGNED → Int64, BIGINT UNSIGNED → Uint64).
func MysqlToParquetNode2(typeToken string, unsigned bool) parquet.Node {
	return mysqlToParquetNode(typeToken, unsigned)
}

// mysqlToParquetNode maps a MySQL type token (plus its UNSIGNED attribute) to the
// appropriate parquet-go node. All fields are Optional so NULL values can be
// represented. UNSIGNED integers are widened so the full unsigned range
// round-trips without overflow into the signed-NULL fallback (issue #506):
//   - INT/INTEGER UNSIGNED (max 4294967295) → INT64 (holds it as a positive value)
//   - BIGINT UNSIGNED (max 18446744073709551615) → UINT64 (logical unsigned)
//
// TINYINT/SMALLINT/MEDIUMINT UNSIGNED already fit in int32, so they keep Int(32).
func mysqlToParquetNode(typeToken string, unsigned bool) parquet.Node {
	// Binary-family types first, from the shared authority above: storing WKB or
	// BLOB bytes in the STRING default would place non-UTF-8 bytes in a UTF-8
	// column (#503 item 2). The exact mydumper spatial encoding is unverified
	// end-to-end here; the binary type mapping is the safe floor.
	if IsBinaryType(typeToken) {
		return parquet.Optional(parquet.Leaf(parquet.ByteArrayType))
	}
	switch typeToken {
	case "int", "integer":
		if unsigned {
			return parquet.Optional(parquet.Int(64))
		}
		return parquet.Optional(parquet.Int(32))
	case "tinyint", "smallint", "mediumint":
		return parquet.Optional(parquet.Int(32))
	case "bigint":
		if unsigned {
			return parquet.Optional(parquet.Uint(64))
		}
		return parquet.Optional(parquet.Int(64))
	case "float":
		return parquet.Optional(parquet.Leaf(parquet.FloatType))
	case "double", "real":
		return parquet.Optional(parquet.Leaf(parquet.DoubleType))
	case "decimal", "numeric":
		// Preserve as string to avoid precision loss.
		return parquet.Optional(parquet.String())
	case "datetime", "timestamp":
		// Microseconds since Unix epoch (INT64 with timestamp logical type).
		return parquet.Optional(parquet.Timestamp(parquet.Microsecond))
	case "date":
		// Days since Unix epoch (INT32 with date logical type).
		return parquet.Optional(parquet.Date())
	case "time":
		return parquet.Optional(parquet.String())
	case "year":
		return parquet.Optional(parquet.Int(32))
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext",
		"enum", "set", "json":
		return parquet.Optional(parquet.String())
	default:
		// Unknown type — treat as string to avoid data loss.
		return parquet.Optional(parquet.String())
	}
}
