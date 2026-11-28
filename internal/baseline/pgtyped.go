package baseline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// A PostgreSQL source's snapshot file holds every value as the text
// PostgreSQL wrote (#593, #2755), and the views cast that text back to the
// column's type on every read. Reading text costs most of what a report on
// the copy spends (#3315), so a file can now hold some columns as their type
// instead. Which ones, and as what, the file says itself: MetaKeyPGTypedColumns.
// A file without the key holds every column as text, as every file did
// before it; the readers take both, mixed in one snapshot.
//
// A column is stored typed only when its type has one spelling for each
// value, and the engine that reads the file spells that value back the same
// way: the integers, oid, date, uuid, and a numeric with a precision and a
// scale below it. So a reader that needs PostgreSQL's text of a stored
// column gets it with CAST(column AS VARCHAR), byte for byte (PGTextSQL),
// and every reader that compares, carries or writes text again goes on
// working on text. The writer checks it value by value: the text a value is
// written from must be the one it renders back to (pgTypedValue), and a
// value that is not (a date before the common era, a numeric NaN) is not
// written typed. The table is then written as text, as before.
//
// The primary key's columns stay text in every file: the key a change
// carries is that text (pgdelta.KeyOf), and the pairs beside a file find
// their rows by it.

// MetaKeyPGTypedColumns lists, on a PostgreSQL source's snapshot file, the
// columns the file holds as their type and not as text. The value is JSON:
// {"v":1,"columns":{"<name>":"<DuckDB type>",...}}, each type the one the
// views read the column as (PGCast.As).
const MetaKeyPGTypedColumns = "bintrail.pg_typed_columns"

// pgTypedVersion is the version of MetaKeyPGTypedColumns this build writes
// and reads. A file of another version may hold types this build would read
// through other rules, so it is refused (ParsePGTypedColumns).
const pgTypedVersion = 1

// PGTypedDisableEnv turns the typed columns off when set to 0, false, no or
// off: every column is then written as text, as before #3315.
const PGTypedDisableEnv = "BINTRAIL_PG_TYPED_COPY"

type pgTypedValue struct {
	V       int               `json:"v"`
	Columns map[string]string `json:"columns"`
}

// PGTypedAs is the type column c of a PostgreSQL table is stored as when its
// file holds it typed, or "" when it is always stored as text. It is always
// the type the views read the column as (pgCastOf), so a view reads a stored
// column as it is.
func PGTypedAs(c PGColumn) string {
	if _, ok := pgArrayElements[c.TypeOID]; ok {
		return ""
	}
	switch c.TypeOID {
	case pgOIDInt2, pgOIDInt4, pgOIDInt8, pgOIDOID, pgOIDDate, pgOIDUUID:
		return pgCastOf(c)
	case pgOIDNumeric:
		as := pgCastOf(c)
		p, s, ok := pgDecimalOf(as)
		// A scale equal to the precision has no digit before the point,
		// and the engine writes 0.5 as ".50" where PostgreSQL writes
		// "0.50": such a column stays text.
		if !ok || s >= p {
			return ""
		}
		return as
	}
	return ""
}

// PGTypedColumns are the columns of rel a snapshot file stores typed, by
// name: every column PGTypedAs gives a type, but the primary key's. A table
// with no primary key recorded stores none: its rows are told apart by
// other columns, which must stay text.
func PGTypedColumns(rel PGRelation) map[string]string {
	if len(rel.PrimaryKey) == 0 {
		return nil
	}
	// Two columns whose names differ only in case are one to the engine
	// that reads the file, and the readers then read the table as the file
	// is (collectPGRelation): such a table stays text.
	folded := map[string]bool{}
	for _, c := range rel.Columns {
		k := strings.ToLower(c.Name)
		if folded[k] {
			return nil
		}
		folded[k] = true
	}
	key := make(map[string]bool, len(rel.PrimaryKey))
	for _, k := range rel.PrimaryKey {
		key[k] = true
	}
	out := map[string]string{}
	for _, c := range rel.Columns {
		if key[c.Name] {
			continue
		}
		if as := PGTypedAs(c); as != "" {
			out[c.Name] = as
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// EncodePGTypedColumns is the footer value for columns.
func EncodePGTypedColumns(columns map[string]string) (string, error) {
	b, err := json.Marshal(pgTypedValue{V: pgTypedVersion, Columns: columns})
	if err != nil {
		return "", fmt.Errorf("encode %s: %w", MetaKeyPGTypedColumns, err)
	}
	return string(b), nil
}

// ParsePGTypedColumns reads a MetaKeyPGTypedColumns value: the stored
// columns, by name. An empty value is no column. A value of another version,
// or one that names a type this build does not store, is an error: the file
// would be read through the wrong rules.
func ParsePGTypedColumns(s string) (map[string]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var v pgTypedValue
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, fmt.Errorf("parse %s: %w", MetaKeyPGTypedColumns, err)
	}
	if v.V != pgTypedVersion {
		return nil, fmt.Errorf("parse %s: version %d, and this build reads version %d; a newer DBTrail wrote this file", MetaKeyPGTypedColumns, v.V, pgTypedVersion)
	}
	for name, as := range v.Columns {
		if name == "" {
			return nil, fmt.Errorf("parse %s: a column with no name", MetaKeyPGTypedColumns)
		}
		if _, err := pgTypedKindOf(as); err != nil {
			return nil, fmt.Errorf("parse %s: column %q: %w", MetaKeyPGTypedColumns, name, err)
		}
	}
	return v.Columns, nil
}

// CheckPGTypedColumns refuses a list of stored columns that does not agree
// with the table's shape: a column the shape does not have, one of the
// primary key, or one whose type is not the one its shape gives. The views
// read a stored column as it is, so a disagreement would read it as another
// type than the table's.
func CheckPGTypedColumns(rel PGRelation, typed map[string]string) error {
	if len(typed) == 0 {
		return nil
	}
	want := PGTypedColumns(rel)
	for name, as := range typed {
		if want[name] != as {
			return fmt.Errorf("%s names column %q as %s, and the table's shape stores it as %q", MetaKeyPGTypedColumns, name, as, want[name])
		}
	}
	return nil
}

// PGTextSQL is the SQL of column q's PostgreSQL text, for a column of a
// PostgreSQL source's snapshot file whether the file holds it as text or
// typed: CAST to VARCHAR, which is the text itself for a text column and the
// text the value was written from for a stored one (pgTypedValue checks it
// when it writes).
func PGTextSQL(q string) string { return "CAST(" + q + " AS VARCHAR)" }

// PGAllTextSQL is the select list that reads every column of a PostgreSQL
// source's snapshot file as PostgreSQL's text, under its own name and in the
// file's order, whatever the file holds typed. except names columns left
// out (file_row_number, for a read that adds it); qualifier, when not "",
// is the relation the columns are read from.
func PGAllTextSQL(qualifier string, except ...string) string {
	star := "*"
	if qualifier != "" {
		star = qualifier + ".*"
	}
	if len(except) > 0 {
		star += " EXCLUDE (" + strings.Join(except, ", ") + ")"
	}
	return "CAST(COLUMNS(" + star + ") AS VARCHAR)"
}

// PGScanSource is the FROM source a reader of every column of the snapshot
// file at lit (a SQL string literal of its path) reads it through:
// parquet_scan(lit), or, for a file of a PostgreSQL source that stores
// columns typed (MetaKeyPGTypedColumns), the same with every column as
// PostgreSQL's text, so the reader gets what it got from a file of text.
// Only the file's footer is read to tell.
//
// A list of another version is an error: this build checked the rendering
// only of the types it stores itself.
func PGScanSource(ctx context.Context, db *sql.DB, lit string) (string, error) {
	rows, err := db.QueryContext(ctx, "SELECT decode(value) FROM parquet_kv_metadata("+lit+") WHERE key = "+sqlQuoteLiteral(MetaKeyPGTypedColumns))
	if err != nil {
		return "", fmt.Errorf("read the footer of %s: %w", lit, err)
	}
	var values []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return "", fmt.Errorf("read the footer of %s: %w", lit, err)
		}
		values = append(values, v)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("read the footer of %s: %w", lit, err)
	}
	if len(values) == 0 {
		return "parquet_scan(" + lit + ")", nil
	}
	for _, v := range values {
		if _, err := ParsePGTypedColumns(v); err != nil {
			return "", fmt.Errorf("%s: %w; update DBTrail to read it", lit, err)
		}
	}
	return "(SELECT " + PGAllTextSQL("") + " FROM parquet_scan(" + lit + "))", nil
}

// ErrPGNotTyped is a value a stored column cannot hold as its type with the
// same text: the table is written as text instead. PGWriter.WriteRow returns
// it as a *PGNotTypedError, which names the column.
var ErrPGNotTyped = errors.New("a value that is not written typed")

// PGNotTypedError is ErrPGNotTyped for one column. It does not carry the
// value: a row's contents do not go into a log.
type PGNotTypedError struct{ Column string }

func (e *PGNotTypedError) Error() string {
	return fmt.Sprintf("column %q holds a value its type does not hold with the same text", e.Column)
}

func (e *PGNotTypedError) Unwrap() error { return ErrPGNotTyped }

type pgTypedKind int

const (
	pgKindInt16 pgTypedKind = iota + 1
	pgKindInt32
	pgKindInt64
	pgKindOID
	pgKindDate
	pgKindUUID
	pgKindDecimal
)

// pgTypedKindOf is the kind of a stored type. A DECIMAL is any precision and
// scale pgTypedValue takes.
func pgTypedKindOf(as string) (pgTypedKind, error) {
	switch as {
	case "SMALLINT":
		return pgKindInt16, nil
	case "INTEGER":
		return pgKindInt32, nil
	case "BIGINT":
		return pgKindInt64, nil
	case "UINTEGER":
		return pgKindOID, nil
	case "DATE":
		return pgKindDate, nil
	case "UUID":
		return pgKindUUID, nil
	}
	if p, s, ok := pgDecimalOf(as); ok && s < p {
		return pgKindDecimal, nil
	}
	return 0, fmt.Errorf("%q is not a type a column is stored as", as)
}

// pgDecimalOf reads "DECIMAL(p,s)" as pgNumericCast writes it.
func pgDecimalOf(as string) (precision, scale int, ok bool) {
	inner, found := strings.CutPrefix(as, "DECIMAL(")
	if !found {
		return 0, 0, false
	}
	inner, found = strings.CutSuffix(inner, ")")
	if !found {
		return 0, 0, false
	}
	ps, ss, found := strings.Cut(inner, ",")
	if !found {
		return 0, 0, false
	}
	p, err1 := strconv.Atoi(ps)
	s, err2 := strconv.Atoi(ss)
	if err1 != nil || err2 != nil || p < 1 || p > MaxDuckDBDecimalPrecision || s < 0 || s > p || fmt.Sprintf("DECIMAL(%d,%d)", p, s) != as {
		return 0, 0, false
	}
	return p, s, true
}

// pgTypedNode is the Parquet node of a stored column, optional like every
// column of a snapshot file.
func pgTypedNode(as string) (parquet.Node, error) {
	kind, err := pgTypedKindOf(as)
	if err != nil {
		return nil, err
	}
	// Whole numbers are written as differences from the one before
	// (DELTA_BINARY_PACKED): the plain encoding holds four or eight bytes
	// for every value, and left the file larger than its text.
	delta := func(n parquet.Node) parquet.Node {
		return parquet.Optional(parquet.Encoded(n, &parquet.DeltaBinaryPacked))
	}
	switch kind {
	case pgKindInt16:
		return delta(parquet.Int(16)), nil
	case pgKindInt32:
		return delta(parquet.Int(32)), nil
	case pgKindInt64:
		return delta(parquet.Int(64)), nil
	case pgKindOID:
		return delta(parquet.Uint(32)), nil
	case pgKindDate:
		return delta(parquet.Date()), nil
	case pgKindUUID:
		return parquet.Optional(parquet.UUID()), nil
	}
	p, s, _ := pgDecimalOf(as)
	switch {
	case p <= 9:
		return delta(parquet.Decimal(s, p, parquet.Int32Type)), nil
	case p <= 18:
		return delta(parquet.Decimal(s, p, parquet.Int64Type)), nil
	}
	return parquet.Optional(parquet.Decimal(s, p, parquet.FixedLenByteArrayType(16))), nil
}

// The days a DATE holds for infinity and -infinity, as the engine that reads
// the file holds them.
const (
	pgDateInfinity    = math.MaxInt32
	pgDateNegInfinity = -math.MaxInt32
)

// pgTypedValue is raw, a value PostgreSQL wrote for a column stored as as,
// as the Parquet value the column holds. ok is false when raw is not the one
// text PostgreSQL writes for the value it reads as, so that the value would
// not read back as raw: the column cannot be stored typed.
func pgTypedValueOf(kind pgTypedKind, precision, scale int, raw string) (parquet.Value, bool) {
	switch kind {
	case pgKindInt16, pgKindInt32, pgKindInt64:
		bits := map[pgTypedKind]int{pgKindInt16: 16, pgKindInt32: 32, pgKindInt64: 64}[kind]
		n, err := strconv.ParseInt(raw, 10, bits)
		if err != nil || strconv.FormatInt(n, 10) != raw {
			return parquet.Value{}, false
		}
		if kind == pgKindInt64 {
			return parquet.Int64Value(n), true
		}
		return parquet.Int32Value(int32(n)), true
	case pgKindOID:
		n, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || strconv.FormatUint(n, 10) != raw {
			return parquet.Value{}, false
		}
		return parquet.Int32Value(int32(uint32(n))), true
	case pgKindDate:
		days, ok := pgDateDays(raw)
		if !ok {
			return parquet.Value{}, false
		}
		return parquet.Int32Value(days), true
	case pgKindUUID:
		b, ok := pgUUIDBytes(raw)
		if !ok {
			return parquet.Value{}, false
		}
		return parquet.FixedLenByteArrayValue(b), true
	case pgKindDecimal:
		return pgDecimalValue(precision, scale, raw)
	}
	return parquet.Value{}, false
}

// pgDateDays reads a date as PostgreSQL writes it under DateStyle ISO (the
// pinned setting): YYYY-MM-DD, the year of four digits or more, or infinity
// and -infinity. A date before the common era ends in " BC" and is not taken.
func pgDateDays(raw string) (int32, bool) {
	switch raw {
	case "infinity":
		return pgDateInfinity, true
	case "-infinity":
		return pgDateNegInfinity, true
	}
	if len(raw) < 10 || raw[len(raw)-3] != '-' || raw[len(raw)-6] != '-' {
		return 0, false
	}
	ys, ms, ds := raw[:len(raw)-6], raw[len(raw)-5:len(raw)-3], raw[len(raw)-2:]
	if len(ys) < 4 || len(ys) > 7 || !allDigits(ys) || !allDigits(ms) || !allDigits(ds) {
		return 0, false
	}
	y, _ := strconv.ParseInt(ys, 10, 64)
	m, _ := strconv.Atoi(ms)
	d, _ := strconv.Atoi(ds)
	if y < 1 || m < 1 || m > 12 || d < 1 || d > 31 {
		return 0, false
	}
	days := daysFromCivil(y, m, d)
	if days <= pgDateNegInfinity || days >= pgDateInfinity {
		return 0, false
	}
	if pgDateText(int32(days)) != raw {
		// 2023-02-29 lands on 2023-03-01: not the text it was read from.
		return 0, false
	}
	return int32(days), true
}

// pgDateText is days as PostgreSQL writes the date under DateStyle ISO.
func pgDateText(days int32) string {
	switch days {
	case pgDateInfinity:
		return "infinity"
	case pgDateNegInfinity:
		return "-infinity"
	}
	y, m, d := civilFromDays(int64(days))
	return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
}

// daysFromCivil is the days since 1970-01-01 of a date of the proleptic
// Gregorian calendar (Howard Hinnant's algorithm), for any year.
func daysFromCivil(y int64, m, d int) int64 {
	if m <= 2 {
		y--
	}
	era := y / 400
	if y < 0 && y%400 != 0 {
		era--
	}
	yoe := y - era*400
	mp := (m + 9) % 12
	doy := int64((153*mp+2)/5 + d - 1)
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return era*146097 + doe - 719468
}

// civilFromDays is daysFromCivil the other way.
func civilFromDays(z int64) (y int64, m, d int) {
	z += 719468
	era := z / 146097
	if z < 0 && z%146097 != 0 {
		era--
	}
	doe := z - era*146097
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365
	y = yoe + era*400
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	mp := (5*doy + 2) / 153
	d = int(doy - (153*mp+2)/5 + 1)
	if mp < 10 {
		m = int(mp + 3)
	} else {
		m = int(mp - 9)
	}
	if m <= 2 {
		y++
	}
	return y, m, d
}

// pgUUIDBytes reads a uuid as PostgreSQL writes it: 32 lowercase hex digits
// in groups of 8, 4, 4, 4 and 12. Any other spelling is not PostgreSQL's.
func pgUUIDBytes(raw string) ([]byte, bool) {
	if len(raw) != 36 {
		return nil, false
	}
	out := make([]byte, 16)
	j := 0
	for i := 0; i < 36; {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if raw[i] != '-' {
				return nil, false
			}
			i++
			continue
		}
		hi, ok1 := lowerHex(raw[i])
		lo, ok2 := lowerHex(raw[i+1])
		if !ok1 || !ok2 {
			return nil, false
		}
		out[j] = hi<<4 | lo
		j++
		i += 2
	}
	return out, true
}

func lowerHex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	}
	return 0, false
}

// pgUUIDText is 16 bytes as PostgreSQL writes the uuid.
func pgUUIDText(b []byte) string {
	const hexd = "0123456789abcdef"
	out := make([]byte, 0, 36)
	for i, c := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out = append(out, '-')
		}
		out = append(out, hexd[c>>4], hexd[c&15])
	}
	return string(out)
}

// pgDecimalValue reads a numeric(precision, scale) as PostgreSQL writes it:
// an optional minus, the digits before the point (one 0 when there are
// none), and exactly scale digits after it (no point for a scale of 0). The
// value is the integer of all its digits, unscaled, as Parquet stores a
// DECIMAL. NaN, Infinity, a minus zero and any digit past the precision are
// not taken.
func pgDecimalValue(precision, scale int, raw string) (parquet.Value, bool) {
	s := raw
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	intPart, frac := s, ""
	if scale > 0 {
		var found bool
		intPart, frac, found = strings.Cut(s, ".")
		if !found || len(frac) != scale || !allDigits(frac) {
			return parquet.Value{}, false
		}
	}
	if intPart == "" || !allDigits(intPart) || (len(intPart) > 1 && intPart[0] == '0') || len(intPart) > precision-scale {
		return parquet.Value{}, false
	}
	digits := strings.TrimLeft(intPart+frac, "0")
	if digits == "" {
		if neg {
			return parquet.Value{}, false
		}
		digits = "0"
	}
	if precision <= 18 {
		n, err := strconv.ParseInt(digits, 10, 64)
		if err != nil {
			return parquet.Value{}, false
		}
		if neg {
			n = -n
		}
		if precision <= 9 {
			return parquet.Int32Value(int32(n)), true
		}
		return parquet.Int64Value(n), true
	}
	n, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return parquet.Value{}, false
	}
	if neg {
		n.Neg(n)
	}
	return parquet.FixedLenByteArrayValue(decimal128Bytes(n)), true
}

// decimal128Bytes is n as 16 bytes, big-endian two's complement, how
// Parquet stores a DECIMAL in a fixed length of 16.
func decimal128Bytes(n *big.Int) []byte {
	v := new(big.Int).Set(n)
	if v.Sign() < 0 {
		v.Add(v, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	out := make([]byte, 16)
	v.FillBytes(out)
	return out
}

// pgDecimalText is an unscaled DECIMAL as PostgreSQL writes the numeric.
func pgDecimalText(unscaled *big.Int, scale int) string {
	neg := unscaled.Sign() < 0
	digits := new(big.Int).Abs(unscaled).String()
	if scale > 0 {
		if len(digits) <= scale {
			digits = strings.Repeat("0", scale-len(digits)+1) + digits
		}
		digits = digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	}
	if neg {
		return "-" + digits
	}
	return digits
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s != ""
}

// pgTypedText is a stored value back as PostgreSQL's text: what the file's
// reader gets with CAST(column AS VARCHAR), written here for the writer's own
// tests.
func pgTypedText(kind pgTypedKind, scale int, v parquet.Value) string {
	switch kind {
	case pgKindInt16, pgKindInt32:
		return strconv.FormatInt(int64(v.Int32()), 10)
	case pgKindInt64:
		return strconv.FormatInt(v.Int64(), 10)
	case pgKindOID:
		return strconv.FormatUint(uint64(uint32(v.Int32())), 10)
	case pgKindDate:
		return pgDateText(v.Int32())
	case pgKindUUID:
		return pgUUIDText(v.ByteArray())
	case pgKindDecimal:
		var n *big.Int
		switch v.Kind() {
		case parquet.Int32:
			n = big.NewInt(int64(v.Int32()))
		case parquet.Int64:
			n = big.NewInt(v.Int64())
		default:
			n = new(big.Int).SetBytes(v.ByteArray())
			if len(v.ByteArray()) > 0 && v.ByteArray()[0]&0x80 != 0 {
				n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(8*len(v.ByteArray()))))
			}
		}
		return pgDecimalText(n, scale)
	}
	return ""
}

// PGWriter writes one table of a PostgreSQL source's snapshot: the columns of
// typed as their type, every other column as text, as Writer writes them.
// The rows, their order and the row groups are Writer's, so a file written
// typed holds every row at the row number the text file of the same COPY
// would (file_row_number, which a table delta names its rows by).
type PGWriter struct {
	pw          *parquet.Writer
	file        *os.File
	cols        []pgWriterColumn
	sourceOrder []int
}

type pgWriterColumn struct {
	name      string
	kind      pgTypedKind // 0: text
	precision int
	scale     int
}

// NewPGWriter creates the file at path for the columns names (the table's
// order, as the rows give their values), storing typed's columns typed.
// A name of typed that names no column is an error.
func NewPGWriter(path string, names []string, typed map[string]string, cfg WriterConfig) (*PGWriter, error) {
	cols := make([]Column, len(names))
	for i, n := range names {
		cols[i] = Column{Name: n, RawText: true}
	}
	sorted, order := sortColumnsForParquet(cols)
	group := make(parquet.Group, len(sorted))
	wcols := make([]pgWriterColumn, len(sorted))
	used := 0
	for i, c := range sorted {
		wcols[i] = pgWriterColumn{name: c.Name}
		as, ok := typed[c.Name]
		if !ok {
			group[c.Name] = parquet.Optional(parquet.String())
			continue
		}
		used++
		node, err := pgTypedNode(as)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", c.Name, err)
		}
		kind, _ := pgTypedKindOf(as)
		p, s, _ := pgDecimalOf(as)
		wcols[i] = pgWriterColumn{name: c.Name, kind: kind, precision: p, scale: s}
		group[c.Name] = node
	}
	if used != len(typed) {
		var missing []string
		for n := range typed {
			if !slices.Contains(names, n) {
				missing = append(missing, n)
			}
		}
		sort.Strings(missing)
		return nil, fmt.Errorf("typed columns %v are not columns of the table", missing)
	}
	if err := ValidateCodec(cfg.Compression); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create parquet file: %w", err)
	}
	opts := []parquet.WriterOption{parquet.NewSchema("row", group), parquet.MaxRowsPerRowGroup(int64(cfg.RowGroupSize))}
	for k, v := range cfg.Metadata {
		opts = append(opts, parquet.KeyValueMetadata(k, v))
	}
	if codec := resolveCodec(cfg.Compression); codec != nil {
		opts = append(opts, parquet.Compression(codec))
	}
	return &PGWriter{pw: parquet.NewWriter(f, opts...), file: f, cols: wcols, sourceOrder: order}, nil
}

// WriteRow writes one row, its values in the table's order. A value of a
// stored column that is not the one text PostgreSQL writes for it is
// ErrPGNotTyped, naming the column: nothing of the row is written.
func (w *PGWriter) WriteRow(values []string, nulls []bool) error {
	row := make(parquet.Row, len(w.cols))
	for i, c := range w.cols {
		src := w.sourceOrder[i]
		if src >= len(nulls) || nulls[src] {
			row[i] = parquet.NullValue().Level(0, 0, i)
			continue
		}
		raw := ""
		if src < len(values) {
			raw = values[src]
		}
		if c.kind == 0 {
			row[i] = parquet.ByteArrayValue([]byte(raw)).Level(0, 1, i)
			continue
		}
		v, ok := pgTypedValueOf(c.kind, c.precision, c.scale, raw)
		if !ok {
			return &PGNotTypedError{Column: c.name}
		}
		row[i] = v.Level(0, 1, i)
	}
	_, err := w.pw.WriteRows([]parquet.Row{row})
	return err
}

// SetMetadata is Writer.SetMetadata.
func (w *PGWriter) SetMetadata(key, value string) { w.pw.SetKeyValueMetadata(key, value) }

// Close flushes and closes the file.
func (w *PGWriter) Close() error {
	if err := w.pw.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("close parquet writer: %w", err)
	}
	return w.file.Close()
}
