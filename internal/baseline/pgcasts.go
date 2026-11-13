package baseline

import (
	"fmt"
	"sort"
)

// PGCast says how one column of a PostgreSQL-source snapshot file is read. The
// file holds every value as the text PostgreSQL wrote (#2755); As is what a
// state view reads that text as:
//
//   - a DuckDB type name (BIGINT, DECIMAL(12,2), DATE, ...): CAST to it
//   - PGAsText: text that compares byte by byte, as PostgreSQL's does
//   - PGAsBytes: a bytea, written as \x and hex digits
//   - PGAsNumeric: a numeric declared without precision
//   - "": left as the text it is (an array, an enum, a type not listed here)
type PGCast struct {
	Name string
	As   string
}

const (
	PGAsText    = "text"
	PGAsBytes   = "bytes"
	PGAsNumeric = "numeric"
)

// PGNumericScale is the scale a numeric declared without precision is read
// with: an exact DECIMAL(38, PGNumericScale), 20 digits before the point. A
// value with more digits on either side does not fit and fails the statement
// that reads it; it is never rounded.
const PGNumericScale = 18

// PostgreSQL's built-in type OIDs (pg_type.dat). They are fixed across
// versions; a type created by an extension or a user has an OID assigned at
// creation and is not listed.
const (
	pgOIDBool        = 16
	pgOIDBytea       = 17
	pgOIDName        = 19
	pgOIDInt8        = 20
	pgOIDInt2        = 21
	pgOIDInt4        = 23
	pgOIDText        = 25
	pgOIDOID         = 26
	pgOIDFloat4      = 700
	pgOIDFloat8      = 701
	pgOIDBpchar      = 1042
	pgOIDVarchar     = 1043
	pgOIDDate        = 1082
	pgOIDTimestamp   = 1114
	pgOIDTimestamptz = 1184
	pgOIDNumeric     = 1700
	pgOIDUUID        = 2950
)

// PGCasts lists the relation's columns in the table's own order, each with
// how its text is read. Every column is listed, the ones left as text
// included, so the list is also the table's column order.
//
// Left as text on purpose: json and jsonb (the operators that read them take
// text, and PostgreSQL's spelling of a jsonb value is kept), time and timetz
// (24:00:00 is a time in PostgreSQL and not in DuckDB), interval, arrays,
// enums, domains, ranges and every type of an extension.
func PGCasts(rel PGRelation) []PGCast {
	cols := make([]PGColumn, len(rel.Columns))
	copy(cols, rel.Columns)
	ordinals := true
	for _, c := range cols {
		if c.Ordinal <= 0 {
			ordinals = false
			break
		}
	}
	if ordinals {
		// attnum is the table's order. A snapshot written before it was
		// recorded keeps the order the columns were listed in, which the
		// catalog read also returns by attnum.
		sort.SliceStable(cols, func(i, j int) bool { return cols[i].Ordinal < cols[j].Ordinal })
	}
	out := make([]PGCast, len(cols))
	for i, c := range cols {
		out[i] = PGCast{Name: c.Name, As: pgCastOf(c)}
	}
	return out
}

func pgCastOf(c PGColumn) string {
	switch c.TypeOID {
	case pgOIDBool:
		return "BOOLEAN"
	case pgOIDInt2:
		return "SMALLINT"
	case pgOIDInt4:
		return "INTEGER"
	case pgOIDInt8:
		return "BIGINT"
	case pgOIDOID:
		return "UINTEGER"
	case pgOIDFloat4:
		return "FLOAT"
	case pgOIDFloat8:
		return "DOUBLE"
	case pgOIDDate:
		return "DATE"
	case pgOIDTimestamp:
		return "TIMESTAMP"
	case pgOIDTimestamptz:
		return "TIMESTAMPTZ"
	case pgOIDUUID:
		return "UUID"
	case pgOIDBytea:
		return PGAsBytes
	case pgOIDText, pgOIDVarchar, pgOIDBpchar, pgOIDName:
		return PGAsText
	case pgOIDNumeric:
		return pgNumericCast(c.TypeMod)
	}
	return ""
}

// pgNumericCast reads numeric's type modifier: -1 is no precision, else
// ((precision << 16) | scale) + 4, with the scale an 11-bit signed number
// (PostgreSQL 15 allows a negative one, and one above the precision). A
// column DuckDB has no DECIMAL for stays text.
func pgNumericCast(typmod int32) string {
	if typmod < 4 {
		return PGAsNumeric
	}
	mod := typmod - 4
	precision := int((mod >> 16) & 0xffff)
	scale := int(mod & 0x7ff)
	if scale >= 0x400 {
		scale -= 0x800
	}
	if precision < 1 || precision > MaxDuckDBDecimalPrecision || scale < 0 || scale > precision {
		return ""
	}
	return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
}
