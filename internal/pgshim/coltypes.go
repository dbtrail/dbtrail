package pgshim

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// The type each result column is announced with (#2768), and its cells in
// PostgreSQL's binary format when a client asks for that.
//
// A column's type comes from the type the copy's engine gives it. For a
// PostgreSQL source the copy reads each column as the type the table
// declares (baseline.PGCasts, #2755), so a bare column of a type the copy
// reads comes back as that type: integer, bigint, numeric(p,s), double
// precision, boolean, date, timestamp, timestamptz, time, timetz, interval,
// uuid, bytea and one-dimensional arrays of those. A column the copy keeps as
// text (json, jsonb, varchar(n), char(n), an enum) is announced as text: the
// answer does not say which table column a value came from, and an
// expression over one is text anyway. A type of the engine that has no
// PostgreSQL equivalent this port is sure of (a struct, a map, a list of
// lists, a fixed-size array, a timestamp in nanoseconds, a bit string, json)
// is announced as text too, and its cells are the text this port always sent
// for it.
//
// The text of every cell stays the one pgtext.Cell writes, which is the text
// PostgreSQL writes for a value of the type. The binary form is read from
// that text, never from the engine's own value, so the two formats cannot
// disagree: a cell whose text does not read as its type is an error, never
// bytes that mean another value.

// colType is how one result column is announced in RowDescription.
type colType struct {
	oid    uint32
	size   int16
	typmod int32
}

// The type OIDs this file announces beyond the ones params.go reads.
const (
	oidTimetz = 1266
)

// textColumn is a column announced as text, whose binary form is its text.
var textColumn = colType{oid: oidText, size: -1, typmod: -1}

// arrayOIDs is the array type of each element type a list column can be
// announced with (pg_type.dat; fixed across versions).
var arrayOIDs = map[uint32]uint32{
	oidBool:        1000,
	oidBytea:       1001,
	oidInt2:        1005,
	oidInt4:        1007,
	oidText:        1009,
	oidInt8:        1016,
	oidFloat4:      1021,
	oidFloat8:      1022,
	oidTimestamp:   1115,
	oidDate:        1182,
	oidTime:        1183,
	oidTimestamptz: 1185,
	oidInterval:    1187,
	oidNumeric:     1231,
	oidTimetz:      1270,
	oidUUID:        2951,
}

// arrayElement is the element type of each array type in arrayOIDs.
var arrayElement = func() map[uint32]uint32 {
	m := make(map[uint32]uint32, len(arrayOIDs))
	for elem, arr := range arrayOIDs {
		m[arr] = elem
	}
	return m
}()

// columnType is how a column of the copy's type duck (the engine's type
// name, sqlsandbox.Column.Type) is announced. A type it does not list is
// text.
func columnType(duck string) colType {
	if elem, ok := strings.CutSuffix(duck, "[]"); ok {
		et, ok := scalarType(elem)
		arr, known := arrayOIDs[et.oid]
		if !ok || !known {
			return textColumn
		}
		// An array column's type modifier is its elements'.
		return colType{oid: arr, size: -1, typmod: et.typmod}
	}
	if t, ok := scalarType(duck); ok {
		return t
	}
	return textColumn
}

// scalarType is the PostgreSQL type of an engine type that is not a list, and
// whether there is one this port is sure of.
func scalarType(duck string) (colType, bool) {
	switch duck {
	case "BOOLEAN":
		return colType{oidBool, 1, -1}, true
	case "TINYINT", "SMALLINT", "UTINYINT":
		return colType{oidInt2, 2, -1}, true
	case "INTEGER", "USMALLINT":
		return colType{oidInt4, 4, -1}, true
	case "BIGINT", "UINTEGER":
		return colType{oidInt8, 8, -1}, true
	case "UBIGINT", "HUGEINT", "UHUGEINT":
		// Whole numbers wider than a bigint: PostgreSQL's sum of bigints is a
		// numeric too.
		return colType{oidNumeric, -1, -1}, true
	case "FLOAT":
		return colType{oidFloat4, 4, -1}, true
	case "DOUBLE":
		return colType{oidFloat8, 8, -1}, true
	case "DATE":
		return colType{oidDate, 4, -1}, true
	case "TIME":
		return colType{oidTime, 8, -1}, true
	case "TIMETZ", "TIME WITH TIME ZONE":
		return colType{oidTimetz, 12, -1}, true
	case "TIMESTAMP":
		return colType{oidTimestamp, 8, -1}, true
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return colType{oidTimestamptz, 8, -1}, true
	case "INTERVAL":
		return colType{oidInterval, 16, -1}, true
	case "UUID":
		return colType{oidUUID, 16, -1}, true
	case "BLOB":
		return colType{oidBytea, -1, -1}, true
	case "VARCHAR":
		return textColumn, true
	case "DECIMAL":
		return colType{oidNumeric, -1, -1}, true
	}
	if p, s, ok := decimalPrecision(duck); ok {
		// numeric's type modifier: ((precision << 16) | scale) + 4.
		return colType{oidNumeric, -1, int32(p<<16|s) + 4}, true
	}
	return colType{}, false
}

// decimalPrecision reads DECIMAL(p,s).
func decimalPrecision(duck string) (p, s int, ok bool) {
	inner, found := strings.CutPrefix(duck, "DECIMAL(")
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
	if err1 != nil || err2 != nil || p < 1 || p > 1000 || s < 0 || s > p {
		return 0, 0, false
	}
	return p, s, true
}

// columnTypes announces each column of the copy's answer.
func columnTypes(types []string) []colType {
	out := make([]colType, len(types))
	for i, t := range types {
		out[i] = columnType(t)
	}
	return out
}

// binaryCell is a cell in PostgreSQL's binary format for t, read from its
// text (what pgtext.Cell wrote, the text PostgreSQL writes). An error says
// the text does not read as t, or holds a value t's binary form cannot.
func binaryCell(t colType, text []byte) ([]byte, error) {
	if elem, ok := arrayElement[t.oid]; ok {
		return binaryArray(elem, string(text))
	}
	return binaryScalar(t.oid, string(text))
}

// pgEpoch is where PostgreSQL's binary dates and timestamps count from.
var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// A date or timestamp is read with a four-digit year: the copy refuses one
// before the common era (views.pgParts), and a year past 9999 does not parse,
// so it is an error here rather than a count PostgreSQL would read as another
// day.

func binaryScalar(oid uint32, s string) ([]byte, error) {
	switch oid {
	case oidText:
		return []byte(s), nil
	case oidBool:
		switch s {
		case "t":
			return []byte{1}, nil
		case "f":
			return []byte{0}, nil
		}
	case oidInt2, oidInt4, oidInt8:
		bits := map[uint32]int{oidInt2: 16, oidInt4: 32, oidInt8: 64}[oid]
		n, err := strconv.ParseInt(s, 10, bits)
		if err != nil {
			break
		}
		b := make([]byte, bits/8)
		switch bits {
		case 16:
			binary.BigEndian.PutUint16(b, uint16(n))
		case 32:
			binary.BigEndian.PutUint32(b, uint32(n))
		default:
			binary.BigEndian.PutUint64(b, uint64(n))
		}
		return b, nil
	case oidFloat4:
		f, ok := parsePGFloat(s, 32)
		if !ok {
			break
		}
		return binary.BigEndian.AppendUint32(nil, math.Float32bits(float32(f))), nil
	case oidFloat8:
		f, ok := parsePGFloat(s, 64)
		if !ok {
			break
		}
		return binary.BigEndian.AppendUint64(nil, math.Float64bits(f)), nil
	case oidNumeric:
		return binaryNumeric(s)
	case oidDate:
		d, err := time.Parse("2006-01-02", s)
		if err != nil {
			break
		}
		days := (d.Unix() - pgEpoch.Unix()) / 86400
		return binary.BigEndian.AppendUint32(nil, uint32(int32(days))), nil
	case oidTime:
		us, rest, ok := parseClock(s)
		if !ok || rest != "" || us > 24*3600*1e6 {
			break
		}
		return binary.BigEndian.AppendUint64(nil, uint64(us)), nil
	case oidTimetz:
		us, rest, ok := parseClock(s)
		if !ok || us > 24*3600*1e6 {
			break
		}
		off, ok := parseOffset(rest)
		if !ok {
			break
		}
		b := binary.BigEndian.AppendUint64(nil, uint64(us))
		// PostgreSQL keeps the zone as seconds WEST of UTC: +02 is -7200.
		return binary.BigEndian.AppendUint32(b, uint32(int32(-off))), nil
	case oidTimestamp, oidTimestamptz:
		text := s
		if oid == oidTimestamptz {
			var found bool
			// This port answers in UTC (startupParameters), and pgtext.Cell
			// writes every instant there.
			if text, found = strings.CutSuffix(s, "+00"); !found {
				break
			}
		}
		at, err := time.Parse("2006-01-02 15:04:05.999999", text)
		if err != nil {
			break
		}
		// Counted from Unix seconds, not as a time.Duration, which holds
		// only some 292 years.
		us := (at.Unix()-pgEpoch.Unix())*1e6 + int64(at.Nanosecond()/1000)
		return binary.BigEndian.AppendUint64(nil, uint64(us)), nil
	case oidInterval:
		months, days, us, ok := parseInterval(s)
		if !ok {
			break
		}
		b := binary.BigEndian.AppendUint64(nil, uint64(us))
		b = binary.BigEndian.AppendUint32(b, uint32(days))
		return binary.BigEndian.AppendUint32(b, uint32(months)), nil
	case oidUUID:
		h := strings.ReplaceAll(s, "-", "")
		b, err := hex.DecodeString(h)
		if err != nil || len(b) != 16 || len(s) != 36 {
			break
		}
		return b, nil
	case oidBytea:
		h, found := strings.CutPrefix(s, `\x`)
		if !found {
			break
		}
		b, err := hex.DecodeString(h)
		if err != nil {
			break
		}
		return b, nil
	default:
		return nil, fmt.Errorf("type %d has no binary form on this port", oid)
	}
	return nil, fmt.Errorf("%q does not read as a %s", s, typeName(oid))
}

// typeName is PostgreSQL's name for a type this file encodes, for an error.
func typeName(oid uint32) string {
	if n, ok := pgTypeName[oid]; ok {
		return n
	}
	switch oid {
	case oidDate:
		return "date"
	case oidTime:
		return "time"
	case oidTimetz:
		return "time with time zone"
	case oidTimestamp:
		return "timestamp"
	case oidTimestamptz:
		return "timestamp with time zone"
	case oidInterval:
		return "interval"
	case oidUUID:
		return "uuid"
	case oidText:
		return "text"
	}
	return fmt.Sprintf("type %d", oid)
}

// parsePGFloat reads a float as PostgreSQL writes one: digits, an exponent,
// NaN, Infinity or -Infinity.
func parsePGFloat(s string, bits int) (float64, bool) {
	switch s {
	case "NaN":
		return math.NaN(), true
	case "Infinity":
		return math.Inf(1), true
	case "-Infinity":
		return math.Inf(-1), true
	}
	if s == "" || strings.ContainsAny(s, "iInN_xXpP") {
		// Go reads inf, nan, hex floats and underscores; PostgreSQL's text
		// holds none of them.
		return 0, false
	}
	f, err := strconv.ParseFloat(s, bits)
	return f, err == nil
}

// errNumeric is a numeric's text this file does not read.
var errNumeric = errors.New("does not read as a numeric")

// binaryNumeric writes numeric's binary form: the count of base-10000 digits,
// the weight of the first one, the sign, the scale written (dscale), then the
// digits.
func binaryNumeric(s string) ([]byte, error) {
	header := func(ndigits, weight int, sign uint16, dscale int) []byte {
		b := binary.BigEndian.AppendUint16(nil, uint16(ndigits))
		b = binary.BigEndian.AppendUint16(b, uint16(int16(weight)))
		b = binary.BigEndian.AppendUint16(b, sign)
		return binary.BigEndian.AppendUint16(b, uint16(dscale))
	}
	switch s {
	case "NaN":
		return header(0, 0, 0xC000, 0), nil
	case "Infinity":
		return header(0, 0, 0xD000, 0), nil
	case "-Infinity":
		return header(0, 0, 0xF000, 0), nil
	}
	var sign uint16
	digits := s
	if rest, found := strings.CutPrefix(digits, "-"); found {
		sign, digits = 0x4000, rest
	}
	whole, frac, _ := strings.Cut(digits, ".")
	if whole == "" || !allDigits(whole) || !allDigits(frac) || (frac == "" && strings.HasSuffix(digits, ".")) {
		return nil, fmt.Errorf("%q %w", s, errNumeric)
	}
	dscale := len(frac)
	if dscale > 0x3FFF {
		return nil, fmt.Errorf("%q has more decimals than a numeric holds", s)
	}
	// Whole digits padded on the left and decimals on the right to groups
	// of four.
	if pad := (4 - len(whole)%4) % 4; pad > 0 {
		whole = strings.Repeat("0", pad) + whole
	}
	if pad := (4 - len(frac)%4) % 4; pad > 0 {
		frac += strings.Repeat("0", pad)
	}
	all := whole + frac
	groups := make([]uint16, len(all)/4)
	for i := range groups {
		n, _ := strconv.Atoi(all[i*4 : i*4+4])
		groups[i] = uint16(n)
	}
	weight := len(whole)/4 - 1
	for len(groups) > 0 && groups[0] == 0 {
		groups = groups[1:]
		weight--
	}
	for len(groups) > 0 && groups[len(groups)-1] == 0 {
		groups = groups[:len(groups)-1]
	}
	if len(groups) == 0 {
		// Zero: no digits, a weight of 0 and no sign, as PostgreSQL writes it.
		return header(0, 0, 0, dscale), nil
	}
	if weight > math.MaxInt16 || weight < math.MinInt16 {
		return nil, fmt.Errorf("%q is out of a numeric's range", s)
	}
	b := header(len(groups), weight, sign, dscale)
	for _, g := range groups {
		b = binary.BigEndian.AppendUint16(b, g)
	}
	return b, nil
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// parseClock reads HH:MM:SS[.ffffff] at the start of s as microseconds and
// returns what follows it.
func parseClock(s string) (us int64, rest string, ok bool) {
	if len(s) < 8 || s[2] != ':' || s[5] != ':' || !allDigits(s[0:2]) || !allDigits(s[3:5]) || !allDigits(s[6:8]) {
		return 0, "", false
	}
	h, _ := strconv.Atoi(s[0:2])
	m, _ := strconv.Atoi(s[3:5])
	sec, _ := strconv.Atoi(s[6:8])
	if m > 59 || sec > 59 {
		return 0, "", false
	}
	us = int64(h*3600+m*60+sec) * 1e6
	rest = s[8:]
	if f, found := strings.CutPrefix(rest, "."); found {
		n := 0
		for n < len(f) && f[n] >= '0' && f[n] <= '9' {
			n++
		}
		if n == 0 || n > 6 {
			return 0, "", false
		}
		v, _ := strconv.Atoi(f[:n] + strings.Repeat("0", 6-n))
		us += int64(v)
		rest = f[n:]
	}
	return us, rest, true
}

// parseOffset reads a zone offset as PostgreSQL writes it after a time:
// +HH, +HH:MM or +HH:MM:SS, as seconds east of UTC.
func parseOffset(s string) (int, bool) {
	if len(s) < 3 || (s[0] != '+' && s[0] != '-') {
		return 0, false
	}
	neg := s[0] == '-'
	parts := strings.Split(s[1:], ":")
	if len(parts) > 3 {
		return 0, false
	}
	secs := 0
	for i, p := range parts {
		if len(p) != 2 || !allDigits(p) {
			return 0, false
		}
		n, _ := strconv.Atoi(p)
		if i > 0 && n > 59 {
			return 0, false
		}
		secs += n * []int{3600, 60, 1}[i]
	}
	if secs > 15*3600+59*60+59 {
		return 0, false
	}
	if neg {
		secs = -secs
	}
	return secs, true
}

// parseInterval reads an interval as PostgreSQL writes one under
// IntervalStyle postgres (pgtext.Cell's pgInterval): counts of years, mons
// and days, each signed and at most once, in that order, then a clock
// [+-]HH:MM:SS[.ffffff] with as many hours as it needs.
func parseInterval(s string) (months, days int32, us int64, ok bool) {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return 0, 0, 0, false
	}
	seen := 0
	var m int64
	for i := 0; i < len(fields); {
		f := fields[i]
		if strings.Contains(f, ":") {
			if i != len(fields)-1 {
				return 0, 0, 0, false
			}
			neg := false
			switch {
			case strings.HasPrefix(f, "-"):
				neg, f = true, f[1:]
			case strings.HasPrefix(f, "+"):
				f = f[1:]
			}
			h, rest, found := strings.Cut(f, ":")
			if !found || h == "" || len(h) > 10 || !allDigits(h) {
				return 0, 0, 0, false
			}
			clock, tail, good := parseClock("00:" + rest)
			if !good || tail != "" {
				return 0, 0, 0, false
			}
			hours, _ := strconv.ParseInt(h, 10, 64)
			if hours > math.MaxInt64/3600e6-1 {
				return 0, 0, 0, false
			}
			us = hours*3600e6 + clock
			if neg {
				us = -us
			}
			i++
			continue
		}
		if i+1 >= len(fields) {
			return 0, 0, 0, false
		}
		n, err := strconv.ParseInt(f, 10, 32)
		if err != nil {
			return 0, 0, 0, false
		}
		switch unit := fields[i+1]; {
		case (unit == "year" || unit == "years") && seen < 1:
			m, seen = n*12, 1
		case (unit == "mon" || unit == "mons") && seen < 2:
			m, seen = m+n, 2
		case (unit == "day" || unit == "days") && seen < 3:
			days, seen = int32(n), 3
		default:
			return 0, 0, 0, false
		}
		i += 2
	}
	if m > math.MaxInt32 || m < math.MinInt32 {
		return 0, 0, 0, false
	}
	return int32(m), days, us, true
}

// binaryArray writes a one-dimensional array's binary form from its text:
// the number of dimensions (0 for an empty array), whether an element is
// NULL, the element type, the length and lower bound (1) of the dimension,
// then each element as its length and its binary form, -1 for NULL.
func binaryArray(elem uint32, s string) ([]byte, error) {
	elems, ok := splitArrayText(s)
	if !ok {
		return nil, fmt.Errorf("%q does not read as an array of one dimension", s)
	}
	if len(elems) == 0 {
		b := binary.BigEndian.AppendUint32(nil, 0)
		b = binary.BigEndian.AppendUint32(b, 0)
		return binary.BigEndian.AppendUint32(b, elem), nil
	}
	hasNull := uint32(0)
	for _, e := range elems {
		if e == nil {
			hasNull = 1
		}
	}
	b := binary.BigEndian.AppendUint32(nil, 1)
	b = binary.BigEndian.AppendUint32(b, hasNull)
	b = binary.BigEndian.AppendUint32(b, elem)
	b = binary.BigEndian.AppendUint32(b, uint32(len(elems)))
	b = binary.BigEndian.AppendUint32(b, 1)
	for _, e := range elems {
		if e == nil {
			b = binary.BigEndian.AppendUint32(b, math.MaxUint32) // -1
			continue
		}
		v, err := binaryScalar(elem, *e)
		if err != nil {
			return nil, err
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(v)))
		b = append(b, v...)
	}
	return b, nil
}

// splitArrayText reads the elements of an array's text as pgtext.Cell writes
// it (array_out's rules): between braces, separated by commas, each bare or
// between double quotes with a backslash before a quote or a backslash, and
// NULL bare for a missing one (a string that spells NULL is quoted). A nil
// element is NULL. ok is false for anything else, nested braces included.
func splitArrayText(s string) (elems []*string, ok bool) {
	inner, found := strings.CutPrefix(s, "{")
	if !found {
		return nil, false
	}
	if inner, found = strings.CutSuffix(inner, "}"); !found {
		return nil, false
	}
	if inner == "" {
		return []*string{}, true
	}
	for i := 0; ; {
		var e string
		if i < len(inner) && inner[i] == '"' {
			var b strings.Builder
			i++
			closed := false
			for i < len(inner) {
				c := inner[i]
				if c == '\\' {
					if i+1 >= len(inner) {
						return nil, false
					}
					b.WriteByte(inner[i+1])
					i += 2
					continue
				}
				if c == '"' {
					closed = true
					i++
					break
				}
				b.WriteByte(c)
				i++
			}
			if !closed {
				return nil, false
			}
			e = b.String()
			elems = append(elems, &e)
		} else {
			j := strings.IndexByte(inner[i:], ',')
			if j < 0 {
				j = len(inner) - i
			}
			e = inner[i : i+j]
			if e == "" || strings.ContainsAny(e, "{}\"\\") {
				return nil, false
			}
			i += j
			if e == "NULL" {
				elems = append(elems, nil)
			} else {
				elems = append(elems, &e)
			}
		}
		if i == len(inner) {
			return elems, true
		}
		if inner[i] != ',' {
			return nil, false
		}
		i++
	}
}

// typeAt is column i's type: text when types has none for it.
func typeAt(types []colType, i int) colType {
	if i < len(types) && types[i].oid != 0 {
		return types[i]
	}
	return textColumn
}

// formatAt is column i's result format of n: formats holds none (text), one
// for every column, or one each.
func formatAt(formats []int16, n, i int) int16 {
	switch len(formats) {
	case 1:
		return formats[0]
	case n:
		return formats[i]
	}
	return pgproto3.TextFormat
}
