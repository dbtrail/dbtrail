package sqlsandbox

import (
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// The cell text of three types the driver hands over in shapes of its own
// (#2614), each printed as the engine prints it (its ::VARCHAR), so a cell
// reads the same in the console, over MCP and on the MySQL port.

// intervalText prints an INTERVAL as DuckDB does: years, months and days
// as words ("1 year 2 months", "-1 day"), then the time of day as
// [-]HH:MM:SS with the fraction's trailing zeros dropped, and 00:00:00 for
// a zero interval. The driver's value is three counts (days, months,
// microseconds), which fmt prints as {0 3 18000000000}.
func intervalText(iv duckdb.Interval) string {
	var parts []string
	unit := func(n int64, one, many string) {
		if n == 0 {
			return
		}
		word := many
		if n == 1 || n == -1 {
			word = one
		}
		parts = append(parts, strconv.FormatInt(n, 10)+" "+word)
	}
	months := int64(iv.Months)
	unit(months/12, "year", "years")
	unit(months%12, "month", "months")
	unit(int64(iv.Days), "day", "days")
	if iv.Micros != 0 || len(parts) == 0 {
		parts = append(parts, clockText(iv.Micros))
	}
	return strings.Join(parts, " ")
}

// clockText prints a signed count of microseconds as [-]HH:MM:SS[.ffffff],
// hours not wrapped at 24 (an INTERVAL of 25 hours is 25:00:00).
func clockText(micros int64) string {
	var b strings.Builder
	u := uint64(micros)
	if micros < 0 {
		b.WriteByte('-')
		u = uint64(-(micros + 1)) + 1 // also right for the smallest int64
	}
	const perSecond = 1_000_000
	secs, frac := u/perSecond, u%perSecond
	two := func(n uint64) {
		if n < 10 {
			b.WriteByte('0')
		}
		b.WriteString(strconv.FormatUint(n, 10))
	}
	two(secs / 3600)
	b.WriteByte(':')
	two(secs / 60 % 60)
	b.WriteByte(':')
	two(secs % 60)
	if frac != 0 {
		f := strconv.FormatUint(frac+perSecond, 10)[1:] // six digits, zero padded
		b.WriteByte('.')
		b.WriteString(strings.TrimRight(f, "0"))
	}
	return b.String()
}

// timeTZText prints a TIMETZ as DuckDB does: the wall clock it holds, then
// its offset as +HH, +HH:MM or +HH:MM:SS. The driver hands it over as a
// time.Time on 1 January of year 1 in a fixed zone of that offset; printed
// in UTC, as other times are, it read as another wall-clock time.
//
// 24:00:00, which DuckDB allows, reaches Go as hour 24 of 1 January, which
// time.Date normalizes to midnight of 2 January: the day tells it apart. A
// TIME of 24:00:00 cannot be told from 00:00:00, since the driver builds it
// from the hour of a Unix time, which has already wrapped.
func timeTZText(t time.Time) string {
	_, off := t.Zone()
	sign := byte('+')
	if off < 0 {
		sign, off = '-', -off
	}
	var b strings.Builder
	if t.Day() == 2 {
		b.WriteString("24:00:00")
	} else {
		b.WriteString(t.Format("15:04:05.999999"))
	}
	b.WriteByte(sign)
	two := func(n int) {
		if n < 10 {
			b.WriteByte('0')
		}
		b.WriteString(strconv.Itoa(n))
	}
	two(off / 3600)
	if m, s := off/60%60, off%60; m != 0 || s != 0 {
		b.WriteByte(':')
		two(m)
		if s != 0 {
			b.WriteByte(':')
			two(s)
		}
	}
	return b.String()
}

// uuidText prints a UUID's 16 bytes in the canonical 8-4-4-4-12 form. The
// driver hands a UUID over as those bytes, which printed as text when they
// happened to be valid UTF-8 and as 0x hex otherwise.
func uuidText(b []byte) string {
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// The element types of a nested column, read out of the type name DuckDB
// gives it: DATE[] and DATE[2] (a LIST and an ARRAY of DATE),
// STRUCT("d" DATE, "x, y" DECIMAL(10,2)), UNION("d" DATE), MAP(VARCHAR,
// DATE). Names are double quoted with "" for a quote, and a type can hold
// commas, parentheses and single-quoted text (DECIMAL(10,2), ENUM('a,b')),
// so the name is split only at the top level. A name that cannot be read
// gives no element types, and the elements render as they did before: by
// their Go value alone.

// listElemType is the element type of a LIST or ARRAY type, or "".
func listElemType(typ string) string {
	if !strings.HasSuffix(typ, "]") {
		return ""
	}
	open := -1
	if !scanTopLevel(typ, func(i int, c byte) {
		if c == '[' {
			open = i
		}
	}) || open <= 0 {
		return ""
	}
	for _, c := range typ[open+1 : len(typ)-1] {
		if c < '0' || c > '9' {
			return ""
		}
	}
	return typ[:open]
}

// structFieldTypes maps the fields of a STRUCT or UNION type to their types,
// or is nil.
func structFieldTypes(typ string) map[string]string {
	var inner string
	switch {
	case strings.HasPrefix(typ, "STRUCT(") && strings.HasSuffix(typ, ")"):
		inner = typ[len("STRUCT(") : len(typ)-1]
	case strings.HasPrefix(typ, "UNION(") && strings.HasSuffix(typ, ")"):
		inner = typ[len("UNION(") : len(typ)-1]
	default:
		return nil
	}
	parts := splitTopLevel(inner)
	if len(parts) == 0 {
		return nil
	}
	fields := make(map[string]string, len(parts))
	for _, p := range parts {
		name, rest, ok := cutFieldName(strings.TrimSpace(p))
		if !ok {
			return nil
		}
		fields[name] = rest
	}
	return fields
}

// mapTypes is the key and value type of a MAP type, or "", "".
func mapTypes(typ string) (key, val string) {
	if !strings.HasPrefix(typ, "MAP(") || !strings.HasSuffix(typ, ")") {
		return "", ""
	}
	parts := splitTopLevel(typ[len("MAP(") : len(typ)-1])
	if len(parts) != 2 {
		return "", ""
	}
	key, val = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	if key == "" || val == "" {
		return "", ""
	}
	return key, val
}

// cutFieldName splits `"x, y" DECIMAL(10,2)` (or `a DATE`) into the name
// and the type.
func cutFieldName(p string) (name, typ string, ok bool) {
	if strings.HasPrefix(p, `"`) {
		var b strings.Builder
		for i := 1; i < len(p); i++ {
			if p[i] != '"' {
				b.WriteByte(p[i])
				continue
			}
			if i+1 < len(p) && p[i+1] == '"' {
				b.WriteByte('"')
				i++
				continue
			}
			typ = strings.TrimSpace(p[i+1:])
			return b.String(), typ, typ != ""
		}
		return "", "", false
	}
	name, typ, found := strings.Cut(p, " ")
	typ = strings.TrimSpace(typ)
	return name, typ, found && name != "" && typ != ""
}

// splitTopLevel splits s at the commas outside parentheses, brackets and
// quotes, or is nil when s does not balance.
func splitTopLevel(s string) []string {
	var parts []string
	start := 0
	if !scanTopLevel(s, func(i int, c byte) {
		if c == ',' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}) {
		return nil
	}
	return append(parts, s[start:])
}

// scanTopLevel calls at for each byte of s outside quotes at depth 0 (an
// opening bracket is reported at the depth it opens from), and reports
// whether s balances: no closing bracket without its opening one, every
// bracket and quote closed.
func scanTopLevel(s string, at func(i int, c byte)) bool {
	depth := 0
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == quote {
				if i+1 < len(s) && s[i+1] == quote {
					i++ // a doubled quote stays inside
					continue
				}
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
		case '(', '[':
			if depth == 0 {
				at(i, c)
			}
			depth++
		case ')', ']':
			depth--
			if depth < 0 {
				return false
			}
		default:
			if depth == 0 {
				at(i, c)
			}
		}
	}
	return depth == 0 && quote == 0
}
