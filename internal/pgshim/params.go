package pgshim

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Parameters of the extended query protocol (#2767). The copy runs one
// statement as text per sandbox job and the time-travel shapes are parsed
// from text, so neither has a server-side statement to bind arguments to. A
// prepared statement is therefore a template, as on the MySQL port
// (internal/shim/prepared.go): Parse cuts the text at its $n placeholders,
// Bind decodes each argument by its type and format and writes it into the
// template as a SQL literal, and the finished text runs exactly as if the
// client had sent it with the simple protocol.
//
// Where a placeholder is must be read the way the copy's engine reads the
// statement: a $1 inside a string, a quoted name or a comment is text, not a
// parameter, and an argument written there would end that string early. The
// engine's parser is PostgreSQL's own lexer (DuckDB's comes from libpg_query),
// so this reads strings, escape strings, dollar quotes, quoted names and
// nested comments by PostgreSQL's rules. Where the two could disagree the
// reading here errs on the side of "this is still a string or a comment": a
// placeholder left in place makes the statement fail, it never lets an
// argument out of its literal.

// maxParams is the protocol's own ceiling: a Bind message counts its
// parameters in 16 bits.
const maxParams = 65535

// maxPlaceholders is how many placeholders one statement may have, a
// parameter named twice counted twice: past it the template is refused.
const maxPlaceholders = maxParams

// template is a statement cut at its placeholders.
type template struct {
	// parts is the text around the placeholders: len(parts) == len(refs)+1.
	parts []string
	// refs is, for each placeholder in the order it appears, the 0-based
	// number of the parameter it names ($1 is 0). A parameter may appear
	// more than once, or not at all.
	refs []int
	// highest is the largest $n the text names, 0 for none.
	highest int
}

func isIdentByte(c byte) bool {
	return c == '_' || c == '$' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

func isIdentStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// splitParams cuts q at its $n placeholders. A $0, or a number past the
// protocol's ceiling, is refused as PostgreSQL refuses it (42P02).
func splitParams(q string) (template, *pgErr) {
	var t template
	n := len(q)
	start := 0
	lineEnd := func(i int) int {
		if j := strings.IndexByte(q[i:], '\n'); j >= 0 {
			return i + j + 1
		}
		return n
	}
	for i := 0; i < n; {
		c := q[i]
		switch {
		case c == '\'':
			// An escape string is E'...' with the E starting its own token;
			// in it a backslash escapes the next character, a quote too.
			escapes := i > 0 && (q[i-1] == 'e' || q[i-1] == 'E') && (i < 2 || !isIdentByte(q[i-2]))
			i = stringEnd(q, i+1, escapes)
		case c == '"':
			i = stringEnd(q, i+1, false) // a quoted name: "" is a quote in it
		case c == '-' && i+1 < n && q[i+1] == '-':
			i = lineEnd(i)
		case c == '/' && i+1 < n && q[i+1] == '*':
			i = commentEnd(q, i+2)
		case c == '$':
			if i > 0 && isIdentByte(q[i-1]) {
				// Part of a name: col$1 is one identifier.
				i++
				continue
			}
			if i+1 < n && q[i+1] >= '0' && q[i+1] <= '9' {
				j := i + 1
				for j < n && q[j] >= '0' && q[j] <= '9' {
					j++
				}
				num, err := strconv.Atoi(q[i+1 : j])
				if err != nil || num < 1 || num > maxParams {
					return template{}, &pgErr{"42P02", fmt.Sprintf("there is no parameter %s", q[i:j])}
				}
				if len(t.refs) >= maxPlaceholders {
					return template{}, &pgErr{"54000", fmt.Sprintf("a statement may name its parameters at most %d times", maxPlaceholders)}
				}
				t.parts = append(t.parts, q[start:i])
				t.refs = append(t.refs, num-1)
				t.highest = max(t.highest, num)
				start, i = j, j
				continue
			}
			if tag := dollarTag(q, i); tag != "" {
				end := strings.Index(q[i+len(tag):], tag)
				if end < 0 {
					i = n
				} else {
					i += len(tag) + end + len(tag)
				}
				continue
			}
			i++
		default:
			i++
		}
	}
	t.parts = append(t.parts, q[start:])
	return t, nil
}

// stringEnd returns the index just past the quote that closes the string or
// quoted name whose body starts at i (the opening quote is q[i-1]). A doubled
// quote is a quote inside it; with escapes a backslash takes the next byte
// along. An unclosed one runs to the end of the text.
func stringEnd(q string, i int, escapes bool) int {
	quote := q[i-1]
	for i < len(q) {
		switch {
		case escapes && q[i] == '\\':
			i += 2
		case q[i] == quote:
			if i+1 < len(q) && q[i+1] == quote {
				i += 2
				continue
			}
			return i + 1
		default:
			i++
		}
	}
	return len(q)
}

// commentEnd returns the index just past the */ that closes the block
// comment whose body starts at i. Block comments nest, as in PostgreSQL.
func commentEnd(q string, i int) int {
	depth := 1
	for i < len(q) {
		switch {
		case q[i] == '/' && i+1 < len(q) && q[i+1] == '*':
			depth++
			i += 2
		case q[i] == '*' && i+1 < len(q) && q[i+1] == '/':
			depth--
			i += 2
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return len(q)
}

// dollarTag returns the opening tag of a dollar-quoted string at q[i] ($$ or
// $name$), or "" when there is none.
func dollarTag(q string, i int) string {
	j := i + 1
	if j < len(q) && q[j] == '$' {
		return "$$"
	}
	if j >= len(q) || !isIdentStart(q[j]) {
		return ""
	}
	for j < len(q) && q[j] != '$' && isIdentByte(q[j]) {
		j++
	}
	if j < len(q) && q[j] == '$' {
		return q[i : j+1]
	}
	return ""
}

// PostgreSQL type OIDs the port reads arguments as.
const (
	oidUnspecified = 0
	oidBool        = 16
	oidBytea       = 17
	oidChar        = 18
	oidName        = 19
	oidInt8        = 20
	oidInt2        = 21
	oidInt4        = 23
	oidText        = 25
	oidOID         = 26
	oidJSON        = 114
	oidXML         = 142
	oidFloat4      = 700
	oidFloat8      = 701
	oidUnknown     = 705
	oidBpchar      = 1042
	oidVarchar     = 1043
	oidDate        = 1082
	oidTime        = 1083
	oidTimestamp   = 1114
	oidTimestamptz = 1184
	oidInterval    = 1186
	oidNumeric     = 1700
	oidUUID        = 2950
	oidJSONB       = 3802
)

// argKind is how an argument is written into the statement.
type argKind int

const (
	// argText is a string literal with no type of its own: the engine gives
	// it the type the statement needs there, as PostgreSQL does an argument
	// of unknown type.
	argText argKind = iota
	argInt
	argFloat
	argNumeric
	argBool
	argBytes
	// argTyped is a string literal cast to the type the client declared.
	argTyped
)

// arg is one decoded argument.
type arg struct {
	null bool
	kind argKind
	// text is the value as text: digits for argInt, the number for argFloat
	// and argNumeric, "true"/"false" for argBool, the string otherwise.
	text string
	// bytes is the value of an argBytes.
	bytes []byte
	// cast is the copy's type for argFloat and argTyped, and for a typed NULL.
	cast string
}

// typedCast is the copy's type for each declared type an argument is cast
// to, and that a NULL of that type is written as.
var typedCast = map[uint32]string{
	oidBool:        "BOOLEAN",
	oidBytea:       "BLOB",
	oidInt2:        "SMALLINT",
	oidInt4:        "INTEGER",
	oidInt8:        "BIGINT",
	oidOID:         "UINTEGER",
	oidFloat4:      "FLOAT",
	oidFloat8:      "DOUBLE",
	oidDate:        "DATE",
	oidTime:        "TIME",
	oidTimestamp:   "TIMESTAMP",
	oidTimestamptz: "TIMESTAMPTZ",
	oidInterval:    "INTERVAL",
	oidUUID:        "UUID",
}

// pgTypeName is PostgreSQL's name for a type in its error messages.
var pgTypeName = map[uint32]string{
	oidBool: "boolean", oidBytea: "bytea", oidInt2: "smallint", oidInt4: "integer", oidInt8: "bigint",
	oidOID: "oid", oidFloat4: "real", oidFloat8: "double precision", oidNumeric: "numeric",
}

// decodeArg reads argument i (0-based) as the client sent it: raw is nil for
// NULL, format is 0 for text and 1 for binary, oid the type the statement
// declares for it (0 when it declares none).
func decodeArg(i int, oid uint32, format int16, raw []byte) (arg, *pgErr) {
	if raw == nil {
		return arg{null: true, cast: typedCast[oid]}, nil
	}
	switch format {
	case 0:
		return decodeTextArg(oid, raw)
	case 1:
		return decodeBinaryArg(i, oid, raw)
	}
	return arg{}, &pgErr{"22023", fmt.Sprintf("unsupported format code: %d", format)}
}

// validText refuses text PostgreSQL would not take as UTF-8: a NUL byte, or
// bytes that are not UTF-8.
func validText(b []byte) *pgErr {
	if i := indexByte(b, 0); i >= 0 {
		return &pgErr{"22021", `invalid byte sequence for encoding "UTF8": 0x00`}
	}
	if !utf8.Valid(b) {
		return &pgErr{"22021", `invalid byte sequence for encoding "UTF8"`}
	}
	return nil
}

func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}

var numericText = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$`)

func decodeTextArg(oid uint32, raw []byte) (arg, *pgErr) {
	if perr := validText(raw); perr != nil {
		return arg{}, perr
	}
	s := string(raw)
	invalid := func() *pgErr {
		return &pgErr{"22P02", fmt.Sprintf("invalid input syntax for type %s: %q", pgTypeName[oid], s)}
	}
	switch oid {
	case oidInt2, oidInt4, oidInt8, oidOID:
		bits := map[uint32]int{oidInt2: 16, oidInt4: 32, oidInt8: 64, oidOID: 32}[oid]
		v := strings.TrimSpace(s)
		var text string
		var err error
		if oid == oidOID {
			var u uint64
			u, err = strconv.ParseUint(v, 10, bits)
			text = strconv.FormatUint(u, 10)
		} else {
			var n int64
			n, err = strconv.ParseInt(v, 10, bits)
			text = strconv.FormatInt(n, 10)
		}
		if err != nil {
			if ne, ok := err.(*strconv.NumError); ok && ne.Err == strconv.ErrRange {
				return arg{}, &pgErr{"22003", fmt.Sprintf("value %q is out of range for type %s", s, pgTypeName[oid])}
			}
			return arg{}, invalid()
		}
		return arg{kind: argInt, text: text}, nil
	case oidFloat4, oidFloat8:
		bits := 64
		if oid == oidFloat4 {
			bits = 32
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(s), bits)
		if err != nil {
			if ne, ok := err.(*strconv.NumError); ok && ne.Err == strconv.ErrRange {
				return arg{}, &pgErr{"22003", fmt.Sprintf("%q is out of range for type %s", s, pgTypeName[oid])}
			}
			return arg{}, invalid()
		}
		return arg{kind: argFloat, text: floatText(f, bits), cast: typedCast[oid]}, nil
	case oidNumeric:
		v := strings.TrimSpace(s)
		switch strings.ToLower(v) {
		case "nan":
			return arg{kind: argText, text: "NaN"}, nil
		case "infinity", "+infinity", "inf", "+inf":
			return arg{kind: argText, text: "Infinity"}, nil
		case "-infinity", "-inf":
			return arg{kind: argText, text: "-Infinity"}, nil
		}
		if !numericText.MatchString(v) {
			return arg{}, invalid()
		}
		return arg{kind: argNumeric, text: v}, nil
	case oidBool:
		b, ok := parseBool(s)
		if !ok {
			return arg{}, invalid()
		}
		return arg{kind: argBool, text: strconv.FormatBool(b)}, nil
	case oidBytea:
		b, ok := parseByteaText(s)
		if !ok {
			return arg{}, invalid()
		}
		return arg{kind: argBytes, bytes: b}, nil
	case oidDate, oidTime, oidTimestamp, oidTimestamptz, oidInterval, oidUUID:
		return arg{kind: argTyped, text: s, cast: typedCast[oid]}, nil
	}
	// text, varchar, char, name, json, jsonb, xml, unknown, a type declared
	// as 0, and every type this does not list: a string the engine reads
	// where it stands.
	return arg{kind: argText, text: s}, nil
}

// parseBool reads a boolean the way PostgreSQL's boolin does: any prefix of
// true, false, yes or no, on, off, 1 or 0, in any case, around blanks.
func parseBool(s string) (bool, bool) {
	v := strings.ToLower(strings.TrimSpace(s))
	switch v {
	case "":
		return false, false
	case "1", "on":
		return true, true
	case "0", "of", "off":
		return false, true
	}
	switch {
	case strings.HasPrefix("true", v), strings.HasPrefix("yes", v):
		return true, true
	case strings.HasPrefix("false", v), strings.HasPrefix("no", v):
		return false, true
	}
	return false, false
}

// parseByteaText reads bytea's text forms: hex (\x0102, blanks allowed
// between the pairs) or escape (\\ for a backslash, \ooo for a byte).
func parseByteaText(s string) ([]byte, bool) {
	if strings.HasPrefix(s, `\x`) {
		var out []byte
		h := s[2:]
		for i := 0; i < len(h); {
			if h[i] == ' ' || h[i] == '\t' || h[i] == '\n' || h[i] == '\r' {
				i++
				continue
			}
			if i+1 >= len(h) {
				return nil, false
			}
			b, err := hex.DecodeString(h[i : i+2])
			if err != nil {
				return nil, false
			}
			out = append(out, b[0])
			i += 2
		}
		if out == nil {
			out = []byte{}
		}
		return out, true
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); {
		if s[i] != '\\' {
			out = append(out, s[i])
			i++
			continue
		}
		if i+1 < len(s) && s[i+1] == '\\' {
			out = append(out, '\\')
			i += 2
			continue
		}
		if i+3 < len(s) && s[i+1] >= '0' && s[i+1] <= '3' && isOctal(s[i+2]) && isOctal(s[i+3]) {
			out = append(out, (s[i+1]-'0')<<6|(s[i+2]-'0')<<3|(s[i+3]-'0'))
			i += 4
			continue
		}
		return nil, false
	}
	return out, true
}

func isOctal(c byte) bool { return c >= '0' && c <= '7' }

func floatText(f float64, bits int) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	return strconv.FormatFloat(f, 'g', -1, bits)
}

// pgEpoch is where PostgreSQL's binary dates and timestamps count from.
var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

func decodeBinaryArg(i int, oid uint32, raw []byte) (arg, *pgErr) {
	bad := func() *pgErr {
		return &pgErr{"22P03", fmt.Sprintf("incorrect binary data format in bind parameter %d", i+1)}
	}
	be := binary.BigEndian
	switch oid {
	case oidInt2:
		if len(raw) != 2 {
			return arg{}, bad()
		}
		return arg{kind: argInt, text: strconv.FormatInt(int64(int16(be.Uint16(raw))), 10)}, nil
	case oidInt4:
		if len(raw) != 4 {
			return arg{}, bad()
		}
		return arg{kind: argInt, text: strconv.FormatInt(int64(int32(be.Uint32(raw))), 10)}, nil
	case oidInt8:
		if len(raw) != 8 {
			return arg{}, bad()
		}
		return arg{kind: argInt, text: strconv.FormatInt(int64(be.Uint64(raw)), 10)}, nil
	case oidOID:
		if len(raw) != 4 {
			return arg{}, bad()
		}
		return arg{kind: argInt, text: strconv.FormatUint(uint64(be.Uint32(raw)), 10)}, nil
	case oidFloat4:
		if len(raw) != 4 {
			return arg{}, bad()
		}
		return arg{kind: argFloat, text: floatText(float64(math.Float32frombits(be.Uint32(raw))), 32), cast: typedCast[oid]}, nil
	case oidFloat8:
		if len(raw) != 8 {
			return arg{}, bad()
		}
		return arg{kind: argFloat, text: floatText(math.Float64frombits(be.Uint64(raw)), 64), cast: typedCast[oid]}, nil
	case oidBool:
		if len(raw) != 1 {
			return arg{}, bad()
		}
		return arg{kind: argBool, text: strconv.FormatBool(raw[0] != 0)}, nil
	case oidBytea:
		return arg{kind: argBytes, bytes: append([]byte{}, raw...)}, nil
	case oidUnspecified, oidUnknown, oidText, oidVarchar, oidBpchar, oidName, oidChar, oidJSON, oidXML:
		// The binary form of a text type is its bytes.
		if perr := validText(raw); perr != nil {
			return arg{}, perr
		}
		return arg{kind: argText, text: string(raw)}, nil
	case oidJSONB:
		if len(raw) == 0 || raw[0] != 1 {
			return arg{}, bad()
		}
		if perr := validText(raw[1:]); perr != nil {
			return arg{}, perr
		}
		return arg{kind: argText, text: string(raw[1:])}, nil
	case oidUUID:
		if len(raw) != 16 {
			return arg{}, bad()
		}
		h := hex.EncodeToString(raw)
		return arg{kind: argTyped, text: h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:], cast: typedCast[oid]}, nil
	case oidDate:
		if len(raw) != 4 {
			return arg{}, bad()
		}
		days := int32(be.Uint32(raw))
		switch days {
		case math.MaxInt32:
			return arg{kind: argTyped, text: "infinity", cast: typedCast[oid]}, nil
		case math.MinInt32:
			return arg{kind: argTyped, text: "-infinity", cast: typedCast[oid]}, nil
		}
		d := pgEpoch.AddDate(0, 0, int(days))
		if d.Year() < 1 || d.Year() > 9999 {
			return arg{}, &pgErr{"22008", fmt.Sprintf("date out of range for this port in bind parameter %d (years 1 to 9999)", i+1)}
		}
		return arg{kind: argTyped, text: d.Format("2006-01-02"), cast: typedCast[oid]}, nil
	case oidTimestamp, oidTimestamptz:
		if len(raw) != 8 {
			return arg{}, bad()
		}
		us := int64(be.Uint64(raw))
		switch us {
		case math.MaxInt64:
			return arg{kind: argTyped, text: "infinity", cast: typedCast[oid]}, nil
		case math.MinInt64:
			return arg{kind: argTyped, text: "-infinity", cast: typedCast[oid]}, nil
		}
		ts := pgEpoch.Add(time.Duration(us/1e6) * time.Second).Add(time.Duration(us%1e6) * time.Microsecond)
		if ts.Year() < 1 || ts.Year() > 9999 {
			return arg{}, &pgErr{"22008", fmt.Sprintf("timestamp out of range for this port in bind parameter %d (years 1 to 9999)", i+1)}
		}
		text := ts.Format("2006-01-02 15:04:05.999999")
		if oid == oidTimestamptz {
			text += "+00"
		}
		return arg{kind: argTyped, text: text, cast: typedCast[oid]}, nil
	case oidTime:
		if len(raw) != 8 {
			return arg{}, bad()
		}
		us := int64(be.Uint64(raw))
		if us < 0 || us > 24*3600*1e6 {
			return arg{}, bad()
		}
		text := fmt.Sprintf("%02d:%02d:%02d", us/3600e6, us/60e6%60, us/1e6%60)
		if f := us % 1e6; f != 0 {
			text += strings.TrimRight(fmt.Sprintf(".%06d", f), "0")
		}
		return arg{kind: argTyped, text: text, cast: typedCast[oid]}, nil
	case oidNumeric:
		text, ok := numericBinaryText(raw)
		if !ok {
			return arg{}, bad()
		}
		if text == "NaN" || strings.HasSuffix(text, "Infinity") {
			return arg{kind: argText, text: text}, nil
		}
		return arg{kind: argNumeric, text: text}, nil
	}
	return arg{}, &pgErr{"0A000", fmt.Sprintf("parameter %d is in binary format with type OID %d, which this port does not read; send it in text format", i+1, oid)}
}

// numericBinaryText reads numeric's binary form: digit count, weight, sign
// and display scale, then base-10000 digits.
func numericBinaryText(b []byte) (string, bool) {
	if len(b) < 8 {
		return "", false
	}
	be := binary.BigEndian
	ndigits := int(int16(be.Uint16(b[0:2])))
	weight := int(int16(be.Uint16(b[2:4])))
	sign := be.Uint16(b[4:6])
	dscale := int(be.Uint16(b[6:8]))
	switch sign {
	case 0xC000:
		return "NaN", true
	case 0xD000:
		return "Infinity", true
	case 0xF000:
		return "-Infinity", true
	case 0x0000, 0x4000:
	default:
		return "", false
	}
	// The copy holds no number this long, and a client never needs one.
	if ndigits < 0 || len(b) != 8+2*ndigits || weight > 100 || weight < -100 || dscale > 400 {
		return "", false
	}
	digits := make([]int, ndigits)
	for i := range digits {
		d := int(be.Uint16(b[8+2*i:]))
		if d > 9999 {
			return "", false
		}
		digits[i] = d
	}
	digit := func(i int) int {
		if i >= 0 && i < ndigits {
			return digits[i]
		}
		return 0
	}
	var out strings.Builder
	if sign == 0x4000 {
		out.WriteByte('-')
	}
	if weight < 0 {
		out.WriteByte('0')
	} else {
		for i := 0; i <= weight; i++ {
			if i == 0 {
				out.WriteString(strconv.Itoa(digit(i)))
			} else {
				fmt.Fprintf(&out, "%04d", digit(i))
			}
		}
	}
	if dscale > 0 {
		var frac strings.Builder
		for i := weight + 1; frac.Len() < dscale; i++ {
			fmt.Fprintf(&frac, "%04d", digit(i))
		}
		out.WriteByte('.')
		out.WriteString(frac.String()[:dscale])
	}
	return out.String(), true
}

// reader is who reads the finished statement text.
type reader int

const (
	// readCopy is the copy's engine: standard strings, a quote doubled, a
	// backslash a character.
	readCopy reader = iota
	// readTimeTravel is the time-travel parser (shim.Parse): it reads a
	// literal MySQL's way (a backslash escapes) and takes no quote inside one.
	readTimeTravel
)

// literal writes a as a SQL literal for rd.
func (a arg) literal(rd reader) (string, *pgErr) {
	if rd == readTimeTravel {
		return a.timeTravelLiteral()
	}
	if a.null {
		if a.cast != "" {
			return "CAST(NULL AS " + a.cast + ")", nil
		}
		return "NULL", nil
	}
	switch a.kind {
	case argInt, argNumeric:
		if a.kind == argNumeric && strings.ContainsAny(a.text, "eE") {
			// Written as a number it would be a double on the copy, not an
			// exact numeric; as a string it takes the type it is compared to.
			return quoteStandard(a.text), nil
		}
		if strings.HasPrefix(a.text, "-") {
			// In parentheses: -9223372036854775808::bigint casts before it
			// negates, and the positive number does not fit.
			return "(" + a.text + ")", nil
		}
		return strings.TrimPrefix(a.text, "+"), nil
	case argBool:
		return strings.ToUpper(a.text), nil
	case argBytes:
		return "unhex('" + hex.EncodeToString(a.bytes) + "')", nil
	case argFloat, argTyped:
		return "CAST(" + quoteStandard(a.text) + " AS " + a.cast + ")", nil
	}
	return quoteStandard(a.text), nil
}

// timeTravelLiteral writes a for the time-travel parser, which takes a number
// or a quoted string with no quote in it.
func (a arg) timeTravelLiteral() (string, *pgErr) {
	if a.null {
		return "NULL", nil
	}
	text := a.text
	switch a.kind {
	case argInt:
		return text, nil
	case argBytes:
		text = `\x` + hex.EncodeToString(a.bytes)
	}
	if strings.ContainsRune(text, '\'') {
		return "", &pgErr{"22023", "a time-travel statement cannot take an argument with a quote (') in it; write the value in the statement's text"}
	}
	return "'" + strings.ReplaceAll(text, `\`, `\\`) + "'", nil
}

// quoteStandard writes s as a standard SQL string: the quote doubled, every
// other byte as it is.
func quoteStandard(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// fill writes args into the template for rd. Every placeholder takes its
// argument with a space on each side: a placeholder is a token of its own,
// and a literal written flush against its neighbours is not (5-$1 with -3
// would read 5--3, a comment).
func (t template) fill(args []arg, rd reader) (string, *pgErr) {
	lits := make([]string, len(args))
	for i, a := range args {
		lit, perr := a.literal(rd)
		if perr != nil {
			return "", perr
		}
		lits[i] = lit
	}
	// The length is counted before anything is written: a placeholder named
	// many times repeats its argument each time, and the finished text must
	// not grow past the longest statement a client may send.
	total := 0
	for i, part := range t.parts {
		total += len(part)
		if i < len(t.refs) {
			ref := t.refs[i]
			if ref >= len(lits) {
				return "", &pgErr{"08P01", fmt.Sprintf("there is no parameter $%d", ref+1)}
			}
			total += len(lits[ref]) + 2
		}
		if total > maxStatementMessage {
			return "", &pgErr{"54000", fmt.Sprintf(
				"the statement with its arguments written in is longer than %d MB, the most this port takes; send shorter arguments, or name each one fewer times",
				maxStatementMessage>>20)}
		}
	}
	var b strings.Builder
	b.Grow(total)
	for i, part := range t.parts {
		b.WriteString(part)
		if i == len(t.refs) {
			break
		}
		b.WriteByte(' ')
		b.WriteString(lits[t.refs[i]])
		b.WriteByte(' ')
	}
	return b.String(), nil
}
