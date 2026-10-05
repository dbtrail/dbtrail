package shim

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/readrouter"
)

// Prepared statements (#2036). The copy and the time-travel shapes have no
// server-side statement to keep (the copy runs one statement per sandbox
// child, the time-travel shapes are parsed from text), so for them a
// prepared statement is a template: PREPARE counts its placeholders, EXECUTE
// writes the arguments into it as SQL literals and runs the result through
// HandleQuery, exactly as if the client had sent that text, and the answer
// goes back in the binary row encoding the client asked for.
//
// With read routing bound, a statement that is not a time-travel shape is
// MySQL's: it is prepared on the source and executed there with the source
// binding the arguments, never rebuilt from text (a write whose argument is
// escaped under the wrong sql_mode would store another value). Only an
// execution the routing ladder sends to the copy is written out as text,
// and that text goes to the copy alone.

// maxPreparedParams is the protocol's own ceiling (a 16-bit count).
const maxPreparedParams = 65535

// preparedStmt is the context PREPARE hands back and EXECUTE receives.
type preparedStmt struct {
	// parts are the template's text around its placeholders: len(parts) is
	// the placeholder count plus one.
	parts []string
	// mysqlEscapes selects how a string argument is quoted. The time-travel
	// parser reads literals the way MySQL does (a backslash escapes), the
	// copy the way standard SQL does (a backslash is a character), so the
	// same value needs a different spelling for each reader.
	mysqlEscapes bool
	// up is the statement on the source, under read routing; nil for a
	// template. query is its text, which the routing ladder reads.
	up    readrouter.Stmt
	query string
	// copyRefusal, when set, is why the template cannot be written for the
	// copy (readrouter.ForCopy refused it): every execution is MySQL's.
	copyRefusal string
	// db is the database selected when the statement was prepared: the
	// source resolves the statement's unqualified names in it for good.
	db string
}

// stmtFields hands the session the source's own parameter and column
// definitions, to relay in the PREPARE answer.
func (st *preparedStmt) stmtFields() (params, columns [][]byte) {
	if st.up == nil {
		return nil, nil
	}
	return st.up.ParamFields(), st.up.ColumnFields()
}

// readByTimeTravel reports which reader a statement text is headed for: the
// time-travel parser, or the copy. It asks Parse itself, the function
// HandleQuery dispatches on, so the answer cannot drift from the dispatch (a
// looser test would call a copy statement that merely mentions "as of" inside
// a string a time-travel one, and quote its arguments for the wrong reader).
func readByTimeTravel(text, db string) bool {
	_, err := Parse(text, db)
	return !errors.Is(err, ErrNotTimeTravel)
}

// splitPlaceholders cuts a statement at its `?` placeholders: a `?` inside a
// quoted string, a backtick identifier or a comment is text, not a
// parameter. Where a string or a comment ends depends on who reads the
// statement. mysqlStyle is the time-travel parser's reading: a backslash
// escapes inside a string, `--` opens a comment only before whitespace, `#`
// opens one. Otherwise it is the copy's: a backslash is a character, `--`
// always opens a comment, `#` never does.
func splitPlaceholders(stmt string, mysqlStyle bool) []string {
	var parts []string
	start := 0
	n := len(stmt)
	lineComment := func(i int) int {
		j := strings.IndexByte(stmt[i:], '\n')
		if j < 0 {
			return n
		}
		return i + j + 1
	}
	for i := 0; i < n; {
		c := stmt[i]
		switch {
		case c == '?':
			parts = append(parts, stmt[start:i])
			start = i + 1
			i++
		case c == '\'' || c == '"':
			i++
			for i < n {
				if mysqlStyle && stmt[i] == '\\' {
					i += 2
					continue
				}
				if stmt[i] == c {
					if i+1 < n && stmt[i+1] == c {
						i += 2
						continue
					}
					break
				}
				i++
			}
			i++
		case c == '`':
			j := strings.IndexByte(stmt[i+1:], '`')
			if j < 0 {
				i = n
			} else {
				i += j + 2
			}
		case c == '/' && i+1 < n && stmt[i+1] == '*':
			j := strings.Index(stmt[i+2:], "*/")
			if j < 0 {
				i = n
			} else {
				i += j + 4
			}
		case c == '#' && mysqlStyle:
			i = lineComment(i)
		case c == '-' && i+1 < n && stmt[i+1] == '-' &&
			(!mysqlStyle || i+2 >= n || stmt[i+2] == ' ' || stmt[i+2] == '\t' || stmt[i+2] == '\n'):
			i = lineComment(i)
		default:
			i++
		}
	}
	return append(parts, stmt[start:])
}

// probeArgument stands in for every placeholder when PREPARE asks which
// reader the statement is headed for; the arguments do not exist yet.
const probeArgument = "'x'"

// HandleStmtPrepare answers COM_STMT_PREPARE. For a template: the
// placeholder count, and no column definitions (the columns are only known
// once the statement runs; the EXECUTE response carries them, which is where
// drivers read them). For a statement prepared on the source: the source's
// own counts and definitions.
func (h *Handler) HandleStmtPrepare(query string) (params int, columns int, context any, err error) {
	h.mu.Lock()
	db := h.db
	h.mu.Unlock()
	// Which reader decides where a string ends, so the split depends on the
	// answer: read the statement the MySQL way first (the time-travel
	// parser's), and if it turns out to be the copy's, split it again the
	// copy's way.
	parts := splitPlaceholders(query, true)
	mysqlEscapes := readByTimeTravel(strings.Join(parts, probeArgument), db)
	if !mysqlEscapes && h.router != nil && h.freeSQL != nil {
		return h.prepareRouted(query, parts)
	}
	if !mysqlEscapes {
		parts = splitPlaceholders(query, false)
	}
	if len(parts)-1 > maxPreparedParams {
		return 0, 0, nil, mysql.NewError(mysql.ER_PS_MANY_PARAM, "Prepared statement contains too many placeholders")
	}
	return len(parts) - 1, 0, &preparedStmt{parts: parts, mysqlEscapes: mysqlEscapes}, nil
}

// HandleStmtExecute answers COM_STMT_EXECUTE. The arguments must come from a
// Session: go-mysql's own command loop hands over nil for every argument of
// a re-execution whose types the client did not re-send, which would run
// here as `= NULL`.
//
// The template with its
// arguments written in, run as text, its rows re-encoded for the binary
// protocol. The statement runs buffered: a resultset streamed to the
// connection would be text rows, which this client cannot read.
func (h *Handler) HandleStmtExecute(context any, _ string, args []any) (*mysql.Result, error) {
	st, ok := context.(*preparedStmt)
	if !ok {
		return nil, mysql.NewError(mysql.ER_UNKNOWN_STMT_HANDLER, "unknown prepared statement")
	}
	if st.up != nil {
		return h.executeRouted(st, args)
	}
	text, err := st.interpolate(args)
	if err != nil {
		return nil, mysql.NewError(mysql.ER_WRONG_ARGUMENTS, err.Error())
	}
	h.mu.Lock()
	db := h.db
	h.mu.Unlock()
	if readByTimeTravel(text, db) != st.mysqlEscapes {
		// The arguments were quoted for one reader and the finished text
		// would be read by the other (a quote inside the AS OF argument
		// stops the statement from being a time-travel shape). Never run
		// text under the reader it was not escaped for.
		return nil, mysql.NewError(mysql.ER_WRONG_ARGUMENTS, "an argument changes how this prepared statement is read; send the statement as text")
	}
	h.buffered = true
	defer func() { h.buffered = false }()
	res, err := h.HandleQuery(text)
	if err != nil || res == nil || res.Resultset == nil {
		return res, err
	}
	bin, err := binaryResultset(res.Resultset)
	if err != nil {
		return nil, err
	}
	out := *res
	out.Resultset = bin
	return &out, nil
}

// HandleStmtClose answers COM_STMT_CLOSE: a template holds nothing; a routed
// statement is freed on the source.
func (h *Handler) HandleStmtClose(context any) error {
	if st, ok := context.(*preparedStmt); ok && st.up != nil {
		st.up.Close()
	}
	return nil
}

// prepareRouted prepares a statement that is MySQL's on the source. parts is
// the statement split the MySQL way, kept for the executions the routing
// ladder sends to the copy.
func (h *Handler) prepareRouted(query string, parts []string) (int, int, any, error) {
	// A statement being prepared replaces the refused one as "the last
	// statement", except SHOW WARNINGS itself: preparing the statement that
	// reads the refusal must not erase it.
	if !showWarningsRE.MatchString(query) {
		h.clearRefusal()
	}
	if err := h.readOnlyRefusal(query); err != nil {
		return 0, 0, nil, err
	}
	ctx, cancel := h.queryContext()
	defer cancel()
	up, err := h.router.Prepare(ctx, query)
	if err != nil {
		if readrouter.IsLost(err) {
			h.observeRoute(RouteMySQL, RouteReasonUpstreamLost)
			h.routeWarn("lost", "read routing: the connection to the source was lost; this client connection answers 2006 until it reconnects", err)
		}
		return 0, 0, nil, err
	}
	h.mu.Lock()
	db := h.db
	h.mu.Unlock()
	st := &preparedStmt{up: up, query: query, db: db}
	if len(parts)-1 == up.Params() {
		st.parts, st.copyRefusal = copyParts(parts)
	}
	// Otherwise the source counted other placeholders than this port did:
	// the statement still runs there, and is never written out for the copy.
	return up.Params(), up.Columns(), st, nil
}

// copyParts is a routed template as the copy is sent it: each piece of text
// around the placeholders with its backtick-quoted names in double quotes
// (readrouter.ForCopy). A placeholder is never inside a string, a name or a
// comment, so every piece is whole and can be rewritten on its own; the
// arguments are written in afterwards and are never rescanned, since they
// are spelled the copy's way (a backslash is a character there) and this
// scanner reads MySQL's. With a refusal the pieces come back as written.
func copyParts(parts []string) (out []string, refusal string) {
	out = make([]string, len(parts))
	for i, part := range parts {
		text, why := readrouter.ForCopy(part)
		if why != "" {
			return parts, why
		}
		out[i] = text
	}
	return out, ""
}

// executeRouted runs one execution of a statement prepared on the source
// through the routing ladder (route): the source binds the arguments on
// every rung but the copy's.
func (h *Handler) executeRouted(st *preparedStmt, args []any) (*mysql.Result, error) {
	ctx, cancel := h.queryContext()
	defer cancel()
	h.mu.Lock()
	currentDB := h.db
	forwarded, refusal := h.routeLastForwarded, h.routeRefusal
	h.mu.Unlock()
	if showWarningsRE.MatchString(st.query) && (!forwarded || refusal != "") {
		// After a copy-served statement the warnings are the port's own,
		// as for the text statement; the source's would be its EXPLAIN's.
		// After a read-only refusal they are that refusal.
		res, err := h.showWarnings()
		if refusal != "" {
			res, err = refusalDiagnostics(refusal)
		}
		if err != nil || res == nil || res.Resultset == nil {
			return res, err
		}
		bin, err := binaryResultset(res.Resultset)
		if err != nil {
			return nil, err
		}
		out := *res
		out.Resultset = bin
		return &out, nil
	}
	h.setWarnings(nil)
	return h.route(ctx, st.query, routeOps{
		forward: func(reason RouteReason, detail string) (*mysql.Result, error) {
			return h.forwardWith(reason, detail, true, func(sink readrouter.RowSink) (*mysql.Result, error) {
				return st.up.Execute(ctx, args, sink)
			})
		},
		decide: func() (readrouter.Decision, error) { return st.up.Decide(ctx, args) },
		runCopy: func(reason string, unchangedWithin time.Duration) (*mysql.Result, error) {
			return h.runPreparedOnCopy(st, args, currentDB, reason, unchangedWithin)
		},
		extraVeto: func() string {
			switch {
			case st.parts == nil:
				return "the source counts other placeholders than the port"
			case st.copyRefusal != "":
				return st.copyRefusal
			case currentDB != st.db:
				// The source runs the statement in the database it was
				// prepared in; the copy would resolve its names in the
				// current one.
				return "the database changed since the statement was prepared"
			}
			return copyUnsafeArgument(args)
		},
	})
}

// plainInteger is a number MySQL and the copy both read as that integer.
var plainInteger = regexp.MustCompile(`^[+-]?\d+$`)

// copyUnsafeArgument names an argument the copy would compare differently
// than MySQL, or "" when there is none. A string that spells a number but
// not a plain integer is one: against an integer column MySQL compares it
// as a double ('12.7' matches nothing) and the copy casts it to the column's
// type (and finds 13).
func copyUnsafeArgument(args []any) string {
	for i, a := range args {
		tb, ok := a.(mysql.TypedBytes)
		if !ok {
			continue
		}
		switch tb.Type {
		case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL,
			mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE, mysql.MYSQL_TYPE_DATETIME,
			mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIME:
			continue
		}
		if readrouter.TwoDigitYear(string(tb.Bytes)) {
			return fmt.Sprintf("argument %d is a string that starts with a two-digit year (year 2026 or 1970 on MySQL; year 26 or 70 on the copy)", i+1)
		}
		text := bytes.TrimSpace(tb.Bytes)
		if decimalText.Match(text) && !plainInteger.Match(text) {
			return fmt.Sprintf("argument %d is a string that spells a non-integer number (compared as a number on MySQL, cast to the column's type on the copy)", i+1)
		}
	}
	return ""
}

// runPreparedOnCopy writes the arguments into the statement the copy's way
// (standard SQL: a quote doubled, a backslash a character) and runs it there.
// The template's names are already in double quotes (copyParts).
// The text never goes anywhere else: MySQL's rungs execute the statement
// prepared on the source.
func (h *Handler) runPreparedOnCopy(st *preparedStmt, args []any, db, reason string, unchangedWithin time.Duration) (*mysql.Result, error) {
	text, err := st.interpolate(args)
	if err != nil {
		return nil, err
	}
	res, err := h.runFreeSQLRouted(db, text, readrouter.ShapeOf(st.query), reason, unchangedWithin)
	if err != nil {
		return nil, err
	}
	bin, err := binaryResultset(res.Resultset)
	if err != nil {
		return nil, err
	}
	out := *res
	out.Resultset = bin
	return &out, nil
}

// interpolate writes the arguments into the template.
func (st *preparedStmt) interpolate(args []any) (string, error) {
	if len(args) != len(st.parts)-1 {
		return "", fmt.Errorf("prepared statement takes %d arguments, got %d", len(st.parts)-1, len(args))
	}
	var b strings.Builder
	for i, part := range st.parts {
		b.WriteString(part)
		if i == len(args) {
			break
		}
		lit, err := sqlLiteral(args[i], st.mysqlEscapes)
		if err != nil {
			return "", fmt.Errorf("argument %d: %w", i+1, err)
		}
		// A space on each side: a placeholder is a token of its own, and a
		// literal written flush against its neighbours is not. `LIMIT?`
		// would become `LIMIT5` (an alias), `?e1` the number 5e1, and
		// `5-?` with -3 the comment `5--3`.
		b.WriteByte(' ')
		b.WriteString(lit)
		b.WriteByte(' ')
	}
	return b.String(), nil
}

var decimalText = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$`)

// sqlLiteral renders one bound argument (the Go values go-mysql's server
// decodes COM_STMT_EXECUTE into) as a SQL literal.
func sqlLiteral(arg any, mysqlEscapes bool) (string, error) {
	switch v := arg.(type) {
	case nil:
		return "NULL", nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float32:
		// Widened first, as MySQL widens a FLOAT argument: 0.1 bound as a
		// float is 0.10000000149011612, not the decimal 0.1.
		return floatLiteral(float64(v), 64)
	case float64:
		return floatLiteral(v, 64)
	case mysql.TypedBytes:
		return typedBytesLiteral(v, mysqlEscapes)
	case []byte:
		return quoteBytes(v, mysqlEscapes), nil
	case string:
		return quoteBytes([]byte(v), mysqlEscapes), nil
	}
	return "", fmt.Errorf("unsupported argument type %T", arg)
}

func floatLiteral(f float64, bits int) (string, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "", fmt.Errorf("%v is not a SQL number", f)
	}
	return strconv.FormatFloat(f, 'g', -1, bits), nil
}

func typedBytesLiteral(v mysql.TypedBytes, mysqlEscapes bool) (string, error) {
	switch v.Type {
	case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL:
		if decimalText.Match(v.Bytes) {
			return string(v.Bytes), nil
		}
		return quoteBytes(v.Bytes, mysqlEscapes), nil
	case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP:
		s, err := decodeBinaryDateTime(v.Bytes)
		if err != nil {
			return "", err
		}
		return "'" + s + "'", nil
	case mysql.MYSQL_TYPE_TIME:
		s, err := decodeBinaryTime(v.Bytes)
		if err != nil {
			return "", err
		}
		return "'" + s + "'", nil
	}
	return quoteBytes(v.Bytes, mysqlEscapes), nil
}

// quoteBytes renders text as a quoted string. The quote is doubled for the
// copy and backslash-escaped, with the backslash itself, for the time-travel
// parser. Bytes that are not valid UTF-8 cannot travel inside a quoted
// string: for the copy they are spelled unhex('…'), which is a BLOB there
// (its X'…' is a text literal, not bytes), and for the time-travel parser
// X'…', which it refuses with a syntax error rather than match as text.
func quoteBytes(b []byte, mysqlEscapes bool) string {
	if !utf8.Valid(b) {
		if mysqlEscapes {
			return "X'" + hex.EncodeToString(b) + "'"
		}
		return "unhex('" + hex.EncodeToString(b) + "')"
	}
	var out strings.Builder
	out.Grow(len(b) + 2)
	out.WriteByte('\'')
	for _, c := range b {
		switch {
		case c == '\'' && mysqlEscapes:
			out.WriteString(`\'`)
		case c == '\'':
			out.WriteString(`''`)
		case c == '\\' && mysqlEscapes:
			out.WriteString(`\\`)
		default:
			out.WriteByte(c)
		}
	}
	out.WriteByte('\'')
	return out.String()
}

// decodeBinaryDateTime reads the binary protocol's DATE/DATETIME/TIMESTAMP
// value (0, 4, 7 or 11 bytes after the length byte go-mysql already
// consumed) into MySQL's text spelling.
func decodeBinaryDateTime(b []byte) (string, error) {
	switch len(b) {
	case 0:
		return "0000-00-00 00:00:00", nil
	case 4, 7, 11:
	default:
		return "", fmt.Errorf("malformed binary date (%d bytes)", len(b))
	}
	s := fmt.Sprintf("%04d-%02d-%02d", binary.LittleEndian.Uint16(b[0:2]), b[2], b[3])
	if len(b) >= 7 {
		s += fmt.Sprintf(" %02d:%02d:%02d", b[4], b[5], b[6])
	}
	if len(b) == 11 {
		s += fmt.Sprintf(".%06d", binary.LittleEndian.Uint32(b[7:11]))
	}
	return s, nil
}

// decodeBinaryTime reads the binary protocol's TIME value (0, 8 or 12 bytes).
func decodeBinaryTime(b []byte) (string, error) {
	switch len(b) {
	case 0:
		return "00:00:00", nil
	case 8, 12:
	default:
		return "", fmt.Errorf("malformed binary time (%d bytes)", len(b))
	}
	sign := ""
	if b[0] != 0 {
		sign = "-"
	}
	hours := uint64(binary.LittleEndian.Uint32(b[1:5]))*24 + uint64(b[5])
	s := fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, b[6], b[7])
	if len(b) == 12 {
		s += fmt.Sprintf(".%06d", binary.LittleEndian.Uint32(b[8:12]))
	}
	return s, nil
}

// binaryResultset re-encodes a text resultset (what every path behind
// HandleQuery builds) for the binary protocol COM_STMT_EXECUTE answers in.
// Each column is encoded by its declared type; a column where some cell does
// not read as that type (a column typed from its first row, a date the
// source spells unusually) is sent as text instead, with its definition
// changed to match, so the client never decodes bytes under the wrong type.
func binaryResultset(rs *mysql.Resultset) (*mysql.Resultset, error) {
	nf := len(rs.Fields)
	fields := make([]*mysql.Field, nf)
	for i, f := range rs.Fields {
		cp := *f
		fields[i] = &cp
	}
	texts, err := textCells(rs, nf)
	if err != nil {
		return nil, err
	}
	// A column keeps its type only if every cell encodes under it.
	for c, f := range fields {
		if isLenEncType(f.Type) {
			continue
		}
		for r := range texts {
			if texts[r][c] == nil {
				continue
			}
			if _, ok := encodeBinaryCell(f, texts[r][c]); !ok {
				f.Type = mysql.MYSQL_TYPE_VAR_STRING
				f.Charset = 255
				f.Flag &^= mysql.BINARY_FLAG | mysql.UNSIGNED_FLAG
				break
			}
		}
	}
	bitmapLen := (nf + 7 + 2) >> 3
	rows := make([]mysql.RowData, len(texts))
	for r, cells := range texts {
		data := make([]byte, 1+bitmapLen, 1+bitmapLen+16*nf)
		for c, cell := range cells {
			if cell == nil {
				data[1+(c+2)/8] |= 1 << (uint(c+2) % 8)
				continue
			}
			enc, _ := encodeBinaryCell(fields[c], cell)
			data = append(data, enc...)
		}
		rows[r] = data
	}
	out := mysql.NewResultset(nf)
	out.Fields = fields
	out.FieldNames = rs.FieldNames
	out.RowDatas = rows
	return out, nil
}

// textCells reads every cell of a text resultset as the bytes the text
// protocol carries, nil for NULL. The rows live in RowDatas (what
// BuildSimpleTextResultset and a buffered upstream read both fill); a
// resultset that only has parsed Values is read from those.
func textCells(rs *mysql.Resultset, nf int) ([][][]byte, error) {
	if len(rs.RowDatas) == 0 && len(rs.Values) > 0 {
		texts := make([][][]byte, len(rs.Values))
		for r, row := range rs.Values {
			if len(row) != nf {
				return nil, fmt.Errorf("row %d has %d cells for %d columns", r, len(row), nf)
			}
			cells := make([][]byte, nf)
			for c := range row {
				cells[c] = fieldValueText(&row[c])
			}
			texts[r] = cells
		}
		return texts, nil
	}
	texts := make([][][]byte, len(rs.RowDatas))
	for r, data := range rs.RowDatas {
		cells := make([][]byte, nf)
		pos := 0
		for c := 0; c < nf; c++ {
			if pos >= len(data) {
				return nil, fmt.Errorf("row %d ends after %d of %d columns", r, c, nf)
			}
			v, isNull, n, err := mysql.LengthEncodedString(data[pos:])
			if err != nil {
				return nil, fmt.Errorf("row %d column %d: %w", r, c, err)
			}
			pos += n
			if isNull {
				continue
			}
			if v == nil {
				v = []byte{}
			}
			cells[c] = v
		}
		texts[r] = cells
	}
	return texts, nil
}

// fieldValueText is a cell as the text protocol would send it; nil is NULL.
func fieldValueText(fv *mysql.FieldValue) []byte {
	switch fv.Type {
	case mysql.FieldValueTypeNull:
		return nil
	case mysql.FieldValueTypeUnsigned:
		return strconv.AppendUint(nil, fv.AsUint64(), 10)
	case mysql.FieldValueTypeSigned:
		return strconv.AppendInt(nil, fv.AsInt64(), 10)
	case mysql.FieldValueTypeFloat:
		return strconv.AppendFloat(nil, fv.AsFloat64(), 'g', -1, 64)
	}
	s := fv.AsString()
	if s == nil {
		return []byte{}
	}
	return s
}

// encodeBinaryValue encodes one non-NULL cell as go-mysql's client hands it
// over after reading a row from the source (integers, floats, bytes; a
// temporal value as its text), under the column's type.
func encodeBinaryValue(f *mysql.Field, v any) ([]byte, error) {
	var text []byte
	switch x := v.(type) {
	case []byte:
		text = x
	case string:
		text = []byte(x)
	case int64:
		text = strconv.AppendInt(nil, x, 10)
	case uint64:
		text = strconv.AppendUint(nil, x, 10)
	case float64:
		text = strconv.AppendFloat(nil, x, 'g', -1, 64)
	default:
		return nil, fmt.Errorf("unsupported cell type %T", v)
	}
	enc, ok := encodeBinaryCell(f, text)
	if !ok {
		return nil, fmt.Errorf("value %q does not encode as column type %d", text, f.Type)
	}
	return enc, nil
}

// isLenEncType: the types the binary protocol carries as a length-encoded
// string, the same bytes the text protocol uses.
func isLenEncType(t uint8) bool {
	switch t {
	case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_YEAR, mysql.MYSQL_TYPE_INT24,
		mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_FLOAT, mysql.MYSQL_TYPE_DOUBLE,
		mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP,
		mysql.MYSQL_TYPE_TIME:
		return false
	}
	return true
}

// encodeBinaryCell encodes one non-NULL cell under the field's type; ok is
// false when the text does not read as that type.
func encodeBinaryCell(f *mysql.Field, text []byte) (enc []byte, ok bool) {
	if isLenEncType(f.Type) {
		return mysql.PutLengthEncodedString(text), true
	}
	s := string(text)
	switch f.Type {
	case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_YEAR, mysql.MYSQL_TYPE_INT24,
		mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONGLONG:
		width := 8
		switch f.Type {
		case mysql.MYSQL_TYPE_TINY:
			width = 1
		case mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_YEAR:
			width = 2
		case mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG:
			width = 4
		}
		var u uint64
		if f.Flag&mysql.UNSIGNED_FLAG != 0 {
			v, err := strconv.ParseUint(s, 10, width*8)
			if err != nil {
				return nil, false
			}
			u = v
		} else {
			v, err := strconv.ParseInt(s, 10, width*8)
			if err != nil {
				return nil, false
			}
			u = uint64(v)
		}
		out := make([]byte, 8)
		binary.LittleEndian.PutUint64(out, u)
		return out[:width], true
	case mysql.MYSQL_TYPE_FLOAT:
		v, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return nil, false
		}
		out := make([]byte, 4)
		binary.LittleEndian.PutUint32(out, math.Float32bits(float32(v)))
		return out, true
	case mysql.MYSQL_TYPE_DOUBLE:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, false
		}
		out := make([]byte, 8)
		binary.LittleEndian.PutUint64(out, math.Float64bits(v))
		return out, true
	case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP:
		return encodeBinaryDateTime(s)
	case mysql.MYSQL_TYPE_TIME:
		return encodeBinaryTime(s)
	}
	return nil, false
}

var (
	dateTimeText = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?)?$`)
	timeText     = regexp.MustCompile(`^(-?)(\d{1,3}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?$`)
)

// micros reads a fractional-second string ("5", "123456") as microseconds.
func micros(frac string) uint32 {
	if frac == "" {
		return 0
	}
	for len(frac) < 6 {
		frac += "0"
	}
	v, _ := strconv.ParseUint(frac, 10, 32)
	return uint32(v)
}

func atoiByte(s string) byte {
	v, _ := strconv.Atoi(s)
	return byte(v)
}

// encodeBinaryDateTime writes MySQL's binary DATE/DATETIME: a length byte
// (0, 4, 7 or 11) and that many bytes.
func encodeBinaryDateTime(s string) ([]byte, bool) {
	m := dateTimeText.FindStringSubmatch(s)
	if m == nil {
		return nil, false
	}
	year, _ := strconv.Atoi(m[1])
	out := []byte{4, byte(year), byte(year >> 8), atoiByte(m[2]), atoiByte(m[3])}
	us := micros(m[7])
	if m[4] == "" {
		return out, true
	}
	out = append(out, atoiByte(m[4]), atoiByte(m[5]), atoiByte(m[6]))
	out[0] = 7
	if us != 0 {
		out = binary.LittleEndian.AppendUint32(out, us)
		out[0] = 11
	}
	return out, true
}

// encodeBinaryTime writes MySQL's binary TIME: a length byte (8 or 12), the
// sign, days, hours, minutes, seconds and, when non-zero, microseconds.
func encodeBinaryTime(s string) ([]byte, bool) {
	m := timeText.FindStringSubmatch(s)
	if m == nil {
		return nil, false
	}
	hours, _ := strconv.Atoi(m[2])
	out := []byte{8, 0}
	if m[1] == "-" {
		out[1] = 1
	}
	out = binary.LittleEndian.AppendUint32(out, uint32(hours/24))
	out = append(out, byte(hours%24), atoiByte(m[3]), atoiByte(m[4]))
	if us := micros(m[5]); us != 0 {
		out = binary.LittleEndian.AppendUint32(out, us)
		out[0] = 12
	}
	return out, true
}
