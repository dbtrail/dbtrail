package shim

import (
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// A session in utf8mb3 on a routing connection (#2917).
//
// utf8mb3 is UTF-8 cut at three bytes: every character below U+10000 has the
// same bytes in it as in utf8mb4, the copy's only character set, and the
// characters from U+10000 up (emoji, some CJK, historic scripts) do not exist
// in it. MySQL converts them to '?' wherever utf8mb3 is the target:
//
//   - character_set_connection: a literal of the statement (and a prepared
//     statement's string argument) is converted to it, so MySQL compares and
//     returns '?' where the copy keeps the character. A statement holding
//     one is MySQL's (narrowStatementVeto), wherever in the text it is: a
//     comment or a name too, where MySQL errors or ignores it and the copy
//     would not care, since telling those apart is not worth a wrong answer.
//     The same holds when only the results are in utf8mb3: the session's
//     character_set_client, which the read-back does not read, is then most
//     likely utf8mb3 too (a client that asked for it and later changed only
//     its collation), and MySQL reads the statement's bytes in it.
//   - character_set_results: MySQL sends each text cell converted, so a
//     four-byte character goes out as '?'. The copy's answer is checked
//     before anything of it is written (narrowAnswerCheck); one with such a
//     character in a text cell is not sent, the statement goes to MySQL,
//     which answers it its own way (narrowResultError). Bytes that are not
//     UTF-8 at all are treated the same: what MySQL makes of them is not
//     the copy's to guess. A statement so forwarded is remembered on the
//     connection (narrowMemo) and goes straight to MySQL the next times,
//     instead of costing a run on the copy and then one on MySQL again.
//
// The column definitions say utf8mb3 too: MySQL declares each text column of
// a converted answer in the results' character set, utf8mb3_general_ci (33),
// and a client that sizes or decodes by the definition sees what MySQL's
// answer would have told it (declareUTF8MB3).

// utf8mb3GeneralCI is the collation id MySQL and MariaDB declare a text
// column with under character_set_results = utf8mb3: the set's default.
const utf8mb3GeneralCI = 33

// fitsUTF8MB3 says s is UTF-8 with no character from U+10000 up: what
// utf8mb3 carries as the same bytes.
func fitsUTF8MB3(s string) bool {
	for i := 0; i < len(s); {
		if s[i] < utf8.RuneSelf {
			i++
			continue
		}
		r, n := utf8.DecodeRuneInString(s[i:])
		if (r == utf8.RuneError && n <= 1) || r > 0xFFFF {
			return false
		}
		i += n
	}
	return true
}

// fitsUTF8MB3Bytes is fitsUTF8MB3 for bytes.
func fitsUTF8MB3Bytes(b []byte) bool {
	for i := 0; i < len(b); {
		if b[i] < utf8.RuneSelf {
			i++
			continue
		}
		r, n := utf8.DecodeRune(b[i:])
		if (r == utf8.RuneError && n <= 1) || r > 0xFFFF {
			return false
		}
		i += n
	}
	return true
}

// narrowSession is what the read-back said of the session's character sets.
func (h *Handler) narrowSession() (results, connection bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.routeSess.narrowResults, h.routeSess.narrowConnection
}

// narrowStatementVeto is why a statement is MySQL's under a session in
// utf8mb3 (its connection or its results), or "". text is what the copy
// would read: the client's statement, or a prepared one with its arguments
// written in.
func (h *Handler) narrowStatementVeto(text string) string {
	if results, conn := h.narrowSession(); !(conn || results) || fitsUTF8MB3(text) {
		return ""
	}
	return narrowStatementWhy
}

const narrowStatementWhy = "the statement holds a character utf8mb3 does not have (four bytes in UTF-8, as an emoji) or bytes that are not UTF-8, " +
	"and the session's character set is utf8mb3: MySQL reads it as '?', the copy as it is"

// narrowArgumentVeto is narrowStatementVeto over a prepared statement's
// arguments as the client sent them. Their text is not enough: an argument
// that is not UTF-8 is written into it as unhex('...'), which is ASCII.
// MySQL converts a string argument as it converts a literal; one the client
// typed as a BLOB it takes as bytes (the binary character set), and so does
// the copy (unhex, or the same bytes as text), so a BLOB is not looked at.
// Nor is a number or a date, sent in a binary form that is not text at all.
func (h *Handler) narrowArgumentVeto(args []any) string {
	if results, conn := h.narrowSession(); !(conn || results) {
		return ""
	}
	for _, a := range args {
		var b []byte
		switch v := a.(type) {
		case string:
			b = []byte(v)
		case []byte:
			b = v
		case mysql.TypedBytes:
			switch v.Type {
			case mysql.MYSQL_TYPE_TINY_BLOB, mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB,
				mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIME:
				continue
			}
			b = v.Bytes
		default:
			continue
		}
		if !fitsUTF8MB3Bytes(b) {
			return narrowStatementWhy
		}
	}
	return ""
}

// narrowMemoSize bounds narrowMemo: past it the oldest statement is
// forgotten, and costs one more run on the copy if it comes back.
const narrowMemoSize = 256

// narrowMemo is the statements (by readrouter.Fingerprint) whose answer
// from the copy held what utf8mb3 results cannot carry, on this
// connection's session as it was read: kept on the routeSession, so a
// session read again (after a SET NAMES, a SET of any kind, a change the
// source reported) starts with none. Under the connection's lock.
type narrowMemo struct {
	seen  map[string]bool
	order []string
}

func (m *narrowMemo) add(fp string) {
	if m.seen[fp] {
		return
	}
	if m.seen == nil {
		m.seen = map[string]bool{}
	}
	if len(m.order) >= narrowMemoSize {
		delete(m.seen, m.order[0])
		m.order = m.order[1:]
	}
	m.seen[fp] = true
	m.order = append(m.order, fp)
}

// rememberNarrow records that the copy's answer to a statement of this
// fingerprint was not sent (narrowResultError).
func (h *Handler) rememberNarrow(fp string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.routeSess.known {
		return
	}
	if h.routeSess.narrowMemo == nil {
		h.routeSess.narrowMemo = &narrowMemo{}
	}
	h.routeSess.narrowMemo.add(fp)
}

// narrowRemembered says rememberNarrow recorded fp on the session as it is.
func (h *Handler) narrowRemembered(fp string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.routeSess.narrowMemo != nil && h.routeSess.narrowMemo.seen[fp]
}

// narrowStatementError is narrowStatementVeto found in a prepared
// statement's arguments, which are only known once the copy is about to run.
type narrowStatementError struct{ why string }

func (e *narrowStatementError) Error() string { return e.why }

// narrowResultError is an answer of the copy that holds, in a text cell, what
// utf8mb3 results cannot carry: not sent, the ladder runs the statement on
// MySQL. It names the cell, never its value.
type narrowResultError struct {
	col  int
	name string
	row  int
}

func (e *narrowResultError) Error() string {
	return fmt.Sprintf("character_set_results = utf8mb3: the copy's answer holds a character utf8mb3 does not have (four bytes in UTF-8, as an emoji), "+
		"or bytes that are not UTF-8, in column %d (%s) of row %d; MySQL sends it as '?'", e.col+1, e.name, e.row+1)
}

// narrowTextColumn says a column of the copy's answer is text MySQL would
// convert to character_set_results: what applyDuckType declares utf8mb4
// (a nested column's JSON included), but not a BLOB, which MySQL sends as
// its bytes whatever the session's character sets (the copy declares it
// utf8mb4 too, an older difference).
func narrowTextColumn(duckType string) bool {
	base, nested := duckBaseType(duckType)
	if nested {
		// Sent as JSON text, whatever its elements (a BLOB[] too).
		return true
	}
	switch base {
	case "BLOB", "BYTEA", "BINARY", "VARBINARY", "BIT", "BITSTRING":
		return false
	}
	f := &mysql.Field{}
	applyDuckType(f, duckType)
	return f.Charset == 255
}

// narrowAnswerCheck looks through the copy's answer, rs as built from res
// (or the file res.Spool, for a result past the memory cap), for a text cell
// utf8mb3 cannot carry. Called before anything is written or counted, so an
// answer it refuses was never served.
func narrowAnswerCheck(rs *mysql.Resultset, res sqlsandbox.Result, loc *time.Location) error {
	var text []int
	for c, col := range res.Columns {
		if narrowTextColumn(col.Type) {
			text = append(text, c)
		}
	}
	if len(text) == 0 {
		return nil
	}
	if res.Spool != nil {
		row := 0
		return res.Spool.Each(func(cells []any) error {
			defer func() { row++ }()
			for _, c := range text {
				if c >= len(cells) || cells[c] == nil {
					continue
				}
				v, err := freeSQLCell(cells[c], res.Columns[c].Type, loc, false)
				if err != nil {
					return cellError(err, res.Columns, c, row)
				}
				if b, _ := v.([]byte); !fitsUTF8MB3Bytes(b) {
					return &narrowResultError{col: c, name: res.Columns[c].Name, row: row}
				}
			}
			return nil
		})
	}
	cells, err := textCells(rs, len(rs.Fields))
	if err != nil {
		return err
	}
	for r, row := range cells {
		for _, c := range text {
			if c < len(row) && row[c] != nil && !fitsUTF8MB3Bytes(row[c]) {
				return &narrowResultError{col: c, name: res.Columns[c].Name, row: r}
			}
		}
	}
	return nil
}

// declareUTF8MB3 declares each text column of the copy's answer as MySQL
// declares it under character_set_results = utf8mb3. A nested column keeps
// the copy's declaration: MySQL types JSON its own way, which the copy does
// not follow under any session.
func declareUTF8MB3(fields []*mysql.Field, columns []sqlsandbox.Column) {
	for i, c := range columns {
		if i >= len(fields) {
			break
		}
		if _, nested := duckBaseType(c.Type); nested || !narrowTextColumn(c.Type) {
			continue
		}
		if fields[i].Charset == 255 {
			fields[i].Charset = utf8mb3GeneralCI
		}
	}
}
