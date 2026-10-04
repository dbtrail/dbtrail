package shim

import (
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// A `?` is a parameter only where the statement's reader would see one: not
// inside a quoted string, a backtick identifier or a comment. The two readers
// disagree on where a string and a comment end, so the count is per reader.
func TestSplitPlaceholders(t *testing.T) {
	cases := []struct {
		stmt        string
		mysql, copy int
	}{
		{"SELECT 1", 0, 0},
		{"SELECT * FROM t WHERE a = ? AND b = ?", 2, 2},
		{"SELECT ?", 1, 1},
		{"?", 1, 1},
		{"SELECT '?' , ?", 1, 1},
		{`SELECT "?" , ?`, 1, 1},
		{"SELECT `a?b` FROM t WHERE c = ?", 1, 1},
		{"SELECT /* ? */ ? -- ?\n, ?", 2, 2},
		{"SELECT 'a''?' , ?", 1, 1},
		{"SELECT 'unterminated ? ", 0, 0},
		{"SELECT `unterminated ? ", 0, 0},
		{"SELECT a-? FROM t", 1, 1}, // a minus, not a comment
		// A backslash escapes the quote for MySQL and is a character for the
		// copy, where the string ends at that quote.
		{`SELECT 'it\'s ?' , ?`, 1, 1}, // not the same one: the copy's string ends at the first quote pair
		{`SELECT replace(p, '\', '/') FROM f WHERE id = ?`, 0, 1},
		// `--` without a space is a comment only for the copy; `#` only for MySQL.
		{"SELECT a --? x\n FROM t", 1, 0},
		{"SELECT a # ?\n, ?", 1, 2},
	}
	for _, tc := range cases {
		for _, mode := range []struct {
			mysqlStyle bool
			want       int
		}{{true, tc.mysql}, {false, tc.copy}} {
			parts := splitPlaceholders(tc.stmt, mode.mysqlStyle)
			if got := len(parts) - 1; got != mode.want {
				t.Errorf("splitPlaceholders(%q, mysql=%v): %d placeholders, want %d (parts %q)", tc.stmt, mode.mysqlStyle, got, mode.want, parts)
			}
			if strings.Join(parts, "?") != tc.stmt {
				t.Errorf("splitPlaceholders(%q) does not rebuild the statement: %q", tc.stmt, parts)
			}
		}
	}
}

func dateTimeArg(typ uint8, year, month, day int, rest ...int) mysql.TypedBytes {
	b := []byte{byte(year), byte(year >> 8), byte(month), byte(day)}
	if len(rest) >= 3 {
		b = append(b, byte(rest[0]), byte(rest[1]), byte(rest[2]))
	}
	if len(rest) == 4 {
		b = binary.LittleEndian.AppendUint32(b, uint32(rest[3]))
	}
	return mysql.TypedBytes{Type: typ, Bytes: b}
}

// Every Go value go-mysql's server decodes a bound parameter into, as the
// literal each reader takes: the copy (standard SQL, a backslash is a
// character) and the time-travel parser (MySQL, a backslash escapes).
func TestSQLLiteral(t *testing.T) {
	str := func(s string) mysql.TypedBytes {
		return mysql.TypedBytes{Type: mysql.MYSQL_TYPE_VAR_STRING, Bytes: []byte(s)}
	}
	cases := []struct {
		name      string
		arg       any
		copy, mys string
	}{
		{"null", nil, "NULL", "NULL"},
		{"int8", int8(-5), " -5", " -5"},
		{"uint8", uint8(250), "250", "250"},
		{"int16", int16(-300), " -300", " -300"},
		{"uint16", uint16(65000), "65000", "65000"},
		{"int32", int32(-70000), " -70000", " -70000"},
		{"uint32", uint32(4000000000), "4000000000", "4000000000"},
		{"int64", int64(math.MinInt64), " -9223372036854775808", " -9223372036854775808"},
		{"uint64", uint64(math.MaxUint64), "18446744073709551615", "18446744073709551615"},
		{"float32", float32(1.5), "1.5", "1.5"},
		{"float64", 2.25e10, "2.25e+10", "2.25e+10"},
		{"string", str("live"), "'live'", "'live'"},
		{"empty string", str(""), "''", "''"},
		{"quote", str("it's"), "'it''s'", `'it\'s'`},
		{"backslash", str(`C:\tmp`), `'C:\tmp'`, `'C:\\tmp'`},
		{"injection", str("x'; DROP TABLE t; --"), "'x''; DROP TABLE t; --'", `'x\'; DROP TABLE t; --'`},
		{"question mark", str("why?"), "'why?'", "'why?'"},
		{"newline", str("a\nb"), "'a\nb'", "'a\nb'"},
		{"not utf-8", mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte{0xff, 0x00, 0x27}}, "unhex('ff0027')", "X'ff0027'"},
		{"decimal", mysql.TypedBytes{Type: mysql.MYSQL_TYPE_NEWDECIMAL, Bytes: []byte("-12.50")}, " -12.50", " -12.50"},
		{"decimal that is not one", mysql.TypedBytes{Type: mysql.MYSQL_TYPE_NEWDECIMAL, Bytes: []byte("1; DROP")}, "'1; DROP'", "'1; DROP'"},
		{"date", dateTimeArg(mysql.MYSQL_TYPE_DATE, 2026, 10, 4), "'2026-10-04'", "'2026-10-04'"},
		{"datetime", dateTimeArg(mysql.MYSQL_TYPE_DATETIME, 2026, 10, 4, 13, 5, 9), "'2026-10-04 13:05:09'", "'2026-10-04 13:05:09'"},
		{"datetime micros", dateTimeArg(mysql.MYSQL_TYPE_TIMESTAMP, 2026, 10, 4, 13, 5, 9, 250), "'2026-10-04 13:05:09.000250'", "'2026-10-04 13:05:09.000250'"},
		{"zero datetime", mysql.TypedBytes{Type: mysql.MYSQL_TYPE_DATETIME}, "'0000-00-00 00:00:00'", "'0000-00-00 00:00:00'"},
		{"time", mysql.TypedBytes{Type: mysql.MYSQL_TYPE_TIME, Bytes: []byte{1, 1, 0, 0, 0, 2, 3, 4}}, "'-26:03:04'", "'-26:03:04'"},
		{"zero time", mysql.TypedBytes{Type: mysql.MYSQL_TYPE_TIME}, "'00:00:00'", "'00:00:00'"},
	}
	for _, tc := range cases {
		for _, mode := range []struct {
			mysqlEscapes bool
			want         string
		}{{false, tc.copy}, {true, tc.mys}} {
			got, err := sqlLiteral(tc.arg, mode.mysqlEscapes)
			if err != nil || got != mode.want {
				t.Errorf("%s (mysqlEscapes=%v): %q, %v; want %q", tc.name, mode.mysqlEscapes, got, err, mode.want)
			}
		}
	}
	for name, arg := range map[string]any{
		"NaN":            math.NaN(),
		"Inf":            math.Inf(1),
		"malformed date": mysql.TypedBytes{Type: mysql.MYSQL_TYPE_DATETIME, Bytes: []byte{1, 2, 3}},
		"malformed time": mysql.TypedBytes{Type: mysql.MYSQL_TYPE_TIME, Bytes: []byte{1, 2, 3}},
		"unknown type":   struct{}{},
	} {
		if got, err := sqlLiteral(arg, false); err == nil {
			t.Errorf("%s: literal %q, want an error", name, got)
		}
	}
}

// The copy's reading of an interpolated string is the argument's own value:
// the time-travel parser's unescape turns the MySQL spelling back into it.
func TestSQLLiteral_mysqlSpellingRoundTrips(t *testing.T) {
	for _, v := range []string{"it's", `C:\tmp\new`, `a\'b`, `50%\_x`, "plain"} {
		lit := quoteBytes([]byte(v), true)
		if got := unescapeStringLiteral(lit[1 : len(lit)-1]); got != v {
			t.Errorf("%q spelled %s reads back as %q", v, lit, got)
		}
	}
}

// binaryResultset's rows must read back, through go-mysql's own binary row
// parser, as the values the text resultset held, under each column's type.
func TestBinaryResultset_roundTrip(t *testing.T) {
	names := []string{"big", "ubig", "i", "tiny", "d", "f", "day", "ts", "tsus", "dur", "dec", "txt", "empty"}
	rows := [][]any{
		{"-42", "18446744073709551615", "-70000", "-5", "2.5", "1.5", "2026-10-04", "2026-10-04 13:05:09", "2026-10-04 13:05:09.25", "-26:03:04", "12.50", "live", []byte{}}, // an empty []byte: the builder turns an empty Go string into NULL
		{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{"7", "0", "1", "1", "-0.125", "0", "0000-00-00", "1999-12-31 23:59:59", "2000-01-01 00:00:00.000001", "838:59:59", "0", "ñandú", "x"},
	}
	rs, err := mysql.BuildSimpleTextResultset(names, rows)
	if err != nil {
		t.Fatal(err)
	}
	types := []uint8{mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_TINY,
		mysql.MYSQL_TYPE_DOUBLE, mysql.MYSQL_TYPE_FLOAT, mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP,
		mysql.MYSQL_TYPE_TIME, mysql.MYSQL_TYPE_NEWDECIMAL, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_VAR_STRING}
	for i, typ := range types {
		rs.Fields[i].Type = typ
	}
	rs.Fields[1].Flag |= mysql.UNSIGNED_FLAG

	bin, err := binaryResultset(rs)
	if err != nil {
		t.Fatal(err)
	}
	if len(bin.RowDatas) != 3 || len(bin.Fields) != len(names) {
		t.Fatalf("got %d rows, %d fields", len(bin.RowDatas), len(bin.Fields))
	}
	for i, typ := range types {
		if bin.Fields[i].Type != typ {
			t.Errorf("column %s: type %d, want %d (kept)", names[i], bin.Fields[i].Type, typ)
		}
	}
	want := [][]any{
		{int64(-42), uint64(math.MaxUint64), int64(-70000), int64(-5), 2.5, 1.5, "2026-10-04", "2026-10-04 13:05:09", "2026-10-04 13:05:09.250000", "-26:03:04", "12.50", "live", ""},
		{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{int64(7), uint64(0), int64(1), int64(1), -0.125, 0.0, "0000-00-00", "1999-12-31 23:59:59", "2000-01-01 00:00:00.000001", "838:59:59", "0", "ñandú", "x"},
	}
	for r, data := range bin.RowDatas {
		vals, err := data.ParseBinary(bin.Fields, nil)
		if err != nil {
			t.Fatalf("row %d does not parse as binary: %v", r, err)
		}
		for c := range vals {
			got := vals[c].Value()
			if b, ok := got.([]byte); ok {
				got = string(b)
			}
			if got != want[r][c] {
				t.Errorf("row %d col %s = %#v, want %#v", r, names[c], got, want[r][c])
			}
		}
	}
	// The source resultset is untouched: its fields and text rows still serve
	// a text-protocol client.
	if rs.Fields[0].Type != mysql.MYSQL_TYPE_LONGLONG || len(rs.RowDatas) != 3 {
		t.Error("binaryResultset changed the resultset it was given")
	}
}

// A column whose cells do not all read as its declared type goes out as
// text, definition included: the client must never decode bytes under the
// wrong type.
func TestBinaryResultset_mistypedColumnBecomesText(t *testing.T) {
	rs, err := mysql.BuildSimpleTextResultset([]string{"n", "when", "ok"}, [][]any{
		{"1", "2026-10-04 13:05:09", "5"},
		{"not a number", "yesterday", "6"},
	})
	if err != nil {
		t.Fatal(err)
	}
	rs.Fields[0].Type = mysql.MYSQL_TYPE_LONGLONG
	rs.Fields[0].Flag |= mysql.UNSIGNED_FLAG
	rs.Fields[1].Type = mysql.MYSQL_TYPE_DATETIME
	rs.Fields[2].Type = mysql.MYSQL_TYPE_LONGLONG
	bin, err := binaryResultset(rs)
	if err != nil {
		t.Fatal(err)
	}
	if bin.Fields[0].Type != mysql.MYSQL_TYPE_VAR_STRING || bin.Fields[1].Type != mysql.MYSQL_TYPE_VAR_STRING || bin.Fields[2].Type != mysql.MYSQL_TYPE_LONGLONG {
		t.Fatalf("types = %d, %d, %d; want the two mistyped columns as VAR_STRING and the third kept", bin.Fields[0].Type, bin.Fields[1].Type, bin.Fields[2].Type)
	}
	if bin.Fields[0].Flag&mysql.UNSIGNED_FLAG != 0 {
		t.Error("a column turned into text kept its UNSIGNED flag")
	}
	want := [][]any{{"1", "2026-10-04 13:05:09", int64(5)}, {"not a number", "yesterday", int64(6)}}
	for r, data := range bin.RowDatas {
		vals, err := data.ParseBinary(bin.Fields, nil)
		if err != nil {
			t.Fatalf("row %d: %v", r, err)
		}
		for c := range vals {
			got := vals[c].Value()
			if b, ok := got.([]byte); ok {
				got = string(b)
			}
			if got != want[r][c] {
				t.Errorf("row %d col %d = %#v, want %#v", r, c, got, want[r][c])
			}
		}
	}
}

func preparedHandler(t *testing.T, f *fakeFreeSQL) *Handler {
	t.Helper()
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	return h
}

// PREPARE counts placeholders; EXECUTE sends the copy the statement with the
// arguments written in the copy's own spelling, and answers in binary rows.
func TestPrepared_executeRunsOnTheCopy(t *testing.T) {
	f := &fakeFreeSQL{res: oneCell("n", "BIGINT", "3")}
	h := preparedHandler(t, f)
	params, cols, ctx, err := h.HandleStmtPrepare("SELECT count(*) AS n FROM orders WHERE status = ? AND note <> ? AND id > ?")
	if err != nil {
		t.Fatal(err)
	}
	if params != 3 || cols != 0 {
		t.Fatalf("prepare = %d params, %d columns; want 3 and 0", params, cols)
	}
	res, err := h.HandleStmtExecute(ctx, "", []any{
		mysql.TypedBytes{Type: mysql.MYSQL_TYPE_VAR_STRING, Bytes: []byte("it's")},
		mysql.TypedBytes{Type: mysql.MYSQL_TYPE_VAR_STRING, Bytes: []byte(`a\b`)},
		int64(10),
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := `SELECT count(*) AS n FROM orders WHERE status = 'it''s' AND note <> 'a\b' AND id > 10`; f.gotStmt != want {
		t.Errorf("the copy got\n  %s\nwant\n  %s", f.gotStmt, want)
	}
	if res == nil || res.Resultset == nil || len(res.RowDatas) != 1 {
		t.Fatalf("result = %+v", res)
	}
	vals, err := res.RowDatas[0].ParseBinary(res.Fields, nil)
	if err != nil {
		t.Fatalf("the row is not binary-encoded: %v", err)
	}
	if got := vals[0].Value(); got != int64(3) {
		t.Errorf("n = %#v, want int64 3 (a typed column)", got)
	}
	if h.buffered {
		t.Error("the handler stayed in buffered mode after the statement")
	}
	// The same statement runs again with other arguments.
	if _, err := h.HandleStmtExecute(ctx, "", []any{nil, []byte{0xff, 0x41}, int8(-1)}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.gotStmt, "status = NULL AND note <> unhex('ff41') AND id >  -1") {
		t.Errorf("second execution sent %s", f.gotStmt)
	}
	if err := h.HandleStmtClose(ctx); err != nil {
		t.Errorf("close: %v", err)
	}
}

func mysqlErrCode(err error) uint16 {
	var me *mysql.MyError
	if errors.As(err, &me) {
		return me.Code
	}
	return 0
}

func TestPrepared_refusals(t *testing.T) {
	f := &fakeFreeSQL{res: oneCell("n", "BIGINT", "3")}
	h := preparedHandler(t, f)
	_, _, ctx, err := h.HandleStmtPrepare("SELECT ? + ?")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{int64(1)}); mysqlErrCode(err) != mysql.ER_WRONG_ARGUMENTS {
		t.Errorf("one argument for two placeholders: err = %v, want 1210", err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{math.NaN(), int64(1)}); mysqlErrCode(err) != mysql.ER_WRONG_ARGUMENTS {
		t.Errorf("NaN: err = %v, want 1210", err)
	}
	if _, err := h.HandleStmtExecute("not ours", "", nil); mysqlErrCode(err) != mysql.ER_UNKNOWN_STMT_HANDLER {
		t.Errorf("foreign context: err = %v, want 1243", err)
	}
	if f.calls != 0 {
		t.Errorf("a refused execution reached the copy %d times", f.calls)
	}
	if h.buffered {
		t.Error("a refused execution left the handler buffered")
	}
	// The copy's own error comes back as it does for a text statement.
	f.err = errors.New("Binder Error: no such column")
	if _, err := h.HandleStmtExecute(ctx, "", []any{int64(1), int64(2)}); err == nil {
		t.Error("the copy's error did not reach the client")
	}
	if h.buffered {
		t.Error("a failed execution left the handler buffered")
	}

	// With read routing bound the port refuses to prepare: a forwarded
	// statement must be bound by MySQL, not rebuilt from text here.
	r := &fakeRouter{}
	rh := routingHandler(t, r, f, time.Minute)
	if _, _, _, err := rh.HandleStmtPrepare("SELECT ?"); mysqlErrCode(err) != mysql.ER_UNSUPPORTED_PS {
		t.Errorf("prepare under routing: err = %v, want 1295", err)
	}
	if len(r.forwarded)+len(r.explained) != 0 {
		t.Error("a refused prepare reached the source")
	}
}

// A template headed for the time-travel parser gets MySQL's spelling of a
// string (the parser unescapes backslashes); one for the copy does not.
func TestPrepared_escapeStyleFollowsTheReader(t *testing.T) {
	h := preparedHandler(t, &fakeFreeSQL{})
	for tmpl, want := range map[string]bool{
		"SELECT * FROM _flashback.orders AS OF ? WHERE id = ?":    true,
		"SELECT * FROM _snapshot.orders WHERE id = ?":             true,
		"SELECT * FROM _diff.orders WHERE id = ?":                 true,
		"SELECT * FROM orders WHERE id = ? AS OF '5 minutes ago'": true,
		"SELECT * FROM orders AS OF ?":                            true,
		"SELECT * FROM orders WHERE status = ?":                   false,
		"SELECT flashback_col FROM orders WHERE status = ?":       false,
		// Time-travel words that are not the statement's shape: the copy
		// reads these, so its arguments must be quoted the copy's way.
		"SELECT * FROM orders WHERE note = ? AND tag = 'as of'":          false,
		"SELECT dbtrail_attempts FROM orders WHERE id = ?":               false,
		"SELECT * FROM orders WHERE note LIKE '%_flashback.%' AND c = ?": false,
		// The parser's own screen reads a comment too; the style follows the
		// parser, whatever it then makes of the statement.
		"SELECT * FROM orders /* from _snapshot.x */ WHERE c = ?": true,
	} {
		_, _, ctx, err := h.HandleStmtPrepare(tmpl)
		if err != nil {
			t.Fatal(err)
		}
		if got := ctx.(*preparedStmt).mysqlEscapes; got != want {
			t.Errorf("%q: mysqlEscapes = %v, want %v", tmpl, got, want)
		}
	}
}

// The quoting follows the reader even when the statement mentions time-travel
// words, and an argument that would hand the text to the other reader is
// refused: text is never run under a reader it was not escaped for.
func TestPrepared_argumentsNeverSwitchTheReader(t *testing.T) {
	f := &fakeFreeSQL{res: oneCell("n", "BIGINT", "3")}
	h := preparedHandler(t, f)
	str := func(s string) mysql.TypedBytes {
		return mysql.TypedBytes{Type: mysql.MYSQL_TYPE_VAR_STRING, Bytes: []byte(s)}
	}
	_, _, ctx, err := h.HandleStmtPrepare("SELECT count(*) AS n FROM t WHERE note = ? AND tag = 'as of'")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{str(`x' OR 1=1 --`)}); err != nil {
		t.Fatal(err)
	}
	if want := `SELECT count(*) AS n FROM t WHERE note = 'x'' OR 1=1 --' AND tag = 'as of'`; f.gotStmt != want {
		t.Errorf("the copy got\n  %s\nwant\n  %s", f.gotStmt, want)
	}

	// `AS OF ?` is a time-travel shape; a quote in the argument breaks the
	// shape, and the MySQL-escaped text would fall through to the copy.
	_, _, ctx, err = h.HandleStmtPrepare("SELECT * FROM orders AS OF ?")
	if err != nil {
		t.Fatal(err)
	}
	calls := f.calls
	if _, err := h.HandleStmtExecute(ctx, "", []any{str(`x' OR 1=1 --`)}); mysqlErrCode(err) != mysql.ER_WRONG_ARGUMENTS || !strings.Contains(err.Error(), "changes how") {
		t.Errorf("an argument that switches the reader: err = %v, want the 1210 refusal", err)
	}
	if f.calls != calls {
		t.Error("text escaped for the time-travel parser reached the copy")
	}

	// A copy statement is split the copy's way: a backslash in its string is
	// a character, so the string ends where the copy says and the `?` after
	// it is a placeholder.
	params, _, ctx, err := h.HandleStmtPrepare(`SELECT count(*) AS n FROM f WHERE replace(p, '\', '/') = ?`)
	if err != nil || params != 1 {
		t.Fatalf("prepare = %d params, %v; want 1", params, err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{str("a/b")}); err != nil {
		t.Fatal(err)
	}
	if want := `SELECT count(*) AS n FROM f WHERE replace(p, '\', '/') = 'a/b'`; f.gotStmt != want {
		t.Errorf("the copy got %s, want %s", f.gotStmt, want)
	}

	// A minus before a negative argument must not become the copy's comment.
	_, _, ctx, err = h.HandleStmtPrepare("SELECT count(*) AS n FROM t WHERE x > 5-? AND y = 1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{int64(-3)}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(f.gotStmt, "--") || !strings.HasSuffix(f.gotStmt, "5- -3 AND y = 1") {
		t.Errorf("the copy got %s", f.gotStmt)
	}
}
