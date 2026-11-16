package shim

import (
	"bytes"
	"slices"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2917: a session in utf8mb3 (the MariaDB client's default, ProxySQL's
// mysql-default_charset=utf8) is answered by the copy. utf8mb3 and utf8mb4
// are the same bytes for every character below U+10000, so only a statement
// or an answer holding a four-byte character (or bytes that are no UTF-8)
// is MySQL's.

const emoji = "\U0001F600" // four bytes in UTF-8, outside utf8mb3

func TestFitsUTF8MB3_2917(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want bool
	}{
		{"empty", "", true},
		{"ascii", "SELECT 1", true},
		{"latin letters", "é ñ ß", true},
		{"a CJK letter", "中", true},
		{"the last BMP character", "￿", true},
		{"U+FFFD written out", "�", true},
		{"U+10000", "\U00010000", false},
		{"an emoji", "a" + emoji + "b", false},
		{"a lone continuation byte", "a\x80b", false},
		{"a cut sequence", "a\xe4\xb8", false},
		{"an encoded surrogate", "\xed\xa0\x80", false},
		{"an overlong NUL", "\xc0\x80", false},
		{"a four-byte lead with nothing after it", "\xf0", false},
	} {
		if got := fitsUTF8MB3(tc.in); got != tc.want {
			t.Errorf("%s: fitsUTF8MB3(%q) = %v, want %v", tc.name, tc.in, got, tc.want)
		}
		if got := fitsUTF8MB3Bytes([]byte(tc.in)); got != tc.want {
			t.Errorf("%s: fitsUTF8MB3Bytes(%q) = %v, want %v", tc.name, tc.in, got, tc.want)
		}
	}
}

// What the read-back makes of each character set, case by case.
func TestRoutedSession_utf8mb3ReadBack_2917(t *testing.T) {
	instants := zoneProbeInstants(time.Now())
	for _, tc := range []struct {
		name               string
		results            any
		conn, coll         string
		wantDiffers        string // a prefix of differs; "" for none
		wantNarrowResults  bool
		wantNarrowConn     bool
		wantCollationDiffs bool
	}{
		{"the MariaDB client: utf8mb3 everywhere, utf8mb3_general_ci", "utf8mb3", "utf8mb3", "11001", "", true, true, true},
		{"results spelled utf8", "utf8", "utf8", "11110", "", true, true, false},
		{"results in capitals", "UTF8MB3", "UTF8MB3", "11110", "", true, true, false},
		{"connection utf8mb3, results utf8mb4", "utf8mb4", "utf8mb3", "11110", "", false, true, false},
		{"connection utf8mb4, results utf8mb3", "utf8mb3", "utf8mb4", "11110", "", true, false, false},
		{"results latin1", "latin1", "utf8mb4", "11110", "character_set_results = latin1", false, false, false},
		{"connection latin1", "utf8mb4", "latin1", "11110", "character_set_connection = latin1", false, false, false},
		{"results NULL", nil, "utf8mb4", "11110", "", false, false, false},
		{"results binary", "binary", "utf8mb4", "11110", "", false, false, false},
		{"utf8mb4", "utf8mb4", "utf8mb4", "11110", "", false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := stockSource()
			src.cs, src.csConn, src.coll = tc.results, tc.conn, tc.coll
			rs, err := sessionFromReadBack(src.sessionRow(sessionReadBackSQL(instants)), instants, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantDiffers == "" && rs.differs != "" {
				t.Errorf("differs %q, want none", rs.differs)
			}
			if tc.wantDiffers != "" && !bytes.HasPrefix([]byte(rs.differs), []byte(tc.wantDiffers)) {
				t.Errorf("differs %q, want it to open with %q", rs.differs, tc.wantDiffers)
			}
			if tc.wantDiffers != "" {
				return
			}
			if rs.narrowResults != tc.wantNarrowResults || rs.narrowConnection != tc.wantNarrowConn {
				t.Errorf("narrow results %v connection %v, want %v %v", rs.narrowResults, rs.narrowConnection, tc.wantNarrowResults, tc.wantNarrowConn)
			}
			if (rs.collationDiffers != "") != tc.wantCollationDiffs {
				t.Errorf("collationDiffers %q, want set %v", rs.collationDiffers, tc.wantCollationDiffs)
			}
		})
	}
}

// narrowRig is a routing connection whose source session is the MariaDB
// client's: utf8mb3 results and connection. coll is the collation probe.
func narrowRig(t *testing.T, results any, conn, coll string) *sessRig {
	t.Helper()
	rig := newSessRig(t)
	rig.r.src.cs = results
	rig.r.src.csConn = conn
	rig.r.src.coll = coll
	return rig
}

// The MariaDB client's session lands where #2836 put a collation the copy
// does not reproduce: the copy answers what the collation cannot change,
// MySQL the rest.
func TestRoutedSession_utf8mb3GeneralCIIsTheRestrictedPath_2917(t *testing.T) {
	rig := narrowRig(t, "utf8mb3", "utf8mb3", "11001")
	for _, stmt := range []string{
		expensive,
		"SELECT SUM(k), MAX(k), COUNT(*) FROM t", // not AVG: kept on MySQL under every session (#2806)
	} {
		if got := firstCell(t, rig.run(t, stmt)); got != "copy" {
			t.Errorf("%q answered by %s (%s), want the copy\n%s", stmt, got, rig.last(), rig.logs.String())
		}
	}
	const withLiteral = "SELECT status, count(*) FROM t WHERE status = 'x' GROUP BY status"
	calls := rig.f.calls
	if got := firstCell(t, rig.run(t, withLiteral)); got != "mysql" || rig.last() != "mysql/session_differs" {
		t.Errorf("%q answered by %s (%s), want mysql/session_differs", withLiteral, got, rig.last())
	}
	if rig.f.calls != calls {
		t.Error("the copy was asked")
	}
}

// Under a utf8mb3 connection MySQL turns a four-byte character of the
// statement into '?': such a statement is MySQL's, wherever the character
// is. The same statement under utf8mb4 is the copy's, so the veto is this
// rule and no other.
func TestRoutedSession_utf8mb3StatementWithAFourByteCharacter_2917(t *testing.T) {
	for _, tc := range []struct{ name, stmt string }{
		{"in a literal", "SELECT status, count(*) FROM t WHERE status = '" + emoji + "' GROUP BY status"},
		{"in a comment", "SELECT status, count(*) FROM t /* " + emoji + " */ GROUP BY status"},
		{"in an identifier", "SELECT `a" + emoji + "`, count(*) FROM t GROUP BY 1"},
		{"bytes that are no UTF-8", "SELECT status, count(*) FROM t WHERE status = '\xff' GROUP BY status"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wide := narrowRig(t, "utf8mb4", "utf8mb4", "11110")
			if got := firstCell(t, wide.run(t, tc.stmt)); got != "copy" {
				t.Fatalf("under utf8mb4 answered by %s (%s), want the copy: the test would prove nothing", got, wide.last())
			}
			rig := narrowRig(t, "utf8mb4", "utf8mb3", "11110")
			calls := rig.f.calls
			if got := firstCell(t, rig.run(t, tc.stmt)); got != "mysql" || rig.last() != "mysql/veto" {
				t.Errorf("under a utf8mb3 connection answered by %s (%s), want mysql/veto", got, rig.last())
			}
			if rig.f.calls != calls {
				t.Error("the copy was asked")
			}
		})
	}
	// Text below U+10000 is the same bytes in both: the copy's.
	rig := narrowRig(t, "utf8mb3", "utf8mb3", "11110")
	const bmp = "SELECT status, count(*) FROM t WHERE status = 'é中ñ' GROUP BY status"
	if got := firstCell(t, rig.run(t, bmp)); got != "copy" {
		t.Errorf("%q answered by %s (%s), want the copy", bmp, got, rig.last())
	}
}

// A prepared statement's arguments are literals too: MySQL converts a string
// argument to the connection's character set.
func TestRoutedSession_utf8mb3PreparedArgument_2917(t *testing.T) {
	rig := narrowRig(t, "utf8mb3", "utf8mb3", "11110")
	_, _, sel, err := rig.h.HandleStmtPrepare("SELECT status, count(*) FROM t WHERE status = ? GROUP BY status")
	if err != nil {
		t.Fatal(err)
	}
	res, err := rig.h.HandleStmtExecute(sel, "", []any{strArg("é中")})
	if err != nil || binaryFirstCell(t, res) != "copy" {
		t.Fatalf("a BMP argument: %v (%s), want the copy", err, rig.last())
	}
	calls := rig.f.calls
	res, err = rig.h.HandleStmtExecute(sel, "", []any{strArg("a" + emoji)})
	if err != nil || binaryFirstCell(t, res) != "mysql" || rig.last() != "mysql/veto" {
		t.Errorf("a four-byte argument: %v (%s), want mysql/veto", err, rig.last())
	}
	if rig.f.calls != calls {
		t.Error("the copy was asked")
	}
}

// narrowAnswer is a copy answer of many rows: text, a BLOB, a number.
func narrowAnswer(text, blob any) sqlsandbox.Result {
	cols := []sqlsandbox.Column{{Name: "s", Type: "VARCHAR"}, {Name: "b", Type: "BLOB"}, {Name: "n", Type: "BIGINT"}}
	var rows [][]any
	for range 50 {
		rows = append(rows, []any{"plain é", "bytes", "1"})
	}
	rows = append(rows, []any{text, blob, "2"})
	for range 50 {
		rows = append(rows, []any{"中", nil, nil})
	}
	return sqlsandbox.Result{Columns: cols, Rows: rows}
}

// Under utf8mb3 results MySQL would send a four-byte character as '?': an
// answer of the copy holding one in a text cell is not sent, the statement
// is run on MySQL instead. Only text: a BLOB is sent as its bytes.
func TestRoutedSession_utf8mb3AnswerWithAFourByteCharacter_2917(t *testing.T) {
	for _, tc := range []struct {
		name       string
		res        sqlsandbox.Result
		wantMySQL  bool
		wideToCopy bool // and under utf8mb4 results, the copy answers
	}{
		{"one text cell of many rows", narrowAnswer("a"+emoji, "x"), true, true},
		{"a text cell that is no UTF-8", narrowAnswer("a\xffb", "x"), true, true},
		{"a NULL text cell", narrowAnswer(nil, "x"), false, true},
		{"a BLOB holding a four-byte sequence", narrowAnswer("x", emoji), false, true},
		{"a nested column", sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "l", Type: "VARCHAR[]"}},
			Rows: [][]any{{[]any{"a", emoji}}}}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wideToCopy {
				wide := narrowRig(t, "utf8mb4", "utf8mb4", "11110")
				wide.f.res = tc.res
				wide.run(t, expensive)
				if wide.last() != "copy/expensive_plan" {
					t.Fatalf("under utf8mb4 observed %s, want copy/expensive_plan", wide.last())
				}
			}
			rig := narrowRig(t, "utf8mb3", "utf8mb4", "11110")
			rig.f.res = tc.res
			rec := audittest.Install(t)
			res := rig.run(t, expensive)
			if !tc.wantMySQL {
				if rig.last() != "copy/expensive_plan" {
					t.Errorf("observed %s, want copy/expensive_plan", rig.last())
				}
				return
			}
			if got := firstCell(t, res); got != "mysql" || rig.last() != "mysql/session_differs" {
				t.Errorf("answered by %s (%s), want mysql/session_differs", got, rig.last())
			}
			if len(rig.r.forwarded) != 1 {
				t.Errorf("forwarded %v, want the statement once", rig.r.forwarded)
			}
			if ev := rec.Events(); len(ev) != 0 {
				t.Errorf("an answer never sent left audit records: %+v", ev)
			}
			if !bytes.Contains(rig.logs.Bytes(), []byte("character_set_results")) {
				t.Errorf("the log does not say why:\n%s", rig.logs.String())
			}
		})
	}
}

// The same answer from a result file: read before the header goes out, the
// file removed, the statement run on MySQL.
func TestRoutedSession_utf8mb3AnswerFromAFile_2917(t *testing.T) {
	ans := narrowAnswer("a"+emoji, "x")
	f := &fakeFreeSQL{res: sqlsandbox.Result{Columns: ans.Columns}, spoolRows: ans.Rows}
	src := stockSource()
	src.cs = "utf8mb3"
	r := &fakeRouter{toCopy: true, reason: "expensive", src: src}
	pw := &capturePW{}
	h := spoolRouting(t, f, r, pw)
	res, err := h.HandleQuery(spoolStmt)
	if err != nil {
		t.Fatal(err)
	}
	if got := firstCell(t, res); got != "mysql" || len(f.spools) != 1 {
		t.Fatalf("answered by %s after %d file(s), want mysql after the copy's file", got, len(f.spools))
	}
	if len(pw.packets) != 0 {
		t.Errorf("%d packets of the copy's answer on the wire", len(pw.packets))
	}
	gone(t, f.spools)
	if h.spooled != nil {
		t.Error("the copy's answer stayed pending")
	}
}

// A prepared statement's answer holding one is MySQL's too.
func TestRoutedSession_utf8mb3PreparedAnswer_2917(t *testing.T) {
	rig := narrowRig(t, "utf8mb3", "utf8mb3", "11110")
	rig.f.res = narrowAnswer("a"+emoji, "x")
	_, _, sel, err := rig.h.HandleStmtPrepare("SELECT s, b, n FROM t WHERE n > ?")
	if err != nil {
		t.Fatal(err)
	}
	res, err := rig.h.HandleStmtExecute(sel, "", []any{int64(0)})
	if err != nil || binaryFirstCell(t, res) != "mysql" || rig.last() != "mysql/session_differs" {
		t.Errorf("%v (%s), want mysql/session_differs", err, rig.last())
	}
}

// Under utf8mb3 results MySQL declares a text column in utf8mb3 (33,
// utf8mb3_general_ci); a number stays binary (63). The copy's answer says
// the same, in memory and from a file, text and prepared. Under utf8mb4 or
// no conversion a text column stays utf8mb4 (255).
func TestRoutedSession_utf8mb3ColumnCharset_2917(t *testing.T) {
	ans := narrowAnswer("x", "y")
	const prep = "SELECT s, b, n FROM t WHERE n > ?"
	run := func(t *testing.T, results any, file, prepared bool) (*mysql.Result, *capturePW) {
		t.Helper()
		f := &fakeFreeSQL{res: sqlsandbox.Result{Columns: ans.Columns}}
		rows := slices.Clone(ans.Rows)
		if file {
			f.spoolRows = rows
		} else {
			f.res.Rows = rows
		}
		src := stockSource()
		src.cs = results
		pw := &capturePW{}
		h := spoolRouting(t, f, &fakeRouter{toCopy: true, reason: "expensive", src: src}, pw)
		var res *mysql.Result
		var err error
		if prepared {
			_, _, ctx, perr := h.HandleStmtPrepare(prep)
			if perr != nil {
				t.Fatal(perr)
			}
			res, err = h.HandleStmtExecute(ctx, "", []any{int64(0)})
		} else {
			res, err = h.HandleQuery(spoolStmt)
		}
		if err != nil || res == nil || res.Resultset == nil || f.calls != 1 {
			t.Fatalf("(%v, %v) after %d copy run(s)", res, err, f.calls)
		}
		if file && len(f.spools) != 1 {
			t.Fatal("not answered from a file")
		}
		return res, pw
	}
	for _, prepared := range []bool{false, true} {
		for _, tc := range []struct {
			name     string
			results  any
			wantText uint16
		}{
			{"utf8mb3", "utf8mb3", 33},
			{"utf8", "utf8", 33},
			{"utf8mb4", "utf8mb4", 255},
			{"NULL", nil, 255},
		} {
			name := tc.name
			if prepared {
				name += "/prepared"
			}
			t.Run(name, func(t *testing.T) {
				mem, _ := run(t, tc.results, false, prepared)
				fs := mem.Resultset.Fields
				if fs[0].Charset != tc.wantText {
					t.Errorf("text column charset %d, want %d", fs[0].Charset, tc.wantText)
				}
				if fs[2].Charset != 63 {
					t.Errorf("number column charset %d, want 63", fs[2].Charset)
				}
				if fs[1].Charset != 255 {
					t.Errorf("BLOB column charset %d, want the copy's 255 as before", fs[1].Charset)
				}
				_, pw := run(t, tc.results, true, prepared)
				for i, fd := range fs {
					if !bytes.Equal(pw.packets[1+i], fd.Dump()) {
						t.Errorf("column %d: from the file %q, in memory %q", i, pw.packets[1+i], fd.Dump())
					}
				}
			})
		}
	}
}
