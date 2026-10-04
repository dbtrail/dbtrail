package shim

import (
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

// stmtRecorder records what the session hands a handler.
type stmtRecorder struct {
	server.EmptyHandler
	params  int
	execs   [][]any
	closed  []any
	execErr error
}

func (h *stmtRecorder) HandleStmtPrepare(q string) (int, int, any, error) {
	if q == "bad" {
		return 0, 0, nil, mysql.NewError(mysql.ER_PARSE_ERROR, "bad")
	}
	return h.params, 0, "ctx:" + q, nil
}

func (h *stmtRecorder) HandleStmtExecute(ctx any, q string, args []any) (*mysql.Result, error) {
	h.execs = append(h.execs, append([]any{ctx, q}, args...))
	if h.execErr != nil {
		return nil, h.execErr
	}
	return &mysql.Result{Status: 2}, nil
}

func (h *stmtRecorder) HandleStmtClose(ctx any) error {
	h.closed = append(h.closed, ctx)
	return nil
}

// execPacket builds a COM_STMT_EXECUTE payload (after the command byte).
// types nil means "types not re-sent" (the new-params-bound byte is 0).
func execPacket(id uint32, params int, nulls []int, types []byte, values []byte) []byte {
	p := binary.LittleEndian.AppendUint32(nil, id)
	p = append(p, 0, 1, 0, 0, 0)
	if params == 0 {
		return p
	}
	bitmap := make([]byte, (params+7)>>3)
	for _, n := range nulls {
		bitmap[n>>3] |= 1 << (uint(n) % 8)
	}
	p = append(p, bitmap...)
	if types != nil {
		p = append(p, 1)
		p = append(p, types...)
	} else {
		p = append(p, 0)
	}
	return append(p, values...)
}

func prepareStmt(t *testing.T, s *Session, q string) uint32 {
	t.Helper()
	st, ok := s.dispatch(mysql.COM_STMT_PREPARE, []byte(q)).(*server.Stmt)
	if !ok {
		t.Fatalf("prepare %q did not answer with a statement", q)
	}
	return st.ID
}

// The reason the session exists: a client that sends its argument types only
// on the first execution (Connector/J server prepares, the C API, PHP
// mysqlnd) must have its later executions decoded with those types, not
// handed over as NULLs.
func TestSession_reexecutionWithoutTypesKeepsItsArguments(t *testing.T) {
	h := &stmtRecorder{params: 2}
	s := NewSession(nil, h)
	id := prepareStmt(t, s, "SELECT ? , ?")

	types := []byte{mysql.MYSQL_TYPE_LONG, 0, mysql.MYSQL_TYPE_VAR_STRING, 0}
	first := append(binary.LittleEndian.AppendUint32(nil, 2), 3, 'o', 'n', 'e')
	if v := s.dispatch(mysql.COM_STMT_EXECUTE, execPacket(id, 2, nil, types, first)); v == nil {
		t.Fatal("first execute: no answer")
	} else if err, ok := v.(error); ok {
		t.Fatalf("first execute: %v", err)
	}
	second := append(binary.LittleEndian.AppendUint32(nil, 3), 3, 't', 'w', 'o')
	if err, ok := s.dispatch(mysql.COM_STMT_EXECUTE, execPacket(id, 2, nil, nil, second)).(error); ok {
		t.Fatalf("second execute (types not re-sent): %v", err)
	}
	// A NULL in the bitmap on a re-execution is a NULL, and only that one.
	third := []byte{3, 's', 'i', 'x'}
	if err, ok := s.dispatch(mysql.COM_STMT_EXECUTE, execPacket(id, 2, []int{0}, nil, third)).(error); ok {
		t.Fatalf("third execute: %v", err)
	}
	str := func(v string) mysql.TypedBytes {
		return mysql.TypedBytes{Type: mysql.MYSQL_TYPE_VAR_STRING, Bytes: []byte(v)}
	}
	want := [][]any{
		{"ctx:SELECT ? , ?", "SELECT ? , ?", int32(2), str("one")},
		{"ctx:SELECT ? , ?", "SELECT ? , ?", int32(3), str("two")},
		{"ctx:SELECT ? , ?", "SELECT ? , ?", nil, str("six")},
	}
	if !reflect.DeepEqual(h.execs, want) {
		t.Errorf("the handler got\n  %#v\nwant\n  %#v", h.execs, want)
	}
}

func TestSession_statementLifecycle(t *testing.T) {
	h := &stmtRecorder{params: 1}
	s := NewSession(nil, h)

	// A refused prepare answers with the handler's own error and holds nothing.
	if err, _ := s.dispatch(mysql.COM_STMT_PREPARE, []byte("bad")).(error); mysqlErrCode(err) != mysql.ER_PARSE_ERROR {
		t.Errorf("refused prepare: %v, want 1064", err)
	}
	if len(s.stmts) != 0 {
		t.Error("a refused prepare left a statement behind")
	}

	a := prepareStmt(t, s, "a ?")
	b := prepareStmt(t, s, "b ?")
	if a == b || a == 0 {
		t.Fatalf("statement ids %d and %d", a, b)
	}
	tiny := []byte{mysql.MYSQL_TYPE_TINY, 0}

	// Types are per statement: b never sent any.
	if err, _ := s.dispatch(mysql.COM_STMT_EXECUTE, execPacket(a, 1, nil, tiny, []byte{7})).(error); err != nil {
		t.Fatal(err)
	}
	if err, _ := s.dispatch(mysql.COM_STMT_EXECUTE, execPacket(b, 1, nil, nil, []byte{7})).(error); mysqlErrCode(err) != mysql.ER_WRONG_ARGUMENTS {
		t.Errorf("an execution with no types ever sent: %v, want 1210", err)
	}
	// The handler's error reaches the writer as it is, code included.
	h.execErr = mysql.NewError(mysql.ER_PARSE_ERROR, "no")
	if v := s.dispatch(mysql.COM_STMT_EXECUTE, execPacket(a, 1, nil, nil, []byte{8})); v != error(h.execErr) {
		t.Errorf("the handler's error came back as %#v", v)
	}
	h.execErr = nil

	// Malformed and unknown. In order: the last one leaves LONGLONG as the
	// statement's types.
	for _, tc := range []struct {
		name string
		data []byte
		code uint16
	}{
		{"short packet", []byte{1, 0, 0}, 0},
		{"unknown statement", execPacket(99, 0, nil, nil, nil), mysql.ER_UNKNOWN_STMT_HANDLER},
		{"no bitmap", execPacket(a, 0, nil, nil, nil), 0},
		{"short types", execPacket(a, 1, nil, []byte{mysql.MYSQL_TYPE_TINY}, nil), 0},
		{"unknown type", execPacket(a, 1, nil, []byte{0x7f, 0}, []byte{1}), mysql.ER_WRONG_ARGUMENTS},
		{"cursor", append(binary.LittleEndian.AppendUint32(nil, a), 1, 1, 0, 0, 0, 0, 0), mysql.ER_UNKNOWN_ERROR},
		{"truncated value", execPacket(a, 1, nil, []byte{mysql.MYSQL_TYPE_LONGLONG, 0}, []byte{1, 2}), 0},
	} {
		before := len(h.execs)
		err, _ := s.dispatch(mysql.COM_STMT_EXECUTE, tc.data).(error)
		if err == nil || (tc.code != 0 && mysqlErrCode(err) != tc.code) || (tc.code == 0 && !errors.Is(err, mysql.ErrMalformPacket)) {
			t.Errorf("%s: %v", tc.name, err)
		}
		if len(h.execs) != before {
			t.Errorf("%s reached the handler", tc.name)
		}
	}
	// The types a client sent stay the statement's types even when that
	// execution's values did not decode: the next one is read under them.
	big := binary.LittleEndian.AppendUint64(nil, 5)
	if err, _ := s.dispatch(mysql.COM_STMT_EXECUTE, execPacket(a, 1, nil, nil, big)).(error); err != nil {
		t.Fatal(err)
	}
	if got := h.execs[len(h.execs)-1][2]; got != int64(5) {
		t.Errorf("argument = %#v, want int64 5", got)
	}

	// Close tells the handler, forgets the statement, and answers nothing.
	if v := s.dispatch(mysql.COM_STMT_CLOSE, binary.LittleEndian.AppendUint32(nil, a)); v != (noReply{}) {
		t.Errorf("close answered %#v", v)
	}
	if len(h.closed) != 1 || h.closed[0] != "ctx:a ?" {
		t.Errorf("closed = %v", h.closed)
	}
	if err, _ := s.dispatch(mysql.COM_STMT_EXECUTE, execPacket(a, 1, nil, tiny, []byte{1})).(error); mysqlErrCode(err) != mysql.ER_UNKNOWN_STMT_HANDLER {
		t.Errorf("execute after close: %v", err)
	}
	if v := s.dispatch(mysql.COM_STMT_CLOSE, binary.LittleEndian.AppendUint32(nil, 99)); v != (noReply{}) {
		t.Errorf("close of an unknown statement answered %#v", v)
	}
	// Reset answers OK for a known statement, an error for an unknown one.
	if _, ok := s.dispatch(mysql.COM_STMT_RESET, binary.LittleEndian.AppendUint32(nil, b)).(*mysql.Result); !ok {
		t.Error("reset did not answer OK")
	}
	if err, _ := s.dispatch(mysql.COM_STMT_RESET, binary.LittleEndian.AppendUint32(nil, 99)).(error); mysqlErrCode(err) != mysql.ER_UNKNOWN_STMT_HANDLER {
		t.Errorf("reset of an unknown statement: %v", err)
	}
}

// A value sent ahead in chunks is the parameter's value for the next
// execution, and only that one.
func TestSession_longData(t *testing.T) {
	h := &stmtRecorder{params: 2}
	s := NewSession(nil, h)
	id := prepareStmt(t, s, "q")
	chunk := func(param uint16, b string) []byte {
		p := binary.LittleEndian.AppendUint32(nil, id)
		p = binary.LittleEndian.AppendUint16(p, param)
		return append(p, b...)
	}
	for _, c := range [][]byte{chunk(1, "hel"), chunk(1, "lo"), chunk(9, "dropped"), {1}} {
		if v := s.dispatch(mysql.COM_STMT_SEND_LONG_DATA, c); v != (noReply{}) {
			t.Fatalf("long data answered %#v", v)
		}
	}
	if st := s.stmts[id]; len(st.long) != 1 || st.longBytes != 5 || s.longBytes != 5 {
		t.Errorf("held long data = %v (%d bytes, %d on the connection), want parameter 1 alone, 5 bytes", st.long, st.longBytes, s.longBytes)
	}
	types := []byte{mysql.MYSQL_TYPE_TINY, 0, mysql.MYSQL_TYPE_BLOB, 0}
	if err, _ := s.dispatch(mysql.COM_STMT_EXECUTE, execPacket(id, 2, nil, types, []byte{4})).(error); err != nil {
		t.Fatal(err)
	}
	if got := h.execs[0][3]; !reflect.DeepEqual(got, mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("hello")}) {
		t.Errorf("long-data argument = %#v", got)
	}
	// The next execution carries its own value again.
	if err, _ := s.dispatch(mysql.COM_STMT_EXECUTE, execPacket(id, 2, nil, nil, []byte{5, 2, 'h', 'i'})).(error); err != nil {
		t.Fatal(err)
	}
	if got := h.execs[1][3]; !reflect.DeepEqual(got, mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("hi")}) {
		t.Errorf("argument after a long-data execution = %#v", got)
	}
	if s.longBytes != 0 {
		t.Errorf("the connection still counts %d bytes of long data", s.longBytes)
	}

	// Long data never outlives the attempt it was sent for: a refused
	// execution (here a cursor), a reset and a close all drop it, so the
	// next execution reads its own inline value.
	cursor := append(binary.LittleEndian.AppendUint32(nil, id), 1, 1, 0, 0, 0, 0, 0)
	reset := binary.LittleEndian.AppendUint32(nil, id)
	for name, drop := range map[string]func(){
		"refused execution": func() { s.dispatch(mysql.COM_STMT_EXECUTE, cursor) },
		"reset":             func() { s.dispatch(mysql.COM_STMT_RESET, reset) },
	} {
		s.dispatch(mysql.COM_STMT_SEND_LONG_DATA, chunk(1, "stale"))
		drop()
		if st := s.stmts[id]; len(st.long) != 0 || st.longBytes != 0 || s.longBytes != 0 {
			t.Errorf("after a %s the statement still holds %v (%d bytes on the connection)", name, st.long, s.longBytes)
		}
		if err, _ := s.dispatch(mysql.COM_STMT_EXECUTE, execPacket(id, 2, nil, nil, []byte{6, 3, 'n', 'e', 'w'})).(error); err != nil {
			t.Fatalf("execution after a %s: %v", name, err)
		}
		if got := h.execs[len(h.execs)-1][3]; !reflect.DeepEqual(got, mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("new")}) {
			t.Errorf("argument after a %s = %#v, want the inline value", name, got)
		}
	}
	s.dispatch(mysql.COM_STMT_SEND_LONG_DATA, chunk(1, "stale"))
	s.dispatch(mysql.COM_STMT_CLOSE, reset)
	if s.longBytes != 0 {
		t.Errorf("a closed statement left %d bytes counted on the connection", s.longBytes)
	}
}

// Long data is a string whatever type the client declared for the
// parameter: relayed under an integer or a temporal type, the source would
// read its bytes as that type and misplace every argument after it.
func TestSession_longDataIsAStringWhateverTheDeclaredType(t *testing.T) {
	h := &stmtRecorder{params: 2}
	s := NewSession(nil, h)
	id := prepareStmt(t, s, "q")
	chunk := append(binary.LittleEndian.AppendUint16(binary.LittleEndian.AppendUint32(nil, id), 0), "12345"...)
	s.dispatch(mysql.COM_STMT_SEND_LONG_DATA, chunk)
	types := []byte{mysql.MYSQL_TYPE_LONG, 0, mysql.MYSQL_TYPE_VAR_STRING, 0}
	if err, _ := s.dispatch(mysql.COM_STMT_EXECUTE, execPacket(id, 2, nil, types, []byte{1, 'x'})).(error); err != nil {
		t.Fatal(err)
	}
	want := []any{mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("12345")}, mysql.TypedBytes{Type: mysql.MYSQL_TYPE_VAR_STRING, Bytes: []byte("x")}}
	if got := h.execs[0][2:]; !reflect.DeepEqual(got, want) {
		t.Errorf("arguments = %#v, want %#v", got, want)
	}
}

// The long data a connection may hold is bounded across all its statements.
// Data past the bound is not kept, and the execution it belongs to is
// refused rather than run with a cut value; the one after is clean.
func TestSession_longDataIsBounded(t *testing.T) {
	old := maxLongData
	maxLongData = 8
	defer func() { maxLongData = old }()
	h := &stmtRecorder{params: 1}
	s := NewSession(nil, h)
	a, b := prepareStmt(t, s, "a"), prepareStmt(t, s, "b")
	chunk := func(id uint32) []byte {
		return append(binary.LittleEndian.AppendUint16(binary.LittleEndian.AppendUint32(nil, id), 0), "12345"...)
	}
	s.dispatch(mysql.COM_STMT_SEND_LONG_DATA, chunk(a))
	// The second statement's one chunk would take the CONNECTION past the
	// bound, though the statement alone is under it.
	s.dispatch(mysql.COM_STMT_SEND_LONG_DATA, chunk(b))
	// A later chunk that would fit is not kept either: the value is already
	// cut, and its execution will be refused.
	s.dispatch(mysql.COM_STMT_SEND_LONG_DATA, append(binary.LittleEndian.AppendUint16(binary.LittleEndian.AppendUint32(nil, b), 0), 'x'))
	if s.longBytes != 5 || len(s.stmts[b].long[0]) != 0 {
		t.Errorf("the connection holds %d bytes, statement b %d; want a's 5 alone", s.longBytes, len(s.stmts[b].long[0]))
	}
	types := []byte{mysql.MYSQL_TYPE_BLOB, 0}
	if err, _ := s.dispatch(mysql.COM_STMT_EXECUTE, execPacket(b, 1, nil, types, nil)).(error); mysqlErrCode(err) != mysql.ER_NET_PACKET_TOO_LARGE {
		t.Errorf("execution after oversized long data: %v, want 1153", err)
	}
	if len(h.execs) != 0 {
		t.Error("the oversized execution reached the handler")
	}
	if err, _ := s.dispatch(mysql.COM_STMT_EXECUTE, execPacket(b, 1, nil, types, []byte{2, 'o', 'k'})).(error); err != nil {
		t.Errorf("the execution after it: %v", err)
	}
	// Statement a's data, under the bound, is intact.
	if err, _ := s.dispatch(mysql.COM_STMT_EXECUTE, execPacket(a, 1, nil, types, nil)).(error); err != nil {
		t.Fatal(err)
	}
	if got := h.execs[len(h.execs)-1][2]; !reflect.DeepEqual(got, mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("12345")}) {
		t.Errorf("statement a ran with %#v", got)
	}
	if s.longBytes != 0 {
		t.Errorf("%d bytes still counted", s.longBytes)
	}
}

// A connection may keep only so many statements open.
func TestSession_statementsAreBounded(t *testing.T) {
	old := maxSessionStmts
	maxSessionStmts = 2
	defer func() { maxSessionStmts = old }()
	h := &stmtRecorder{}
	s := NewSession(nil, h)
	a := prepareStmt(t, s, "a")
	prepareStmt(t, s, "b")
	if err, _ := s.dispatch(mysql.COM_STMT_PREPARE, []byte("c")).(error); mysqlErrCode(err) != mysql.ER_MAX_PREPARED_STMT_COUNT_REACHED {
		t.Fatalf("third prepare: %v, want 1461", err)
	}
	s.dispatch(mysql.COM_STMT_CLOSE, binary.LittleEndian.AppendUint32(nil, a))
	prepareStmt(t, s, "c")
}

// Every binary-protocol type decodes to the Go value go-mysql's server
// produces for it (what sqlLiteral is written against).
func TestDecodeStmtArgs_types(t *testing.T) {
	u := mysql.PARAM_UNSIGNED
	le := binary.LittleEndian
	types := []byte{
		mysql.MYSQL_TYPE_TINY, 0, mysql.MYSQL_TYPE_TINY, byte(u),
		mysql.MYSQL_TYPE_SHORT, 0, mysql.MYSQL_TYPE_YEAR, byte(u),
		mysql.MYSQL_TYPE_LONG, 0, mysql.MYSQL_TYPE_INT24, byte(u),
		mysql.MYSQL_TYPE_LONGLONG, 0, mysql.MYSQL_TYPE_LONGLONG, byte(u),
		mysql.MYSQL_TYPE_FLOAT, 0, mysql.MYSQL_TYPE_DOUBLE, 0,
		mysql.MYSQL_TYPE_NULL, 0, mysql.MYSQL_TYPE_DATETIME, 0,
		mysql.MYSQL_TYPE_NEWDECIMAL, 0, mysql.MYSQL_TYPE_VAR_STRING, 0,
	}
	var v []byte
	v = append(v, 0xfb, 0xfb)
	v = le.AppendUint16(v, 0xfffe)
	v = le.AppendUint16(v, 2026)
	v = le.AppendUint32(v, 0xfffffffd)
	v = le.AppendUint32(v, 4000000000)
	v = le.AppendUint64(v, math.MaxUint64)
	v = le.AppendUint64(v, math.MaxUint64)
	v = le.AppendUint32(v, math.Float32bits(1.5))
	v = le.AppendUint64(v, math.Float64bits(-2.25))
	v = append(v, 4, 0xea, 0x07, 10, 4)
	v = append(v, 5, '-', '1', '.', '5', '0')
	v = append(v, 0)
	args := make([]any, 14)
	if err := decodeStmtArgs(args, []byte{0, 0}, types, v, nil); err != nil {
		t.Fatal(err)
	}
	want := []any{
		int8(-5), uint8(0xfb), int16(-2), uint16(2026), int32(-3), uint32(4000000000),
		int64(-1), uint64(math.MaxUint64), float32(1.5), -2.25, nil,
		mysql.TypedBytes{Type: mysql.MYSQL_TYPE_DATETIME, Bytes: []byte{0xea, 0x07, 10, 4}},
		mysql.TypedBytes{Type: mysql.MYSQL_TYPE_NEWDECIMAL, Bytes: []byte("-1.50")},
		mysql.TypedBytes{Type: mysql.MYSQL_TYPE_VAR_STRING, Bytes: []byte{}},
	}
	if !reflect.DeepEqual(args, want) {
		t.Errorf("decoded\n  %#v\nwant\n  %#v", args, want)
	}
}

// The non-statement commands are answered as go-mysql answers them.
func TestSession_otherCommands(t *testing.T) {
	h := NewHandler(nil, nil)
	s := NewSession(nil, h)
	if v := s.dispatch(mysql.COM_PING, nil); v != nil {
		t.Errorf("ping answered %#v", v)
	}
	if v := s.dispatch(mysql.COM_INIT_DB, []byte("shop")); v != nil {
		t.Errorf("init db answered %#v", v)
	}
	if h.db != "shop" {
		t.Errorf("db = %q", h.db)
	}
	if _, ok := s.dispatch(mysql.COM_QUERY, []byte("SET NAMES utf8mb4")).(*mysql.Result); !ok {
		t.Error("a query did not answer with a result")
	}
	if _, ok := s.dispatch(mysql.COM_QUERY, []byte("DELETE FROM t")).(error); !ok {
		t.Error("a refused query did not answer with an error")
	}
	if _, ok := s.dispatch(0x7e, nil).(error); !ok {
		t.Error("an unknown command did not answer with an error")
	}
}
