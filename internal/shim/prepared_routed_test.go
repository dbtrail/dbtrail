package shim

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/readrouter"
)

func strArg(s string) mysql.TypedBytes {
	return mysql.TypedBytes{Type: mysql.MYSQL_TYPE_VAR_STRING, Bytes: []byte(s)}
}

// binaryFirstCell reads row 0, column 0 of a prepared statement's answer.
func binaryFirstCell(t *testing.T, res *mysql.Result) string {
	t.Helper()
	if res == nil || res.Resultset == nil || len(res.RowDatas) == 0 {
		t.Fatalf("no rows in %+v", res)
	}
	vals, err := res.RowDatas[0].ParseBinary(res.Fields, nil)
	if err != nil {
		t.Fatalf("the row is not binary-encoded: %v", err)
	}
	if b, ok := vals[0].Value().([]byte); ok {
		return string(b)
	}
	t.Fatalf("first cell = %#v", vals[0].Value())
	return ""
}

// The routing ladder for a prepared statement, one rung per case. On every
// rung but the copy's the statement prepared on the source is executed with
// the arguments as they arrived: no text is built for MySQL.
func TestPreparedRouted_ladder(t *testing.T) {
	fresh := time.Now().Add(-10 * time.Second)
	args := []any{strArg(`it's a\b`), int64(-3)}
	cases := []struct {
		name       string
		stmt       string
		router     fakeRouter
		copyAge    time.Time
		maxAge     time.Duration
		copyErr    error
		wantSide   string
		wantDecide bool
		wantReason RouteReason
	}{
		{"a write", "UPDATE t SET a = ? WHERE id = ?", fakeRouter{toCopy: true}, fresh, time.Minute, nil, "mysql", false, RouteReasonWrite},
		{"not a select", "SHOW COLUMNS FROM t WHERE a = ? OR b = ?", fakeRouter{toCopy: true}, fresh, time.Minute, nil, "mysql", false, RouteReasonNotASelect},
		{"vetoed", "SELECT NOW(), ?, ?", fakeRouter{toCopy: true}, fresh, time.Minute, nil, "mysql", false, RouteReasonVeto},
		// The vetoes of #2122 read the template, placeholders and all.
		{"vetoed: ~", "SELECT ~?, ?", fakeRouter{toCopy: true}, fresh, time.Minute, nil, "mysql", false, RouteReasonVeto},
		{"vetoed: hexadecimal literal", "SELECT count(*) FROM t WHERE a = ? AND k = 0x10 AND b > ?", fakeRouter{toCopy: true}, fresh, time.Minute, nil, "mysql", false, RouteReasonVeto},
		{"vetoed: hexadecimal string", "SELECT count(*) FROM t WHERE a = ? AND k = x'41' AND b > ?", fakeRouter{toCopy: true}, fresh, time.Minute, nil, "mysql", false, RouteReasonVeto},
		{"vetoed: two-part interval unit", "SELECT count(*) FROM t WHERE d > ? + INTERVAL ? MINUTE_SECOND", fakeRouter{toCopy: true}, fresh, time.Minute, nil, "mysql", false, RouteReasonVeto},
		{"vetoed: date arithmetic", "SELECT count(*) FROM t WHERE d > CAST(? AS DATE) + ?", fakeRouter{toCopy: true}, fresh, time.Minute, nil, "mysql", false, RouteReasonVeto},
		// What the copy refuses (#2114): kept on MySQL from the template, so
		// no plan is asked for and the copy is not tried.
		{"vetoed: LIMIT offset, count", "SELECT a, count(*) FROM t WHERE b = ? GROUP BY a ORDER BY a LIMIT ?, 20", fakeRouter{toCopy: true}, fresh, time.Minute, nil, "mysql", false, RouteReasonVeto},
		{"vetoed: ORDER BY NULL", "SELECT a, count(*) FROM t WHERE b = ? AND c > ? GROUP BY a ORDER BY NULL", fakeRouter{toCopy: true}, fresh, time.Minute, nil, "mysql", false, RouteReasonVeto},
		{"vetoed: _binary string", "SELECT count(*) FROM t WHERE a = ? AND b > ? AND k = _binary'x'", fakeRouter{toCopy: true}, fresh, time.Minute, nil, "mysql", false, RouteReasonVeto},
		{"in transaction", "SELECT count(*) FROM t WHERE a = ? AND b > ?", fakeRouter{toCopy: true, inTxn: true}, fresh, time.Minute, nil, "mysql", false, RouteReasonInTransaction},
		{"routing off", "SELECT count(*) FROM t WHERE a = ? AND b > ?", fakeRouter{toCopy: true}, fresh, 0, nil, "mysql", false, RouteReasonRoutingOff},
		{"cheap plan", "SELECT * FROM t WHERE a = ? AND id = ?", fakeRouter{toCopy: false, reason: "cheap"}, fresh, time.Minute, nil, "mysql", true, RouteReasonCheapPlan},
		{"bounded limit", "SELECT * FROM t WHERE a = ? AND id > ? LIMIT 5", fakeRouter{toCopy: false, rule: readrouter.RuleBoundedLimit}, fresh, time.Minute, nil, "mysql", true, RouteReasonBoundedLimit},
		{"explain failed", "SELECT * FROM t WHERE a = ? AND id = ?", fakeRouter{toCopy: true, decideErr: errors.New("boom")}, fresh, time.Minute, nil, "mysql", true, RouteReasonExplainFailed},
		{"copy age unknown", "SELECT count(*) FROM t WHERE a = ? AND b > ?", fakeRouter{toCopy: true}, time.Time{}, time.Minute, nil, "mysql", true, RouteReasonCopyAgeUnknown},
		{"copy too old", "SELECT count(*) FROM t WHERE a = ? AND b > ?", fakeRouter{toCopy: true}, time.Now().Add(-time.Hour), time.Minute, nil, "mysql", true, RouteReasonCopyTooOld},
		{"copy refused", "SELECT count(*) FROM t WHERE a = ? AND b > ?", fakeRouter{toCopy: true}, fresh, time.Minute, errors.New("Binder Error"), "mysql", true, RouteReasonCopyRefused},
		{"expensive plan", "SELECT count(*) FROM t WHERE a = ? AND b > 5-?", fakeRouter{toCopy: true, reason: "plan cost 20146 >= 10000"}, fresh, time.Minute, nil, "copy", true, RouteReasonExpensivePlan},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.router
			f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: tc.copyAge, err: tc.copyErr}
			h := routingHandler(t, &r, f, tc.maxAge)
			var observed []string
			h.routerCfg.Observe = func(side RouteSide, reason RouteReason) {
				observed = append(observed, string(side)+"/"+string(reason))
			}
			params, cols, ctx, err := h.HandleStmtPrepare(tc.stmt)
			if err != nil {
				t.Fatal(err)
			}
			if len(r.prepared) != 1 || r.prepared[0].query != tc.stmt {
				t.Fatalf("the source was asked to prepare %v", r.prepared)
			}
			up := r.prepared[0]
			if params != 2 || cols != 1 {
				t.Errorf("prepare = %d params, %d columns; want the source's 2 and 1", params, cols)
			}
			res, err := h.HandleStmtExecute(ctx, "", args)
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			if got := binaryFirstCell(t, res); got != tc.wantSide {
				t.Errorf("answered by %s, want %s", got, tc.wantSide)
			}
			if tc.wantSide == "mysql" {
				if len(up.executed) != 1 || !reflect.DeepEqual(up.executed[0], args) {
					t.Errorf("the source executed with %v, want the arguments as they arrived", up.executed)
				}
			} else if len(up.executed) != 0 {
				t.Errorf("a copy-served execution also ran on the source: %v", up.executed)
			}
			if want := []string{tc.wantSide + "/" + string(tc.wantReason)}; !reflect.DeepEqual(observed, want) {
				t.Errorf("observed %v, want exactly %v", observed, want)
			}
			if got := len(up.decided) == 1; got != tc.wantDecide {
				t.Errorf("plan asked = %v, want %v", got, tc.wantDecide)
			}
			if tc.wantDecide && !reflect.DeepEqual(up.decided[0], args) {
				t.Errorf("the plan was asked with %v, want this execution's arguments", up.decided[0])
			}
			// No text statement ever reaches MySQL for a prepared one.
			if len(r.forwarded)+len(r.explained) != 0 {
				t.Errorf("text reached the source: forwarded %q, explained %q", r.forwarded, r.explained)
			}
			if tc.wantSide == "copy" || tc.copyErr != nil {
				// The copy's text: its own quoting (a quote doubled, the
				// backslash a character), a negative number kept off the
				// minus before it.
				if !strings.Contains(squash(f.gotStmt), `a = 'it''s a\b'`) || strings.Contains(f.gotStmt, "--") {
					t.Errorf("the copy got %s", f.gotStmt)
				}
			} else if f.calls != f.unchangedAsks {
				// The one MySQL rung that asks the copy first: a snapshot past
				// the limit, asked for an answer only over unchanged tables
				// (#2085), which this copy does not give.
				t.Errorf("the copy was asked on a MySQL rung: %s", f.gotStmt)
			}
			if wantAsks := map[bool]int{true: 1}[tc.wantReason == RouteReasonCopyTooOld]; f.unchangedAsks != wantAsks {
				t.Errorf("the copy was asked for unchanged tables %d times, want %d", f.unchangedAsks, wantAsks)
			}
			if err := h.HandleStmtClose(ctx); err != nil || up.closed != 1 {
				t.Errorf("close: %v, the source's statement closed %d times", err, up.closed)
			}
		})
	}
}

// The source counting other placeholders than the port did means the port
// cannot write the statement out for the copy: it stays on MySQL.
func TestPreparedRouted_placeholderMismatchStaysOnMySQL(t *testing.T) {
	r := &fakeRouter{toCopy: true, paramsOverride: 3}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Minute)
	params, _, ctx, err := h.HandleStmtPrepare("SELECT count(*) FROM t WHERE a = ?")
	if err != nil || params != 3 {
		t.Fatalf("prepare = %d, %v; want the source's count", params, err)
	}
	res, err := h.HandleStmtExecute(ctx, "", []any{int64(1), int64(2), int64(3)})
	if err != nil {
		t.Fatal(err)
	}
	if got := binaryFirstCell(t, res); got != "mysql" || f.calls != 0 {
		t.Errorf("answered by %s with %d copy calls, want mysql and none", got, f.calls)
	}
	if n := len(r.prepared[0].decided); n != 0 {
		t.Errorf("the plan was asked %d times for a statement that can never go to the copy", n)
	}
}

// SHOW WARNINGS, prepared: after a copy-served statement the warnings are
// the port's own; after a forwarded one, the source's.
func TestPreparedRouted_showWarnings(t *testing.T) {
	r := &fakeRouter{toCopy: true}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Minute)
	_, _, sel, err := h.HandleStmtPrepare("SELECT count(*) FROM t WHERE a = ?")
	if err != nil {
		t.Fatal(err)
	}
	_, _, sw, err := h.HandleStmtPrepare("SHOW WARNINGS")
	if err != nil {
		t.Fatal(err)
	}
	up := r.prepared[1]
	if _, err := h.HandleStmtExecute(sel, "", []any{int64(1)}); err != nil {
		t.Fatal(err)
	}
	res, err := h.HandleStmtExecute(sw, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(up.executed) != 0 {
		t.Error("after a copy-served statement SHOW WARNINGS was asked of the source")
	}
	if res == nil || res.Resultset == nil || len(res.Fields) != 3 {
		t.Errorf("SHOW WARNINGS answer = %+v, want the port's three columns", res)
	}
	// After a forwarded statement, the source answers.
	r.toCopy = false
	if _, err := h.HandleStmtExecute(sel, "", []any{int64(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(sw, "", nil); err != nil {
		t.Fatal(err)
	}
	if len(up.executed) != 1 {
		t.Errorf("after a forwarded statement SHOW WARNINGS reached the source %d times, want 1", len(up.executed))
	}
}

func TestPreparedRouted_errorsAndTimeTravel(t *testing.T) {
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}

	// The source's refusal to prepare is the client's answer.
	refuse := mysql.NewError(mysql.ER_PARSE_ERROR, "You have an error in your SQL syntax")
	h := routingHandler(t, &fakeRouter{prepareErr: refuse}, f, time.Minute)
	if _, _, _, err := h.HandleStmtPrepare("SELEC ?"); err != error(refuse) {
		t.Errorf("prepare error = %v, want the source's own", err)
	}

	// So is its refusal to execute; nothing else is tried.
	r := &fakeRouter{forwardErr: mysql.NewError(mysql.ER_DUP_ENTRY, "Duplicate entry")}
	h = routingHandler(t, r, f, time.Minute)
	_, _, ctx, err := h.HandleStmtPrepare("INSERT INTO t VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{int64(1)}); mysqlErrCode(err) != mysql.ER_DUP_ENTRY {
		t.Errorf("execute error = %v, want the source's 1062", err)
	}
	if f.calls != 0 {
		t.Error("a failed write reached the copy")
	}

	// A lost source is 2006 at prepare too.
	lost := mysql.NewError(readrouter.CodeUpstreamLost, "gone")
	h = routingHandler(t, &fakeRouter{prepareErr: lost}, f, time.Minute)
	if _, _, _, err := h.HandleStmtPrepare("SELECT ?"); !readrouter.IsLost(err) {
		t.Errorf("prepare on a lost source = %v, want 2006", err)
	}

	// A time-travel shape is the port's own under routing too: a template,
	// never prepared on the source.
	r = &fakeRouter{}
	h = routingHandler(t, r, f, time.Minute)
	params, _, ctx, err := h.HandleStmtPrepare("SELECT * FROM _flashback.orders AS OF ? WHERE id = ?")
	if err != nil || params != 2 {
		t.Fatalf("prepare = %d, %v", params, err)
	}
	if len(r.prepared) != 0 {
		t.Error("a time-travel shape was prepared on the source")
	}
	if st := ctx.(*preparedStmt); st.up != nil || !st.mysqlEscapes {
		t.Errorf("time-travel statement = %+v, want a template with MySQL escapes", st)
	}
}

// A session relays the source's own definitions in the PREPARE answer.
func TestPreparedRouted_sessionRelaysTheSourcesDefinitions(t *testing.T) {
	r := &fakeRouter{}
	h := routingHandler(t, r, &fakeFreeSQL{}, time.Minute)
	s := NewSession(nil, h)
	st, ok := s.dispatch(mysql.COM_STMT_PREPARE, []byte("SELECT a FROM t WHERE id = ?")).(*server.Stmt)
	if !ok {
		t.Fatal("prepare did not answer with a statement")
	}
	if st.Params != 1 || st.Columns != 1 {
		t.Errorf("prepare = %d params, %d columns", st.Params, st.Columns)
	}
	if len(st.RawParamFields) != 1 || string(st.RawParamFields[0]) != "param-def" ||
		len(st.RawColumnFields) != 1 || string(st.RawColumnFields[0]) != "column-def" {
		t.Errorf("definitions = %q / %q, want the source's own", st.RawParamFields, st.RawColumnFields)
	}
	// A template (no source statement) has none to relay.
	tt, ok := s.dispatch(mysql.COM_STMT_PREPARE, []byte("SELECT * FROM _flashback.t AS OF ? WHERE id = ?")).(*server.Stmt)
	if !ok || tt.RawParamFields != nil || tt.RawColumnFields != nil {
		t.Errorf("a template answered with definitions: %+v", tt)
	}
}

// A forwarded prepared statement's rows go out in the binary encoding, under
// the source's column types.
func TestStreamWriter_binaryRows(t *testing.T) {
	pw := &capturePW{}
	w := newStreamWriterFields(pw, nil)
	w.binary = true
	fields := []*mysql.Field{
		{Name: []byte("id"), Type: mysql.MYSQL_TYPE_LONGLONG},
		{Name: []byte("u"), Type: mysql.MYSQL_TYPE_LONG, Flag: mysql.UNSIGNED_FLAG},
		{Name: []byte("f"), Type: mysql.MYSQL_TYPE_FLOAT},
		{Name: []byte("d"), Type: mysql.MYSQL_TYPE_DOUBLE},
		{Name: []byte("day"), Type: mysql.MYSQL_TYPE_DATE},
		{Name: []byte("ts"), Type: mysql.MYSQL_TYPE_DATETIME},
		{Name: []byte("dur"), Type: mysql.MYSQL_TYPE_TIME},
		{Name: []byte("dec"), Type: mysql.MYSQL_TYPE_NEWDECIMAL},
		{Name: []byte("txt"), Type: mysql.MYSQL_TYPE_VAR_STRING},
		{Name: []byte("nul"), Type: mysql.MYSQL_TYPE_LONG},
	}
	if err := w.Header(fields); err != nil {
		t.Fatal(err)
	}
	row := []any{int64(-42), uint64(4000000000), float64(float32(1.1)), -2.25, []byte("2026-10-04"), []byte("2026-10-04 13:05:09.250000"), []byte("-26:03:04"), []byte("12.50"), []byte(""), nil}
	if err := w.Row(row); err != nil {
		t.Fatal(err)
	}
	data := mysql.RowData(pw.packets[len(pw.packets)-1])
	vals, err := data.ParseBinary(fields, nil)
	if err != nil {
		t.Fatalf("the row does not parse as binary: %v", err)
	}
	want := []any{int64(-42), uint64(4000000000), float64(float32(1.1)), -2.25, "2026-10-04", "2026-10-04 13:05:09.250000", "-26:03:04", "12.50", "", nil}
	for c := range vals {
		got := vals[c].Value()
		if b, ok := got.([]byte); ok {
			got = string(b)
		}
		if got != want[c] {
			t.Errorf("column %s = %#v, want %#v", fields[c].Name, got, want[c])
		}
	}
	// A cell that cannot be encoded under the announced type is an error,
	// not a guess; so is a row of the wrong width.
	if err := w.Row([]any{[]byte("x"), nil, nil, nil, nil, nil, nil, nil, nil, nil}); err == nil {
		t.Error("text under a BIGINT column was written")
	}
	if err := w.Row([]any{int64(1)}); err == nil {
		t.Error("a short row was written")
	}
}

// What the copy would read differently than MySQL stays on MySQL: a
// placeholder flush against its neighbours, a database changed since the
// prepare, a string that spells a non-integer number.
func TestPreparedRouted_copyReadsTheSameStatement(t *testing.T) {
	fresh := time.Now()
	newH := func() (*Handler, *fakeRouter, *fakeFreeSQL) {
		r := &fakeRouter{toCopy: true}
		f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: fresh}
		h := routingHandler(t, r, f, time.Minute)
		if err := h.UseDB("shop"); err != nil {
			t.Fatal(err)
		}
		return h, r, f
	}
	run := func(h *Handler, ctx any, args ...any) string {
		t.Helper()
		res, err := h.HandleStmtExecute(ctx, "", args)
		if err != nil {
			t.Fatal(err)
		}
		return binaryFirstCell(t, res)
	}

	// A literal is its own token, as the placeholder was.
	h, _, f := newH()
	_, _, ctx, err := h.HandleStmtPrepare("SELECT ?e1 FROM t LIMIT?")
	if err != nil {
		t.Fatal(err)
	}
	if got := run(h, ctx, int64(5), int64(7)); got != "copy" {
		t.Fatalf("answered by %s", got)
	}
	if want := "SELECT 5 e1 FROM t LIMIT 7"; squash(f.gotStmt) != want {
		t.Errorf("the copy got %q, want %q", f.gotStmt, want)
	}

	// A float argument is the double MySQL widens it to.
	_, _, ctx, err = h.HandleStmtPrepare("SELECT count(*) FROM t WHERE d = ?")
	if err != nil {
		t.Fatal(err)
	}
	run(h, ctx, float32(0.1))
	if !strings.Contains(f.gotStmt, "0.10000000149011612") {
		t.Errorf("the copy got %q, want the widened float", f.gotStmt)
	}

	// The database changed since PREPARE: the source still runs it in the
	// old one, so the copy must not answer from the new one.
	if err := h.UseDB("other"); err != nil {
		t.Fatal(err)
	}
	calls := f.calls
	if got := run(h, ctx, float64(1)); got != "mysql" || f.calls != calls {
		t.Errorf("after USE the statement was answered by %s (%d copy calls)", got, f.calls-calls)
	}
	if err := h.UseDB("shop"); err != nil {
		t.Fatal(err)
	}
	if got := run(h, ctx, float64(1)); got != "copy" {
		t.Errorf("back in the prepare-time database it was answered by %s, want copy", got)
	}

	// A string spelling a non-integer number: MySQL compares it as a number.
	_, _, ctx, err = h.HandleStmtPrepare("SELECT count(*) FROM t WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	for arg, want := range map[string]string{"12.7": "mysql", " 1e2 ": "mysql", "13": "copy", "-4": "copy", "paid": "copy", "": "copy"} {
		if got := run(h, ctx, strArg(arg)); got != want {
			t.Errorf("string argument %q answered by %s, want %s", arg, got, want)
		}
	}
	// A DECIMAL argument is a number on both sides.
	if got := run(h, ctx, mysql.TypedBytes{Type: mysql.MYSQL_TYPE_NEWDECIMAL, Bytes: []byte("12.7")}); got != "copy" {
		t.Errorf("a DECIMAL argument was answered by %s, want copy", got)
	}
}

// A prepared statement's backtick-quoted names reach the copy in double
// quotes, like a text statement's (#2081). The arguments are written in
// AFTER the rewrite and never rescanned: they are spelled the copy's way,
// where a backslash is a character, and a scanner reading MySQL's escapes
// would take the quote after it for part of the string and rewrite a
// backtick inside the next argument.
func TestPreparedRouted_backtickNames(t *testing.T) {
	r := &fakeRouter{toCopy: true}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Minute)
	if err := h.UseDB("shop"); err != nil {
		t.Fatal(err)
	}
	const stmt = "SELECT `o`.`id` FROM `orders` `o` WHERE `o`.`path` = ? AND `o`.`note` = ? AND `o`.`tag` = 'a `b` ?' ORDER BY `o`.`id`"
	n, _, ctx, err := h.HandleStmtPrepare(stmt)
	if err != nil || n != 2 {
		t.Fatalf("prepare: %d params, err %v", n, err)
	}
	if r.prepared[0].query != stmt {
		t.Errorf("the source prepared %q, want the client's text", r.prepared[0].query)
	}
	res, err := h.HandleStmtExecute(ctx, "", []any{strArg(`C:\`), strArg("a `name` b")})
	if err != nil {
		t.Fatal(err)
	}
	if got := binaryFirstCell(t, res); got != "copy" {
		t.Fatalf("answered by %s, want the copy", got)
	}
	want := "SELECT \"o\".\"id\" FROM \"orders\" \"o\" WHERE \"o\".\"path\" = 'C:\\' AND \"o\".\"note\" = 'a `name` b' AND \"o\".\"tag\" = 'a `b` ?' ORDER BY \"o\".\"id\""
	if squash(f.gotStmt) != want {
		t.Errorf("the copy got\n %q\nwant\n %q", squash(f.gotStmt), want)
	}

	// A template the rewrite refuses is MySQL's on every execution.
	for _, refused := range []string{
		"SELECT `a``b` FROM `t` WHERE `c` = ?",
		"SELECT `sum`(`a`) FROM `t` WHERE `c` = ?",
		"SELECT `a` AS `x\"y` FROM `t` WHERE `c` = ?",
	} {
		_, _, ctx, err := h.HandleStmtPrepare(refused)
		if err != nil {
			t.Fatal(err)
		}
		calls := f.calls
		res, err := h.HandleStmtExecute(ctx, "", []any{int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		if got := binaryFirstCell(t, res); got != "mysql" || f.calls != calls {
			t.Errorf("%q: answered by %s with %d copy calls, want mysql and none", refused, got, f.calls-calls)
		}
	}
}

// A statement whose template could not be written for the copy never
// reaches it, even if the ladder's own veto were to let it through.
func TestPreparedRouted_copyRefusalKeepsItOnMySQL(t *testing.T) {
	r := &fakeRouter{toCopy: true}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Minute)
	if err := h.UseDB("shop"); err != nil {
		t.Fatal(err)
	}
	_, _, ctx, err := h.HandleStmtPrepare("SELECT a FROM t WHERE c = ?")
	if err != nil {
		t.Fatal(err)
	}
	ctx.(*preparedStmt).copyRefusal = "not written for the copy"
	res, err := h.HandleStmtExecute(ctx, "", []any{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if got := binaryFirstCell(t, res); got != "mysql" || f.calls != 0 {
		t.Errorf("answered by %s with %d copy calls, want mysql and none", got, f.calls)
	}
}

// copyParts and the ladder's veto are two readers of one template: whatever
// one refuses the other must, or a template would be vetoed on one path and
// written out on the other.
func TestCopyParts_agreesWithTheVeto(t *testing.T) {
	for _, stmt := range []string{
		"SELECT `a` FROM `t` WHERE `b` = ? AND `c` = ?",
		"SELECT `a``b` FROM `t` WHERE `c` = ?",
		"SELECT `a` FROM `t` WHERE `c` = ? AND `d` = \"x\"",
		"SELECT `a` FROM `t` WHERE `c` = ? AND `d` = 'x\\n'",
		"SELECT `f`(?) FROM `t`",
		"SELECT `a` FROM `t` WHERE `c` = ? /* x /* y */",
		"SELECT `a` FROM `t` WHERE `c` = ? AND `d` = 'open",
		"SELECT /*+ MAX_EXECUTION_TIME(1) */ `a` FROM `t` WHERE `c` = ?",
	} {
		parts := splitPlaceholders(stmt, true)
		out, refusal := copyParts(parts)
		_, why := readrouter.ForCopy(stmt)
		if (refusal == "") != (why == "") {
			t.Errorf("%q: copyParts refusal %q, ForCopy refusal %q", stmt, refusal, why)
		}
		if refusal == "" {
			whole, _ := readrouter.ForCopy(stmt)
			if got := strings.Join(out, "?"); got != whole {
				t.Errorf("%q: pieces rewrite to %q, the whole statement to %q", stmt, got, whole)
			}
		}
	}
}

// Text and prepared statements go down ONE ladder: the same statement, on
// the same connection state, is observed the same way on both paths.
func TestPreparedRouted_sameLadderAsText(t *testing.T) {
	fresh := time.Now().Add(-10 * time.Second)
	for _, tc := range []struct {
		name   string
		stmt   string
		router fakeRouter
		age    time.Time
		maxAge time.Duration
	}{
		{"write", "UPDATE t SET a = 1", fakeRouter{toCopy: true}, fresh, time.Minute},
		{"set", "SET sql_mode = 'ANSI'", fakeRouter{toCopy: true}, fresh, time.Minute},
		{"other", "SHOW TABLES", fakeRouter{toCopy: true}, fresh, time.Minute},
		{"veto", "SELECT NOW()", fakeRouter{toCopy: true}, fresh, time.Minute},
		{"txn", "SELECT count(*) FROM t", fakeRouter{toCopy: true, inTxn: true}, fresh, time.Minute},
		{"off", "SELECT count(*) FROM t", fakeRouter{toCopy: true}, fresh, 0},
		{"cheap", "SELECT count(*) FROM t", fakeRouter{}, fresh, time.Minute},
		{"bounded", "SELECT count(*) FROM t", fakeRouter{rule: readrouter.RuleBoundedLimit}, fresh, time.Minute},
		{"explain failed", "SELECT count(*) FROM t", fakeRouter{toCopy: true, decideErr: errors.New("boom")}, fresh, time.Minute},
		{"age unknown", "SELECT count(*) FROM t", fakeRouter{toCopy: true}, time.Time{}, time.Minute},
		{"too old", "SELECT count(*) FROM t", fakeRouter{toCopy: true}, time.Now().Add(-time.Hour), time.Minute},
		{"copy", "SELECT count(*) FROM t", fakeRouter{toCopy: true}, fresh, time.Minute},
	} {
		observe := func(prepared bool) []string {
			r := tc.router
			f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: tc.age}
			h := routingHandler(t, &r, f, tc.maxAge)
			var seen []string
			h.routerCfg.Observe = func(side RouteSide, reason RouteReason) {
				seen = append(seen, string(side)+"/"+string(reason))
			}
			if !prepared {
				if _, err := h.HandleQuery(tc.stmt); err != nil {
					t.Fatalf("%s as text: %v", tc.name, err)
				}
				return seen
			}
			_, _, ctx, err := h.HandleStmtPrepare(tc.stmt)
			if err != nil {
				t.Fatalf("%s prepare: %v", tc.name, err)
			}
			if _, err := h.HandleStmtExecute(ctx, "", nil); err != nil {
				t.Fatalf("%s execute: %v", tc.name, err)
			}
			return seen
		}
		text, prep := observe(false), observe(true)
		if len(text) != 1 || !reflect.DeepEqual(text, prep) {
			t.Errorf("%s: text observed %v, prepared %v; want one identical observation", tc.name, text, prep)
		}
	}
}
