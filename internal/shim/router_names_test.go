package shim

import (
	"bytes"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// What the copy answers a statement with, under the copy's own names: an
// expression with no alias named DuckDB's way, a column as it was declared.
func copyNamedResult() sqlsandbox.Result {
	return sqlsandbox.Result{
		Columns: []sqlsandbox.Column{{Name: "ol_w_id", Type: "INTEGER"}, {Name: "count_star()", Type: "BIGINT"}, {Name: "sum(ol_amount)", Type: "VARCHAR"}},
		Rows:    [][]any{{"1", "300182", "449496174.11"}},
	}
}

func resultFieldNames(rs *mysql.Resultset) []string {
	out := make([]string, len(rs.Fields))
	for i, f := range rs.Fields {
		out[i] = string(f.Name)
	}
	return out
}

const namesStmt = "SELECT OL_W_ID, COUNT(*), SUM(ol_amount) FROM order_line GROUP BY ol_w_id"

func namesHandler(t *testing.T, r *fakeRouter) (*Handler, *fakeFreeSQL, *bytes.Buffer) {
	t.Helper()
	f := &fakeFreeSQL{res: copyNamedResult(), updatedAt: time.Now().Add(-10 * time.Second)}
	logs := &bytes.Buffer{}
	h := NewHandler(nil, slog.New(slog.NewTextHandler(logs, nil)))
	h.BindFreeSQL(f)
	h.BindRouter(r, RouterConfig{MaxCopyAge: time.Minute})
	return h, f, logs
}

// A statement the copy answers reaches the client under the names MySQL
// gives its columns, asked of the source for the statement as the client
// sent it. The cells are the copy's.
func TestRouter_theCopysAnswerCarriesTheSourcesColumnNames(t *testing.T) {
	want := []string{"OL_W_ID", "COUNT(*)", "SUM(ol_amount)"}
	r := &fakeRouter{toCopy: true, srcNames: map[string][]string{namesStmt: want}}
	h, f, logs := namesHandler(t, r)
	res, err := h.HandleQuery(namesStmt)
	if err != nil {
		t.Fatal(err)
	}
	if f.calls != 1 || len(r.forwarded) != 0 {
		t.Fatalf("the copy ran %d times and the source %d: want the copy alone", f.calls, len(r.forwarded))
	}
	if got := resultFieldNames(res.Resultset); !reflect.DeepEqual(got, want) {
		t.Errorf("columns = %q, want the source's %q", got, want)
	}
	if _, stale := res.FieldNames["count_star()"]; stale {
		t.Error("the result still finds a column under the copy's name")
	}
	if got := textRows(t, res.Resultset); !reflect.DeepEqual(got, [][]string{{"1", "300182", "449496174.11"}}) {
		t.Errorf("cells = %v", got)
	}
	if !reflect.DeepEqual(r.namesAsked, []string{namesStmt}) {
		t.Errorf("the source was asked about %q, want the statement as the client sent it, once", r.namesAsked)
	}
	if strings.Contains(logs.String(), "names") {
		t.Errorf("a warning for names that were given:\n%s", logs)
	}
}

// Two columns of one name: both are renamed.
func TestRouter_twoColumnsOfOneName(t *testing.T) {
	r := &fakeRouter{toCopy: true, namesDefault: []string{"n", "n", "x"}}
	h, _, _ := namesHandler(t, r)
	res, err := h.HandleQuery(namesStmt)
	if err != nil {
		t.Fatal(err)
	}
	if got := resultFieldNames(res.Resultset); !reflect.DeepEqual(got, []string{"n", "n", "x"}) {
		t.Errorf("columns = %q", got)
	}
}

// When the source cannot say, or counts other columns than the copy
// returned, the copy's answer is still served, under the copy's names, and
// the log says so once per connection.
func TestRouter_withoutTheSourcesNamesTheCopysStay(t *testing.T) {
	copys := []string{"ol_w_id", "count_star()", "sum(ol_amount)"}
	for _, c := range []struct {
		what   string
		router *fakeRouter
		says   string
	}{
		{"the source refuses", &fakeRouter{toCopy: true, namesErr: errors.New("ERROR 1295: This command is not supported in the prepared statement protocol yet")}, "could not say how it names"},
		{"fewer columns", &fakeRouter{toCopy: true, namesDefault: []string{"a", "b"}}, "the source names 2 columns, the copy returned 3"},
		{"more columns", &fakeRouter{toCopy: true, namesDefault: []string{"a", "b", "c", "d"}}, "the source names 4 columns, the copy returned 3"},
		{"no columns", &fakeRouter{toCopy: true}, "the source names 0 columns, the copy returned 3"},
	} {
		h, f, logs := namesHandler(t, c.router)
		for range 2 {
			res, err := h.HandleQuery(namesStmt)
			if err != nil {
				t.Fatalf("%s: %v", c.what, err)
			}
			if got := resultFieldNames(res.Resultset); !reflect.DeepEqual(got, copys) {
				t.Errorf("%s: columns = %q, want the copy's own", c.what, got)
			}
		}
		if f.calls != 2 || len(c.router.forwarded) != 0 {
			t.Errorf("%s: the copy ran %d times and the source %d, want the copy twice", c.what, f.calls, len(c.router.forwarded))
		}
		if n := strings.Count(logs.String(), c.says); n != 1 {
			t.Errorf("%s: %q logged %d times over two statements, want once:\n%s", c.what, c.says, n, logs)
		}
	}
}

// The source is asked for names only for an answer the copy gave: not for a
// statement MySQL answers, nor for one the copy handed back.
func TestRouter_namesAreAskedOnlyForAnAnswerOfTheCopy(t *testing.T) {
	r := &fakeRouter{toCopy: false}
	h, _, _ := namesHandler(t, r)
	if _, err := h.HandleQuery(namesStmt); err != nil {
		t.Fatal(err)
	}
	if len(r.namesAsked) != 0 || len(r.forwarded) != 1 {
		t.Errorf("a cheap statement: names asked %d times, forwarded %d; want 0 and 1", len(r.namesAsked), len(r.forwarded))
	}

	r = &fakeRouter{toCopy: true}
	h, f, _ := namesHandler(t, r)
	f.err = &sqlsandbox.ColumnsDifferError{Reason: "a star"}
	if _, err := h.HandleQuery(namesStmt); err != nil {
		t.Fatal(err)
	}
	if len(r.namesAsked) != 0 || len(r.forwarded) != 1 {
		t.Errorf("a statement the copy handed back: names asked %d times, forwarded %d; want 0 and 1", len(r.namesAsked), len(r.forwarded))
	}
}

// A prepared statement: the names are the ones the source answered PREPARE
// with, which the client was told then. No second question is asked.
func TestPreparedRouted_theCopysAnswerCarriesTheSourcesColumnNames(t *testing.T) {
	defs := [][]byte{
		(&mysql.Field{Name: []byte("OL_W_ID"), Type: mysql.MYSQL_TYPE_LONG}).Dump(),
		(&mysql.Field{Name: []byte("COUNT(*)"), Type: mysql.MYSQL_TYPE_LONGLONG}).Dump(),
		(&mysql.Field{Name: []byte("SUM(ol_amount)"), Type: mysql.MYSQL_TYPE_NEWDECIMAL}).Dump(),
	}
	const tmpl = "SELECT OL_W_ID, COUNT(*), SUM(ol_amount) FROM order_line WHERE ol_quantity > ? GROUP BY ol_w_id"
	r := &fakeRouter{toCopy: true, stmtColumns: defs}
	h, f, logs := namesHandler(t, r)
	_, _, ctx, err := h.HandleStmtPrepare(tmpl)
	if err != nil {
		t.Fatal(err)
	}
	res, err := h.HandleStmtExecute(ctx, "", []any{int64(3)})
	if err != nil {
		t.Fatal(err)
	}
	if f.calls != 1 {
		t.Fatalf("the copy ran %d times, want once", f.calls)
	}
	if got, want := resultFieldNames(res.Resultset), []string{"OL_W_ID", "COUNT(*)", "SUM(ol_amount)"}; !reflect.DeepEqual(got, want) {
		t.Errorf("columns = %q, want the source's %q", got, want)
	}
	if _, err := res.RowDatas[0].ParseBinary(res.Fields, nil); err != nil {
		t.Errorf("the row is not binary-encoded under the renamed columns: %v", err)
	}
	if len(r.namesAsked) != 0 {
		t.Errorf("the source was asked for names %d times: a prepared statement already has them", len(r.namesAsked))
	}
	if strings.Contains(logs.String(), "names") {
		t.Errorf("a warning for names that were given:\n%s", logs)
	}

	// Definitions that cannot be read: the copy's names stay, said once.
	r = &fakeRouter{toCopy: true}
	h, _, logs = namesHandler(t, r)
	if _, _, ctx, err = h.HandleStmtPrepare(tmpl); err != nil {
		t.Fatal(err)
	}
	res, err = h.HandleStmtExecute(ctx, "", []any{int64(3)})
	if err != nil {
		t.Fatal(err)
	}
	if got := resultFieldNames(res.Resultset); got[1] != "count_star()" || !strings.Contains(logs.String(), "could not say how it names") || !strings.Contains(logs.String(), "its definition cannot be read") {
		t.Errorf("unreadable definitions: columns = %q, log:\n%s", got, logs)
	}
}

// The names are asked under a deadline of their own. The statement's began
// before its plan was asked for; when the copy answers after it has passed,
// a call on the source under it would drop the connection to the source,
// which is the client's session there. So the source is asked with a
// context that is still running, and the answer is renamed all the same.
func TestRouter_namesAreAskedUnderADeadlineOfTheirOwn(t *testing.T) {
	want := []string{"OL_W_ID", "COUNT(*)", "SUM(ol_amount)"}
	r := &fakeRouter{toCopy: true, namesDefault: want}
	h, f, _ := namesHandler(t, r)
	h.cfg.QueryTimeout = 40 * time.Millisecond
	// The copy answers after the statement's own deadline has passed.
	f.onRun = func() { time.Sleep(90 * time.Millisecond) }
	res, err := h.HandleQuery(namesStmt)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.namesCtxErr) != 1 || r.namesCtxErr[0] != nil {
		t.Fatalf("the source was asked for names under a context in state %v, want one call under a running one", r.namesCtxErr)
	}
	if got := resultFieldNames(res.Resultset); !reflect.DeepEqual(got, want) {
		t.Errorf("columns = %q, want the source's %q", got, want)
	}
}
