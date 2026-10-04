package shim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// fakeFreeSQL records what the handler asked and answers a canned result.
type fakeFreeSQL struct {
	res       sqlsandbox.Result
	err       error
	calls     int
	gotStmt   string
	gotSchema string
	gotSess   sqlsandbox.Session
	updatedAt time.Time
	ageCalls  int
}

func (f *fakeFreeSQL) CopyUpdatedAt(context.Context) time.Time { f.ageCalls++; return f.updatedAt }

func (f *fakeFreeSQL) Run(_ context.Context, statement, schema string, sess sqlsandbox.Session) (sqlsandbox.Result, error) {
	f.calls++
	f.gotStmt, f.gotSchema, f.gotSess = statement, schema, sess
	return f.res, f.err
}

func oneCell(name, typ string, v any) sqlsandbox.Result {
	return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: name, Type: typ}}, Rows: [][]any{{v}}}
}

// textRows decodes a text resultset back to strings, "<NULL>" for NULL.
func textRows(t *testing.T, rs *mysql.Resultset) [][]string {
	t.Helper()
	var out [][]string
	for _, rd := range rs.RowDatas {
		fvs, err := rd.ParseText(rs.Fields, nil)
		if err != nil {
			t.Fatalf("ParseText: %v", err)
		}
		row := make([]string, len(fvs))
		for i := range fvs {
			switch v := fvs[i].Value().(type) {
			case nil:
				row[i] = "<NULL>"
			case []byte:
				row[i] = string(v)
			default:
				row[i] = fmt.Sprint(v)
			}
		}
		out = append(out, row)
	}
	return out
}

func wantMyError(t *testing.T, err error, code uint16, contains ...string) *mysql.MyError {
	t.Helper()
	var me *mysql.MyError
	if !errors.As(err, &me) {
		t.Fatalf("err = %v (%T), want *mysql.MyError %d", err, err, code)
	}
	if me.Code != code {
		t.Fatalf("code = %d (%s), want %d", me.Code, me.Message, code)
	}
	for _, c := range contains {
		if !strings.Contains(me.Message, c) {
			t.Errorf("message %q does not contain %q", me.Message, c)
		}
	}
	return me
}

// An ordinary statement goes to the bound executor with the USE'd schema;
// the executor's rows come back as a text resultset.
func TestFreeSQL_dispatchesWithTheUSEdSchema(t *testing.T) {
	f := &fakeFreeSQL{res: oneCell("id", "INTEGER", json.Number("7"))}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	if err := h.UseDB("shop"); err != nil {
		t.Fatal(err)
	}
	res, err := h.HandleQuery("SELECT id FROM orders")
	if err != nil {
		t.Fatalf("HandleQuery: %v", err)
	}
	if f.gotStmt != "SELECT id FROM orders" || f.gotSchema != "shop" {
		t.Errorf("executor got (%q, %q), want the statement unchanged and schema shop", f.gotStmt, f.gotSchema)
	}
	if got := textRows(t, res.Resultset); len(got) != 1 || got[0][0] != "7" {
		t.Errorf("rows = %v, want [[7]]", got)
	}
	if res.Warnings != 0 {
		t.Errorf("warnings = %d on an uncut result", res.Warnings)
	}

	// No USE: the schema is empty and the executor keeps DuckDB's default.
	h2 := NewHandler(nil, nil)
	h2.BindFreeSQL(f)
	if _, err := h2.HandleQuery("SELECT 2"); err != nil {
		t.Fatal(err)
	}
	if f.gotSchema != "" {
		t.Errorf("schema = %q with no USE, want empty", f.gotSchema)
	}
}

// Nothing bound: the refusal every shim gave before, and when the serving
// layer said why there is no SQL here, the reason is part of it.
func TestFreeSQL_unboundKeepsRefusing(t *testing.T) {
	h := NewHandler(nil, nil)
	_, err := h.HandleQuery("SELECT id FROM orders")
	me := wantMyError(t, err, mysql.ER_NOT_SUPPORTED_YET, "only handles _flashback / _snapshot / _diff", "SELECT id FROM orders")
	if strings.Contains(me.Message, "unavailable") {
		t.Errorf("no reason was given, yet the message carries one: %q", me.Message)
	}

	h.BindFreeSQLUnavailable("the copy for this server is only on S3")
	_, err = h.HandleQuery("SELECT id FROM orders")
	wantMyError(t, err, mysql.ER_NOT_SUPPORTED_YET, "SQL on the copy is unavailable here: the copy for this server is only on S3")

	// Binding an executor clears the reason.
	h.BindFreeSQL(&fakeFreeSQL{res: oneCell("x", "INTEGER", json.Number("1"))})
	if h.freeSQLWhyNot != "" {
		t.Error("BindFreeSQL left the unavailability reason set")
	}
}

// Time travel and handshake noise keep their place ahead of free SQL: a
// malformed virtual-schema query is still 1064 and a client's setup
// statement is still swallowed, with the executor never called for either.
func TestFreeSQL_timeTravelAndNoiseComeFirst(t *testing.T) {
	f := &fakeFreeSQL{res: oneCell("x", "INTEGER", json.Number("1"))}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	if err := h.UseDB("shop"); err != nil {
		t.Fatal(err)
	}
	_, err := h.HandleQuery("SELECT * FROM _flashback.orders AS OF 'not a time' WHERE id = 1")
	wantMyError(t, err, mysql.ER_PARSE_ERROR)
	for _, q := range []string{"SET NAMES utf8mb4", "select database()", "select @@session.some_unknown_variable"} {
		res, err := h.HandleQuery(q)
		if err != nil {
			t.Errorf("%q: %v", q, err)
		}
		if res == nil || res.Resultset != nil {
			t.Errorf("%q: want the empty OK the noise allowlist gives, got %+v", q, res)
		}
	}
	// A select of system variables the port knows is answered with a row.
	if res, err := h.HandleQuery("select @@version_comment limit 1"); err != nil || res == nil || res.Resultset == nil {
		t.Errorf("select @@version_comment: want a row, got %+v, %v", res, err)
	}
	if f.calls != 0 {
		t.Errorf("executor called %d times by time travel / noise statements", f.calls)
	}
}

func TestRewriteForDuckDB(t *testing.T) {
	cases := map[string]string{
		"SHOW DATABASES":                      showDatabasesSQL,
		" show schemas ; ":                    showDatabasesSQL,
		"SHOW COLUMNS FROM orders":            `DESCRIBE "orders"`,
		"show fields from `orders` from shop": `DESCRIBE "shop"."orders"`,
		"SHOW COLUMNS IN orders IN `we\"ird`": `DESCRIBE "we""ird"."orders"`,
		"SHOW COLUMNS FROM shop.orders":       `DESCRIBE "shop"."orders"`, // the dotted spelling clients send
		"SHOW COLUMNS FROM `odd.name`":        `DESCRIBE "odd.name"`,      // a quoted dot is part of the name
		"SHOW TABLES":                         "SHOW TABLES",
		"DESCRIBE orders":                     "DESCRIBE orders",
		"SELECT 'SHOW DATABASES'":             "SELECT 'SHOW DATABASES'",
		"SHOW DATABASES LIKE 'x'":             "SHOW DATABASES LIKE 'x'", // not the bare form: DuckDB's own error says so
	}
	for in, want := range cases {
		got, schema := rewriteForDuckDB(in, "shop")
		if got != want {
			t.Errorf("rewrite(%q) = %q, want %q", in, got, want)
		}
		// SHOW DATABASES drops the schema: a current database that is not in
		// the copy must not take the one statement that lists the copy down.
		wantSchema := "shop"
		if want == showDatabasesSQL {
			wantSchema = ""
		}
		if schema != wantSchema {
			t.Errorf("rewrite(%q) schema = %q, want %q", in, schema, wantSchema)
		}
	}
}

// Column types follow the DuckDB type, not the first row: a first row of
// NULLs (the BuildSimpleTextResultset trap) still yields typed columns, and
// so does an empty result. Cells are rendered by type.
func TestFreeSQLResultset_typesAndCells(t *testing.T) {
	res := sqlsandbox.Result{
		Columns: []sqlsandbox.Column{
			{Name: "id", Type: "INTEGER"}, {Name: "price", Type: "DECIMAL(10,2)"}, {Name: "ok", Type: "BOOLEAN"},
			{Name: "at", Type: "TIMESTAMP"}, {Name: "tags", Type: "INTEGER[]"}, {Name: "meta", Type: "STRUCT(a INTEGER)"},
			{Name: "name", Type: "VARCHAR"}, {Name: "n", Type: "UBIGINT"}, {Name: "big", Type: "HUGEINT"}, {Name: "d", Type: "DATE"},
		},
		Rows: [][]any{
			{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
			{json.Number("1"), json.Number("12.50"), true, "2026-01-02T03:04:05.5Z", []any{json.Number("1"), json.Number("2")},
				map[string]any{"a": json.Number("1")}, "x", json.Number("18446744073709551615"), "170141183460469231731687303715884105727", "2026-01-02"},
		},
	}
	rs, err := freeSQLResultset(res, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantTypes := []uint8{
		mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_NEWDECIMAL, mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_DATETIME,
		mysql.MYSQL_TYPE_JSON, mysql.MYSQL_TYPE_JSON, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_LONGLONG,
		mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_DATE,
	}
	for i, want := range wantTypes {
		if got := rs.Fields[i].Type; got != want {
			t.Errorf("field %s type = %d, want %d", res.Columns[i].Name, got, want)
		}
	}
	if rs.Fields[1].Decimal != 2 {
		t.Errorf("DECIMAL(10,2) scale = %d, want 2", rs.Fields[1].Decimal)
	}
	if rs.Fields[6].Charset != 255 || rs.Fields[6].Flag&mysql.BINARY_FLAG != 0 {
		t.Errorf("VARCHAR charset/flag = %d/%d: a binary-charset text column shows as hex in the mysql client", rs.Fields[6].Charset, rs.Fields[6].Flag)
	}
	if rs.Fields[7].Flag&mysql.UNSIGNED_FLAG == 0 {
		t.Error("UBIGINT lost its UNSIGNED flag")
	}
	rows := textRows(t, rs)
	if len(rows) != 2 {
		t.Fatalf("%d rows, want 2", len(rows))
	}
	for i, v := range rows[0] {
		if v != "<NULL>" {
			t.Errorf("first row col %d = %q, want NULL", i, v)
		}
	}
	want := []string{"1", "12.50", "1", "2026-01-02 03:04:05.5", "[1,2]", `{"a":1}`, "x", "18446744073709551615",
		"170141183460469231731687303715884105727", "2026-01-02"}
	for i, w := range want {
		if rows[1][i] != w {
			t.Errorf("col %s = %q, want %q", res.Columns[i].Name, rows[1][i], w)
		}
	}

	empty, err := freeSQLResultset(sqlsandbox.Result{Columns: res.Columns[:2]}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if empty.Fields[0].Type != mysql.MYSQL_TYPE_LONGLONG || empty.Fields[1].Type != mysql.MYSQL_TYPE_NEWDECIMAL {
		t.Errorf("empty result column types = %d/%d, want typed from DuckDB, not NULL", empty.Fields[0].Type, empty.Fields[1].Type)
	}
}

// A result cut at the cap raises one warning, SHOW WARNINGS returns it, and
// the next statement clears it. Without free SQL bound SHOW WARNINGS stays
// the empty OK it always was.
func TestFreeSQL_truncationWarning(t *testing.T) {
	cut := oneCell("id", "INTEGER", json.Number("1"))
	cut.Truncated = true
	cut.TruncatedCells = 3
	f := &fakeFreeSQL{res: cut}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	conn := &warningsConn{}
	h.BindConn(conn)
	if _, err := h.HandleQuery("SELECT id FROM orders"); err != nil {
		t.Fatal(err)
	}
	// The count travels on the CONNECTION: go-mysql writes the EOF packet's
	// warning count from Conn.SetWarnings, never from Result.Warnings.
	if conn.warnings != 2 {
		t.Errorf("connection warnings = %d, want 2 (rows cut, cells cut)", conn.warnings)
	}
	w, err := h.HandleQuery("SHOW WARNINGS")
	if err != nil {
		t.Fatal(err)
	}
	rows := textRows(t, w.Resultset)
	if len(rows) != 2 || rows[0][0] != "Warning" || !strings.Contains(rows[0][2], "cut at 1 rows") || !strings.Contains(rows[0][2], "LIMIT") ||
		!strings.Contains(rows[1][2], "3 cell(s)") {
		t.Errorf("SHOW WARNINGS rows = %v, want the row cut (with a LIMIT) and the 3 cut cells", rows)
	}
	if f.calls != 1 {
		t.Errorf("SHOW WARNINGS reached the executor (calls = %d)", f.calls)
	}

	// Any later statement clears it, a time-travel one included (it fails
	// here, with no index behind the handler; the clearing is the point).
	_, _ = h.HandleQuery("SELECT * FROM _flashback.orders AS OF '2026-01-01' WHERE id = 1")
	w, err = h.HandleQuery("show warnings;")
	if err != nil {
		t.Fatal(err)
	}
	if rows := textRows(t, w.Resultset); len(rows) != 0 {
		t.Errorf("SHOW WARNINGS after a clean statement = %v, want none", rows)
	}
	if conn.warnings != 0 {
		t.Errorf("connection warnings after a clean statement = %d, want 0 (a stale count would mark the next result)", conn.warnings)
	}

	plain := NewHandler(nil, nil)
	r, err := plain.HandleQuery("SHOW WARNINGS")
	if err != nil || r.Resultset != nil {
		t.Errorf("unbound SHOW WARNINGS = (%+v, %v), want the empty OK", r, err)
	}
}

// Each executor error lands on the MySQL code a client can act on, and a
// worker failure never carries its text (host paths) to the client.
func TestFreeSQL_errorMapping(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		code     uint16
		contains string
		absent   string
	}{
		{"refused", &sqlsandbox.RefusedError{Reason: "only a single SELECT statement can run here"}, mysql.ER_PARSE_ERROR, "only a single SELECT", ""},
		{"query", &sqlsandbox.QueryError{Message: `Catalog Error: Table with name nope does not exist`}, mysql.ER_UNKNOWN_ERROR, "Table with name nope", ""},
		{"timeout", &sqlsandbox.TimeoutError{Limit: 30 * time.Second}, mysql.ER_QUERY_INTERRUPTED, "cap of 30 s", ""},
		{"busy", sqlsandbox.ErrBusy, mysql.ER_TOO_MANY_USER_CONNECTIONS, "is busy", ""},
		{"busy after the wait", &sqlsandbox.BusyError{Waited: 30 * time.Second, MaxInFlight: 2}, mysql.ER_TOO_MANY_USER_CONNECTIONS, "waited 30s", ""},
		{"cancelled", context.Canceled, mysql.ER_QUERY_INTERRUPTED, "cancelled", ""},
		{"deadline without a cap", context.DeadlineExceeded, mysql.ER_QUERY_INTERRUPTED, "cancelled", ""},
		{"too large", sqlsandbox.ErrResultTooLarge, mysql.ER_UNKNOWN_ERROR, "too large", ""},
		{"not local", sqlsandbox.ErrCopyNotLocal, mysql.ER_UNKNOWN_ERROR, "only on S3", ""},
		{"worker", &sqlsandbox.WorkerError{Err: errors.New("exit 2"), Stderr: "/Users/dani/secret/path"}, mysql.ER_UNKNOWN_ERROR, "DBTrail's log", "secret"},
		{"gate", errors.New("the copy for this server is only on S3; SQL needs a local copy"), mysql.ER_UNKNOWN_ERROR, "only on S3", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(nil, nil)
			h.BindFreeSQL(&fakeFreeSQL{err: tc.err})
			_, err := h.HandleQuery("SELECT id FROM orders")
			me := wantMyError(t, err, tc.code, tc.contains)
			if tc.absent != "" && strings.Contains(me.Message, tc.absent) {
				t.Errorf("message %q leaks %q", me.Message, tc.absent)
			}
		})
	}
}

// USE sent as statement text (drivers and `mysql -e "USE x; ..."` do, where
// the interactive client sends COM_INIT_DB) selects the schema like UseDB:
// the time-travel default and free SQL's search path move together, and the
// schema allowlist still gates it.
func TestUseStatementText(t *testing.T) {
	f := &fakeFreeSQL{res: oneCell("x", "INTEGER", json.Number("1"))}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	for _, stmt := range []string{"USE shop", " use `shop` ; ", "USE shop;"} {
		res, err := h.HandleQuery(stmt)
		if err != nil || res == nil || res.Resultset != nil {
			t.Fatalf("%q: (%+v, %v), want an empty OK", stmt, res, err)
		}
	}
	if _, err := h.HandleQuery("SELECT 2"); err != nil {
		t.Fatal(err)
	}
	if f.gotSchema != "shop" {
		t.Errorf("schema after USE text = %q, want shop", f.gotSchema)
	}
	if f.calls != 1 {
		t.Errorf("USE reached the executor (calls = %d)", f.calls)
	}
	// Not the bare form: a USE with more after the name is not ours.
	if _, err := h.HandleQuery("USE shop extra"); err != nil || f.calls != 2 {
		t.Errorf("'USE shop extra' should have gone to the executor as an ordinary statement (calls = %d, err = %v)", f.calls, err)
	}

	denied := NewHandler(nil, nil)
	denied.BindAllowedSchemas([]string{"myapp"})
	_, err := denied.HandleQuery("USE shop")
	wantMyError(t, err, mysql.ER_DBACCESS_DENIED_ERROR)
}

// warningsConn is the slice of server.Conn the warning count needs.
type warningsConn struct{ warnings uint16 }

func (c *warningsConn) WritePacket([]byte) error { return nil }
func (c *warningsConn) SetWarnings(n uint16)     { c.warnings = n }

// The connection's own QueryTimeout (shorter than the sandbox's) is reported
// as that cap, not as a cancel nobody issued.
func TestFreeSQL_connectionDeadlineIsNamed(t *testing.T) {
	h := NewHandlerWithConfig(nil, Config{QueryTimeout: 7 * time.Second}, nil)
	h.BindFreeSQL(&fakeFreeSQL{err: context.DeadlineExceeded})
	_, err := h.HandleQuery("SELECT id FROM orders")
	wantMyError(t, err, mysql.ER_QUERY_INTERRUPTED, "this connection's cap of 7 s")
}
