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

	"github.com/dbtrail/dbtrail/internal/audittest"
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
	// The copy's own zone conversion, asked once per zone before the copy
	// runs under a named zone (routedsession.go): zoneProbes counts the
	// asks, zoneErr fails them, zoneOffsets replaces the answer.
	zoneProbes  int
	zoneErr     error
	zoneOffsets func(zone string, at []time.Time, fromZone []string) []string
}

func (f *fakeFreeSQL) CopyUpdatedAt(context.Context) time.Time { f.ageCalls++; return f.updatedAt }

func (f *fakeFreeSQL) Run(_ context.Context, statement, schema string, sess sqlsandbox.Session) (sqlsandbox.Result, error) {
	if strings.HasPrefix(statement, copyZoneProbePrefix) {
		f.zoneProbes++
		if f.zoneErr != nil {
			return sqlsandbox.Result{}, f.zoneErr
		}
		at := probeInstantsIn(statement)
		offs := zoneOffsets(sess.TimeZone, at)
		if f.zoneOffsets != nil {
			offs = f.zoneOffsets(sess.TimeZone, at, offs)
		}
		return oneCell("offsets", "VARCHAR", strings.Join(offs, ",")), nil
	}
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

// A DECIMAL travels as the worker printed it, scale and trailing zeros
// intact, on the text protocol and on the binary one a prepared statement is
// answered in, and the column says how many decimals it has (#2083).
func TestFreeSQLResultset_decimalKeepsItsScale(t *testing.T) {
	res := sqlsandbox.Result{
		Columns: []sqlsandbox.Column{
			{Name: "total", Type: "DECIMAL(38,2)"}, {Name: "whole", Type: "DECIMAL(12,0)"}, {Name: "rate", Type: "DECIMAL(38,10)"},
		},
		Rows: [][]any{
			{"117329550.00", "7", "0.0000000000"},
			{nil, nil, nil},
			{"-0.05", "-42", "12345678901234567890.1234567890"},
		},
	}
	rs, err := freeSQLResultset(res, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"117329550.00", "7", "0.0000000000"},
		{"<NULL>", "<NULL>", "<NULL>"},
		{"-0.05", "-42", "12345678901234567890.1234567890"},
	}
	check := func(proto string, rs *mysql.Resultset, rows [][]string) {
		t.Helper()
		for i, d := range []uint8{2, 0, 10} {
			if rs.Fields[i].Type != mysql.MYSQL_TYPE_NEWDECIMAL || rs.Fields[i].Decimal != d {
				t.Errorf("%s: %s is type %d with %d decimals, want NEWDECIMAL with %d", proto, res.Columns[i].Name, rs.Fields[i].Type, rs.Fields[i].Decimal, d)
			}
		}
		for r := range want {
			for c := range want[r] {
				if rows[r][c] != want[r][c] {
					t.Errorf("%s: row %d %s = %q, want %q", proto, r, res.Columns[c].Name, rows[r][c], want[r][c])
				}
			}
		}
	}
	check("text", rs, textRows(t, rs))
	bin, err := binaryResultset(rs)
	if err != nil {
		t.Fatal(err)
	}
	var binRows [][]string
	for r, data := range bin.RowDatas {
		vals, err := data.ParseBinary(bin.Fields, nil)
		if err != nil {
			t.Fatalf("row %d does not parse as binary: %v", r, err)
		}
		row := make([]string, len(vals))
		for c := range vals {
			switch v := vals[c].Value().(type) {
			case nil:
				row[c] = "<NULL>"
			case []byte:
				row[c] = string(v)
			default:
				row[c] = fmt.Sprint(v)
			}
		}
		binRows = append(binRows, row)
	}
	check("binary", bin, binRows)
}

// A result with more rows than the cap is an error naming the cap and the
// ways out (#2037), never the first rows with a warning: nothing is
// returned, nothing is audited as served, and no warning is left behind.
func TestFreeSQL_rowCapIsAnError(t *testing.T) {
	cut := sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "id", Type: "INTEGER"}},
		Rows: [][]any{{json.Number("1")}, {json.Number("2")}, {json.Number("3")}}, Truncated: true}
	f := &fakeFreeSQL{res: cut}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	conn := &warningsConn{}
	h.BindConn(conn)
	rec := audittest.Install(t)

	res, err := h.HandleQuery("SELECT id FROM orders")
	if res != nil {
		t.Errorf("a cut result came back with the error: %+v", res)
	}
	me := wantMyError(t, err, mysql.ER_TOO_BIG_SELECT, "more than 3 rows", "row cap", "LIMIT of 3 or less", "no LIMIT of its own", "sql_select_limit to 3 or less")
	if strings.Contains(me.Message, "above the cap") {
		t.Errorf("no sql_select_limit is set, yet the message speaks of one: %q", me.Message)
	}
	if conn.warnings != 0 {
		t.Errorf("connection warnings = %d after the refusal, want 0", conn.warnings)
	}
	if n := len(rec.Events()); n != 0 {
		t.Errorf("a refused result was audited as served (%d event(s))", n)
	}

	// A sql_select_limit above the cap does not lift it: the same refusal,
	// saying the limit is above the cap. At or under the cap the executor
	// cuts at the client's limit and does not report it as truncated, so
	// the result is returned and nothing is raised.
	if _, err := h.HandleQuery("SET sql_select_limit = 50"); err != nil {
		t.Fatal(err)
	}
	_, err = h.HandleQuery("SELECT id FROM orders")
	wantMyError(t, err, mysql.ER_TOO_BIG_SELECT, "more than 3 rows", "it is 50 now, above the cap")
	if f.gotSess.SelectLimit != 50 {
		t.Errorf("the executor was given limit %d, want the client's 50 (it applies the cap)", f.gotSess.SelectLimit)
	}
	if _, err := h.HandleQuery("SET sql_select_limit = 3"); err != nil {
		t.Fatal(err)
	}
	f.res.Truncated = false // what the executor reports for a cut at the client's own limit
	res, err = h.HandleQuery("SELECT id FROM orders")
	if err != nil || len(textRows(t, res.Resultset)) != 3 {
		t.Fatalf("a cut the client asked for: (%v, %v), want the 3 rows and no error", res, err)
	}
	if conn.warnings != 0 {
		t.Errorf("a cut the client asked for raised %d warning(s), want none", conn.warnings)
	}

	// A listing takes no LIMIT and no sql_select_limit: its refusal points at
	// the catalog instead of at advice that cannot be followed.
	f.res.Truncated = true
	for _, stmt := range []string{"SHOW TABLES", "describe orders", "/* c */ SHOW ALL TABLES"} {
		_, err = h.HandleQuery(stmt)
		me := wantMyError(t, err, mysql.ER_TOO_BIG_SELECT, "more than 3 rows", "takes no LIMIT", "information_schema")
		if strings.Contains(me.Message, "sql_select_limit") {
			t.Errorf("%s: the refusal advises sql_select_limit, which a listing ignores: %q", stmt, me.Message)
		}
	}

	// A prepared statement is refused the same way.
	f.res.Truncated = true
	_, _, ctx, err := h.HandleStmtPrepare("SELECT id FROM orders WHERE id > ?")
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.HandleStmtExecute(ctx, "", []any{int64(0)})
	wantMyError(t, err, mysql.ER_TOO_BIG_SELECT, "more than 3 rows")
}

// A cell cut at the cell cap raises one warning, SHOW WARNINGS returns it,
// and the next statement clears it. Without free SQL bound SHOW WARNINGS
// stays the empty OK it always was.
func TestFreeSQL_truncationWarning(t *testing.T) {
	cut := oneCell("id", "INTEGER", json.Number("1"))
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
	if conn.warnings != 1 {
		t.Errorf("connection warnings = %d, want 1 (cells cut)", conn.warnings)
	}
	w, err := h.HandleQuery("SHOW WARNINGS")
	if err != nil {
		t.Fatal(err)
	}
	rows := textRows(t, w.Resultset)
	if len(rows) != 1 || rows[0][0] != "Warning" || !strings.Contains(rows[0][2], "3 cell(s)") {
		t.Errorf("SHOW WARNINGS rows = %v, want the 3 cut cells", rows)
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
