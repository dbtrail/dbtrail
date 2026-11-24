package pgshim

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Session settings, SHOW and the connect-time functions (#2769). The
// expected texts are what PostgreSQL 16 answered to the same statements on
// the test box.

// copyLogin binds every connection to a stand-in for the copy that answers
// the port's own questions (a zone's wall clock, whether a schema exists)
// the way the copy's engine does, records what each statement ran in, and
// blocks a statement that says "block" until its context ends.
type copyLogin struct {
	mu   sync.Mutex
	ran  []string
	opts []RunOptions
	// disagree makes the copy read this zone an hour off.
	disagree string
	// running is told each time a statement that blocks starts.
	running chan struct{}
	// release lets a blocked statement finish with a row.
	release chan struct{}
}

func (l *copyLogin) run(ctx context.Context, st string, opts RunOptions) (sqlsandbox.Result, error) {
	l.mu.Lock()
	l.ran = append(l.ran, st)
	l.opts = append(l.opts, opts)
	l.mu.Unlock()
	switch {
	case strings.HasPrefix(st, "SELECT CAST(CAST(TIMESTAMPTZ"):
		loc := time.UTC
		if opts.TimeZone != "" {
			var err error
			if loc, err = time.LoadLocation(opts.TimeZone); err != nil {
				return sqlsandbox.Result{}, &sqlsandbox.RefusedError{Reason: fmt.Sprintf("the session time zone %q (SET time_zone) cannot be applied on the copy: unknown", opts.TimeZone)}
			}
		}
		var res sqlsandbox.Result
		row := []any{}
		for i, part := range strings.Split(strings.TrimPrefix(st, "SELECT "), ", ") {
			at, err := time.Parse("2006-01-02 15:04:05", part[len("CAST(CAST(TIMESTAMPTZ '"):len("CAST(CAST(TIMESTAMPTZ '")+19])
			if err != nil {
				return res, err
			}
			wall := at.In(loc)
			if opts.TimeZone == l.disagree {
				wall = wall.Add(time.Hour)
			}
			res.Columns = append(res.Columns, sqlsandbox.Column{Name: fmt.Sprintf("c%d", i), Type: "VARCHAR"})
			row = append(row, wall.Format("2006-01-02 15:04:05"))
		}
		res.Rows = [][]any{row}
		return res, nil
	case strings.Contains(st, "information_schema.schemata"):
		n := "0"
		for _, s := range []string{"'public'", "'sales'"} {
			if strings.HasSuffix(st, "schema_name = "+s) {
				n = "1"
			}
		}
		return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "n", Type: "BIGINT"}}, Rows: [][]any{{n}}}, nil
	case strings.Contains(st, "dbtrail_describe"):
		return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "stmt", Type: "VARCHAR"}}}, nil
	case strings.Contains(st, "block"):
		if l.running != nil {
			l.running <- struct{}{}
		}
		select {
		case <-ctx.Done():
			return sqlsandbox.Result{}, ctx.Err()
		case <-l.release:
		}
	case strings.Contains(st, "tstz"):
		return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "tstz", Type: "TIMESTAMP WITH TIME ZONE"}},
			Rows: [][]any{{"2026-01-01T10:00:00Z"}, {"infinity"}}}, nil
	}
	return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "stmt", Type: "VARCHAR"}}, Rows: [][]any{{st}}}, nil
}

func (l *copyLogin) last() (string, RunOptions) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ran[len(l.ran)-1], l.opts[len(l.opts)-1]
}

func (l *copyLogin) login(_ context.Context, _, _ string, password *Password, _ func()) (*Binding, *Error) {
	if !password.Matches([]string{portPassword}) {
		return nil, &Error{Code: "28P01", Message: "no"}
	}
	return &Binding{Run: l.run, Actor: "server:orders", Schema: "public"}, nil
}

// statusConn is a logged-in raw connection whose reads say what each
// ParameterStatus and error carried.
type statusConn struct {
	t       *testing.T
	c       net.Conn
	fe      *pgproto3.Frontend
	startup []string
	key     *pgproto3.BackendKeyData
}

func dialStatus(t *testing.T, addr string, params map[string]string) (*statusConn, error) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	r := &statusConn{t: t, c: c, fe: pgproto3.NewFrontend(c, c)}
	p := map[string]string{"user": "app", "database": "orders"}
	for k, v := range params {
		p[k] = v
	}
	r.fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: p})
	if err := r.fe.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := scramClientLogin(r.fe, portPassword, nil); err != nil {
		return nil, err
	}
	for {
		m, err := r.fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		switch x := m.(type) {
		case *pgproto3.ParameterStatus:
			r.startup = append(r.startup, x.Name+"="+x.Value)
		case *pgproto3.BackendKeyData:
			r.key = &pgproto3.BackendKeyData{ProcessID: x.ProcessID, SecretKey: append([]byte(nil), x.SecretKey...)}
		case *pgproto3.ErrorResponse:
			return nil, &serverError{x.Code, x.Message}
		case *pgproto3.ReadyForQuery:
			return r, nil
		}
	}
}

func (r *statusConn) query(q string) []string {
	r.t.Helper()
	r.fe.Send(&pgproto3.Query{String: q})
	if err := r.fe.Flush(); err != nil {
		r.t.Fatal(err)
	}
	return r.until()
}

func (r *statusConn) until() []string {
	r.t.Helper()
	var out []string
	for {
		m, err := r.fe.Receive()
		if err != nil {
			r.t.Fatalf("after %v: %v", out, err)
		}
		switch x := m.(type) {
		case *pgproto3.ParameterStatus:
			out = append(out, "PS "+x.Name+"="+x.Value)
		case *pgproto3.ErrorResponse:
			out = append(out, "Error "+x.Code+" "+x.Message)
		case *pgproto3.NoticeResponse:
			out = append(out, "Notice "+x.Code+" "+x.Message)
		case *pgproto3.RowDescription:
			var f []string
			for _, d := range x.Fields {
				f = append(f, fmt.Sprintf("%s:%d", d.Name, d.DataTypeOID))
			}
			out = append(out, "Columns "+strings.Join(f, ","))
		default:
			out = append(out, summarize(m))
		}
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			return out
		}
	}
}

// show is the one value a SHOW (or a one-cell SELECT) answers.
func (r *statusConn) show(q string) string {
	r.t.Helper()
	for _, m := range r.query(q) {
		if v, ok := strings.CutPrefix(m, "DataRow "); ok {
			return v
		}
		if strings.HasPrefix(m, "Error") {
			return m
		}
	}
	return "(no row)"
}

func TestSettings_startupReportsWhatPostgreSQLReports(t *testing.T) {
	addr := serveAddrCfg(t, Config{Login: (&copyLogin{}).login})
	r, err := dialStatus(t, addr, map[string]string{"application_name": "capapp"})
	if err != nil {
		t.Fatal(err)
	}
	want(t, "start-up ParameterStatus", r.startup,
		"application_name=capapp", "client_encoding=UTF8", "DateStyle=ISO, MDY", "default_transaction_read_only=on",
		"in_hot_standby=off", "integer_datetimes=on", "IntervalStyle=postgres", "is_superuser=off",
		"server_encoding=UTF8", "server_version="+serverVersion, "session_authorization=app",
		"standard_conforming_strings=on", "TimeZone=UTC")
	if r.key == nil || r.key.ProcessID == 0 || r.key.ProcessID > 1<<31-1 || len(r.key.SecretKey) != 4 {
		t.Fatalf("BackendKeyData = %+v, want a positive process number and a four-byte key", r.key)
	}
	// A second connection has another number.
	r2, err := dialStatus(t, addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r2.key.ProcessID == r.key.ProcessID {
		t.Fatalf("two connections share the process number %d", r.key.ProcessID)
	}
}

func TestSettings_setShowAndReport(t *testing.T) {
	l := &copyLogin{}
	addr := serveAddrCfg(t, Config{Login: l.login})
	r, err := dialStatus(t, addr, map[string]string{"application_name": "capapp"})
	if err != nil {
		t.Fatal(err)
	}
	steps := []struct {
		q    string
		want []string
	}{
		{"SET TimeZone = 'America/Bogota'", []string{"CommandComplete SET", "PS TimeZone=America/Bogota", "ReadyForQuery I"}},
		// The same value again is not reported again.
		{"SET TimeZone = 'america/bogota'", []string{"CommandComplete SET", "ReadyForQuery I"}},
		{"SET TimeZone = 'utc'", []string{"CommandComplete SET", "PS TimeZone=UTC", "ReadyForQuery I"}},
		{"SET TIME ZONE 'Asia/Kolkata'", []string{"CommandComplete SET", "PS TimeZone=Asia/Kolkata", "ReadyForQuery I"}},
		{"SET TIME ZONE LOCAL", []string{"CommandComplete SET", "PS TimeZone=UTC", "ReadyForQuery I"}},
		{"SET TimeZone = 'GMT-05:00'", []string{"CommandComplete SET", "PS TimeZone=GMT-05:00", "ReadyForQuery I"}},
		{"RESET TimeZone", []string{"CommandComplete RESET", "PS TimeZone=UTC", "ReadyForQuery I"}},
		{"SET application_name = 'añoé x'", []string{"CommandComplete SET", `PS application_name=a\xc3\xb1o\xc3\xa9 x`, "ReadyForQuery I"}},
		{"SET application_name = 'PostgreSQL JDBC Driver'", []string{"CommandComplete SET", "PS application_name=PostgreSQL JDBC Driver", "ReadyForQuery I"}},
		{"SET extra_float_digits = 3", []string{"CommandComplete SET", "ReadyForQuery I"}},
		{"SET DateStyle = 'ISO'", []string{"CommandComplete SET", "ReadyForQuery I"}},
		{"SET DateStyle = ISO, MDY", []string{"CommandComplete SET", "ReadyForQuery I"}},
		{"SET client_encoding = 'unicode'", []string{"CommandComplete SET", "ReadyForQuery I"}},
		{"SET NAMES 'utf8'", []string{"CommandComplete SET", "ReadyForQuery I"}},
		{"SET client_encoding = 'sql_ascii'", []string{"CommandComplete SET", "PS client_encoding=SQL_ASCII", "ReadyForQuery I"}},
		{"SET client_encoding = 'UTF8'", []string{"CommandComplete SET", "PS client_encoding=UTF8", "ReadyForQuery I"}},
		{"SET statement_timeout = 5000", []string{"CommandComplete SET", "ReadyForQuery I"}},
		{"SET LOCAL TimeZone = 'Asia/Tokyo'", []string{"Notice 25P01 SET LOCAL can only be used in transaction blocks", "CommandComplete SET", "ReadyForQuery I"}},
		{"SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY", []string{"CommandComplete SET", "ReadyForQuery I"}},
		{"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", []string{"Notice 25P01 SET TRANSACTION can only be used in transaction blocks", "CommandComplete SET", "ReadyForQuery I"}},
		{"SET lock_timeout = '1s'", []string{"CommandComplete SET", "ReadyForQuery I"}},
	}
	for _, s := range steps {
		want(t, s.q, r.query(s.q), s.want...)
	}
	for q, v := range map[string]string{
		"SHOW TimeZone": "UTC", "SHOW TIME ZONE": "UTC", "SHOW application_name": "PostgreSQL JDBC Driver",
		"SHOW extra_float_digits": "3", "SHOW DateStyle": "ISO, MDY", "SHOW client_encoding": "UTF8",
		"SHOW statement_timeout": "5s", "SHOW lock_timeout": "1s", "SHOW search_path": `"$user", public`,
		"show transaction isolation level": "read committed", "show standard_conforming_strings": "on",
		"show server_version_num": "140000", "show max_identifier_length": "63", "show transaction_read_only": "on",
		"show is_superuser": "off", "SHOW SESSION AUTHORIZATION": "app", "SHOW \"TimeZone\"": "UTC",
		"SELECT version()":                   "PostgreSQL " + serverVersion,
		"select pg_catalog.version();":       "PostgreSQL " + serverVersion,
		"select current_schema()":            "public",
		"SELECT pg_catalog.current_schema()": "public",
	} {
		if got := r.show(q); got != v {
			t.Errorf("%s = %q, want %q", q, got, v)
		}
	}
	// SHOW heads its column with the parameter's own name, as text; current_schema() is a name.
	want(t, "SHOW's column", r.query("show timezone"), "Columns TimeZone:25", "DataRow UTC", "CommandComplete SHOW", "ReadyForQuery I")
	want(t, "current_schema's column", r.query("select current_schema()"), "Columns current_schema:19", "DataRow public", "CommandComplete SELECT 1", "ReadyForQuery I")
	// The durations as PostgreSQL shows them.
	for v, shown := range map[string]string{"'1500ms'": "1500ms", "'1.5s'": "1500ms", "60000": "1min", "0": "0", "'2h'": "2h", "'1d'": "1d", "'500us'": "0"} {
		r.query("SET statement_timeout = " + v)
		if got := r.show("SHOW statement_timeout"); got != shown {
			t.Errorf("statement_timeout %s shows %q, want %q", v, got, shown)
		}
	}
	// RESET ALL goes back to the start-up message's values.
	r.query("SET TimeZone = 'Asia/Tokyo'")
	want(t, "RESET ALL", r.query("RESET ALL"), "CommandComplete RESET", "PS application_name=capapp", "PS TimeZone=UTC", "ReadyForQuery I")
	if got := r.show("SHOW statement_timeout"); got != "0" {
		t.Errorf("statement_timeout after RESET ALL %q", got)
	}
	// SHOW ALL lists every parameter, by name.
	rows := 0
	for _, m := range r.query("SHOW ALL") {
		if strings.HasPrefix(m, "DataRow ") {
			rows++
		}
	}
	if rows != len(gucs) {
		t.Errorf("SHOW ALL answered %d rows, want %d", rows, len(gucs))
	}
	// None of this reached the copy but the zone questions.
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, st := range l.ran {
		if !strings.HasPrefix(st, "SELECT CAST(CAST(TIMESTAMPTZ") {
			t.Errorf("the copy was asked %q", st)
		}
	}
}

func TestSettings_refusals(t *testing.T) {
	copyZonesAgreed.Range(func(k, _ any) bool { copyZonesAgreed.Delete(k); return true })
	addr := serveAddrCfg(t, Config{Login: (&copyLogin{disagree: "Asia/Seoul"}).login})
	r, err := dialStatus(t, addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	for q, code := range map[string]string{
		"SET extra_float_digits = 4":                            "22023 4 is outside the valid range for parameter \"extra_float_digits\" (-15 .. 3)",
		"SET extra_float_digits = 0":                            "0A000",
		"SET extra_float_digits = 'x'":                          "22023",
		"SET statement_timeout = -1":                            "22023 -1 ms is outside the valid range for parameter \"statement_timeout\" (0 .. 2147483647)",
		"SET statement_timeout = '5S'":                          "22023",
		"SET DateStyle = 'ISO, DMY'":                            "0A000",
		"SET DateStyle = 'SQL'":                                 "0A000",
		"SET DateStyle = 'nonsense'":                            "22023",
		"SET IntervalStyle = 'sql_standard'":                    "0A000",
		"SET bytea_output = 'escape'":                           "0A000",
		"SET standard_conforming_strings = off":                 "0A000",
		"SET client_encoding = 'LATIN1'":                        "0A000",
		"SET nosuch_param = 1":                                  "42704",
		"SET my.custom = 'x'":                                   "42704",
		"SHOW nosuch":                                           "", // the copy's own SHOW
		"SET server_version = '1'":                              "55P02",
		"RESET server_version":                                  "55P02",
		"SET TimeZone = 'Nowhere/Zone'":                         "22023",
		"SET TimeZone = 'Local'":                                "22023",
		"SET TIME ZONE -5":                                      "0A000",
		"SET TIME ZONE INTERVAL '-05:00' HOUR TO MINUTE":        "0A000",
		"SET TimeZone = 'GMT+05:30'":                            "22023",
		"SET TimeZone = 'Asia/Seoul'":                           "22023",
		"SET TimeZone = 'UTC', 'GMT'":                           "42601",
		"SET search_path = sales, public":                       "0A000",
		"SET search_path = ''":                                  "0A000",
		"SET search_path = nowhere":                             "3F000",
		"SET search_path = 'sales, public'":                     "3F000",
		"SET ROLE admin":                                        "0A000",
		"SET SESSION AUTHORIZATION admin":                       "0A000",
		"SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE": "0A000",
		"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE": "0A000",
		"BEGIN ISOLATION LEVEL REPEATABLE READ":                                   "0A000",
		"SET default_transaction_read_only = off":                                 "0A000",
		"SET application_name = $1":                                               "42601",
		"SET":                                                                     "42601",
		"SET timezone":                                                            "42601",
		"SET TRANSACTION SNAPSHOT '1'":                                            "0A000",
	} {
		got := r.query(q)
		isErr := strings.HasPrefix(got[0], "Error ")
		switch {
		case code == "" && isErr && strings.Contains(got[0], "configuration parameter"):
			t.Errorf("%s: %v, want it sent on to the copy", q, got)
		case code != "" && (!isErr || !strings.HasPrefix(strings.TrimPrefix(got[0], "Error "), code)):
			t.Errorf("%s: %v, want %s", q, got, code)
		}
		if got[len(got)-1] != "ReadyForQuery I" {
			t.Errorf("%s left the connection in %s", q, got[len(got)-1])
		}
	}
	// Nothing refused was applied.
	for q, v := range map[string]string{"SHOW TimeZone": "UTC", "SHOW search_path": `"$user", public`, "SHOW extra_float_digits": "1", "SHOW DateStyle": "ISO, MDY"} {
		if got := r.show(q); got != v {
			t.Errorf("%s = %q after the refusals, want %q", q, got, v)
		}
	}
}

// A SET in a transaction block goes as on PostgreSQL: a SET LOCAL ends with
// the block, a ROLLBACK undoes a SET, and a block that fails undoes what it
// set when it fails, which the error's ReadyForQuery reports.
func TestSettings_followTheTransactionBlock(t *testing.T) {
	addr := serveAddrCfg(t, Config{Login: (&copyLogin{}).login})
	r, err := dialStatus(t, addr, map[string]string{"application_name": "capapp"})
	if err != nil {
		t.Fatal(err)
	}
	want(t, "BEGIN", r.query("BEGIN"), "CommandComplete BEGIN", "ReadyForQuery T")
	want(t, "SET LOCAL", r.query("SET LOCAL TimeZone = 'Asia/Tokyo'"), "CommandComplete SET", "PS TimeZone=Asia/Tokyo", "ReadyForQuery T")
	want(t, "SET", r.query("SET application_name = 'inblock'"), "CommandComplete SET", "PS application_name=inblock", "ReadyForQuery T")
	want(t, "COMMIT", r.query("COMMIT"), "CommandComplete COMMIT", "PS TimeZone=UTC", "ReadyForQuery I")
	want(t, "BEGIN", r.query("BEGIN"), "CommandComplete BEGIN", "ReadyForQuery T")
	want(t, "SET", r.query("SET application_name = 'rolledback'"), "CommandComplete SET", "PS application_name=rolledback", "ReadyForQuery T")
	want(t, "ROLLBACK", r.query("ROLLBACK"), "CommandComplete ROLLBACK", "PS application_name=inblock", "ReadyForQuery I")
	want(t, "BEGIN", r.query("BEGIN"), "CommandComplete BEGIN", "ReadyForQuery T")
	want(t, "SET", r.query("SET TimeZone = 'Asia/Kolkata'"), "CommandComplete SET", "PS TimeZone=Asia/Kolkata", "ReadyForQuery T")
	got := r.query("SET extra_float_digits = 9")
	if len(got) != 3 || !strings.HasPrefix(got[0], "Error 22023") || got[1] != "PS TimeZone=UTC" || got[2] != "ReadyForQuery E" {
		t.Fatalf("a failed block: %q", got)
	}
	want(t, "ROLLBACK", r.query("ROLLBACK"), "CommandComplete ROLLBACK", "ReadyForQuery I")
	// The block's isolation level is the one BEGIN named, then the default.
	r.query("BEGIN ISOLATION LEVEL READ UNCOMMITTED")
	if got := r.show("SHOW transaction_isolation"); got != "read uncommitted" {
		t.Errorf("in the block: %q", got)
	}
	r.query("COMMIT")
	if got := r.show("SHOW transaction_isolation"); got != "read committed" {
		t.Errorf("after the block: %q", got)
	}
	// client_min_messages above a warning silences it, as on PostgreSQL.
	r.query("SET client_min_messages = error")
	want(t, "a silenced warning", r.query("COMMIT"), "CommandComplete COMMIT", "ReadyForQuery I")
}

// The start-up message's settings are the connection's from the start,
// pgx's and JDBC's included, and a value this port refuses ends the login
// with the reason.
func TestSettings_startupParameters(t *testing.T) {
	l := &copyLogin{}
	addr := serveAddrCfg(t, Config{Login: l.login})
	// What JDBC sends.
	r, err := dialStatus(t, addr, map[string]string{"DateStyle": "ISO", "TimeZone": "America/Bogota", "client_encoding": "UTF8", "extra_float_digits": "2"})
	if err != nil {
		t.Fatal(err)
	}
	if got := r.show("SHOW TimeZone"); got != "America/Bogota" {
		t.Fatalf("TimeZone %q", got)
	}
	// RESET goes back to it.
	r.query("SET TimeZone = 'UTC'")
	r.query("RESET TimeZone")
	if got := r.show("SHOW TimeZone"); got != "America/Bogota" {
		t.Fatalf("TimeZone after RESET %q", got)
	}
	// libpq's PGOPTIONS.
	r, err = dialStatus(t, addr, map[string]string{"options": `-c statement_timeout=5s --search-path=sales -capplication_name=a\ b`})
	if err != nil {
		t.Fatal(err)
	}
	for q, v := range map[string]string{"SHOW statement_timeout": "5s", "SHOW search_path": "sales", "SHOW application_name": "a b", "select current_schema()": "sales"} {
		if got := r.show(q); got != v {
			t.Errorf("%s = %q, want %q", q, got, v)
		}
	}
	for params, code := range map[string]string{
		"TimeZone=Nowhere/Zone":  "22023",
		"client_encoding=LATIN1": "0A000",
		"nosuch=1":               "42704",
		"replication=database":   "0A000",
		"search_path=nowhere":    "3F000",
		"options=-B 100":         "0A000",
		"extra_float_digits=0":   "0A000",
		"server_version=1":       "55P02",
	} {
		k, v, _ := strings.Cut(params, "=")
		_, err := dialStatus(t, addr, map[string]string{k: v})
		var se *serverError
		if !errors.As(err, &se) || se.Code != code {
			t.Errorf("start-up %s: %v, want FATAL %s", params, err, code)
		}
	}
	// _pq_ protocol options are not settings.
	if _, err := dialStatus(t, addr, map[string]string{"_pq_.something": "x"}); err != nil {
		t.Errorf("a _pq_ option: %v", err)
	}
}

// The session's zone and schema reach the copy with every statement, and a
// timestamp with time zone comes back in the zone, as text and in binary.
func TestSettings_zoneAndSchemaReachTheCopy(t *testing.T) {
	l := &copyLogin{}
	addr := serveAddrCfg(t, Config{Login: l.login})
	r, err := dialStatus(t, addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.query("SET TimeZone = 'America/Bogota'")
	r.query("SET search_path TO sales")
	want(t, "a timestamp with time zone", r.query("SELECT tstz FROM t"),
		"Columns tstz:1184", "DataRow 2026-01-01 05:00:00-05", "DataRow infinity", "CommandComplete SELECT 2", "ReadyForQuery I")
	if _, opts := l.last(); !reflect.DeepEqual(opts, RunOptions{Schema: "sales", TimeZone: "America/Bogota"}) {
		t.Fatalf("the statement ran in %+v", opts)
	}
	// Binary: the instant, whatever the zone, and infinity's end of the count.
	r.fe.Send(&pgproto3.Parse{Query: "SELECT tstz FROM t"})
	r.fe.Send(&pgproto3.Bind{ResultFormatCodes: []int16{1}})
	r.fe.Send(&pgproto3.Execute{})
	r.fe.Send(&pgproto3.Sync{})
	_ = r.fe.Flush()
	var cells [][]byte
	for {
		m, err := r.fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		if d, ok := m.(*pgproto3.DataRow); ok {
			cells = append(cells, append([]byte(nil), d.Values[0]...))
		}
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	at := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	us := (at.Unix() - pgEpoch.Unix()) * 1e6
	if len(cells) != 2 || int64(binary.BigEndian.Uint64(cells[0])) != us || fmt.Sprintf("%x", cells[1]) != "7fffffffffffffff" {
		t.Fatalf("binary cells %x, want %x and 7fffffffffffffff", cells, us)
	}
	// The Describe runs in them too.
	r.fe.Send(&pgproto3.Parse{Name: "d", Query: "SELECT stmt FROM t"})
	r.fe.Send(&pgproto3.Describe{ObjectType: 'S', Name: "d"})
	r.fe.Send(&pgproto3.Sync{})
	_ = r.fe.Flush()
	r.until()
	if st, opts := l.last(); !strings.Contains(st, "dbtrail_describe") || !reflect.DeepEqual(opts, RunOptions{Schema: "sales", TimeZone: "America/Bogota"}) {
		t.Fatalf("the describe ran %q in %+v", st, opts)
	}
}

// A zone the copy could not be asked about when it was set is asked about
// before the next statement, which fails if the copy disagrees.
func TestSettings_aZoneIsAskedAboutBeforeTheStatement(t *testing.T) {
	copyZonesAgreed.Range(func(k, _ any) bool { copyZonesAgreed.Delete(k); return true })
	failing := true
	var mu sync.Mutex
	l := &copyLogin{disagree: "Europe/Lisbon"}
	login := func(ctx context.Context, user, db string, pw *Password, drop func()) (*Binding, *Error) {
		b, e := l.login(ctx, user, db, pw, drop)
		if b != nil {
			b.Run = func(ctx context.Context, st string, o RunOptions) (sqlsandbox.Result, error) {
				mu.Lock()
				f := failing
				mu.Unlock()
				if f && strings.HasPrefix(st, "SELECT CAST") {
					return sqlsandbox.Result{}, sqlsandbox.ErrBusy
				}
				return l.run(ctx, st, o)
			}
		}
		return b, e
	}
	addr := serveAddrCfg(t, Config{Login: login})
	r, err := dialStatus(t, addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	want(t, "a SET the copy could not be asked about", r.query("SET TimeZone = 'Europe/Lisbon'"),
		"CommandComplete SET", "PS TimeZone=Europe/Lisbon", "ReadyForQuery I")
	got := r.query("SELECT stmt FROM t")
	if !strings.HasPrefix(got[0], "Error 53000") {
		t.Fatalf("a statement while the copy cannot be asked: %q", got)
	}
	mu.Lock()
	failing = false
	mu.Unlock()
	got = r.query("SELECT stmt FROM t")
	if !strings.HasPrefix(got[0], "Error 22023") || !strings.Contains(got[0], "disagree") {
		t.Fatalf("a statement in a zone the copy disagrees on: %q", got)
	}
}

// On the time-travel port the copy is not there to run a zone in: TimeZone
// is UTC only; search_path is where its tables resolve.
func TestSettings_timeTravelPort(t *testing.T) {
	conn := dialPGWire(t, testAuth(t), testUser, testPass)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := conn.Exec(ctx, "SET TimeZone = 'America/Bogota'")
	if pe := requirePgError(t, err); pe.Code != "0A000" {
		t.Fatalf("SET TimeZone: %s", pe.Code)
	}
	if _, err := conn.Exec(ctx, "SET TimeZone = 'UTC'"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "SET search_path = sales"); err != nil {
		t.Fatal(err)
	}
	var schema, tz string
	if err := conn.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil || schema != "sales" {
		t.Fatalf("current_schema %q %v", schema, err)
	}
	if err := conn.QueryRow(ctx, "SHOW TimeZone").Scan(&tz); err != nil || tz != "UTC" {
		t.Fatalf("SHOW TimeZone %q %v", tz, err)
	}
	_, err = conn.Exec(ctx, "SHOW nosuch")
	if pe := requirePgError(t, err); pe.Code != "42704" {
		t.Fatalf("SHOW nosuch: %s", pe.Code)
	}
}

// pgx in every extended mode describes and runs SHOW and SET.
func TestSettings_extendedProtocol(t *testing.T) {
	for name, mode := range extendedModes {
		t.Run(name, func(t *testing.T) {
			l := &copyLogin{}
			addr := serveAddrCfg(t, Config{Login: l.login})
			cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://app:%s@%s/orders?sslmode=disable", portPassword, addr))
			if err != nil {
				t.Fatal(err)
			}
			cfg.DefaultQueryExecMode = mode
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(context.Background())
			if _, err := conn.Exec(ctx, "SET TimeZone = 'Asia/Kolkata'"); err != nil {
				t.Fatal(err)
			}
			if got := conn.PgConn().ParameterStatus("TimeZone"); got != "Asia/Kolkata" {
				t.Fatalf("ParameterStatus TimeZone %q", got)
			}
			var tz, v string
			if err := conn.QueryRow(ctx, "SHOW TimeZone").Scan(&tz); err != nil || tz != "Asia/Kolkata" {
				t.Fatalf("SHOW TimeZone %q %v", tz, err)
			}
			if err := conn.QueryRow(ctx, "select pg_catalog.version()").Scan(&v); err != nil || !strings.HasPrefix(v, "PostgreSQL 14.0") {
				t.Fatalf("version %q %v", v, err)
			}
		})
	}
}

func TestParseSessionCmd(t *testing.T) {
	for _, c := range []struct {
		q    string
		want string // "" for not a session statement; "error" for a syntax error
	}{
		{"SET TimeZone = 'America/Bogota'", "set timezone [America/Bogota]"},
		{"set timezone to 'UTC';", "set timezone [UTC]"},
		{"SET SESSION TimeZone TO 'UTC' ; ", "set timezone [UTC]"},
		{"SET LOCAL statement_timeout = 5000", "set local statement_timeout [5000]"},
		{"SET TIME ZONE 'Europe/Madrid'", "set timezone [Europe/Madrid]"},
		{"SET TIME ZONE LOCAL", "reset timezone"},
		{"SET TIME ZONE DEFAULT", "reset timezone"},
		{"SET TIME ZONE -5", "set timezone [-5]"},
		{"SET TIME ZONE INTERVAL '-05:00' HOUR TO MINUTE", "refused"},
		{"SET NAMES 'UTF8'", "set client_encoding [UTF8]"},
		{"SET SCHEMA 'sales'", "set search_path [sales]"},
		{`SET search_path = sales, "$user", 'x y', Public`, "set search_path [sales $user x y public]"},
		{`SET "TimeZone" = 'UTC'`, "set timezone [UTC]"},
		{"SET my.custom = 'x'", "set my.custom [x]"},
		{"SET application_name = 'it''s'", "set application_name [it's]"},
		{`SET application_name = E'a\'b'`, "set application_name [a'b]"},
		{"SET statement_timeout TO DEFAULT", "reset statement_timeout"},
		{"/* a /* nested */ comment */ SET TimeZone = 'UTC' -- the end", "set timezone [UTC]"},
		{"SET\n\tTimeZone\n=\n'UTC'\n", "set timezone [UTC]"},
		{"SET extra_float_digits = +3", "set extra_float_digits [3]"},
		{"SET ROLE admin", "refused"},
		{"SET SESSION AUTHORIZATION admin", "refused"},
		{"SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY", "session ro"},
		{"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", "tx serializable"},
		{"SET application_name = $1", "error"},
		{"SET", "error"},
		{"SET timezone", "error"},
		{"SET timezone = ", "error"},
		{"SET x = 1; SELECT 1", ""},
		{"SET x = 1; SELECT * FROM t", ""},
		{"RESET ALL", "reset all"},
		{"RESET TimeZone", "reset timezone"},
		{"RESET TIME ZONE", "reset timezone"},
		{"RESET ROLE", "noop"},
		{"RESET SESSION AUTHORIZATION", "noop"},
		{"SHOW ALL", "show all"},
		{"SHOW TimeZone", "show timezone"},
		{"show transaction isolation level", "show transaction_isolation"},
		{"SHOW TIME ZONE", "show timezone"},
		{"SHOW SESSION AUTHORIZATION", "show session_authorization"},
		{"SHOW TABLES", "show tables"},
		{"SHOW ALL TABLES", ""},
		{"SHOW", ""},
		{"BEGIN", "begin"},
		{"begin read only;", "begin"},
		{"BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY", "begin repeatable read"},
		{"START TRANSACTION", "begin"},
		{"BEGIN WORK", "begin"},
		{"COMMIT", "commit"},
		{"END", "commit"},
		{"ROLLBACK ; ", "rollback"},
		{"ABORT", "rollback"},
		{"COMMIT AND CHAIN", ""},
		{"BEGIN; SELECT 1", ""},
		{"SELECT version()", "func version"},
		{"select pg_catalog.version();", "func version"},
		{"SELECT current_schema()", "func current_schema"},
		{"select current_schema", "func current_schema"},
		{"select pg_catalog.current_schema()", "func current_schema"},
		{"SELECT version() AS v", ""},
		// psycopg-based tools on connect (the HTAP bench of 2026-11-18).
		{"SELECT pg_backend_pid(), current_setting('server_version')", "func pg_backend_pid,current_setting(server_version)"},
		{"select PG_BACKEND_PID()", "func pg_backend_pid"},
		{"select pg_catalog.pg_backend_pid();", "func pg_backend_pid"},
		{"SELECT pg_is_in_recovery()", "func pg_is_in_recovery"},
		{"SELECT current_setting('TimeZone')", "func current_setting(timezone)"},
		{"SELECT current_setting('nope', true)", "func current_setting(nope)?"},
		{"SELECT current_setting('nope', false)", "func current_setting(nope)"},
		{"SELECT version(), current_schema", "func version,current_schema"},
		{"SELECT pg_backend_pid(),", ""},
		{"SELECT pg_backend_pid() AS pid", ""},
		{"SELECT pg_backend_pid(1)", ""},
		{"SELECT pg_backend_pid", ""},
		{"SELECT current_setting(name)", ""},
		{"SELECT current_setting('a', 'b')", ""},
		{"SELECT current_setting()", ""},
		{"SELECT current_setting('server_version_num')::int/100 as version", ""},
		{"SELECT pg_backend_pid() FROM t", ""},
		{"SELECT pg_backend_pid(), 1", ""},
		{"SELECT", ""},
		{"select 1", ""},
		{"settle", ""},
		{"", ""},
	} {
		cmd, ok, perr := parseSessionCmd(c.q)
		got := ""
		switch {
		case perr != nil:
			got = "error"
			if perr.code != "42601" {
				got = "error " + perr.code
			}
		case !ok:
		case cmd.refusal != nil:
			got = "refused"
		default:
			got = describeCmd(cmd)
		}
		if got != c.want {
			t.Errorf("parseSessionCmd(%q) = %q, want %q", c.q, got, c.want)
		}
	}
}

func describeCmd(c *sessionCmd) string {
	local := ""
	if c.local {
		local = "local "
	}
	switch c.kind {
	case cmdSet:
		var v []string
		for _, x := range c.vals {
			v = append(v, x.text)
		}
		return fmt.Sprintf("set %s%s [%s]", local, c.param, strings.Join(v, " "))
	case cmdReset:
		if c.all {
			return "reset all"
		}
		return "reset " + c.param
	case cmdShow:
		if c.all {
			return "show all"
		}
		return "show " + c.param
	case cmdNoop:
		return "noop"
	case cmdSetSession:
		if c.modes.readOnly {
			return "session ro"
		}
		return "session " + c.modes.iso
	case cmdSetTx:
		return "tx " + c.modes.iso
	case cmdBegin:
		if c.modes.iso != "" {
			return "begin " + c.modes.iso
		}
		return "begin"
	case cmdCommit:
		return "commit"
	case cmdRollback:
		return "rollback"
	case cmdFunc:
		names := make([]string, len(c.fns))
		for i, f := range c.fns {
			names[i] = f.fn
			if f.param != "" {
				names[i] += "(" + f.param + ")"
			}
			if f.missingOK {
				names[i] += "?"
			}
		}
		return "func " + strings.Join(names, ",")
	}
	return "?"
}

// ---- cancel ----------------------------------------------------------------

// A cancel request with the connection's process number and key stops the
// statement it runs, which answers 57014 in PostgreSQL's words; the
// connection goes on. One with the wrong key, or for no statement, stops
// nothing.
func TestCancel_stopsTheRunningStatement(t *testing.T) {
	l := &copyLogin{running: make(chan struct{}, 1), release: make(chan struct{})}
	addr := serveAddrCfg(t, Config{Login: l.login})
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://app:%s@%s/orders?sslmode=disable", portPassword, addr))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	pc := conn.PgConn()
	if pc.PID() == 0 || len(pc.SecretKey()) != 4 {
		t.Fatalf("pid %d key %x", pc.PID(), pc.SecretKey())
	}

	run := func() chan error {
		done := make(chan error, 1)
		go func() {
			_, err := pc.Exec(context.Background(), "SELECT block").ReadAll()
			done <- err
		}()
		<-l.running
		return done
	}

	// The wrong key stops nothing.
	done := run()
	sendCancel(t, addr, nil, pc.PID(), []byte{1, 2, 3, 4})
	sendCancel(t, addr, nil, pc.PID()+1, pc.SecretKey())
	select {
	case err := <-done:
		t.Fatalf("a wrong cancel stopped the statement: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	l.release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatalf("the statement after the wrong cancels: %v", err)
	}

	// The right one stops it.
	done = run()
	if err := pc.CancelRequest(ctx); err != nil {
		t.Fatal(err)
	}
	err = <-done
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "57014" || pe.Message != "canceling statement due to user request" {
		t.Fatalf("a canceled statement: %v", err)
	}
	// The connection goes on, and a cancel between statements stops nothing.
	if err := pc.CancelRequest(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := pc.Exec(ctx, "SELECT 1").ReadAll(); err != nil {
		t.Fatalf("the connection after the cancel: %v", err)
	}

	// statement_timeout stops a statement the same way, in its own words.
	if _, err := pc.Exec(ctx, "SET statement_timeout = 100").ReadAll(); err != nil {
		t.Fatal(err)
	}
	_, err = pc.Exec(ctx, "SELECT block").ReadAll()
	<-l.running
	if !errors.As(err, &pe) || pe.Code != "57014" || pe.Message != "canceling statement due to statement timeout" {
		t.Fatalf("a statement past statement_timeout: %v", err)
	}
}

// sendCancel sends a cancel request for pid with key, in clear text or over
// TLS (with tlsCfg), and waits for the port to close the connection.
func sendCancel(t *testing.T, addr string, tlsCfg *tls.Config, pid uint32, key []byte) {
	t.Helper()
	var c net.Conn = rawStart(t, addr)
	if tlsCfg != nil {
		if b := request(t, c, sslRequestCode); b != 'S' {
			t.Fatalf("SSLRequest answered %q", b)
		}
		tc := tls.Client(c, tlsCfg)
		if err := tc.Handshake(); err != nil {
			t.Fatal(err)
		}
		c = tc
	}
	m, err := (&pgproto3.CancelRequest{ProcessID: pid, SecretKey: key}).Encode(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(m); err != nil {
		t.Fatal(err)
	}
	// The port says nothing and closes.
	b := make([]byte, 1)
	if n, err := c.Read(b); n != 0 || err == nil {
		t.Fatalf("the cancel connection read %d bytes, %v", n, err)
	}
}

// A cancel request is taken over TLS (after an SSLRequest, or a TLS handshake
// straight away) and in clear text on a port that serves TLS, as libpq 16
// sends it whatever the connection's sslmode; and on a port that has all the
// connections it takes, when stopping a statement matters most.
func TestCancel_overTLSAndOnAFullPort(t *testing.T) {
	_, srvCfg := testCertConfig(t, testutil.TestCertOptions{})
	for _, maxConns := range []int{0, 1} {
		for _, how := range []string{"sslrequest", "direct", "clear"} {
			t.Run(fmt.Sprintf("max%d/%s", maxConns, how), func(t *testing.T) {
				l := &copyLogin{running: make(chan struct{}, 1), release: make(chan struct{})}
				addr := serveAddrCfg(t, Config{Login: l.login, TLS: srvCfg, MaxConns: maxConns})
				host, port, _ := net.SplitHostPort(addr)
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				conn, err := pgx.Connect(ctx, fmt.Sprintf("host=%s port=%s user=app password=%s dbname=orders sslmode=require", host, port, portPassword))
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close(context.Background())
				pc := conn.PgConn()
				done := make(chan error, 1)
				go func() {
					_, err := pc.Exec(context.Background(), "SELECT block").ReadAll()
					done <- err
				}()
				<-l.running
				client := &tls.Config{InsecureSkipVerify: true}
				switch how {
				case "sslrequest":
					sendCancel(t, addr, client, pc.PID(), pc.SecretKey())
				case "clear":
					sendCancel(t, addr, nil, pc.PID(), pc.SecretKey())
				case "direct":
					c := rawStart(t, addr)
					tc := tls.Client(c, &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"postgresql"}})
					if err := tc.Handshake(); err != nil {
						t.Fatal(err)
					}
					m, _ := (&pgproto3.CancelRequest{ProcessID: pc.PID(), SecretKey: pc.SecretKey()}).Encode(nil)
					if _, err := tc.Write(m); err != nil {
						t.Fatal(err)
					}
					b := make([]byte, 1)
					if n, err := tc.Read(b); n != 0 || err == nil {
						t.Fatalf("the cancel connection read %d bytes, %v", n, err)
					}
				}
				select {
				case err := <-done:
					var pe *pgconn.PgError
					if !errors.As(err, &pe) || pe.Code != "57014" {
						t.Fatalf("the statement: %v, want 57014", err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("the cancel request did not stop the statement")
				}
			})
		}
	}
}

func TestCancelRegistry(t *testing.T) {
	r := newCancelRegistry()
	seen := map[uint32]bool{}
	var releases []func()
	for range 1000 {
		tg, release := r.register()
		if tg.pid == 0 || tg.pid > 1<<31-1 || seen[tg.pid] {
			t.Fatalf("process number %d (seen before: %v)", tg.pid, seen[tg.pid])
		}
		seen[tg.pid] = true
		releases = append(releases, release)
	}
	tg, release := r.register()
	stopped := 0
	done := tg.running(func(error) { stopped++ })
	for _, key := range [][]byte{nil, {}, tg.key[:3], append(tg.key[:4:4], 0), {tg.key[0] ^ 1, tg.key[1], tg.key[2], tg.key[3]}} {
		if found, matched := r.cancel(tg.pid, key); !found || matched {
			t.Errorf("key %x: found %v matched %v", key, found, matched)
		}
	}
	if stopped != 0 {
		t.Fatal("a wrong key stopped the statement")
	}
	if _, matched := r.cancel(tg.pid, tg.key[:]); !matched || stopped != 1 {
		t.Fatalf("the right key: matched %v stopped %d", matched, stopped)
	}
	done()
	r.cancel(tg.pid, tg.key[:])
	if stopped != 1 {
		t.Fatal("a cancel between statements stopped something")
	}
	release()
	if found, _ := r.cancel(tg.pid, tg.key[:]); found {
		t.Fatal("a connection that ended is still on the list")
	}
	for _, f := range releases {
		f()
	}
	if len(r.conns) != 0 {
		t.Fatalf("%d connections left on the list", len(r.conns))
	}
	// A nil list (a test's port) cancels nothing and does not fail.
	var none *cancelRegistry
	if tg, release := none.register(); tg != nil {
		t.Fatal("a nil list registered a connection")
	} else {
		release()
	}
	handleCancel(none, 1, []byte{1, 2, 3, 4}, discardLogger())
}

// The infinite dates and timestamps are sent in binary as PostgreSQL sends
// them (the bytes PostgreSQL 16 sent on the test box), and a timestamp with
// time zone is read with whatever offset its zone had.
func TestBinaryCell_infinityAndOffsets_2769(t *testing.T) {
	for _, c := range []struct {
		duck, text, want string
	}{
		{"DATE", "infinity", "7fffffff"},
		{"DATE", "-infinity", "80000000"},
		{"TIMESTAMP", "infinity", "7fffffffffffffff"},
		{"TIMESTAMP", "-infinity", "8000000000000000"},
		{"TIMESTAMP WITH TIME ZONE", "infinity", "7fffffffffffffff"},
		{"TIMESTAMP WITH TIME ZONE", "-infinity", "8000000000000000"},
		{"DATE[]", "{infinity,-infinity}", "00000001000000000000043a0000000200000001000000047fffffff0000000480000000"},
	} {
		got, err := binaryCell(columnType(c.duck), []byte(c.text))
		if err != nil || fmt.Sprintf("%x", got) != c.want {
			t.Errorf("%s %q: %x %v, want %s", c.duck, c.text, got, err, c.want)
		}
	}
	utc, _ := binaryCell(columnType("TIMESTAMPTZ"), []byte("2026-01-01 10:00:00.5+00"))
	for _, text := range []string{"2026-01-01 05:00:00.5-05", "2026-01-01 15:30:00.5+05:30", "2026-01-01 10:19:32.5+00:19:32"} {
		got, err := binaryCell(columnType("TIMESTAMPTZ"), []byte(text))
		if err != nil || fmt.Sprintf("%x", got) != fmt.Sprintf("%x", utc) {
			t.Errorf("%q: %x %v, want %x (the same instant)", text, got, err, utc)
		}
	}
	for _, text := range []string{"infinity+00", "Infinity", "2026-01-01 10:00:00", "2026-01-01 10:00:00+5", "2026-01-01 10:00:00+16"} {
		if got, err := binaryCell(columnType("TIMESTAMPTZ"), []byte(text)); err == nil {
			t.Errorf("%q: %x, want an error", text, got)
		}
	}
}

// Time travel reads its AS OF moment in UTC: under another session zone it
// is refused by name, never read hours off.
func TestSettings_timeTravelUnderAnotherZoneIsRefused(t *testing.T) {
	l := &copyLogin{}
	login := func(ctx context.Context, user, db string, pw *Password, drop func()) (*Binding, *Error) {
		b, e := l.login(ctx, user, db, pw, drop)
		if b != nil {
			b.TimeTravel = newDeadEngine(t)
		}
		return b, e
	}
	addr := serveAddrCfg(t, Config{Login: login})
	r, err := dialStatus(t, addr, map[string]string{"TimeZone": "America/Bogota"})
	if err != nil {
		t.Fatal(err)
	}
	got := r.query("SELECT * FROM _flashback.orders AS OF '2026-07-01 09:00:00' WHERE id = 1")
	if !strings.HasPrefix(got[0], "Error 0A000") || !strings.Contains(got[0], "reads AS OF in UTC") {
		t.Fatalf("time travel under America/Bogota: %q", got)
	}
	// In UTC it reaches the engine (whose index nobody answers on).
	r.query("SET TimeZone = 'UTC'")
	got = r.query("SELECT * FROM _flashback.orders AS OF '2026-07-01 09:00:00' WHERE id = 1")
	if strings.Contains(got[0], "reads AS OF in UTC") {
		t.Fatalf("time travel in UTC: %q", got)
	}
}
