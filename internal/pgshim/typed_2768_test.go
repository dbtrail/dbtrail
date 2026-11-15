package pgshim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2768: the columns of SQL on the copy are announced with their PostgreSQL
// types, and sent in binary when a client asks, against an executor that
// answers with fixed typed columns.

// typedLogin binds every connection to answer(statement).
type typedLogin struct {
	mu     sync.Mutex
	ran    []string
	answer func(statement string) (sqlsandbox.Result, error)
}

func (l *typedLogin) login(_ context.Context, _, _ string, password *Password, _ func()) (*Binding, *Error) {
	if !password.Matches([]string{portPassword}) {
		return nil, &Error{Code: "28P01", Message: "no"}
	}
	return &Binding{Actor: "server:t", Schema: "public", Run: func(_ context.Context, st string) (sqlsandbox.Result, error) {
		l.mu.Lock()
		l.ran = append(l.ran, st)
		l.mu.Unlock()
		return l.answer(st)
	}}, nil
}

func (l *typedLogin) statements() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ran...)
}

func dialTyped(t *testing.T, l *typedLogin, mode pgx.QueryExecMode) *pgx.Conn {
	t.Helper()
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
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// everyType is one row with a column of each type the copy answers with,
// and one with every cell NULL, in the cell shapes the sandbox hands over.
func everyType(st string) (sqlsandbox.Result, error) {
	cols := []sqlsandbox.Column{
		{Name: "i", Type: "INTEGER"}, {Name: "b", Type: "BIGINT"}, {Name: "s", Type: "SMALLINT"},
		{Name: "n", Type: "DECIMAL(10,2)"}, {Name: "h", Type: "HUGEINT"}, {Name: "f", Type: "DOUBLE"},
		{Name: "r", Type: "FLOAT"}, {Name: "ok", Type: "BOOLEAN"}, {Name: "d", Type: "DATE"},
		{Name: "ts", Type: "TIMESTAMP"}, {Name: "tz", Type: "TIMESTAMPTZ"}, {Name: "tm", Type: "TIME"},
		{Name: "iv", Type: "INTERVAL"}, {Name: "u", Type: "UUID"}, {Name: "by", Type: "BLOB"},
		{Name: "t", Type: "VARCHAR"}, {Name: "a", Type: "INTEGER[]"}, {Name: "st", Type: `STRUCT("a" INTEGER)`},
	}
	res := sqlsandbox.Result{Columns: cols}
	if strings.Contains(st, "dbtrail_describe") {
		return res, nil
	}
	res.Rows = [][]any{
		{json.Number("42"), json.Number("9007199254740993"), json.Number("-7"), "10.50", "170141183460469231731687303715884105727",
			json.Number("-0"), json.Number("1.100000023841858"), true, "2026-03-04",
			"2026-03-04T05:06:07.123456Z", "2026-03-04T05:06:07Z", "12:00:00.5",
			"1 year 2 months -3 days 04:05:06.5", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "0xDEAD",
			"año", []any{json.Number("1"), nil}, map[string]any{"a": json.Number("1")}},
		make([]any, len(cols)),
	}
	return res, nil
}

// Every type the copy answers with is announced as PostgreSQL's, on the
// simple protocol and on each extended mode; a type with none is text.
func TestTyped_rowDescriptionCarriesTheTypes_2768(t *testing.T) {
	want := []uint32{23, 20, 21, 1700, 1700, 701, 700, 16, 1082, 1114, 1184, 1083, 1186, 2950, 17, 25, 1007, 25}
	modes := map[string]pgx.QueryExecMode{"SimpleProtocol": pgx.QueryExecModeSimpleProtocol}
	for k, v := range extendedModes {
		modes[k] = v
	}
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := dialTyped(t, &typedLogin{answer: everyType}, mode)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			rows, err := conn.Query(ctx, "SELECT * FROM t")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			fds := rows.FieldDescriptions()
			for i, f := range fds {
				if f.DataTypeOID != want[i] {
					t.Errorf("column %s: OID %d, want %d", f.Name, f.DataTypeOID, want[i])
				}
			}
			if fds[3].TypeModifier != 10<<16|2+4 {
				t.Errorf("numeric(10,2): type modifier %d", fds[3].TypeModifier)
			}
			n := 0
			for rows.Next() {
				n++
			}
			if err := rows.Err(); err != nil || n != 2 {
				t.Fatalf("rows %d, %v", n, err)
			}
		})
	}
}

// pgx reads every column as its Go type, in each mode: the binary ones
// (pgx asks binary for the types it knows) and the text ones give the same
// values. NULL is NULL in either format.
func TestTyped_pgxReadsTypedValues_2768(t *testing.T) {
	modes := map[string]pgx.QueryExecMode{"SimpleProtocol": pgx.QueryExecModeSimpleProtocol}
	for k, v := range extendedModes {
		modes[k] = v
	}
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := dialTyped(t, &typedLogin{answer: everyType}, mode)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			rows, err := conn.Query(ctx, "SELECT * FROM t")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			if !rows.Next() {
				t.Fatal(rows.Err())
			}
			var (
				i     int32
				b     int64
				s     int16
				n     pgtype.Numeric
				h     pgtype.Numeric
				f     float64
				r     float32
				ok    bool
				d, ts time.Time
				tz    time.Time
				tm    pgtype.Time
				iv    pgtype.Interval
				u     [16]byte
				by    []byte
				txt   string
				a     []*int32
				st    string
			)
			if err := rows.Scan(&i, &b, &s, &n, &h, &f, &r, &ok, &d, &ts, &tz, &tm, &iv, &u, &by, &txt, &a, &st); err != nil {
				t.Fatal(err)
			}
			nv, _ := n.Value()
			hv, _ := h.Value()
			got := fmt.Sprintf("%d %d %d %v %v %v %v %v %s %s %s %d %d/%d/%d %x %x %s %d,%v %s",
				i, b, s, nv, hv, f, r, ok, d.Format("2006-01-02"), ts.Format(time.RFC3339Nano), tz.UTC().Format(time.RFC3339Nano),
				tm.Microseconds, iv.Months, iv.Days, iv.Microseconds, u, by, txt, *a[0], a[1], st)
			want := "42 9007199254740993 -7 10.50 170141183460469231731687303715884105727 -0 1.1 true 2026-03-04 " +
				"2026-03-04T05:06:07.123456Z 2026-03-04T05:06:07Z 43200500000 14/-3/14706500000 a0eebc999c0b4ef8bb6d6bb9bd380a11 dead año 1,<nil> {\"a\":1}"
			if got != want {
				t.Errorf("got  %s\nwant %s", got, want)
			}
			if !rows.Next() {
				t.Fatal(rows.Err())
			}
			vals, err := rows.Values()
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range vals {
				if v != nil {
					t.Errorf("column %d of the NULL row: %v", k, v)
				}
			}
		})
	}
}

// What a client asks for in binary and has none is refused, with no row
// sent: the engine's "infinity" date is no date PostgreSQL holds. In text
// the same cell is sent as it always was.
func TestTyped_aCellWithNoBinaryFormIsRefused_2768(t *testing.T) {
	l := &typedLogin{answer: func(string) (sqlsandbox.Result, error) {
		return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "d", Type: "DATE"}}, Rows: [][]any{{"2026-01-01"}, {"5881580-07-11"}}}, nil
	}}
	conn := dialTyped(t, l, pgx.QueryExecModeCacheStatement)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := conn.Query(ctx, "SELECT d FROM t")
	if err == nil {
		n := 0
		for rows.Next() {
			n++
		}
		err = rows.Err()
		if n != 0 {
			t.Errorf("%d rows were read before the refusal", n)
		}
	}
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "0A000" || !strings.Contains(pe.Message, `column "d" cannot be sent in binary format`) {
		t.Fatalf("binary: %v, want 0A000 naming the column", err)
	}
	var got []string
	rows, err = conn.Query(ctx, "SELECT d FROM t", pgx.QueryResultFormats{pgx.TextFormatCode})
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		got = append(got, string(rows.RawValues()[0]))
	}
	if rows.Err() != nil || strings.Join(got, ",") != "2026-01-01,5881580-07-11" {
		t.Fatalf("text: %q, %v", got, rows.Err())
	}
}

// An execution whose column comes back with another type than the Describe
// announced is held to the announced one, which is how pgx decodes it: a
// cell that reads as that type is sent as it, one that does not refuses the
// execution in PostgreSQL's words, never bytes read as another value.
func TestTyped_anExecutionIsHeldToItsDescribedTypes_2768(t *testing.T) {
	for _, c := range []struct {
		name, cell, executed string
		want                 string
	}{
		{"a bigint text reads as the described integer", "41", "VARCHAR", "41"},
		{"a wider whole number", "41", "BIGINT", "41"},
		{"text that is no integer", "hello", "VARCHAR", "error 0A000 cached plan must not change result type"},
		{"a bigint past an integer", "5000000000", "BIGINT", "error 0A000 cached plan must not change result type"},
	} {
		t.Run(c.name, func(t *testing.T) {
			l := &typedLogin{answer: func(st string) (sqlsandbox.Result, error) {
				if strings.Contains(st, "dbtrail_describe") {
					return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "x", Type: "INTEGER"}}}, nil
				}
				cell := any(c.cell)
				if c.executed == "BIGINT" {
					cell = json.Number(c.cell)
				}
				return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "x", Type: c.executed}}, Rows: [][]any{{cell}}}, nil
			}}
			conn := dialTyped(t, l, pgx.QueryExecModeCacheStatement)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var got string
			var n int32
			if err := conn.QueryRow(ctx, "SELECT x FROM t WHERE id = $1::int", 1).Scan(&n); err != nil {
				var pe *pgconn.PgError
				if errors.As(err, &pe) {
					got = "error " + pe.Code + " " + pe.Message
				} else {
					got = err.Error()
				}
			} else {
				got = fmt.Sprint(n)
			}
			if got != c.want {
				t.Fatalf("%s, want %s", got, c.want)
			}
		})
	}
}

// An argument the client gave no type is described as a NULL, which the
// engine answers as an INTEGER, and runs as the client's string: SELECT $1
// is announced as text (as PostgreSQL reads an argument of unknown type
// there), so "hello" is answered. A column that does not move with the
// argument keeps its type.
func TestTyped_anUntypedArgumentIsText_2768(t *testing.T) {
	l := &typedLogin{}
	l.answer = func(st string) (sqlsandbox.Result, error) {
		arg := "INTEGER"
		if strings.Contains(st, "CAST(NULL AS VARCHAR)") {
			arg = "VARCHAR"
		}
		cols := []sqlsandbox.Column{{Name: "x", Type: arg}, {Name: "id", Type: "INTEGER"}}
		if strings.Contains(st, "dbtrail_describe") {
			return sqlsandbox.Result{Columns: cols}, nil
		}
		cols[0].Type = "VARCHAR"
		return sqlsandbox.Result{Columns: cols, Rows: [][]any{{"hello", json.Number("7")}}}, nil
	}
	conn := dialTyped(t, l, pgx.QueryExecModeCacheStatement)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var x string
	var id int32
	if err := conn.QueryRow(ctx, "SELECT $1 AS x, id FROM t", "hello").Scan(&x, &id); err != nil {
		t.Fatal(err)
	}
	if x != "hello" || id != 7 {
		t.Fatalf("%q %d", x, id)
	}
	sd, err := conn.Prepare(ctx, "again", "SELECT $1 AS x, id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if sd.Fields[0].DataTypeOID != 25 || sd.Fields[1].DataTypeOID != 23 {
		t.Fatalf("described %d %d, want text and integer", sd.Fields[0].DataTypeOID, sd.Fields[1].DataTypeOID)
	}
	// Asked twice, the second time with a NULL of text.
	var probes []string
	for _, st := range l.statements() {
		if strings.Contains(st, "dbtrail_describe") && strings.Contains(st, "AS x") {
			probes = append(probes, st)
		}
	}
	if len(probes) < 2 || !strings.Contains(probes[1], "CAST(NULL AS VARCHAR)") {
		t.Fatalf("the describes: %q", probes)
	}
}

// A column announced as text is sent as its bytes in either format.
func TestTyped_textIsItsBytesInBinary_2768(t *testing.T) {
	if b, err := binaryCell(textColumn, []byte("42")); err != nil || string(b) != "42" {
		t.Fatalf("%q %v", b, err)
	}
}
