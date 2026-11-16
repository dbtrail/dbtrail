package pgshim

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// Per-statement routing (#2770): which statements the copy may answer when
// the source can, and that a statement the copy does not answer goes to the
// source before the client is told anything.

// routeTypes are the copy's column types of internal/pgcompare's seed, as
// the copy engine names them.
var routeTypes = map[string][]string{
	"id": {"INTEGER", "BIGINT"}, "customer_id": {"INTEGER"}, "amount": {"DECIMAL(12,2)"}, "qty": {"INTEGER"},
	"ratio": {"DOUBLE"}, "status": {"VARCHAR"}, "note": {"VARCHAR"}, "placed_at": {"TIMESTAMP"},
	"seen_at": {"TIMESTAMP WITH TIME ZONE"}, "tag": {"UUID"}, "attrs": {"VARCHAR"}, "payload": {"BLOB"},
	"labels": {"VARCHAR[]"}, "name": {"VARCHAR"}, "city": {"VARCHAR"}, "active": {"BOOLEAN"}, "signup": {"DATE"},
	"free": {"DECIMAL(38,18)"}, "fixed": {"DECIMAL(10,2)"}, "span": {"INTERVAL"}, "starts": {"TIME"},
}

func TestCopyScreen(t *testing.T) {
	copyOK := []string{
		`SELECT 1`,
		`select count(*) from pgcmp_orders`,
		`SELECT id, note FROM pgcmp_orders WHERE status = 'new'`,
		`SELECT o.id, c.name FROM pgcmp_orders o JOIN pgcmp_customers c ON c.id = o.customer_id`,
		`SELECT id FROM pgcmp_orders ORDER BY id`,
		`SELECT id FROM pgcmp_orders ORDER BY amount DESC NULLS LAST, id LIMIT 3`,
		`SELECT id FROM pgcmp_orders WHERE status = 'paid' ORDER BY id`,
		`SELECT id FROM pgcmp_orders WHERE placed_at < TIMESTAMP '2026-02-01 00:00:00'`,
		`SELECT name FROM pgcmp_customers WHERE signup >= DATE '2026-01-01'`,
		`SELECT id FROM pgcmp_orders WHERE placed_at::date > '2026-01-01'::date`,
		`SELECT id, CAST(placed_at AS date) FROM pgcmp_orders`,
		`SELECT id, qty / 2 AS half FROM pgcmp_orders`,
		`SELECT id, ratio / 2 FROM pgcmp_orders`,
		`SELECT id, amount * qty FROM pgcmp_orders`,
		`SELECT status || qty FROM pgcmp_orders`,
		`SELECT 'a' || 'b'`,
		`SELECT id, attrs->>'k' FROM pgcmp_orders`,
		`SELECT id, labels[1] FROM pgcmp_orders WHERE 'red' = ANY(labels)`,
		`SELECT date_trunc('month', placed_at) AS m, count(*) FROM pgcmp_orders GROUP BY 1`,
		`SELECT id, extract(year FROM placed_at) FROM pgcmp_orders`,
		`SELECT id, date_part('year', placed_at) FROM pgcmp_orders`,
		`SELECT customer_id, count(*) FROM pgcmp_orders GROUP BY customer_id HAVING count(*) > 2`,
		`SELECT id, row_number() OVER (ORDER BY id) FROM pgcmp_orders`,
		`SELECT count(*) FILTER (WHERE qty > 5) FROM pgcmp_orders`,
		`SELECT DISTINCT ON (customer_id) customer_id, id FROM pgcmp_orders ORDER BY customer_id, id`,
		`WITH big AS (SELECT * FROM pgcmp_orders WHERE qty >= 10) SELECT count(*) FROM big`,
		`SELECT id, CASE WHEN qty > 5 THEN 'big' ELSE 'small' END FROM pgcmp_orders`,
		`SELECT min(seen_at), max(amount) FROM pgcmp_orders`,
		`SELECT id FROM pgcmp_orders WHERE qty BETWEEN 2 AND 9`,
		`SELECT id FROM pgcmp_orders WHERE note IS DISTINCT FROM 'x'`,
		`SELECT id FROM pgcmp_orders WHERE status LIKE 'p%'`,
		`SELECT id FROM pgcmp_orders WHERE id = $1`,
		`SELECT id FROM pgcmp_orders WHERE qty > $1 ORDER BY id LIMIT $2`,
		`SELECT id FROM pgcmp_orders WHERE id > '0'`,
		`SELECT id FROM pgcmp_orders WHERE '3' <= qty`,
		`SELECT id FROM pgcmp_orders WHERE placed_at BETWEEN '2026-01-01' AND '2026-02-01'`,
		`SELECT "at" FROM t`,
		`SELECT now FROM t`,
		`SELECT id FROM pgcmp_orders WHERE note = 'now() FOR UPDATE ~ x'`,
		`SELECT id /* now() */ FROM pgcmp_orders -- FOR UPDATE`,
		`SELECT id, substring(note FROM 1 FOR 2) FROM pgcmp_orders`,
		`SELECT id FROM pgcmp_orders WHERE tag = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'`,
		`(SELECT id FROM pgcmp_orders ORDER BY id)`,
		`SELECT id FROM pgcmp_orders WHERE id < 3 UNION ALL SELECT id FROM pgcmp_customers`,
		`SELECT sum(fixed) FROM pgcmp_odd`,
	}
	for _, q := range copyOK {
		if why := CopyScreen(q, routeTypes); why != "" {
			t.Errorf("%s: sent to the source (%s), want the copy", q, why)
		}
	}
	toSource := map[string]string{
		// Measured by pgcompare as answered otherwise or refused.
		`SELECT avg(qty) FROM pgcmp_orders`:         "avg",
		`SELECT id, round(ratio) FROM pgcmp_orders`: "round",
		`SELECT id, amount / 2 FROM pgcmp_orders`:   "division",
		`SELECT id, qty / 2.0 FROM pgcmp_orders`:    "division",
		`SELECT id, qty / 0 FROM pgcmp_orders`:      "division",
		`SELECT id, amount / qty FROM pgcmp_orders`: "division",
		`SELECT 7 / 2`: "division",
		`SELECT sum(qty) / count(*) FROM pgcmp_orders`:               "division",
		`SELECT id, qty % 3 FROM pgcmp_orders`:                       "%",
		`SELECT id FROM pgcmp_orders WHERE status ~ '^p'`:            "~",
		`SELECT id FROM pgcmp_orders WHERE status SIMILAR TO 'p%'`:   "SIMILAR",
		`SELECT id FROM pgcmp_orders WHERE attrs ? 'n'`:              "?",
		`SELECT id, attrs->'k' FROM pgcmp_orders`:                    "->",
		`SELECT id, attrs #>> '{k}' FROM pgcmp_orders`:               "#>>",
		`SELECT id FROM pgcmp_orders WHERE attrs @> '{"k": "v"}'`:    "@>",
		`SELECT id, ratio::text FROM pgcmp_orders`:                   "cast",
		`SELECT id, round(ratio::numeric, 1) FROM pgcmp_orders`:      "cast",
		`SELECT 'x'::regclass`:                                       "cast",
		`SELECT id, CAST(qty AS bigint) FROM pgcmp_orders`:           "CAST",
		`SELECT generate_series(1, 3)`:                               "generate_series",
		`SELECT id, to_char(placed_at, 'YYYY-MM') FROM pgcmp_orders`: "to_char",
		`SELECT upper(name) FROM pgcmp_customers`:                    "upper",
		`SELECT id FROM pgcmp_orders WHERE note ILIKE 'F%'`:          "ILIKE",
		`SELECT id, seen_at AT TIME ZONE 'UTC' FROM pgcmp_orders`:    "zone",
		`SELECT id FROM pgcmp_orders ORDER BY note COLLATE "C"`:      "COLLATE",
		`SELECT id FROM pgcmp_orders FOR UPDATE`:                     "FOR UPDATE",
		`SELECT id FROM pgcmp_orders FOR SHARE`:                      "FOR UPDATE",
		`SELECT nextval('s')`:                                        "nextval",
		// Text by order.
		`SELECT name FROM pgcmp_customers ORDER BY name`:                    "ORDER BY",
		`SELECT name FROM pgcmp_customers ORDER BY 1`:                       "ORDER BY",
		`SELECT name AS n FROM pgcmp_customers ORDER BY n`:                  "ORDER BY",
		`SELECT min(name), max(name) FROM pgcmp_customers`:                  "min",
		`SELECT name FROM pgcmp_customers WHERE name > 'B'`:                 "order",
		`SELECT 'a' < 'B' AS lt`:                                            "order",
		`SELECT id FROM t WHERE $1 < $2`:                                    "order",
		`SELECT greatest(name, city) FROM pgcmp_customers`:                  "greatest",
		`SELECT id FROM pgcmp_customers WHERE name BETWEEN 'a' AND 'c'`:     "BETWEEN",
		`SELECT id FROM pgcmp_customers WHERE name > $1`:                    "order",
		`SELECT id FROM t WHERE unknowncol > '0'`:                           "order",
		`SELECT id, labels FROM pgcmp_orders ORDER BY labels`:               "ORDER BY",
		`SELECT string_agg(status, ',' ORDER BY status) FROM pgcmp_orders`:  "ORDER BY",
		`SELECT id, row_number() OVER (ORDER BY name) FROM pgcmp_customers`: "ORDER BY",
		// The clock and the session.
		`SELECT now()`: "now",
		`SELECT id FROM pgcmp_orders WHERE placed_at > now() - interval '1 day'`: "now",
		`SELECT current_date`:          "CURRENT_DATE",
		`SELECT CURRENT_TIMESTAMP`:     "CURRENT_TIMESTAMP",
		`SELECT localtimestamp`:        "LOCALTIMESTAMP",
		`SELECT current_user`:          "CURRENT_USER",
		`SELECT clock_timestamp()`:     "clock_timestamp",
		`SELECT random()`:              "random",
		`SELECT "now"()`:               "now",
		`SELECT pg_catalog.lower('A')`: "pg_catalog",
		// Constructs not measured.
		`WITH RECURSIVE r(n) AS (SELECT 1) SELECT * FROM r`:       "RECURSIVE",
		`SELECT * FROM pgcmp_orders TABLESAMPLE SYSTEM (10)`:      "TABLESAMPLE",
		`SELECT * FROM pgcmp_orders, LATERAL (SELECT 1) x`:        "LATERAL",
		`SELECT id INTO x FROM pgcmp_orders`:                      "INTO",
		`SELECT ARRAY[1, 2]`:                                      "ARRAY",
		`SELECT id FROM pgcmp_orders FETCH FIRST 3 ROWS ONLY`:     "FETCH",
		`SELECT id, labels[1:2] FROM pgcmp_orders`:                ":",
		`SELECT TIMESTAMPTZ '2026-01-01 00:00:00+00'`:             "timestamptz",
		`SELECT TIMESTAMP WITH TIME ZONE '2026-01-01'`:            "zone",
		`SELECT id FROM "Orders"`:                                 "capitals",
		`SELECT ~1`:                                               "~",
		`SELECT date_trunc('month', signup) FROM pgcmp_customers`: "date_trunc",
		`SELECT extract(epoch FROM placed_at) FROM pgcmp_orders`:  "extract",
		`SELECT id || ratio FROM pgcmp_orders`:                    "||",
		`SELECT concat(id, ratio) FROM pgcmp_orders`:              "concat",
		// What the copy is never handed.
		`SELECT E'a\nb'`:             "prefix",
		`SELECT $$x$$`:               "dollar",
		`SELECT $tag$now()$tag$`:     "dollar",
		`SELECT B'101'`:              "prefix",
		`SELECT U&"d\0061t\+000061"`: "prefix",
		`SELECT 0x1F`:                "number",
		`SELECT 1_000`:               "number",
		`SELECT 'unterminated`:       "unterminated",
		`SELECT 1 /* open`:           "unterminated",
		`VALUES (1)`:                 "SELECT",
		`TABLE pgcmp_orders`:         "SELECT",
		`EXPLAIN SELECT 1`:           "SELECT",
	}
	for q, why := range toSource {
		got := CopyScreen(q, routeTypes)
		if got == "" {
			t.Errorf("%s: left to the copy, want the source (%s)", q, why)
		}
	}
	// Columns not known (the catalog could not be read): any name may be
	// text, so whatever orders is the source's.
	if got := CopyScreen(`SELECT id FROM t ORDER BY id`, nil); got == "" {
		t.Error("ORDER BY with the columns not known was left to the copy")
	}
	if got := CopyScreen(`SELECT count(*) FROM t WHERE id = 3`, nil); got != "" {
		t.Errorf("a statement that orders nothing was sent to the source with the columns not known: %s", got)
	}
}

func TestFingerprint(t *testing.T) {
	for in, want := range map[string]string{
		`SELECT id, note FROM t WHERE id = 42 AND note = 'secret'`:  `select id, note from t where id = ? and note = ?`,
		"select\n  *  from \"My T\" -- c\n where x in (1, 2.5, $1)": `select * from "My T" where x in (?, ?, ?)`,
		`SELECT count(*), now() FROM t`:                             `select count(*), now() from t`,
		`SELECT a.b::date FROM t`:                                   `select a.b::date from t`,
		`SELECT $$hidden value$$`:                                   `select …`,
		`SELECT E'hidden'`:                                          `select …`,
		"  -- nothing\n":                                            ``,
	} {
		if got := Fingerprint(in); got != want {
			t.Errorf("Fingerprint(%q) = %q, want %q", in, got, want)
		}
	}
}

// routeSession is a logged-in session on a port that can reach its source
// (forward true) or cannot, whose copy holds routeTypes' columns.
func routeSession(forward bool) *session {
	b := &Binding{Schema: "public", Catalog: func(context.Context) ([]CatalogTable, error) {
		var cols []CatalogColumn
		for n, ts := range routeTypes {
			cols = append(cols, CatalogColumn{Name: n, Type: ts[0]})
		}
		return []CatalogTable{{Schema: "public", Name: "pgcmp_orders", Columns: cols}}, nil
	}}
	if forward {
		b.Forward = &Forward{DSN: "postgres://fwd:pw@127.0.0.1:1/db"}
	}
	s := &session{bound: b, logger: discardLogger(), currentDB: "public"}
	s.initSettings("public", "app")
	return s
}

func TestRoute_decisions(t *testing.T) {
	type want struct{ side, reason string }
	cases := []struct {
		forward bool
		zone    bool
		stmt    string
		want    want
	}{
		{false, false, `SELECT now()`, want{RouteCopy, ReasonNoForwardAccount}},
		{false, false, `INSERT INTO t VALUES (1)`, want{RouteCopy, ReasonNoForwardAccount}},
		{true, false, `SELECT count(*) FROM pgcmp_orders`, want{RouteCopy, ReasonAnswersTheSame}},
		{true, false, `SELECT now()`, want{RouteSource, ReasonVeto}},
		{true, false, `SELECT $$x$$`, want{RouteSource, ReasonVeto}},
		{true, true, `SELECT count(*) FROM pgcmp_orders`, want{RouteSource, ReasonSessionDiffers}},
		{true, false, `SHOW TABLES`, want{RouteCopy, ReasonCopyCommand}},
		{true, false, `DESCRIBE pgcmp_orders`, want{RouteCopy, ReasonCopyCommand}},
		{true, false, `EXPLAIN SELECT 1`, want{RouteSource, ReasonNotASelect}},
		{true, false, `VALUES (1)`, want{RouteSource, ReasonNotASelect}},
		{true, false, `INSERT INTO t VALUES (1)`, want{RouteRefused, ReasonReadOnly}},
		{true, false, `DELETE FROM t`, want{RouteRefused, ReasonReadOnly}},
		{true, false, `SELECT relname FROM pg_class WHERE relkind = 'v'`, want{RouteSource, ReasonCatalogNotOnCopy}},
		// The port's own: neither side.
		{true, false, `SET search_path = public`, want{}},
		{true, false, `SHOW TimeZone`, want{}},
		{true, false, `BEGIN`, want{}},
	}
	for _, tc := range cases {
		s := routeSession(tc.forward)
		if tc.zone {
			s.set.zone = zoneSetting{name: "America/Bogota", duck: "America/Bogota"}
		}
		got := s.route(s.classify(tc.stmt), tc.stmt)
		if got.side != tc.want.side || got.reason != tc.want.reason {
			t.Errorf("forward=%v %s: %s/%s, want %s/%s", tc.forward, tc.stmt, got.side, got.reason, tc.want.side, tc.want.reason)
		}
	}
	// A block on the source: everything in it is the source's.
	s := routeSession(true)
	s.up = &upstream{inTx: true}
	if got := s.route(s.classify(`SELECT 1`), `SELECT 1`); got.side != RouteSource || got.reason != ReasonInTransaction {
		t.Errorf("in a block: %+v", got)
	}
}

func TestCopyFallback_reasons(t *testing.T) {
	s := routeSession(true)
	for _, tc := range []struct {
		err  error
		perr *pgErr
		want string
	}{
		{&sqlsandbox.BehindSourceError{Reason: "x"}, &pgErr{"55000", "x"}, ReasonCopyBehindSource},
		{&sqlsandbox.BehindSourceError{Reason: "x", Unknown: true}, &pgErr{"55000", "x"}, ReasonCopyAgeUnknown},
		{&sqlsandbox.BusyError{Waited: time.Second}, &pgErr{"53000", "x"}, ReasonCopyWaitTimeout},
		{sqlsandbox.ErrBusy, &pgErr{"53000", "x"}, ReasonCopyQueueFull},
		{sqlsandbox.ErrResultTooLarge, &pgErr{"54000", "x"}, ReasonResultOverCap},
		{&sqlsandbox.QueryError{Message: "Catalog Error: Table with name x does not exist!"}, &pgErr{"42P01", "x"}, ReasonTableNotOnCopy},
		{&sqlsandbox.QueryError{Message: "Parser Error: syntax error at or near \"at\""}, &pgErr{"42601", "x"}, ReasonCopyRefused},
		{&sqlsandbox.TimeoutError{Limit: time.Second}, &pgErr{"57014", "x"}, ""},
		{&stoppedErr{&pgErr{"57014", "cancelled"}}, &pgErr{"57014", "cancelled"}, ""},
		{context.Canceled, &pgErr{"57014", "x"}, ""},
	} {
		if got := s.copyFallback(tc.err, tc.perr); got != tc.want {
			t.Errorf("%v: %q, want %q", tc.err, got, tc.want)
		}
	}
	// Without a forwarding account there is nothing to fall back to.
	if got := routeSession(false).copyFallback(errors.New("x"), &pgErr{"XX000", "x"}); got != "" {
		t.Errorf("without a forwarding account: %q", got)
	}
}

// fallbackLogin is a port whose copy fails or answers as asked, and whose
// source (when forward) cannot be reached: a statement the client gets
// 08001 for was sent there.
type fallbackLogin struct {
	forward bool
	// barred bars the source (Forward.Barred), as a server that hides
	// tables or columns does.
	barred bool
	mu     sync.Mutex
	notes  []RouteNote
}

func (l *fallbackLogin) run(_ context.Context, st string, _ RunOptions) (sqlsandbox.Result, error) {
	switch {
	case strings.Contains(st, "missing"):
		return sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: "Catalog Error: Table with name missing does not exist!"}
	case strings.Contains(st, "unsized"):
		return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "free", Type: "DECIMAL(38,18)"}}, Rows: [][]any{{"1.500000000000000000"}}}, nil
	}
	return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, Rows: [][]any{{int32(7)}}}, nil
}

func (l *fallbackLogin) login(_ context.Context, _, _ string, password *Password, _ func()) (*Binding, *Error) {
	if !password.Matches([]string{portPassword}) {
		return nil, &Error{Code: "28P01", Message: "no"}
	}
	b := &Binding{Run: l.run, Actor: "server:orders", Schema: "public", Routed: func(n RouteNote) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.notes = append(l.notes, n)
	}}
	if l.forward || l.barred {
		b.Forward = &Forward{DSN: "postgres://fwd:pw@127.0.0.1:1/db?sslmode=disable&connect_timeout=2"}
	}
	if l.barred {
		b.Forward.Barred = func() string { return "it hides tables" }
	}
	return b, nil
}

func (l *fallbackLogin) last() RouteNote {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.notes) == 0 {
		return RouteNote{}
	}
	return l.notes[len(l.notes)-1]
}

func TestRoute_aStatementTheCopyDoesNotAnswerGoesToTheSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, mode := range []string{"none", "forward", "barred"} {
		forward := mode == "forward"
		l := &fallbackLogin{forward: forward, barred: mode == "barred"}
		addr := serveAddrCfg(t, Config{Login: l.login})
		c, err := pgconn.Connect(ctx, fmt.Sprintf("host=%s port=%s user=orders password=%s sslmode=disable", hostOf(addr), portOf(addr), portPassword))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(context.Background())
		code := func(err error) string {
			var pe *pgconn.PgError
			if errors.As(err, &pe) {
				return pe.Code
			}
			if err != nil {
				return err.Error()
			}
			return ""
		}
		type outcome struct{ code, side, reason string }
		check := func(what string, got, want outcome) {
			t.Helper()
			if got != want {
				t.Errorf("%s %s: %+v, want %+v", mode, what, got, want)
			}
		}
		simple := func(stmt string) outcome {
			_, err := c.Exec(ctx, stmt).ReadAll()
			n := l.last()
			return outcome{code(err), n.Side, n.Reason}
		}
		extended := func(stmt string) outcome {
			err := c.ExecParams(ctx, stmt, nil, nil, nil, nil).Read().Err
			n := l.last()
			return outcome{code(err), n.Side, n.Reason}
		}
		if forward {
			// 08001: the port tried the source, which is down.
			check("a table the copy does not hold", simple("SELECT * FROM missing"), outcome{"08001", RouteSource, ReasonTableNotOnCopy})
			check("a numeric written otherwise", simple("SELECT unsized FROM t"), outcome{"08001", RouteSource, ReasonCopyColumnsDiffer})
			check("now()", simple("SELECT now()"), outcome{"08001", RouteSource, ReasonVeto})
			check("a read the copy answers", simple("SELECT n FROM t"), outcome{"", RouteCopy, ReasonAnswersTheSame})
			check("a write", simple("INSERT INTO t VALUES (1)"), outcome{"25006", RouteRefused, ReasonReadOnly})
			check("extended: a table the copy does not hold", extended("SELECT * FROM missing"), outcome{"08001", RouteSource, ReasonTableNotOnCopy})
			check("extended: now()", extended("SELECT now()"), outcome{"08001", RouteSource, ReasonVeto})
			check("extended: a read the copy answers", extended("SELECT n FROM t"), outcome{"", RouteCopy, ReasonAnswersTheSame})
		} else if mode == "barred" {
			// Nothing reaches the source (no 08001): the copy's own answer or
			// error, a transaction block included.
			check("a table the copy does not hold", simple("SELECT * FROM missing"), outcome{"42P01", RouteCopy, ReasonHiddenRules})
			check("now()", simple("SELECT now()"), outcome{"", RouteCopy, ReasonHiddenRules})
			check("extended: a table the copy does not hold", extended("SELECT * FROM missing"), outcome{"42P01", RouteCopy, ReasonHiddenRules})
			if _, err := c.Exec(ctx, "BEGIN").ReadAll(); err != nil {
				t.Errorf("barred: BEGIN: %v, want it answered by the port", err)
			}
			check("in a block", simple("SELECT now()"), outcome{"", RouteCopy, ReasonHiddenRules})
			if _, err := c.Exec(ctx, "ROLLBACK").ReadAll(); err != nil {
				t.Errorf("barred: ROLLBACK: %v", err)
			}
			if _, err := c.Exec(ctx, "SELECT relname FROM pg_class WHERE relkind = 'v'").ReadAll(); code(err) != "0A000" {
				t.Errorf("barred: a catalog statement the copy does not answer: %v, want the port's refusal", err)
			}
		} else {
			// As before: the copy's own answer or error.
			check("a table the copy does not hold", simple("SELECT * FROM missing"), outcome{"42P01", RouteCopy, ReasonNoForwardAccount})
			check("a numeric written otherwise", simple("SELECT unsized FROM t"), outcome{"", RouteCopy, ReasonNoForwardAccount})
			check("now()", simple("SELECT now()"), outcome{"", RouteCopy, ReasonNoForwardAccount})
			check("extended: a table the copy does not hold", extended("SELECT * FROM missing"), outcome{"42P01", RouteCopy, ReasonNoForwardAccount})
		}
		if st := c.TxStatus(); st != 'I' {
			t.Errorf("%s: the connection is left in status %c", mode, st)
		}
	}
}
