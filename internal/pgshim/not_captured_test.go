package pgshim

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// lostKnob is a host's Binding.NotCaptured: the tables it lists now, or an
// error, and how many times it was asked.
type lostKnob struct {
	mu    sync.Mutex
	lost  []TableNotCaptured
	err   error
	asked atomic.Int32
}

func (k *lostKnob) set(err error, lost ...TableNotCaptured) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.lost, k.err = lost, err
}

func (k *lostKnob) notCaptured(context.Context) ([]TableNotCaptured, error) {
	k.asked.Add(1)
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.lost, k.err
}

var (
	lostBig = TableNotCaptured{Schema: "public", Name: "big", Why: "capture dropped changes of public.big (replica_identity_not_full) since the snapshot's rows were read from the source",
		SnapshotRead: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	notCapturd = costOutcome{"42", "", RouteSource, ReasonTableNotCaptured}
)

// newLostPort is newTablesPort with a host that says which tables capture
// does not keep whole.
func newLostPort(t *testing.T) (*tablesPort, *lostKnob) {
	t.Helper()
	p := newTablesPort(t)
	k := &lostKnob{}
	p.l.notCaptured = k.notCaptured
	return p, k
}

// The defect: a NEW connection, which the source never answered, reading a
// table whose capture drops its changes. The copy holds that table as of
// its snapshot only, and nothing else held the answer back (no source
// answer on the connection, no lag limit). Now the source answers, and the
// copy runs nothing.
func TestNotCaptured_aFreshConnectionReadingADroppedTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, k := newLostPort(t)
	k.set(nil, lostBig)
	for _, tc := range []struct {
		name, stmt string
		want       costOutcome
		runs       int32
	}{
		{"the dropped table", "SELECT sum(n) FROM big", notCapturd, 0},
		{"the dropped table with its schema", "SELECT sum(n) FROM public.big", notCapturd, 0},
		{"a join of the dropped table with a captured one", "SELECT sum(n) FROM wide JOIN big ON big.id = wide.id", notCapturd, 0},
		{"the dropped table in a subquery", "SELECT sum(n) FROM wide WHERE id IN (SELECT id FROM big)", notCapturd, 0},
		{"a captured table alone", "SELECT sum(n) FROM wide", tablesOnCopy, 1},
		{"the same name in another schema, written with it", "SELECT sum(n) FROM other.big", tablesOnCopy, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := costPortConn(t, ctx, p.l)
			runs := p.l.runs.Load()
			if got := costSimple(ctx, c, p.l, tc.stmt); got != tc.want {
				t.Errorf("%+v, want %+v", got, tc.want)
			}
			if d := p.l.runs.Load() - runs; d != tc.runs {
				t.Errorf("the copy ran %d statements, want %d: nothing runs on the copy for a statement the source answers", d, tc.runs)
			}
		})
	}
	// Without the table listed, the same connection reads the copy again:
	// the table's next snapshot read after its last drop heals it.
	c := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != notCapturd {
		t.Fatalf("listed: %+v", got)
	}
	k.set(nil)
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got.side != RouteSource || got.reason != ReasonCopyTooOld {
		// The source answered this connection about big a moment ago, and
		// the copy's answer is a snapshot's: the rule of #2985, as before.
		t.Errorf("no longer listed, after the source answered about it: %+v, want copy_too_old", got)
	}
	if got := costSimple(ctx, costPortConn(t, ctx, p.l), p.l, "SELECT sum(n) FROM big"); got != tablesOnCopy {
		t.Errorf("no longer listed, a new connection: %+v, want the copy", got)
	}
}

// A host that cannot say which tables capture keeps whole (the index cannot
// be read) sends the statement to the source.
func TestNotCaptured_anErrorOfTheHostGoesToTheSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, k := newLostPort(t)
	k.set(errors.New("dial tcp 10.0.3.7:3306: connect: connection refused"))
	c := costPortConn(t, ctx, p.l)
	runs := p.l.runs.Load()
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != notCapturd {
		t.Errorf("%+v, want %+v", got, notCapturd)
	}
	if d := p.l.runs.Load() - runs; d != 0 {
		t.Errorf("the copy ran %d statements, want none", d)
	}
}

// The copy's own commands are not held to it: the source would refuse them.
func TestNotCaptured_theCopysOwnCommands(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, k := newLostPort(t)
	// A ledger that does not say which tables: every table is listed.
	k.set(nil, lostBig, TableNotCaptured{Schema: "public", Name: "wide", Why: "w"})
	c := costPortConn(t, ctx, p.l)
	for _, stmt := range []string{"SHOW TABLES", "DESCRIBE big", "SUMMARIZE big"} {
		if got := costSimple(ctx, c, p.l, stmt); got.code != "" || got.side != RouteCopy || got.reason != ReasonCopyCommand {
			t.Errorf("simple %s: %+v, want the copy's", stmt, got)
		}
		if _, err := c.Prepare(ctx, "", stmt, nil); err != nil {
			t.Errorf("prepare %s: %v", stmt, err)
			continue
		}
		if res := c.ExecPrepared(ctx, "", nil, nil, nil).Read(); res.Err != nil {
			t.Errorf("extended %s: %v", stmt, res.Err)
		}
	}
	if n := k.asked.Load(); n != 0 {
		t.Errorf("the host was asked %d times, want never", n)
	}
}

// A statement the source answers anyway does not ask the host: a cheap one,
// one in a transaction block (which keeps the port's transaction rules), one
// the screen sends there.
func TestNotCaptured_askedOnlyForTheCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, k := newLostPort(t)
	k.set(nil, lostBig, TableNotCaptured{Schema: "public", Name: "t", Why: "t"})
	c := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, c, p.l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource || got.reason != ReasonCheapPlan {
		t.Errorf("a cheap read: %+v", got)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big WHERE now() IS NOT NULL"); got.side != RouteSource || got.reason != ReasonVeto {
		t.Errorf("a read the screen sends to the source: %+v", got)
	}
	for _, stmt := range []string{"BEGIN", "SELECT sum(n) FROM big"} {
		if _, err := c.Exec(ctx, stmt).ReadAll(); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if n := p.l.last(); n.Side != RouteSource || n.Reason != ReasonInTransaction {
		t.Errorf("in a transaction block: %s/%s, want source/in_transaction", n.Side, n.Reason)
	}
	if _, err := c.Exec(ctx, "COMMIT").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if n := k.asked.Load(); n != 0 {
		t.Errorf("the host was asked %d times, want never", n)
	}
}

// Prepared statements (Parse, Bind, Execute) go the same way, and do not
// keep the source once the table is captured whole again.
func TestNotCaptured_preparedStatements(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, k := newLostPort(t)
	k.set(nil, lostBig)
	c := costPortConn(t, ctx, p.l)
	for _, name := range []string{"report", "other"} {
		stmt := map[string]string{"report": "SELECT sum(n) FROM big", "other": "SELECT sum(n) FROM wide"}[name]
		if _, err := c.Prepare(ctx, name, stmt, nil); err != nil {
			t.Fatal(err)
		}
	}
	exec := func(name string) (string, RouteNote) {
		t.Helper()
		res := c.ExecPrepared(ctx, name, nil, nil, nil).Read()
		if res.Err != nil || len(res.Rows) != 1 {
			t.Fatalf("%s: %v %q", name, res.Err, res.Rows)
		}
		return string(res.Rows[0][0]), p.l.last()
	}
	if got, n := exec("other"); got != "7" || n.Side != RouteCopy {
		t.Errorf("a captured table: %s %s/%s, want the copy's 7", got, n.Side, n.Reason)
	}
	for i := range 2 {
		if got, n := exec("report"); got != "42" || n.Side != RouteSource || n.Reason != ReasonTableNotCaptured {
			t.Errorf("run %d, the dropped table: %s %s/%s, want the source's 42, table_not_captured", i, got, n.Side, n.Reason)
		}
	}
	k.set(nil)
	d := costPortConn(t, ctx, p.l)
	if _, err := d.Prepare(ctx, "report", "SELECT sum(n) FROM big", nil); err != nil {
		t.Fatal(err)
	}
	if res := d.ExecPrepared(ctx, "report", nil, nil, nil).Read(); res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "7" {
		t.Errorf("captured whole again, a new connection: %v %q, want the copy's 7", res.Err, res.Rows)
	}
	if !transientFallback(ReasonTableNotCaptured) {
		t.Error("table_not_captured must not keep a prepared statement on the source for good")
	}
}

// A connection that cannot reach its source (no forwarding account) is
// answered by the copy as before, from the snapshot alone, and is told so
// with a NOTICE: there is nowhere else to send the statement.
func TestNotCaptured_noForwardingAccount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	k := &lostKnob{}
	k.set(nil, lostBig)
	l := &costLogin{notCaptured: k.notCaptured, catalog: func() []CatalogTable { return catalogOf("big", "wide") }}
	addr := serveAddrCfg(t, Config{Login: l.login})
	cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=%s port=%s user=orders password=%s sslmode=disable", hostOf(addr), portOf(addr), portPassword))
	if err != nil {
		t.Fatal(err)
	}
	var notices []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) { notices = append(notices, n.Message) }
	c, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	res, err := c.Exec(ctx, "SELECT sum(n) FROM big").ReadAll()
	if err != nil || len(res) != 1 || len(res[0].Rows) != 1 || string(res[0].Rows[0][0]) != "7" {
		t.Fatalf("%v %v, want the copy's 7", err, res)
	}
	if n := l.last(); n.Side != RouteCopy || n.Reason != ReasonNoForwardAccount {
		t.Errorf("%s/%s, want copy/no_forward_account", n.Side, n.Reason)
	}
	if n := k.asked.Load(); n != 1 {
		t.Errorf("the host was asked %d times, want once, for the notice", n)
	}
	if len(notices) != 1 || !strings.Contains(notices[0], "reads public.big") || !strings.Contains(notices[0], "2026-11-21 12:00:00 UTC") || !strings.Contains(notices[0], lostBig.Why) {
		t.Errorf("notices %q, want one that names public.big, its snapshot's time and why", notices)
	}
	// A table capture keeps whole: no notice.
	notices = nil
	if _, err := c.Exec(ctx, "SELECT sum(n) FROM wide").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if len(notices) != 0 {
		t.Errorf("notices for a captured table: %q", notices)
	}
	// The host cannot say: the copy answers, with no notice and no error.
	k.set(errors.New("the index cannot be reached"))
	if res, err := c.Exec(ctx, "SELECT sum(n) FROM big").ReadAll(); err != nil || string(res[0].Rows[0][0]) != "7" {
		t.Errorf("a host error on a connection without forwarding: %v %v, want the copy's 7", err, res)
	}
}

func TestNotCapturedNotice(t *testing.T) {
	got := notCapturedNotice(TableNotCaptured{Schema: "public", Name: "a", Why: "w"}, false)
	if !strings.Contains(got, "may read public.a") || !strings.Contains(got, "from its snapshot,") {
		t.Errorf("a text not read whole, no time known: %q", got)
	}
}

func TestReadsNotCaptured(t *testing.T) {
	lost := []TableNotCaptured{lostBig, {Schema: "sales", Name: "Mixed", Why: "mixed"}}
	for _, tc := range []struct {
		stmt, want string
	}{
		{"SELECT 1", ""},
		{"SELECT sum(n) FROM wide", ""},
		{"SELECT sum(n) FROM big", lostBig.Why},
		{"SELECT sum(n) FROM other.big", ""},
		{"SELECT sum(n) FROM public.big", lostBig.Why},
		{`SELECT sum(n) FROM "BIG"`, lostBig.Why}, // case is not compared: more, never fewer
		{`SELECT sum(n) FROM sales."Mixed"`, "mixed"},
		{"SELECT sum(n) FROM mixed", "mixed"},
		{"SELECT sum(n) FROM db.public.wide", lostBig.Why}, // a database before the schema: any table
		{"WITH big AS (SELECT 1 AS n) SELECT n FROM big", lostBig.Why},
		{"SELECT sum(n) FROM wide, LATERAL (SELECT 1) x", lostBig.Why}, // not read whole: any table
		{"SELECT sum(n) FROM generate_series(1, 3) g(n)", lostBig.Why},
	} {
		if got := readsNotCaptured(tc.stmt, lost); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.stmt, got, tc.want)
		}
	}
	if got := readsNotCaptured("SELECT sum(n) FROM big", nil); got != "" {
		t.Errorf("nothing listed: %q", got)
	}
}
