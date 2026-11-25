package pgshim

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #3109: a statement headed for the copy that finds the copy busy (every
// worker taken, and the line for one full or its wait over) is refused with
// an error the client sends again, and is not handed to the source. What
// the copy cannot answer the way PostgreSQL does still goes to the source.

// busySwitch is what a copy (or its catalog) answers for the moment: nil for
// its answer, an error for that error.
type busySwitch struct {
	mu  sync.Mutex
	err error
}

func (b *busySwitch) set(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.err = err
}

func (b *busySwitch) get() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

func (b *busySwitch) copyErr(string) error { return b.get() }

func TestCopyFailed_aBusyCopyIsRefusedTheRestGoToTheSource_3109(t *testing.T) {
	for _, tc := range []struct {
		name              string
		err               error
		perr              *pgErr
		refused, fallback string
	}{
		{"the line was full", sqlsandbox.ErrBusy, &pgErr{"53000", "x"}, ReasonCopyQueueFull, ""},
		{"the wait ran out", &sqlsandbox.BusyError{Waited: time.Second}, &pgErr{"53000", "x"}, ReasonCopyWaitTimeout, ""},
		{"behind the server's limit", &sqlsandbox.BehindSourceError{Reason: "x"}, &pgErr{"55000", "x"}, "", ReasonCopyBehindSource},
		{"its lag unknown", &sqlsandbox.BehindSourceError{Reason: "x", Unknown: true}, &pgErr{"55000", "x"}, "", ReasonCopyAgeUnknown},
		{"a table the copy does not hold", &sqlsandbox.QueryError{Message: "Catalog Error: Table with name x does not exist!"}, &pgErr{"42P01", "x"}, "", ReasonTableNotOnCopy},
		{"a statement the copy refuses", &sqlsandbox.QueryError{Message: "Parser Error: syntax error"}, &pgErr{"42601", "x"}, "", ReasonCopyRefused},
		{"a result over the cap", sqlsandbox.ErrResultTooLarge, &pgErr{"54000", "x"}, "", ""},
		{"cancelled", context.Canceled, &pgErr{"57014", "x"}, "", ""},
	} {
		s := routeSession(true)
		got := s.copyFailed(tc.err, tc.perr)
		if s.busyRefused != tc.refused || s.fallback != tc.fallback {
			t.Errorf("%s: refused %q, to the source %q; want %q and %q", tc.name, s.busyRefused, s.fallback, tc.refused, tc.fallback)
		}
		if tc.refused == "" {
			if got != tc.perr {
				t.Errorf("%s: the client's error became %+v", tc.name, got)
			}
			continue
		}
		if got.code != "53000" || !strings.Contains(got.msg, tc.refused) {
			t.Errorf("%s: %+v, want 53000 naming %s", tc.name, got, tc.refused)
		}
	}
	// A connection that cannot reach its source, and a statement of a block
	// on the source: the copy's own error, as before.
	for name, s := range map[string]*session{"no forwarding account": routeSession(false), "in a block": func() *session {
		s := routeSession(true)
		s.up = &upstream{inTx: true}
		return s
	}()} {
		perr := &pgErr{"53000", sqlsandbox.ErrBusy.Error()}
		if got := s.copyFailed(sqlsandbox.ErrBusy, perr); got != perr || s.busyRefused != "" || s.fallback != "" {
			t.Errorf("%s: %+v, refused %q, to the source %q; want the copy's error as it was", name, got, s.busyRefused, s.fallback)
		}
	}
}

func TestBusyRefusal_saysWhatHappenedAndWhatToDo_3109(t *testing.T) {
	for _, reason := range []string{ReasonCopyQueueFull, ReasonCopyWaitTimeout} {
		e := busyRefusal(reason)
		t.Logf("%s: %s %s", reason, e.code, e.msg)
		for _, w := range []string{"the copy is busy (" + reason + ")", "not run", "not sent to your PostgreSQL", "Run it again", "--workers", "BINTRAIL_PG_ROUTER_WORKERS"} {
			if !strings.Contains(e.msg, w) {
				t.Errorf("%s: %q does not say %q", reason, e.msg, w)
			}
		}
		if waits := strings.Contains(e.msg, "--max-wait"); waits != (reason == ReasonCopyWaitTimeout) {
			t.Errorf("%s: names --max-wait %v", reason, waits)
		}
		if e.code != "53000" {
			t.Errorf("%s: code %s", reason, e.code)
		}
	}
	if busyReason(busyError(ReasonCopyWaitTimeout)) != ReasonCopyWaitTimeout || busyReason(busyError(ReasonCopyQueueFull)) != ReasonCopyQueueFull {
		t.Error("busyError and busyReason disagree")
	}
	if busyReason(errors.New("x")) != "" || busyReason(nil) != "" {
		t.Error("busyReason reads another error as busy")
	}
}

func TestWithScreenBusy_3109(t *testing.T) {
	for _, tc := range []struct {
		d    routeDecision
		busy string
		want routeDecision
	}{
		{routeDecision{RouteCopy, ReasonAnswersTheSame}, ReasonCopyQueueFull, routeDecision{RouteCopy, ReasonCopyQueueFull}},
		{routeDecision{RouteCopy, ReasonSourceUnreachable}, ReasonCopyWaitTimeout, routeDecision{RouteCopy, ReasonCopyWaitTimeout}},
		{routeDecision{RouteSource, ReasonCheapPlan}, ReasonCopyQueueFull, routeDecision{RouteSource, ReasonCheapPlan}},
		{routeDecision{RouteSource, ReasonExplainFailed}, ReasonCopyQueueFull, routeDecision{RouteSource, ReasonExplainFailed}},
		{routeDecision{RouteCopy, ReasonAnswersTheSame}, "", routeDecision{RouteCopy, ReasonAnswersTheSame}},
	} {
		got := withScreenBusy(tc.d, tc.busy)
		if got != tc.want {
			t.Errorf("%+v with %q: %+v, want %+v", tc.d, tc.busy, got, tc.want)
		}
		if busyOnCopy(got) != (tc.busy != "" && tc.d.side == RouteCopy) {
			t.Errorf("%+v with %q: busyOnCopy %v", tc.d, tc.busy, busyOnCopy(got))
		}
	}
}

// busyOutcome is what the client got and how the port noted it.
type busyOutcome struct{ answer, code, side, reason string }

func pgOutcome(err error, rows [][][]byte, l *costLogin) (busyOutcome, string) {
	o := busyOutcome{}
	msg := ""
	var pe *pgconn.PgError
	switch {
	case errors.As(err, &pe):
		o.code, msg = pe.Code, pe.Message
	case err != nil:
		o.code = err.Error()
	}
	if len(rows) > 0 && len(rows[0]) > 0 {
		o.answer = string(rows[0][0])
	}
	n := l.last()
	o.side, o.reason = n.Side, n.Reason
	return o, msg
}

func busySimple(ctx context.Context, c *pgconn.PgConn, l *costLogin, stmt string) (busyOutcome, string) {
	res, err := c.Exec(ctx, stmt).ReadAll()
	var rows [][][]byte
	if len(res) > 0 {
		rows = res[0].Rows
	}
	return pgOutcome(err, rows, l)
}

func busyExtended(ctx context.Context, c *pgconn.PgConn, l *costLogin, stmt string, args ...[]byte) (busyOutcome, string) {
	res := c.ExecParams(ctx, stmt, args, nil, nil, nil).Read()
	return pgOutcome(res.Err, res.Rows, l)
}

func busyPrepared(ctx context.Context, c *pgconn.PgConn, l *costLogin, name string, args ...[]byte) (busyOutcome, string) {
	res := c.ExecPrepared(ctx, name, args, nil, nil).Read()
	return pgOutcome(res.Err, res.Rows, l)
}

// The port as clients reach it, over both protocols, with a source that
// counts what it runs (fakeSource: 42 is its answer, 7 the copy's).
func TestBusyCopy_isRefusedAndNothingReachesTheSource_3109(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := startFakeSource(t, costPlans)
	sw := &busySwitch{}
	l := &costLogin{src: src, copyErr: sw.copyErr}
	c := costPortConn(t, ctx, l)
	check := func(what string, got busyOutcome, msg string, want busyOutcome, wantRan bool, ranBefore int) {
		t.Helper()
		if got != want {
			t.Errorf("%s: %+v (%s), want %+v", what, got, msg, want)
		}
		if ran := src.ran()[ranBefore:]; (len(ran) > 0) != wantRan {
			t.Errorf("%s: the source ran %q", what, ran)
		}
		if want.code == "53000" && (!strings.Contains(msg, want.reason) || !strings.Contains(msg, "not sent to your PostgreSQL")) {
			t.Errorf("%s: the message does not say why: %q", what, msg)
		}
		if st := c.TxStatus(); st != 'I' {
			t.Errorf("%s: the connection is left in status %c", what, st)
		}
	}
	report := "SELECT sum(n) FROM big"
	reportArg := "SELECT sum(n) FROM big WHERE n = $1"

	// Simple protocol, a heavy report.
	for _, tc := range []struct {
		err    error
		reason string
	}{{sqlsandbox.ErrBusy, ReasonCopyQueueFull}, {&sqlsandbox.BusyError{Waited: time.Second}, ReasonCopyWaitTimeout}} {
		sw.set(tc.err)
		n := len(src.ran())
		got, msg := busySimple(ctx, c, l, report)
		t.Logf("simple, %s: %s %s", tc.reason, got.code, msg)
		check("simple, "+tc.reason, got, msg, busyOutcome{"", "53000", RouteRefused, tc.reason}, false, n)
		// Unchanged while the copy is busy: what the source answers anyway.
		n = len(src.ran())
		got, msg = busySimple(ctx, c, l, "SELECT n FROM t WHERE id = 1")
		check("a cheap read", got, msg, busyOutcome{"42", "", RouteSource, ReasonCheapPlan}, true, n)
		n = len(src.ran())
		got, msg = busySimple(ctx, c, l, "SELECT now()")
		check("a veto", got, msg, busyOutcome{"42", "", RouteSource, ReasonVeto}, true, n)
		// Extended, the unnamed statement.
		n = len(src.ran())
		got, msg = busyExtended(ctx, c, l, reportArg, []byte("1"))
		check("extended, "+tc.reason, got, msg, busyOutcome{"", "53000", RouteRefused, tc.reason}, false, n)
		n = len(src.ran())
		got, msg = busyExtended(ctx, c, l, "SELECT n FROM t WHERE id = $1", []byte("1"))
		check("extended, a cheap read", got, msg, busyOutcome{"42", "", RouteSource, ReasonCheapPlan}, true, n)
	}

	// The copy free again: the same connection is answered by the copy.
	sw.set(nil)
	n := len(src.ran())
	got, msg := busySimple(ctx, c, l, report)
	check("the copy free again", got, msg, busyOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}, false, n)

	// A prepared statement executed while the copy is busy is refused, and
	// tried on the copy again at its next execution: nothing keeps it on the
	// source.
	if _, err := c.Prepare(ctx, "rep", reportArg, nil); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	sw.set(&sqlsandbox.BusyError{Waited: time.Second})
	n = len(src.ran())
	got, msg = busyPrepared(ctx, c, l, "rep", []byte("1"))
	check("prepared, busy", got, msg, busyOutcome{"", "53000", RouteRefused, ReasonCopyWaitTimeout}, false, n)
	sw.set(nil)
	got, msg = busyPrepared(ctx, c, l, "rep", []byte("1"))
	check("prepared, the copy free again", got, msg, busyOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}, false, n)

	// Prepared while the copy is busy: its Describe is refused and counted,
	// and it is prepared again under its name once the copy is free.
	sw.set(sqlsandbox.ErrBusy)
	n = len(src.ran())
	_, err := c.Prepare(ctx, "rep2", report, nil)
	got, msg = pgOutcome(err, nil, l)
	check("described while busy", got, msg, busyOutcome{"", "53000", RouteRefused, ReasonCopyQueueFull}, false, n)
	sw.set(nil)
	if _, err := c.Prepare(ctx, "rep2", report, nil); err != nil {
		t.Fatalf("prepared again once the copy is free: %v", err)
	}
	got, msg = busyPrepared(ctx, c, l, "rep2")
	check("prepared again", got, msg, busyOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}, false, n)

	// A transaction block lives on the source: unchanged.
	if _, err := c.Exec(ctx, "BEGIN").ReadAll(); err != nil {
		t.Fatal(err)
	}
	sw.set(sqlsandbox.ErrBusy)
	n = len(src.ran())
	got, msg = busySimple(ctx, c, l, report)
	if got != (busyOutcome{"42", "", RouteSource, ReasonInTransaction}) || len(src.ran()) == n {
		t.Errorf("in a block: %+v (%s)", got, msg)
	}
	if _, err := c.Exec(ctx, "COMMIT").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if st := c.TxStatus(); st != 'I' {
		t.Errorf("after the block: status %c", st)
	}
}

// Without a forwarding account nothing changes: the copy's own error, as
// before, and the statement noted as the copy's.
func TestBusyCopy_withoutAForwardingAccountIsAsBefore_3109(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sw := &busySwitch{}
	sw.set(sqlsandbox.ErrBusy)
	l := &costLogin{copyErr: sw.copyErr}
	c := costPortConn(t, ctx, l)
	for name, run := range map[string]func() (busyOutcome, string){
		"simple": func() (busyOutcome, string) { return busySimple(ctx, c, l, "SELECT sum(n) FROM big") },
		"extended": func() (busyOutcome, string) {
			return busyExtended(ctx, c, l, "SELECT sum(n) FROM big WHERE n = $1", []byte("1"))
		},
	} {
		got, msg := run()
		if got != (busyOutcome{"", "53000", RouteCopy, ReasonNoForwardAccount}) || msg != sqlsandbox.ErrBusy.Error() {
			t.Errorf("%s: %+v %q, want the copy's own 53000 as before", name, got, msg)
		}
	}
}

// The copy too busy to list its columns, which the screen needs: the
// source's plan still decides, what it sends to the copy is refused without
// asking the copy, and what it finds cheap goes to the source.
func TestBusyCopy_aCatalogTheCopyIsTooBusyToList_3109(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := startFakeSource(t, costPlans)
	cat := &busySwitch{}
	cat.set(&sqlsandbox.BusyError{Waited: time.Second})
	col := func(n string) CatalogColumn { return CatalogColumn{Name: n, Type: "INTEGER", SourceType: oidInt4} }
	l := &costLogin{src: src, catalogErr: cat.get, catalog: func() []CatalogTable {
		return []CatalogTable{{Schema: "public", Name: "big", Columns: []CatalogColumn{col("n")}}, {Schema: "public", Name: "t", Columns: []CatalogColumn{col("n"), col("id")}}}
	}}
	c := costPortConn(t, ctx, l)
	step := func(what string, got busyOutcome, msg string, want busyOutcome, wantRan bool, ranBefore int, runsBefore int32) {
		t.Helper()
		if got != want {
			t.Errorf("%s: %+v (%s), want %+v", what, got, msg, want)
		}
		if ran := src.ran()[ranBefore:]; (len(ran) > 0) != wantRan {
			t.Errorf("%s: the source ran %q", what, ran)
		}
		if want.code == "53000" && l.runs.Load() != runsBefore {
			t.Errorf("%s: the copy was asked to run it", what)
		}
		if st := c.TxStatus(); st != 'I' {
			t.Errorf("%s: status %c", what, st)
		}
	}
	n, r := len(src.ran()), l.runs.Load()
	got, msg := busySimple(ctx, c, l, "SELECT sum(n) FROM big")
	step("simple, a report", got, msg, busyOutcome{"", "53000", RouteRefused, ReasonCopyWaitTimeout}, false, n, r)
	n, r = len(src.ran()), l.runs.Load()
	got, msg = busySimple(ctx, c, l, "SELECT n FROM t WHERE id = 1")
	step("simple, a cheap read", got, msg, busyOutcome{"42", "", RouteSource, ReasonCheapPlan}, true, n, r)
	n, r = len(src.ran()), l.runs.Load()
	got, msg = busyExtended(ctx, c, l, "SELECT sum(n) FROM big WHERE n = $1", []byte("1"))
	step("extended, a report", got, msg, busyOutcome{"", "53000", RouteRefused, ReasonCopyWaitTimeout}, false, n, r)
	n, r = len(src.ran()), l.runs.Load()
	_, err := c.Prepare(ctx, "b", "SELECT sum(n) FROM big", nil)
	got, msg = pgOutcome(err, nil, l)
	step("described", got, msg, busyOutcome{"", "53000", RouteRefused, ReasonCopyWaitTimeout}, false, n, r)
	n, r = len(src.ran()), l.runs.Load()
	got, msg = busyExtended(ctx, c, l, "SELECT n FROM t WHERE id = $1", []byte("1"))
	step("extended, a cheap read", got, msg, busyOutcome{"42", "", RouteSource, ReasonCheapPlan}, true, n, r)

	// What the screen vetoes whatever the columns' types, when expensive on
	// the source, is the copy's anyway (heavy.go): with the list unread it is
	// refused as busy, as any other expensive statement, and not sent to the
	// source.
	for _, stmt := range []string{"SELECT now() FROM big", "SELECT count(*) FROM big WHERE n = 1 AND DATE '2026-03-02+02' IS NOT NULL"} {
		n, r = len(src.ran()), l.runs.Load()
		got, msg = busySimple(ctx, c, l, stmt)
		step("a veto, "+stmt, got, msg, busyOutcome{"", "53000", RouteRefused, ReasonCopyWaitTimeout}, false, n, r)
	}
	// Cheap, the veto stands: the source answers.
	n, r = len(src.ran()), l.runs.Load()
	got, msg = busySimple(ctx, c, l, "SELECT now() FROM t WHERE id = 1")
	step("a cheap veto", got, msg, busyOutcome{"42", "", RouteSource, ReasonVeto}, true, n, r)

	// Listed again: the copy answers the report, prepared or not.
	cat.set(nil)
	n, r = len(src.ran()), l.runs.Load()
	got, msg = busySimple(ctx, c, l, "SELECT sum(n) FROM big")
	step("simple, listed", got, msg, busyOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}, false, n, r)
	if _, err := c.Prepare(ctx, "b", "SELECT sum(n) FROM big", nil); err != nil {
		t.Fatalf("prepared again: %v", err)
	}
	got, msg = busyPrepared(ctx, c, l, "b")
	step("prepared, listed", got, msg, busyOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}, false, n, r)
}

// A session time zone the copy is asked about as the statement runs: a busy
// copy there refuses the statement as busy, not as copy_refused for the
// source (runFree).
func TestZoneOnCopy_aBusyCopyIsBusy_3109(t *testing.T) {
	loc, err := time.LoadLocation("Europe/Paris")
	if err != nil {
		t.Skip("no zone database here")
	}
	z := zoneSetting{name: "Europe/Paris", duck: "Europe/Paris", loc: loc}
	for _, forward := range []bool{true, false} {
		copyZonesAgreed.Range(func(k, _ any) bool { copyZonesAgreed.Delete(k); return true })
		s := routeSession(forward)
		s.bound.Run = func(context.Context, string, RunOptions) (sqlsandbox.Result, error) {
			return sqlsandbox.Result{}, &sqlsandbox.BusyError{Waited: time.Second}
		}
		perr := s.zoneOnCopy(z, true)
		switch {
		case perr == nil || perr.code != "53000":
			t.Errorf("forward=%v: %+v, want 53000", forward, perr)
		case forward && (s.busyRefused != ReasonCopyWaitTimeout || !strings.Contains(perr.msg, "copy_wait_timeout")):
			t.Errorf("forward: %+v, refused %q; want the busy refusal", perr, s.busyRefused)
		case !forward && s.busyRefused != "":
			t.Errorf("no forwarding account: refused %q, want the copy's error as before", s.busyRefused)
		}
	}
}

// The char(n) check before the copy runs a statement reads the catalog too:
// a busy copy there refuses the statement as busy, and does not send it to
// the source as a veto.
func TestPaddedRefusal_aBusyCatalogIsBusyNotAVeto_3109(t *testing.T) {
	for _, forward := range []bool{true, false} {
		s := routeSession(forward)
		s.bound.Catalog = func(context.Context) ([]CatalogTable, error) { return nil, sqlsandbox.ErrBusy }
		perr := s.paddedRefusal("SELECT n FROM t")
		switch {
		case perr == nil:
			t.Errorf("forward=%v: answered by the copy with its columns unknown", forward)
		case forward && (perr.code != "53000" || s.busyRefused != ReasonCopyQueueFull || s.fallback != ""):
			t.Errorf("forward: %+v, refused %q, to the source %q; want 53000 under copy_queue_full", perr, s.busyRefused, s.fallback)
		case !forward && (perr.code != "0A000" || s.busyRefused != ""):
			t.Errorf("no forwarding account: %+v, want the refusal as before", perr)
		}
	}
}
