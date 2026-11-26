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

// A statement that ran out of memory on the copy in the copy's join order
// and again in the order written (sqlsandbox oomretry.go) goes to the source
// under its own reason; one the copy did not run again goes there as
// copy_refused, as before.
func TestCopyFallback_outOfMemoryAfterTheOrderWritten(t *testing.T) {
	s := routeSession(true)
	oom := "Out of Memory Error: could not allocate block of size 256.0 KiB"
	for _, tc := range []struct {
		err  error
		want string
	}{
		{&sqlsandbox.QueryError{Message: oom, WrittenOrderRetry: sqlsandbox.WrittenOrderRetryFailed}, ReasonCopyRefusedAfterWrittenOrder},
		{fmt.Errorf("wrapped: %w", &sqlsandbox.QueryError{Message: oom, WrittenOrderRetry: sqlsandbox.WrittenOrderRetryFailed}), ReasonCopyRefusedAfterWrittenOrder},
		{&sqlsandbox.QueryError{Message: oom}, ReasonCopyRefused},
		{&sqlsandbox.QueryError{Message: "Binder Error: x"}, ReasonCopyRefused},
		// Not run again for want of time: copy_refused, as before.
		{&sqlsandbox.QueryError{Message: oom, WrittenOrderRetry: sqlsandbox.WrittenOrderRetrySkippedNoTime}, ReasonCopyRefused},
		// The second run's own error is that error: a result too large is
		// not sent to the source, as for one the first run gave.
		{&sqlsandbox.WrittenOrderRetryError{Outcome: sqlsandbox.WrittenOrderRetryOtherError, Err: sqlsandbox.ErrResultTooLarge}, ""},
		{&sqlsandbox.WrittenOrderRetryError{Outcome: sqlsandbox.WrittenOrderRetryOtherError, Err: &sqlsandbox.QueryError{Message: "Binder Error: x"}}, ReasonCopyRefused},
	} {
		perr := freeError(tc.err, s.logger)
		if got := s.copyFallback(tc.err, perr); got != tc.want {
			t.Errorf("%v: %q, want %q", tc.err, got, tc.want)
		}
	}
	tooLarge := &sqlsandbox.WrittenOrderRetryError{Outcome: sqlsandbox.WrittenOrderRetryOtherError, Err: sqlsandbox.ErrResultTooLarge}
	if perr := freeError(tooLarge, s.logger); perr.code != freeError(sqlsandbox.ErrResultTooLarge, s.logger).code {
		t.Errorf("a result too large after the retry: %+v", perr)
	}
	// A cancel or statement_timeout during the retry is not sent anywhere.
	stopped := &stoppedErr{&pgErr{"57014", "cancelled"}}
	if got := s.copyFallback(stopped, stopped.perr); got != "" {
		t.Errorf("a stopped statement: %q", got)
	}
	// Nothing to fall back to without a forwarding account.
	marked := &sqlsandbox.QueryError{Message: oom, WrittenOrderRetry: sqlsandbox.WrittenOrderRetryFailed}
	if got := routeSession(false).copyFallback(marked, freeError(marked, s.logger)); got != "" {
		t.Errorf("without a forwarding account: %q", got)
	}
	// The statement's own, not the moment's: a prepared one stays with the
	// source, as for copy_refused.
	if transientFallback(ReasonCopyRefusedAfterWrittenOrder) != transientFallback(ReasonCopyRefused) {
		t.Error("the new reason is not kept as copy_refused is")
	}
	if rows := (LastAnswer{Side: RouteSource, Reason: ReasonCopyRefusedAfterWrittenOrder}).Rows(); len(rows) != 4 || rows[2][1] == "" {
		t.Errorf("no sentence for the reason: %q", rows)
	}
}

// oomLogin is a port whose copy runs out of memory, retried in the order
// written or not by the statement's text, and whose source cannot be
// reached (a statement that gets 08001 was sent there).
type oomLogin struct {
	forward bool
	mu      sync.Mutex
	notes   []RouteNote
	runs    int
}

func (l *oomLogin) run(_ context.Context, st string, _ RunOptions) (sqlsandbox.Result, error) {
	l.mu.Lock()
	l.runs++
	l.mu.Unlock()
	switch {
	case strings.Contains(st, "bothorders"):
		return sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: "Out of Memory Error: x. It ran out of memory in the order written too.", WrittenOrderRetry: sqlsandbox.WrittenOrderRetryFailed}
	case strings.Contains(st, "notretried"):
		return sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: "Out of Memory Error: x"}
	}
	res := sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, Rows: [][]any{{int32(7)}}}
	if strings.Contains(st, "retried") {
		// Answered in the order written.
		res.WrittenOrderRetry = sqlsandbox.WrittenOrderRetryOK
	}
	return res, nil
}

func (l *oomLogin) login(_ context.Context, _, _ string, password *Password, _ func()) (*Binding, *Error) {
	if !password.Matches([]string{portPassword}) {
		return nil, &Error{Code: "28P01", Message: "no"}
	}
	b := &Binding{Run: l.run, Actor: "server:orders", Schema: "public", Routed: func(n RouteNote) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.notes = append(l.notes, n)
	}}
	if l.forward {
		b.Forward = &Forward{DSN: "postgres://fwd:pw@127.0.0.1:1/db?sslmode=disable&connect_timeout=2", Cost: &CostPolicy{}}
	}
	return b, nil
}

func (l *oomLogin) last() (RouteNote, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.notes) == 0 {
		return RouteNote{}, l.runs
	}
	return l.notes[len(l.notes)-1], l.runs
}

// On both protocols, which share the run (runFree): an answer after the
// retry is the copy's; out of memory in both orders goes to the source with
// its own reason; one not run again goes there as copy_refused. Each is run
// on the copy once by the port: the retry is the sandbox's, inside that run.
func TestRoute_outOfMemoryAfterTheOrderWrittenGoesToTheSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, forward := range []bool{true, false} {
		l := &oomLogin{forward: forward}
		addr := serveAddrCfg(t, Config{Login: l.login})
		c, err := pgconn.Connect(ctx, fmt.Sprintf("host=%s port=%s user=orders password=%s sslmode=disable", hostOf(addr), portOf(addr), portPassword))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(context.Background())
		type outcome struct {
			code, side, reason, retry string
			runs                      int
		}
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
		var before int
		result := func(err error) outcome {
			n, runs := l.last()
			o := outcome{code(err), n.Side, n.Reason, n.OOMRetry, runs - before}
			before = runs
			return o
		}
		simple := func(stmt string) outcome {
			_, err := c.Exec(ctx, stmt).ReadAll()
			return result(err)
		}
		extended := func(stmt string) outcome {
			err := c.ExecParams(ctx, stmt, nil, nil, nil, nil).Read().Err
			// A Describe may run it on the copy too: not counted here.
			o := result(err)
			o.runs = 0
			return o
		}
		check := func(what string, got, want outcome) {
			t.Helper()
			if got != want {
				t.Errorf("forward %v, %s: %+v, want %+v", forward, what, got, want)
			}
		}
		if forward {
			check("answered after the retry", simple("SELECT n FROM retried"), outcome{"", RouteCopy, ReasonAnswersTheSame, sqlsandbox.WrittenOrderRetryOK, 1})
			check("out of memory in both orders", simple("SELECT n FROM bothorders"), outcome{"08001", RouteSource, ReasonCopyRefusedAfterWrittenOrder, sqlsandbox.WrittenOrderRetryFailed, 1})
			check("out of memory, not run again", simple("SELECT n FROM notretried"), outcome{"08001", RouteSource, ReasonCopyRefused, "", 1})
			check("extended: answered after the retry", extended("SELECT n FROM retried"), outcome{"", RouteCopy, ReasonAnswersTheSame, sqlsandbox.WrittenOrderRetryOK, 0})
			check("extended: out of memory in both orders", extended("SELECT n FROM bothorders"), outcome{"08001", RouteSource, ReasonCopyRefusedAfterWrittenOrder, sqlsandbox.WrittenOrderRetryFailed, 0})
			check("extended: out of memory, not run again", extended("SELECT n FROM notretried"), outcome{"08001", RouteSource, ReasonCopyRefused, "", 0})
			// A Describe without an Execute runs on the copy and is noted by
			// no statement: the next statement does not take its retry.
			if _, err := c.Prepare(ctx, "described", "SELECT n FROM retried WHERE n > $1", nil); err != nil {
				t.Fatalf("Prepare: %v", err)
			}
			check("a plain read after a Describe of a retried one", simple("SELECT n FROM t"), outcome{"", RouteCopy, ReasonAnswersTheSame, "", 1})
		} else {
			// Nowhere else to send it: the copy's error, which says it was
			// run again.
			_, err := c.Exec(ctx, "SELECT n FROM bothorders").ReadAll()
			result(err)
			var pe *pgconn.PgError
			if !errors.As(err, &pe) || pe.Code != "XX000" || !strings.Contains(pe.Message, "order written") {
				t.Errorf("without a forwarding account: %v", err)
			}
			check("answered after the retry", simple("SELECT n FROM retried"), outcome{"", RouteCopy, ReasonNoForwardAccount, sqlsandbox.WrittenOrderRetryOK, 1})
		}
	}
}

// statement_timeout is a cancel of the statement, not its context's
// deadline: the statement's context says when it ends, for the copy's retry
// in the order written.
func TestStatementContext_carriesTheClientsLimit(t *testing.T) {
	s := routeSession(true)
	s.set.stmtTimeout = time.Minute
	ctx, done := s.statementContext()
	defer done()
	if _, ok := ctx.Deadline(); ok {
		t.Error("statement_timeout became a deadline: what stops it would change")
	}
	at, _ := sqlsandbox.ClientDeadlineOf(ctx)
	if left := time.Until(at); left < 59*time.Second || left > time.Minute {
		t.Errorf("client deadline in %v, want about a minute", left)
	}
	s.set.stmtTimeout = 0
	ctx2, done2 := s.statementContext()
	defer done2()
	if _, ok := sqlsandbox.ClientDeadlineOf(ctx2); ok {
		t.Error("a client deadline without statement_timeout")
	}
}
