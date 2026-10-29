package readrouter

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
)

// newDeadlineForwarder is a forwarder whose statements have limit as their
// deadline, the way the port gives one (the context and the forwarder's own
// query timeout are the same value).
func newDeadlineForwarder(t *testing.T, src *slowSource, limit time.Duration) *Forwarder {
	t.Helper()
	fw, err := NewForwarder("u:p@tcp("+src.addr+")/", config.SSL{Mode: "disabled"}, DefaultPolicy(), limit)
	if err != nil {
		t.Fatal(err)
	}
	fw.StopOnSource = true
	fw.killGrace = 2 * time.Second
	t.Cleanup(fw.Close)
	return fw
}

func deadlineCtx(t *testing.T, limit time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	t.Cleanup(cancel)
	return ctx
}

func errCode(err error) uint16 {
	var me *mysql.MyError
	if errors.As(err, &me) {
		return me.Code
	}
	return 0
}

// A statement that runs past its deadline is stopped ON THE SOURCE (KILL
// QUERY on a connection of its own) and the client is told so with 1317;
// the connection, its session and its transaction stay. The limit is short
// on purpose and equal to the forwarder's query timeout: were the socket's
// read deadline the same value, the read would expire with the context and
// the connection would be lost before the source answered.
func TestForwarder_deadlineStopsTheStatementOnTheSource(t *testing.T) {
	src := newSlowSource(t)
	const limit = 300 * time.Millisecond
	fw := newDeadlineForwarder(t, src, limit)
	if _, err := fw.Forward(context.Background(), "SELECT 1", &BufferSink{}); err != nil {
		t.Fatal(err)
	}
	id := fw.ThreadID()

	_, err := fw.Forward(deadlineCtx(t, limit), "SELECT SLEEP(25)", &BufferSink{})
	if errCode(err) != mysql.ER_QUERY_INTERRUPTED || IsLost(err) {
		t.Fatalf("a statement past its deadline answered %v, want 1317 with the connection kept", err)
	}
	for _, want := range []string{"0.3 s", "still open"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the client is told %q, want it to name %q", err.Error(), want)
		}
	}
	if got := src.kills(); !slices.Equal(got, []string{"KILL QUERY " + threadText(id)}) {
		t.Errorf("the source was sent %q, want only a KILL QUERY of this connection (%d)", got, id)
	}
	if _, err := fw.Forward(context.Background(), "SELECT 1", &BufferSink{}); err != nil {
		t.Errorf("the next statement on the same connection: %v", err)
	}
	if fw.ThreadID() != id {
		t.Errorf("the connection was replaced (thread %d, then %d)", id, fw.ThreadID())
	}
}

// The same for a prepared statement: the source shows it with its arguments
// in place, and that is still recognized as this connection's statement.
func TestForwarder_deadlineStopsAPreparedStatement(t *testing.T) {
	src := newSlowSource(t)
	const limit = 300 * time.Millisecond
	fw := newDeadlineForwarder(t, src, limit)
	st, err := fw.Prepare(context.Background(), "SELECT ? FROM DUAL WHERE SLEEP(?) = 0")
	if err != nil {
		t.Fatal(err)
	}
	id := fw.ThreadID()
	_, err = st.Execute(deadlineCtx(t, limit), []any{"abc", 25}, &BufferSink{})
	// The fake answers a prepared statement's error as 1105 wrapping the
	// 1317 (go-mysql's server does; MySQL does not, see the integration
	// test): what is checked here is that the statement was recognized,
	// stopped on the source, and its connection kept.
	if err == nil || IsLost(err) || !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("a prepared statement past its deadline answered %v, want it stopped with the connection kept", err)
	}
	if got := src.kills(); !slices.Equal(got, []string{"KILL QUERY " + threadText(id)}) {
		t.Errorf("the source was sent %q, want only a KILL QUERY of this connection (%d)", got, id)
	}
	if _, err := fw.Forward(context.Background(), "SELECT 1", &BufferSink{}); err != nil {
		t.Errorf("the next statement on the same connection: %v", err)
	}
}

// A statement that ends on its own while the source is being asked to stop
// it (the process list still showed it running) gets the answer it had: and
// the KILL QUERY that was already on its way lands while the connection is
// idle, never on the connection's next statement. That is why the end of a
// statement waits for a stop under way.
func TestForwarder_lateKillDoesNotReachTheNextStatement(t *testing.T) {
	// What the process list shows once the statement has ended: still the
	// statement (it was read just before), or the idle connection.
	shows := map[string]func(running) [][]any{
		"read before it ended": func(r running) [][]any { return [][]any{{"u", r.command, r.info}} },
		"read after it ended":  func(running) [][]any { return [][]any{{"u", "Sleep", nil}} },
	}
	for name, show := range shows {
		t.Run(name, func(t *testing.T) {
			src := newSlowSource(t)
			src.processlist = func(id uint32, r running) [][]any {
				src.finishOK(id)
				time.Sleep(300 * time.Millisecond)
				return show(r)
			}
			const limit = 300 * time.Millisecond
			fw := newDeadlineForwarder(t, src, limit)
			if _, err := fw.Forward(deadlineCtx(t, limit), "SELECT SLEEP(25)", &BufferSink{}); err != nil {
				t.Fatalf("a statement that ended on its own answered %v, want its result", err)
			}
			src.mu.Lock()
			src.processlist = nil
			src.mu.Unlock()
			<-src.entered // the statement above
			go func() {
				id := <-src.entered
				time.Sleep(time.Second)
				src.finishOK(id)
			}()
			if _, err := fw.Forward(context.Background(), "SELECT SLEEP(1)", &BufferSink{}); err != nil {
				t.Errorf("the next statement answered %v: the stop that was under way reached it", err)
			}
		})
	}
}

// A client that leaves (its context is cancelled, not expired) loses the
// connection as before: nothing is stopped on the source from here.
func TestForwarder_cancelDoesNotKillOnTheSource(t *testing.T) {
	src := newSlowSource(t)
	fw := newDeadlineForwarder(t, src, 30*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := fw.Forward(ctx, "SELECT SLEEP(25)", &BufferSink{})
		done <- err
	}()
	<-src.entered
	cancel()
	if err := <-done; !IsLost(err) {
		t.Errorf("a cancelled statement answered %v, want the lost connection", err)
	}
	if got := src.kills(); len(got) != 0 {
		t.Errorf("the source was sent %q on a cancel, want nothing", got)
	}
}

// When the process list does not show THIS connection running THIS
// statement (another server answered the side connection, an account of
// another name, a different statement, nothing), no KILL is sent: killing a
// thread id on the wrong server would stop someone else's work. The
// connection is lost as before.
func TestForwarder_deadlineKillsOnlyWhatItRecognizes(t *testing.T) {
	cases := map[string]func(uint32, running) [][]any{
		"no such thread":          func(uint32, running) [][]any { return nil },
		"another account":         func(_ uint32, r running) [][]any { return [][]any{{"other", r.command, r.info}} },
		"account in another case": func(_ uint32, r running) [][]any { return [][]any{{"U", r.command, r.info}} },
		"another statement":       func(_ uint32, r running) [][]any { return [][]any{{"u", "Query", "SELECT SLEEP(26)"}} },
		"idle":                    func(uint32, running) [][]any { return [][]any{{"u", "Sleep", nil}} },
		"two rows":                func(_ uint32, r running) [][]any { return [][]any{{"u", r.command, r.info}, {"u", r.command, r.info}} },
	}
	for name, show := range cases {
		t.Run(name, func(t *testing.T) {
			src := newSlowSource(t)
			src.processlist = show
			const limit = 300 * time.Millisecond
			fw := newDeadlineForwarder(t, src, limit)
			var told []error
			fw.OnStopFailed = func(why error) { told = append(told, why) }
			_, err := fw.Forward(deadlineCtx(t, limit), "SELECT SLEEP(25)", &BufferSink{})
			if !IsLost(err) || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) ||
				!strings.Contains(err.Error(), "could not be stopped on the source") {
				t.Errorf("answered %v, want the lost connection naming the deadline and why it was not stopped", err)
			}
			if len(told) != 1 {
				t.Errorf("OnStopFailed was told %v, want once", told)
			}
			if got := src.kills(); len(got) != 0 {
				t.Errorf("the source was sent %q, want no KILL", got)
			}
		})
	}
}

// A statement that does not stop when told to (KILL QUERY accepted, the
// statement still running after the grace) gets its connection ended on the
// source too, and the client sees the lost connection as before.
func TestForwarder_deadlineEndsAStatementThatDoesNotStop(t *testing.T) {
	src := newSlowSource(t)
	src.deaf = true
	const limit = 300 * time.Millisecond
	fw := newDeadlineForwarder(t, src, limit)
	fw.killGrace = 300 * time.Millisecond
	if _, err := fw.Forward(context.Background(), "SELECT 1", &BufferSink{}); err != nil {
		t.Fatal(err)
	}
	id := fw.ThreadID()
	start := time.Now()
	_, err := fw.Forward(deadlineCtx(t, limit), "SELECT SLEEP(25)", &BufferSink{})
	if !IsLost(err) {
		t.Errorf("answered %v, want the lost connection", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("took %v to give up: the grace is not bounding the wait", took)
	}
	if got := src.kills(); !slices.Equal(got, []string{"KILL QUERY " + threadText(id), "KILL " + threadText(id)}) {
		t.Errorf("the source was sent %q, want KILL QUERY then KILL of thread %d", got, id)
	}
}

// What the process list shows is matched against the statement the
// connection is running. A false "no" costs what the port did before (the
// connection is lost); a false "yes" would kill someone else's statement,
// so every doubt is a no.
func TestRunningMatches(t *testing.T) {
	long := "SELECT '" + strings.Repeat("x", 70000) + "'"
	cases := []struct {
		name, command, info, stmt string
		prepared                  bool
		want                      bool
	}{
		{"same text", "Query", "SELECT SLEEP(1)", "SELECT SLEEP(1)", false, true},
		{"other text", "Query", "SELECT SLEEP(2)", "SELECT SLEEP(1)", false, false},
		{"longer text", "Query", "SELECT SLEEP(1) AND 1", "SELECT SLEEP(1)", false, false},
		{"shorter text", "Query", "SELECT SLEEP", "SELECT SLEEP(1)", false, false},
		{"text cut by the source", "Query", long[:65535], long, false, true},
		{"short prefix is not a cut", "Query", long[:100], long, false, false},
		{"empty info", "Query", "", "SELECT 1", false, false},
		{"idle", "Sleep", "SELECT SLEEP(1)", "SELECT SLEEP(1)", false, false},
		{"text shown as executing", "Execute", "SELECT SLEEP(1)", "SELECT SLEEP(1)", false, false},
		{"prepared, arguments in place", "Execute", "SELECT 'abc' FROM DUAL WHERE SLEEP(2) = 0", "SELECT ? FROM DUAL WHERE SLEEP(?) = 0", true, true},
		{"prepared, no arguments", "Execute", "SELECT 1", "SELECT 1", true, true},
		{"prepared, other statement", "Execute", "SELECT 'abc' FROM t WHERE SLEEP(2) = 0", "SELECT ? FROM DUAL WHERE SLEEP(?) = 0", true, false},
		{"prepared, argument at the end", "Execute", "SELECT * FROM t WHERE id = 42", "SELECT * FROM t WHERE id = ?", true, true},
		{"prepared, text after the end", "Execute", "SELECT * FROM t WHERE id = 42 LIMIT 1", "SELECT * FROM t WHERE id = ? LIMIT 2", true, false},
		{"prepared, shown as a query", "Query", "SELECT * FROM t WHERE id = 42", "SELECT * FROM t WHERE id = ?", true, false},
	}
	for _, c := range cases {
		if got := runningMatches(c.command, c.info, c.stmt, c.prepared); got != c.want {
			t.Errorf("%s: runningMatches = %v, want %v", c.name, got, c.want)
		}
	}
}

func threadText(id uint32) string { return strconv.FormatUint(uint64(id), 10) }

// A forwarder of the port's own (a probe, the KILL of dropped connections)
// does not stop statements on the source: past its deadline the connection
// is closed under the statement, as it always was.
func TestForwarder_deadlineWithoutStopOnSourceClosesTheSocket(t *testing.T) {
	src := newSlowSource(t)
	const limit = 300 * time.Millisecond
	fw := newDeadlineForwarder(t, src, limit)
	fw.StopOnSource = false
	_, err := fw.Forward(deadlineCtx(t, limit), "SELECT SLEEP(25)", &BufferSink{})
	if !IsLost(err) || strings.Contains(err.Error(), "could not be stopped") {
		t.Errorf("answered %v, want the plain lost connection", err)
	}
	if got := src.kills(); len(got) != 0 {
		t.Errorf("the source was sent %q, want nothing", got)
	}
}

// A statement whose deadline passed before it was sent (the plan took the
// time) is not sent: the client is told it ran out of time, and the
// connection, untouched, goes on.
func TestForwarder_expiredBeforeSendingIsNotSent(t *testing.T) {
	src := newSlowSource(t)
	fw := newDeadlineForwarder(t, src, 30*time.Second)
	st, err := fw.Prepare(context.Background(), "SELECT ? FROM DUAL WHERE SLEEP(?) = 0")
	if err != nil {
		t.Fatal(err)
	}
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err := fw.Forward(expired, "SELECT SLEEP(25)", &BufferSink{}); errCode(err) != mysql.ER_QUERY_INTERRUPTED || !strings.Contains(err.Error(), "still open") {
		t.Errorf("text statement: %v, want 1317 saying the connection is still open", err)
	}
	if _, err := st.Execute(expired, []any{"abc", 25}, &BufferSink{}); errCode(err) != mysql.ER_QUERY_INTERRUPTED {
		t.Errorf("prepared statement: %v, want 1317", err)
	}
	select {
	case <-src.entered:
		t.Error("a statement past its deadline reached the source")
	default:
	}
	if _, err := fw.Forward(context.Background(), "SELECT 1", &BufferSink{}); err != nil {
		t.Errorf("the next statement: %v", err)
	}
}

// Before the connection is ended (KILL), the process list is read again:
// a thread that by then shows another statement is not ended, and the
// operator is told the statement was not stopped.
func TestForwarder_hardKillChecksAgain(t *testing.T) {
	src := newSlowSource(t)
	src.deaf = true
	var reads int
	src.processlist = func(_ uint32, r running) [][]any {
		reads++
		if reads > 1 {
			return [][]any{{"u", "Query", "SELECT 'someone else'"}}
		}
		return [][]any{{"u", r.command, r.info}}
	}
	const limit = 300 * time.Millisecond
	fw := newDeadlineForwarder(t, src, limit)
	fw.killGrace = 300 * time.Millisecond
	var told []error
	fw.OnStopFailed = func(why error) { told = append(told, why) }
	if _, err := fw.Forward(context.Background(), "SELECT 1", &BufferSink{}); err != nil {
		t.Fatal(err)
	}
	id := fw.ThreadID()
	if _, err := fw.Forward(deadlineCtx(t, limit), "SELECT SLEEP(25)", &BufferSink{}); !IsLost(err) {
		t.Errorf("answered %v, want the lost connection", err)
	}
	if got := src.kills(); !slices.Equal(got, []string{"KILL QUERY " + threadText(id)}) {
		t.Errorf("the source was sent %q, want only the KILL QUERY", got)
	}
	if len(told) != 1 || !strings.Contains(told[0].Error(), "not ended on the source") {
		t.Errorf("OnStopFailed was told %v, want that the connection was not ended", told)
	}
}
