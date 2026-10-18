package querykill

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// ---- a fake driver whose statements run until told to, or until killed ----

// fakeServer plays the index server: it hands out thread ids and remembers
// which statement each thread is running, so a KILL can be checked against
// what was really running on its target.
type fakeServer struct {
	mu      sync.Mutex
	nextID  uint64
	running map[uint64]string // thread id -> statement text
	killed  map[uint64]chan struct{}

	idErr error // CONNECTION_ID() fails when set
	// hostFor names the server a new connection reaches (default "h1").
	hostFor func(id uint64) string
	// connectHook runs inside Connect's identity read (tests that cancel there).
	idHook func(ctx context.Context) error
	// killSent records every KILL QUERY the default KillFunc sent, with the
	// host it reached.
	killSent []string
}

func newFakeServer() *fakeServer {
	return &fakeServer{nextID: 100, running: map[uint64]string{}, killed: map[uint64]chan struct{}{}}
}

func (s *fakeServer) start(id uint64, q string) chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running[id] = q
	ch := make(chan struct{})
	s.killed[id] = ch
	return ch
}

func (s *fakeServer) stop(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.running, id)
}

// kill is what KILL QUERY does on the fake: it ends the thread's statement,
// and reports what it hit.
func (s *fakeServer) kill(id uint64) (hit string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hit = s.running[id]
	if ch, ok := s.killed[id]; ok && hit != "" {
		close(ch)
		delete(s.killed, id)
	}
	delete(s.running, id)
	return hit
}

func (s *fakeServer) isRunning(id uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running[id] != ""
}

type fakeConnector struct{ srv *fakeServer }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) {
	c.srv.mu.Lock()
	c.srv.nextID++
	id := c.srv.nextID
	c.srv.mu.Unlock()
	host := "h1"
	if c.srv.hostFor != nil {
		host = c.srv.hostFor(id)
	}
	return &fakeConn{srv: c.srv, id: id, host: host}, nil
}
func (c fakeConnector) Driver() driver.Driver { return nil }

type fakeConn struct {
	srv    *fakeServer
	id     uint64
	host   string
	closed bool
}

func (c *fakeConn) Prepare(q string) (driver.Stmt, error) { return &fakeStmt{c: c, q: q}, nil }
func (c *fakeConn) Close() error                          { c.closed = true; return nil }
func (c *fakeConn) Begin() (driver.Tx, error)             { return nil, errors.New("no tx") }

// QueryContext: "SELECT CONNECTION_ID()" answers at once. Any query with
// args declines (ErrSkip), like go-sql-driver without interpolateParams.
// "SLOW" runs until killed or until its context ends (then, like the real
// driver, it returns an error with the server still running it). "FAST"
// returns two rows at once.
func (c *fakeConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if q == threadQuery {
		if c.srv.idHook != nil {
			if err := c.srv.idHook(ctx); err != nil {
				return nil, err
			}
		}
		if c.srv.idErr != nil {
			return nil, c.srv.idErr
		}
		return &fakeRows{vals: [][]driver.Value{{int64(c.id), []byte(c.host), int64(3306), int64(1)}}, cols: []string{"id", "h", "p", "s"}}, nil
	}
	if q == "BADCONN" {
		return nil, driver.ErrBadConn
	}
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}
	return c.run(ctx, q)
}

func (c *fakeConn) run(ctx context.Context, q string) (driver.Rows, error) {
	killed := c.srv.start(c.id, q)
	if strings.HasPrefix(q, "STREAM") {
		return &streamRows{ctx: ctx, killed: killed, srv: c.srv, id: c.id}, nil
	}
	if strings.HasPrefix(q, "FAST") {
		c.srv.stop(c.id)
		return &fakeRows{vals: [][]driver.Value{{int64(1)}, {int64(2)}}, cols: []string{"n"}}, nil
	}
	select {
	case <-killed:
		return nil, &mysql.MySQLError{Number: 1317, Message: "Query execution was interrupted"}
	case <-ctx.Done():
		// The real driver closes the socket and returns the context's
		// error; the server keeps running the statement. So does the fake:
		// running stays set.
		return nil, ctx.Err()
	}
}

func (c *fakeConn) ExecContext(ctx context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(q, "KILL QUERY ") {
		c.srv.mu.Lock()
		c.srv.killSent = append(c.srv.killSent, c.host+" "+q)
		c.srv.mu.Unlock()
		return driver.RowsAffected(0), nil
	}
	killed := c.srv.start(c.id, q)
	select {
	case <-killed:
		return nil, errors.New("killed")
	case <-ctx.Done():
		return nil, driver.ErrBadConn
	}
}

type fakeStmt struct {
	c *fakeConn
	q string
}

func (s *fakeStmt) Close() error                               { return nil }
func (s *fakeStmt) NumInput() int                              { return -1 }
func (s *fakeStmt) Exec([]driver.Value) (driver.Result, error) { return nil, errors.New("unused") }
func (s *fakeStmt) Query([]driver.Value) (driver.Rows, error)  { return nil, errors.New("unused") }
func (s *fakeStmt) QueryContext(ctx context.Context, _ []driver.NamedValue) (driver.Rows, error) {
	return s.c.run(ctx, s.q)
}

type fakeRows struct {
	cols []string
	vals [][]driver.Value
	i    int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.i >= len(r.vals) {
		return io.EOF
	}
	copy(dest, r.vals[r.i])
	r.i++
	return nil
}

// ---- harness ----

type killRecord struct {
	id  uint64
	hit string
}

type harness struct {
	srv   *fakeServer
	db    *sql.DB
	mu    sync.Mutex
	kills []killRecord
	logs  *bytes.Buffer
}

func newHarness(t *testing.T, opts ...Option) *harness {
	t.Helper()
	h := &harness{srv: newFakeServer(), logs: &bytes.Buffer{}}
	var logMu sync.Mutex
	logger := slog.New(slog.NewTextHandler(lockedWriter{&logMu, h.logs}, &slog.HandlerOptions{Level: slog.LevelWarn}))
	base := []Option{
		WithLogger(logger),
		WithKillFunc(func(_ context.Context, t Thread) error {
			hit := h.srv.kill(t.ID)
			h.mu.Lock()
			h.kills = append(h.kills, killRecord{t.ID, hit})
			h.mu.Unlock()
			return nil
		}),
	}
	h.db = sql.OpenDB(New(fakeConnector{h.srv}, append(base, opts...)...))
	t.Cleanup(func() { h.db.Close() })
	return h
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func (h *harness) killList() []killRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]killRecord(nil), h.kills...)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// ---- the edge cases ----

func TestCancelMidQueryKillsStatement(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, err := h.db.QueryContext(ctx, "SLOW scan"); errc <- err }()
	waitFor(t, "statement to start", h.anyRunning)
	cancel()
	if err := <-errc; err == nil {
		t.Fatal("query on a cancelled context returned no error")
	}
	k := h.killList()
	if len(k) != 1 || k[0].hit != "SLOW scan" {
		t.Fatalf("kills = %+v, want exactly one, hitting the abandoned statement", k)
	}
}

func (h *harness) anyRunning() bool {
	h.srv.mu.Lock()
	defer h.srv.mu.Unlock()
	return len(h.srv.running) > 0
}

// The console's filtered reads carry bind args, so database/sql runs them as
// prepared statements: the stmt path must be guarded too.
func TestCancelMidPreparedQueryKillsStatement(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, err := h.db.QueryContext(ctx, "SLOW with args", 1); errc <- err }()
	waitFor(t, "statement to start", h.anyRunning)
	cancel()
	<-errc
	k := h.killList()
	if len(k) != 1 || k[0].hit != "SLOW with args" {
		t.Fatalf("kills = %+v, want exactly one on the prepared statement", k)
	}
}

func TestCancelWithoutDeadlineKills(t *testing.T) {
	// context.WithCancel has no deadline: abandonment is the cancel itself.
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("test context must have no deadline")
	}
	done := make(chan struct{})
	go func() { _, _ = h.db.QueryContext(ctx, "SLOW nodeadline"); close(done) }()
	waitFor(t, "statement to start", h.anyRunning)
	cancel()
	<-done
	if len(h.killList()) != 1 {
		t.Fatalf("kills = %+v, want 1", h.killList())
	}
}

func TestDeadlineExpiryKills(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _ = h.db.QueryContext(ctx, "SLOW deadline")
	if len(h.killList()) != 1 {
		t.Fatalf("kills = %+v, want 1", h.killList())
	}
}

// After the statement is fully read, a cancel that comes before Close must
// send nothing: the thread may already be serving the next statement.
func TestFinishedBeforeCancelNoKill(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	rows, err := h.db.QueryContext(ctx, "FAST")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	if n != 2 {
		t.Fatalf("read %d rows, want 2", n)
	}
	cancel()
	rows.Close()
	time.Sleep(20 * time.Millisecond)
	if k := h.killList(); len(k) != 0 {
		t.Fatalf("a finished statement was killed: %+v", k)
	}
}

func TestClosedThenCancelNoKill(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	rows, err := h.db.QueryContext(ctx, "FAST")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
	cancel()
	time.Sleep(20 * time.Millisecond)
	if k := h.killList(); len(k) != 0 {
		t.Fatalf("a closed statement was killed: %+v", k)
	}
}

// The race the guard exists for: a statement that finishes just as its
// context ends. Whatever wins, a KILL is never sent once the statement's rows
// were closed, and a KILL never hits a statement that is not ours.
func TestCancelRacingCompletionNeverKillsAnotherStatement(t *testing.T) {
	h := newHarness(t)
	h.db.SetMaxOpenConns(1) // one thread: a stray KILL would hit the next statement
	for i := range 300 {
		ctx, cancel := context.WithCancel(context.Background())
		rows, err := h.db.QueryContext(ctx, "FAST race")
		if err != nil {
			cancel()
			continue // the cancel can win before the statement starts
		}
		go cancel()
		for rows.Next() {
		}
		rows.Close()
		// The next statement on the (possibly same) thread: a KILL that
		// arrives now would hit it.
		next, err := h.db.QueryContext(context.Background(), "FAST next")
		if err != nil {
			t.Fatalf("iteration %d: next statement failed: %v", i, err)
		}
		for next.Next() {
		}
		next.Close()
		cancel()
	}
	for _, k := range h.killList() {
		if k.hit == "FAST next" {
			t.Fatalf("a KILL hit another statement: %+v", k)
		}
	}
}

// A connection a KILL was sent for must leave the pool.
func TestKilledConnIsDiscarded(t *testing.T) {
	h := newHarness(t)
	h.db.SetMaxOpenConns(1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _, _ = h.db.QueryContext(ctx, "SLOW discard"); close(done) }()
	waitFor(t, "statement to start", h.anyRunning)
	cancel()
	<-done
	killedID := h.killList()[0].id
	var id, port, sid int64
	var host string
	if err := h.db.QueryRowContext(context.Background(), threadQuery).Scan(&id, &host, &port, &sid); err != nil {
		t.Fatal(err)
	}
	if uint64(id) == killedID {
		t.Fatalf("the pool reused connection %d after a KILL was sent for it", id)
	}
}

func TestConnValidityAfterKill(t *testing.T) {
	srv := newFakeServer()
	c := New(fakeConnector{srv}, WithKillFunc(func(context.Context, Thread) error { return nil }))
	dc, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cn := dc.(*conn)
	if !cn.IsValid() || cn.ResetSession(context.Background()) != nil {
		t.Fatal("fresh connection reported bad")
	}
	cn.kill()
	if cn.IsValid() {
		t.Error("IsValid true after a KILL")
	}
	if !errors.Is(cn.ResetSession(context.Background()), driver.ErrBadConn) {
		t.Error("ResetSession did not return ErrBadConn after a KILL")
	}
}

// A BYO index whose account may not KILL (or a proxy that refuses it): the
// read still fails for its caller as before, nothing hangs or crashes, and the
// operator gets one warning.
func TestKillRefusedLogsWarningAndReturns(t *testing.T) {
	var h *harness
	h = newHarness(t, WithKillFunc(func(context.Context, Thread) error {
		return &mysql.MySQLError{Number: 1095, Message: "You are not owner of thread 7"}
	}))
	for range 3 {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := h.db.QueryContext(ctx, "SLOW refused"); done <- err }()
		waitFor(t, "statement to start", h.anyRunning)
		cancel()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("cancelled query returned no error")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("query hung after a refused KILL")
		}
		h.srv.mu.Lock()
		h.srv.running = map[uint64]string{}
		h.srv.mu.Unlock()
	}
	out := h.logs.String()
	if got := strings.Count(out, "could not stop an abandoned read"); got != 1 {
		t.Fatalf("want exactly one warning for three refused KILLs, got %d:\n%s", got, out)
	}
	if !strings.Contains(out, "1095") {
		t.Errorf("warning does not carry the server's error:\n%s", out)
	}
}

func TestKillHangsIsBounded(t *testing.T) {
	h := newHarness(t,
		WithKillTimeout(100*time.Millisecond),
		WithKillFunc(func(ctx context.Context, _ Thread) error { <-ctx.Done(); return ctx.Err() }))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _, _ = h.db.QueryContext(ctx, "SLOW hang"); close(done) }()
	waitFor(t, "statement to start", h.anyRunning)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a KILL that never answers held the query past its budget")
	}
	if !strings.Contains(h.logs.String(), "could not stop an abandoned read") {
		t.Errorf("a timed-out KILL was not reported:\n%s", h.logs.String())
	}
}

func TestNoSuchThreadIsQuiet(t *testing.T) {
	h := newHarness(t, WithKillFunc(func(context.Context, Thread) error {
		return &mysql.MySQLError{Number: 1094, Message: "Unknown thread id"}
	}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _, _ = h.db.QueryContext(ctx, "SLOW gone"); close(done) }()
	waitFor(t, "statement to start", h.anyRunning)
	cancel()
	<-done
	if h.logs.Len() != 0 {
		t.Fatalf("a thread that already ended was reported:\n%s", h.logs.String())
	}
}

// A context that cannot end is never watched, and nothing is killed.
func TestBackgroundContextNeverKilled(t *testing.T) {
	h := newHarness(t)
	rows, err := h.db.QueryContext(context.Background(), "FAST bg")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if len(h.killList()) != 0 {
		t.Fatal("killed a statement on a context that cannot end")
	}
}

// Work that runs on its own context (a background refresh started by a
// request) survives the request that asked for it.
func TestOtherContextsCancelDoesNotKillStatement(t *testing.T) {
	h := newHarness(t)
	_, reqCancel := context.WithCancel(context.Background())
	workCtx, workCancel := context.WithTimeout(context.Background(), time.Minute)
	defer workCancel()
	done := make(chan struct{})
	go func() { _, _ = h.db.QueryContext(workCtx, "SLOW refresh"); close(done) }()
	waitFor(t, "statement to start", h.anyRunning)
	reqCancel()
	time.Sleep(50 * time.Millisecond)
	if len(h.killList()) != 0 || !h.anyRunning() {
		t.Fatal("a statement on its own context was killed by another context's cancel")
	}
	workCancel()
	<-done
}

// Writes are never killed: a timed-out write is left to commit (see the
// package doc).
func TestExecIsNeverKilled(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _, _ = h.db.ExecContext(ctx, "INSERT slow"); close(done) }()
	waitFor(t, "statement to start", h.anyRunning)
	cancel()
	<-done
	if k := h.killList(); len(k) != 0 {
		t.Fatalf("a write was killed: %+v", k)
	}
}

func TestAlreadyCancelledContextSendsNothing(t *testing.T) {
	h := newHarness(t)
	db := h.db
	// Get a connection first so the cancelled call reaches the driver.
	c, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.QueryContext(ctx, "SLOW never"); err == nil {
		t.Fatal("no error on a cancelled context")
	}
	if len(h.killList()) != 0 || h.anyRunning() {
		t.Fatal("a statement was sent or killed on an already-cancelled context")
	}
}

// A connection whose CONNECTION_ID() cannot be read still serves, unguarded,
// with one warning.
func TestConnectionIDUnreadableDisablesGuard(t *testing.T) {
	h := newHarness(t)
	h.srv.idErr = errors.New("proxy refused")
	rows, err := h.db.QueryContext(context.Background(), "FAST")
	if err != nil {
		t.Fatalf("connection without an id refused to serve: %v", err)
	}
	rows.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _, _ = h.db.QueryContext(ctx, "SLOW noid"); close(done) }()
	waitFor(t, "statement to start", h.anyRunning)
	cancel()
	<-done
	if len(h.killList()) != 0 {
		t.Fatal("KILL sent without a known thread id")
	}
	if !strings.Contains(h.logs.String(), "could not read CONNECTION_ID()") {
		t.Fatalf("missing warning:\n%s", h.logs.String())
	}
}

// A guarded read still reports its column types (the SQL pages read them).
func TestColumnTypesForwarded(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rows, err := h.db.QueryContext(ctx, "FAST types")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ct, err := rows.ColumnTypes()
	if err != nil || len(ct) != 1 || ct[0].Name() != "n" {
		t.Fatalf("ColumnTypes = %v, %v", ct, err)
	}
}

// streamRows sends one row at once, then the server keeps working on the rest
// (a large result being produced) until killed or abandoned.
type streamRows struct {
	ctx    context.Context
	killed chan struct{}
	srv    *fakeServer
	id     uint64
	sent   bool
}

func (r *streamRows) Columns() []string { return []string{"n"} }
func (r *streamRows) Next(dest []driver.Value) error {
	if !r.sent {
		r.sent = true
		dest[0] = int64(1)
		return nil
	}
	select {
	case <-r.killed:
		return &mysql.MySQLError{Number: 1317, Message: "Query execution was interrupted"}
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}

// Close on an abandoned stream fails like the real driver's (the socket is
// gone); the server is still producing rows.
func (r *streamRows) Close() error {
	if r.ctx.Err() != nil {
		return errors.New("invalid connection")
	}
	r.srv.stop(r.id)
	return nil
}

func TestCancelWhileStreamingRowsKills(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	rows, err := h.db.QueryContext(ctx, "STREAM big result")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("no first row")
	}
	cancel()
	for rows.Next() {
	}
	rows.Close()
	k := h.killList()
	if len(k) != 1 || k[0].hit != "STREAM big result" {
		t.Fatalf("kills = %+v, want one on the streaming statement", k)
	}
}

// end, reached on the error path with the context already over, must send
// the KILL itself: the watcher may not have run yet, and once end returns the
// statement is marked over and the watcher will not.
func TestEndAfterCancelKillsEvenBeforeWatcherRuns(t *testing.T) {
	srv := newFakeServer()
	var kills atomic.Int32
	c := New(fakeConnector{srv}, WithKillFunc(func(context.Context, Thread) error { kills.Add(1); return nil }))
	cn := &conn{c: c, thread: Thread{ID: 7, Server: "h1:3306/1"}, hasID: true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	st := &statement{cn: cn, ctx: ctx, stop: make(chan struct{}), exited: make(chan struct{})}
	close(st.exited) // no watcher: end alone decides
	st.end(false)
	if kills.Load() != 1 {
		t.Fatalf("kills = %d, want 1", kills.Load())
	}
	if cn.IsValid() {
		t.Fatal("connection still valid after its KILL")
	}
}

func TestEndCompleteNeverKills(t *testing.T) {
	srv := newFakeServer()
	var kills atomic.Int32
	c := New(fakeConnector{srv}, WithKillFunc(func(context.Context, Thread) error { kills.Add(1); return nil }))
	cn := &conn{c: c, thread: Thread{ID: 7, Server: "h1:3306/1"}, hasID: true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	st := &statement{cn: cn, ctx: ctx, stop: make(chan struct{}), exited: make(chan struct{})}
	close(st.exited)
	st.end(true)
	if kills.Load() != 0 || !cn.IsValid() {
		t.Fatal("a statement the server was done with was killed")
	}
}

// end must not return while the watcher's KILL is in flight: database/sql
// would hand the connection to the next statement, which the KILL then hits.
func TestEndWaitsForInFlightKill(t *testing.T) {
	srv := newFakeServer()
	release := make(chan struct{})
	entered := make(chan struct{})
	c := New(fakeConnector{srv}, WithKillFunc(func(context.Context, Thread) error {
		close(entered)
		<-release
		return nil
	}))
	cn := &conn{c: c, thread: Thread{ID: 7, Server: "h1:3306/1"}, hasID: true}
	ctx, cancel := context.WithCancel(context.Background())
	st := &statement{cn: cn, ctx: ctx, stop: make(chan struct{}), exited: make(chan struct{})}
	go st.watch()
	cancel()
	<-entered
	ended := make(chan struct{})
	go func() { st.end(true); close(ended) }()
	select {
	case <-ended:
		t.Fatal("end returned while the KILL was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-ended
}

// The KILL connection is dialed anew; behind a name with several addresses
// it can reach another server, where the same thread id is someone else's.
// The default KillFunc checks the server before sending anything.
func TestKillOnOtherServerIsRefused(t *testing.T) {
	srv := newFakeServer()
	// The statement ran on h1; the KILL connection lands on h2.
	srv.hostFor = func(uint64) string { return "h2" }
	c := New(fakeConnector{srv})
	cn := &conn{c: c, thread: Thread{ID: 101, Server: "h1:3306/1"}, hasID: true}
	err := c.dialAndKill(context.Background(), cn.thread)
	if !errors.Is(err, errOtherServer) {
		t.Fatalf("err = %v, want errOtherServer", err)
	}
	if len(srv.killSent) != 0 {
		t.Fatalf("KILL sent on the other server: %v", srv.killSent)
	}
}

// Same server: the default KillFunc sends KILL QUERY <id>.
func TestKillOnSameServerIsSent(t *testing.T) {
	srv := newFakeServer()
	c := New(fakeConnector{srv})
	if err := c.dialAndKill(context.Background(), Thread{ID: 101, Server: "h1:3306/1"}); err != nil {
		t.Fatal(err)
	}
	if len(srv.killSent) != 1 || srv.killSent[0] != "h1 KILL QUERY 101" {
		t.Fatalf("killSent = %v, want one KILL QUERY 101 on h1", srv.killSent)
	}
}

// A caller that gives up while a new connection reads its identity gets its
// own error, no unguarded connection and no warning.
func TestConnectCancelledDuringIDReadReturnsError(t *testing.T) {
	srv := newFakeServer()
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	srv.idHook = func(context.Context) error { cancel(); return errors.New("invalid connection") }
	c := New(fakeConnector{srv}, WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	dc, err := c.Connect(ctx)
	if err == nil || dc != nil {
		t.Fatalf("Connect = %v, %v; want the context's error and no connection", dc, err)
	}
	if logs.Len() != 0 {
		t.Fatalf("a cancelled connect was reported as a guard failure:\n%s", logs.String())
	}
}

// go-sql-driver returns ErrBadConn only when nothing was written: there is no
// statement of ours to kill, and a KILL could hit an earlier one on the thread.
func TestBadConnBeforeSendNoKill(t *testing.T) {
	srv := newFakeServer()
	cn := &conn{c: New(fakeConnector{srv}, WithKillFunc(func(context.Context, Thread) error {
		t.Error("KILL sent for a statement that was never written")
		return nil
	})), thread: Thread{ID: 9, Server: "h1:3306/1"}, hasID: true}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The context ends while the driver fails before writing anything.
	_, err := cn.guard(ctx, func() (driver.Rows, error) { cancel(); return nil, driver.ErrBadConn })
	if !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("err = %v", err)
	}
	if !cn.IsValid() {
		t.Fatal("connection poisoned though nothing was killed")
	}
}
