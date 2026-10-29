package readrouter

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/config"
)

// slowSource is a MySQL-protocol endpoint whose SLEEP statements do not
// answer until the test ends, so a statement can be caught in flight. It
// records the KILL statements it is sent and tells which connection each
// arrived on.
type slowSource struct {
	addr    string
	release chan struct{}
	entered chan uint32 // connection id of each statement that started to sleep

	mu     sync.Mutex
	killed []string
	// running is what each connection is running, as the source's process
	// list shows it (the statement's text; a prepared one with its
	// arguments in place), and stop ends it the way KILL QUERY does.
	running map[uint32]running
	stop    map[uint32]chan struct{}
	finish  map[uint32]chan struct{} // ends the statement with its answer (finishOK)
	// deaf: KILL QUERY is accepted and does nothing, like a statement that
	// does not look at the kill flag while it waits.
	deaf bool
	// processlist, when set, replaces what the process list shows for id.
	processlist func(id uint32, r running) [][]any
}

type running struct{ command, info string }

type slowHandler struct {
	server.EmptyHandler
	src *slowSource
	id  uint32
}

func (h slowHandler) HandleQuery(q string) (*mysql.Result, error) {
	switch {
	case strings.Contains(q, "SLEEP"):
		return h.sleep(running{"Query", q})
	case strings.Contains(q, "PROCESSLIST"):
		return h.src.processlistRows(q)
	case strings.HasPrefix(q, "KILL"):
		h.src.kill(q)
		return mysql.NewResultReserveResultset(0), nil
	}
	return oneRow()
}

func oneRow() (*mysql.Result, error) {
	rs, err := mysql.BuildSimpleTextResultset([]string{"x"}, [][]any{{"1"}})
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(rs), nil
}

// sleep holds the statement until the test ends or a KILL QUERY names this
// connection, which answers 1317 as MySQL does.
func (h slowHandler) sleep(r running) (*mysql.Result, error) {
	stop, finish := make(chan struct{}), make(chan struct{})
	h.src.mu.Lock()
	if h.src.running == nil {
		h.src.running, h.src.stop, h.src.finish = map[uint32]running{}, map[uint32]chan struct{}{}, map[uint32]chan struct{}{}
	}
	h.src.running[h.id], h.src.stop[h.id], h.src.finish[h.id] = r, stop, finish
	h.src.mu.Unlock()
	defer func() {
		h.src.mu.Lock()
		delete(h.src.running, h.id)
		delete(h.src.stop, h.id)
		delete(h.src.finish, h.id)
		h.src.mu.Unlock()
	}()
	h.src.entered <- h.id
	select {
	case <-h.src.release:
		return oneRow()
	case <-finish:
		return oneRow()
	case <-stop:
		return nil, mysql.NewError(mysql.ER_QUERY_INTERRUPTED, "Query execution was interrupted")
	}
}

func (h slowHandler) HandleStmtPrepare(q string) (int, int, any, error) {
	return strings.Count(q, "?"), 1, nil, nil
}

func (h slowHandler) HandleStmtExecute(_ any, q string, args []any) (*mysql.Result, error) {
	if !strings.Contains(q, "SLEEP") {
		return oneRow()
	}
	// The process list shows a prepared statement with its arguments in
	// place, quoted when they are text (measured on MySQL 8.0).
	for _, a := range args {
		v := fmt.Sprint(a)
		if b, ok := a.([]byte); ok {
			v = "'" + string(b) + "'"
		}
		q = strings.Replace(q, "?", v, 1)
	}
	return h.sleep(running{"Execute", q})
}

func (h slowHandler) HandleStmtClose(any) error { return nil }

// processlistRows answers SELECT USER, COMMAND, INFO FROM ... PROCESSLIST
// WHERE ID = n for the connection n is.
func (s *slowSource) processlistRows(q string) (*mysql.Result, error) {
	var id uint32
	if _, err := fmt.Sscanf(q[strings.LastIndex(q, "=")+1:], "%d", &id); err != nil {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "no id in "+q)
	}
	s.mu.Lock()
	r, busy := s.running[id]
	override := s.processlist
	s.mu.Unlock()
	var rows [][]any
	switch {
	case override != nil:
		rows = override(id, r)
	case busy:
		rows = [][]any{{"u", r.command, r.info}}
	default:
		rows = [][]any{{"u", "Sleep", nil}}
	}
	rs, err := mysql.BuildSimpleTextResultset([]string{"USER", "COMMAND", "INFO"}, rows)
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(rs), nil
}

// finishOK ends the statement connection id is running with its answer.
func (s *slowSource) finishOK(id uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if finish, ok := s.finish[id]; ok {
		close(finish)
		delete(s.finish, id)
	}
}

func (s *slowSource) kill(q string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.killed = append(s.killed, q)
	var id uint32
	if _, err := fmt.Sscanf(q, "KILL QUERY %d", &id); err == nil && !s.deaf {
		if stop, ok := s.stop[id]; ok {
			close(stop)
			delete(s.stop, id)
		}
	}
}

func newSlowSource(t *testing.T) *slowSource {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	src := &slowSource{addr: ln.Addr().String(), release: make(chan struct{}), entered: make(chan uint32, 64)}
	t.Cleanup(func() { close(src.release); ln.Close() })
	conf := server.NewServer("8.0.11", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	auth := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	if err := auth.AddUser("u", "p"); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				h := &slowHandler{src: src}
				mc, err := server.NewCustomizedConn(c, conf, auth, h)
				if err != nil {
					return
				}
				h.id = mc.ConnectionID()
				for mc.HandleCommand() == nil {
				}
			}()
		}
	}()
	return src
}

func (s *slowSource) kills() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.killed...)
}

func newSlowForwarder(t *testing.T, src *slowSource) *Forwarder {
	t.Helper()
	fw, err := NewForwarder("u:p@tcp("+src.addr+")/", config.SSL{Mode: "disabled"}, DefaultPolicy(), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return fw
}

// A statement in flight is interrupted from another goroutine (its context
// ends: the client left, the deadline passed, the server's connections were
// dropped) without that goroutine touching the client library's connection
// state. Run under -race: the interrupt and the statement overlap by design.
func TestForwarder_interruptedMidStatement(t *testing.T) {
	src := newSlowSource(t)

	// Blocked waiting for the answer.
	fw := newSlowForwarder(t, src)
	if _, err := fw.Forward(context.Background(), "SELECT 1", &BufferSink{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := fw.Forward(ctx, "SELECT SLEEP(25)", &BufferSink{})
		done <- err
	}()
	select {
	case <-src.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the statement never reached the source")
	}
	cancel()
	select {
	case err := <-done:
		// The client is told why: the context's own reason, not the
		// closed socket that reason produced.
		if !IsLost(err) || !strings.Contains(err.Error(), context.Canceled.Error()) {
			t.Errorf("an interrupted statement answered %v, want the lost-connection error naming the cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the statement did not return after its context ended: a hang, not a lost connection")
	}
	if _, err := fw.Forward(context.Background(), "SELECT 1", &BufferSink{}); !IsLost(err) {
		t.Errorf("after the interrupt the connection is used again: %v", err)
	}
	fw.Close()

	// The interrupt lands anywhere in a statement: while it is being written,
	// while its rows are read, just after it ended, between two statements.
	for i := range 40 {
		fw := newSlowForwarder(t, src)
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 6 {
				if _, err := fw.Forward(ctx, "SELECT 1", &BufferSink{}); err != nil {
					return
				}
			}
		}()
		time.Sleep(time.Duration(i%8) * 150 * time.Microsecond)
		cancel()
		wg.Wait()
		fw.Close()
	}
}

// KillThreads ends connections on the source by id, over one connection of
// its own; an id the source no longer knows is not a failure, an account
// that cannot log in is.
func TestKillThreads(t *testing.T) {
	src := newSlowSource(t)
	dsn := "u:p@tcp(" + src.addr + ")/"
	off := config.SSL{Mode: "disabled"}
	if err := KillThreads(context.Background(), dsn, off, []uint32{7, 9}, 5*time.Second); err != nil {
		t.Fatalf("KillThreads: %v", err)
	}
	if got := src.kills(); len(got) != 2 || got[0] != "KILL 7" || got[1] != "KILL 9" {
		t.Errorf("the source was sent %q, want KILL 7 and KILL 9", got)
	}
	if err := KillThreads(context.Background(), dsn, off, nil, 5*time.Second); err != nil || len(src.kills()) != 2 {
		t.Errorf("nothing to end: err %v, statements sent %q", err, src.kills())
	}
	err := KillThreads(context.Background(), "u:wrong@tcp("+src.addr+")/", off, []uint32{7}, 5*time.Second)
	if _, refused := AccountRefused(err); !refused || strings.Contains(err.Error(), "gone away") {
		t.Errorf("an account that cannot log in: %v, want the source's own refusal", err)
	}
}

// The source's "no such thread" is the normal answer for a connection that
// ended on its own; any other error of a KILL is reported, and the rest are
// still sent.
func TestKillThreads_unknownThreadIsNotAFailure(t *testing.T) {
	var sent []string
	run := func(q string) error {
		sent = append(sent, q)
		switch q {
		case "KILL 1":
			return mysql.NewError(mysql.ER_NO_SUCH_THREAD, "Unknown thread id: 1")
		case "KILL 2":
			return mysql.NewError(mysql.ER_KILL_DENIED_ERROR, "You are not owner of thread 2")
		}
		return nil
	}
	err := killEach([]uint32{1, 2, 3}, run)
	if len(sent) != 3 {
		t.Errorf("sent %q, want all three", sent)
	}
	if err == nil || !strings.Contains(err.Error(), "thread 2") || strings.Contains(err.Error(), "thread id: 1") {
		t.Errorf("got %v, want only thread 2's refusal", err)
	}
}
