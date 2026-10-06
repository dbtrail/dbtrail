package consoleapp

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/testutil/mysqlwire"
)

// panickyCreds is the port's credentials, panicking for one user name: a
// panic on a client connection's own goroutine, where the client library
// reads packets.
type panickyCreds struct {
	flashbackCreds
}

func (p panickyCreds) GetCredential(username string) (server.Credential, bool, error) {
	if username == "boom" {
		panic("index out of range [3] with length 2")
	}
	return p.flashbackCreds.GetCredential(username)
}

// syncBuffer is a log sink two goroutines can write and read.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// A panic on one client connection ends that connection and nothing else:
// the process goes on (in `watch` it is also capture), the panic is logged
// with its stack, the client reads a closed socket, and the port goes on
// serving the connections that come after.
func TestFlashbackConnPanicEndsOnlyThatConnection(t *testing.T) {
	srv := newFlashbackConsole(t, "tok")
	mysrv, err := shim.NewMySQLServer("")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var logs syncBuffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	creds := panickyCreds{flashbackCreds{srv: srv}}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				handleFlashbackConn(ctx, srv, c, mysrv, creds, shim.NewGate(0), flashbackConfig{}.withDefaults(), logger)
			}()
		}
	}()
	defer func() {
		cancel()
		ln.Close()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("a connection's goroutine did not end")
		}
	}()

	if c, err := mysqlwire.Dial(ln.Addr().String(), "boom", "tok", ""); err == nil {
		c.Close()
		t.Fatal("the connection that panicked completed its login")
	}
	// The log line is written by the connection's goroutine after its
	// socket is closed, which is what the client just saw.
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(logs.String(), "panicked") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	out := logs.String()
	for _, want := range []string{"level=ERROR", "a client connection panicked and was closed", "index out of range [3] with length 2", "handleFlashbackConn", "panickyCreds"} {
		if !strings.Contains(out, want) {
			t.Errorf("the log does not have %q:\n%s", want, out)
		}
	}
	// The port goes on: a login, then a command.
	for range 2 {
		c, err := mysqlwire.Dial(ln.Addr().String(), "no-such-server", "tok", "")
		if err != nil {
			t.Fatalf("a connection after the panic: %v", err)
		}
		if _, err := c.Ping(); err != nil {
			t.Errorf("PING on a connection after the panic: %v", err)
		}
		c.Close()
	}
}

// The connection's own note is run after the log line (a connection that
// asked its source for session tracking tells the port not to ask again).
func TestFlashbackConnPanickedRunsTheConnectionsNote(t *testing.T) {
	var logs bytes.Buffer
	ran := 0
	flashbackConnPanicked(slog.New(slog.NewTextHandler(&logs, nil)), &net.TCPAddr{IP: net.IPv4(10, 0, 0, 7), Port: 5123}, "boom", []byte("goroutine 9 [running]:\nsome.frame()"), func() { ran++ })
	if ran != 1 {
		t.Errorf("the note ran %d times, want 1", ran)
	}
	for _, want := range []string{"10.0.0.7:5123", "panic=boom", "some.frame()"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("the log does not have %q: %s", want, logs.String())
		}
	}
	flashbackConnPanicked(slog.New(slog.NewTextHandler(&logs, nil)), &net.TCPAddr{}, "boom", nil, nil)
}

// What an operator reads when a source does not report session changes: once
// per server, at warn level, saying what is missing, what follows from it and
// that the rest works.
func TestWarnSessionUntracked(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	id := "srv-" + t.Name()
	for range 3 {
		warnSessionUntracked(logger, id, "shop", "the source did not agree to CLIENT_SESSION_TRACK when the connection opened")
	}
	warnSessionUntracked(logger, id+"-other", "billing", "the source refused to: ERROR 1193 (HY000): Unknown system variable")
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("%d lines for two servers, want one each:\n%s", len(lines), logs.String())
	}
	t.Log(lines[0])
	const want = `level=WARN msg="read routing: this source does not tell the port when a statement changes a session setting, ` +
		`so a setting changed inside a stored function that a SELECT calls is not seen until the next statement that changes settings (a SET, a write, a CALL); ` +
		`until then the copy can answer an expensive read under the setting from before. Everything else about read routing works as before" ` +
		`server=shop why="the source did not agree to CLIENT_SESSION_TRACK when the connection opened" note="logged once per server"`
	if !strings.HasSuffix(lines[0], want) {
		t.Errorf("the line is\n%s\nwant it to end with\n%s", lines[0], want)
	}
	if !strings.Contains(lines[1], "server=billing") || !strings.Contains(lines[1], "Unknown system variable") {
		t.Errorf("the second server's line: %s", lines[1])
	}
	if strings.ContainsRune(logs.String(), '\u2014') {
		t.Error("an em dash in the text")
	}
}

// The wiring from a Forwarder's report to what the port does with it: a
// source that does not report session changes is said in the log once and on
// GET /api/flashback until a connection is tracked; a source whose
// connections broke when asked is not asked again by later connections.
func TestBindReadRouterSessionTracking(t *testing.T) {
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", FlashbackListen: "127.0.0.1:3308",
		ReadRouting: console.ReadRoutingConfig{MaxCopyAge: time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	id := "srv-" + t.Name()
	bind := func() *readrouter.Forwarder {
		h := shim.NewHandler(nil, nil)
		h.BindFreeSQL(routeTestFreeSQL{})
		fw := bindReadRouter(h, srv, console.FlashbackTarget{ID: id, SQL: &console.SQLOnCopy{}, ForwardDSN: "nobody:x@tcp(127.0.0.1:1)/none", SourceSSL: console.ServerEntry{}.SourceSSL()},
			"shop", flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.DefaultPolicy()}, logger)
		if fw == nil {
			t.Fatal("no router bound")
		}
		return fw
	}
	note := func() string {
		req := httptest.NewRequest("GET", "http://127.0.0.1/api/flashback", nil)
		req.Header.Set("Authorization", "Bearer tok")
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		var fb struct {
			Routing struct {
				Servers map[string]struct {
					SessionUntracked string `json:"session_untracked"`
				} `json:"servers"`
			} `json:"routing"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &fb); err != nil {
			t.Fatalf("decode %s: %v", rec.Body.String(), err)
		}
		return fb.Routing.Servers[id].SessionUntracked
	}
	warns := func() int {
		return strings.Count(logs.String(), "level=WARN msg=\"read routing: this source does not tell the port")
	}

	first := bind()
	if !first.TrackSession {
		t.Fatal("the port's forwarder does not ask the source for session tracking")
	}
	if note() != "" || warns() != 0 {
		t.Fatalf("before any connection: note %q, %d warning(s)", note(), warns())
	}
	// A source that declines: said, and still asked by the next connection.
	first.OnUntracked("the source refused", false)
	first.OnConnect(nil)
	if note() != "the source refused" || warns() != 1 {
		t.Errorf("after a source declined: note %q, %d warning(s); want the reason and one", note(), warns())
	}
	second := bind()
	if !second.TrackSession {
		t.Error("a source that declined once is no longer asked")
	}
	// A connection that is tracked clears the note; the warning is not repeated.
	second.OnConnect(nil)
	if note() != "" {
		t.Errorf("after a tracked connection the note is still %q", note())
	}
	// A source whose connection broke when asked: not asked again.
	second.OnUntracked("its answers could not be read", true)
	if note() != "its answers could not be read" || warns() != 1 {
		t.Errorf("after a connection broke: note %q, %d warning(s); want the reason and still one per server", note(), warns())
	}
	third := bind()
	if third.TrackSession {
		t.Error("a source whose connections break when asked for session tracking is asked again")
	}
	// Its connections say nothing about tracking either way: the note stays.
	third.OnConnect(nil)
	if note() == "" {
		t.Error("a connection that did not ask cleared the note")
	}
}
