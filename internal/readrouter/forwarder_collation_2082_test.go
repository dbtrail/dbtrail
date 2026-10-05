package readrouter

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/config"
)

// collationSource is a source that records every statement it is sent and
// answers the session's collation from what the test set.
type collationSource struct {
	server.EmptyHandler
	mu        sync.Mutex
	seen      []string
	collation string
	setErr    error
	// drop: the connection is closed under the SET instead of answering it.
	drop bool
	conn net.Conn
}

func (h *collationSource) statements() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.seen...)
}

func (h *collationSource) HandleQuery(q string) (*mysql.Result, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen = append(h.seen, q)
	switch q {
	case "SELECT @@collation_connection":
		rs, err := mysql.BuildSimpleTextResultset([]string{"@@collation_connection"}, [][]any{{h.collation}})
		if err != nil {
			return nil, err
		}
		return &mysql.Result{Resultset: rs}, nil
	case "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci":
		if h.drop {
			h.conn.Close()
			return nil, errors.New("gone")
		}
		if h.setErr != nil {
			return nil, h.setErr
		}
		h.collation = "utf8mb4_unicode_ci"
	}
	return mysql.NewResultReserveResultset(0), nil
}

// newCollationForwarder is a Forwarder over a source that greets with the
// given version and gives a session the given collation.
func newCollationForwarder(t *testing.T, version, collation string) (*Forwarder, *collationSource) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	h := &collationSource{collation: collation}
	conf := server.NewServer(version, mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
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
				_ = c.SetDeadline(time.Now().Add(30 * time.Second))
				h.mu.Lock()
				h.conn = c
				h.mu.Unlock()
				mc, err := server.NewCustomizedConn(c, conf, auth, h)
				if err != nil {
					return
				}
				for mc.HandleCommand() == nil {
				}
			}()
		}
	}()
	f, err := NewForwarder("u:p@tcp("+ln.Addr().String()+")/db?tls=false", config.SSL{Mode: "disabled"}, DefaultPolicy(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.Close)
	return f, h
}

// The upstream handshake asks for utf8mb4_0900_ai_ci. A MariaDB that does not
// know it (10.11) gives the session its own default, utf8mb4_general_ci,
// which no client asked for and under which the copy cannot answer. The
// forwarder then names utf8mb4_unicode_ci on that new connection, once,
// before anything of the client's. A source that gave what was asked is sent
// nothing, and MySQL is not even asked.
func TestForwarder_collationTheSourceDidNotKnow(t *testing.T) {
	ctx := context.Background()
	const ask, set = "SELECT @@collation_connection", "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"
	cases := []struct {
		name, version, collation string
		want                     []string
	}{
		{"mysql 8.4", "8.4.9", "utf8mb4_0900_ai_ci", []string{"DO 1"}},
		{"mysql 8.0", "8.0.11", "utf8mb4_0900_ai_ci", []string{"DO 1"}},
		{"mariadb 11.4 knows the collation", "11.4.13-MariaDB-ubu2404-log", "utf8mb4_0900_ai_ci", []string{ask, "DO 1"}},
		{"mariadb 10.11 fell back", "10.11.9-MariaDB-ubu2204-log", "utf8mb4_general_ci", []string{ask, set, "DO 1"}},
		{"the old handshake spelling", "5.5.5-10.11.9-MariaDB", "utf8mb4_general_ci", []string{ask, set, "DO 1"}},
		{"a server default that is not general_ci either", "10.11.9-MariaDB", "latin1_swedish_ci", []string{ask, set, "DO 1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, src := newCollationForwarder(t, tc.version, tc.collation)
			var told []uint16
			f.OnSession = func(st uint16) { told = append(told, st) }
			if len(src.statements()) != 0 {
				t.Fatal("the source was sent something before the first statement")
			}
			if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); err != nil {
				t.Fatal(err)
			}
			// A second statement, and another way in: nothing is repeated.
			if _, err := f.Forward(ctx, "DO 2", &BufferSink{}); err != nil {
				t.Fatal(err)
			}
			if got, want := src.statements(), append(append([]string(nil), tc.want...), "DO 2"); !reflect.DeepEqual(got, want) {
				t.Errorf("the source saw %q, want %q", got, want)
			}
			// The port's own statements are not the client's first: the
			// session's state is told once, with the client's statement.
			if len(told) != 1 {
				t.Errorf("OnSession told %d time(s), want once (with the client's first statement)", len(told))
			}
		})
	}
	t.Run("the SET fails: the connection still works", func(t *testing.T) {
		f, src := newCollationForwarder(t, "10.11.9-MariaDB", "utf8mb4_general_ci")
		src.setErr = mysql.NewError(mysql.ER_UNKNOWN_COLLATION, "Unknown collation: 'utf8mb4_unicode_ci'")
		var said []string
		f.OnCollation = func(collation string, err error) { said = append(said, collation+": "+err.Error()) }
		for _, stmt := range []string{"DO 1", "DO 2"} {
			if _, err := f.Forward(ctx, stmt, &BufferSink{}); err != nil {
				t.Fatalf("%s: %v", stmt, err)
			}
		}
		if got, want := src.statements(), []string{ask, set, "DO 1", "DO 2"}; !reflect.DeepEqual(got, want) {
			t.Errorf("the source saw %q, want %q (asked once, not again)", got, want)
		}
		if f.Lost() != nil {
			t.Error("a refused SET NAMES lost the connection")
		}
		if len(said) != 1 || !strings.HasPrefix(said[0], "utf8mb4_general_ci: ") || !strings.Contains(said[0], "Unknown collation") {
			t.Errorf("OnCollation was told %q, want once: the session's collation and the source's refusal", said)
		}
	})
	t.Run("the connection breaks while it is settled", func(t *testing.T) {
		f, src := newCollationForwarder(t, "10.11.9-MariaDB", "utf8mb4_general_ci")
		src.drop = true
		var connect []error
		f.OnConnect = func(err error) { connect = append(connect, err) }
		_, err := f.Forward(ctx, "DO 1", &BufferSink{})
		if err == nil {
			t.Fatal("a statement was answered on a connection that broke while it was opened")
		}
		if len(connect) != 1 || connect[0] == nil {
			t.Errorf("OnConnect was told %v, want the one failure", connect)
		}
		for _, q := range src.statements() {
			if q == "DO 1" {
				t.Error("the client's statement reached the source")
			}
		}
	})
	t.Run("the first use is a prepare or a plan", func(t *testing.T) {
		f, src := newCollationForwarder(t, "10.11.9-MariaDB", "utf8mb4_general_ci")
		if err := f.UseDB(ctx, "db"); err != nil {
			t.Fatal(err)
		}
		_, _ = f.Decide(ctx, "SELECT a, count(*) FROM t GROUP BY a")
		got := src.statements()
		if len(got) < 3 || got[0] != ask || got[1] != set {
			t.Errorf("the source saw %q, want the collation settled before the EXPLAIN", got)
		}
	})
}
