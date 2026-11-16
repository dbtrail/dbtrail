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

// explainSource records every statement it is sent; the SET of the plan
// format can be refused (setErr) or answered by closing the connection (drop).
type explainSource struct {
	server.EmptyHandler
	mu     sync.Mutex
	seen   []string
	setErr error
	drop   bool
	conn   net.Conn
}

func (h *explainSource) statements() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.seen...)
}

func (h *explainSource) HandleQuery(q string) (*mysql.Result, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen = append(h.seen, q)
	if q == ExplainVersionSet {
		if h.drop {
			h.conn.Close()
			return nil, errors.New("gone")
		}
		if h.setErr != nil {
			return nil, h.setErr
		}
	}
	return mysql.NewResultReserveResultset(0), nil
}

// newExplainForwarder is a Forwarder over a source that greets with version.
func newExplainForwarder(t *testing.T, version string) (*Forwarder, *explainSource) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	h := &explainSource{}
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

func TestExplainVersionDefaultsToTwo(t *testing.T) {
	for _, tc := range []struct {
		version string
		want    bool
	}{
		{"9.7.4", true},
		{"9.0.0", true},
		{"9.7.2-3", true}, // Percona Server spells its build after a dash
		{"26.1.0", true},  // the numbering after 9.7: not supported, still MySQL
		{"8.4.9", false},
		{"8.0.11", false},
		{"8.4.3-3", false},
		{"11.4.13-MariaDB-ubu2404-log", false},
		{"12.3.1-MariaDB", false},
		{"5.5.5-10.11.9-MariaDB", false},
		{"", false},
		{"9", false},
		{"x.7.4", false},
		{" 9.7.4", false},
	} {
		if got := ExplainVersionDefaultsToTwo(tc.version); got != tc.want {
			t.Errorf("ExplainVersionDefaultsToTwo(%q) = %v, want %v", tc.version, got, tc.want)
		}
	}
}

// A MySQL 9 source answers EXPLAIN FORMAT=JSON in a layout ParsePlan does not
// read, so the forwarder asks for version 1 once on each connection it opens,
// before anything of the client's. 8.0, 8.4 and MariaDB are sent nothing.
func TestForwarder_explainVersionOnMySQL9(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, version string
		want          []string
	}{
		{"mysql 9.7", "9.7.4", []string{ExplainVersionSet, "DO 1", "DO 2"}},
		{"mysql 8.4", "8.4.9", []string{"DO 1", "DO 2"}},
		{"mysql 8.0", "8.0.11", []string{"DO 1", "DO 2"}},
		// MariaDB is asked its collation (settleCollation), nothing about plans.
		{"mariadb 12.3", "12.3.1-MariaDB", []string{"SELECT @@collation_connection", "DO 1", "DO 2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, src := newExplainForwarder(t, tc.version)
			f.OnExplainFormat = func(err error) { t.Errorf("OnExplainFormat(%v) on a source that refused nothing", err) }
			var told []uint16
			f.OnSession = func(st uint16) { told = append(told, st) }
			for _, stmt := range []string{"DO 1", "DO 2"} {
				if _, err := f.Forward(ctx, stmt, &BufferSink{}); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}
			if got := src.statements(); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("the source saw %q, want %q", got, tc.want)
			}
			// The port's own statement is not the client's first.
			if len(told) != 1 {
				t.Errorf("OnSession told %d time(s), want once (with the client's first statement)", len(told))
			}
		})
	}
	t.Run("the source refuses the SET: the connection still works", func(t *testing.T) {
		f, src := newExplainForwarder(t, "9.7.4")
		src.setErr = mysql.NewError(mysql.ER_UNKNOWN_SYSTEM_VARIABLE, "Unknown system variable 'explain_json_format_version'")
		var told []error
		f.OnExplainFormat = func(err error) { told = append(told, err) }
		for _, stmt := range []string{"DO 1", "DO 2"} {
			if _, err := f.Forward(ctx, stmt, &BufferSink{}); err != nil {
				t.Fatalf("%s: %v", stmt, err)
			}
		}
		if got, want := src.statements(), []string{ExplainVersionSet, "DO 1", "DO 2"}; !reflect.DeepEqual(got, want) {
			t.Errorf("the source saw %q, want %q (asked once, not again)", got, want)
		}
		if f.Lost() != nil {
			t.Errorf("a refused SET lost the connection: %v", f.Lost())
		}
		if len(told) != 1 || !strings.Contains(told[0].Error(), "Unknown system variable") {
			t.Errorf("OnExplainFormat was told %v, want the refusal once", told)
		}
	})
	t.Run("the connection breaks under the SET", func(t *testing.T) {
		f, src := newExplainForwarder(t, "9.7.4")
		src.drop = true
		var connect []error
		f.OnConnect = func(err error) { connect = append(connect, err) }
		_, err := f.Forward(ctx, "DO 1", &BufferSink{})
		if err == nil {
			t.Fatal("a statement was answered on a connection that broke while it was opened")
		}
		if len(connect) != 1 || connect[0] == nil || !strings.Contains(connect[0].Error(), "plan format") {
			t.Errorf("OnConnect was told %v, want the one failure, naming the plan format", connect)
		}
		for _, q := range src.statements() {
			if q == "DO 1" {
				t.Error("the client's statement reached a connection that did not open")
			}
		}
	})
}

// The version 2 layout is named as such, so the reason a statement stayed on
// MySQL says what to look at; any other JSON keeps the old message.
func TestParsePlan_version2Layout(t *testing.T) {
	_, err := ParsePlan([]byte(`{"query": "select 1", "query_plan": {"operation": "Covering index scan on user using PRIMARY"}}`))
	if err == nil || !strings.Contains(err.Error(), "explain_json_format_version = 2") {
		t.Errorf("version 2 layout: err = %v, want it named", err)
	}
	_, err = ParsePlan([]byte(`{"something": {}}`))
	if err == nil || err.Error() != "explain output has no query_block" {
		t.Errorf("other JSON: err = %v, want the old message", err)
	}
}
