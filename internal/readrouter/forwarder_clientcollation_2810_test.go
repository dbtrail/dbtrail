package readrouter

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/config"
)

// clientCollationAsk is what the forwarder asks the source for the names of
// the collation the client's handshake gave by number (#2810).
func clientCollationAsk(id uint16) string {
	return "SELECT CHARACTER_SET_NAME, COLLATION_NAME FROM information_schema.COLLATIONS WHERE ID = " + strconv.Itoa(int(id))
}

// newWrappedCollationForwarder is newCollationForwarder with the source's
// handler wrapped.
func newWrappedCollationForwarder(t *testing.T, version, collation string, wrap func(*collationSource) server.Handler) (*Forwarder, *collationSource) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	inner := &collationSource{collation: collation}
	h := wrap(inner)
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
				inner.mu.Lock()
				inner.conn = c
				inner.mu.Unlock()
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
	return f, inner
}

// clientCollationSource answers the forwarder's question about a collation
// number from names, and records every statement as collationSource does.
type clientCollationSource struct {
	*collationSource
	names map[uint16][2]string
	// clientCS is the session's character_set_client: start, then what a
	// SET NAMES set ("" is utf8mb4). setNamesErr refuses SET NAMES utf8mb4.
	clientCS    string
	setNamesErr error
}

// answerClientCS answers the read-only port's question about the session's
// client character set, and records it.
func answerClientCS(h *collationSource, cs string) (*mysql.Result, error) {
	h.mu.Lock()
	h.seen = append(h.seen, "SELECT @@character_set_client")
	h.mu.Unlock()
	if cs == "" {
		cs = "utf8mb4"
	}
	rs, err := mysql.BuildSimpleTextResultset([]string{"@@character_set_client"}, [][]any{{cs}})
	if err != nil {
		return nil, err
	}
	return &mysql.Result{Resultset: rs}, nil
}

func (h *clientCollationSource) HandleQuery(q string) (*mysql.Result, error) {
	if q == "SELECT @@character_set_client" {
		return answerClientCS(h.collationSource, h.clientCS)
	}
	if q == "SET NAMES utf8mb4" && h.setNamesErr != nil {
		h.mu.Lock()
		h.seen = append(h.seen, q)
		h.mu.Unlock()
		return nil, h.setNamesErr
	}
	if cs, ok := strings.CutPrefix(q, "SET NAMES "); ok && (h.setErr == nil || cs == "utf8mb4") {
		cs, _, _ = strings.Cut(cs, " ")
		h.clientCS = cs
	}
	for id, n := range h.names {
		if q == clientCollationAsk(id) {
			h.mu.Lock()
			h.seen = append(h.seen, q)
			h.mu.Unlock()
			rows := [][]any{{n[0], n[1]}}
			if n[0] == "" {
				rows = nil
			}
			rs, err := mysql.BuildSimpleTextResultset([]string{"CHARACTER_SET_NAME", "COLLATION_NAME"}, rows)
			if err != nil {
				return nil, err
			}
			return &mysql.Result{Resultset: rs}, nil
		}
	}
	if strings.HasPrefix(q, "SET NAMES ") && h.setErr != nil {
		h.mu.Lock()
		h.seen = append(h.seen, q)
		h.mu.Unlock()
		return nil, h.setErr
	}
	if strings.HasPrefix(q, "SELECT CHARACTER_SET_NAME") {
		// A number the test gave no names for: no such collation.
		h.mu.Lock()
		h.seen = append(h.seen, q)
		h.mu.Unlock()
		rs, err := mysql.BuildSimpleTextResultset([]string{"CHARACTER_SET_NAME", "COLLATION_NAME"}, nil)
		if err != nil {
			return nil, err
		}
		return &mysql.Result{Resultset: rs}, nil
	}
	return h.collationSource.HandleQuery(q)
}

// #2810: a client's connection forwards under the collation and character
// set its handshake asked the port for. The port's own handshake to the
// source asks for utf8mb4_0900_ai_ci, so a client that asked for that is sent
// nothing more, on MySQL and on MariaDB alike (the source reads the number
// as it would from the client: a MariaDB 10.11 falls back as it does for the
// client itself). Any other number is looked up on the source, which knows
// what its numbers mean, and set with SET NAMES before anything of the
// client's. A port's own connection (no client collation) is as before.
func TestForwarder_clientCollation_2810(t *testing.T) {
	ctx := context.Background()
	names := map[uint16][2]string{
		45:  {"utf8mb4", "utf8mb4_general_ci"},
		8:   {"latin1", "latin1_swedish_ci"},
		63:  {"binary", "binary"},
		224: {"utf8mb4", "utf8mb4_unicode_ci"},
		46:  {"utf8mb4", "utf8mb4_bin"},
	}
	const ask, settle = "SELECT @@collation_connection", "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"
	cases := []struct {
		name, version, sessionCollation string
		client                          uint16
		want                            []string
	}{
		{"the driver's default on MySQL", "8.0.46", "utf8mb4_0900_ai_ci", 45, []string{clientCollationAsk(45), "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci"}},
		{"latin1 on MySQL", "8.4.9", "utf8mb4_0900_ai_ci", 8, []string{clientCollationAsk(8), "SET NAMES latin1 COLLATE latin1_swedish_ci"}},
		{"binary on MySQL", "8.0.46", "utf8mb4_0900_ai_ci", 63, []string{clientCollationAsk(63), "SET NAMES binary"}},
		{"utf8mb4_bin", "8.0.46", "utf8mb4_0900_ai_ci", 46, []string{clientCollationAsk(46), "SET NAMES utf8mb4 COLLATE utf8mb4_bin"}},
		{"unicode_ci on MySQL", "8.0.46", "utf8mb4_0900_ai_ci", 224, []string{clientCollationAsk(224), "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"}},
		{"what the port asks for itself, on MySQL", "8.0.46", "utf8mb4_0900_ai_ci", 255, nil},
		{"what the port asks for itself, on a MariaDB that falls back", "10.11.9-MariaDB", "utf8mb4_general_ci", 255, nil},
		{"the driver's default on MariaDB 10.11: no settling", "10.11.9-MariaDB", "utf8mb4_general_ci", 45, []string{clientCollationAsk(45), "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci"}},
		{"the port's own connection on MariaDB 10.11: settled as before", "10.11.9-MariaDB", "utf8mb4_general_ci", 0, []string{ask, settle}},
		{"the port's own connection on MySQL: nothing", "8.0.46", "utf8mb4_0900_ai_ci", 0, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var src *clientCollationSource
			f, _ := newWrappedCollationForwarder(t, tc.version, tc.sessionCollation, func(inner *collationSource) server.Handler {
				src = &clientCollationSource{collationSource: inner, names: names}
				return src
			})
			f.ClientCollation = tc.client
			f.OnCollation = func(collation string, err error) {
				t.Errorf("OnCollation(%q, %v) on a source that refused nothing", collation, err)
			}
			for _, stmt := range []string{"DO 1", "DO 2"} {
				if _, err := f.Forward(ctx, stmt, &BufferSink{}); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}
			want := append(append([]string(nil), tc.want...), "DO 1", "DO 2")
			if got := src.statements(); !reflect.DeepEqual(got, want) {
				t.Errorf("the source saw %q, want %q", got, want)
			}
		})
	}
}

// #2810: what cannot be set leaves the session as the source gave it, says
// so once, and keeps the connection: MySQL still answers every statement.
func TestForwarder_clientCollationRefused_2810(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name  string
		names map[uint16][2]string
		set   error
		want  []string
		said  string
	}{
		{"a number the source has no collation for", map[uint16][2]string{}, nil,
			[]string{clientCollationAsk(200)}, "has no collation numbered 200"},
		{"names that are not a name", map[uint16][2]string{200: {"utf8mb4", "x; DROP TABLE t"}}, nil,
			[]string{clientCollationAsk(200)}, "not a collation name"},
		{"the SET NAMES refused", map[uint16][2]string{200: {"ucs2", "ucs2_general_ci"}},
			mysql.NewError(mysql.ER_WRONG_VALUE_FOR_VAR, "Variable 'character_set_client' can't be set to the value of 'ucs2'"),
			[]string{clientCollationAsk(200), "SET NAMES ucs2 COLLATE ucs2_general_ci"}, "can't be set"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var src *clientCollationSource
			f, _ := newWrappedCollationForwarder(t, "8.0.46", "utf8mb4_0900_ai_ci", func(inner *collationSource) server.Handler {
				inner.setErr = tc.set
				src = &clientCollationSource{collationSource: inner, names: tc.names}
				return src
			})
			f.ClientCollation = 200
			var said []string
			f.OnCollation = func(collation string, err error) { said = append(said, err.Error()) }
			for _, stmt := range []string{"DO 1", "DO 2"} {
				if _, err := f.Forward(ctx, stmt, &BufferSink{}); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}
			if f.Lost() != nil {
				t.Error("the connection was lost")
			}
			want := append(append([]string(nil), tc.want...), "DO 1", "DO 2")
			if got := src.statements(); !reflect.DeepEqual(got, want) {
				t.Errorf("the source saw %q, want %q", got, want)
			}
			if len(said) != 1 || !strings.Contains(said[0], tc.said) {
				t.Errorf("OnCollation was told %q, want once, with %q", said, tc.said)
			}
		})
	}
	t.Run("the connection breaks during the question", func(t *testing.T) {
		f, _ := newWrappedCollationForwarder(t, "8.0.46", "utf8mb4_0900_ai_ci", func(inner *collationSource) server.Handler {
			return &droppingSource{&clientCollationSource{collationSource: inner, names: map[uint16][2]string{45: {"utf8mb4", "utf8mb4_general_ci"}}}}
		})
		f.ClientCollation = 45
		if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); err == nil {
			t.Fatal("a statement was answered on a connection that broke while it was opened")
		}
	})
}

// droppingSource closes the connection under the forwarder's question.
type droppingSource struct{ *clientCollationSource }

func (h *droppingSource) HandleQuery(q string) (*mysql.Result, error) {
	if strings.HasPrefix(q, "SELECT CHARACTER_SET_NAME") {
		h.mu.Lock()
		c := h.conn
		h.mu.Unlock()
		c.Close()
		return nil, errors.New("gone")
	}
	return h.clientCollationSource.HandleQuery(q)
}

// #2810: a read-only port gives a session no character set outside
// safeCharsets, the handshake's or the global one after a reset: under gbk,
// sjis or big5 a second byte 0x5C is part of a letter to the source and a
// backslash to the screen, which would read another statement than the one
// the source runs. The session keeps utf8mb4 and OnCollation says why.
func TestForwarder_clientCollationReadOnly_2810(t *testing.T) {
	ctx := context.Background()
	names := map[uint16][2]string{
		28: {"gbk", "gbk_chinese_ci"}, 13: {"sjis", "sjis_japanese_ci"}, 1: {"big5", "big5_chinese_ci"},
		95: {"cp932", "cp932_japanese_ci"}, 8: {"latin1", "latin1_swedish_ci"}, 45: {"utf8mb4", "utf8mb4_general_ci"},
	}
	for id, n := range names {
		safe := n[0] == "latin1" || n[0] == "utf8mb4"
		for _, readOnly := range []bool{true, false} {
			var src *clientCollationSource
			f, _ := newWrappedCollationForwarder(t, "8.0.46", "utf8mb4_0900_ai_ci", func(inner *collationSource) server.Handler {
				src = &clientCollationSource{collationSource: inner, names: names}
				return src
			})
			f.ClientCollation, f.SafeCharsetsOnly = id, readOnly
			var said []string
			f.OnCollation = func(collation string, err error) { said = append(said, collation+": "+err.Error()) }
			if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); err != nil {
				t.Fatal(err)
			}
			set := "SET NAMES " + n[0] + " COLLATE " + n[1]
			check := []string{}
			if readOnly {
				check = []string{"SELECT @@character_set_client"}
			}
			want := append([]string{clientCollationAsk(id), set}, append(check, "DO 1")...)
			if readOnly && !safe {
				want = []string{clientCollationAsk(id), "SELECT @@character_set_client", "DO 1"}
				if len(said) != 1 || !strings.Contains(said[0], "read-only") || !strings.Contains(said[0], n[0]) {
					t.Errorf("%s, read-only: OnCollation was told %q, want once, naming the character set", n[0], said)
				}
			} else if len(said) != 0 {
				t.Errorf("%s, read-only %v: OnCollation was told %q", n[0], readOnly, said)
			}
			if got := src.statements(); !reflect.DeepEqual(got, want) {
				t.Errorf("%s, read-only %v: the source saw %q, want %q", n[0], readOnly, got, want)
			}
		}
	}
}

// globalSource answers the global character sets with the given values.
type globalSource struct {
	*collationSource
	client, results, collation any
}

func (h *globalSource) HandleQuery(q string) (*mysql.Result, error) {
	if q == "SELECT @@character_set_client" {
		return answerClientCS(h.collationSource, "")
	}
	if q == "SELECT @@GLOBAL.character_set_client, @@GLOBAL.character_set_results, @@GLOBAL.collation_connection" {
		h.mu.Lock()
		h.seen = append(h.seen, q)
		h.mu.Unlock()
		rs, err := mysql.BuildSimpleTextResultset([]string{"a", "b", "c"}, [][]any{{h.client, h.results, h.collation}})
		if err != nil {
			return nil, err
		}
		return &mysql.Result{Resultset: rs}, nil
	}
	return h.collationSource.HandleQuery(q)
}

// #2810: the connection that replaces one the client reset takes the
// source's global character sets and collation, as the source's own reset
// leaves the session; a client collation it had is not used again.
func TestForwarder_globalSessionAfterReset_2810(t *testing.T) {
	ctx := context.Background()
	const ask = "SELECT @@GLOBAL.character_set_client, @@GLOBAL.character_set_results, @@GLOBAL.collation_connection"
	cases := []struct {
		name                       string
		client, results, collation any
		readOnly                   bool
		want                       []string
		said                       string
	}{
		{"MySQL's defaults", "utf8mb4", "utf8mb4", "utf8mb4_0900_ai_ci", false,
			[]string{ask, "SET SESSION character_set_client = utf8mb4, character_set_results = utf8mb4, collation_connection = utf8mb4_0900_ai_ci"}, ""},
		{"results NULL", "latin1", nil, "latin1_swedish_ci", false,
			[]string{ask, "SET SESSION character_set_client = latin1, character_set_results = NULL, collation_connection = latin1_swedish_ci"}, ""},
		{"gbk on a read-only port", "gbk", "gbk", "gbk_chinese_ci", true, []string{ask}, "read-only"},
		{"gbk on a port that is not read-only", "gbk", "gbk", "gbk_chinese_ci", false,
			[]string{ask, "SET SESSION character_set_client = gbk, character_set_results = gbk, collation_connection = gbk_chinese_ci"}, ""},
		{"a name that is not one", "utf8mb4; DROP TABLE t", "utf8mb4", "utf8mb4_0900_ai_ci", false, []string{ask}, "not a character set"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var src *globalSource
			f, _ := newWrappedCollationForwarder(t, "8.0.46", "utf8mb4_0900_ai_ci", func(inner *collationSource) server.Handler {
				src = &globalSource{collationSource: inner, client: tc.client, results: tc.results, collation: tc.collation}
				return src
			})
			f.GlobalSession, f.ClientCollation, f.SafeCharsetsOnly = true, 8, tc.readOnly
			var said []string
			f.OnCollation = func(collation string, err error) { said = append(said, err.Error()) }
			if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); err != nil {
				t.Fatal(err)
			}
			want := append([]string(nil), tc.want...)
			if tc.readOnly {
				want = append(want, "SELECT @@character_set_client")
			}
			want = append(want, "DO 1")
			if got := src.statements(); !reflect.DeepEqual(got, want) {
				t.Errorf("the source saw %q, want %q", got, want)
			}
			switch {
			case tc.said == "" && len(said) != 0:
				t.Errorf("OnCollation was told %q", said)
			case tc.said != "" && (len(said) != 1 || !strings.Contains(said[0], tc.said)):
				t.Errorf("OnCollation was told %q, want once, with %q", said, tc.said)
			}
		})
	}
}

// #2810: on a read-only port the source is asked, last, which character set
// it reads the client's bytes in, however the session was settled: a server
// that ignores the login's (skip-character-set-client-handshake) or a
// MariaDB that falls back to its global one can leave gbk there unasked.
// It is replaced with utf8mb4 and OnCollation is told; a source that will
// not take utf8mb4 opens no connection. A port that is not read-only does
// not ask.
func TestForwarder_readOnlyChecksTheClientCharset_2810(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name          string
		client        uint16
		readOnly      bool
		sessionCS     string
		setNamesErr   error
		want          []string
		said, opens   bool
	}{
		{"read-only, the source left gbk", 255, true, "gbk", nil, []string{"SELECT @@character_set_client", "SET NAMES utf8mb4", "DO 1"}, true, true},
		{"read-only, a port's own connection, the source left sjis", 0, true, "sjis", nil, []string{"SELECT @@character_set_client", "SET NAMES utf8mb4", "DO 1"}, true, true},
		{"read-only, utf8mb4", 255, true, "utf8mb4", nil, []string{"SELECT @@character_set_client", "DO 1"}, false, true},
		{"read-only, the source refuses utf8mb4", 255, true, "gbk", mysql.NewError(mysql.ER_UNKNOWN_CHARACTER_SET, "no"), nil, false, false},
		{"not read-only, gbk left", 255, false, "gbk", nil, []string{"DO 1"}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var src *clientCollationSource
			f, _ := newWrappedCollationForwarder(t, "8.0.46", "utf8mb4_0900_ai_ci", func(inner *collationSource) server.Handler {
				src = &clientCollationSource{collationSource: inner, names: map[uint16][2]string{}, clientCS: tc.sessionCS, setNamesErr: tc.setNamesErr}
				return src
			})
			f.ClientCollation, f.SafeCharsetsOnly = tc.client, tc.readOnly
			var said []string
			f.OnCollation = func(collation string, err error) { said = append(said, collation+": "+err.Error()) }
			_, err := f.Forward(ctx, "DO 1", &BufferSink{})
			if tc.opens != (err == nil) {
				t.Fatalf("Forward: %v, want it answered: %v", err, tc.opens)
			}
			if !tc.opens {
				return
			}
			if got := src.statements(); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("the source saw %q, want %q", got, tc.want)
			}
			if tc.said != (len(said) == 1 && strings.Contains(said[0], "read-only")) || len(said) > 1 {
				t.Errorf("OnCollation was told %q, want told: %v", said, tc.said)
			}
		})
	}
}
