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

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/config"
)

const trackSET = "SET SESSION session_track_system_variables = '" + SessionTrackedVariables + "'"

// trackSource is a source that records every statement it is sent and marks
// the answers a test asks it to with SERVER_SESSION_STATE_CHANGED, the way a
// server that tracks the session does.
type trackSource struct {
	server.EmptyHandler
	mu   sync.Mutex
	seen []string
	// setErr answers the SET of the tracked list; drop closes the connection
	// under it instead.
	setErr error
	drop   bool
	// unmarked: the SET is answered without the mark a server that tracks
	// puts on it.
	unmarked bool
	conn     net.Conn
	mc       *server.Conn
}

func (h *trackSource) statements() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.seen...)
}

func (h *trackSource) HandleQuery(q string) (*mysql.Result, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen = append(h.seen, q)
	// The status a resultset ends with is the connection's; an OK's is the
	// result's own.
	h.mc.UnsetStatus(mysql.SERVER_SESSION_STATE_CHANGED)
	marked := strings.Contains(q, "marked")
	switch {
	case strings.HasPrefix(q, "SET SESSION session_track_system_variables"):
		if h.drop {
			h.conn.Close()
			return nil, errors.New("gone")
		}
		if h.setErr != nil {
			return nil, h.setErr
		}
		marked = !h.unmarked
	case strings.Contains(q, "fails"):
		return nil, mysql.NewError(mysql.ER_SIGNAL_EXCEPTION, "raised")
	case strings.HasPrefix(q, "SELECT"), strings.HasPrefix(q, "EXPLAIN"):
		if marked {
			h.mc.SetStatus(mysql.SERVER_SESSION_STATE_CHANGED)
		}
		rs, err := mysql.BuildSimpleTextResultset([]string{"c"}, [][]any{{"1"}})
		if err != nil {
			return nil, err
		}
		return &mysql.Result{Resultset: rs}, nil
	}
	res := mysql.NewResultReserveResultset(0)
	if marked {
		res.Status |= mysql.SERVER_SESSION_STATE_CHANGED
	}
	return res, nil
}

// newTrackForwarder is a Forwarder with TrackSession set over a trackSource
// that greets with the given version. agreed is what the handshake is said to
// have settled on; "" leaves what it really did (the library's server has
// session tracking and not CLIENT_DEPRECATE_EOF).
func newTrackForwarder(t *testing.T, version, agreed string) (*Forwarder, *trackSource) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	h := &trackSource{}
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
				mc, err := server.NewCustomizedConn(c, conf, auth, h)
				if err != nil {
					return
				}
				h.mu.Lock()
				h.conn, h.mc = c, mc
				h.mu.Unlock()
				for mc.HandleCommand() == nil {
				}
			}()
		}
	}()
	f, err := NewForwarder("u:p@tcp("+ln.Addr().String()+")/db?tls=false", config.SSL{Mode: "disabled"}, DefaultPolicy(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	f.TrackSession = true
	if agreed != "" {
		f.capabilities = func(*client.Conn) string { return agreed }
	}
	t.Cleanup(f.Close)
	return f, h
}

const bothAgreed = "CLIENT_PROTOCOL_41|CLIENT_SESSION_TRACK|CLIENT_DEPRECATE_EOF"

// The source is asked to report session changes once, when the connection
// opens and before anything of the client's, and never again: N statements
// of the client are N statements on the source, plus that one.
func TestForwarder_trackSession_oneStatementPerConnection(t *testing.T) {
	ctx := context.Background()
	f, src := newTrackForwarder(t, "8.4.9", bothAgreed)
	f.OnUntracked = func(why string) { t.Errorf("OnUntracked(%q) on a source that tracks", why) }
	if f.SessionTracked() {
		t.Error("tracked before the connection is opened")
	}
	if len(src.statements()) != 0 {
		t.Fatal("the source was sent something before the first statement")
	}
	want := []string{trackSET}
	for i := range 25 {
		stmt := "SELECT " + strings.Repeat("1", i+1)
		if _, err := f.Forward(ctx, stmt, &BufferSink{}); err != nil {
			t.Fatal(err)
		}
		want = append(want, stmt)
	}
	if got := src.statements(); !reflect.DeepEqual(got, want) {
		t.Errorf("the source saw %d statements for the client's 25, want 26 (the one that asks for tracking, then the client's):\n%q", len(got), got)
	}
	if !f.SessionTracked() {
		t.Error("not tracked after the source took the list")
	}
	if f.TakeSessionChanged() {
		t.Error("a change was heard on a session where nothing was marked")
	}
}

// A Forwarder that was not told to track sends nothing and asks the
// handshake for nothing (the one that kills a statement, the one that tests a
// connection).
func TestForwarder_trackSession_onlyWhenAsked(t *testing.T) {
	f, src := newTrackForwarder(t, "8.4.9", bothAgreed)
	f.TrackSession = false
	if _, err := f.Forward(context.Background(), "DO 1", &BufferSink{}); err != nil {
		t.Fatal(err)
	}
	if got := src.statements(); !reflect.DeepEqual(got, []string{"DO 1"}) {
		t.Errorf("the source saw %q", got)
	}
	if f.SessionTracked() {
		t.Error("tracked without being asked")
	}
	f.mu.Lock()
	agreed := f.conn.CapabilityString()
	f.mu.Unlock()
	if strings.Contains(agreed, "CLIENT_SESSION_TRACK") {
		t.Errorf("the handshake settled on %s without TrackSession", agreed)
	}
}

// With TrackSession the handshake asks for session tracking (the library
// does not by itself).
func TestForwarder_trackSession_asksTheHandshake(t *testing.T) {
	f, _ := newTrackForwarder(t, "8.4.9", "")
	if _, err := f.Forward(context.Background(), "DO 1", &BufferSink{}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	agreed := f.conn.CapabilityString()
	f.mu.Unlock()
	if !strings.Contains(agreed, "CLIENT_SESSION_TRACK") {
		t.Errorf("the handshake settled on %s, without CLIENT_SESSION_TRACK", agreed)
	}
}

// On MariaDB the collation is settled first, then tracking is asked for.
func TestForwarder_trackSession_afterTheCollation(t *testing.T) {
	f, src := newTrackForwarder(t, "10.11.9-MariaDB", bothAgreed)
	f.OnCollation = func(string, error) {}
	if _, err := f.Forward(context.Background(), "DO 1", &BufferSink{}); err != nil {
		t.Fatal(err)
	}
	got := src.statements()
	if len(got) < 3 || got[0] != "SELECT @@collation_connection" || got[len(got)-2] != trackSET || got[len(got)-1] != "DO 1" {
		t.Errorf("the source saw %q, want the collation question, then the tracked list, then the client's statement", got)
	}
}

// A source that cannot report changes with a SELECT's answer is not tracked:
// nothing is sent, the connection works, and the caller is told why, once.
func TestForwarder_trackSession_sourceThatCannot(t *testing.T) {
	cases := []struct{ name, agreed, wantWhy string }{
		// The library's own server: session tracking, and resultsets that
		// end in an EOF packet.
		{"what the handshake really settled on", "", "CLIENT_DEPRECATE_EOF"},
		{"no session tracking", "CLIENT_PROTOCOL_41|CLIENT_DEPRECATE_EOF", "CLIENT_SESSION_TRACK"},
		{"neither", "CLIENT_PROTOCOL_41", "CLIENT_SESSION_TRACK"},
		// A flag whose name only contains the one looked for.
		{"a longer name", "CLIENT_SESSION_TRACKER|CLIENT_DEPRECATE_EOF", "CLIENT_SESSION_TRACK"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f, src := newTrackForwarder(t, "8.4.9", tc.agreed)
			var told []string
			f.OnUntracked = func(why string) { told = append(told, why) }
			for range 3 {
				if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); err != nil {
					t.Fatal(err)
				}
			}
			if got := src.statements(); !reflect.DeepEqual(got, []string{"DO 1", "DO 1", "DO 1"}) {
				t.Errorf("the source saw %q, want the client's statements alone", got)
			}
			if len(told) != 1 || !strings.Contains(told[0], tc.wantWhy+" ") {
				t.Errorf("OnUntracked told %q, want once, naming %s", told, tc.wantWhy)
			}
			if f.SessionTracked() {
				t.Error("tracked")
			}
		})
	}
}

// A source that refuses the list keeps its session, and the client its
// connection: MySQL answers as before, untracked.
func TestForwarder_trackSession_sourceThatRefuses(t *testing.T) {
	ctx := context.Background()
	f, src := newTrackForwarder(t, "8.4.9", bothAgreed)
	src.setErr = mysql.NewError(mysql.ER_UNKNOWN_SYSTEM_VARIABLE, "Unknown system variable 'session_track_system_variables'")
	var told []string
	f.OnUntracked = func(why string) { told = append(told, why) }
	var connected []error
	f.OnConnect = func(err error) { connected = append(connected, err) }
	for range 2 {
		if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); err != nil {
			t.Fatalf("a statement on a source that refused to track: %v", err)
		}
	}
	if got := src.statements(); !reflect.DeepEqual(got, []string{trackSET, "DO 1", "DO 1"}) {
		t.Errorf("the source saw %q", got)
	}
	if len(told) != 1 || !strings.Contains(told[0], "Unknown system variable") {
		t.Errorf("OnUntracked told %q, want once, with the source's own words", told)
	}
	if len(connected) != 1 || connected[0] != nil {
		t.Errorf("OnConnect told %v, want one success", connected)
	}
	if f.SessionTracked() {
		t.Error("tracked after the source refused")
	}
}

// A source that takes the list and does not report that change in its answer
// is one whose marks do not reach the port (a proxy that answers the SET
// itself, or drops the flag): not tracked, and said so.
func TestForwarder_trackSession_sourceThatDoesNotMark(t *testing.T) {
	ctx := context.Background()
	f, src := newTrackForwarder(t, "8.4.9", bothAgreed)
	src.unmarked = true
	var told []string
	f.OnUntracked = func(why string) { told = append(told, why) }
	for range 2 {
		if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); err != nil {
			t.Fatal(err)
		}
	}
	if got := src.statements(); !reflect.DeepEqual(got, []string{trackSET, "DO 1", "DO 1"}) {
		t.Errorf("the source saw %q", got)
	}
	if len(told) != 1 || !strings.Contains(told[0], "did not report that change") {
		t.Errorf("OnUntracked told %q, want once", told)
	}
	if f.SessionTracked() {
		t.Error("tracked on a source that marks nothing")
	}
}

// A connection that breaks under the SET did not open.
func TestForwarder_trackSession_connectionLost(t *testing.T) {
	ctx := context.Background()
	f, src := newTrackForwarder(t, "8.4.9", bothAgreed)
	src.drop = true
	var connected []error
	f.OnConnect = func(err error) { connected = append(connected, err) }
	f.OnUntracked = func(why string) { t.Errorf("OnUntracked(%q) for a connection that broke", why) }
	if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); !IsLost(err) {
		t.Fatalf("err = %v, want the lost error", err)
	}
	if len(connected) != 1 || connected[0] == nil {
		t.Errorf("OnConnect told %v, want the failure", connected)
	}
	if f.SessionTracked() {
		t.Error("tracked on a connection that did not open")
	}
	if f.Lost() != nil {
		t.Error("a session is said to be lost where none was opened")
	}
}

// What makes a change heard: the source's mark on an answer (a resultset's
// closing packet, an OK packet, the EXPLAIN of a decision), and any error
// packet. Nothing else, and it is forgotten once taken.
func TestForwarder_heardSessionChanges(t *testing.T) {
	ctx := context.Background()
	f, _ := newTrackForwarder(t, "8.4.9", bothAgreed)
	forward := func(stmt string) func() {
		return func() { _, _ = f.Forward(ctx, stmt, &BufferSink{}) }
	}
	steps := []struct {
		name string
		run  func()
		want bool
	}{
		{"a SELECT that changed nothing", forward("SELECT 1"), false},
		{"a SELECT whose answer is marked", forward("SELECT marked()"), true},
		{"taken: forgotten", func() {}, false},
		{"a SELECT after it", forward("SELECT 1"), false},
		{"an OK that is marked", forward("DO marked()"), true},
		{"an OK that is not", forward("DO 1"), false},
		{"a statement the source answers with an error", forward("SELECT fails()"), true},
		{"the EXPLAIN of a decision, marked", func() { _, _ = f.Decide(ctx, "SELECT marked() FROM t") }, true},
		{"the EXPLAIN of a decision, not marked", func() { _, _ = f.Decide(ctx, "SELECT a FROM t") }, false},
		{"the EXPLAIN of a decision, refused", func() { _, _ = f.Decide(ctx, "SELECT fails() FROM t") }, true},
		{"a PING", func() { _ = f.Ping(ctx) }, false},
	}
	for _, s := range steps {
		s.run()
		if got := f.TakeSessionChanged(); got != s.want {
			t.Errorf("%s: change heard = %v, want %v", s.name, got, s.want)
		}
		if f.TakeSessionChanged() {
			t.Errorf("%s: heard twice", s.name)
		}
	}
	if err := f.Lost(); err != nil {
		t.Errorf("the connection was lost along the way: %v", err)
	}
}

// On a session that is not tracked an error is not a change: nothing else is
// heard there, and the connection behaves as it did before tracking was
// asked for.
func TestForwarder_untrackedSessionHearsNothing(t *testing.T) {
	ctx := context.Background()
	f, src := newTrackForwarder(t, "8.4.9", bothAgreed)
	src.unmarked = true
	if _, err := f.Forward(ctx, "SELECT fails()", &BufferSink{}); err == nil {
		t.Fatal("the source's error did not come back")
	}
	if _, err := f.Decide(ctx, "SELECT fails() FROM t"); err == nil {
		t.Fatal("the EXPLAIN's error did not come back")
	}
	if f.SessionTracked() {
		t.Fatal("tracked: this case tests nothing")
	}
	if f.TakeSessionChanged() {
		t.Error("an error on a session that is not tracked was taken for a change")
	}
}

func TestTracksSession(t *testing.T) {
	for list, want := range map[string]bool{
		SessionTrackedVariables: true,
		// MariaDB 11.4 prints the list sorted, without two of its names.
		"character_set_connection,character_set_results,div_precision_increment,lc_time_names,max_join_size,session_track_system_variables,sql_auto_is_null,sql_mode,sql_select_limit,time_zone": true,
		"*": true,
		"time_zone, Session_Track_System_Variables": true,
		// What a client leaves when it replaces the list.
		"autocommit,character_set_client,character_set_connection,character_set_results,time_zone": false,
		"time_zone,autocommit,transaction_isolation":                                               false,
		"":                                    false,
		"session_track_system_variables_x":    false,
		"xsession_track_system_variables":     false,
		"session_track_state_change,sql_mode": false,
	} {
		if got := TracksSession(list); got != want {
			t.Errorf("TracksSession(%q) = %v, want %v", list, got, want)
		}
	}
}

func TestTrackSessionSQL(t *testing.T) {
	const head = "SET SESSION session_track_system_variables = '"
	cases := []struct{ has, want string }{
		{"", SessionTrackedVariables},
		// The client's are kept, in front; a name is never written twice
		// (MySQL refuses a list with a duplicate), whatever its case.
		{"autocommit,TIME_ZONE, transaction_isolation", "autocommit,TIME_ZONE,transaction_isolation," + strings.TrimPrefix(SessionTrackedVariables, "time_zone,")},
		{SessionTrackedVariables, SessionTrackedVariables},
		{",,", SessionTrackedVariables},
		// Nothing of the source's text can end the string it is put in.
		{"a'b,c\\d,autocommit", "autocommit," + SessionTrackedVariables},
	}
	for _, tc := range cases {
		if got := trackSessionSQL(tc.has); got != head+tc.want+"'" {
			t.Errorf("trackSessionSQL(%q) =\n %s\nwant\n %s", tc.has, got, head+tc.want+"'")
		}
	}
}

// TrackSessionAgain puts the port's settings back in a list a client
// replaced; a source that refuses is untracked from there, and says so.
func TestForwarder_trackSessionAgain(t *testing.T) {
	ctx := context.Background()
	f, src := newTrackForwarder(t, "8.4.9", bothAgreed)
	var told []string
	f.OnUntracked = func(why string) { told = append(told, why) }
	// No connection yet: nothing to ask, and none is opened for it.
	if err := f.TrackSessionAgain(ctx, "time_zone"); err != nil || len(src.statements()) != 0 {
		t.Fatalf("before the connection opened: err %v, the source saw %q", err, src.statements())
	}
	if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); err != nil {
		t.Fatal(err)
	}
	if err := f.TrackSessionAgain(ctx, "autocommit,time_zone"); err != nil {
		t.Fatal(err)
	}
	want := "SET SESSION session_track_system_variables = 'autocommit,time_zone," + strings.TrimPrefix(SessionTrackedVariables, "time_zone,") + "'"
	if got := src.statements(); got[len(got)-1] != want {
		t.Errorf("the source was sent\n %s\nwant\n %s", got[len(got)-1], want)
	}
	if !f.SessionTracked() || len(told) != 0 {
		t.Errorf("tracked = %v, told %q", f.SessionTracked(), told)
	}
	src.mu.Lock()
	src.setErr = mysql.NewError(mysql.ER_UNKNOWN_ERROR, "not now")
	src.mu.Unlock()
	if err := f.TrackSessionAgain(ctx, "time_zone"); err == nil || IsLost(err) {
		t.Fatalf("err = %v, want the source's refusal", err)
	}
	if f.SessionTracked() || len(told) != 1 || !strings.Contains(told[0], "not now") {
		t.Errorf("after a refusal: tracked = %v, told %q", f.SessionTracked(), told)
	}
	// The session goes on.
	if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); err != nil {
		t.Errorf("a statement after the refusal: %v", err)
	}
	// Taken, and not marked: untracked too.
	src.mu.Lock()
	src.setErr, src.unmarked = nil, true
	src.mu.Unlock()
	f.mu.Lock()
	f.tracked = true
	f.mu.Unlock()
	if err := f.TrackSessionAgain(ctx, "time_zone"); err == nil || IsLost(err) {
		t.Fatalf("err = %v, want an error that keeps the session", err)
	}
	if f.SessionTracked() || len(told) != 2 || !strings.Contains(told[1], "did not report that change") {
		t.Errorf("after an answer without the mark: tracked = %v, told %q", f.SessionTracked(), told)
	}
	// A connection that breaks under it is lost.
	src.mu.Lock()
	src.setErr, src.drop = nil, true
	src.mu.Unlock()
	if err := f.TrackSessionAgain(ctx, "time_zone"); !IsLost(err) {
		t.Errorf("err = %v, want the lost error", err)
	}
	// And stays lost: nothing is sent, and the caller is not told all is well.
	sent := len(src.statements())
	if err := f.TrackSessionAgain(ctx, "time_zone"); !IsLost(err) || len(src.statements()) != sent {
		t.Errorf("on a lost connection: err = %v, %d more statement(s) sent", err, len(src.statements())-sent)
	}
}
