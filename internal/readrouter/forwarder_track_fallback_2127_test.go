package readrouter

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
)

// unreadableOK is an OK packet whose session-state data names a tracker type
// the client library does not know (9): what a source, or a proxy in front of
// it, could write once it has agreed to CLIENT_SESSION_TRACK. The library
// returns an error for it, which is not an error packet from the source.
var unreadableOK = []byte{0x00, 0x00, 0x00, 0x02, 0x40, 0x00, 0x00, 0x00, 0x03, 0x09, 0x01, 0x00}

// spoiler stands between a Forwarder and a trackSource and replaces chosen
// answers of the source with unreadableOK, but only on a connection whose
// handshake asked for session tracking: the source it plays works for a
// client that does not ask.
type spoiler struct {
	mu sync.Mutex
	// spoil says whether the OK that answers query is replaced; query is ""
	// for the OK that ends the login.
	spoil func(query string) bool
	// garble says whether the answer to query is replaced with a packet no
	// client can read, whatever the connection asked for.
	garble func(query string) bool
	// refuseFrom, when not zero, closes every connection from that one on
	// (counted from 1) as soon as it is made.
	refuseFrom int
	// conns counts the connections made; asked how many of them asked the
	// handshake for session tracking.
	conns, asked int
}

func (s *spoiler) counts() (conns, asked int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns, s.asked
}

func readPacket(r io.Reader) (head [4]byte, payload []byte, err error) {
	if _, err = io.ReadFull(r, head[:]); err != nil {
		return head, nil, err
	}
	payload = make([]byte, int(head[0])|int(head[1])<<8|int(head[2])<<16)
	_, err = io.ReadFull(r, payload)
	return head, payload, err
}

func writePacket(w io.Writer, seq byte, payload []byte) error {
	n := len(payload)
	_, err := w.Write(append([]byte{byte(n), byte(n >> 8), byte(n >> 16), seq}, payload...))
	return err
}

// start listens and relays each connection to upstream.
func (s *spoiler) start(t *testing.T, upstream string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns++
			refuse := s.refuseFrom != 0 && s.conns >= s.refuseFrom
			s.mu.Unlock()
			if refuse {
				c.Close()
				continue
			}
			go s.relay(c, upstream)
		}
	}()
	return ln.Addr().String()
}

func (s *spoiler) relay(c net.Conn, upstream string) {
	defer c.Close()
	up, err := net.Dial("tcp", upstream)
	if err != nil {
		return
	}
	defer up.Close()
	var (
		mu       sync.Mutex
		asked    bool
		loggedIn bool
		query    string
	)
	// The client's packets: its handshake answer says what it asked for, and
	// each command after it what the next answer is to.
	go func() {
		defer up.Close()
		first := true
		for {
			head, payload, err := readPacket(c)
			if err != nil {
				return
			}
			mu.Lock()
			switch {
			case first && len(payload) >= 4:
				first = false
				asked = binary.LittleEndian.Uint32(payload)&mysql.CLIENT_SESSION_TRACK != 0
				if asked {
					s.mu.Lock()
					s.asked++
					s.mu.Unlock()
				}
			case head[3] == 0 && len(payload) > 0:
				loggedIn, query = true, ""
				if payload[0] == mysql.COM_QUERY {
					query = string(payload[1:])
				}
			}
			mu.Unlock()
			if writePacket(up, head[3], payload) != nil {
				return
			}
		}
	}()
	greeting := true
	for {
		head, payload, err := readPacket(up)
		if err != nil {
			return
		}
		mu.Lock()
		spoil := !greeting && asked && len(payload) > 0 && payload[0] == mysql.OK_HEADER && s.spoil != nil &&
			(loggedIn && query != "" && s.spoil(query) || !loggedIn && s.spoil(""))
		garble := !greeting && loggedIn && query != "" && s.garble != nil && s.garble(query)
		mu.Unlock()
		greeting = false
		switch {
		case garble:
			payload = []byte{mysql.LocalInFile_HEADER}
		case spoil:
			payload = unreadableOK
		}
		if writePacket(c, head[3], payload) != nil {
			return
		}
	}
}

// trackedThroughSpoiler is a tracking Forwarder whose source spoils the
// answers spoil picks.
func trackedThroughSpoiler(t *testing.T, sp *spoiler) (*Forwarder, *trackSource, *[]string) {
	t.Helper()
	addr, src := startTrackSource(t, "8.4.9")
	f := trackForwarderAt(t, sp.start(t, addr), bothAgreed)
	var unusable []string
	f.OnUntracked = func(why string, bad bool) {
		if !bad {
			t.Errorf("OnUntracked(%q) without saying tracking is unusable", why)
		}
		unusable = append(unusable, why)
	}
	return f, src, &unusable
}

// A source that agrees to session tracking and then answers the login, or
// the statement that asks for the list, with data the client library cannot
// read worked before tracking was asked for. It still does: the connection is
// opened once more without asking, the caller is told not to ask this source
// again, and the client's statements flow.
func TestForwarder_trackSession_fallsBackWhenAnswersCannotBeRead(t *testing.T) {
	isTrackSET := func(q string) bool { return strings.HasPrefix(q, "SET SESSION session_track_system_variables") }
	cases := []struct {
		name  string
		spoil func(query string) bool
		// wantSeen is what the source was sent over both connections.
		wantSeen []string
	}{
		{"the answer to the login", func(q string) bool { return q == "" }, []string{"DO 1", "SELECT 2"}},
		{"the answer to the list", isTrackSET, []string{trackSET, "DO 1", "SELECT 2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			sp := &spoiler{spoil: tc.spoil}
			f, src, unusable := trackedThroughSpoiler(t, sp)
			var connected []error
			f.OnConnect = func(err error) { connected = append(connected, err) }
			for _, stmt := range []string{"DO 1", "SELECT 2"} {
				if _, err := f.Forward(ctx, stmt, &BufferSink{}); err != nil {
					t.Fatalf("%s on a source that works without session tracking: %v", stmt, err)
				}
			}
			if conns, asked := sp.counts(); conns != 2 || asked != 1 {
				t.Errorf("%d connection(s), %d asking for session tracking; want 2 and 1 (one more attempt, without asking)", conns, asked)
			}
			if got := src.statements(); !reflect.DeepEqual(got, tc.wantSeen) {
				t.Errorf("the source saw %q, want %q", got, tc.wantSeen)
			}
			if len(*unusable) != 1 || !strings.Contains((*unusable)[0], "unknown change type") {
				t.Errorf("told %q, want once, with what could not be read", *unusable)
			}
			if len(connected) != 1 || connected[0] != nil {
				t.Errorf("OnConnect told %v, want one success", connected)
			}
			if f.SessionTracked() {
				t.Error("tracked on the connection opened without asking")
			}
			if f.TakeSessionChanged() {
				t.Error("a change was heard on a session that is not tracked")
			}
		})
	}
}

// The second attempt is made once. When it fails too, session tracking was
// not what broke the connection: its error is the one reported, the source is
// not called unusable for tracking, and the connection stays lost.
func TestForwarder_trackSession_oneMoreAttemptOnly(t *testing.T) {
	ctx := context.Background()
	sp := &spoiler{spoil: func(q string) bool { return q == "" }, refuseFrom: 2}
	f, _, unusable := trackedThroughSpoiler(t, sp)
	var connected []error
	f.OnConnect = func(err error) { connected = append(connected, err) }
	for range 3 {
		if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); !IsLost(err) {
			t.Fatalf("err = %v, want the lost error", err)
		}
	}
	if conns, _ := sp.counts(); conns != 2 {
		t.Errorf("%d connections, want 2", conns)
	}
	if len(*unusable) != 0 {
		t.Errorf("told %q for a source the second attempt could not reach either", *unusable)
	}
	if len(connected) != 1 || connected[0] == nil || strings.Contains(connected[0].Error(), "unknown change type") {
		t.Errorf("OnConnect told %v, want the second attempt's error, once", connected)
	}
}

// What is not a reason to try again: an error packet from the source (a
// login it refuses), and a source that cannot be reached or hangs up.
func TestForwarder_trackSession_noSecondAttempt(t *testing.T) {
	ctx := context.Background()
	t.Run("the source refuses the login", func(t *testing.T) {
		addr, _ := startTrackSource(t, "8.4.9")
		sp := &spoiler{}
		f, err := NewForwarder("u:wrong@tcp("+sp.start(t, addr)+")/db?tls=false", config.SSL{Mode: "disabled"}, DefaultPolicy(), 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(f.Close)
		f.TrackSession = true
		f.OnUntracked = func(why string, _ bool) { t.Errorf("OnUntracked(%q)", why) }
		var connected []error
		f.OnConnect = func(err error) { connected = append(connected, err) }
		if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); !IsLost(err) {
			t.Fatalf("err = %v", err)
		}
		if _, refused := AccountRefused(connected[0]); !refused || len(connected) != 1 {
			t.Errorf("OnConnect told %v, want the source's refusal, once", connected)
		}
		if conns, _ := sp.counts(); conns != 1 {
			t.Errorf("%d connections for a login the source refused, want 1", conns)
		}
	})
	t.Run("the source hangs up", func(t *testing.T) {
		addr, _ := startTrackSource(t, "8.4.9")
		sp := &spoiler{refuseFrom: 1}
		f := trackForwarderAt(t, sp.start(t, addr), bothAgreed)
		f.OnUntracked = func(why string, _ bool) { t.Errorf("OnUntracked(%q)", why) }
		if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); !IsLost(err) {
			t.Fatalf("err = %v", err)
		}
		if conns, _ := sp.counts(); conns != 1 {
			t.Errorf("%d connections to a source that hangs up, want 1", conns)
		}
	})
}

// An answer that cannot be read LATER in the session cannot be put right in
// place: the connection is lost, as any connection whose packets cannot be
// read is. The caller is told, once, so that the client's next connection
// does not ask this source for session tracking.
func TestForwarder_trackSession_unreadableAnswerMidSession(t *testing.T) {
	ctx := context.Background()
	sp := &spoiler{spoil: func(q string) bool { return strings.Contains(q, "poison") }}
	f, _, unusable := trackedThroughSpoiler(t, sp)
	if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); err != nil {
		t.Fatal(err)
	}
	if !f.SessionTracked() || len(*unusable) != 0 {
		t.Fatalf("before: tracked = %v, told %q", f.SessionTracked(), *unusable)
	}
	for range 2 {
		if _, err := f.Forward(ctx, "DO poison", &BufferSink{}); !IsLost(err) {
			t.Fatalf("err = %v, want the lost error", err)
		}
	}
	if len(*unusable) != 1 || !strings.Contains((*unusable)[0], "unknown change type") {
		t.Errorf("told %q, want once", *unusable)
	}
	if conns, _ := sp.counts(); conns != 1 {
		t.Errorf("%d connections: a lost session is not reopened behind the client", conns)
	}
	if f.Lost() == nil {
		t.Error("the session is not reported lost")
	}
}

// A connection that was opened again WITHOUT asking and is lost later to a
// packet nobody could read says nothing more about session tracking: it did
// not ask for it.
func TestForwarder_trackSession_lossOnAConnectionThatDidNotAsk(t *testing.T) {
	ctx := context.Background()
	sp := &spoiler{spoil: func(q string) bool { return q == "" }, garble: func(q string) bool { return strings.Contains(q, "garble") }}
	f, _, unusable := trackedThroughSpoiler(t, sp)
	if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); err != nil {
		t.Fatal(err)
	}
	if len(*unusable) != 1 {
		t.Fatalf("told %q after the connection was opened again, want once", *unusable)
	}
	if _, err := f.Forward(ctx, "DO garble", &BufferSink{}); !IsLost(err) {
		t.Fatalf("err = %v, want the lost error", err)
	}
	if len(*unusable) != 1 {
		t.Errorf("told %q: the loss of a connection that did not ask was blamed on session tracking", *unusable)
	}
}

func TestProtocolError(t *testing.T) {
	f, err := NewForwarder("u:p@tcp(127.0.0.1:1)/db?tls=false", config.SSL{Mode: "disabled"}, DefaultPolicy(), 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _, dialErr := f.open(context.Background(), true)
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"nil":                          {nil, false},
		"an error packet":              {mysql.NewError(mysql.ER_ACCESS_DENIED_ERROR, "no"), false},
		"a source nobody answers at":   {dialErr, false},
		"an end of file":               {io.EOF, false},
		"a closed socket":              {net.ErrClosed, false},
		"the library's bad connection": {mysql.ErrBadConn, false},
		"a client that left":           {context.Canceled, false},
		"a packet that makes no sense": {mysql.ErrMalformPacket, true},
	} {
		if got := protocolError(tc.err); got != tc.want {
			t.Errorf("protocolError(%s: %v) = %v, want %v", name, tc.err, got, tc.want)
		}
	}
	if dialErr == nil {
		t.Fatal("port 1 answered")
	}
}
