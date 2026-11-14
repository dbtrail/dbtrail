package shim

import (
	"errors"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

// fakeChanger is a handler that changes user: "app2" has password "pw2".
type fakeChanger struct {
	server.EmptyHandler
	changed string
	// collation is what ChangeUserCollation was told (#2810).
	mu        sync.Mutex
	collation uint16
}

func (f *fakeChanger) ChangeUserCredential(user string) (server.Credential, bool, any) {
	if user != "app2" {
		return server.Credential{}, false, nil
	}
	return server.Credential{Passwords: []string{"pw2"}, AuthPluginName: mysql.AUTH_NATIVE_PASSWORD}, true, nil
}
func (f *fakeChanger) ChangeUser(user, _ string, _ any) error { f.changed = user; return nil }
func (f *fakeChanger) ResetConnection() error                 { return nil }

// sessionPair is a client connected to a Session over fakeChanger; the
// session's HandleCommand errors arrive on errs.
func sessionPair(t *testing.T) (*client.Conn, *fakeChanger, chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	h := &fakeChanger{}
	errs := make(chan error, 1)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			errs <- err
			return
		}
		sc, err := server.NewDefaultServer().NewConn(nc, "app1", "pw1", h)
		if err != nil {
			errs <- err
			return
		}
		s := NewSession(sc, h)
		for {
			if err := s.HandleCommand(); err != nil {
				errs <- err
				return
			}
			if sc.Conn == nil {
				errs <- nil
				return
			}
		}
	}()
	c, err := client.Connect(ln.Addr().String(), "app1", "pw1", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, h, errs
}

func changeUserPkt(user string) []byte {
	pkt := []byte{0, 0, 0, 0, mysql.COM_CHANGE_USER}
	pkt = append(pkt, user...)
	pkt = append(pkt, 0, 1, 'x', 0, 0x21, 0x00)
	pkt = append(pkt, mysql.AUTH_NATIVE_PASSWORD...)
	return append(pkt, 0)
}

// The client reads the same "Access denied" for an unknown user and for a
// wrong password; the error the session returns (the operator's log) says
// which it was.
func TestChangeUser_logSaysWhy(t *testing.T) {
	for _, tc := range []struct{ user, password, why string }{
		{"app2", "wrong", "(wrong password)"},
		{"nobody", "pw2", "(no password is accepted for this user"},
	} {
		c, h, errs := sessionPair(t)
		c.ResetSequence()
		if err := c.WritePacket(changeUserPkt(tc.user)); err != nil {
			t.Fatal(err)
		}
		sw, err := c.ReadPacket()
		if err != nil || sw[0] != mysql.EOF_HEADER {
			t.Fatalf("%s: no auth switch: %x %v", tc.user, sw, err)
		}
		salt := sw[1+len(mysql.AUTH_NATIVE_PASSWORD)+1 : len(sw)-1]
		if err := c.WritePacket(append([]byte{0, 0, 0, 0}, mysql.CalcNativePassword(salt, []byte(tc.password))...)); err != nil {
			t.Fatal(err)
		}
		resp, err := c.ReadPacket()
		if err != nil || resp[0] != mysql.ERR_HEADER || !strings.Contains(string(resp), "Access denied for user '"+tc.user+"'") {
			t.Fatalf("%s: client read %q %v", tc.user, resp, err)
		}
		if strings.Contains(string(resp), "wrong password") || strings.Contains(string(resp), "no password") {
			t.Fatalf("%s: the client was told why: %q", tc.user, resp)
		}
		got := <-errs
		if !errors.Is(got, ErrChangeUserRefused) || !strings.Contains(got.Error(), tc.why) {
			t.Fatalf("%s: session returned %v, want ErrChangeUserRefused saying %q", tc.user, got, tc.why)
		}
		if h.changed != "" {
			t.Fatalf("%s: bound to %q", tc.user, h.changed)
		}
	}
}

// A client that goes away while the port asks for its password is a lost
// connection, not a refused change of user.
func TestChangeUser_lostConnectionIsNotARefusal(t *testing.T) {
	c, _, errs := sessionPair(t)
	c.ResetSequence()
	if err := c.WritePacket(changeUserPkt("app2")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReadPacket(); err != nil {
		t.Fatal(err)
	}
	c.Close()
	got := <-errs
	if got == nil || errors.Is(got, ErrChangeUserRefused) {
		t.Fatalf("session returned %v, want a lost connection", got)
	}
}
