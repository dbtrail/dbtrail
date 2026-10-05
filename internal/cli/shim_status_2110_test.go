package cli

import (
	"context"
	"net"
	"testing"

	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/testutil/mysqlwire"
)

// The standalone shim's handshake and the OK that ends authentication
// announce autocommit, and so does every OK after (#2110): the shim has no
// transactions, and a driver that reads status 0 takes it for "autocommit is
// off".
func TestShimAnnouncesAutocommit(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	srv, err := shim.NewMySQLServer("")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := shim.NewTenantAuth(map[string]string{"u": "p"})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go handleConn(context.Background(), c, nil, srv, auth, shim.Config{}, nil, nil)
		}
	}()
	c, err := mysqlwire.Dial(ln.Addr().String(), "u", "p", "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	const auto = mysqlwire.StatusAutocommit
	if c.Handshake.Status != auto {
		t.Errorf("handshake status 0x%04x, want 0x%04x (autocommit)", c.Handshake.Status, auto)
	}
	if c.AuthStatus != auto {
		t.Errorf("status of the OK that ends authentication 0x%04x, want 0x%04x", c.AuthStatus, auto)
	}
	for _, q := range []string{"SET autocommit=0", "SELECT @@autocommit"} {
		rep, err := c.Exec(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if rep.Status != auto {
			t.Errorf("%s: status 0x%04x, want 0x%04x", q, rep.Status, auto)
		}
	}
	rep, err := c.Ping()
	if err != nil {
		t.Fatal(err)
	}
	if rep.Status != auto {
		t.Errorf("PING status 0x%04x, want 0x%04x", rep.Status, auto)
	}
}
