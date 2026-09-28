// Package fakemysql is a test-only MySQL server that answers every statement
// with one chosen server error number. It exists so a test can drive the REAL
// query driver through a REAL code path (a batch INSERT, a checkpoint) and get
// back the error the driver itself builds from the wire, instead of an error
// value the test assembled by hand. No Docker, no real MySQL.
//
// It is its own package, not part of testutil, because it links the go-mysql
// wire-protocol server and testutil must stay a leaf.
package fakemysql

import (
	"net"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

// User and Password are the only credentials the server accepts.
const (
	User     = "root"
	Password = "x"
)

// secretMessage is the text of every error packet. It reads like the kind of
// message a real server sends (a table name, a host), so a test can assert
// that none of it reaches what the code under test puts on the wire.
const secretMessage = "refused for customer_orders on db.internal"

// SecretFragments are the pieces of the server's error text a caller can
// search for to prove the message did not leak.
var SecretFragments = []string{"customer_orders", "db.internal"}

// RefuseEverything starts a server that completes the handshake and then
// answers every statement, prepared or not, with server error number code.
// It returns a go-sql-driver DSN. The listener closes on test cleanup.
func RefuseEverything(t *testing.T, code uint16) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fakemysql: listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	srv := server.NewDefaultServer()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go func(c net.Conn) {
				defer c.Close()
				// Read deadlines guarantee a server goroutine can never
				// wedge a test.
				c.SetReadDeadline(time.Now().Add(15 * time.Second))
				mc, err := srv.NewConn(c, User, Password, refuser{code: code})
				if err != nil {
					return
				}
				for {
					c.SetReadDeadline(time.Now().Add(15 * time.Second))
					if err := mc.HandleCommand(); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return User + ":" + Password + "@tcp(" + ln.Addr().String() + ")/db?timeout=5s"
}

// refuser is the go-mysql Handler behind RefuseEverything.
type refuser struct{ code uint16 }

func (r refuser) err() error { return gomysql.NewError(r.code, secretMessage) }

func (r refuser) UseDB(string) error                          { return nil }
func (r refuser) HandleQuery(string) (*gomysql.Result, error) { return nil, r.err() }
func (r refuser) HandleStmtClose(any) error                   { return nil }
func (r refuser) HandleOtherCommand(byte, []byte) error       { return r.err() }
func (r refuser) HandleStmtPrepare(string) (int, int, any, error) {
	return 0, 0, nil, r.err()
}

func (r refuser) HandleFieldList(string, string) ([]*gomysql.Field, error) {
	return nil, r.err()
}

func (r refuser) HandleStmtExecute(any, string, []any) (*gomysql.Result, error) {
	return nil, r.err()
}
