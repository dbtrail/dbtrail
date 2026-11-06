package shim

import (
	"bytes"
	"net"
	"reflect"
	"testing"
	"unsafe"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/server"
)

// handshakeSalt reads go-mysql's unexported field. If an upgrade renames or
// reshapes it, this fails here instead of every change of user ProxySQL makes
// failing with 1045 in production.
func TestHandshakeSalt_readsTheLibrarysField(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	served := make(chan *server.Conn, 1)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			served <- nil
			return
		}
		sc, err := server.NewDefaultServer().NewConn(nc, "u", "p", server.EmptyHandler{})
		if err != nil {
			nc.Close()
			served <- nil
			return
		}
		served <- sc
	}()
	c, err := client.Connect(ln.Addr().String(), "u", "p", "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	sc := <-served
	if sc == nil {
		t.Fatal("the server side of the handshake failed")
	}
	defer sc.Close()

	got := handshakeSalt(sc)
	f := reflect.ValueOf(c).Elem().FieldByName("salt")
	want := reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Bytes()
	if len(got) != 20 || !bytes.Equal(got, want) {
		t.Fatalf("handshakeSalt = %x, the client was sent %x", got, want)
	}
	// A copy: the caller cannot change the connection's salt.
	got[0] ^= 0xff
	if again := handshakeSalt(sc); !bytes.Equal(again, want) {
		t.Fatal("handshakeSalt returned the connection's own slice")
	}
	if handshakeSalt(nil) != nil {
		t.Fatal("handshakeSalt(nil) is not nil")
	}
}
