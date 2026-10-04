package readrouter

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// A source that cannot be reached, or that drops the connection, is lost for
// the rest of the client connection: every later call fails with
// CodeUpstreamLost without dialling again, so the client learns its session
// is gone instead of getting a fresh one behind its back.
func TestForwarder_lostStaysLost(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var accepted atomic.Int32
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			c.Close() // no handshake: the dial succeeds, the protocol fails
		}
	}()
	f, err := NewForwarder("u:p@tcp("+ln.Addr().String()+")/db?tls=false", DefaultPolicy(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	f.connectTimeout = time.Second
	ctx := context.Background()
	if _, err := f.Forward(ctx, "SELECT 1", &BufferSink{}); !IsLost(err) {
		t.Fatalf("first statement: err = %v, want the lost error", err)
	}
	if _, err := f.Decide(ctx, "SELECT 1"); !IsLost(err) {
		t.Errorf("Decide after the loss: err = %v, want the lost error", err)
	}
	if err := f.UseDB(ctx, "x"); !IsLost(err) {
		t.Errorf("UseDB after the loss: err = %v, want the lost error", err)
	}
	if n := accepted.Load(); n != 1 {
		t.Errorf("the source was dialled %d times, want once: a lost connection must not be replaced", n)
	}
	if f.InTransaction() {
		t.Error("a lost connection reports a transaction")
	}
}

func TestNewForwarder_dsn(t *testing.T) {
	f, err := NewForwarder("root:pw@tcp(db.example)/shop?tls=skip-verify&parseTime=true", Policy{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if f.addr != "db.example:3306" || f.db != "shop" || f.tls == nil || !f.tls.InsecureSkipVerify {
		t.Errorf("parsed addr=%q db=%q tls=%v, want the default port, the database and the DSN's TLS", f.addr, f.db, f.tls)
	}
	if _, err := NewForwarder("root@unix(/tmp/sock)/x", Policy{}, 0); err == nil {
		t.Error("a unix-socket DSN was accepted")
	}
	if _, err := NewForwarder("not a dsn", Policy{}, 0); err == nil {
		t.Error("garbage was accepted")
	}
}
