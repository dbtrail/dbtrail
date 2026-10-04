package shim

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

// TestWatchConnCancelsOnClientClose is the unit proof of the #823
// disconnect pump: bytes flow through the wrapped conn transparently,
// and closing the client side cancels the returned context promptly —
// the signal the host binds to the Handler so in-flight fetches
// abort.
func TestWatchConnCancelsOnClientClose(t *testing.T) {
	client, srvSide := net.Pipe()
	defer client.Close()
	wc, ctx, stop := WatchConn(context.Background(), srvSide)
	defer stop()

	go func() { _, _ = client.Write([]byte("hello")) }()
	buf := make([]byte, 5)
	if _, err := io.ReadFull(wc, buf); err != nil || string(buf) != "hello" {
		t.Fatalf("read through watched conn: %q, %v", buf, err)
	}
	select {
	case <-ctx.Done():
		t.Fatal("context done while the client is still connected")
	default:
	}

	client.Close()
	select {
	case <-ctx.Done():
		// disconnect detected
	case <-time.After(2 * time.Second):
		t.Fatal("client close did not cancel the connection context within 2s")
	}
}

// TestWatchConnCancelsOnParentCancel: the daemon's serve context is the
// pump context's parent, so SIGTERM propagates to every in-flight query.
func TestWatchConnCancelsOnParentCancel(t *testing.T) {
	client, srvSide := net.Pipe()
	defer client.Close()
	parent, parentCancel := context.WithCancel(context.Background())
	_, ctx, stop := WatchConn(parent, srvSide)
	defer stop()

	parentCancel()
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("parent cancel did not propagate to the connection context")
	}
}
