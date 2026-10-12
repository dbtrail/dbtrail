package shim

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

// What the pump took in ahead is handed to the protocol layer whole and in
// order, more of it than it holds at once, and the socket's end comes after
// the last byte, not instead of it.
func TestWatchConnHandsOverEverythingReadAheadInOrder(t *testing.T) {
	client, srvSide := net.Pipe()
	wc, ctx, stop := WatchConn(context.Background(), srvSide)
	defer stop()

	want := make([]byte, 3*watchReadAhead+777)
	for i := range want {
		want[i] = byte(i*31 + i>>8)
	}
	go func() {
		for sent := 0; sent < len(want); {
			n := min(1000+sent%4096, len(want)-sent)
			if _, err := client.Write(want[sent : sent+n]); err != nil {
				return
			}
			sent += n
		}
		client.Close()
	}()
	// Nobody reads for a while: the pump holds its share and no more.
	time.Sleep(100 * time.Millisecond)
	got, err := io.ReadAll(wc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("read %d bytes, want the %d sent, in order", len(got), len(want))
	}
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the close after the last byte was not seen")
	}
}

// The pump does not take in without limit for a client that sends and
// never lets its answers be read.
func TestReadAheadWaitsPastItsShare(t *testing.T) {
	a := newReadAhead(10)
	if !a.add(make([]byte, 10)) {
		t.Fatal("the first bytes were refused")
	}
	added := make(chan bool, 1)
	go func() { added <- a.add([]byte{1}) }()
	select {
	case <-added:
		t.Fatal("bytes past the share were taken in without waiting")
	case <-time.After(100 * time.Millisecond):
	}
	if b, _ := a.next(); len(b) != 10 {
		t.Fatalf("next() = %d bytes, want the 10 held", len(b))
	}
	if !<-added {
		t.Fatal("the waiting bytes were refused once there was room")
	}
	// Nobody will read: a waiting add is let go.
	go func() { added <- a.add(make([]byte, 20)) && a.add([]byte{2}) }()
	time.Sleep(50 * time.Millisecond)
	a.abandon()
	if <-added {
		t.Fatal("an add went through after the reader gave up")
	}
}
