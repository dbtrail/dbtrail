package consoleapp

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowCloseListener behaves like a net.Listener on a loaded machine: Close
// wakes Accept (which returns net.ErrClosed at once) and only later releases
// the socket. Go's own listener has the same order, with a window of
// microseconds instead of 200ms.
type slowCloseListener struct {
	stop     chan struct{}
	stopOnce sync.Once
	released atomic.Bool
}

func (l *slowCloseListener) Accept() (net.Conn, error) {
	<-l.stop
	return nil, net.ErrClosed
}

// Close mirrors net.Listener: only the first call releases the socket, and a
// second call returns at once, while the first may still be releasing it.
func (l *slowCloseListener) Close() error {
	first := false
	l.stopOnce.Do(func() { close(l.stop); first = true })
	if !first {
		return net.ErrClosed
	}
	time.Sleep(200 * time.Millisecond)
	l.released.Store(true)
	return nil
}

func (l *slowCloseListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
}

// TestFlashbackControl_OffAnswersAfterTheSocketIsReleased (#2149): turning the
// port off must not return while the listening socket still exists, or the
// web interface answers "off" over a port that still completes connections.
// Waiting for the accept loop alone is not enough: it exits as soon as Close
// has started.
func TestFlashbackControl_OffAnswersAfterTheSocketIsReleased(t *testing.T) {
	c := newControl(t)
	ln := &slowCloseListener{stop: make(chan struct{})}
	c.mu.Lock()
	c.serveLocked("127.0.0.1:1", ln)
	c.mu.Unlock()

	if err := c.Apply(""); err != nil {
		t.Fatal(err)
	}
	if !ln.released.Load() {
		t.Fatal("Apply(\"\") returned before the listening socket was released: the port still takes connections after it answered off")
	}
	if got := c.Listening(); got != "" {
		t.Fatalf("Listening() = %q after turning off", got)
	}
}

// TestCloseOnceListener_SecondCloseWaitsForTheFirst: when the serving
// goroutine's Close got there first, the stopper's Close must still return
// only once the socket is released, not with the early "already closing"
// error a plain listener gives the second caller.
func TestCloseOnceListener_SecondCloseWaitsForTheFirst(t *testing.T) {
	slow := &slowCloseListener{stop: make(chan struct{})}
	w := &closeOnceListener{Listener: slow}
	go func() { _ = w.Close() }()
	<-slow.stop // the first Close has started: Accept would already return
	if err := w.Close(); err != nil {
		t.Fatalf("second Close = %v, want the first one's result (nil)", err)
	}
	if !slow.released.Load() {
		t.Fatal("the second Close returned while the first was still releasing the socket")
	}
}
