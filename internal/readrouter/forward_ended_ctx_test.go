package readrouter

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A statement whose context has ended (its client left while it waited for
// the copy) is not sent to the source, and the session it would have run in
// is left as it was: the next statement on the connection is answered.
func TestForward_aStatementWhoseContextEndedIsNotSent(t *testing.T) {
	src := newSlowSource(t)
	fw := newSlowForwarder(t, src)
	defer fw.Close()
	if _, err := fw.Forward(context.Background(), "SELECT 1", &BufferSink{}); err != nil {
		t.Fatal(err)
	}
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := fw.Forward(ended, "SELECT SLEEP(25)", &BufferSink{})
	if !errors.Is(err, context.Canceled) || IsLost(err) {
		t.Fatalf("err = %v, want the context's Canceled and the session kept", err)
	}
	select {
	case <-src.entered:
		t.Fatal("the statement started on the source")
	case <-time.After(150 * time.Millisecond):
	}
	if _, err := fw.Forward(context.Background(), "SELECT 1", &BufferSink{}); err != nil {
		t.Fatalf("the session was not kept: the next statement got %v", err)
	}
}
