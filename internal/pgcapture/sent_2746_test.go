package pgcapture

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
)

// #2746: a keepalive's position becomes an EventSentThrough marker, in stream
// order, only when it is past the last one marked. A quiet database's
// walsender sends the same position at every wakeup, and each copy would be
// one more event on the channel for nothing.
func TestSentMarker_2746(t *testing.T) {
	var s sentMarker
	steps := []struct {
		walEnd uint64
		emit   bool
	}{
		{0, false},      // 0/0 is no position
		{0x100, true},   // the first one
		{0x100, false},  // the same again
		{0x80, false},   // never backwards
		{0x101, true},   // one byte further
		{1 << 32, true}, // across the high word
	}
	for i, st := range steps {
		ev, ok := s.mark(st.walEnd)
		if ok != st.emit {
			t.Fatalf("step %d (%#x): emitted %v, want %v", i, st.walEnd, ok, st.emit)
		}
		if !ok {
			continue
		}
		if ev.EventType != event.EventSentThrough || ev.EndPos != st.walEnd {
			t.Fatalf("step %d: got %+v, want an EventSentThrough at %#x", i, ev, st.walEnd)
		}
		// A marker is not a change: nothing that would index it as a row.
		if ev.Schema != "" || ev.Table != "" || ev.RowAfter != nil || ev.RowBefore != nil || !ev.Timestamp.IsZero() {
			t.Fatalf("step %d: the marker carries change data: %+v", i, ev)
		}
	}
}
