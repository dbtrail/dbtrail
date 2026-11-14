package pgstreamrun

import (
	"testing"
	"time"
)

func TestCaughtUpSamples(t *testing.T) {
	t0 := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	type step struct {
		// add a reading (lsn, at second), or ask with a sent-through position.
		add     uint64
		addAt   int
		sent    uint64
		ask     bool
		want    int // the second of the moment answered
		wantNot bool
	}
	for _, tc := range []struct {
		name  string
		steps []step
	}{
		{name: "nothing read, nothing reached", steps: []step{{ask: true, sent: 9000, wantNot: true}}},
		{name: "a reading the capture has reached", steps: []step{
			{add: 500, addAt: 0}, {ask: true, sent: 500, want: 0}}},
		{name: "a reading the capture is one short of", steps: []step{
			{add: 500, addAt: 0}, {ask: true, sent: 499, wantNot: true}, {ask: true, sent: 500, want: 0}}},
		{name: "a zero position reaches nothing", steps: []step{
			{add: 500, addAt: 0}, {ask: true, sent: 0, wantNot: true}}},
		{name: "a reading that failed is not one", steps: []step{
			{add: 0, addAt: 0}, {ask: true, sent: 9000, wantNot: true}}},
		{name: "the newest reached is the moment, and it is answered once", steps: []step{
			{add: 500, addAt: 0}, {add: 600, addAt: 30}, {add: 900, addAt: 60},
			{ask: true, sent: 700, want: 30},
			{ask: true, sent: 700, wantNot: true},
			{ask: true, sent: 900, want: 60}}},
		{name: "a capture behind stamps the older reading it reached, not the newest", steps: []step{
			{add: 500, addAt: 0}, {add: 800, addAt: 30}, {ask: true, sent: 500, want: 0}}},
		{name: "a quiet source: the same position read again is reached at once", steps: []step{
			{add: 500, addAt: 0}, {ask: true, sent: 500, want: 0}, {add: 500, addAt: 30}, {ask: true, sent: 500, want: 30}}},
		{name: "only the newest readings are kept", steps: []step{
			{add: 100, addAt: 0}, {add: 200, addAt: 30}, {add: 300, addAt: 60}, {add: 400, addAt: 90}, {add: 500, addAt: 120},
			// The reading at second 0 is gone: a capture that only reached
			// it has no moment recent enough to stamp.
			{ask: true, sent: 100, wantNot: true},
			{ask: true, sent: 200, want: 30}}},
		{name: "a source whose WAL went backward: what was read before is another history", steps: []step{
			{add: 900, addAt: 0}, {add: 300, addAt: 30},
			{ask: true, sent: 900, want: 30},
			{ask: true, sent: 900, wantNot: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var s caughtUpSamples
			for i, st := range tc.steps {
				if !st.ask {
					s.add(st.add, at(st.addAt))
					if len(s.pending) > maxCaughtUpSamples {
						t.Fatalf("step %d: %d readings kept, at most %d", i, len(s.pending), maxCaughtUpSamples)
					}
					continue
				}
				got, ok := s.reached(st.sent)
				if ok == st.wantNot || (ok && !got.Equal(at(st.want))) {
					t.Fatalf("step %d: reached(%d) = %v, %v; want second %d, none: %v", i, st.sent, got.Sub(t0), ok, st.want, st.wantNot)
				}
			}
		})
	}
}
