package pgstreamrun

import (
	"testing"
	"time"
)

func TestCaughtUpProof(t *testing.T) {
	t0 := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) time.Time { return t0.Add(d) }
	type step struct {
		// ask: a reading (lsn at askAt). Otherwise the loop received a sent
		// position (sent at now), and want is the moment answered.
		ask     bool
		lsn     uint64
		askAt   time.Duration
		sent    uint64
		now     time.Duration
		want    time.Duration
		wantNot bool
	}
	asked := func(lsn uint64, d time.Duration) step { return step{ask: true, lsn: lsn, askAt: d} }
	got := func(sent uint64, now, want time.Duration) step { return step{sent: sent, now: now, want: want} }
	none := func(sent uint64, now time.Duration) step { return step{sent: sent, now: now, wantNot: true} }
	for _, tc := range []struct {
		name  string
		steps []step
	}{
		{name: "nothing asked, nothing reached", steps: []step{none(9000, 0)}},
		{name: "reached at once", steps: []step{asked(500, 0), got(500, 5*time.Millisecond, 0)}},
		{name: "past the reading", steps: []step{asked(500, 0), got(900, 5*time.Millisecond, 0)}},
		{name: "one short of the reading, then reached", steps: []step{
			asked(500, 0), none(499, 5*time.Millisecond), got(500, 20*time.Millisecond, 0)}},
		{name: "a zero position reaches nothing", steps: []step{asked(500, 0), none(0, 0)}},
		{name: "reached at the edge of in time", steps: []step{asked(500, 0), got(500, caughtUpWithin, 0)}},
		{name: "reached too late: behind by that much, and no stamp", steps: []step{
			asked(500, 0), none(500, caughtUpWithin+time.Millisecond)}},
		{name: "a reading reached too late is spent, not answered at the next position", steps: []step{
			asked(500, 0), none(500, 20*time.Second), none(900, 21*time.Second)}},
		{name: "a reading is answered once", steps: []step{
			asked(500, 0), got(500, time.Millisecond, 0), none(900, 2*time.Millisecond)}},
		{name: "a capture twenty seconds behind is never stamped, poll after poll", steps: []step{
			asked(500, 0), none(400, time.Second),
			asked(800, 30*time.Second), none(500, 30*time.Second+time.Millisecond), none(700, 40*time.Second),
			asked(1100, 60*time.Second), none(800, 60*time.Second+time.Millisecond)}},
		{name: "a new reading replaces one that was not reached", steps: []step{
			asked(500, 0), asked(800, 30*time.Second),
			// 500 was reached long ago by now; it is not what is waiting.
			none(600, 30*time.Second+time.Millisecond),
			got(800, 30*time.Second+10*time.Millisecond, 30*time.Second)}},
		{name: "a reading that failed leaves none waiting", steps: []step{
			asked(500, 0), asked(0, 30*time.Second), none(9000, 30*time.Second+time.Millisecond)}},
		{name: "a quiet source: the position received before the asking answers it", steps: []step{
			asked(500, 0), got(500, time.Millisecond, 0),
			asked(500, 30*time.Second), got(500, 30*time.Second+time.Millisecond, 30*time.Second)}},
		{name: "a source whose WAL went backward is read as any other", steps: []step{
			asked(900, 0), asked(300, 30*time.Second), got(900, 30*time.Second+time.Millisecond, 30*time.Second)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var p caughtUpProof
			for i, st := range tc.steps {
				if st.ask {
					// The source's clock, seven hours off this host's: it
					// travels with the reading it was read with.
					p.asked(st.lsn, at(st.askAt), at(st.askAt).Add(-7*time.Hour))
					continue
				}
				moment, sourceAt, ok := p.received(st.sent, at(st.now))
				if ok && !sourceAt.Equal(at(st.want).Add(-7*time.Hour)) {
					t.Fatalf("step %d: the source's clock for the reading = %v, want the one read with it", i, sourceAt)
				}
				if !ok && !sourceAt.IsZero() {
					t.Fatalf("step %d: a reading not reached answered a source clock %v", i, sourceAt)
				}
				if ok == st.wantNot || (ok && !moment.Equal(at(st.want))) {
					t.Fatalf("step %d: received(%d at %s) = %v, %v; want %s, none: %v", i, st.sent, st.now, moment.Sub(t0), ok, st.want, st.wantNot)
				}
			}
		})
	}
}
