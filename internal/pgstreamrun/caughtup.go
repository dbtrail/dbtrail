package pgstreamrun

import "time"

// When capture was last caught up with its source (#2845), for
// stream_state.caught_up_at: the stamp the web interface and the router read
// to say how far behind the source an answer is (#2556).
//
// The MySQL capture takes it from a replication heartbeat, which a source
// sends only when it has nothing left to send. PostgreSQL has no such
// message: a walsender's keepalive carries how far it has decoded, and it
// also sends one while it is still working through a backlog. So the proof
// is made with two readings instead:
//
//   - the health poll, every healthPollInterval, asks the source how far its
//     WAL is flushed, and the loop notes when it asked;
//   - if the walsender's sent position, as this loop has received it, is at
//     or past that reading within caughtUpWithin of the asking, the loop had
//     by then consumed everything the source had when it was asked. The
//     moment it asked is the stamp, written by the next checkpoint, which
//     first commits every complete transaction received before it.
//
// "Within caughtUpWithin" is what makes it a statement about that moment. A
// capture that reaches the reading twenty seconds later holds what the source
// had then, twenty seconds late: it is twenty seconds behind, and a stamp for
// it would tell the router "zero" for as long as that lag lasted. It gets no
// stamp, and reads as behind.
//
// Stamped about once a poll on a capture that keeps up: the cadence of the
// MySQL stamp on a quiet source, which is what the readers' window (two
// heartbeat periods) is sized for.

// caughtUpWithin is how soon after the source was asked the capture must
// have received its position for that moment to count. The walsender reports
// its position as soon as it runs out of WAL to decode, so a capture that
// keeps up has it in milliseconds; this is also the most the stamp can
// overstate: a capture up to this far behind at that moment is stamped.
const caughtUpWithin = time.Second

// caughtUpProof holds the one reading of the source that is still waiting
// to be reached: how far its WAL was flushed (lsn; 0 = none waiting) at a
// moment no later than the read (at: taken before the source was asked).
type caughtUpProof struct {
	lsn uint64
	at  time.Time
}

// asked notes a reading, in place of the one that was waiting: that one was
// not reached in time, or it would be gone. A zero lsn (the source could not
// be read) leaves none waiting.
func (p *caughtUpProof) asked(lsn uint64, at time.Time) {
	p.lsn, p.at = lsn, at
}

// received is told the newest sent position this loop has received (sent)
// and the time (now). When that position reaches the waiting reading it
// answers with the reading's moment, and ok says whether it was reached in
// time; either way the reading is spent. A zero position reaches nothing.
func (p *caughtUpProof) received(sent uint64, now time.Time) (at time.Time, ok bool) {
	if p.lsn == 0 || sent < p.lsn {
		return time.Time{}, false
	}
	at, ok = p.at, now.Sub(p.at) <= caughtUpWithin
	p.lsn, p.at = 0, time.Time{}
	if !ok {
		return time.Time{}, false
	}
	return at, true
}
