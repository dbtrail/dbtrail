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
//   - once the capture's sent-through position (every commit below it is in
//     the index, #2746) reaches that reading, the capture holds everything
//     the source had when it was asked. That moment is the stamp.
//
// Stamped about once a poll on a capture that keeps up, written at the next
// checkpoint: the cadence of the MySQL stamp on a quiet source, which is what
// the readers' window (two heartbeat periods) is sized for. A capture that is
// behind stamps the older reading it did reach, or nothing, and reads as
// behind.

// maxCaughtUpSamples bounds the readings kept while the capture has not
// reached them: two minutes of polls. An older one would stamp a moment the
// readers' window no longer counts.
const maxCaughtUpSamples = 4

// sourceReading is how far the source's WAL was flushed (lsn) at a moment no
// later than the read (at: taken before the source was asked).
type sourceReading struct {
	lsn uint64
	at  time.Time
}

// caughtUpSamples holds the readings the capture has not reached yet, oldest
// first.
type caughtUpSamples struct {
	pending []sourceReading
}

// add notes a reading. A zero lsn (the source was not read) is not one. A
// reading before the newest kept one means the source's WAL went backward (a
// source restored or replaced): what was kept is about another history, and
// is dropped.
func (s *caughtUpSamples) add(lsn uint64, at time.Time) {
	if lsn == 0 {
		return
	}
	if n := len(s.pending); n > 0 && lsn < s.pending[n-1].lsn {
		s.pending = s.pending[:0]
	}
	if len(s.pending) == maxCaughtUpSamples {
		s.pending = append(s.pending[:0], s.pending[1:]...)
	}
	s.pending = append(s.pending, sourceReading{lsn: lsn, at: at})
}

// reached answers with the moment of the newest reading the capture's
// sent-through position has reached, and forgets it and the older ones. ok
// is false when it has reached none (a zero position reaches none).
func (s *caughtUpSamples) reached(sentThrough uint64) (at time.Time, ok bool) {
	n := 0
	for n < len(s.pending) && s.pending[n].lsn <= sentThrough {
		n++
	}
	if n == 0 {
		return time.Time{}, false
	}
	at = s.pending[n-1].at
	s.pending = append(s.pending[:0], s.pending[n:]...)
	return at, true
}
