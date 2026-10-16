package streamrun

import (
	"time"

	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/parser"
)

// observeCommitLag publishes the commit-side availability metrics for a batch
// that has just landed in the index (#1223/#1225). now is T2 — the moment the
// INSERT committed and the batch became queryable and recoverable.
//
// Pure apart from the metric writes and separated from flush() so the decisions
// below are testable without a MySQL round trip; the caller has already
// established that the flush SUCCEEDED.
//
// Three rules the tests pin, each of which is a way this could lie:
//
//   - A zero ReadAt is SKIPPED, never observed as 0. Zero means the event was
//     built by something that does not read replication (file-mode re-index, a
//     synthesized event); recording it would report a re-index of month-old
//     binlogs as a 0-second latency, the most convincing possible "everything
//     is fresh". Per event, so a mixed batch cannot contaminate the histogram.
//   - The availability lag is the batch MAXIMUM, not the last event's: the
//     oldest change in the batch defines the recovery point, and a lag gauge
//     that reports anything better than its worst event is not a safety signal.
//   - LastFlushTimestamp is set for EVERY successful non-empty flush, including
//     one where every ReadAt was zero. It answers "when did this process last
//     make anything queryable", which is exactly the question the other two
//     gauges cannot answer once traffic stops.
func observeCommitLag(m *observe.StreamMetrics, batch []parser.Event, now time.Time) {
	observeCommitStamps(m, appendCommitStamps(nil, batch), now)
}

// commitStamp is the part of a written row observeCommitLag needs. Rows that
// wait in an open index transaction keep only this until it commits (#2391),
// not the row images.
type commitStamp struct {
	readAt, ts time.Time
}

// appendCommitStamps appends batch's stamps to dst.
func appendCommitStamps(dst []commitStamp, batch []parser.Event) []commitStamp {
	for i := range batch {
		dst = append(dst, commitStamp{readAt: batch[i].ReadAt, ts: batch[i].Timestamp})
	}
	return dst
}

// observeCommitStamps is observeCommitLag over stamps already taken.
func observeCommitStamps(m *observe.StreamMetrics, batch []commitStamp, now time.Time) {
	if m == nil || len(batch) == 0 {
		return
	}
	maxLag, haveLag := 0.0, false
	for i := range batch {
		ev := &batch[i]
		if ev.readAt.IsZero() {
			continue
		}
		m.IndexCommitLatency.Observe(now.Sub(ev.readAt).Seconds())
		if ev.ts.IsZero() {
			continue
		}
		// T2−T0 crosses the source's clock and ours. A source clock running
		// ahead yields a negative, which is not a state that exists: clamp to 0
		// rather than publish a number that would drag an average below zero.
		lag := now.Sub(ev.ts).Seconds()
		if lag < 0 {
			lag = 0
		}
		if !haveLag || lag > maxLag {
			maxLag, haveLag = lag, true
		}
	}
	if haveLag {
		m.AvailabilityLag.Set(maxLag)
	}
	m.LastFlushTimestamp.Set(float64(now.Unix()))
}
