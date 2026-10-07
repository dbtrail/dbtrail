package console

import (
	"slices"
	"sync"
	"time"
)

// Capture lag trend (#2201). One coverage read says how far behind capture is
// now; it cannot say whether the gap is closing or widening, and those need
// opposite responses: a gap that closes is a burst being worked off, a gap
// that keeps widening is capture that cannot keep up with the source (an index
// MySQL on its 128 MB default buffer pool, #2141, or a write rate past what
// the index can take). The book keeps the last few coverage reads per server
// so the Overview can tell the two apart.
//
// Growth is measured as the time that passed on THIS host between two reads
// minus how far the newest indexed change moved between them. Both halves are
// differences on one clock each, so a constant offset between the source's
// clock and this host's cancels out; subtracting two lag figures would not,
// because the lag is clamped at zero when the source's clock runs ahead.
//
// The samples are fed by the reads themselves (any page, any tab), so a trend
// exists only once the coverage has been read at least twice, several minutes
// apart. The Overview reads it every five minutes when nothing new arrives,
// which is the case this is for: a stuck capture indexes nothing new.

const (
	// lagTrendWindow is how far back samples are kept.
	lagTrendWindow = 15 * time.Minute
	// lagTrendMinSpan is how long the samples must span before growth is
	// said at all: "several minutes", and one idle refresh of the Overview.
	lagTrendMinSpan = 4 * time.Minute
	// lagTrendMinGrowth is the least growth worth a card. Below it a slow
	// read or a burst of writes on the source explains the drift.
	lagTrendMinGrowth = 2 * time.Minute
	// lagTrendMinRatio: the gap must grow by at least this share of the
	// time that passed. Capture keeping 90 % of the pace for an hour still
	// grows the gap by six minutes, and that is worth saying; a few
	// seconds per minute is noise.
	lagTrendMinRatio = 0.2
	// lagTrendDedupe: a read closer than this to the previous sample is not
	// kept, so page reloads cannot push the older samples out.
	lagTrendDedupe = 20 * time.Second
	// lagTrendRecent is how far back "the last stretch" starts: the gap must
	// still be growing over it, not only over the whole window.
	lagTrendRecent = time.Minute
)

type lagSample struct {
	at      time.Time // this host's clock
	deltaTo time.Time // newest indexed change, the source's clock
}

// lagTrendBook is safe for concurrent use; the zero value is ready.
type lagTrendBook struct {
	mu      sync.Mutex
	samples map[string][]lagSample
}

func newLagTrendBook() *lagTrendBook { return &lagTrendBook{} }

// lagGrowthDTO is GET /api/coverage → lag_growth: present only when the gap
// between the source and the index widened by GrownSeconds over the last
// OverSeconds, by the rules above.
type lagGrowthDTO struct {
	GrownSeconds int64 `json:"grown_seconds"`
	OverSeconds  int64 `json:"over_seconds"`
	// Indexing: the newest indexed change moved forward over at least two
	// of the intervals between reads. Capture is still indexing, only slower
	// than the source writes. Without it the gap widened with nothing new
	// indexed, which a quiet database does just the same as a stuck capture,
	// so only the source itself can tell those two apart.
	Indexing bool `json:"indexing,omitempty"`
}

// observe records one read for server and says whether the gap is growing.
// A zero deltaTo (empty index) is not a sample: there is no edge to measure.
func (b *lagTrendBook) observe(server string, now, deltaTo time.Time) *lagGrowthDTO {
	if deltaTo.IsZero() {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.samples == nil {
		b.samples = map[string][]lagSample{}
	}
	kept := b.samples[server][:0:0]
	for _, s := range b.samples[server] {
		// Older than the window, or a newest change LATER than the one read
		// now: the index went backwards (rotated empty, rebuilt, another
		// index), and those samples describe something else.
		if now.Sub(s.at) > lagTrendWindow || s.deltaTo.After(deltaTo) {
			continue
		}
		kept = append(kept, s)
	}
	cur := lagSample{at: now, deltaTo: deltaTo}
	if n := len(kept); n == 0 || now.Sub(kept[n-1].at) >= lagTrendDedupe {
		kept = append(kept, cur)
	}
	b.samples[server] = kept
	return lagGrowth(kept, cur)
}

// grown is how much the gap widened from s to cur.
func grown(s, cur lagSample) time.Duration {
	return cur.at.Sub(s.at) - cur.deltaTo.Sub(s.deltaTo)
}

// lagGrowth decides on samples oldest first, cur being the read just made.
func lagGrowth(samples []lagSample, cur lagSample) *lagGrowthDTO {
	if len(samples) == 0 {
		return nil
	}
	first := samples[0]
	span := cur.at.Sub(first.at)
	if span < lagTrendMinSpan {
		return nil
	}
	g := grown(first, cur)
	if g < lagTrendMinGrowth || float64(g) < lagTrendMinRatio*float64(span) {
		return nil
	}
	// The last stretch: from the newest sample at least lagTrendRecent
	// before this read (the first one when none is). Growth earlier in the
	// window that has since stopped is not what is happening now.
	recent := first
	for _, s := range samples {
		if cur.at.Sub(s.at) >= lagTrendRecent {
			recent = s
		}
	}
	if rs := cur.at.Sub(recent.at); rs > 0 && float64(grown(recent, cur)) < lagTrendMinRatio*float64(rs) {
		return nil
	}
	// cur is the last sample unless the dedupe left it out.
	pts := samples
	if last := samples[len(samples)-1]; !last.at.Equal(cur.at) {
		pts = append(slices.Clone(samples), cur)
	}
	advances := 0
	for i := 1; i < len(pts); i++ {
		if pts[i].deltaTo.After(pts[i-1].deltaTo) {
			advances++
		}
	}
	return &lagGrowthDTO{GrownSeconds: int64(g / time.Second), OverSeconds: int64(span / time.Second), Indexing: advances >= 2}
}
