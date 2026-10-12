package console

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// The trend charts (#2330) read numbers the service already publishes as
// metrics. Nothing scrapes anything: the web interface runs in the same
// process as capture, so it reads the metrics registry directly on a timer
// and keeps the readings in memory. A restart empties them, and every answer
// says how long they have been collected for.
const (
	// SeriesSampleEvery is how often the registry is read.
	SeriesSampleEvery = 15 * time.Second
	// SeriesKeep is how far back readings are kept.
	SeriesKeep = 24 * time.Hour
	// seriesPerRing is SeriesKeep at one reading a minute: an answer has one
	// point a minute, so only the last reading of each minute is kept.
	seriesPerRing = int(SeriesKeep / seriesStep)
	// SeriesMax bounds how many series are kept. The registry is this
	// program's own, so the count is small; the bound is what keeps a label
	// that turns out to be unbounded from growing memory without limit.
	SeriesMax = 512
	// seriesPrefix: only this program's own metrics are kept, not the Go
	// runtime's or the process collector's.
	seriesPrefix = "bintrail_"
	// seriesStep is the width of one point in an answer.
	seriesStep = time.Minute
)

// Kinds a series answer names. The literals are the wire format.
const (
	SeriesGauge   = "gauge"
	SeriesCounter = "counter"
)

// SeriesWindows are the windows an answer can cover, by their wire name.
var SeriesWindows = map[string]time.Duration{
	"15m": 15 * time.Minute, "1h": time.Hour, "6h": 6 * time.Hour, "24h": 24 * time.Hour,
}

type seriesSample struct {
	at int64 // unix seconds
	v  float64
}

// seriesRing is one series' readings, the last of each minute, oldest first,
// bounded to seriesPerRing.
type seriesRing struct {
	name   string
	kind   string
	labels map[string]string
	buf    []seriesSample // at is the START of the minute
	head   int            // index of the oldest reading once buf is full
	last   int64          // when the newest reading was taken, unix seconds
	// fromZero: the series appeared after readings began, so it did not
	// exist before its first reading and that reading is a rise from zero.
	// (A counter with labels is only published once it first counts.)
	fromZero bool
	// firstMin is the minute of the series' first reading ever.
	firstMin int64
}

// add files a reading taken at unix second at. A reading that is not later
// than the newest one (the clock stepped back) is not kept: minutes out of
// order would be added up twice, or read as a counter falling.
func (r *seriesRing) add(at int64, v float64, limit int) {
	if len(r.buf) > 0 && at <= r.last {
		return
	}
	step := int64(seriesStep / time.Second)
	min := at / step * step
	if len(r.buf) == 0 {
		r.firstMin = min
	}
	r.last = at
	if n := len(r.buf); n > 0 {
		newest := (r.head + n - 1) % n
		if r.buf[newest].at == min {
			r.buf[newest].v = v
			return
		}
	}
	if len(r.buf) < limit {
		r.buf = append(r.buf, seriesSample{min, v})
		return
	}
	r.buf[r.head] = seriesSample{min, v}
	r.head = (r.head + 1) % len(r.buf)
}

// each calls fn with every reading, oldest first.
func (r *seriesRing) each(fn func(seriesSample)) {
	for i := range r.buf {
		fn(r.buf[(r.head+i)%len(r.buf)])
	}
}

// SeriesStore keeps the readings. The zero value is not usable; NewSeriesStore.
type SeriesStore struct {
	gather prometheus.Gatherer
	limit  int // readings per ring; seriesPerRing outside tests

	mu      sync.Mutex
	rings   map[string]*seriesRing
	kinds   map[string]string // metric name -> kind, for a name with no reading in the window
	first   time.Time         // the first read that succeeded; zero before it
	dropped int               // series not kept by the newest read because SeriesMax was reached
	lastErr string            // the newest read's failure, empty after a good one
}

// NewSeriesStore returns a store that reads g. It reads nothing until Sample
// or Run is called.
func NewSeriesStore(g prometheus.Gatherer) *SeriesStore {
	return &SeriesStore{gather: g, limit: seriesPerRing, rings: map[string]*seriesRing{}, kinds: map[string]string{}}
}

// newSeriesStoreOrNil is NewSeriesStore for a gatherer that may be absent. A
// nil interface must become a nil store, not a store that reads nothing.
func newSeriesStoreOrNil(g prometheus.Gatherer) *SeriesStore {
	if g == nil {
		return nil
	}
	return NewSeriesStore(g)
}

// Run reads the registry now and then every SeriesSampleEvery until ctx ends.
func (st *SeriesStore) Run(ctx context.Context) {
	st.Sample(time.Now())
	t := time.NewTicker(SeriesSampleEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			st.Sample(now)
		}
	}
}

// Sample reads the registry once and files every gauge and counter of this
// program's own under now. A value that is not a finite number is not kept:
// it could not be drawn, and it could not be written as JSON either.
func (st *SeriesStore) Sample(now time.Time) {
	// Gather can return families AND an error (one collector failed); what
	// it did return is still good.
	fams, err := st.gather.Gather()
	st.mu.Lock()
	defer st.mu.Unlock()
	was, wasDropped := st.lastErr, st.dropped
	st.lastErr, st.dropped = "", 0
	if err != nil {
		st.lastErr = err.Error()
	}
	began := !st.first.IsZero()
	if len(fams) > 0 && !began {
		st.first = now
	}
	at := now.Unix()
	// Said when it starts and when it changes, not every 15 seconds.
	defer func() {
		if st.lastErr != "" && st.lastErr != was {
			slog.Warn("trend charts: reading the metrics failed; the charts are missing what it could not read", "error", st.lastErr)
		}
		if st.dropped > 0 && wasDropped == 0 {
			slog.Warn("trend charts: more series than are kept; the rest are not charted", "kept", SeriesMax, "left_out", st.dropped)
		}
		// A series nothing has reported for a whole day gives its place up.
		for key, ring := range st.rings {
			if at-ring.last > int64(SeriesKeep/time.Second) {
				delete(st.rings, key)
			}
		}
	}()
	for _, f := range fams {
		name := f.GetName()
		if !strings.HasPrefix(name, seriesPrefix) {
			continue
		}
		var kind string
		switch f.GetType().String() {
		case "GAUGE":
			kind = SeriesGauge
		case "COUNTER":
			kind = SeriesCounter
		default:
			continue
		}
		st.kinds[name] = kind
		for _, m := range f.GetMetric() {
			v := m.GetGauge().GetValue()
			if kind == SeriesCounter {
				v = m.GetCounter().GetValue()
			}
			if math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			labels := make(map[string]string, len(m.GetLabel()))
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			key := seriesKey(name, labels)
			ring := st.rings[key]
			if ring == nil {
				if len(st.rings) >= SeriesMax {
					st.dropped++
					continue
				}
				ring = &seriesRing{name: name, kind: kind, labels: labels, fromZero: began}
				st.rings[key] = ring
			}
			ring.add(at, v, st.limit)
		}
	}
}

func seriesKey(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		fmt.Fprintf(&b, "\x00%s\x00%s", k, labels[k])
	}
	return b.String()
}

// SeriesPoint is one minute of an answer: its start, unix seconds, and the
// value. It is written as a two-element array.
type SeriesPoint struct {
	At int64
	V  float64
}

// MarshalJSON writes the point as [at, value].
func (p SeriesPoint) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, "[%d,%s]", p.At, formatSeriesValue(p.V)), nil
}

func formatSeriesValue(v float64) string {
	return strings.TrimSuffix(strings.TrimRight(fmt.Sprintf("%.6f", v), "0"), ".")
}

// SeriesAnswer is one name's readings over a window, one point per minute.
type SeriesAnswer struct {
	Name string `json:"name"`
	// Kind is empty when no metric of that name has been read: not known
	// here, which is a different thing from known and quiet.
	Kind   string `json:"kind,omitempty"`
	Window string `json:"window"`
	// Unit says what a point's value is: the reading itself for a gauge,
	// the increase per minute for a counter.
	Unit string `json:"unit,omitempty"`
	// Series is how many label sets were added up into the points.
	Series int `json:"series"`
	// CollectedSeconds is how long readings have been kept for, whatever the
	// window: a chart over 24 hours of a service started an hour ago holds
	// an hour.
	CollectedSeconds int64 `json:"collected_seconds"`
	// ReadError is why the newest read of the metrics failed, and LeftOut
	// how many series it could not keep: either way the points may be
	// missing something, and the page says so instead of drawing them as
	// whole.
	ReadError string        `json:"read_error,omitempty"`
	LeftOut   int           `json:"left_out,omitempty"`
	Points    []SeriesPoint `json:"points"`
}

// ErrSeriesAmbiguous: the name and labels match several gauges. Adding
// gauges up gives a number that means nothing (two servers' lag, two
// timestamps), so the caller has to say which one.
var ErrSeriesAmbiguous = errors.New("several series match; add labels to pick one")

// Query answers name over window ending at now. A gauge's minute is its last
// reading in it, and the name and labels must pick ONE gauge. A counter's
// minute is how much it rose since the previous minute that has a reading,
// per minute, added up over every series that matches; a minute where it
// went DOWN (the series was re-created) has no point, since the fall says
// nothing about how much was counted. A counter's current minute is left
// out: it is not over, and a part of a minute drawn as a whole one would dip
// at the right edge of every chart.
//
// What a chart of these cannot show: most stream gauges only move when a
// change arrives, so a stalled stream draws a flat line, not a rising one.
func (st *SeriesStore) Query(name string, want map[string]string, window string, now time.Time) (SeriesAnswer, error) {
	span, ok := SeriesWindows[window]
	if !ok {
		return SeriesAnswer{}, fmt.Errorf("window must be one of 15m, 1h, 6h, 24h")
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	ans := SeriesAnswer{Name: name, Kind: st.kinds[name], Window: window, Points: []SeriesPoint{},
		ReadError: st.lastErr, LeftOut: st.dropped}
	if !st.first.IsZero() && now.After(st.first) {
		ans.CollectedSeconds = int64(now.Sub(st.first) / time.Second)
	}
	if ans.Kind == SeriesCounter {
		ans.Unit = "per_minute"
	}
	step := int64(seriesStep / time.Second)
	thisMin := now.Unix() / step * step
	from := now.Add(-span).Unix() / step * step
	sums := map[int64]float64{}
	for _, ring := range st.rings {
		if ring.name != name || !seriesLabelsInclude(ring.labels, want) {
			continue
		}
		ans.Series++
		if ring.kind == SeriesGauge && ans.Series > 1 {
			return SeriesAnswer{}, ErrSeriesAmbiguous
		}
		var prev *seriesSample
		ring.each(func(s seriesSample) {
			cur := s
			defer func() { prev = &cur }()
			if s.at < from || s.at > thisMin {
				return
			}
			if ring.kind == SeriesGauge {
				sums[s.at] += s.v
				return
			}
			if s.at == thisMin {
				return
			}
			switch {
			case prev != nil && s.v >= prev.v:
				sums[s.at] += (s.v - prev.v) / float64((s.at-prev.at)/step)
			case prev == nil && ring.fromZero && s.at == ring.firstMin:
				sums[s.at] += s.v
			}
		})
	}
	for at, v := range sums {
		ans.Points = append(ans.Points, SeriesPoint{At: at, V: v})
	}
	slices.SortFunc(ans.Points, func(a, b SeriesPoint) int { return int(a.At - b.At) })
	return ans, nil
}

func seriesLabelsInclude(have, want map[string]string) bool {
	for k, v := range want {
		if got, ok := have[k]; !ok || got != v {
			return false
		}
	}
	return true
}

// ParseSeriesLabels reads "k=v,k2=v2" into a map. Empty is no filter. A
// value may hold anything but a comma; a part with no "=" or no key is an
// error, never skipped: a filter read wrong would add up the wrong series.
func ParseSeriesLabels(s string) (map[string]string, error) {
	out := map[string]string{}
	if strings.TrimSpace(s) == "" {
		return out, nil
	}
	for part := range strings.SplitSeq(s, ",") {
		k, v, ok := strings.Cut(part, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("labels must look like name=value,name=value")
		}
		if _, dup := out[k]; dup {
			return nil, fmt.Errorf("label %q is given twice", k)
		}
		out[k] = v
	}
	return out, nil
}
