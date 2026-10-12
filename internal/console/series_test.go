package console

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type seriesFixture struct {
	reg   *prometheus.Registry
	st    *SeriesStore
	lag   *prometheus.GaugeVec
	count *prometheus.CounterVec
}

func newSeriesFixture(t *testing.T) *seriesFixture {
	t.Helper()
	f := &seriesFixture{reg: prometheus.NewRegistry()}
	f.lag = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "bintrail_stream_replication_lag_seconds"}, []string{"source"})
	f.count = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "bintrail_stream_events_indexed_total"}, []string{"source"})
	f.reg.MustRegister(f.lag, f.count)
	f.st = NewSeriesStore(f.reg)
	return f
}

var seriesT0 = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

func seriesPoints(t *testing.T, st *SeriesStore, name, labels, window string, now time.Time) SeriesAnswer {
	t.Helper()
	want, err := ParseSeriesLabels(labels)
	if err != nil {
		t.Fatal(err)
	}
	ans, err := st.Query(name, want, window, now)
	if err != nil {
		t.Fatal(err)
	}
	return ans
}

// A name never read is "not known" (no kind), which the page must be able to
// tell from a known metric that had nothing in the window.
func TestSeries_unknownNameHasNoKind(t *testing.T) {
	f := newSeriesFixture(t)
	ans := seriesPoints(t, f.st, "bintrail_nope", "", "1h", seriesT0)
	if ans.Kind != "" || len(ans.Points) != 0 || ans.Points == nil || ans.CollectedSeconds != 0 {
		t.Fatalf("got %+v", ans)
	}
}

// A gauge's minute is its last reading in that minute.
func TestSeries_gaugeKeepsTheLastReadingOfEachMinute(t *testing.T) {
	f := newSeriesFixture(t)
	for i, v := range []float64{5, 7, 9, 11, 2} { // 12:00:00 :15 :30 :45, 12:01:00
		f.lag.WithLabelValues("a").Set(v)
		f.st.Sample(seriesT0.Add(time.Duration(i) * 15 * time.Second))
	}
	ans := seriesPoints(t, f.st, "bintrail_stream_replication_lag_seconds", "source=a", "15m", seriesT0.Add(90*time.Second))
	if ans.Kind != SeriesGauge || ans.Unit != "" || ans.Series != 1 || ans.CollectedSeconds != 90 {
		t.Fatalf("got %+v", ans)
	}
	want := []SeriesPoint{{seriesT0.Unix(), 11}, {seriesT0.Unix() + 60, 2}}
	if len(ans.Points) != 2 || ans.Points[0] != want[0] || ans.Points[1] != want[1] {
		t.Fatalf("points = %+v, want %+v", ans.Points, want)
	}
}

// A counter's minute is how much it rose since the previous minute that has
// a reading, per minute. The first minute has nothing to rise from.
func TestSeries_counterIsARatePerMinute(t *testing.T) {
	f := newSeriesFixture(t)
	c := f.count.WithLabelValues("a")
	at := func(min int) time.Time { return seriesT0.Add(time.Duration(min) * time.Minute) }
	c.Add(100)
	f.st.Sample(at(0))
	c.Add(60)
	f.st.Sample(at(1))
	// No reading at minute 2: the rise over two minutes is spread over both.
	c.Add(300)
	f.st.Sample(at(3))
	ans := seriesPoints(t, f.st, "bintrail_stream_events_indexed_total", "", "15m", at(4))
	if ans.Kind != SeriesCounter || ans.Unit != "per_minute" {
		t.Fatalf("got %+v", ans)
	}
	want := []SeriesPoint{{at(1).Unix(), 60}, {at(3).Unix(), 150}}
	if len(ans.Points) != 2 || ans.Points[0] != want[0] || ans.Points[1] != want[1] {
		t.Fatalf("points = %+v, want %+v", ans.Points, want)
	}
}

// A counter that goes DOWN was re-created. That minute has no point: a
// negative rate would be drawn as if changes had been un-counted.
func TestSeries_counterThatFallsHasNoPointThatMinute(t *testing.T) {
	f := newSeriesFixture(t)
	at := func(min int) time.Time { return seriesT0.Add(time.Duration(min) * time.Minute) }
	f.count.WithLabelValues("a").Add(500)
	f.st.Sample(at(0))
	f.count.Reset()
	f.count.WithLabelValues("a").Add(20)
	f.st.Sample(at(1))
	f.count.WithLabelValues("a").Add(30)
	f.st.Sample(at(2))
	ans := seriesPoints(t, f.st, "bintrail_stream_events_indexed_total", "source=a", "15m", at(3))
	if len(ans.Points) != 1 || ans.Points[0] != (SeriesPoint{at(2).Unix(), 30}) {
		t.Fatalf("points = %+v", ans.Points)
	}
}

// Series are added up when the filter matches several, and a filter that
// names a label is exact: "a" does not match "ab".
func TestSeries_labelsFilterExactlyAndMatchesAreAddedUp(t *testing.T) {
	f := newSeriesFixture(t)
	f.lag.WithLabelValues("a").Set(3)
	f.lag.WithLabelValues("ab").Set(40)
	f.st.Sample(seriesT0)
	name := "bintrail_stream_replication_lag_seconds"
	now := seriesT0.Add(time.Second)
	if ans := seriesPoints(t, f.st, name, "", "15m", now); ans.Series != 2 || ans.Points[0].V != 43 {
		t.Fatalf("no filter: %+v", ans)
	}
	if ans := seriesPoints(t, f.st, name, "source=a", "15m", now); ans.Series != 1 || ans.Points[0].V != 3 {
		t.Fatalf("source=a: %+v", ans)
	}
	// Known metric, no series under that label: the kind is still named.
	if ans := seriesPoints(t, f.st, name, "source=zz", "15m", now); ans.Series != 0 || len(ans.Points) != 0 || ans.Kind != SeriesGauge {
		t.Fatalf("source=zz: %+v", ans)
	}
	if ans := seriesPoints(t, f.st, name, "nolabel=a", "15m", now); ans.Series != 0 {
		t.Fatalf("a label the series does not have matched: %+v", ans)
	}
}

func TestSeries_windowCutsOlderMinutes(t *testing.T) {
	f := newSeriesFixture(t)
	for min := range 30 {
		f.lag.WithLabelValues("a").Set(float64(min))
		f.st.Sample(seriesT0.Add(time.Duration(min) * time.Minute))
	}
	now := seriesT0.Add(29*time.Minute + 30*time.Second)
	ans := seriesPoints(t, f.st, "bintrail_stream_replication_lag_seconds", "", "15m", now)
	if len(ans.Points) != 16 || ans.Points[0].V != 14 || ans.Points[15].V != 29 {
		t.Fatalf("15m: %d points, first %+v", len(ans.Points), ans.Points[0])
	}
	if _, err := f.st.Query("bintrail_x", nil, "2h", now); err == nil {
		t.Fatal("a window that is not offered was accepted")
	}
}

// What is not kept: other programs' metrics, histograms, and a value that is
// not a finite number (it cannot be drawn, nor written as JSON).
func TestSeries_keepsOnlyOwnFiniteGaugesAndCounters(t *testing.T) {
	f := newSeriesFixture(t)
	other := prometheus.NewGauge(prometheus.GaugeOpts{Name: "go_goroutines_fake"})
	hist := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "bintrail_sql_statement_seconds"})
	f.reg.MustRegister(other, hist)
	other.Set(1)
	hist.Observe(1)
	f.lag.WithLabelValues("nan").Set(math.NaN())
	f.lag.WithLabelValues("inf").Set(math.Inf(1))
	f.lag.WithLabelValues("ok").Set(1)
	f.st.Sample(seriesT0)
	if n := len(f.st.rings); n != 1 {
		t.Fatalf("kept %d series, want 1", n)
	}
	b, err := json.Marshal(seriesPoints(t, f.st, "bintrail_stream_replication_lag_seconds", "", "15m", seriesT0.Add(time.Second)))
	if err != nil || !strings.Contains(string(b), `"points":[[1791720000,1]]`) {
		t.Fatalf("json = %s, err %v", b, err)
	}
}

func TestSeries_ringAndSeriesCountAreBounded(t *testing.T) {
	f := newSeriesFixture(t)
	f.st.limit = 4
	for i := range 10 {
		f.lag.WithLabelValues("a").Set(float64(i))
		f.st.Sample(seriesT0.Add(time.Duration(i) * time.Minute))
	}
	ans := seriesPoints(t, f.st, "bintrail_stream_replication_lag_seconds", "", "1h", seriesT0.Add(10*time.Minute))
	if len(ans.Points) != 4 || ans.Points[0].V != 6 || ans.Points[3].V != 9 {
		t.Fatalf("ring: %+v", ans.Points)
	}
	for i := range SeriesMax + 5 {
		f.lag.WithLabelValues("s" + strings.Repeat("x", i)).Set(1)
	}
	f.st.Sample(seriesT0.Add(11 * time.Minute))
	if len(f.st.rings) != SeriesMax || f.st.dropped != 6 {
		t.Fatalf("kept %d series, dropped %d", len(f.st.rings), f.st.dropped)
	}
}

func TestParseSeriesLabels(t *testing.T) {
	got, err := ParseSeriesLabels(" source = a , route=copy=x ")
	if err != nil || got["source"] != " a " || got["route"] != "copy=x " || len(got) != 2 {
		t.Fatalf("got %q, %v", got, err)
	}
	if m, err := ParseSeriesLabels("  "); err != nil || len(m) != 0 {
		t.Fatalf("empty: %v %v", m, err)
	}
	for _, bad := range []string{"source", "=a", "source=a,", "source=a,source=b", ","} {
		if _, err := ParseSeriesLabels(bad); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
}

func seriesServer(t *testing.T, g prometheus.Gatherer) *Server {
	t.Helper()
	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, SeriesGatherer: g})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func getSeries(t *testing.T, srv *Server, query string) (int, string) {
	t.Helper()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/series"+query, nil)
	req.Host = "127.0.0.1:8090"
	req.Header.Set("Authorization", "Bearer t")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func TestSeriesRoute(t *testing.T) {
	// No registry to read: refused, never an empty chart.
	if code, body := getSeries(t, seriesServer(t, nil), "?name=bintrail_x"); code != http.StatusForbidden || !strings.Contains(body, "runs apart") {
		t.Fatalf("no store: %d %s", code, body)
	}
	f := newSeriesFixture(t)
	srv := seriesServer(t, f.reg)
	f.lag.WithLabelValues("a").Set(4)
	srv.series.Sample(time.Now().Add(-time.Second))
	code, body := getSeries(t, srv, "?name=bintrail_stream_replication_lag_seconds&labels=source%3Da")
	if code != http.StatusOK || !strings.Contains(body, `"kind":"gauge"`) || !strings.Contains(body, `"window":"1h"`) || !strings.Contains(body, `,4]]`) {
		t.Fatalf("got %d %s", code, body)
	}
	for _, bad := range []string{"", "?name=go_goroutines", "?name=bintrail_x&window=2h", "?name=bintrail_x&labels=broken"} {
		if code, body := getSeries(t, srv, bad); code != http.StatusBadRequest {
			t.Errorf("%q: got %d %s", bad, code, body)
		}
	}
}
