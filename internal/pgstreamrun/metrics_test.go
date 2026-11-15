package pgstreamrun

import (
	"bufio"
	"context"
	"database/sql"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// Prometheus metrics of a PostgreSQL capture (#2762).

// scrapeMetrics reads addr/metrics the way Prometheus does and returns each
// sample by its line's name and labels, e.g.
// `bintrail_pg_last_commit_lsn{source="s"}`.
func scrapeMetrics(t *testing.T, addr string) map[string]float64 {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("scrape %s: %v", addr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("scrape %s: status %d", addr, resp.StatusCode)
	}
	out := map[string]float64{}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			t.Fatalf("scrape %s: a line with no value: %q", addr, line)
		}
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if err != nil {
			t.Fatalf("scrape %s: %q: %v", addr, line, err)
		}
		out[line[:i]] = v
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scrape %s: %v", addr, err)
	}
	return out
}

// serveMetrics starts the package's metrics server on a free local port.
func serveMetrics(t *testing.T) string {
	t.Helper()
	addr, stop, err := startMetricsServer("127.0.0.1:0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("startMetricsServer: %v", err)
	}
	t.Cleanup(stop)
	return addr
}

// sample returns one series of a source, failing when it is not published.
func sample(t *testing.T, got map[string]float64, name, source string) float64 {
	t.Helper()
	key := name + `{source="` + source + `"}`
	v, ok := got[key]
	if !ok {
		t.Fatalf("%s is not published", key)
	}
	return v
}

// A loop started without metrics calls every method on a nil handle.
func TestPGMetricsNilPublishesNothingAndDoesNotPanic_2762(t *testing.T) {
	var m *pgMetrics
	now := time.Now()
	m.received(now, now)
	m.statement(3)
	m.failed(errTypeBatchFlush)
	m.split()
	m.published(3, []indexer.CommitStamp{{ReadAt: now, Timestamp: now}}, now)
	m.checkpointSaved(10, now)
	m.slotRead(pgcapture.HealthSnapshot{Slot: pgcapture.SlotHealth{Exists: true, RetainedBytes: 1}}, now)
	// The hooks the loop hands the writer are method values of that handle.
	st := &pgStreamState{}
	f := st.metrics.statement
	f(1)
}

func TestPGMetricsSourceLabel_2762(t *testing.T) {
	if got := metricsSource(Config{MetricsSource: "entry-1", SlotName: "slot_a"}); got != "entry-1" {
		t.Errorf("with a source given: %q, want it", got)
	}
	if got := metricsSource(Config{SlotName: "slot_a"}); got != "slot_a" {
		t.Errorf("with none given: %q, want the slot's name", got)
	}
	// Neither: the series are still well formed, under the MySQL capture's
	// fallback label.
	addr := serveMetrics(t)
	newPGMetrics(metricsSource(Config{})).checkpointSaved(7, time.Time{})
	if v := sample(t, scrapeMetrics(t, addr), "bintrail_pg_last_commit_lsn", "default"); v != 7 {
		t.Errorf("with no source and no slot: last_commit_lsn{source=default} = %v, want 7", v)
	}
}

// Every series exists from the moment the handles do, at zero: a capture that
// has done nothing yet answers a scrape with all of them, and an alert on a
// timestamp's age fires instead of finding no data.
func TestPGMetricsPublishedAtZeroBeforeAnyTraffic_2762(t *testing.T) {
	addr := serveMetrics(t)
	const src = "zero-2762"
	newPGMetrics(src)
	got := scrapeMetrics(t, addr)
	for _, name := range []string{
		"bintrail_stream_events_received_total", "bintrail_stream_events_indexed_total",
		"bintrail_stream_batch_flushes_total", "bintrail_stream_checkpoint_saves_total",
		"bintrail_stream_transaction_splits_total", "bintrail_stream_last_event_timestamp_seconds",
		"bintrail_stream_replication_lag_seconds", "bintrail_stream_availability_lag_seconds",
		"bintrail_stream_last_flush_timestamp_seconds",
		"bintrail_pg_last_commit_lsn", "bintrail_pg_slot_retained_wal_bytes",
		"bintrail_pg_slot_health_timestamp_seconds", "bintrail_pg_caught_up_timestamp_seconds",
	} {
		if v := sample(t, got, name, src); v != 0 {
			t.Errorf("%s = %v before anything happened, want 0", name, v)
		}
	}
}

func TestPGMetricsReceived_2762(t *testing.T) {
	addr := serveMetrics(t)
	const src = "received-2762"
	m := newPGMetrics(src)
	now := time.Unix(1_800_000_100, 0)
	m.received(now.Add(-40*time.Second), now)
	// A change with no time is counted, and is not a reading of the lag.
	m.received(time.Time{}, now)
	got := scrapeMetrics(t, addr)
	if v := sample(t, got, "bintrail_stream_events_received_total", src); v != 2 {
		t.Errorf("events_received_total = %v, want 2", v)
	}
	if v := sample(t, got, "bintrail_stream_last_event_timestamp_seconds", src); v != 1_800_000_060 {
		t.Errorf("last_event_timestamp_seconds = %v, want 1800000060", v)
	}
	if v := sample(t, got, "bintrail_stream_replication_lag_seconds", src); v != 40 {
		t.Errorf("replication_lag_seconds = %v, want 40", v)
	}
}

func TestPGMetricsPublished_2762(t *testing.T) {
	addr := serveMetrics(t)
	now := time.Unix(1_800_000_000, 0)
	read := func(src string) (indexed, lag, flush, latencies, latencySum float64) {
		got := scrapeMetrics(t, addr)
		return sample(t, got, "bintrail_stream_events_indexed_total", src),
			sample(t, got, "bintrail_stream_availability_lag_seconds", src),
			sample(t, got, "bintrail_stream_last_flush_timestamp_seconds", src),
			got[`bintrail_stream_index_commit_latency_seconds_count{source="`+src+`"}`],
			got[`bintrail_stream_index_commit_latency_seconds_sum{source="`+src+`"}`]
	}

	t.Run("the oldest row of the commit is its lag", func(t *testing.T) {
		const src = "published-max-2762"
		newPGMetrics(src).published(3, []indexer.CommitStamp{
			{ReadAt: now.Add(-2 * time.Second), Timestamp: now.Add(-5 * time.Second)},
			{ReadAt: now.Add(-1 * time.Second), Timestamp: now.Add(-30 * time.Second)},
			{ReadAt: now.Add(-1 * time.Second), Timestamp: now.Add(-7 * time.Second)},
		}, now)
		indexed, lag, flush, n, sum := read(src)
		if indexed != 3 || lag != 30 || flush != float64(now.Unix()) || n != 3 || sum != 4 {
			t.Errorf("indexed %v lag %v flush %v latencies %v (sum %v); want 3, 30, %d, 3 (sum 4)", indexed, lag, flush, n, sum, now.Unix())
		}
	})

	t.Run("a row with no read time is not observed as a wait of zero", func(t *testing.T) {
		const src = "published-noread-2762"
		newPGMetrics(src).published(2, []indexer.CommitStamp{
			{Timestamp: now.Add(-50 * time.Second)},
			{},
		}, now)
		indexed, lag, flush, n, _ := read(src)
		if indexed != 2 || lag != 0 || n != 0 {
			t.Errorf("indexed %v lag %v latencies %v; want 2, 0 (not set), 0", indexed, lag, n)
		}
		if flush != float64(now.Unix()) {
			t.Errorf("last flush %v, want %d: rows were committed", flush, now.Unix())
		}
	})

	t.Run("a source clock ahead of this one is a lag of zero, not a negative", func(t *testing.T) {
		const src = "published-ahead-2762"
		m := newPGMetrics(src)
		m.published(1, []indexer.CommitStamp{{ReadAt: now, Timestamp: now.Add(-9 * time.Second)}}, now)
		m.published(1, []indexer.CommitStamp{{ReadAt: now, Timestamp: now.Add(20 * time.Second)}}, now)
		if _, lag, _, _, _ := read(src); lag != 0 {
			t.Errorf("lag %v, want 0", lag)
		}
	})

	t.Run("a row read with no commit time is observed and sets no lag", func(t *testing.T) {
		const src = "published-notime-2762"
		newPGMetrics(src).published(1, []indexer.CommitStamp{{ReadAt: now.Add(-3 * time.Second)}}, now)
		if _, lag, _, n, sum := read(src); lag != 0 || n != 1 || sum != 3 {
			t.Errorf("lag %v latencies %v (sum %v); want 0, 1 (sum 3)", lag, n, sum)
		}
	})

	t.Run("a commit of nothing is not a flush", func(t *testing.T) {
		const src = "published-empty-2762"
		newPGMetrics(src).published(0, nil, now)
		if indexed, _, flush, _, _ := read(src); indexed != 0 || flush != 0 {
			t.Errorf("indexed %v last flush %v; want 0, 0", indexed, flush)
		}
	})
}

func TestPGMetricsCheckpointSaved_2762(t *testing.T) {
	addr := serveMetrics(t)
	const src = "checkpoint-2762"
	m := newPGMetrics(src)
	read := func() (saves, lsn, caughtUp float64) {
		got := scrapeMetrics(t, addr)
		return sample(t, got, "bintrail_stream_checkpoint_saves_total", src),
			sample(t, got, "bintrail_pg_last_commit_lsn", src),
			sample(t, got, "bintrail_pg_caught_up_timestamp_seconds", src)
	}
	// 0x1A2B3C4D5E is past 32 bits: the LSN is published whole.
	m.checkpointSaved(0x1A2B3C4D5E, time.Time{})
	if saves, lsn, caughtUp := read(); saves != 1 || lsn != float64(0x1A2B3C4D5E) || caughtUp != 0 {
		t.Fatalf("saves %v lsn %v caught up %v; want 1, %d, 0 (nothing stamped)", saves, lsn, caughtUp, 0x1A2B3C4D5E)
	}
	at := time.Unix(1_800_000_000, 250_000_000)
	m.checkpointSaved(0x1A2B3C4D60, at)
	if saves, lsn, caughtUp := read(); saves != 2 || lsn != float64(0x1A2B3C4D60) || caughtUp != 1_800_000_000.25 {
		t.Fatalf("saves %v lsn %v caught up %v; want 2, %d, 1800000000.25", saves, lsn, caughtUp, 0x1A2B3C4D60)
	}
	// A checkpoint with nothing new to stamp keeps the last caught-up moment.
	m.checkpointSaved(0x1A2B3C4D60, time.Time{})
	if _, _, caughtUp := read(); caughtUp != 1_800_000_000.25 {
		t.Fatalf("caught up %v after a checkpoint that stamped nothing, want it kept", caughtUp)
	}
}

func TestPGMetricsSlotRead_2762(t *testing.T) {
	addr := serveMetrics(t)
	const src = "slot-2762"
	m := newPGMetrics(src)
	read := func() (retained, at float64) {
		got := scrapeMetrics(t, addr)
		return sample(t, got, "bintrail_pg_slot_retained_wal_bytes", src),
			sample(t, got, "bintrail_pg_slot_health_timestamp_seconds", src)
	}
	first := time.Unix(1_800_000_000, 0)
	m.slotRead(pgcapture.HealthSnapshot{Slot: pgcapture.SlotHealth{Exists: true, RetainedBytes: 5 << 30, SafeWalSize: sql.NullInt64{}}}, first)
	if retained, at := read(); retained != float64(5<<30) || at != float64(first.Unix()) {
		t.Fatalf("retained %v read at %v; want %d at %d", retained, at, 5<<30, first.Unix())
	}
	// The slot is gone: there is nothing to report, and the reading there is
	// stays, with its time, so it shows as old.
	m.slotRead(pgcapture.HealthSnapshot{}, first.Add(time.Minute))
	if retained, at := read(); retained != float64(5<<30) || at != float64(first.Unix()) {
		t.Fatalf("retained %v read at %v after a reading that found no slot; want the last one kept", retained, at)
	}
	m.slotRead(pgcapture.HealthSnapshot{Slot: pgcapture.SlotHealth{Exists: true, RetainedBytes: 0}}, first.Add(2*time.Minute))
	if retained, at := read(); retained != 0 || at != float64(first.Add(2*time.Minute).Unix()) {
		t.Fatalf("retained %v read at %v; want 0 at the new reading", retained, at)
	}
}

func TestPGMetricsErrorsByType_2762(t *testing.T) {
	addr := serveMetrics(t)
	const src = "errors-2762"
	m := newPGMetrics(src)
	m.failed(errTypeHealthProbe)
	m.failed(errTypeHealthProbe)
	m.failed(errTypeCheckpoint)
	m.failed(errTypeBatchFlush)
	m.split()
	m.statement(250)
	got := scrapeMetrics(t, addr)
	for typ, want := range map[string]float64{"health_probe": 2, "checkpoint": 1, "batch_flush": 1} {
		if v := got[`bintrail_stream_errors_total{source="`+src+`",type="`+typ+`"}`]; v != want {
			t.Errorf("errors_total{type=%s} = %v, want %v", typ, v, want)
		}
	}
	if v := sample(t, got, "bintrail_stream_transaction_splits_total", src); v != 1 {
		t.Errorf("transaction_splits_total = %v, want 1", v)
	}
	if v := sample(t, got, "bintrail_stream_batch_flushes_total", src); v != 1 {
		t.Errorf("batch_flushes_total = %v, want 1", v)
	}
	if v := got[`bintrail_stream_batch_size_sum{source="`+src+`"}`]; v != 250 {
		t.Errorf("batch_size sum = %v, want 250", v)
	}
}

// An address that cannot be opened fails One before it connects to anything:
// the DSNs here reach nothing, and the error is the address's.
func TestOneRefusesAMetricsAddressItCannotOpen_2762(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer held.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = One(ctx, Config{
		IndexDSN: "u:p@tcp(127.0.0.1:1)/nothing", ReplDSN: "postgres://127.0.0.1:1/x?replication=database",
		QueryDSN: "postgres://127.0.0.1:1/x", SlotName: "s", Publication: "p",
		MetricsAddr: held.Addr().String(), Logger: slog.New(slog.DiscardHandler),
	})
	if err == nil || !strings.Contains(err.Error(), "metrics server: cannot bind "+held.Addr().String()) {
		t.Fatalf("One with a metrics address already in use: %v; want it refused, naming the address", err)
	}
}

// With no address, One opens none, and the capture runs as before (here it
// fails on the index it cannot reach, not on metrics).
func TestOneWithoutAMetricsAddressOpensNone_2762(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := One(ctx, Config{
		IndexDSN: "u:p@tcp(127.0.0.1:1)/nothing", ReplDSN: "postgres://127.0.0.1:1/x?replication=database",
		QueryDSN: "postgres://127.0.0.1:1/x", SlotName: "s", Publication: "p",
		Logger: slog.New(slog.DiscardHandler),
	})
	if err == nil || strings.Contains(err.Error(), "metrics") {
		t.Fatalf("One with no metrics address: %v; want the index connection's error", err)
	}
}
