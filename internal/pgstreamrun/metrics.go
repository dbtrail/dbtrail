package pgstreamrun

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// Prometheus metrics of a PostgreSQL capture (#2762).
//
// What means the same as in the MySQL capture is published under the same
// names, bintrail_stream_* with the same "source" label (observe.ForSource),
// so one dashboard and one alert rule serve both. What only PostgreSQL has is
// below, as bintrail_pg_*: registered here and not in internal/observe, so a
// binary that does not link this package does not publish them.
var (
	// A float64 holds every whole number up to 2^53 exactly, which as an LSN
	// is 8 PiB of WAL written since the cluster was created. Past that the
	// gauge still only grows, in steps of a few bytes.
	pgLastCommitLSN = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "pg",
		Name:      "last_commit_lsn",
		Help:      "Commit LSN of the last source transaction saved in the checkpoint, as a number of bytes (0 until the first checkpoint).",
	}, []string{"source"})

	pgSlotRetainedWALBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "pg",
		Name:      "slot_retained_wal_bytes",
		Help:      "Bytes of WAL the source keeps for the replication slot, as of the last reading of the slot.",
	}, []string{"source"})

	// The reading above is taken on a timer and kept when a reading fails, so
	// by itself it cannot tell "small" from "not read for an hour". This says
	// when it was read.
	pgSlotHealthTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "pg",
		Name:      "slot_health_timestamp_seconds",
		Help:      "Unix timestamp of the last reading of the replication slot that found it (0 until the first).",
	}, []string{"source"})

	pgCaughtUpTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "pg",
		Name:      "caught_up_timestamp_seconds",
		Help:      "Unix timestamp of the last moment the capture is known to have held everything the source had (0 until the first).",
	}, []string{"source"})
)

// The values of the "type" label of bintrail_stream_errors_total a PostgreSQL
// capture uses. The first two are the MySQL capture's.
const (
	errTypeBatchFlush  = "batch_flush"
	errTypeCheckpoint  = "checkpoint"
	errTypeHealthProbe = "health_probe"
)

// pgMetrics is one capture's metric handles. Every method is a no-op on a nil
// receiver, so a loop started without metrics (the tests' loops) calls them
// the same way. None of them blocks or returns an error: each is an atomic
// write to a value in memory, so the capture loop cannot stall or fail on one.
type pgMetrics struct {
	s             *observe.StreamMetrics
	lastCommitLSN prometheus.Gauge
	retainedWAL   prometheus.Gauge
	slotHealthAt  prometheus.Gauge
	caughtUpAt    prometheus.Gauge
}

// metricsSource is the "source" label of a capture: the one its supervisor
// gave it, else the replication slot's name (a standalone `bintrail-pg
// stream` has no other name for its source).
func metricsSource(cfg Config) string {
	if cfg.MetricsSource != "" {
		return cfg.MetricsSource
	}
	return cfg.SlotName
}

// newPGMetrics returns the handles for one source label. An empty source
// becomes "default", as observe.ForSource makes it.
func newPGMetrics(source string) *pgMetrics {
	if source == "" {
		source = "default"
	}
	return &pgMetrics{
		s:             observe.ForSource(source),
		lastCommitLSN: pgLastCommitLSN.WithLabelValues(source),
		retainedWAL:   pgSlotRetainedWALBytes.WithLabelValues(source),
		slotHealthAt:  pgSlotHealthTimestamp.WithLabelValues(source),
		caughtUpAt:    pgCaughtUpTimestamp.WithLabelValues(source),
	}
}

// received counts one row change read from the source, and how old it is.
// Receive time, like the MySQL capture's: the row is not in the index yet.
func (m *pgMetrics) received(at, now time.Time) {
	if m == nil {
		return
	}
	m.s.EventsReceived.Inc()
	if !at.IsZero() {
		ts := float64(at.Unix())
		m.s.LastEventTimestamp.Set(ts)
		m.s.ReplicationLag.Set(float64(now.Unix()) - ts)
	}
}

// statement counts one INSERT statement and its rows.
func (m *pgMetrics) statement(rows int) {
	if m == nil {
		return
	}
	m.s.BatchSize.Observe(float64(rows))
	m.s.BatchFlushes.Inc()
}

// failed counts one error of the given type.
func (m *pgMetrics) failed(errType string) {
	if m == nil {
		return
	}
	m.s.Errors.WithLabelValues(errType).Inc()
}

// split counts one source transaction made visible in parts.
func (m *pgMetrics) split() {
	if m == nil {
		return
	}
	m.s.TxnSplits.Inc()
}

// published records n rows committed to the index at now, with the same
// three rules as the MySQL capture (streamrun.observeCommitStamps):
//
//   - a row with no read time is not observed, never as a wait of zero;
//   - the availability lag is the oldest row's of the commit, clamped at 0
//     when the source's clock is ahead of this one;
//   - the last flush time is set by every commit that wrote rows.
func (m *pgMetrics) published(n int64, stamps []indexer.CommitStamp, now time.Time) {
	if m == nil {
		return
	}
	if n > 0 {
		m.s.EventsIndexed.Add(float64(n))
	}
	if len(stamps) == 0 {
		return
	}
	maxLag, haveLag := 0.0, false
	for i := range stamps {
		st := &stamps[i]
		if st.ReadAt.IsZero() {
			continue
		}
		m.s.IndexCommitLatency.Observe(now.Sub(st.ReadAt).Seconds())
		if st.Timestamp.IsZero() {
			continue
		}
		lag := max(now.Sub(st.Timestamp).Seconds(), 0)
		if !haveLag || lag > maxLag {
			maxLag, haveLag = lag, true
		}
	}
	if haveLag {
		m.s.AvailabilityLag.Set(maxLag)
	}
	m.s.LastFlushTimestamp.Set(float64(now.Unix()))
}

// checkpointSaved records a checkpoint saved at commitLSN, and the caught-up
// moment it stamped when it stamped one.
func (m *pgMetrics) checkpointSaved(commitLSN uint64, caughtUpAt time.Time) {
	if m == nil {
		return
	}
	m.s.CheckpointSaves.Inc()
	m.lastCommitLSN.Set(float64(commitLSN))
	if !caughtUpAt.IsZero() {
		m.caughtUpAt.Set(float64(caughtUpAt.UnixMicro()) / 1e6)
	}
}

// slotRead records a reading of the source's health taken at now. A reading
// that did not find the slot changes nothing: it has no retained WAL to
// report, and leaving the timestamp where it was is what makes the reading
// visibly old.
func (m *pgMetrics) slotRead(snap pgcapture.HealthSnapshot, now time.Time) {
	if m == nil || !snap.Slot.Exists {
		return
	}
	m.retainedWAL.Set(float64(snap.Slot.RetainedBytes))
	m.slotHealthAt.Set(float64(now.Unix()))
}

// startMetricsServer serves the process's Prometheus metrics at addr/metrics
// and returns the address it listens on (the port chosen, when addr asks for
// any) and the function that stops it. The address is bound before it
// returns, so one that cannot be opened fails the command: metrics were asked
// for, and running without them is worse than not starting. The same contract
// as the MySQL capture's (streamrun.StartMetricsServer), kept apart so this
// package does not link the MySQL capture.
func startMetricsServer(addr string, logger *slog.Logger) (bound string, stop func(), err error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Handler: mux}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", nil, fmt.Errorf("metrics server: cannot bind %s: %w", addr, err)
	}
	bound = ln.Addr().String()
	go func() {
		logger.Info("metrics server starting", "addr", bound)
		if serveErr := srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Error("metrics server error", "error", serveErr)
		}
	}()
	return bound, func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}, nil
}
