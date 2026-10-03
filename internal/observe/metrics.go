package observe

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Stream metrics — all in the bintrail_stream namespace.
//
// Every metric carries a "source" label so N concurrent streams in one
// process (the `bintrail-console watch` control plane) stay distinguishable: without
// it the counters of different sources conflate and the gauges clobber each
// other last-writer-wins. The label value is the supervisor's entry ID for
// monitored streams and the resolved bintrail_id (or "default") for a
// standalone `bintrail stream`. Call ForSource to obtain handles curried to
// one source; the underlying vectors are deliberately unexported.
var (
	streamEventsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "events_received_total",
		Help:      "Total number of binlog row events received from the replication stream.",
	}, []string{"source"})

	streamEventsIndexed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "events_indexed_total",
		Help:      "Total number of binlog row events written to binlog_events.",
	}, []string{"source"})

	streamBatchFlushes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "batch_flushes_total",
		Help:      "Total number of batch INSERT operations executed.",
	}, []string{"source"})

	streamCheckpointSaves = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "checkpoint_saves_total",
		Help:      "Total number of successful checkpoint saves to stream_state.",
	}, []string{"source"})

	streamLastEventTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "last_event_timestamp_seconds",
		Help:      "Unix timestamp of the last binlog event processed.",
	}, []string{"source"})

	streamReplicationLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "replication_lag_seconds",
		Help:      "Seconds between now and the timestamp of the last processed binlog event.",
	}, []string{"source"})

	// streamIndexCommitLatency is T2−T1 (#1223): the seconds between the
	// replication client delivering an event and that event being committed to
	// the index — i.e. how long a change existed but was NOT yet queryable or
	// recoverable. Both ends are the same process's clock, so it is skew-free
	// and sub-second-accurate, which is why it and not the availability gauge is
	// the metric to alert on. Observed PER EVENT: the point of a histogram here
	// is the tail (the slowest event in a batch is the one that defines the real
	// RPO), and a per-flush maximum would hide how many events waited.
	streamIndexCommitLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "index_commit_latency_seconds",
		Help:      "Seconds from replication read to the event being committed to the index (queryable/recoverable).",
		Buckets:   prometheus.ExponentialBuckets(0.005, 2, 14), // 5ms … ~40s
	}, []string{"source"})

	// streamAvailabilityLag is T2−T0: source commit → queryable, the effective
	// RPO. APPROXIMATE by construction — it subtracts the SOURCE's clock from
	// the local one, so it carries any skew between them, and the binlog header
	// timestamp it starts from has one-SECOND resolution. Negatives (a source
	// clock ahead of ours) are clamped to 0 rather than reported: a negative
	// availability lag is not a real state and would poison a rate/avg panel.
	streamAvailabilityLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "availability_lag_seconds",
		Help:      "Approximate seconds from source commit to queryable in the index (crosses source and local clocks; 1s source resolution; clamped at 0).",
	}, []string{"source"})

	// streamLastFlushTimestamp exists because every gauge above is only moved BY
	// TRAFFIC: under an idle source they all freeze at their last value and a
	// dashboard reads "caught up" forever, including when the stream died. Alert
	// on `time() - bintrail_stream_last_flush_timestamp_seconds` instead, and use
	// checkpoint recency (the checkpoint ticker runs with or without traffic) to
	// tell IDLE from STALLED.
	streamLastFlushTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "last_flush_timestamp_seconds",
		Help:      "Unix timestamp of the last batch successfully committed to the index.",
	}, []string{"source"})

	streamErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "errors_total",
		Help:      "Total number of errors encountered, partitioned by type.",
	}, []string{"source", "type"})

	streamBatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "batch_size",
		Help:      "Distribution of batch sizes (events per INSERT batch).",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 11), // 1, 2, 4, ..., 1024
	}, []string{"source"})

	// statementDMLDropped counts DML statements (INSERT/UPDATE/DELETE/REPLACE/
	// LOAD DATA) seen in the binlog as a QUERY_EVENT rather than as ROW events —
	// the signature of binlog_format=STATEMENT/MIXED or a session-level override
	// away from ROW. Each such statement is a change bintrail could NOT capture
	// (the row image is not in the binlog). It pairs with a loud slog.Warn at the
	// parser capture boundary (#776), symmetric to the partial-image guard (#493).
	//
	// Deliberately top-level (no "stream" subsystem, no "source" label): the name
	// renders exactly bintrail_statement_dml_dropped_total, which alerts key off,
	// and the parser has no source handle to curry. Concurrent `watch` streams
	// conflate into this single counter — the paired warn carries file/pos as the
	// per-source disambiguator.
	statementDMLDropped = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "bintrail",
		Name:      "statement_dml_dropped_total",
		Help:      "DML statements seen in the binlog under STATEMENT/MIXED format (or a session override) — changes bintrail could not capture as row events.",
	})

	// unhandledRowsDropped counts ROWS carried by a binlog row event whose type
	// handleRows does not decode — the warn-and-skip default arm shared by the
	// file and stream parsers. Each such row is a change bintrail could NOT
	// capture; the sibling of statementDMLDropped above for the row-event side
	// of the capture boundary.
	//
	// Same shape as its sibling, for the same reasons: top-level (no "stream"
	// subsystem, no "source" label — the parser has no source handle to curry)
	// and no schema/table labels (the documented stance in observability.md:
	// per-table label sets grow as tables come and go). The paired warn carries
	// file/pos, schema.table and the raw event type as the disambiguator.
	unhandledRowsDropped = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "bintrail",
		Name:      "unhandled_rows_dropped_total",
		Help:      "Rows carried by binlog row events of a type bintrail does not decode — changes dropped at capture, never indexed.",
	})
)

// sqlStatementPhase is where a SQL-on-the-copy statement's time went
// (#2026), for the console's SQL card and the embedded port alike: they share
// one runner. Observed only for a statement that RAN to a result; a busy
// refusal or a failed statement has no complete set of phases, and mixing
// partial ones in would pull every quantile toward zero. The phase label is a
// fixed set (view_build, spawn, open, lockdown, views, query, decode, total;
// spawn contains open, lockdown, views and query), so the cardinality is
// bounded. No server or user label on purpose: one series per phase answers
// "where does the time go on this daemon" without growing with the registry.
var sqlStatementPhase = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "bintrail",
	Subsystem: "sql",
	Name:      "statement_phase_seconds",
	Help:      "Seconds per phase of a SQL-on-the-copy statement that ran to a result (view_build, spawn, open, lockdown, views, query, decode, total).",
	Buckets:   prometheus.ExponentialBuckets(0.001, 2, 17), // 1ms … ~65s (the statement cap is 60s)
}, []string{"phase"})

// ObserveSQLStatementPhase records one phase of a SQL-on-the-copy statement.
func ObserveSQLStatementPhase(phase string, d time.Duration) {
	sqlStatementPhase.WithLabelValues(phase).Observe(d.Seconds())
}

// StatementDMLDropped increments the statement-DML-dropped counter. Called from
// the binlog parser's QUERY_EVENT handlers (file and stream paths) when a DML
// statement — not DDL, not transaction-control — is observed, i.e. the
// row-capture invariant (binlog_format=ROW) has been violated at the source.
func StatementDMLDropped() { statementDMLDropped.Inc() }

// UnhandledRowsDropped adds n — the row count of one skipped event — to the
// unhandled-rows-dropped counter. Called from handleRows' default arm (shared
// by the file and stream parsers) when a RowsEvent arrives with a type bintrail
// does not decode: the rows are dropped, so the operator needs a metric to
// alert on, not just the paired warn.
func UnhandledRowsDropped(n int) { unhandledRowsDropped.Add(float64(n)) }

// StreamMetrics is the full set of stream metrics curried to one source
// label — call sites keep the ergonomics of plain counters/gauges.
type StreamMetrics struct {
	EventsReceived     prometheus.Counter
	EventsIndexed      prometheus.Counter
	BatchFlushes       prometheus.Counter
	CheckpointSaves    prometheus.Counter
	LastEventTimestamp prometheus.Gauge
	// ReplicationLag keeps its RECEIVE-time semantics (source commit → read off
	// the stream channel), deliberately unchanged: it is set before batching, so
	// it can read "caught up" while events sit in an unflushed batch. The
	// commit-side handles below are what close that window; existing dashboards
	// built on this gauge keep meaning what they meant.
	ReplicationLag prometheus.Gauge
	// AvailabilityLag and LastFlushTimestamp are COMMIT-side (#1223): set only
	// after a batch is durably in the index.
	AvailabilityLag    prometheus.Gauge
	LastFlushTimestamp prometheus.Gauge
	// Errors keeps its remaining "type" dimension (batch_flush, checkpoint,
	// gtid_update).
	Errors *prometheus.CounterVec
	// BatchSize is the per-source histogram handle.
	BatchSize prometheus.Observer
	// IndexCommitLatency is the per-source read→queryable histogram handle.
	IndexCommitLatency prometheus.Observer
}

// ForSource returns the stream metrics curried to the given source label.
// An empty source falls back to "default" so a standalone stream without a
// resolved identity still produces well-formed series.
func ForSource(source string) *StreamMetrics {
	if source == "" {
		source = "default"
	}
	return &StreamMetrics{
		EventsReceived:     streamEventsReceived.WithLabelValues(source),
		EventsIndexed:      streamEventsIndexed.WithLabelValues(source),
		BatchFlushes:       streamBatchFlushes.WithLabelValues(source),
		CheckpointSaves:    streamCheckpointSaves.WithLabelValues(source),
		LastEventTimestamp: streamLastEventTimestamp.WithLabelValues(source),
		ReplicationLag:     streamReplicationLag.WithLabelValues(source),
		AvailabilityLag:    streamAvailabilityLag.WithLabelValues(source),
		LastFlushTimestamp: streamLastFlushTimestamp.WithLabelValues(source),
		Errors:             streamErrors.MustCurryWith(prometheus.Labels{"source": source}),
		BatchSize:          streamBatchSize.WithLabelValues(source),
		IndexCommitLatency: streamIndexCommitLatency.WithLabelValues(source),
	}
}

// Index metrics — the bintrail_index namespace (#351).
//
// Where the stream metrics describe the live replication pipeline, these
// describe the STATE of the index a DBA cares about for forensic readiness:
// how far back recovery reaches, whether there are coverage gaps, and how much
// disk the index + archives occupy. They are GAUGES set periodically by a
// scraper (see streamrun) from a status snapshot — there is no per-event hot
// path here. Every series carries "source" for the same multi-stream reason as
// the stream metrics.
//
// CARDINALITY: index_events_total is a single aggregate per source — NOT
// labelled per schema/table, which would grow unbounded as tables come and go.
var (
	indexOldestEvent = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail", Subsystem: "index", Name: "oldest_event_timestamp_seconds",
		Help: "Unix timestamp of the oldest event still in the index (recovery floor).",
	}, []string{"source"})

	indexNewestEvent = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail", Subsystem: "index", Name: "newest_event_timestamp_seconds",
		Help: "Unix timestamp of the newest event in the index.",
	}, []string{"source"})

	indexRetentionHorizon = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail", Subsystem: "index", Name: "retention_horizon_seconds",
		Help: "How far back the index reaches: now minus the oldest event timestamp.",
	}, []string{"source"})

	indexEventsTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail", Subsystem: "index", Name: "events_total",
		Help: "Approximate number of row events in the index (information_schema estimate).",
	}, []string{"source"})

	indexPartitions = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail", Subsystem: "index", Name: "partitions_total",
		Help: "Number of binlog_events partitions, by kind (active hourly vs the p_future catch-all).",
	}, []string{"source", "kind"})

	indexGapHours = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail", Subsystem: "index", Name: "gap_hours",
		Help: "Hours that were rotated out of MySQL but not archived to Parquet — a hole in recovery coverage.",
	}, []string{"source"})

	indexStorageBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail", Subsystem: "index", Name: "storage_bytes",
		Help: "Bytes occupied by the index, by location (mysql = binlog_events table, parquet = archived partitions).",
	}, []string{"source", "location"})
)

// IndexSnapshot is a flat, status-package-free view of the index state that the
// scraper passes to IndexMetrics.Set. Keeping it plain avoids an observe→status
// import (status already depends on lower layers; a cycle would be one edit
// away). Zero OldestEvent/NewestEvent mean "unknown" (empty index) and are not
// published, so the timestamp gauges never report a misleading 1970 epoch.
type IndexSnapshot struct {
	OldestEvent      time.Time
	NewestEvent      time.Time
	Events           int64
	ActivePartitions int
	FuturePartitions int
	GapHours         int
	MySQLBytes       int64
	ParquetBytes     int64
	// HaveCoverage reports whether the coverage load succeeded. Events and
	// MySQLBytes come from it; when it degraded (false) they are NOT published,
	// so a swallowed partial-load failure can't mask the real values with a 0 —
	// the same misleading-zero defense as the OldestEvent/NewestEvent guards.
	HaveCoverage bool
}

// IndexMetrics is the index gauge set curried to one source.
type IndexMetrics struct{ source string }

// IndexForSource returns an index-metrics handle for the given source label
// (empty → "default", matching ForSource).
func IndexForSource(source string) *IndexMetrics {
	if source == "" {
		source = "default"
	}
	return &IndexMetrics{source: source}
}

// Set publishes one snapshot to the gauges. now is taken explicitly so the
// retention-horizon computation is deterministic under test.
func (m *IndexMetrics) Set(snap IndexSnapshot, now time.Time) {
	s := m.source
	if !snap.OldestEvent.IsZero() {
		indexOldestEvent.WithLabelValues(s).Set(float64(snap.OldestEvent.Unix()))
		indexRetentionHorizon.WithLabelValues(s).Set(now.Sub(snap.OldestEvent).Seconds())
	}
	if !snap.NewestEvent.IsZero() {
		indexNewestEvent.WithLabelValues(s).Set(float64(snap.NewestEvent.Unix()))
	}
	// events_total and storage_bytes{mysql} are coverage-derived — skip them on a
	// degraded load so a swallowed failure can't publish a misleading 0.
	if snap.HaveCoverage {
		indexEventsTotal.WithLabelValues(s).Set(float64(snap.Events))
		indexStorageBytes.WithLabelValues(s, "mysql").Set(float64(snap.MySQLBytes))
	}
	indexPartitions.WithLabelValues(s, "active").Set(float64(snap.ActivePartitions))
	indexPartitions.WithLabelValues(s, "future").Set(float64(snap.FuturePartitions))
	indexGapHours.WithLabelValues(s).Set(float64(snap.GapHours))
	indexStorageBytes.WithLabelValues(s, "parquet").Set(float64(snap.ParquetBytes))
}
