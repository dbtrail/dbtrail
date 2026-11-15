//go:build integration

package pgstreamrun

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Prometheus metrics of a PostgreSQL capture (#2762), scraped over HTTP.

// waitScrape scrapes until ok accepts what it reads, and returns that.
func waitScrape(t *testing.T, addr, what string, limit time.Duration, ok func(map[string]float64) bool) map[string]float64 {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		got := scrapeMetrics(t, addr)
		if ok(got) {
			return got
		}
		if time.Now().After(deadline) {
			var mine []string
			for k, v := range got {
				if (strings.HasPrefix(k, "bintrail_pg_") || strings.HasPrefix(k, "bintrail_stream_")) && !strings.Contains(k, "_bucket") {
					mine = append(mine, fmt.Sprintf("%s %v", k, v))
				}
			}
			t.Fatalf("%s: not within %s; the scrape has:\n%s", what, limit, strings.Join(mine, "\n"))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The loop, with the events and the source's readings fed by the test: each
// metric moves with the thing it names and with nothing else.
func TestIntegrationPGMetricsFollowTheLoop_2762(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	addr := serveMetrics(t)
	const src = "loop-2762"
	key := func(name string) string { return name + `{source="` + src + `"}` }

	var failing atomic.Bool
	var slotGone atomic.Bool
	var flushed atomic.Uint64
	var retained atomic.Int64
	retained.Store(4096)
	probe := func(context.Context) (pgcapture.HealthSnapshot, error) {
		if failing.Load() {
			return pgcapture.HealthSnapshot{}, errors.New("connection refused")
		}
		return pgcapture.HealthSnapshot{
			Slot:       pgcapture.SlotHealth{Exists: !slotGone.Load(), Active: true, RetainedBytes: retained.Load()},
			FlushedLSN: flushed.Load(),
		}, nil
	}
	idx := indexer.New(db, 1000)
	events := make(chan event.Event)
	done := make(chan error, 1)
	go func() {
		done <- streamLoopPG(context.Background(), events, idx, db, nil, 10*time.Millisecond, probe, 10*time.Millisecond,
			&pgStreamState{serverID: 92762, metrics: newPGMetrics(src)}, slog.New(slog.DiscardHandler), nil)
	}()
	send := func(ev event.Event) {
		t.Helper()
		select {
		case events <- ev:
		case err := <-done:
			t.Fatalf("streamLoopPG ended early: %v", err)
		}
	}
	defer func() {
		close(events)
		if err := <-done; err != nil {
			t.Fatalf("streamLoopPG: %v", err)
		}
	}()
	row := func(n int, committed time.Time) event.Event {
		ev := pgRow(n)
		ev.Timestamp, ev.ReadAt = committed, time.Now()
		return ev
	}

	// The slot is read before anything is captured.
	got := waitScrape(t, addr, "the slot's reading", 10*time.Second, func(m map[string]float64) bool {
		return m[key("bintrail_pg_slot_health_timestamp_seconds")] > 0
	})
	if v := got[key("bintrail_pg_slot_retained_wal_bytes")]; v != 4096 {
		t.Fatalf("slot_retained_wal_bytes = %v, want the reading's 4096", v)
	}
	if v := got[key("bintrail_pg_last_commit_lsn")]; v != 0 {
		t.Fatalf("last_commit_lsn = %v before any commit, want 0", v)
	}

	// Two rows of a transaction not committed yet: received, and not indexed.
	start := time.Now()
	send(row(1, start.Add(-3*time.Second)))
	send(row(2, start.Add(-8*time.Second)))
	waitScrape(t, addr, "two rows received", 10*time.Second, func(m map[string]float64) bool {
		return m[key("bintrail_stream_events_received_total")] == 2
	})
	time.Sleep(100 * time.Millisecond) // ten checkpoint ticks
	got = scrapeMetrics(t, addr)
	if n, lsn, flush := got[key("bintrail_stream_events_indexed_total")], got[key("bintrail_pg_last_commit_lsn")], got[key("bintrail_stream_last_flush_timestamp_seconds")]; n != 0 || lsn != 0 || flush != 0 {
		t.Fatalf("with the transaction still open: indexed %v, last_commit_lsn %v, last flush %v; want 0, 0, 0", n, lsn, flush)
	}
	if v := got[key("bintrail_stream_last_event_timestamp_seconds")]; v != float64(start.Add(-8*time.Second).Unix()) {
		t.Fatalf("last_event_timestamp_seconds = %v, want the last row's commit time %d", v, start.Add(-8*time.Second).Unix())
	}

	// Its commit: the rows are indexed and the checkpoint is at the commit.
	send(event.Event{EventType: event.EventCommit, EndPos: 1500})
	got = waitScrape(t, addr, "the checkpoint at the commit", 10*time.Second, func(m map[string]float64) bool {
		return m[key("bintrail_pg_last_commit_lsn")] == 1500
	})
	if n := got[key("bintrail_stream_events_indexed_total")]; n != 2 {
		t.Fatalf("events_indexed_total = %v, want 2", n)
	}
	if n := got[key("bintrail_stream_checkpoint_saves_total")]; n < 1 {
		t.Fatalf("checkpoint_saves_total = %v after a checkpoint was saved", n)
	}
	if n := got[key("bintrail_stream_batch_flushes_total")]; n < 1 {
		t.Fatalf("batch_flushes_total = %v after rows were written", n)
	}
	if n := got[key("bintrail_stream_index_commit_latency_seconds_count")]; n != 2 {
		t.Fatalf("index_commit_latency_seconds has %v observations, want one per row, 2", n)
	}
	if flush := got[key("bintrail_stream_last_flush_timestamp_seconds")]; flush < float64(start.Unix()) || flush > float64(time.Now().Unix()) {
		t.Fatalf("last_flush_timestamp_seconds = %v, want between the test's start %d and now", flush, start.Unix())
	}
	// The oldest row of the commit was committed 8 s before the test sent it.
	if lag := got[key("bintrail_stream_availability_lag_seconds")]; lag < 8 || lag > 60 {
		t.Fatalf("availability_lag_seconds = %v, want the oldest row's, 8 s and a little", lag)
	}
	if v := got[key("bintrail_pg_caught_up_timestamp_seconds")]; v != 0 {
		t.Fatalf("caught_up_timestamp_seconds = %v with the source never reached, want 0", v)
	}
	// What the metric says is what the index holds.
	var pos uint64
	if err := db.QueryRow(`SELECT binlog_position FROM stream_state WHERE id = 1`).Scan(&pos); err != nil || pos != 1500 {
		t.Fatalf("stream_state.binlog_position = %d (%v), want 1500", pos, err)
	}

	// The capture reaches what the source had: the caught-up moment is
	// published once a checkpoint stamps it.
	flushed.Store(2000)
	send(sentMarkerAt(2000))
	got = waitScrape(t, addr, "the caught-up moment", 10*time.Second, func(m map[string]float64) bool {
		return m[key("bintrail_pg_caught_up_timestamp_seconds")] > 0
	})
	if at := got[key("bintrail_pg_caught_up_timestamp_seconds")]; at < float64(start.Unix()) || at > float64(time.Now().Unix()+1) {
		t.Fatalf("caught_up_timestamp_seconds = %v, want between the test's start %d and now", at, start.Unix())
	}

	// The slot retains more: the next reading says so.
	retained.Store(1 << 33)
	waitScrape(t, addr, "the new reading of the slot", 10*time.Second, func(m map[string]float64) bool {
		return m[key("bintrail_pg_slot_retained_wal_bytes")] == float64(1<<33)
	})

	// The slot is gone: nothing is reported for it, and the last reading
	// stays, growing old. Then the readings fail: counted, and still the
	// last reading.
	slotGone.Store(true)
	time.Sleep(1100 * time.Millisecond)
	held := scrapeMetrics(t, addr)
	failing.Store(true)
	waitScrape(t, addr, "a failed reading counted", 10*time.Second, func(m map[string]float64) bool {
		return m[`bintrail_stream_errors_total{source="`+src+`",type="health_probe"}`] >= 1
	})
	time.Sleep(1100 * time.Millisecond)
	got = scrapeMetrics(t, addr)
	if got[key("bintrail_pg_slot_retained_wal_bytes")] != float64(1<<33) {
		t.Fatalf("slot_retained_wal_bytes = %v after readings that found no slot and readings that failed, want the last reading kept", got[key("bintrail_pg_slot_retained_wal_bytes")])
	}
	if at, was := got[key("bintrail_pg_slot_health_timestamp_seconds")], held[key("bintrail_pg_slot_health_timestamp_seconds")]; at != was || at > float64(time.Now().Unix()-1) {
		t.Fatalf("slot_health_timestamp_seconds = %v (was %v) two seconds after the last good reading, want it left where it was", at, was)
	}
	// None of that stopped the capture.
	send(row(3, time.Now()))
	send(event.Event{EventType: event.EventCommit, EndPos: 2500})
	waitScrape(t, addr, "a change captured while the readings fail", 10*time.Second, func(m map[string]float64) bool {
		return m[key("bintrail_stream_events_indexed_total")] == 3 && m[key("bintrail_pg_last_commit_lsn")] == 2500
	})
}

// Against a real PostgreSQL, through One and its own address: what the issue
// asks for (last flush time, last commit LSN, retained WAL, events written),
// labelled with the slot's name, for as long as One runs.
func TestIntegrationPGMetricsRealSource_2762(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	indexDB, dbName := testutil.CreateTestDB(t)

	const slot, pub, tbl = "bintrail_pgmet_it", "bintrail_pgmet_it_pub", "pgmet_it_t"
	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })
	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		for range 50 {
			_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active", slot)
			var n int
			if err := pg.QueryRow(bg, "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&n); err != nil || n == 0 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	dropAll()
	t.Cleanup(dropAll)
	mustExec := func(q string) {
		t.Helper()
		if _, err := pg.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	prevPoll := healthPollInterval
	healthPollInterval = 300 * time.Millisecond
	t.Cleanup(func() { healthPollInterval = prevPoll })

	// A port that is free now, for One to open.
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := free.Addr().String()
	free.Close()

	repl := pgDSN + "?replication=database"
	if strings.Contains(pgDSN, "?") {
		repl = pgDSN + "&replication=database"
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runErr := make(chan error, 1)
	go func() {
		runErr <- One(runCtx, Config{
			IndexDSN: testutil.BaseDSN() + "/" + dbName, ReplDSN: repl, QueryDSN: pgDSN,
			SlotName: slot, Publication: pub, ServerID: 2762, Tables: "public." + tbl,
			BatchSize: 100, Checkpoint: 200 * time.Millisecond,
			MetricsAddr: addr,
		})
	}()
	waitCaughtUp(t, "the slot active", 30*time.Second, func() bool {
		select {
		case err := <-runErr:
			t.Fatalf("One ended early: %v", err)
		default:
		}
		var active bool
		return pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active) == nil && active
	})
	key := func(name string) string { return name + `{source="` + slot + `"}` }

	start := time.Now()
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tbl))
	got := waitScrape(t, addr, "three rows indexed, the slot read, the capture caught up", 30*time.Second, func(m map[string]float64) bool {
		return m[key("bintrail_stream_events_indexed_total")] == 3 &&
			m[key("bintrail_pg_last_commit_lsn")] > 0 &&
			m[key("bintrail_pg_slot_health_timestamp_seconds")] > 0 &&
			m[key("bintrail_pg_caught_up_timestamp_seconds")] >= float64(start.Unix())
	})
	if n := got[key("bintrail_stream_events_received_total")]; n != 3 {
		t.Errorf("events_received_total = %v, want 3", n)
	}
	if flush := got[key("bintrail_stream_last_flush_timestamp_seconds")]; flush < float64(start.Unix()) || flush > float64(time.Now().Unix()) {
		t.Errorf("last_flush_timestamp_seconds = %v, want between the insert %d and now", flush, start.Unix())
	}
	// The capturer stamps when it read each change, so each row's wait to be
	// in the index is observed.
	if n := got[key("bintrail_stream_index_commit_latency_seconds_count")]; n != 3 {
		t.Errorf("index_commit_latency_seconds has %v observations, want one per row, 3", n)
	}
	if sum := got[key("bintrail_stream_index_commit_latency_seconds_sum")]; sum <= 0 || sum > 90 {
		t.Errorf("index_commit_latency_seconds sums to %v s over 3 rows, want more than 0 and well under the test's time", sum)
	}
	if lag := got[key("bintrail_stream_availability_lag_seconds")]; lag < 0 || lag > 60 {
		t.Errorf("availability_lag_seconds = %v, want a few seconds at most", lag)
	}
	if _, ok := got[key("bintrail_pg_slot_retained_wal_bytes")]; !ok {
		t.Errorf("slot_retained_wal_bytes is not published")
	}
	if at := got[key("bintrail_pg_slot_health_timestamp_seconds")]; at < float64(start.Unix()-30) || at > float64(time.Now().Unix()) {
		t.Errorf("slot_health_timestamp_seconds = %v, want a moment of this test", at)
	}
	// The LSN published is the checkpoint's, and is the source's: at or past
	// where the insert's commit ended, and not past where the source is now.
	lsn := uint64(got[key("bintrail_pg_last_commit_lsn")])
	var pos uint64
	if err := indexDB.QueryRow(`SELECT binlog_position FROM stream_state WHERE id = 1`).Scan(&pos); err != nil {
		t.Fatalf("read the checkpoint: %v", err)
	}
	if lsn != pos {
		t.Errorf("last_commit_lsn = %d, the checkpoint in the index is %d", lsn, pos)
	}
	var sourceNow uint64
	if err := pg.QueryRow(ctx, "SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint").Scan(&sourceNow); err != nil {
		t.Fatalf("read the source's position: %v", err)
	}
	if lsn > sourceNow {
		t.Errorf("last_commit_lsn = %d, past the source's own position %d", lsn, sourceNow)
	}
	// The slot's retained WAL is what the source reports for it.
	var retainedNow int64
	if err := pg.QueryRow(ctx, "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&retainedNow); err != nil {
		t.Fatalf("read the slot: %v", err)
	}
	if v := got[key("bintrail_pg_slot_retained_wal_bytes")]; v < 0 || v > float64(retainedNow)+float64(16<<20) {
		t.Errorf("slot_retained_wal_bytes = %v, the source says %d now", v, retainedNow)
	}

	// One stops: so does its address.
	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("One: %v", err)
	}
	if resp, err := http.Get("http://" + addr + "/metrics"); err == nil {
		resp.Body.Close()
		t.Fatalf("%s still answers after One returned", addr)
	}
}
