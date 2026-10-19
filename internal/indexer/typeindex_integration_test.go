//go:build integration

package indexer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// idx_type_lookup on a real server: who gets it, and that building it on a
// live index neither repeats nor stops capture.

func typeIndexKey(t *testing.T, db *sql.DB) string {
	t.Helper()
	var cols []string
	rows, err := db.Query(`SELECT COLUMN_NAME FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events' AND INDEX_NAME = 'idx_type_lookup'
		ORDER BY SEQ_IN_INDEX`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatal(err)
		}
		cols = append(cols, c)
	}
	return strings.Join(cols, ",")
}

const wantTypeKey = "event_type,schema_name,table_name,event_timestamp"

// fillEvents writes n events spread over the three types and a few tables,
// in one statement per 2,000 rows.
func fillEvents(t *testing.T, db *sql.DB, n int) {
	t.Helper()
	idx := New(db, 2000)
	for off := 0; off < n; off += 2000 {
		evs := make([]event.Event, min(2000, n-off))
		for i := range evs {
			k := off + i
			evs[i] = event.Event{BinlogFile: "binlog.000001", StartPos: uint64(k), EndPos: uint64(k + 1),
				Timestamp: time.Now().UTC(), Schema: "app", Table: fmt.Sprintf("t%d", k%5),
				EventType: event.EventType(1 + k%3), PKValues: fmt.Sprint(k), RowAfter: map[string]any{"id": k, "pad": strings.Repeat("x", 64)}}
		}
		if _, err := idx.InsertBatch(evs); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTypeIndex_freshInstallHasIt(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	if err := CreateIndexTables(context.Background(), db, 3, false, nil); err != nil {
		t.Fatal(err)
	}
	if got := typeIndexKey(t, db); got != wantTypeKey {
		t.Fatalf("fresh binlog_events idx_type_lookup = %q, want %q", got, wantTypeKey)
	}
	state, _, err := TypeIndexStatus(context.Background(), db)
	if err != nil || state != TypeIndexPresent {
		t.Fatalf("state %v, %v", state, err)
	}
}

// EnsureSchema runs at the start of every read command and of the console:
// it must never start the build (minutes on a large index).
func TestTypeIndex_ensureSchemaNeverBuildsIt(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.MustExec(t, db, "ALTER TABLE binlog_events DROP INDEX idx_type_lookup")
	if err := EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	if got := typeIndexKey(t, db); got != "" {
		t.Fatalf("EnsureSchema built idx_type_lookup (%q)", got)
	}
}

// An index from before idx_type_lookup gets it once; a second run (twice, as
// two daemon starts would) finds it and builds nothing.
func TestTypeIndex_existingIndexGetsItOnce(t *testing.T) {
	for name, partitioned := range map[string]bool{"partitioned": true, "unpartitioned": false} {
		t.Run(name, func(t *testing.T) {
			db, _ := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			testutil.MustExec(t, db, "ALTER TABLE binlog_events DROP INDEX idx_type_lookup")
			if !partitioned {
				testutil.MustExec(t, db, "ALTER TABLE binlog_events REMOVE PARTITIONING")
			}
			fillEvents(t, db, 3000)
			for run := range 2 {
				if err := EnsureTypeIndex(context.Background(), db, discardLogger()); err != nil {
					t.Fatalf("run %d: %v", run, err)
				}
				if got := typeIndexKey(t, db); got != wantTypeKey {
					t.Fatalf("run %d: idx_type_lookup = %q, want %q", run, got, wantTypeKey)
				}
			}
			var n int
			if err := db.QueryRow("SELECT COUNT(*) FROM binlog_events FORCE INDEX (idx_type_lookup) WHERE event_type = 3").Scan(&n); err != nil || n != 1000 {
				t.Fatalf("the built index answers %d deletes (%v), want 1000", n, err)
			}
		})
	}
}

// The same key under another name, or our name over another key: nothing is
// built and nothing dropped.
func TestTypeIndex_presentUnderAnotherNameOrShape(t *testing.T) {
	for name, tc := range map[string]struct {
		ddl     string
		state   TypeIndexState
		wantOur string
	}{
		"same key, other name": {"ALTER TABLE binlog_events ADD INDEX my_types (event_type, schema_name, table_name, event_timestamp)", TypeIndexOtherName, ""},
		"our name, other key":  {"ALTER TABLE binlog_events ADD INDEX idx_type_lookup (event_type)", TypeIndexWrongShape, "event_type"},
	} {
		t.Run(name, func(t *testing.T) {
			db, _ := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			testutil.MustExec(t, db, "ALTER TABLE binlog_events DROP INDEX idx_type_lookup")
			testutil.MustExec(t, db, tc.ddl)
			if state, _, err := TypeIndexStatus(context.Background(), db); err != nil || state != tc.state {
				t.Fatalf("state %v (%v), want %v", state, err, tc.state)
			}
			if err := EnsureTypeIndex(context.Background(), db, discardLogger()); err != nil {
				t.Fatal(err)
			}
			if got := typeIndexKey(t, db); got != tc.wantOur {
				t.Fatalf("idx_type_lookup = %q, want %q", got, tc.wantOur)
			}
		})
	}
}

// A build already running on the server (here: one waiting behind a reader's
// metadata lock, the shape of a previous daemon's orphaned ALTER) is seen,
// and no second one starts; the waiting one gives up at the lock-wait bound
// with a retriable error instead of queueing capture behind it for long.
func TestTypeIndex_runningBuildIsNotRepeated(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.MustExec(t, db, "ALTER TABLE binlog_events DROP INDEX idx_type_lookup")
	fillEvents(t, db, 100)

	saved := typeIndexLockWait
	typeIndexLockWait = 3 * time.Second
	t.Cleanup(func() { typeIndexLockWait = saved })

	ctx := context.Background()
	reader, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := reader.QueryRow("SELECT COUNT(*) FROM binlog_events").Scan(&n); err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { first <- EnsureTypeIndex(ctx, db, discardLogger()) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, _, found, err := TypeIndexBuildRunning(ctx, db); err != nil {
			t.Fatal(err)
		} else if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the first build never showed in the process list")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := EnsureTypeIndex(ctx, db, discardLogger()); !errors.Is(err, ErrTypeIndexBuildRunning) {
		t.Fatalf("second EnsureTypeIndex = %v, want ErrTypeIndexBuildRunning", err)
	}
	err = <-first
	var be *TypeIndexBuildError
	var me *mysql.MySQLError
	if !errors.As(err, &be) || !be.Retry || !errors.As(err, &me) || me.Number != 1205 {
		t.Fatalf("the build blocked by the reader = %v, want a retriable 1205", err)
	}
	reader.Rollback()
	if err := EnsureTypeIndex(ctx, db, discardLogger()); err != nil {
		t.Fatal(err)
	}
	if got := typeIndexKey(t, db); got != wantTypeKey {
		t.Fatalf("after the reader ended: %q", got)
	}
}

// Capture keeps writing while the index is built: no insert fails, and none
// waits longer than the bound (measured: under 120 ms at 25 s of build on
// 3M rows).
func TestTypeIndex_captureKeepsWritingDuringTheBuild(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.MustExec(t, db, "ALTER TABLE binlog_events DROP INDEX idx_type_lookup")
	fillEvents(t, db, 200000)

	var stop atomic.Bool
	var wg sync.WaitGroup
	var maxLat atomic.Int64
	var batches atomic.Int64
	insertErr := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		idx := New(db, 50)
		for k := 0; !stop.Load(); k++ {
			evs := make([]event.Event, 50)
			for i := range evs {
				evs[i] = event.Event{BinlogFile: "binlog.000002", StartPos: uint64(k*50 + i), EndPos: uint64(k*50 + i + 1),
					Timestamp: time.Now().UTC(), Schema: "app", Table: "live", EventType: event.EventType(1 + i%3),
					PKValues: fmt.Sprintf("%d-%d", k, i), RowAfter: map[string]any{"k": k}}
			}
			st := time.Now()
			if _, err := idx.InsertBatch(evs); err != nil {
				insertErr <- err
				return
			}
			if l := int64(time.Since(st)); l > maxLat.Load() {
				maxLat.Store(l)
			}
			batches.Add(1)
			time.Sleep(10 * time.Millisecond)
		}
	}()
	time.Sleep(300 * time.Millisecond)
	before := batches.Load()
	st := time.Now()
	err := EnsureTypeIndex(context.Background(), db, discardLogger())
	took := time.Since(st)
	during := batches.Load() - before
	time.Sleep(300 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	select {
	case err := <-insertErr:
		t.Fatalf("an insert failed during the build: %v", err)
	default:
	}
	if err != nil {
		t.Fatal(err)
	}
	if got := typeIndexKey(t, db); got != wantTypeKey {
		t.Fatalf("idx_type_lookup = %q", got)
	}
	t.Logf("build %v, %d insert batches during it, slowest %v", took, during, time.Duration(maxLat.Load()))
	if during == 0 {
		t.Fatalf("no insert completed during the %v build: capture stalled", took)
	}
	if bound := typeIndexLockWait + time.Second; time.Duration(maxLat.Load()) > bound {
		t.Fatalf("slowest insert %v, want under %v", time.Duration(maxLat.Load()), bound)
	}
}
