//go:build integration

package console

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/serverid"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2085 against a real index: "unchanged since the snapshot" is decided by
// the refresh's own fetch, by position, and every state of the index that
// cannot back the claim answers "cannot say". The cases share one index and
// one snapshot; each changes one thing and puts it back.

// unchangedRig is an index with hourly partitions around a snapshot taken
// three days ago, a stream_state that vouches for it, and one table file per
// case with a real footer.
type unchangedRig struct {
	t      *testing.T
	db     *sql.DB
	s      *Server
	b      *bundle
	wm     *fakeWatermark
	root   string
	stamp  time.Time // when the snapshot was written
	anchor query.BinlogPos
	tables atomic.Int32
}

const unchangedSnapshotDir = "2026-04-30T03-20-00Z"

func newUnchangedRig(t *testing.T) *unchangedRig {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.MustExec(t, db, serverid.DDLBintrailServerChanges)
	r := &unchangedRig{t: t, db: db, root: t.TempDir(),
		stamp:  time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Second),
		anchor: query.BinlogPos{File: "binlog.000007", Pos: 4200}}
	// Hourly partitions from two hours before the snapshot's hour to now:
	// the index still holds the whole window. One statement, as the daemon's
	// own index is partitioned.
	r.partitionFrom(r.stamp.Truncate(time.Hour).Add(-2 * time.Hour))
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, events_indexed, last_checkpoint, server_id, capture_skips)
		VALUES (1, 'gtid', 'binlog.000009', 100, '3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50', 0, UTC_TIMESTAMP(), 1, '{}')`)
	r.wm = &fakeWatermark{ago: 5 * time.Second}
	r.s = &Server{cm: newConnManager(nil, false), captureStatus: r.wm}
	r.b = &bundle{db: db, dbName: dbName, engine: query.New(db), baselineSrc: r.root}
	r.s.cm.boot = r.b
	return r
}

// partitionFrom re-partitions the (still small) events table hourly from
// first up to the current hour, plus p_future.
func (r *unchangedRig) partitionFrom(first time.Time) {
	r.t.Helper()
	var parts []string
	for h := first.UTC().Truncate(time.Hour); !h.After(time.Now().UTC()); h = h.Add(time.Hour) {
		parts = append(parts, fmt.Sprintf("PARTITION p_%s VALUES LESS THAN (TO_SECONDS('%s'))",
			h.Format("2006010215"), h.Add(time.Hour).Format("2006-01-02 15:04:05")))
	}
	parts = append(parts, "PARTITION p_future VALUES LESS THAN MAXVALUE")
	testutil.MustExec(r.t, r.db, "ALTER TABLE binlog_events PARTITION BY RANGE (TO_SECONDS(event_timestamp)) ("+strings.Join(parts, ", ")+")")
}

// table writes one table file whose footer vouches for it, changed by edit,
// and returns it as the views would name it. Every case gets its own table,
// so one case's events never answer for another's.
func (r *unchangedRig) table(edit func(md map[string]string)) views.BaselineTable {
	r.t.Helper()
	name := "t" + strconv.Itoa(int(r.tables.Add(1)))
	ddl := "CREATE TABLE `" + name + "` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	stamp := r.stamp.Format(time.RFC3339)
	md := map[string]string{
		baseline.MetaKeyBinlogFile: r.anchor.File, baseline.MetaKeyBinlogPos: strconv.FormatUint(r.anchor.Pos, 10),
		baseline.MetaKeySnapshotTimestamp: stamp, baseline.MetaKeyLastDumpAt: stamp, baseline.MetaKeyFoldGeneration: "0",
		baseline.MetaKeySnapshotProducer: baseline.ProducerDump, baseline.MetaKeyLockMode: string(baseline.LockModeFTWRL),
		baseline.MetaKeyCreateTableSQL: ddl,
	}
	if edit != nil {
		edit(md)
	}
	path := filepath.Join(r.root, unchangedSnapshotDir, "shop", name+".parquet")
	r.write(path, ddl, md)
	return views.BaselineTable{Schema: "shop", Table: name, Path: path}
}

func (r *unchangedRig) write(path, ddl string, md map[string]string) {
	r.t.Helper()
	cols, err := baseline.ParseSchemaText(ddl)
	if err != nil {
		r.t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: md})
	if err != nil {
		r.t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1", "new"}, []bool{false, false}); err != nil {
		r.t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		r.t.Fatal(err)
	}
}

// event indexes one row event of schema.table at a binlog coordinate and an
// execution time.
func (r *unchangedRig) event(schema, table, file string, pos uint64, at time.Time) {
	r.t.Helper()
	testutil.InsertEvent(r.t, r.db, file, pos, pos+50, at.UTC().Format("2006-01-02 15:04:05"), nil,
		schema, table, 2, "1", []byte(`["status"]`), []byte(`{"id":1,"status":"new"}`), []byte(`{"id":1,"status":"paid"}`))
}

func (r *unchangedRig) ask(tables ...views.BaselineTable) string {
	r.t.Helper()
	return r.s.copyUnchanged(context.Background(), r.b, bootServerID, tables, time.Minute)
}

func (r *unchangedRig) wantUnchanged(tables ...views.BaselineTable) {
	r.t.Helper()
	if why := r.ask(tables...); why != "" {
		r.t.Errorf("not vouched for: %s; want unchanged", why)
	}
}

func (r *unchangedRig) wantNot(want string, tables ...views.BaselineTable) {
	r.t.Helper()
	if why := r.ask(tables...); why == "" || !strings.Contains(why, want) {
		r.t.Errorf("answer = %q, want a refusal saying %q", why, want)
	}
}

func TestIntegrationCopyUnchanged_2085(t *testing.T) {
	r := newUnchangedRig(t)
	after := r.stamp.Add(time.Hour)

	t.Run("a table with no event at all", func(t *testing.T) {
		r.wantUnchanged(r.table(nil))
	})
	t.Run("events of the table before its position, however recent their time", func(t *testing.T) {
		tb := r.table(nil)
		// Same file below the position, and an earlier file with a larger
		// offset: both are in the snapshot already. Their execution times are
		// inside the window the lookup scans, so only the position rules
		// them out.
		r.event("shop", tb.Table, r.anchor.File, r.anchor.Pos-100, r.stamp.Add(-10*time.Minute))
		r.event("shop", tb.Table, "binlog.000006", 999999, r.stamp.Add(10*time.Minute))
		r.wantUnchanged(tb)
	})
	t.Run("an event exactly at the position is a change", func(t *testing.T) {
		tb := r.table(nil)
		r.event("shop", tb.Table, r.anchor.File, r.anchor.Pos, after)
		r.wantNot("shop."+tb.Table+" changed since its snapshot", tb)
	})
	t.Run("an event in a later binlog file is a change", func(t *testing.T) {
		tb := r.table(nil)
		r.event("shop", tb.Table, "binlog.000008", 4, after)
		r.wantNot("changed since its snapshot", tb)
	})
	t.Run("a transaction that ran before the snapshot and committed after it", func(t *testing.T) {
		// Its rows carry the time the statement ran, earlier than the
		// snapshot's stamp, and a position after the snapshot's: comparing
		// times would call the table unchanged.
		tb := r.table(nil)
		r.event("shop", tb.Table, r.anchor.File, r.anchor.Pos+900, r.stamp.Add(-20*time.Minute))
		r.wantNot("changed since its snapshot", tb)
	})
	t.Run("events of other tables do not count, a change of one table of several does", func(t *testing.T) {
		quiet, busy := r.table(nil), r.table(nil)
		r.event("shop", busy.Table, "binlog.000008", 500, after)
		r.event("crm", quiet.Table, "binlog.000008", 600, after) // the same name in another schema
		r.wantUnchanged(quiet)
		r.wantNot("shop."+busy.Table+" changed", quiet, busy)
		r.wantNot("shop."+busy.Table+" changed", busy, quiet)
	})
	t.Run("the event-id floor in the footer does not hide a later change", func(t *testing.T) {
		var maxID uint64
		if err := r.db.QueryRow("SELECT COALESCE(MAX(event_id), 0) FROM binlog_events").Scan(&maxID); err != nil {
			t.Fatal(err)
		}
		tb := r.table(func(md map[string]string) { md[baseline.MetaKeyLastEventID] = strconv.FormatUint(maxID, 10) })
		r.wantUnchanged(tb)
		r.event("shop", tb.Table, "binlog.000008", 700, after)
		r.wantNot("changed since its snapshot", tb)
	})
	t.Run("a chain of deltas is cut at its last pair", func(t *testing.T) {
		tb := r.table(nil)
		pairStamp := r.stamp.Add(24 * time.Hour)
		pair := filepath.Join(filepath.Dir(tb.Path), tb.Table+".000001.upserts")
		r.write(pair, "CREATE TABLE `p` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", map[string]string{
			baseline.MetaKeyBinlogFile: "binlog.000008", baseline.MetaKeyBinlogPos: "9000",
			baseline.MetaKeySnapshotTimestamp: pairStamp.Format(time.RFC3339),
			baseline.MetaKeyLastDumpAt:        r.stamp.Format(time.RFC3339), baseline.MetaKeyFoldGeneration: "1",
			baseline.MetaKeySnapshotProducer: baseline.ProducerReconstruct, baseline.MetaKeyLockMode: string(baseline.LockModeFTWRL),
		})
		tb.Delta = true
		tb.DeltaFiles = []baseline.TableDeltaFile{{Seq: 1, SeqLo: 1, Upserts: pair}}
		// After the table file's position and before the pair's: the pair
		// holds it.
		r.event("shop", tb.Table, "binlog.000008", 100, pairStamp.Add(-time.Hour))
		r.wantUnchanged(tb)
		r.event("shop", tb.Table, "binlog.000008", 9000, pairStamp.Add(time.Hour))
		r.wantNot("changed since its snapshot", tb)
	})

	t.Run("schema changes", func(t *testing.T) {
		ddl := func(schema, table, stmt, file string, pos uint64) {
			testutil.MustExec(t, r.db, `INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query) VALUES (?, ?, ?, ?, ?, 'ALTER TABLE', ?)`,
				after.Format("2006-01-02 15:04:05"), file, pos, schema, table, stmt)
		}
		before, truncated, renamedInto, typed, bystander := r.table(nil), r.table(nil), r.table(nil), r.table(nil), r.table(nil)
		// A change no row event carries: TRUNCATE empties the table.
		ddl("shop", truncated.Table, "TRUNCATE TABLE "+truncated.Table, "binlog.000008", 50)
		// Recorded under the OLD name only.
		ddl("shop", "staging", "ALTER TABLE staging RENAME TO "+renamedInto.Table, "binlog.000008", 60)
		// As the statement typed it: another case, and no schema on record.
		ddl("", strings.ToUpper(typed.Table), "alter table "+strings.ToUpper(typed.Table)+" add c int", "binlog.000008", 70)
		// Before the position: already in the snapshot.
		ddl("shop", before.Table, "ALTER TABLE "+before.Table+" ADD c int", r.anchor.File, r.anchor.Pos-10)
		r.wantNot("shop."+truncated.Table+" had a schema change", truncated)
		r.wantNot("shop."+renamedInto.Table+" had a schema change", renamedInto)
		r.wantNot("shop."+typed.Table+" had a schema change", typed)
		r.wantUnchanged(before)
		r.wantUnchanged(bystander)
	})

	// From here on: the table is quiet and the index cannot back the claim.
	quiet := r.table(nil)
	r.wantUnchanged(quiet)
	restore := func(col string) func() {
		return func() { testutil.MustExec(t, r.db, "UPDATE stream_state SET "+col+" WHERE id = 1") }
	}
	t.Run("capture not known to be up to date", func(t *testing.T) {
		defer func() { r.wm.wm, r.wm.ago = CaptureWatermark{}, 5*time.Second }()
		r.wm.wm, r.wm.ago = CaptureWatermark{Detail: "the source did not answer"}, 0
		r.wantNot("the source did not answer", quiet)
		r.wm.ago = 2 * time.Minute
		r.wantNot("more than the limit", quiet)
	})
	t.Run("a table outside what capture records", func(t *testing.T) {
		defer func() { r.wm.wm = CaptureWatermark{} }()
		r.wm.wm.Captures = func(schema, _ string) bool { return schema == "crm" }
		r.wantNot("outside what capture records", quiet)
	})
	t.Run("a binlog gap since the snapshot", func(t *testing.T) {
		defer restore("gap_lost_at = NULL")()
		testutil.MustExec(t, r.db, "UPDATE stream_state SET gap_lost_at = ? WHERE id = 1", after.Format("2006-01-02 15:04:05"))
		r.wantNot("binlog gap", quiet)
	})
	t.Run("a gap older than the snapshot was read again by it", func(t *testing.T) {
		defer restore("gap_lost_at = NULL")()
		testutil.MustExec(t, r.db, "UPDATE stream_state SET gap_lost_at = ? WHERE id = 1", r.stamp.Add(-time.Hour).Format("2006-01-02 15:04:05"))
		r.wantUnchanged(quiet)
	})
	t.Run("events dropped since the snapshot", func(t *testing.T) {
		defer restore("capture_skips = '{}'")()
		testutil.MustExec(t, r.db, "UPDATE stream_state SET capture_skips = ? WHERE id = 1",
			`{"statement_format_dml":{"count":2,"last_at":"`+after.Format(time.RFC3339)+`"}}`)
		r.wantNot("dropped events", quiet)
	})
	t.Run("no capture on record", func(t *testing.T) {
		var gtid string
		if err := r.db.QueryRow("SELECT gtid_set FROM stream_state WHERE id = 1").Scan(&gtid); err != nil {
			t.Fatal(err)
		}
		testutil.MustExec(t, r.db, "DELETE FROM stream_state")
		defer testutil.MustExec(t, r.db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, events_indexed, last_checkpoint, server_id, capture_skips)
			VALUES (1, 'gtid', 'binlog.000009', 100, ?, 0, UTC_TIMESTAMP(), 1, '{}')`, gtid)
		r.wantNot("no live capture", quiet)
	})
	t.Run("the source server changed since the snapshot", func(t *testing.T) {
		defer testutil.MustExec(t, r.db, "DELETE FROM bintrail_server_changes")
		testutil.MustExec(t, r.db, `INSERT INTO bintrail_server_changes (bintrail_id, field_changed, old_value, new_value, detected_at) VALUES ('11111111-2222-3333-4444-555555555555', 'server_uuid', 'a', 'b', ?)`,
			after.Format("2006-01-02 15:04:05"))
		r.wantNot("identity changed", quiet)
	})
	t.Run("a table carried forward from an older snapshot than the index reaches", func(t *testing.T) {
		// Its footer is its own: written a week ago, long before the oldest
		// partition the index still holds. Changes to it may sit in
		// partitions that were rotated away.
		old := r.table(func(md map[string]string) {
			md[baseline.MetaKeySnapshotTimestamp] = r.stamp.Add(-96 * time.Hour).Format(time.RFC3339)
			md[baseline.MetaKeyLastDumpAt] = md[baseline.MetaKeySnapshotTimestamp]
		})
		r.wantNot("no longer holds every change", old)
		// And together with a table the index does cover.
		r.wantNot("no longer holds every change", quiet, old)
	})
	t.Run("a snapshot that cannot be vouched for", func(t *testing.T) {
		r.wantNot("does not record its binlog position", r.table(func(md map[string]string) { delete(md, baseline.MetaKeyBinlogPos) }))
		r.wantNot("its read is torn", r.table(func(md map[string]string) { md[baseline.MetaKeyLockMode] = string(baseline.LockModeNoLock) }))
		r.wantNot("its read is unknown", r.table(func(md map[string]string) { delete(md, baseline.MetaKeyLockMode) }))
		r.wantNot("gap in capture", r.table(func(md map[string]string) { md[baseline.MetaKeyCaptureGap] = "3 files lost" }))
		r.wantNot("foreign key", r.table(func(md map[string]string) {
			md[baseline.MetaKeyCreateTableSQL] = strings.Replace(md[baseline.MetaKeyCreateTableSQL], "PRIMARY KEY (`id`)",
				"PRIMARY KEY (`id`),\n  CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `customers` (`id`) ON DELETE CASCADE", 1)
		}))
		gone := r.table(nil)
		gone.Path += ".missing"
		r.wantNot("could not be read", gone)
	})
	t.Run("the index rotated the window away", func(t *testing.T) {
		// Last: it drops the partitions every other case reads.
		r.partitionFrom(r.stamp.Truncate(time.Hour))
		r.wantNot("no longer holds every change", quiet)
	})
	t.Run("an index that cannot be read", func(t *testing.T) {
		testutil.MustExec(t, r.db, "DROP TABLE bintrail_server_changes")
		r.wantNot("the index could not be read", quiet)
	})
}
