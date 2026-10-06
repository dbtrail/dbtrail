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
	// Hourly partitions from eight hours before the snapshot's hour to now:
	// the index still holds the whole window, and some hours before it.
	r.partitionFrom(r.stamp.Truncate(time.Hour).Add(-8 * time.Hour))
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

// lastID is the id of the newest indexed event of shop.table.
func (r *unchangedRig) lastID(table string) string {
	r.t.Helper()
	var id uint64
	if err := r.db.QueryRow("SELECT MAX(event_id) FROM binlog_events WHERE schema_name = 'shop' AND table_name = ?", table).Scan(&id); err != nil {
		r.t.Fatal(err)
	}
	return strconv.FormatUint(id, 10)
}

// rewrite replaces t's file with one whose footer is edited: what a refresh
// writes over a table it already holds.
func (r *unchangedRig) rewrite(t views.BaselineTable, edit func(md map[string]string)) {
	r.t.Helper()
	ddl := "CREATE TABLE `" + t.Table + "` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	stamp := r.stamp.Format(time.RFC3339)
	md := map[string]string{
		baseline.MetaKeyBinlogFile: r.anchor.File, baseline.MetaKeyBinlogPos: strconv.FormatUint(r.anchor.Pos, 10),
		baseline.MetaKeySnapshotTimestamp: stamp, baseline.MetaKeyLastDumpAt: r.stamp.Add(-6 * time.Hour).Format(time.RFC3339), baseline.MetaKeyFoldGeneration: "1",
		baseline.MetaKeySnapshotProducer: baseline.ProducerReconstruct, baseline.MetaKeyLockMode: string(baseline.LockModeFTWRL),
		baseline.MetaKeyCreateTableSQL: ddl,
	}
	edit(md)
	r.write(t.Path, ddl, md)
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
	t.Run("a refresh that ran while capture was hours behind", func(t *testing.T) {
		// The refresh stamps its own clock and cuts where the index stood:
		// at an event that ran five hours before the stamp. What capture
		// indexes afterwards ran between the two, hours below any time
		// derived from the stamp, and it is positioned after the cut.
		tb := r.table(nil)
		r.event("shop", tb.Table, "binlog.000008", 3000, r.stamp.Add(-5*time.Hour))
		r.rewrite(tb, func(md map[string]string) {
			md[baseline.MetaKeyBinlogFile], md[baseline.MetaKeyBinlogPos] = "binlog.000008", "3050"
			md[baseline.MetaKeyLastEventID] = r.lastID(tb.Table)
		})
		r.wantUnchanged(tb)
		r.event("shop", tb.Table, "binlog.000008", 3100, r.stamp.Add(-4*time.Hour))
		r.wantNot("shop."+tb.Table+" changed since its snapshot", tb)
	})
	t.Run("a last folded event that does not place the cut", func(t *testing.T) {
		gone, other, late, never := r.table(nil), r.table(nil), r.table(nil), r.table(nil)
		// No longer in the index (rotated away with its partition).
		r.rewrite(gone, func(md map[string]string) { md[baseline.MetaKeyLastEventID] = "999999999" })
		r.wantNot("no longer holds the last change the snapshot of shop."+gone.Table, gone)
		// In the index, and another table's: this footer is not this index's.
		r.event("shop", "elsewhere", r.anchor.File, r.anchor.Pos-300, r.stamp.Add(-time.Hour))
		r.rewrite(other, func(md map[string]string) { md[baseline.MetaKeyLastEventID] = r.lastID("elsewhere") })
		r.wantNot("another table's", other)
		// Positioned after the cut it is supposed to be before.
		r.event("shop", late.Table, "binlog.000008", 200, after)
		r.rewrite(late, func(md map[string]string) { md[baseline.MetaKeyLastEventID] = r.lastID(late.Table) })
		r.wantNot("not before its binlog position", late)
		// Rewritten by a refresh that never folded an event into it: only
		// the dump it descends from dates it, and that one is older than
		// the index reaches.
		r.rewrite(never, func(md map[string]string) {
			md[baseline.MetaKeyLastDumpAt] = r.stamp.Add(-96 * time.Hour).Format(time.RFC3339)
		})
		r.wantNot("no longer holds every change", never)
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
		// The pair's position is a refresh's cut: the end of an event the
		// index holds (#2160 reads the newest event against it).
		r.event("shop", "elsewhere", "binlog.000008", 8950, pairStamp.Add(-time.Minute))
		r.wantUnchanged(tb)
		r.event("shop", tb.Table, "binlog.000008", 9000, pairStamp.Add(time.Hour))
		r.wantNot("changed since its snapshot", tb)
	})

	t.Run("schema changes", func(t *testing.T) {
		ddlAt := func(at time.Time, schema, table, stmt, file string, pos uint64) {
			testutil.MustExec(t, r.db, `INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query) VALUES (?, ?, ?, ?, ?, 'ALTER TABLE', ?)`,
				at.Format("2006-01-02 15:04:05"), file, pos, schema, table, stmt)
		}
		ddl := func(schema, table, stmt, file string, pos uint64) { ddlAt(after, schema, table, stmt, file, pos) }
		before, truncated, renamedInto, typed, bystander, slow := r.table(nil), r.table(nil), r.table(nil), r.table(nil), r.table(nil), r.table(nil)
		// An ALTER that started six hours before the snapshot and finished
		// after it: its row carries the time it STARTED, far below any time
		// floor, and a position after the snapshot's.
		ddlAt(r.stamp.Add(-6*time.Hour), "shop", slow.Table, "ALTER TABLE "+slow.Table+" MODIFY status varchar(64)", "binlog.000008", 40)
		r.wantNot("shop."+slow.Table+" had a schema change", slow)
		// A change no row event carries: TRUNCATE empties the table.
		ddl("shop", truncated.Table, "TRUNCATE TABLE "+truncated.Table, "binlog.000008", 50)
		// Recorded under the OLD name only.
		ddl("shop", "staging", "ALTER TABLE staging RENAME TO "+renamedInto.Table, "binlog.000008", 60)
		// As the statement typed it: another case, and no schema on record.
		ddl("", strings.ToUpper(typed.Table), "alter table "+strings.ToUpper(typed.Table)+" add c int", "binlog.000008", 70)
		// Before the position: already in the snapshot.
		ddl("shop", before.Table, "ALTER TABLE "+before.Table+" ADD c int", r.anchor.File, r.anchor.Pos-10)
		// A row with no position at all cannot be placed before anything.
		unplaced := r.table(nil)
		ddl("shop", unplaced.Table, "ALTER TABLE "+unplaced.Table+" ADD c int", "", 0)
		r.wantNot("shop."+unplaced.Table+" had a schema change", unplaced)
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
	t.Run("a capture reset to position mode since the watermark was proven", func(t *testing.T) {
		// The reporter still remembers a watermark (its next read of the
		// source is up to 30 seconds away); the index already says the
		// capture is not the one it was proven for.
		defer restore("mode = 'gtid'")()
		testutil.MustExec(t, r.db, "UPDATE stream_state SET mode = 'position' WHERE id = 1")
		r.wantNot("not in GTID mode", quiet)
	})
	t.Run("a capture restarted from an earlier point, still in GTID mode", func(t *testing.T) {
		// The reporter's watermark stands on a GTID set; the index's saved
		// set is handed to it on every statement.
		defer func() { r.wm.wm = CaptureWatermark{} }()
		var asked string
		r.wm.wm.StillHolds = func(saved string) bool { asked = saved; return false }
		r.wantNot("restarted from an earlier point", quiet)
		if asked != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50" {
			t.Errorf("the saved set handed over = %q, want the index's stream_state.gtid_set", asked)
		}
		r.wm.wm.StillHolds = func(string) bool { return true }
		r.wantUnchanged(quiet)
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
	t.Run("a lookup the index cannot serve is not an empty answer", func(t *testing.T) {
		// Without the index the lookup is forced onto, MySQL refuses the
		// statement: no rows came back, and that must not read as "no
		// change".
		testutil.MustExec(t, r.db, "ALTER TABLE binlog_events DROP INDEX idx_row_lookup")
		r.wantNot("could not be read (the events of shop."+quiet.Table+")", quiet)
		testutil.MustExec(t, r.db, "ALTER TABLE binlog_events ADD INDEX idx_row_lookup (schema_name, table_name, event_timestamp)")
		r.wantUnchanged(quiet)
		testutil.MustExec(t, r.db, "RENAME TABLE schema_changes TO schema_changes_gone")
		r.wantNot("could not be read (the schema changes)", quiet)
		testutil.MustExec(t, r.db, "RENAME TABLE schema_changes_gone TO schema_changes")
		r.wantUnchanged(quiet)
	})
	t.Run("more schema changes than one question reads", func(t *testing.T) {
		// None of them names the table. Reading only the first ones and
		// calling the rest clean would be a guess.
		testutil.MustExec(t, r.db, "SET SESSION cte_max_recursion_depth = 5000")
		testutil.MustExec(t, r.db, `INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
			WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?)
			SELECT /*+ SET_VAR(cte_max_recursion_depth = 5000) */ ?, 'binlog.000008', 2000 + i, 'crm', CONCAT('churn', i), 'ALTER TABLE', CONCAT('ALTER TABLE churn', i, ' ADD c int') FROM n`,
			copyDDLScanMax+1, after.Format("2006-01-02 15:04:05"))
		r.wantNot("too many schema changes", quiet)
		testutil.MustExec(t, r.db, "DELETE FROM schema_changes WHERE schema_name = 'crm' AND table_name LIKE 'churn%'")
		r.wantUnchanged(quiet)
	})
	t.Run("the index rotated the window away", func(t *testing.T) {
		// Last: it drops the partitions every other case reads.
		r.partitionFrom(r.stamp.Truncate(time.Hour))
		r.wantNot("no longer holds", quiet)
	})
	t.Run("an index that cannot be read", func(t *testing.T) {
		testutil.MustExec(t, r.db, "DROP TABLE bintrail_server_changes")
		r.wantNot("the index could not be read", quiet)
	})
}

// A statement that starts long before the snapshot and commits after it
// (#2085 review): a nightly DELETE that begins at 00:30, a snapshot at 02:00,
// the commit at 03:00. Its rows are positioned after the snapshot's cut and
// dated with the time the statement STARTED, more than an hour before the
// floor a search by position starts from, so that search never reaches them.
//
// The events are indexed here in binlog order, as a stream indexes them: the
// one row that decides whether the older hours need searching relies on it.
func TestIntegrationCopyUnchanged_startedLongBeforeCommittedAfter_2085(t *testing.T) {
	r := newUnchangedRig(t)
	floor := r.stamp.Truncate(time.Hour).Add(-time.Hour) // where the search by position starts for a file read at r.stamp
	longAgo := floor.Add(-30 * time.Minute)

	// Before the cut, in binlog order: a table whose only events are old
	// and already in its snapshot, and a table a refresh folded once.
	history, nightly, folded, bystander := r.table(nil), r.table(nil), r.table(nil), r.table(nil)
	r.event("shop", history.Table, "binlog.000006", 100, floor.Add(-3*time.Hour))
	r.event("shop", history.Table, "binlog.000006", 200, floor.Add(-2*time.Hour))
	r.event("shop", folded.Table, "binlog.000006", 300, floor.Add(-2*time.Hour))
	// The refresh that wrote folded cut at the end of an event the index
	// holds, as every refresh does (#2160 reads the newest event against it).
	r.event("shop", "cut", r.anchor.File, r.anchor.Pos-50, floor.Add(-90*time.Minute))
	r.rewrite(folded, func(md map[string]string) { md[baseline.MetaKeyLastEventID] = r.lastID(folded.Table) })
	foldedFloor := floor.Add(-2 * time.Hour).Truncate(time.Hour).Add(-time.Hour) // its search starts from its last folded event

	// Nothing straddles the cut yet: all four are unchanged, old events and
	// all.
	r.wantUnchanged(history)
	r.wantUnchanged(nightly, folded, bystander)

	// The long statements commit: positioned after the cut, dated before
	// each table's floor. Indexed last, like anything that commits last.
	r.event("shop", nightly.Table, r.anchor.File, r.anchor.Pos+900, longAgo)
	r.wantNot("shop."+nightly.Table+" changed since its snapshot", nightly)
	r.wantNot("shop."+nightly.Table+" changed since its snapshot", bystander, nightly)
	// With an event-id floor in the footer (a file a refresh wrote).
	r.wantUnchanged(folded)
	r.event("shop", folded.Table, r.anchor.File, r.anchor.Pos+1900, foldedFloor.Add(-30*time.Minute))
	r.wantNot("shop."+folded.Table+" changed since its snapshot", folded)

	// The older hours are now searched for every table, by position: old
	// events that sit before the cut are still not a change, and a table
	// with no event at all is still unchanged.
	r.wantUnchanged(history)
	r.wantUnchanged(bystander)

	// `bintrail index` loads OLDER binlog files into this index: old dates,
	// old positions, and the newest ids. "The event with the highest id" is
	// now one of those, before every cut, and says nothing about the long
	// statements above, which must still be found. (The verdicts already
	// reached are forgotten first, as a restarted daemon forgets them.)
	r.event("shop", history.Table, "binlog.000006", 250, floor.Add(-90*time.Minute))
	r.event("shop", history.Table, "binlog.000006", 260, foldedFloor.Add(-90*time.Minute))
	testutil.MustExec(t, r.db, `INSERT INTO index_state (binlog_file, file_size, last_position, events_indexed, status, started_at, completed_at)
		VALUES ('binlog.000006', 1000, 1000, 2, 'completed', UTC_TIMESTAMP(), UTC_TIMESTAMP())`)
	r.s.copyChanged.seen = nil
	r.wantNot("shop."+nightly.Table+" changed since its snapshot", nightly)
	r.wantNot("shop."+folded.Table+" changed since its snapshot", folded)
	r.wantUnchanged(history)
	r.wantUnchanged(bystander)
	// An index_state that cannot be read is not "no file was ever indexed".
	r.s.copyChanged.seen = nil
	testutil.MustExec(t, r.db, "RENAME TABLE index_state TO index_state_gone")
	r.wantNot("shop."+nightly.Table+" changed since its snapshot", nightly)
}

// "Changed after this cut" cannot become false again, so it is remembered per
// table and cut and the index is not asked a second time: the search of the
// older hours can take the whole budget on every statement otherwise.
// "Unchanged" is never remembered.
func TestIntegrationCopyUnchanged_changedIsRemembered_2085(t *testing.T) {
	r := newUnchangedRig(t)
	tb, other := r.table(nil), r.table(nil)
	r.wantUnchanged(tb)
	r.wantUnchanged(tb) // and asked again: nothing was remembered
	r.event("shop", tb.Table, "binlog.000008", 500, r.stamp.Add(time.Hour))
	r.wantNot("shop."+tb.Table+" changed since its snapshot", tb)
	// The index can no longer say so, and is not asked.
	testutil.MustExec(t, r.db, "DELETE FROM binlog_events WHERE table_name = ?", tb.Table)
	r.wantNot("shop."+tb.Table+" changed since its snapshot", tb)
	r.wantNot("shop."+tb.Table+" changed since its snapshot", other, tb)
	r.wantUnchanged(other)
	// The same name on another server is another table.
	if why := r.s.copyChanged.get("another-server", tb, r.anchor); why != "" {
		t.Errorf("remembered for a server it was not decided for: %s", why)
	}
	// A newer file of the table has a new cut: asked afresh.
	r.rewrite(tb, func(md map[string]string) {
		md[baseline.MetaKeyBinlogFile], md[baseline.MetaKeyBinlogPos] = "binlog.000008", "900"
		md[baseline.MetaKeyLastDumpAt] = md[baseline.MetaKeySnapshotTimestamp]
	})
	r.wantUnchanged(tb)
}
