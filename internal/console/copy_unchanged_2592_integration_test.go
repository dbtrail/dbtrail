//go:build integration

package console

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2592 for fast mode: "unchanged since the snapshot" for a table nobody
// writes, last read by a full read three days ago, in a directory a refresh
// published ten minutes ago, after rotation dropped every hour but the last
// two. The lookup starts at the refresh's cut; the schema-change check still
// starts at the table's own, because a refresh carries a table whose
// definition changed without a row event unchanged, old definition and all.
func TestIntegrationCopyUnchanged_staticTableAfterRotation_2592(t *testing.T) {
	r, _ := newTailRig(t)
	now := time.Now().UTC()

	r.change("busy", event.EventUpdate, "1", "paid", "binlog.000009", 450, now.Add(-15*time.Minute))
	var markID uint64
	if err := r.db.QueryRow("SELECT MAX(event_id) FROM binlog_events WHERE table_name = 'busy'").Scan(&markID); err != nil {
		t.Fatal(err)
	}
	mark := reconstruct.EventMark{ID: markID, File: "binlog.000009", End: 500}
	runAt := now.Add(-10 * time.Minute).Truncate(time.Second)
	dir := filepath.Join(r.root, reconstruct.SnapshotDirName(runAt))
	ddlOf := func(name string) string {
		return "CREATE TABLE `" + name + "` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	}
	read := r.stamp.Format(time.RFC3339)
	quiet := views.BaselineTable{Schema: "shop", Table: "t1", Path: filepath.Join(dir, "shop", "t1.parquet")}
	r.write(quiet.Path, ddlOf("t1"), map[string]string{
		baseline.MetaKeyBinlogFile: r.anchor.File, baseline.MetaKeyBinlogPos: strconv.FormatUint(r.anchor.Pos, 10),
		baseline.MetaKeySnapshotTimestamp: read, baseline.MetaKeyLastDumpAt: read, baseline.MetaKeyFoldGeneration: "0",
		baseline.MetaKeySnapshotProducer: baseline.ProducerDump, baseline.MetaKeyLockMode: string(baseline.LockModeFTWRL),
		baseline.MetaKeyCreateTableSQL: ddlOf("t1"),
	})
	if err := os.WriteFile(filepath.Join(dir, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	r.rotateBelow(now.Add(-2 * time.Hour).Truncate(time.Hour))

	// A full read's directory: the table's own cut, whose hours are gone.
	r.wantNot("rotated out", quiet)

	r.write(filepath.Join(dir, "shop", "busy.parquet"), ddlOf("busy"), map[string]string{
		baseline.MetaKeyBinlogFile: "binlog.000009", baseline.MetaKeyBinlogPos: "500",
		baseline.MetaKeyFoldCut:           "binlog.000009:500",
		baseline.MetaKeySnapshotTimestamp: runAt.Format(time.RFC3339),
		baseline.MetaKeySnapshotProducer:  baseline.ProducerReconstruct,
		baseline.MetaKeyEventMark:         mark.Encode(),
		baseline.MetaKeyLockMode:          string(baseline.LockModeFTWRL),
		baseline.MetaKeyCreateTableSQL:    ddlOf("busy"),
	})
	r.s.copyRunCuts = copyRunCutMemo{} // the test rewrote a directory, which a published one never is
	r.wantUnchanged(quiet)

	// A schema change between the table's own cut and the refresh's: the
	// refresh carried the old definition, so the table is still refused.
	testutil.MustExec(t, r.db, `INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query) VALUES (?, ?, ?, ?, ?, 'ALTER TABLE', ?)`,
		now.Add(-time.Hour).Format("2006-01-02 15:04:05"), "binlog.000008", 100, "shop", "t1", "ALTER TABLE t1 ADD COLUMN note varchar(8)")
	r.wantNot("had a schema change", quiet)
	testutil.MustExec(t, r.db, "DELETE FROM schema_changes WHERE table_name = 't1'")
	r.wantUnchanged(quiet)

	// The source's address changed after the table's own read and long
	// before the refresh's cut: a refresh only checks the uuid, so this is
	// still asked from the table's own read.
	testutil.MustExec(t, r.db, `INSERT INTO bintrail_server_changes (bintrail_id, field_changed, old_value, new_value, detected_at)
		VALUES ('21870000-0000-0000-0000-000000000000', 'host', 'a', 'b', FROM_UNIXTIME(?))`, now.Add(-24*time.Hour).Unix())
	r.wantNot("identity changed", quiet)
	testutil.MustExec(t, r.db, "DELETE FROM bintrail_server_changes")
	r.wantUnchanged(quiet)

	// The source's binary log started again after the refresh (a RESET
	// MASTER): the newest change capture recorded sorts before the
	// refresh's cut, so no search by position can rule a change out. The
	// table's own cut, a full read's with no mark, would let that pass; the
	// moved one carries the refresh's mark and is folded, which refuses.
	r.change("other", event.EventInsert, "9", "x", "binlog.000002", 4, now.Add(-time.Minute))
	r.wantNot("started again", quiet)
	testutil.MustExec(t, r.db, "DELETE FROM binlog_events WHERE table_name = 'other'")
	r.wantUnchanged(quiet)

	// A change after the refresh's cut is seen.
	r.change(quiet.Table, event.EventUpdate, "1", "late", "binlog.000009", 900, now.Add(-2*time.Minute))
	r.wantNot("changed since its snapshot", quiet)
}
