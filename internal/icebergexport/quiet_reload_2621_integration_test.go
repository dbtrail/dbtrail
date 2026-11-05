//go:build integration

package icebergexport

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2621 against a real index. The index holds events up to binlog.000001:400
// (seedFirstWindow, table orders); a second table, plain, is loaded from a
// baseline dumped later, anchored at binlog.000001:900, with nothing indexed
// after it. That cut (the newest indexed event) is before the anchor, the
// shape a reset or restored index also has; the stream checkpoint decides.
func quietReloadFixture(t *testing.T) (fixture, Config) {
	t.Helper()
	f := seedFixture(t)
	f.seedFirstWindow(t)
	const createSQL = "CREATE TABLE `plain` (\n  `id` int NOT NULL,\n  `v` varchar(10) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	insertSnapshotTyped(t, f.db, 1, f.base, f.schema, "plain", "id", 1, "PRI", "int", "int", "NO")
	insertSnapshotTyped(t, f.db, 1, f.base, f.schema, "plain", "v", 2, "", "varchar", "varchar(10)", "YES")
	writeBaseline(t, f.baseDir, f.base, f.schema, "plain", createSQL, [][]string{{"1", "a"}},
		map[string]string{baseline.MetaKeyBinlogFile: "binlog.000001", baseline.MetaKeyBinlogPos: "900"})
	cfg := f.config(t.TempDir(), f.base.Add(20*time.Minute))
	cfg.Tables = []string{f.schema + ".plain"}
	return f, cfg
}

func setCheckpoint(t *testing.T, f fixture, file string, pos uint64) {
	t.Helper()
	testutil.MustExec(t, f.db, `INSERT INTO stream_state
		(id, mode, binlog_file, binlog_position, gtid_set, events_indexed, last_checkpoint, server_id)
		VALUES (1, 'position', ?, ?, '', 3, NOW(), 1)
		ON DUPLICATE KEY UPDATE binlog_file = VALUES(binlog_file), binlog_position = VALUES(binlog_position)`, file, pos)
}

func TestIntegrationExport_quietReload2621_loadsThenFoldsTheFirstChange(t *testing.T) {
	f, cfg := quietReloadFixture(t)
	setCheckpoint(t, f, "binlog.000001", 950)

	o := runOne(t, cfg)
	if o.Verdict != VerdictLoaded || o.RowsLoaded != 1 || !strings.Contains(o.Detail, "nothing was indexed after the baseline anchor") {
		t.Fatalf("run 1 = %s rows=%d (%s), want loaded with the nothing-indexed-yet note", o.Verdict, o.RowsLoaded, o.Detail)
	}
	if got := storedCursor(t, cfg.Warehouse, f.schema, "plain"); got != o.Cursor || !strings.HasPrefix(got, "binlog.000001:900 ") {
		t.Fatalf("stored cursor = %s, outcome cursor = %s, want the anchor (binlog.000001:900)", got, o.Cursor)
	}

	// Still quiet: not a refusal, and the cursor stays on the anchor.
	cfg.At = f.base.Add(30 * time.Minute)
	o = runOne(t, cfg)
	if o.Verdict == VerdictRefusedGap || o.Verdict == VerdictRefused || o.Events != 0 {
		t.Fatalf("run 2 = %s events=%d (%s), want no refusal and nothing folded", o.Verdict, o.Events, o.Detail)
	}
	if got := storedCursor(t, cfg.Warehouse, f.schema, "plain"); !strings.HasPrefix(got, "binlog.000001:900 ") {
		t.Fatalf("cursor moved on a quiet run: %s", got)
	}

	// The first change after the dump arrives: it is folded, from the anchor.
	testutil.InsertEvent(t, f.db, "binlog.000001", 1000, 1100, f.base.Add(35*time.Minute).Format("2006-01-02 15:04:05"), nil,
		f.schema, "plain", 1, "2", nil, nil, []byte(`{"id":2,"v":"b"}`))
	setCheckpoint(t, f, "binlog.000001", 1100)
	cfg.At = f.base.Add(40 * time.Minute)
	o = runOne(t, cfg)
	if o.Verdict != VerdictExported || o.Events != 1 || o.Upserts != 1 {
		t.Fatalf("run 3 = %s events=%d upserts=%d (%s), want the first change exported", o.Verdict, o.Events, o.Upserts, o.Detail)
	}
	ddb := openDuckDBIceberg(t)
	var n int
	if err := ddb.QueryRow("SELECT count(*) FROM iceberg_scan('" + o.Location + "')").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("rows after run 3 = %d, want 2 (the baseline row and the new one)", n)
	}
}

func TestIntegrationExport_quietReload2621_stillRefusesWithoutProof(t *testing.T) {
	cases := []struct {
		name       string
		checkpoint func(t *testing.T, f fixture)
		why        string // the refusal names why nothing could be shown
	}{
		{"capture behind the anchor", func(t *testing.T, f fixture) { setCheckpoint(t, f, "binlog.000001", 500) },
			"checkpoint binlog.000001:500 is not at or past the anchor"},
		{"no stream checkpoint", func(t *testing.T, f fixture) {},
			"no stream checkpoint to compare"},
		{"checkpoint under another base name", func(t *testing.T, f fixture) { setCheckpoint(t, f, "mysql-bin.000002", 4) },
			"checkpoint mysql-bin.000002:4 is not at or past the anchor under the same binlog name"},
		{"checkpoint with no position", func(t *testing.T, f fixture) { setCheckpoint(t, f, "", 0) },
			"records no binlog position"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, cfg := quietReloadFixture(t)
			tc.checkpoint(t, f)
			o := runOne(t, cfg)
			if o.Verdict != VerdictRefusedGap || !strings.Contains(o.Detail, "is before") || !strings.Contains(o.Detail, tc.why) {
				t.Fatalf("verdict = %s (%s), want refused-gap naming the cut before the cursor and %q", o.Verdict, o.Detail, tc.why)
			}
		})
	}
}
