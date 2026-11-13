package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/icebergexport"
)

func resetExportGlobals(t *testing.T) {
	t.Helper()
	sDSN, sDir, sS3, sWh, sTables, sAt, sBatch, sFormat, sEvery, sCompact := eiIndexDSN, eiBaselineDir, eiBaselineS3, eiWarehouse, eiTables, eiAt, eiFetchBatch, eiFormat, eiEvery, eiCompact
	t.Cleanup(func() {
		eiIndexDSN, eiBaselineDir, eiBaselineS3, eiWarehouse, eiTables, eiAt, eiFetchBatch, eiFormat, eiEvery, eiCompact = sDSN, sDir, sS3, sWh, sTables, sAt, sBatch, sFormat, sEvery, sCompact
		exportIcebergCmd.SetOut(nil)
	})
	eiIndexDSN, eiBaselineDir, eiBaselineS3, eiWarehouse, eiTables, eiAt, eiFetchBatch, eiFormat, eiEvery, eiCompact = "", "", "", "", "", "", 0, "text", "", defaultCompactAfter
}

// TestRunExportIceberg_flagValidation: each refusal fires before anything
// is opened, and the positive control proves the checks were PASSED and the
// run reached the index (an unreachable DSN fails on connect, not on a flag).
func TestRunExportIceberg_flagValidation(t *testing.T) {
	valid := func() {
		eiIndexDSN = "u:p@tcp(127.0.0.1:1)/db?timeout=200ms"
		eiBaselineDir = t.TempDir()
		eiBaselineS3 = ""
		eiWarehouse = t.TempDir()
		eiTables = "shop.orders"
		eiAt = ""
		eiFetchBatch = 0
		eiFormat = "text"
	}
	cases := []struct {
		name string
		mut  func()
		want string
	}{
		{"no dsn", func() { eiIndexDSN = "" }, "--index-dsn is required"},
		{"no baseline", func() { eiBaselineDir = "" }, "one of --baseline-dir or --baseline-s3"},
		{"both baselines", func() { eiBaselineS3 = "s3://b/p/" }, "mutually exclusive"},
		{"no warehouse", func() { eiWarehouse = "" }, "--warehouse is required"},
		{"bad format", func() { eiFormat = "yaml" }, "--format must be text or json"},
		{"negative batch", func() { eiFetchBatch = -1 }, "--fetch-batch-size must be >= 0"},
		{"negative compact-after", func() { eiCompact = -1 }, "--compact-after must be >= 0"},
		{"bad at", func() { eiAt = "yesterday-ish" }, "--at"},
		{"table without schema", func() { eiTables = "orders" }, "must be schema.table"},
		{"tables all blank", func() { eiTables = " , " }, "no entries"},
		{"positive control: connect refused", func() {}, "connect to index"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetExportGlobals(t)
			valid()
			tc.mut()
			exportIcebergCmd.SetContext(context.Background())
			exportIcebergCmd.SetOut(&bytes.Buffer{})
			err := runExportIceberg(exportIcebergCmd, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestWriteExportJSON_shape(t *testing.T) {
	at := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	err := writeExportJSON(&buf, []icebergexport.Outcome{
		{Schema: "shop", Table: "orders", Verdict: icebergexport.VerdictExported, Detail: "3 events", Events: 3, Upserts: 2, Deletes: 1, SnapshotID: i64(42), Cursor: "binlog.000001:700 at 2026-08-28T12:00:00Z", Location: "/w/shop/orders"},
		{Schema: "shop", Table: "customers", Verdict: icebergexport.VerdictRefusedGap, Detail: "gap"},
		{Schema: "shop", Table: "empty", Verdict: icebergexport.VerdictLoaded, Detail: "0 rows", Cursor: "binlog.000001:100 at 2026-08-28T11:00:00Z", Location: "/w/shop/empty"},
	}, "hadoop", "/w", at, false)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, buf.String())
	}
	if got["catalog"] != "hadoop" || got["warehouse"] != "/w" || got["at"] != "2026-08-28T12:00:00Z" {
		t.Fatalf("top level = %v", got)
	}
	tables := got["tables"].([]any)
	if len(tables) != 3 {
		t.Fatalf("tables = %d", len(tables))
	}
	first := tables[0].(map[string]any)
	for _, k := range []string{"schema", "table", "verdict", "detail", "rows_loaded", "events", "upserts", "deletes", "snapshot_id", "cursor", "location"} {
		if _, ok := first[k]; !ok {
			t.Errorf("exported table lacks key %q: %v", k, first)
		}
	}
	if first["verdict"] != "exported" || first["events"].(float64) != 3 || first["snapshot_id"].(float64) != 42 {
		t.Fatalf("first = %v", first)
	}
	second := tables[1].(map[string]any)
	if second["verdict"] != "refused-gap" {
		t.Fatalf("second = %v", second)
	}
	if _, ok := second["snapshot_id"]; ok {
		t.Fatal("a refused table must not carry a snapshot_id")
	}
	// A zero-row load is a loaded table with a cursor and a location and
	// no snapshot: the key is absent, not 0 (#1509).
	third := tables[2].(map[string]any)
	if third["verdict"] != "loaded" || third["rows_loaded"].(float64) != 0 || third["cursor"] != "binlog.000001:100 at 2026-08-28T11:00:00Z" || third["location"] != "/w/shop/empty" {
		t.Fatalf("third = %v", third)
	}
	if _, ok := third["snapshot_id"]; ok {
		t.Fatalf("a loaded table without a snapshot must not carry a snapshot_id: %v", third)
	}
}

func i64(n int64) *int64 { return &n }

// TestWriteExportJSON_compaction: a table the run compacted carries a
// "compaction" object; one it did not compact carries none; one whose
// compaction was not applied says so, with no rows or files after.
func TestWriteExportJSON_compaction(t *testing.T) {
	var buf bytes.Buffer
	err := writeExportJSON(&buf, []icebergexport.Outcome{
		{Schema: "shop", Table: "a", Verdict: icebergexport.VerdictExported, SnapshotID: i64(7),
			Compaction: &icebergexport.Compaction{DataFilesBefore: 11, DeleteFilesBefore: 10, DataFilesAfter: 1, Rows: 500, SnapshotID: i64(7)}},
		{Schema: "shop", Table: "b", Verdict: icebergexport.VerdictUnchanged},
		{Schema: "shop", Table: "c", Verdict: icebergexport.VerdictExported, SnapshotID: i64(3),
			Compaction: &icebergexport.Compaction{DataFilesBefore: 11, DeleteFilesBefore: 10, Err: icebergexport.ErrConcurrentCommit}},
	}, "hadoop", "/w", time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC), true)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Tables []map[string]any `json:"tables"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	a := got.Tables[0]["compaction"].(map[string]any)
	if a["status"] != "applied" || a["data_files_before"].(float64) != 11 || a["delete_files_before"].(float64) != 10 ||
		a["data_files_after"].(float64) != 1 || a["rows"].(float64) != 500 || a["snapshot_id"].(float64) != 7 {
		t.Fatalf("compacted table = %v", a)
	}
	if _, ok := a["error"]; ok {
		t.Fatalf("a compaction that committed carries an error: %v", a)
	}
	if _, ok := got.Tables[1]["compaction"]; ok {
		t.Fatalf("a table the run did not compact carries a compaction: %v", got.Tables[1])
	}
	c := got.Tables[2]["compaction"].(map[string]any)
	if c["status"] != "not_applied" || c["error"] == nil || c["rows"].(float64) != 0 || c["data_files_after"].(float64) != 0 {
		t.Fatalf("refused compaction = %v", c)
	}
	if _, ok := c["snapshot_id"]; ok {
		t.Fatalf("a refused compaction names a snapshot: %v", c)
	}
}

// TestExportIceberg_compactAfterDefault: the flag's default is the constant
// the help and the docs name, and it reaches the run's config.
func TestExportIceberg_compactAfterDefault(t *testing.T) {
	f := exportIcebergCmd.Flags().Lookup("compact-after")
	if f == nil || f.DefValue != strconv.Itoa(defaultCompactAfter) {
		t.Fatalf("--compact-after = %+v, want default %d", f, defaultCompactAfter)
	}
	if !strings.Contains(exportIcebergCmd.Long, "--compact-after delete files (default "+strconv.Itoa(defaultCompactAfter)+")") {
		t.Fatal("the help does not name the default")
	}
	for _, n := range []int{0, defaultCompactAfter, 3} {
		resetExportGlobals(t)
		eiIndexDSN, eiBaselineDir, eiWarehouse, eiTables, eiCompact = "u:p@tcp(127.0.0.1:1)/db", t.TempDir(), t.TempDir(), "shop.orders", n
		exportIcebergCmd.SetContext(context.Background())
		job, _, err := prepareExport(exportIcebergCmd)
		if err != nil {
			t.Fatal(err)
		}
		if job.cfg.CompactAfter != n {
			t.Fatalf("--compact-after %d reached the run as %d", n, job.cfg.CompactAfter)
		}
	}
}

// TestAuditIcebergCommit_snapshotIDOnlyWhenPresent: the audit detail names
// the snapshot when the commit left one and carries no snapshot_id key at
// all when it did not. A zero-row first load is the case: it still audits
// (a table and a cursor were written) with rows 0 (#1509).
func TestAuditIcebergCommit_snapshotIDOnlyWhenPresent(t *testing.T) {
	rec := audittest.Install(t)
	ctx := context.Background()
	auditIcebergCommit(ctx, icebergexport.Commit{Schema: "shop", Table: "empty", Kind: icebergexport.CommitLoad, Rows: 0,
		Cursor: "binlog.000001:100 at 2026-08-28T11:00:00Z", Location: "/w/shop/empty"})
	auditIcebergCommit(ctx, icebergexport.Commit{Schema: "shop", Table: "orders", Kind: icebergexport.CommitLoad, Rows: 2,
		SnapshotID: i64(7), Cursor: "binlog.000001:100 at 2026-08-28T11:00:00Z", Location: "/w/shop/orders"})
	evs := rec.Events()
	if len(evs) != 2 {
		t.Fatalf("events = %d, want 2: %+v", len(evs), evs)
	}
	empty, orders := evs[0].Detail, evs[1].Detail
	if _, ok := empty["snapshot_id"]; ok {
		t.Fatalf("zero-row load detail carries snapshot_id: %v", empty)
	}
	if empty["commit"] != "load" || empty["rows"] != "0" || empty["cursor"] == "" || empty["location"] == "" {
		t.Fatalf("zero-row load detail = %v, want commit load, rows 0, a cursor and a location", empty)
	}
	if orders["snapshot_id"] != "7" {
		t.Fatalf("load with a snapshot detail = %v, want snapshot_id 7", orders)
	}
}

func TestExportIceberg_envReachesWarehouseFlag(t *testing.T) {
	resetExportGlobals(t)
	dir := t.TempDir()
	t.Setenv("BINTRAIL_ICEBERG_WAREHOUSE", dir)
	bindCommandEnv(exportIcebergCmd)
	if eiWarehouse != dir {
		t.Fatalf("eiWarehouse = %q, want the env value %q", eiWarehouse, dir)
	}
}

// TestRunExportIceberg_refusesURLWarehouse: an object-store or file:// URL
// is refused before baseline discovery (with no --tables, a missed check
// would surface as "no tables to export" instead), and nothing is created
// in the working directory. Before #2552 the value became the relative
// directory "./s3:/b/p".
func TestRunExportIceberg_refusesURLWarehouse(t *testing.T) {
	cases := []struct{ wh, want string }{
		{"s3://b/p", "object-store URL"},
		{"S3://B/p", "object-store URL"},
		{"gs://x", "object-store URL"},
		{"file:///tmp/x", "file:// URL"},
		{"   ", "--warehouse is required"},
	}
	for _, tc := range cases {
		t.Run(tc.wh, func(t *testing.T) {
			resetExportGlobals(t)
			wd := t.TempDir()
			t.Chdir(wd)
			eiIndexDSN = "u:p@tcp(127.0.0.1:1)/db?timeout=200ms"
			eiBaselineDir = t.TempDir()
			eiWarehouse = tc.wh
			exportIcebergCmd.SetContext(context.Background())
			exportIcebergCmd.SetOut(&bytes.Buffer{})
			err := runExportIceberg(exportIcebergCmd, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
			if ents, _ := os.ReadDir(wd); len(ents) != 0 {
				t.Fatalf("created %q in the working directory", ents[0].Name())
			}
		})
	}
}
