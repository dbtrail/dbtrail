package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// TestSQLExportSingleFlight pins that the export shares the per-server lock
// with dump/refresh/restore in BOTH directions, the second one literally:
// a restore trigger during a running export must refuse.
func TestSQLExportSingleFlight(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	req := console.SQLExportRequest{ServerID: "srv1", ServerName: "wp",
		IndexDSN: "i:p@tcp(h:3306)/idx", BaselineSrc: t.TempDir(),
		At: time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)}

	sup.mu.Lock()
	sup.refreshes["srv1"] = &console.BaselineStatus{State: "running"}
	sup.mu.Unlock()
	if err := sup.TriggerSQLExport(req); !errors.Is(err, console.ErrBaselineRunning) {
		t.Fatalf("export during refresh: err = %v, want ErrBaselineRunning", err)
	}

	sup.mu.Lock()
	sup.refreshes["srv1"] = &console.BaselineStatus{State: "succeeded"}
	sup.exports["srv1"] = &console.BaselineStatus{State: "running"}
	sup.mu.Unlock()
	rreq := console.BaselineRestoreRequest{ServerID: "srv1", ServerName: "wp",
		IndexDSN: "i:p@tcp(h:3306)/idx", BaselineDir: t.TempDir(),
		At: time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)}
	if err := sup.TriggerRestore(rreq); !errors.Is(err, console.ErrBaselineRunning) {
		t.Fatalf("restore during export: err = %v, want ErrBaselineRunning", err)
	}
}

// seedExport puts a build state + dir into the supervisor the way Trigger
// does, without running a fold. The deadline is stamped as the success tail
// would: a finished build without one counts as expired (#1448), on purpose.
func seedExport(sup *baselineSupervisor, serverID, state, dir string) {
	sup.mu.Lock()
	sup.exports[serverID] = &console.BaselineStatus{State: state,
		ExpiresAt: sup.clock().UTC().Add(sqlExportTTL).Format(time.RFC3339)}
	sup.exportRuns[serverID] = &sqlExportRun{dir: dir}
	sup.mu.Unlock()
}

// TestSQLExportDirGating: the download seam refuses until the status says
// succeeded AND the directory affirmatively wears _SUCCESS (and no
// _INCOMPLETE). Marker-absent is NOT complete here — that legacy default is
// for pre-marker baseline snapshots, and a build dir is never legacy.
func TestSQLExportDirGating(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	if _, _, ok := sup.SQLExportDir("srv1"); ok {
		t.Fatal("no build ever ran: ok must be false")
	}
	dir := filepath.Join(sup.sqlExportRoot("srv1"), "1")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(dir); err != nil {
		t.Fatal(err)
	}
	// A complete directory under a non-succeeded status stays refused: the
	// state is the authority on WHICH build the dir belongs to.
	for _, state := range []string{"failed", "running"} {
		seedExport(sup, "srv1", state, dir)
		if _, _, ok := sup.SQLExportDir("srv1"); ok {
			t.Fatalf("%s build over a complete dir: ok must be false", state)
		}
	}
	seedExport(sup, "srv1", "succeeded", dir)
	got, gotSt, ok := sup.SQLExportDir("srv1")
	if !ok || got != dir || gotSt.State != "succeeded" {
		t.Fatalf("complete build: got (%q,%+v,%v), want (%q, succeeded, true)", got, gotSt, ok, dir)
	}
	// _INCOMPLETE present (the fold's crash-safety marker still in place)
	// overrides everything, even beside a _SUCCESS.
	if err := os.WriteFile(filepath.Join(dir, baseline.IncompleteMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := sup.SQLExportDir("srv1"); ok {
		t.Fatal("_INCOMPLETE dir: ok must be false even with a succeeded status")
	}
	// Marker-less dir (torn staging): refused, not legacy-complete.
	if err := os.Remove(filepath.Join(dir, baseline.IncompleteMarker)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, baseline.SuccessMarker)); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := sup.SQLExportDir("srv1"); ok {
		t.Fatal("marker-less dir: ok must be false (no legacy default for build dirs)")
	}
	// Vanished dir (a tmp-reaping host): refused, so the handler answers 409
	// "build one first" instead of a raw 500.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	seedExport(sup, "srv1", "succeeded", dir)
	if _, _, ok := sup.SQLExportDir("srv1"); ok {
		t.Fatal("vanished dir: ok must be false")
	}
}

// TestSQLExportRun_failure: a build over a store with no usable snapshot
// fails with words about the missing backup, publishes nothing, and is
// deliberately ABSENT from the run history (it publishes no snapshot for
// FindBySnapshot to match; recording it would only evict real records).
func TestSQLExportRun_failure(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	h, err := console.OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	sup.history = h
	req := console.SQLExportRequest{ServerID: "srv1", ServerName: "wp",
		IndexDSN: "i:p@tcp(h:3306)/idx", BaselineSrc: t.TempDir(),
		At: time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)}
	sup.mu.Lock()
	sup.exports["srv1"] = &console.BaselineStatus{State: "running", At: "2026-06-10T11:00:00Z"}
	dir := filepath.Join(sup.sqlExportRoot("srv1"), "1")
	sup.exportRuns["srv1"] = &sqlExportRun{dir: dir}
	sup.mu.Unlock()
	sup.runSQLExport(req, dir)

	st := sup.SQLExportStatus("srv1")
	if st.State != "failed" || !strings.Contains(st.LastError, "no snapshot exists at or before") {
		t.Fatalf("status = %+v, want failed with the no-backup message", st)
	}
	if st.At != "2026-06-10T11:00:00Z" {
		t.Fatalf("At = %q, the requested instant must survive the failure", st.At)
	}
	if _, _, ok := sup.SQLExportDir("srv1"); ok {
		t.Fatal("a failed build must not be downloadable")
	}
	if st.Tables != 0 || st.Rows != 0 || st.Bytes != 0 {
		t.Fatalf("failed status carries attempt-scoped partials (%d tables, %d rows, %d bytes); a failed build published nothing", st.Tables, st.Rows, st.Bytes)
	}
	if recs := h.List("srv1"); len(recs) != 0 {
		t.Fatalf("history = %+v, want none: export runs are deliberately unrecorded", recs)
	}
}

// writeSQLExportBaseline writes a one-table baseline Parquet in the
// FindBaseline layout (a unit-tag sibling of the integration-tagged
// writeConsoleBaseline) so a fold can get past table discovery.
func writeSQLExportBaseline(t *testing.T, snapTime time.Time) string {
	t.Helper()
	root := t.TempDir()
	tableDir := filepath.Join(root, snapTime.UTC().Format("2006-01-02T15-04-05")+"Z", "shop")
	if err := os.MkdirAll(tableDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	w, err := baseline.NewWriter(filepath.Join(tableDir, "orders.parquet"), cols,
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1", "new"}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return root
}

// TestSQLExportRun_failureAfterDiscovery: a fold that fails AFTER counting
// tables (here: unreachable index) must still report zero tables/rows/bytes
// — attempt-scoped partials on a failed status read as progress.
func TestSQLExportRun_failureAfterDiscovery(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	at := time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)
	src := writeSQLExportBaseline(t, at.Add(-time.Hour))
	req := console.SQLExportRequest{ServerID: "srv1", ServerName: "wp",
		IndexDSN: "i:p@tcp(127.0.0.1:1)/idx", BaselineSrc: src, At: at}
	sup.mu.Lock()
	sup.exports["srv1"] = &console.BaselineStatus{State: "running", At: "2026-06-10T11:00:00Z"}
	dir := filepath.Join(sup.sqlExportRoot("srv1"), "1")
	sup.exportRuns["srv1"] = &sqlExportRun{dir: dir}
	sup.mu.Unlock()
	sup.runSQLExport(req, dir)

	st := sup.SQLExportStatus("srv1")
	if st.State != "failed" || st.LastError == "" {
		t.Fatalf("status = %+v, want failed with an error", st)
	}
	if st.Tables != 0 || st.Rows != 0 || st.Bytes != 0 {
		t.Fatalf("failed status carries attempt-scoped partials (%d tables, %d rows, %d bytes); a failed build published nothing", st.Tables, st.Rows, st.Bytes)
	}
	if _, _, ok := sup.SQLExportDir("srv1"); ok {
		t.Fatal("a failed build must not be downloadable")
	}
}

// TestSQLExportTrigger_stampsInstant: the running status carries the chosen
// instant from the very first poll, and nothing is downloadable while the
// build runs.
func TestSQLExportTrigger_stampsInstant(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	at := time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)
	req := console.SQLExportRequest{ServerID: "srv1", ServerName: "wp",
		IndexDSN: "i:p@tcp(h:3306)/idx", BaselineSrc: t.TempDir(), At: at}
	if err := sup.TriggerSQLExport(req); err != nil {
		t.Fatal(err)
	}
	st := sup.SQLExportStatus("srv1")
	if st.At != "2026-06-10T11:00:00Z" || st.Since == "" {
		t.Fatalf("status right after trigger = %+v, want the instant and a start stamp", st)
	}
	if st.State == "running" {
		if _, _, ok := sup.SQLExportDir("srv1"); ok {
			t.Fatal("a running build must not be downloadable")
		}
	}
	// The empty store makes the goroutine fail fast without touching the DSN;
	// wait for it so the test does not leak a running fold.
	waitSettled := func() {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if sup.SQLExportStatus("srv1").State != "running" {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("export never settled")
	}
	waitSettled()
	sup.mu.Lock()
	dir1 := sup.exportRuns["srv1"].dir
	sup.mu.Unlock()
	// A second build gets its OWN directory: a shared path reused across
	// builds is the silent half of the wipe race (two instants interleaved
	// into one archive whose per-file guards all pass).
	if err := sup.TriggerSQLExport(req); err != nil {
		t.Fatal(err)
	}
	waitSettled()
	sup.mu.Lock()
	dir2 := sup.exportRuns["srv1"].dir
	sup.mu.Unlock()
	if dir1 == dir2 {
		t.Fatalf("both builds share %q; every build must get a fresh directory", dir1)
	}
}

// TestSQLExportRunError: a binlog-only table report fails the run even when
// the engine reported no error — a dump without its baseline is never a
// PASS — and names the table; clean reports pass the engine verdict through.
func TestSQLExportRunError(t *testing.T) {
	clean := []*reconstruct.TableReport{{Schema: "shop", Table: "orders"}}
	if err := sqlExportRunError(clean, nil); err != nil {
		t.Fatalf("clean reports: err = %v, want nil", err)
	}
	degraded := []*reconstruct.TableReport{
		{Schema: "shop", Table: "orders"},
		{Schema: "shop", Table: "users", BinlogOnly: true},
	}
	err := sqlExportRunError(degraded, nil)
	if err == nil || !strings.Contains(err.Error(), "shop.users") ||
		!strings.Contains(err.Error(), "recorded changes only") {
		t.Fatalf("binlog-only report: err = %v, want a refusal naming shop.users", err)
	}
	if kept := sqlExportRunError(clean, errors.New("fold died")); kept == nil || kept.Error() != "fold died" {
		t.Fatalf("engine error must pass through, got %v", kept)
	}
}

// TestSQLExportBootSweep: staging left by a previous process is removed at
// supervisor construction — a restart empties the in-memory map, so the
// artifact would otherwise sit unreachable on disk forever.
func TestSQLExportBootSweep(t *testing.T) {
	staging := t.TempDir()
	stale := filepath.Join(staging, "sql-export", "srv-old", "1")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "shop.orders.00000.sql"), []byte("INSERT"), 0o644); err != nil {
		t.Fatal(err)
	}
	newBaselineSupervisor(context.Background(), staging, "")
	if _, err := os.Stat(filepath.Join(staging, "sql-export")); !os.IsNotExist(err) {
		t.Fatalf("stale sql-export staging survived construction (err=%v)", err)
	}
}

// TestSQLExportFoldConfig_sharesTheDaemonBounds pins the SQL export build to
// the same in-daemon posture as the refresh and restore folds.
//
// This build is operator-triggered, which is exactly why it is easy to reason
// about wrongly: the click is attended, the fold that follows is not. It runs
// in the background of the capture process and its warnings go to the daemon
// log, so it needs the bound and the warning for the same reason the periodic
// refresh does.
//
// Both wanted values are non-zero, which is what makes this discriminate:
// deleting either assignment leaves the field at its zero value and fails here.
func TestSQLExportFoldConfig_sharesTheDaemonBounds(t *testing.T) {
	cfg := sqlExportFoldConfig(console.SQLExportRequest{
		ServerID: "s1", IndexDSN: "dsn", At: time.Now(),
	}, "/build", []string{"shop.orders"})

	if cfg.Parallelism == 0 {
		t.Error("Parallelism left at zero: the export would inherit runtime.NumCPU() " +
			"and scale its peak memory with the host, inside the capture process")
	}
	if cfg.Parallelism != daemonFoldParallelism {
		t.Errorf("Parallelism = %d, want %d (the shared in-daemon bound)",
			cfg.Parallelism, daemonFoldParallelism)
	}
	if cfg.WarnEventThreshold == 0 {
		t.Error("WarnEventThreshold left at zero: shouldWarnEvents is " +
			"`threshold > 0 && n > threshold`, so this fold would never warn")
	}
	if cfg.WarnEventThreshold != daemonFoldWarnEventThreshold {
		t.Errorf("WarnEventThreshold = %d, want %d (the shared in-daemon bound)",
			cfg.WarnEventThreshold, daemonFoldWarnEventThreshold)
	}
	if cfg.RemediationHint == "" {
		t.Error("RemediationHint left empty: the warning falls back to the CLI wording, " +
			"which names --at / --parallelism / --warn-event-threshold. bintrail-console " +
			"registers none of them, so the operator is sent after flags that do not exist")
	}
	if cfg.RemediationHint != daemonFoldRemediation {
		t.Errorf("RemediationHint = %q, want the shared constant", cfg.RemediationHint)
	}

	// The engine's fail-closed contract for an artifact the operator will load.
	if cfg.AllowGaps {
		t.Error("AllowGaps = true: a dump built over a known capture gap would " +
			"silently miss rows and still look like a complete snapshot")
	}
}
