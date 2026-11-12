package cli

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestBuildRestorePartitionSQL(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	hours := map[time.Time]bool{
		time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC): true,
		time.Date(2026, 8, 3, 8, 0, 0, 0, time.UTC):  true,
		// Overlaps the horizon — must not duplicate.
		now: true,
	}
	sqlStr, count := buildRestorePartitionSQL("idx", hours, now, 2)
	if count != 4 { // 08, 10, 12, 13
		t.Fatalf("partition count = %d, want 4", count)
	}
	for _, want := range []string{
		"ALTER TABLE `idx`.`binlog_events` PARTITION BY RANGE (TO_SECONDS(event_timestamp))",
		"PARTITION p_2026080308", "PARTITION p_2026080310", "PARTITION p_2026080312", "PARTITION p_2026080313",
	} {
		if !strings.Contains(sqlStr, want) {
			t.Fatalf("missing %q in:\n%s", want, sqlStr)
		}
	}
	// Ordered ascending, p_future last — RANGE partitioning requires it.
	if !strings.HasSuffix(strings.TrimSpace(sqlStr), "PARTITION p_future VALUES LESS THAN MAXVALUE\n)") {
		t.Fatalf("p_future must close the list:\n%s", sqlStr)
	}
	if strings.Index(sqlStr, "p_2026080308") > strings.Index(sqlStr, "p_2026080310") {
		t.Fatalf("partitions must be ascending:\n%s", sqlStr)
	}
}

func TestRestoreIndexTargetEmpty(t *testing.T) {
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		return db, mock
	}
	// All three probed tables absent: fine (fresh database).
	db, mock := newDB(t)
	for i := 0; i < 3; i++ {
		mock.ExpectQuery("information_schema.TABLES").
			WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	}
	if err := restoreIndexTargetEmpty(context.Background(), db, "idx"); err != nil {
		t.Fatalf("absent tables must pass: %v", err)
	}
	// Tables exist but are empty: fine.
	db, mock = newDB(t)
	for i := 0; i < 3; i++ {
		mock.ExpectQuery("information_schema.TABLES").
			WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))
		mock.ExpectQuery("SELECT 1 FROM").WillReturnError(sql.ErrNoRows)
	}
	if err := restoreIndexTargetEmpty(context.Background(), db, "idx"); err != nil {
		t.Fatalf("empty tables must pass: %v", err)
	}
	// binlog_events holds events: refused.
	db, mock = newDB(t)
	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))
	mock.ExpectQuery("SELECT 1 FROM").
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	if err := restoreIndexTargetEmpty(context.Background(), db, "idx"); err == nil ||
		!strings.Contains(err.Error(), "binlog_events") {
		t.Fatalf("populated index must be refused: %v", err)
	}
	// Empty events but a SURVIVING stream_state row: refused — the restarted
	// stream would resume the stale position and fake continuity.
	db, mock = newDB(t)
	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))
	mock.ExpectQuery("SELECT 1 FROM").WillReturnError(sql.ErrNoRows) // binlog_events empty
	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))
	mock.ExpectQuery("SELECT 1 FROM").
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1)) // stream_state row
	if err := restoreIndexTargetEmpty(context.Background(), db, "idx"); err == nil ||
		!strings.Contains(err.Error(), "stream_state") {
		t.Fatalf("surviving stream_state must be refused: %v", err)
	}
}

func TestRestoreIndexReportExitError(t *testing.T) {
	r := &restoreIndexReport{FilesLoaded: 3}
	if err := r.ExitError(); err != nil {
		t.Fatalf("clean load must exit 0: %v", err)
	}
	r.FailedFiles = []string{"p_2026080310: boom"}
	r.PartialRows = 12
	err := r.ExitError()
	if err == nil || !strings.Contains(err.Error(), "p_2026080310") || !strings.Contains(err.Error(), "12 partially-loaded") {
		t.Fatalf("failed file and partial rows must be named: %v", err)
	}
	// archive_state failures alone must not read as lost data.
	r2 := &restoreIndexReport{FilesLoaded: 3, StateRowFailures: []string{"p_2026080311: dup"}}
	err = r2.ExitError()
	if err == nil || !strings.Contains(err.Error(), "events ARE loaded") {
		t.Fatalf("state-row failure must be distinguished from data loss: %v", err)
	}
}

// TestNewestSidecarLocal pins the newest-wins selection and the
// unreadable-sidecar warning (a newer-but-broken sidecar must not silently
// lose without a trace).
func TestNewestSidecarLocal(t *testing.T) {
	sDir, sS3 := riArchiveDir, riArchiveS3
	t.Cleanup(func() { riArchiveDir, riArchiveS3 = sDir, sS3 })
	root := t.TempDir()
	riArchiveDir, riArchiveS3 = root, ""
	write := func(id, writtenAt, body string) {
		dir := filepath.Join(root, "bintrail_id="+id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		content := body
		if content == "" {
			content = `{"written_at":"` + writtenAt + `","schema_snapshots":[{"snapshot_id":1}],"bintrail_servers":[]}`
		}
		if err := os.WriteFile(filepath.Join(dir, "index-meta.json"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("old", "2026-01-01T00:00:00Z", "")
	write("new", "2026-06-01T00:00:00Z", "")
	ids := map[string]bool{"old": true, "new": true, "absent": true}
	m, warnings := newestSidecar(context.Background(), newBucketClients(""), ids)
	if m == nil || !m.WrittenAt.Equal(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("newest sidecar must win: %+v", m)
	}
	if len(warnings) != 0 {
		t.Fatalf("absent sidecars are routine, no warnings expected: %v", warnings)
	}
	// Corrupt the newest: the older one is returned, but WITH a warning.
	write("new", "", "{not json")
	m, warnings = newestSidecar(context.Background(), newBucketClients(""), ids)
	if m == nil || !m.WrittenAt.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("older sidecar must be the fallback: %+v", m)
	}
	if len(warnings) != 1 {
		t.Fatalf("the unreadable newer sidecar must be warned about: %v", warnings)
	}
}
