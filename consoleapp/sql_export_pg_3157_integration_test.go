//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	pgExpTable = "pgexp_orders"
	pgExpSlot  = "bintrail_pgexp_slot"
	pgExpPub   = "bintrail_pgexp_pub"
)

// TestIntegrationPGSQLExport builds the web interface's .sql download for a
// real PostgreSQL server: a full read, changes captured after it, then the
// build for a moment after them.
//
// #3157: the build always asked the engine for a mydumper dump, which the
// engine refuses for a PostgreSQL table, in words that name the terminal
// command's options. A PostgreSQL server's build is the SQL files psql loads.
func TestIntegrationPGSQLExport(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	_, source, exec1 := pgTestSource(t, ctx, pgExpTable, pgExpSlot, pgExpPub, "id int PRIMARY KEY, status text")
	exec1("INSERT INTO " + pgExpTable + " VALUES (1, 'new'), (2, 'new'), (3, 'new')")
	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	_, reg, sup := newScheduleFixture(t, true)

	snapshots := t.TempDir()
	e, err := reg.Add(console.ServerEntry{
		Name: "pg-export", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pgExpSlot, SourcePublication: pgExpPub,
		Schemas: "public", BaselineDir: snapshots,
	})
	if err != nil {
		t.Fatalf("add the server: %v", err)
	}
	if e.DSN, err = mon.DeriveIndexDSN(e.ID); err != nil {
		t.Fatal(err)
	}
	if err := reg.Update(e); err != nil {
		t.Fatalf("save the index connection: %v", err)
	}
	pgTestStart(t, ctx, mon, bootDSN, source, e)

	if err := sup.Trigger(console.BaselineRequestFor(e)); err != nil {
		t.Fatalf("start the full read: %v", err)
	}
	pgTestWait(t, "the full read", 2*time.Minute, func() (bool, string) {
		st := sup.Status(e.ID)
		if st.State == "failed" {
			t.Fatalf("the full read failed: %s", st.LastError)
		}
		return st.State == "succeeded", st.State
	})

	// Changes after the full read, one of each kind: the build has to lay
	// them over the snapshot.
	exec1("INSERT INTO " + pgExpTable + " VALUES (4, 'new')")
	exec1("UPDATE " + pgExpTable + " SET status = 'shipped' WHERE id = 2")
	exec1("DELETE FROM " + pgExpTable + " WHERE id = 3")
	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	pgTestWait(t, "the three changes indexed and checkpointed", 90*time.Second, func() (bool, string) {
		var events, covered int
		err := idx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM binlog_events WHERE table_name = ?),
			COALESCE((SELECT binlog_position FROM stream_state WHERE id = 1) >= (SELECT MAX(end_pos) FROM binlog_events), 0)`,
			pgExpTable).Scan(&events, &covered)
		if err != nil {
			return false, err.Error()
		}
		return events >= 3 && covered == 1, "events=" + strconv.Itoa(events) + " checkpoint_covers=" + strconv.Itoa(covered)
	})
	time.Sleep(1100 * time.Millisecond)

	build := func(flavor string) console.BaselineStatus {
		t.Helper()
		req := console.SQLExportRequest{
			ServerID: e.ID, ServerName: e.Name, IndexDSN: e.DSN, BaselineSrc: snapshots,
			At: time.Now().UTC().Truncate(time.Second), Flavor: flavor,
		}
		if err := sup.TriggerSQLExport(req); err != nil {
			t.Fatalf("start the build: %v", err)
		}
		var st console.BaselineStatus
		pgTestWait(t, "the .sql build", 2*time.Minute, func() (bool, string) {
			st = sup.SQLExportStatus(e.ID)
			return st.State == "succeeded" || st.State == "failed", st.State
		})
		return st
	}

	// The request as the web interface makes it: the server's own flavor.
	st := build(e.SourceFlavor())
	if st.State != "succeeded" {
		t.Fatalf("the build of a PostgreSQL server ended %s: %s", st.State, st.LastError)
	}
	dir, _, ready := sup.SQLExportDir(e.ID)
	if !ready {
		t.Fatal("the build succeeded but has no folder to download")
	}
	if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); err != nil {
		t.Fatalf("the build's folder is not marked complete: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, ent := range entries {
		// The download refuses a folder with a subdirectory.
		if ent.IsDir() {
			t.Fatalf("the build's folder holds a subdirectory, %s", ent.Name())
		}
		if ent.Name() != baseline.SuccessMarker {
			names = append(names, ent.Name())
		}
	}
	want := "public." + pgExpTable + ".sql"
	if len(names) != 1 || names[0] != want {
		t.Fatalf("the build's files are %v, want only %s", names, want)
	}
	raw, err := os.ReadFile(filepath.Join(dir, want))
	if err != nil {
		t.Fatal(err)
	}
	got := string(raw)
	t.Logf("the file:\n%s", got)
	if !strings.Contains(got, "CREATE TABLE") || !strings.Contains(got, "FROM stdin") {
		t.Fatalf("the file is not a CREATE TABLE and a COPY block")
	}
	for _, row := range []string{"1\tnew\n", "2\tshipped\n", "4\tnew\n"} {
		if !strings.Contains(got, "\n"+row) {
			t.Errorf("the file does not hold the row %q", row)
		}
	}
	for _, row := range []string{"3\tnew\n", "2\tnew\n"} {
		if strings.Contains(got, "\n"+row) {
			t.Errorf("the file holds the row %q, which was gone at the moment asked for", row)
		}
	}
	if st.Rows != 3 {
		t.Errorf("the build reports %d rows, want 3", st.Rows)
	}

	// The same server asked for MySQL's format, as every build was before:
	// the engine refuses, and what the page shows names no terminal option.
	st = build(console.FlavorMySQL)
	if st.State != "failed" {
		t.Fatalf("a mydumper build of PostgreSQL tables ended %s, want failed", st.State)
	}
	t.Logf("the refusal: %s", st.LastError)
	if strings.Contains(st.LastError, "--") || strings.Contains(st.LastError, "`") {
		t.Errorf("the refusal names a terminal option or command: %s", st.LastError)
	}
	if !strings.Contains(st.LastError, "captured from PostgreSQL") {
		t.Errorf("the refusal does not say the tables are PostgreSQL's: %s", st.LastError)
	}
}
