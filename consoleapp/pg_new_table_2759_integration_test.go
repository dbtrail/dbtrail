//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	pgNewTable = "pgnew_orders"
	pgNewAdded = "pgnew_audit"
	pgNewSlot  = "bintrail_pgnew_slot"
	pgNewPub   = "bintrail_pgnew_pub"
)

// A table added to a PostgreSQL server's publication after a snapshot
// (#2759), end to end on a real PostgreSQL and a real MySQL index: the
// schedule's update names the table it left out, and the full read the
// schedule then takes on its own puts it in the copy. Nothing is stubbed.
func TestIntegrationPGNewTableIsIncluded_2759(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	baseDSN, source, exec1 := pgTestSource(t, ctx, pgNewTable, pgNewSlot, pgNewPub, "id int PRIMARY KEY, status text")
	t.Cleanup(func() { _, _ = source.Exec(context.Background(), "DROP TABLE IF EXISTS "+pgNewAdded).ReadAll() })
	exec1("DROP TABLE IF EXISTS " + pgNewAdded)
	exec1("INSERT INTO " + pgNewTable + " VALUES (1, 'new')")

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	sup.tableDeltas = true
	t.Cleanup(sup.postRefresh.Wait)
	snapshots := t.TempDir()
	e, err := reg.Add(console.ServerEntry{
		Name: "pg-new-table", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pgNewSlot, SourcePublication: pgNewPub,
		Schemas: "public", BaselineDir: snapshots,
		BackupSchedule: &console.BackupSchedule{Every: "1h", At: "00:00"},
	})
	if err != nil {
		t.Fatalf("add the server: %v", err)
	}
	if e.DSN, err = mon.DeriveIndexDSN(e.ID); err != nil {
		t.Fatal(err)
	}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	pgTestStart(t, ctx, mon, bootDSN, source, e)

	if err := sup.Trigger(console.BaselineRequestFor(e)); err != nil {
		t.Fatalf("start the full read: %v", err)
	}
	pgTestWait(t, "the first full read", 2*time.Minute, func() (bool, string) {
		st := sup.Status(e.ID)
		if st.State == "failed" {
			t.Fatalf("the full read failed: %s", st.LastError)
		}
		return st.State == "succeeded", st.State
	})
	first := pgUpdSnapshots(t, snapshots)
	if len(first) != 1 {
		t.Fatalf("after the full read: %v", first)
	}

	// A table created and added to the publication after that snapshot, and
	// a change of the table the snapshot holds so that the update has work.
	exec1("CREATE TABLE " + pgNewAdded + " (id int PRIMARY KEY, note text)")
	exec1("ALTER TABLE " + pgNewAdded + " REPLICA IDENTITY FULL")
	exec1("ALTER PUBLICATION " + pgNewPub + " ADD TABLE " + pgNewAdded)
	exec1("INSERT INTO " + pgNewAdded + " VALUES (1, 'first'), (2, 'second')")
	exec1("UPDATE " + pgNewTable + " SET status = 'shipped' WHERE id = 1")
	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	pgTestWait(t, "the change indexed and checkpointed", 90*time.Second, func() (bool, string) {
		var events, covered int
		err := idx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM binlog_events WHERE table_name = ?),
			COALESCE((SELECT binlog_position FROM stream_state WHERE id = 1) >= (SELECT MAX(end_pos) FROM binlog_events), 0)`,
			pgNewTable).Scan(&events, &covered)
		if err != nil {
			return false, err.Error()
		}
		return events >= 1 && covered == 1, "waiting"
	})
	time.Sleep(1100 * time.Millisecond) // snapshot folders are named to the second

	p, err := e.BackupSchedule.Parse()
	if err != nil {
		t.Fatal(err)
	}
	b.fire(e, p, time.Now().UTC())

	// The update publishes without the new table and names it; the schedule
	// then reads the source in full, and that snapshot holds it.
	addedFile := func() string {
		for _, name := range pgUpdSnapshots(t, snapshots) {
			found, _ := filepath.Glob(filepath.Join(snapshots, name, "*", pgNewAdded+".parquet"))
			if len(found) == 1 {
				return found[0]
			}
		}
		return ""
	}
	var named []string
	pgTestWait(t, "a snapshot that holds the added table", 3*time.Minute, func() (bool, string) {
		st := b.ScheduleState(e.ID)
		if st.LastSkipReason != "" {
			t.Fatalf("the schedule skipped its slot: %s", st.LastSkipReason)
		}
		if rs := sup.RefreshStatus(e.ID); len(rs.NewTables) > 0 {
			named = rs.NewTables
		}
		if st := sup.Status(e.ID); st.State == "failed" {
			t.Fatalf("the full read for the new table failed: %s", st.LastError)
		}
		return addedFile() != "", "method=" + st.LastMethod
	})
	b.watchers.Wait()
	sup.postRefresh.Wait()

	if !slices.Contains(named, "public."+pgNewAdded) {
		t.Errorf("the update named %v as left out, want public.%s among them", named, pgNewAdded)
	}
	file := addedFile()
	md, err := baseline.ReadParquetMetadata(file)
	if err != nil {
		t.Fatal(err)
	}
	if md.Producer != baseline.ProducerDump {
		t.Fatalf("the added table's file was produced by %q, want a full read (%q)", md.Producer, baseline.ProducerDump)
	}
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()
	rows, err := duck.Query(`SELECT CAST(id AS VARCHAR), note FROM parquet_scan('` + file + `') ORDER BY CAST(id AS BIGINT)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got [][]string
	for rows.Next() {
		var id, note sql.NullString
		if err := rows.Scan(&id, &note); err != nil {
			t.Fatal(err)
		}
		got = append(got, []string{id.String, note.String})
	}
	if want := [][]string{{"1", "first"}, {"2", "second"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("the added table in the copy holds %v, want %v", got, want)
	}
	// Three snapshots, no more: the full read, the update, and one full read
	// for the new table. Not one per slot.
	if all := pgUpdSnapshots(t, snapshots); len(all) != 3 {
		entries, _ := os.ReadDir(snapshots)
		t.Fatalf("snapshots = %v (folder holds %d entries), want the first full read, the update and one full read for the new table", all, len(entries))
	}
}
