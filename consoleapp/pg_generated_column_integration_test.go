//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	pgGenTable = "pggen_lines"
	pgGenSlot  = "bintrail_pggen_slot"
	pgGenPub   = "bintrail_pggen_pub"
)

// A table with a STORED generated column, from the full read to the scheduled
// update, on a real PostgreSQL and a real MySQL index, nothing stubbed.
//
// On PostgreSQL 18 the publication has to publish the column, or the server
// refuses the UPDATE and the DELETE this test runs (and the capture refuses
// to start: pgcapture.CheckGeneratedColumns). The stream then carries a
// column the snapshot does not hold, which a full read leaves out on purpose.
// The updated copy must still equal the source, column for column it holds.
// On 17 and older nothing is published and the same must be true.
func TestIntegrationPGSnapshotUpdateGeneratedColumn(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	baseDSN, source, exec1 := pgTestSource(t, ctx, pgGenTable, pgGenSlot, pgGenPub,
		"id int PRIMARY KEY, qty int, price numeric(10,2), total numeric(12,2) GENERATED ALWAYS AS (qty * price) STORED")
	_, ver, err := pgText(t, source, "SELECT current_setting('server_version_num')")
	if err != nil || len(ver) != 1 {
		t.Fatalf("server version: %v", err)
	}
	pg18 := len(ver[0][0]) >= 6 && ver[0][0] >= "180000"
	if pg18 {
		exec1("ALTER PUBLICATION " + pgGenPub + " SET (publish_generated_columns = stored)")
	}
	exec1("INSERT INTO " + pgGenTable + " (id, qty, price) VALUES (1, 2, 10.50), (2, 1, 20.00), (3, 5, 1.00)")

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	sup.tableDeltas = true
	t.Cleanup(sup.postRefresh.Wait)
	snapshots := t.TempDir()
	e, err := reg.Add(console.ServerEntry{
		Name: "pg-generated", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pgGenSlot, SourcePublication: pgGenPub,
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
	pgTestWait(t, "the full read", 2*time.Minute, func() (bool, string) {
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

	// The writes PostgreSQL 18 refuses when the column is not published.
	exec1("INSERT INTO " + pgGenTable + " (id, qty, price) VALUES (4, 3, 4.00)")
	exec1("UPDATE " + pgGenTable + " SET qty = 7 WHERE id = 2")
	exec1("DELETE FROM " + pgGenTable + " WHERE id = 3")
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
			pgGenTable).Scan(&events, &covered)
		if err != nil {
			return false, err.Error()
		}
		return events >= 3 && covered == 1, "waiting"
	})
	// On 18 the captured rows carry the generated column; before 18 they
	// cannot. Either way the fold has to cope: this is what it is given.
	var after sql.NullString
	if err := idx.QueryRowContext(ctx, "SELECT CAST(row_after AS CHAR) FROM binlog_events WHERE table_name = ? AND pk_values = '4'", pgGenTable).Scan(&after); err != nil {
		t.Fatalf("the captured insert: %v", err)
	}
	if carries := strings.Contains(after.String, `"total"`); carries != pg18 {
		t.Fatalf("the captured row carries the generated column: %v, on a server where it is published: %v (%s)", carries, pg18, after.String)
	}
	time.Sleep(1100 * time.Millisecond)

	p, err := e.BackupSchedule.Parse()
	if err != nil {
		t.Fatal(err)
	}
	b.tick(ctx, time.Now().UTC())
	b.fire(e, p, time.Now().UTC())
	var st console.BackupScheduleState
	pgTestWait(t, "the scheduled job", 3*time.Minute, func() (bool, string) {
		st = b.ScheduleState(e.ID)
		if st.LastSkipReason != "" {
			t.Fatalf("the schedule skipped its slot: %s", st.LastSkipReason)
		}
		return st.Last != nil && !st.Running, "running"
	})
	b.watchers.Wait()
	sup.postRefresh.Wait()
	st = b.ScheduleState(e.ID)
	if st.LastMethod != console.BackupMethodRefresh || st.Last == nil || st.Last.State != "succeeded" || st.LastFallbackAt != "" {
		t.Fatalf("the update did not run or did not succeed: method %q, %+v, fallback %q", st.LastMethod, st.Last, st.LastFallbackReason)
	}

	all := pgUpdSnapshots(t, snapshots)
	if len(all) != 2 {
		t.Fatalf("snapshots after the update: %v", all)
	}
	file := pgTableFileOf(t, filepath.Join(snapshots, all[1]), pgGenTable)
	md, err := baseline.ReadParquetMetadata(file)
	if err != nil {
		t.Fatal(err)
	}
	if md.Producer != baseline.ProducerReconstruct {
		t.Fatalf("the new snapshot was produced by %q, want the update", md.Producer)
	}
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()
	// The copy's columns: the generated one is not among them, before and
	// after the update alike.
	for _, f := range []string{pgTableFileOf(t, filepath.Join(snapshots, all[0]), pgGenTable), file} {
		rows, err := duck.Query(`SELECT column_name FROM (DESCRIBE SELECT * FROM parquet_scan('` + f + `'))`)
		if err != nil {
			t.Fatal(err)
		}
		var cols []string
		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err != nil {
				t.Fatal(err)
			}
			cols = append(cols, c)
		}
		rows.Close()
		// A snapshot file's own column order is not the table's.
		sort.Strings(cols)
		if want := []string{"id", "price", "qty"}; !reflect.DeepEqual(cols, want) {
			t.Fatalf("%s holds columns %v, want %v (a generated column is computed by the database, not copied)", filepath.Base(filepath.Dir(filepath.Dir(f))), cols, want)
		}
	}
	rows, err := duck.Query(`SELECT CAST(id AS VARCHAR), CAST(qty AS VARCHAR), CAST(price AS VARCHAR) FROM parquet_scan('` + file + `') ORDER BY CAST(id AS BIGINT)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got [][]string
	for rows.Next() {
		var id, qty, price sql.NullString
		if err := rows.Scan(&id, &qty, &price); err != nil {
			t.Fatal(err)
		}
		got = append(got, []string{id.String, qty.String, price.String})
	}
	_, src, err := pgText(t, source, "SELECT id::text, qty::text, price::text FROM "+pgGenTable+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if want := [][]string{{"1", "2", "10.50"}, {"2", "7", "20.00"}, {"4", "3", "4.00"}}; !reflect.DeepEqual(got, want) || !reflect.DeepEqual(src, want) {
		t.Fatalf("the updated copy holds %v, the source %v, want %v", got, src, want)
	}
}
