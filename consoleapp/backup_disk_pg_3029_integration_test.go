//go:build integration

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
)

// #3029 against a real PostgreSQL, through the function every PostgreSQL
// snapshot goes through, with the real size query and the real snapshot.
// Only the free space is given, so the test does not depend on the disk it
// runs on.
//
// Below the floor the snapshot is refused with the server's real sizes
// before anything exists: no replication slot, no folder. With room, the
// same request writes its snapshot and its status carries the check's line.
func TestIntegrationPGSnapshotChecksTheDisk_3029(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const table, slot, pub = "disk3029_orders", "disk3029_slot", "disk3029_pub"
	baseDSN, source, exec1 := pgTestSource(t, ctx, table, slot, pub, "id int PRIMARY KEY, v text")
	exec1("INSERT INTO " + table + " SELECT g, md5(g::text) FROM generate_series(1, 50000) g")
	_, rows, err := pgText(t, source, "SELECT pg_table_size('"+table+"')::text, pg_total_relation_size('"+table+"')::text")
	if err != nil || len(rows) != 1 {
		t.Fatalf("size of the table: rows %v, err %v", rows, err)
	}
	t.Logf("pg_table_size = %s bytes, with indexes %s", rows[0][0], rows[0][1])

	local := filepath.Join(t.TempDir(), "snapshots", "not-created-yet")
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.jobs["pg1"] = &console.BaselineStatus{State: "running"}
	req := console.BaselineRequest{ServerID: "pg1", ServerName: "pg", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, Slot: slot, Publication: pub, Schemas: []string{"public"}, LocalDir: local}
	slots := func() string {
		t.Helper()
		_, rows, err := pgText(t, source, "SELECT count(*)::text FROM pg_replication_slots WHERE slot_name = '"+slot+"'")
		if err != nil || len(rows) != 1 {
			t.Fatalf("rows %v, err %v", rows, err)
		}
		return rows[0][0]
	}
	freeIs := func(free uint64) {
		prev := diskSpaceFn
		t.Cleanup(func() { diskSpaceFn = prev })
		diskSpaceFn = func(string) (uint64, uint64, error) { return free, 1 << 40, nil }
	}

	freeIs(700 << 20)
	_, err = sup.executePG(req)
	t.Logf("refusal, as the user reads it:\n%v", err)
	if err == nil || !errors.Is(err, errFoldDiskFull) {
		t.Fatalf("err = %v, want the disk refusal", err)
	}
	for _, want := range []string{
		"the snapshot was not started. The snapshot folder " + local + " has 700.0 MiB free, and DBTrail keeps 1.0 GiB free there",
		"The tables to copy take up about ", " on the database server. Nothing was read from your database",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal lacks %q", want)
		}
	}
	if n := slots(); n != "0" {
		t.Errorf("a refused snapshot created the replication slot (%s)", n)
	}
	if _, serr := os.Stat(local); !os.IsNotExist(serr) {
		t.Errorf("a refused snapshot created its folder: %v", serr)
	}

	// Room that is under the highest ratio measured: it runs, with a warning.
	freeIs(1<<30 + 1<<20)
	exec1("INSERT INTO " + table + " SELECT g, repeat(md5(g::text), 1000) FROM generate_series(50001, 80000) g")
	if _, err := sup.executePG(req); err != nil {
		t.Fatalf("a snapshot with the floor free: %v", err)
	}
	st := sup.Status("pg1")
	t.Logf("with little room: %s: %s", st.DiskCheck, st.DiskNote)
	if st.DiskCheck != dumpDiskLow && st.DiskCheck != dumpDiskOK {
		t.Errorf("check = %q", st.DiskCheck)
	}

	// A snapshot's folder is named by the second it was taken at.
	time.Sleep(1100 * time.Millisecond)
	freeIs(500 << 30)
	out, err := sup.executePG(req)
	if err != nil {
		t.Fatalf("a snapshot with room: %v", err)
	}
	st = sup.Status("pg1")
	t.Logf("with room: %s: %s", st.DiskCheck, st.DiskNote)
	if st.DiskCheck != dumpDiskOK || !strings.HasPrefix(st.DiskNote, "Disk check: the tables to copy take up about ") ||
		!strings.HasSuffix(st.DiskNote, "; 500.0 GiB free at "+local+".") || strings.Contains(st.DiskNote, "about 0 B") {
		t.Errorf("status: %q %s", st.DiskCheck, st.DiskNote)
	}
	if _, serr := os.Stat(filepath.Join(out.snapDir, baseline.SuccessMarker)); serr != nil {
		t.Errorf("no finished snapshot at %q: %v", out.snapDir, serr)
	}
	if out.stats.RowsWritten != 80000 {
		t.Errorf("rows written = %d, want 80000", out.stats.RowsWritten)
	}
}
