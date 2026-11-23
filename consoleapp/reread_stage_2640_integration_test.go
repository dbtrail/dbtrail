//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// stageReread with the real mydumper and the names a table list has to carry
// (#2640): capitals, a space, a dot, a table of another schema, a table asked
// twice, and a table the source does not hold. Run by hand: it needs a
// mydumper on PATH (a wrapper around the pinned image works).
func TestIntegrationStageReread_realMydumperNames_2640(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	if _, err := exec.LookPath("mydumper"); err != nil {
		t.Skip("no mydumper on PATH: this test runs the real one")
	}
	ctx := context.Background()
	db, name := testutil.CreateTestDB(t)
	other := name + "_b"
	testutil.MustExec(t, db, "CREATE DATABASE `"+other+"`")
	t.Cleanup(func() { _, _ = db.Exec("DROP DATABASE IF EXISTS `" + other + "`") })
	tables := []string{name + ".orders", name + ".Order Items", other + ".t"}
	// A dot in a table's name: measured, mydumper 1.0.3-1 does not read it
	// from a table list, so stageReread refuses it before the dump.
	dotted := name + ".v1.2"
	for _, st := range append([]string{dotted}, tables...) {
		schema, table, _ := strings.Cut(st, ".")
		q := "`" + schema + "`.`" + table + "`"
		testutil.MustExec(t, db, "CREATE TABLE "+q+" (id INT PRIMARY KEY, label VARCHAR(20))")
		testutil.MustExec(t, db, "INSERT INTO "+q+" VALUES (1, 'uno'), (2, 'dos'), (3, 'tres')")
	}
	// A table that is not asked for: it must not be read.
	testutil.MustExec(t, db, "CREATE TABLE bystander (id INT PRIMARY KEY)")
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()

	for _, mode := range []baseline.LockMode{baseline.LockModeFTWRL, baseline.LockModeLockAll} {
		t.Run(string(mode), func(t *testing.T) {
			work := t.TempDir()
			s := newBaselineSupervisor(ctx, work, mode)
			s.lockModeChosen = true
			req := console.BaselineRequest{ServerID: "s1", ServerName: "names", SourceDSN: testutil.BaseDSN() + "/", LocalDir: t.TempDir()}

			read, err := s.stageReread(req, append(append([]string{}, tables...), name+".orders"))
			if err != nil {
				t.Fatalf("stageReread: %v", err)
			}
			if len(read.files) != len(tables) {
				t.Fatalf("files = %v, want the %d tables", read.files, len(tables))
			}
			for _, st := range tables {
				p := read.files[st]
				var n int
				if err := ddb.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM parquet_scan('%s')", strings.ReplaceAll(p, "'", "''"))).Scan(&n); err != nil || n != 3 {
					t.Fatalf("%s at %s: %d rows, %v; want 3", st, p, n, err)
				}
				meta, err := baseline.ReadParquetMetadata(p)
				if err != nil {
					t.Fatal(err)
				}
				if meta.Producer != baseline.ProducerDump || !meta.SnapshotTimestamp.Equal(read.at) || meta.BinlogFile == "" || meta.BinlogPos <= 0 {
					t.Fatalf("%s: footer %+v, want a read from the source stamped %s with a binlog position", st, meta, read.at)
				}
			}
			if left := rereadLeft(t, read.root); len(left) != 3 {
				// The read's folder, and the link and the lock baseline.Run
				// leaves beside it.
				t.Logf("the staged folder holds %v", left)
			}
			schemaDirs, _ := exec.Command("ls", "-R", read.root).CombinedOutput()
			if strings.Contains(string(schemaDirs), "bystander") {
				t.Fatalf("a table that was not asked for was read:\n%s", schemaDirs)
			}
			if err := read.discard(); err != nil {
				t.Fatal(err)
			}

			if _, err := s.stageReread(req, []string{name + ".orders", dotted}); !errors.Is(err, errRereadUnavailable) {
				t.Fatalf("err = %v, want a table with a dot in its name refused for a full read", err)
			}
			_, err = s.stageReread(req, []string{name + ".orders", name + ".gone"})
			if !errors.Is(err, errRereadTableMissing) || !strings.Contains(err.Error(), name+".gone") {
				t.Fatalf("err = %v, want the table the source does not hold named", err)
			}
			if left := rereadLeft(t, work); len(left) != 0 {
				t.Fatalf("the working folder holds %v after the reads", left)
			}
		})
	}
}
