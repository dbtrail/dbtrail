//go:build integration

package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationDrill_guardRefusesSecondRun proves the emptiness guard is
// actually WIRED into runDrill (not just unit-tested in isolation): the
// first rehearsal populates the scratch schema, so a second run against the
// same target must be refused before touching anything.
func TestIntegrationDrill_guardRefusesSecondRun(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcSchema := fmt.Sprintf("drillsrc2_%d", time.Now().UnixNano())
	dsn, baseDir, at := drillContractFixture(t, srcSchema)
	resetDrillGlobals(t)
	drlIndexDSN, drlBaselineDir = dsn, baseDir
	drlTables = srcSchema + ".orders"
	drlTargetDSN = testutil.BaseDSN() + "/"
	drlAt = at.Format("2006-01-02 15:04:05")
	drlFormat = "json"

	if err := runDrill(newQueryTestCmd(), nil); err != nil {
		t.Fatalf("first drill must pass: %v", err)
	}
	err := runDrill(newQueryTestCmd(), nil)
	if err == nil || !strings.Contains(err.Error(), "already has table") {
		t.Fatalf("second run against the now-populated target must be refused by the guard: %v", err)
	}
}

// TestIntegrationDrill_tableWithNoSnapshotFailsUnwritten runs the real drill
// over two tables, one of which has no snapshot. The verdict is what it
// always was: that table FAILS with binlog_only set, the other is loaded and
// passes, and the command exits non-zero. What changed is the dump folder:
// drill used to have the table built from its changes first and fail it
// afterwards, which left its partial files under a _SUCCESS marker in
// --output. Now nothing is written for it and the folder is _INCOMPLETE.
func TestIntegrationDrill_tableWithNoSnapshotFailsUnwritten(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcSchema := fmt.Sprintf("drillsrc3_%d", time.Now().UnixNano())
	dsn, baseDir, at := drillContractFixture(t, srcSchema)
	resetDrillGlobals(t)
	out := t.TempDir()
	drlIndexDSN, drlBaselineDir = dsn, baseDir
	drlTables = srcSchema + ".orders," + srcSchema + ".ghost"
	drlTargetDSN = testutil.BaseDSN() + "/"
	drlAt = at.Format("2006-01-02 15:04:05")
	drlFormat = "json"
	drlOutput = out

	var runErr error
	stdout := captureStdout(t, func() { runErr = runDrill(newQueryTestCmd(), nil) })
	t.Logf("exit error: %v\nfiles: %v\n%s", runErr, dirNames(t, out), stdout)

	wantErr := "drill: 1 of 2 table(s) FAILED the restore rehearsal; dump kept at " + out
	if runErr == nil || runErr.Error() != wantErr {
		t.Errorf("exit error = %v, want %q", runErr, wantErr)
	}
	var report drillReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("the report is not JSON: %v\n%s", err, stdout)
	}
	byTable := map[string]drillTableResult{}
	for _, r := range report.Tables {
		byTable[r.Table] = r
	}
	if len(report.Tables) != 2 {
		t.Fatalf("report has %d tables, want both", len(report.Tables))
	}
	if got := byTable["orders"]; got.Status != "pass" || got.BinlogOnly || got.RowsLoaded == 0 {
		t.Errorf("orders, which has its snapshot: %+v", got)
	}
	if got := byTable["ghost"]; got.Status != "fail" || !got.BinlogOnly || got.Error != drillNoSnapshotError || got.RowsLoaded != 0 {
		t.Errorf("ghost, which has no snapshot: %+v", got)
	}
	if !strings.Contains(stdout, `"binlog_only": true`) {
		t.Errorf("the JSON report lost binlog_only")
	}

	for _, name := range dirNames(t, out) {
		if strings.HasPrefix(name, srcSchema+".ghost") {
			t.Errorf("the dump folder holds %s, a file of the table that has no snapshot", name)
		}
	}
	if _, err := os.Stat(filepath.Join(out, baseline.IncompleteMarker)); err != nil {
		t.Errorf("the dump folder is not marked %s: %v", baseline.IncompleteMarker, err)
	}
	if _, err := os.Stat(filepath.Join(out, baseline.SuccessMarker)); err == nil {
		t.Errorf("the dump folder is marked %s", baseline.SuccessMarker)
	}
	if _, err := os.Stat(filepath.Join(out, baseline.ChangesOnlyMarkerFile)); err == nil {
		t.Errorf("the dump folder lists a changes-only table, and none was written")
	}
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	names := make([]string, len(ents))
	for i, e := range ents {
		names[i] = e.Name()
	}
	return names
}
