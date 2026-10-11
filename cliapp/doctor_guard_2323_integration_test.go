//go:build integration

package cliapp

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2323, the doctor command's own wiring on a real index: with no --retain,
// assumedRetain reads the window the index rotates on AND the loop's upgrade
// guard for it, and the report says nothing is being dropped instead of
// projecting over that window. The index here is the guard's first shape: no
// retention record, history 100 days deep.
func TestDoctor_assumedWindowHeldBackByTheUpgradeGuard(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	now := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{now.Add(-100 * 24 * time.Hour), now})
	dsn := testutil.IntegrationDSN(dbName)

	retain, note, guard := assumedRetain(context.Background(), dsn, 48*time.Hour)
	if retain != 30*24*time.Hour || note == "" {
		t.Fatalf("assumed window = %v (%q), want the legacy 30d with its source", retain, note)
	}
	if guard == nil || !guard.Blocked {
		t.Fatalf("guard = %+v, want blocked: 100 days of history under a 30d window nobody chose", guard)
	}

	// The source is the same server: a report whose source does not answer
	// ends there and never reaches the index checks.
	var buf bytes.Buffer
	_ = runDoctorTo(context.Background(), &buf, "text", dsn, dsn, "", retain, note, guard, "", "", "")
	out := buf.String()
	if !strings.Contains(out, "Index disk capacity") {
		t.Fatalf("the report never reached the index capacity check, so it proves nothing:\n%s", out)
	}
	// An empty index has no write rate, so the check skips on history, and
	// the skip must still carry the guard's sentence.
	if !strings.Contains(out, "drops none of it") && !strings.Contains(out, "does not drop that") {
		t.Fatalf("the report does not say the loop is holding the history back:\n%s", out)
	}

	// Without the guard's verdict the same report says nothing of it: the
	// sentence comes from the wiring, not from the index.
	buf.Reset()
	_ = runDoctorTo(context.Background(), &buf, "text", dsn, dsn, "", retain, note, nil, "", "", "")
	if !strings.Contains(buf.String(), "not enough recent history") {
		t.Fatalf("the control report never reached the index capacity check:\n%s", buf.String())
	}
	if strings.Contains(buf.String(), "drops none of it") || strings.Contains(buf.String(), "does not drop that") {
		t.Fatalf("a report built without the guard still claims it:\n%s", buf.String())
	}
}
