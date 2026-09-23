//go:build integration

package cli

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRunStatus_namesUncapturedTables (#1802): `bintrail status` names every
// table the current schema snapshot left out, in both formats, through the
// real command.
func TestRunStatus_namesUncapturedTables(t *testing.T) {
	ctx := context.Background()
	source, src := testutil.CreateTestDB(t)
	index, name := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, index, 4, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := indexer.EnsureSchema(index); err != nil {
		t.Fatal(err)
	}
	testutil.MustExec(t, source, "CREATE TABLE orders (id INT PRIMARY KEY) ENGINE=InnoDB")
	testutil.MustExec(t, source, "CREATE TABLE audit_log (v INT) ENGINE=InnoDB")
	if _, err := metadata.TakeSnapshotExcludingInvalid(source, index, []string{src}); err != nil {
		t.Fatal(err)
	}

	saved := struct {
		dsn, format, baselineDir string
		failOnGap, ack           bool
	}{stIndexDSN, stFormat, stBaselineDir, stFailOnGap, stAckCaptureSkips}
	t.Cleanup(func() {
		stIndexDSN, stFormat, stBaselineDir = saved.dsn, saved.format, saved.baselineDir
		stFailOnGap, stAckCaptureSkips = saved.failOnGap, saved.ack
	})
	stIndexDSN, stBaselineDir, stFailOnGap, stAckCaptureSkips = testutil.IntegrationDSN(name), "", false, false
	statusCmd.SetContext(ctx)

	stFormat = "text"
	out := captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err != nil {
			t.Fatal(err)
		}
	})
	// `bintrail status` reads an index it did not start, so it cannot know
	// which tables that capture watches (nothing records a --tables filter).
	// It names what the schema read left out, with the statement that fixes
	// it, and claims no coverage count.
	for _, want := range []string{
		"=== Tables captured ===",
		src + ".audit_log is not captured: no primary key.",
		"ALTER TABLE `" + src + "`.`audit_log` ADD COLUMN `id`",
		"is set where capture runs",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("text report lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "Capturing") {
		t.Errorf("the command claimed a coverage count it cannot verify:\n%s", out)
	}

	stFormat = "json"
	out = captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err != nil {
			t.Fatal(err)
		}
	})
	var got struct {
		TableCapture *status.TableCaptureView `json:"table_capture"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, out)
	}
	tc := got.TableCapture
	if tc == nil || tc.State != status.TableCaptureChecked || len(tc.Uncaptured) != 1 || tc.Uncaptured[0].Table != "audit_log" ||
		tc.Uncaptured[0].Kind != status.UncapturedNoPrimaryKey {
		t.Errorf("table_capture = %+v", tc)
	}
	if tc.TablesCaptured != nil || tc.TablesTotal != nil || tc.CoverageNote == "" {
		t.Errorf("the JSON claims a count the command cannot verify: %+v", tc)
	}
}
