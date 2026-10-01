//go:build integration

package consoleapp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #1993, against a real source: the previous snapshot holds one table, the
// source has gained more since. With full reads not allowed on this daemon
// the update still publishes, and the tables it left out are named in the
// loop's status and in the run history, the two places the pages read. The
// view, the table of another schema and the table the snapshot already holds
// are never named.
func TestScheduledRefresh_namesTablesCreatedAfterThePreviousSnapshot(t *testing.T) {
	src, srcName := testutil.CreateTestDB(t)
	other, otherName := testutil.CreateTestDB(t)
	for _, q := range []string{
		"CREATE TABLE kept (id INT PRIMARY KEY)",
		"CREATE TABLE orders (id INT PRIMARY KEY)",
		"CREATE TABLE Devices (id INT PRIMARY KEY)",
		"CREATE TABLE nopk (v INT)",
		"CREATE VIEW v_orders AS SELECT id FROM orders",
	} {
		if _, err := src.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if _, err := other.Exec("CREATE TABLE elsewhere (id INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	realList := newestSnapshotTables
	t.Cleanup(func() { newestSnapshotTables = realList })
	newestSnapshotTables = func(context.Context, string) (time.Time, []string, error) {
		return refreshAt.Add(-time.Hour), []string{srcName + ".kept"}, nil
	}
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return []*reconstruct.TableReport{{Schema: srcName, Table: "kept"}}, nil, nil
	})

	b, reg, sup := newScheduleFixture(t, false) // full reads not allowed: the gap is reported, nothing else
	e := addScheduled(t, reg, true)
	e.SourceDSN = testutil.IntegrationDSN(srcName)
	e.Schemas = srcName
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	p, err := e.BackupSchedule.Parse()
	if err != nil {
		t.Fatal(err)
	}
	if err := b.startRebuild(e, p, "2026-10-01T09:00:00Z"); err != nil {
		t.Fatalf("startRebuild: %v", err)
	}
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh)
	if st.Last.State != "succeeded" {
		t.Fatalf("the update did not publish the tables it had: %+v", st.Last)
	}

	status, _ := json.Marshal(st.Last)
	recs := sup.history.List(e.ID)
	if len(recs) == 0 {
		t.Fatal("no run recorded")
	}
	record, _ := json.Marshal(recs[len(recs)-1])
	for what, raw := range map[string]string{"status": string(status), "history": string(record)} {
		for _, want := range []string{srcName + ".orders", srcName + ".Devices", srcName + ".nopk"} {
			if !strings.Contains(raw, `"`+want+`"`) {
				t.Errorf("%s does not name %s, a table the source has and the snapshot does not: %s", what, want, raw)
			}
		}
		for _, never := range []string{"v_orders", otherName + ".elsewhere", `"` + srcName + `.kept"`} {
			if strings.Contains(raw, never) {
				t.Errorf("%s names %s, which is not a table left out: %s", what, never, raw)
			}
		}
	}
}

// The source cannot be reached when the update asks it for its tables: the
// run says the check did not happen, and never reads as "no new tables".
func TestScheduledRefresh_unreachableSourceSaysTheCheckDidNotRun(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	realList := newestSnapshotTables
	t.Cleanup(func() { newestSnapshotTables = realList })
	newestSnapshotTables = func(context.Context, string) (time.Time, []string, error) {
		return refreshAt.Add(-time.Hour), []string{"shop.kept"}, nil
	}
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return []*reconstruct.TableReport{{Schema: "shop", Table: "kept"}}, nil, nil
	})
	b, reg, _ := newScheduleFixture(t, false)
	e := addScheduled(t, reg, true)
	e.SourceDSN = "root:s3cr3t-password@tcp(127.0.0.1:1)/" // nothing listens on port 1
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	p, _ := e.BackupSchedule.Parse()
	if err := b.startRebuild(e, p, "2026-10-01T09:00:00Z"); err != nil {
		t.Fatalf("startRebuild: %v", err)
	}
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh)
	raw, _ := json.Marshal(st.Last)
	if !strings.Contains(string(raw), `"new_tables_unchecked"`) {
		t.Fatalf("an unreachable source left no word that the check did not run: %s", raw)
	}
	if strings.Contains(string(raw), "s3cr3t-password") {
		t.Fatalf("the reason carries the source password: %s", raw)
	}
	if len(st.LastFallbackAt) != 0 {
		t.Fatalf("an unchecked source started a full read: %+v", st)
	}
}
