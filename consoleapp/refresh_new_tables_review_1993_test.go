package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// realTablesCreatedSince is the production index read, taken before init
// below replaces it: unit tests run the quiet-server gate against a DSN that
// reaches nothing, where the real read would answer "could not ask" and fold
// every cycle. The integration test runs this one against a real index.
var realTablesCreatedSince = tablesCreatedSince

func init() {
	tablesCreatedSince = func(context.Context, string, time.Time) (bool, error) { return false, nil }
}

// A skipped slot is written to the history as a dump-kind record with no
// error (AppendSkip). It is not a full read that went through, so it must not
// stop a retry after a full read that failed.
func TestScheduledRefresh_aSkippedSlotIsNotAFullReadThatWentThrough(t *testing.T) {
	b, _, sup, e := newTablesFixture(t, true, []string{"shop.kept"})
	stubSourceTables(t, func(context.Context, string, []string) ([]string, bool, error) {
		return []string{"shop.kept", "shop.orders"}, false, nil
	})
	st := rebuildOnce(t, b, e, "2026-10-01T09:00:00Z")
	if st.LastMethod != console.BackupMethodFull {
		t.Fatalf("no full read started: %+v", st)
	}
	if st = waitTerminalMethod(t, b, e.ID, console.BackupMethodFull); st.Last.State != "failed" {
		t.Fatalf("the fixture's full read did not fail (%+v)", st.Last)
	}
	if _, err := sup.history.AppendSkip(console.BaselineRunRecord{ServerID: e.ID, ServerName: e.Name, Kind: console.BaselineRunDump,
		StartedAt: time.Now().UTC().Add(time.Second).Format(time.RFC3339), SkipReason: "another snapshot job was running for this server at the scheduled time"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if st = rebuildOnce(t, b, e, time.Now().UTC().Format(time.RFC3339)); st.LastMethod != console.BackupMethodFull {
		t.Fatalf("a skipped slot counted as a full read that went through; no retry: %+v", st)
	}
}

// The comparison with what a full read was already tried for uses every new
// table, not the 20 names kept for display: a table sorting after the first
// 20 is still a table no full read was tried for.
func TestScheduledRefresh_aNewTablePastTheDisplayCapStillCounts(t *testing.T) {
	b, _, sup, e := newTablesFixture(t, true, []string{"shop.kept"})
	source := []string{"shop.kept"}
	for i := range 25 {
		source = append(source, fmt.Sprintf("shop.t%02d", i))
	}
	stubSourceTables(t, func(context.Context, string, []string) ([]string, bool, error) {
		return source, false, nil
	})
	if st := rebuildOnce(t, b, e, "2026-10-01T09:00:00Z"); st.LastMethod != console.BackupMethodFull {
		t.Fatalf("no full read started: %+v", st)
	}
	waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	// That full read went through and still lacks them.
	sup.recordRun(e.ID, e.Name, console.BaselineRunRecord{Kind: console.BaselineRunDump,
		StartedAt: time.Now().UTC().Add(time.Second).Format(time.RFC3339)}, nil)
	time.Sleep(1100 * time.Millisecond)
	source = append(source, "shop.zz_new") // sorts after the first 20
	if st := rebuildOnce(t, b, e, time.Now().UTC().Format(time.RFC3339)); st.LastMethod != console.BackupMethodFull {
		t.Fatalf("a new table past the first 20 names never gets a full read: %+v", st)
	}
}

// The daemon-wide loop hands each registry server's source and schema scope to
// its update; without them every periodic update says "not checked" forever.
func TestBaselineRefreshTargets_carryTheSource(t *testing.T) {
	entries := []console.ServerEntry{{ID: "s1", Name: "one", DSN: "idx:pw@tcp(h:3306)/idx", BaselineDir: t.TempDir(),
		SourceDSN: "src:pw@tcp(src:3306)/", Schemas: "shop, crm"}}
	got, _, _ := baselineRefreshTargets(entries, "", "")
	if len(got) != 1 {
		t.Fatalf("targets: %+v", got)
	}
	if got[0].SourceDSN != "src:pw@tcp(src:3306)/" || fmt.Sprint(got[0].Schemas) != "[shop crm]" || got[0].SourcePostgres {
		t.Fatalf("the source did not reach the update: %+v", got[0])
	}
}

// An update that fails right after one that reported new tables does not
// clear them: the newest copy still lacks them (#1993 review).
func TestRunRefresh_aFailedUpdateKeepsTheList(t *testing.T) {
	sup := refusedFixture(t)
	stubSourceTables(t, func(context.Context, string, []string) ([]string, bool, error) {
		return []string{"shop.orders", "shop.users"}, false, nil
	})
	fail := false
	refusingFold(t, []string{"shop.orders"}, func(reconstruct.FullTableConfig) []reconstruct.TableFailure {
		if !fail {
			return nil
		}
		return []reconstruct.TableFailure{{Schema: "shop", Table: "orders", Err: fmt.Errorf("gap: %w", reconstruct.ErrCaptureGap)}}
	})
	run := func() console.BaselineStatus {
		if _, err := sup.TriggerRefresh(refreshRequest{ServerID: "s", ServerName: "s", BaselineDir: t.TempDir(), IndexDSN: "idx",
			SourceDSN: "src:pw@tcp(h:3306)/"}, time.Hour); err != nil {
			t.Fatal(err)
		}
		for deadline := time.Now().Add(10 * time.Second); ; {
			if st := sup.RefreshStatus("s"); st.State != "running" {
				return st
			}
			if time.Now().After(deadline) {
				t.Fatal("refresh did not finish")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	first := run()
	if first.State != "succeeded" || !reflect.DeepEqual(first.NewTables, []string{"shop.users"}) || first.NewTablesSnapshot == "" {
		t.Fatalf("first update: %+v", first)
	}
	fail = true
	second := run()
	if second.State != "failed" {
		t.Fatalf("second update did not fail: %+v", second)
	}
	if !reflect.DeepEqual(second.NewTables, []string{"shop.users"}) || second.NewTablesSnapshot != first.NewTablesSnapshot {
		t.Fatalf("a failed update cleared the list the newest copy still needs: %+v", second)
	}
}

// A CREATE TABLE writes no row, so a quiet server's mark does not move; the
// gate must not skip the update that would look for it (#1993 review).
func TestRefreshCanSkip_aCreatedTableIsNotQuiet(t *testing.T) {
	stubCoverage(t, true, true)
	mark := indexMark{events: 100, schemaChanges: 7}
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := refreshRequest{ServerID: "s", ServerName: "shop", IndexDSN: "d"}
	seedQuietServer(sup, req, mark)
	if !sup.refreshCanSkip(context.Background(), req, mark, refreshAt, 0) {
		t.Fatal("fixture: a quiet server with nothing created was not skipped")
	}
	prev := tablesCreatedSince
	t.Cleanup(func() { tablesCreatedSince = prev })
	tablesCreatedSince = func(context.Context, string, time.Time) (bool, error) { return true, nil }
	if sup.refreshCanSkip(context.Background(), req, mark, refreshAt, 0) {
		t.Fatal("skipped although a table was created since the last snapshot")
	}
	tablesCreatedSince = func(context.Context, string, time.Time) (bool, error) { return false, errors.New("index down") }
	if sup.refreshCanSkip(context.Background(), req, mark, refreshAt, 0) {
		t.Fatal("skipped although the index could not say whether a table was created")
	}
}

// The planner promised a full read, and it could not start (another snapshot
// job holds the server): the live status stops saying one was started.
func TestIncludeNewTables_aFullReadThatDidNotStartIsNotPromised(t *testing.T) {
	b, _, sup, e := newTablesFixture(t, true, []string{"shop.kept"})
	b.mu.Lock()
	b.seen[e.ID] = seenSlot{identity: e.BackupSchedule.Identity()}
	b.newTablesPending[e.ID] = newTablesTry{names: map[string]bool{"shop.orders": true}}
	b.mu.Unlock()
	done := console.BaselineStatus{State: "running", NewTables: []string{"shop.orders"}, NewTablesAction: console.NewTablesActionFullRead}
	sup.mu.Lock()
	cp := done
	sup.refreshes[e.ID] = &cp // a job holds the server
	sup.mu.Unlock()
	b.includeNewTables(e, done)
	if got := sup.RefreshStatus(e.ID).NewTablesAction; got != "" {
		t.Fatalf("the status still promises a full read that did not start: %q", got)
	}
}
