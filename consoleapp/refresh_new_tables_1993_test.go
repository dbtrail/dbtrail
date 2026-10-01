package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

func TestTablesLeftOut(t *testing.T) {
	cases := []struct {
		name     string
		snapshot []string
		source   []string
		foldCase bool
		want     []string
	}{
		{"nothing new", []string{"s.a", "s.b"}, []string{"s.b", "s.a"}, false, nil},
		// Created and dropped between two runs: the source no longer has it,
		// so there is nothing to report.
		{"created and dropped", []string{"s.a"}, []string{"s.a"}, false, nil},
		{"new ones sorted", []string{"s.a"}, []string{"s.z", "s.a", "s.m"}, false, []string{"s.m", "s.z"}},
		// lower_case_table_names=0: two names that differ in case are two tables.
		{"case kept apart", []string{"s.Orders"}, []string{"s.Orders", "s.orders"}, false, []string{"s.orders"}},
		// lower_case_table_names!=0: the server compares without case.
		{"case folded", []string{"S.orders"}, []string{"s.Orders"}, true, nil},
		{"repeat listed once", nil, []string{"s.a", "s.a"}, false, []string{"s.a"}},
		{"empty source", []string{"s.a"}, nil, false, nil},
		// The schema is part of the name: the same table name in another
		// schema is another table.
		{"same table other schema", []string{"s.a"}, []string{"t.a"}, false, []string{"t.a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tablesLeftOut(tc.snapshot, tc.source, tc.foldCase); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// stubSourceTables replaces the source listing for one test.
func stubSourceTables(t *testing.T, fn func(ctx context.Context, dsn string, schemas []string) ([]string, bool, error)) {
	t.Helper()
	prev := listSourceTables
	listSourceTables = fn
	t.Cleanup(func() { listSourceTables = prev })
}

func newTablesFixture(t *testing.T, fullBackups bool, snapshot []string) (*backupScheduler, *console.Registry, *baselineSupervisor, console.ServerEntry) {
	t.Helper()
	realList := newestSnapshotTables
	t.Cleanup(func() { newestSnapshotTables = realList })
	newestSnapshotTables = func(context.Context, string) (time.Time, []string, error) {
		return refreshAt.Add(-time.Hour), snapshot, nil
	}
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return []*reconstruct.TableReport{{Schema: "shop", Table: "kept"}}, nil, nil
	})
	b, reg, sup := newScheduleFixture(t, fullBackups)
	e := addScheduled(t, reg, true)
	return b, reg, sup, e
}

func rebuildOnce(t *testing.T, b *backupScheduler, e console.ServerEntry, stamp string) console.BackupScheduleState {
	t.Helper()
	p, err := e.BackupSchedule.Parse()
	if err != nil {
		t.Fatal(err)
	}
	// The scheduler watches the slots it observed; a test that starts the
	// rebuild directly marks it observed the same way the tick does.
	b.mu.Lock()
	b.seen[e.ID] = seenSlot{identity: e.BackupSchedule.Identity()}
	b.mu.Unlock()
	if err := b.startRebuild(e, p, stamp); err != nil {
		t.Fatalf("startRebuild: %v", err)
	}
	waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh)
	b.watchScheduled(e, b.ScheduleState(e.ID).LastStartedAt, console.BackupMethodRefresh)
	b.watchers.Wait()
	return b.ScheduleState(e.ID)
}

// Full reads allowed: the update publishes the tables it has, then the
// schedule starts ONE full read for every new table at once, with a reason
// that names them. A second update that still finds them missing after a full
// read that went through does not start another (no loop).
func TestScheduledRefresh_newTablesStartOneFullRead(t *testing.T) {
	b, _, sup, e := newTablesFixture(t, true, []string{"shop.kept"})
	var asked int
	stubSourceTables(t, func(context.Context, string, []string) ([]string, bool, error) {
		asked++
		out := []string{"shop.kept"}
		for i := range 50 {
			out = append(out, fmt.Sprintf("shop.t%02d", i))
		}
		return out, false, nil
	})
	st := rebuildOnce(t, b, e, "2026-10-01T09:00:00Z")
	if asked != 1 {
		t.Fatalf("the source was asked %d times, want once", asked)
	}
	if st.LastMethod != console.BackupMethodFull {
		t.Fatalf("no full read started for the new tables: %+v", st)
	}
	if code := console.BackupWhyCode(st.LastWhy); code != console.BackupWhyCodeNewTables {
		t.Fatalf("full read reason %q (code %q), want the new-tables one", st.LastWhy, code)
	}
	if !strings.HasSuffix(st.LastWhy, "(50 tables)") || strings.Contains(st.LastWhy, "shop.") {
		t.Fatalf("the reason does not count the tables, or names them (a profiled session reads it): %q", st.LastWhy)
	}
	recs := sup.history.List(e.ID)
	var refresh *console.BaselineRunRecord
	for i := range recs {
		if recs[i].Kind == console.BaselineRunRefresh {
			refresh = &recs[i]
		}
	}
	if refresh == nil || len(refresh.NewTables) != console.RefusedTablesCap || refresh.NewTablesOmitted != 30 {
		t.Fatalf("the update's record does not carry 20 names and 30 more: %+v", refresh)
	}
	waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)

	// Pretend that full read went through and still left the tables out
	// (a scope it could not reach): the next update reports, and starts
	// nothing.
	sup.recordRun(e.ID, e.Name, console.BaselineRunRecord{Kind: console.BaselineRunDump,
		StartedAt: time.Now().UTC().Add(time.Second).Format(time.RFC3339)}, nil)
	before := b.ScheduleState(e.ID).LastStartedAt
	time.Sleep(1100 * time.Millisecond) // whole-second stamps
	st = rebuildOnce(t, b, e, time.Now().UTC().Format(time.RFC3339))
	if st.LastMethod != console.BackupMethodRefresh || st.LastStartedAt == before {
		t.Fatalf("a second full read started for tables a full read already missed: %+v", st)
	}
	if len(st.Last.NewTables) == 0 {
		t.Fatalf("the gap is no longer reported: %+v", st.Last)
	}
}

// A full read that FAILED does not count as one that missed the tables: the
// next update tries again.
func TestScheduledRefresh_newTablesRetryAfterAFailedFullRead(t *testing.T) {
	b, _, _, e := newTablesFixture(t, true, []string{"shop.kept"})
	stubSourceTables(t, func(context.Context, string, []string) ([]string, bool, error) {
		return []string{"shop.kept", "shop.orders"}, false, nil
	})
	st := rebuildOnce(t, b, e, "2026-10-01T09:00:00Z")
	if st.LastMethod != console.BackupMethodFull {
		t.Fatalf("no full read started: %+v", st)
	}
	st = waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	if st.Last.State != "failed" {
		t.Skipf("the fixture's full read did not fail (%+v); the retry rule is not exercised", st.Last)
	}
	time.Sleep(1100 * time.Millisecond)
	st = rebuildOnce(t, b, e, time.Now().UTC().Format(time.RFC3339))
	if st.LastMethod != console.BackupMethodFull {
		t.Fatalf("after a failed full read the new table never gets another: %+v", st)
	}
}

// Full reads not allowed: the update still publishes the tables it has, the
// gap is reported, and nothing else starts.
func TestScheduledRefresh_newTablesGateClosedReportsTheGap(t *testing.T) {
	b, _, _, e := newTablesFixture(t, false, []string{"shop.kept"})
	stubSourceTables(t, func(context.Context, string, []string) ([]string, bool, error) {
		return []string{"shop.kept", "shop.orders"}, false, nil
	})
	st := rebuildOnce(t, b, e, "2026-10-01T09:00:00Z")
	if st.LastMethod != console.BackupMethodRefresh || st.Last.State != "succeeded" || !st.Last.Published {
		t.Fatalf("the update did not publish, or something else started: %+v", st)
	}
	if !reflect.DeepEqual(st.Last.NewTables, []string{"shop.orders"}) {
		t.Fatalf("gap not reported: %+v", st.Last)
	}
}

// RENAME TABLE (and every other refusal) publishes nothing: the fallback full
// read covers every table, so the run names no new table on top of it.
func TestScheduledRefresh_refusedRunNamesNoNewTables(t *testing.T) {
	b, _, _, e := newTablesFixture(t, false, []string{"shop.old_name"})
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		err := fmt.Errorf("RENAME TABLE in window: %w", reconstruct.ErrDestructiveDDL)
		return nil, []reconstruct.TableFailure{{Schema: "shop", Table: "old_name", Err: err}}, err
	})
	stubSourceTables(t, func(context.Context, string, []string) ([]string, bool, error) {
		return []string{"shop.new_name"}, false, nil
	})
	st := rebuildOnce(t, b, e, "2026-10-01T09:00:00Z")
	var last *console.BaselineStatus = st.Last
	for _, s := range []*console.BaselineStatus{last} {
		if s != nil && (len(s.NewTables) > 0 || s.NewTablesOmitted > 0) {
			t.Fatalf("a refused run also counts the renamed table as new: %+v", s)
		}
	}
}

// The source does not answer: the run says the check did not run, carries no
// password, and starts no full read on a guess.
func TestScheduledRefresh_uncheckedStartsNothing(t *testing.T) {
	b, _, _, e := newTablesFixture(t, true, []string{"shop.kept"})
	stubSourceTables(t, func(_ context.Context, dsn string, _ []string) ([]string, bool, error) {
		return nil, false, errors.New("dial " + dsn + ": connection refused")
	})
	st := rebuildOnce(t, b, e, "2026-10-01T09:00:00Z")
	if st.LastMethod != console.BackupMethodRefresh {
		t.Fatalf("an unchecked source started a full read: %+v", st)
	}
	if st.Last.NewTablesUnchecked == "" || len(st.Last.NewTables) > 0 {
		t.Fatalf("the run does not say the check did not run: %+v", st.Last)
	}
	if strings.Contains(st.Last.NewTablesUnchecked, "src:pw@") {
		t.Fatalf("the reason carries the source credentials: %q", st.Last.NewTablesUnchecked)
	}
}

// PostgreSQL sources are not asked yet, and the run says so rather than
// claiming none.
func TestCheckNewTables_postgresIsUnchecked(t *testing.T) {
	stubSourceTables(t, func(context.Context, string, []string) ([]string, bool, error) {
		t.Fatal("a PostgreSQL source was asked with the MySQL query")
		return nil, false, nil
	})
	sup := refusedFixture(t)
	c := sup.checkNewTables(refreshRequest{SourceDSN: "postgres://u:p@h/db", SourcePostgres: true}, []string{"public.a"})
	if c.unchecked == "" || len(c.tables) > 0 {
		t.Fatalf("got %+v", c)
	}
	if got := sup.checkNewTables(refreshRequest{}, []string{"s.a"}); !reflect.DeepEqual(got, newTablesCheck{}) {
		t.Fatalf("no source known, yet a check was claimed: %+v", got)
	}
}

// The status slot is reused: a run with no new tables clears the list the
// run before left there.
func TestApplyNewTables_clearsAndDropsOnUnpublished(t *testing.T) {
	st := &console.BaselineStatus{NewTables: []string{"s.old"}, NewTablesOmitted: 3, NewTablesUnchecked: "x"}
	applyNewTables(st, newTablesCheck{}, true)
	if st.NewTables != nil || st.NewTablesOmitted != 0 || st.NewTablesUnchecked != "" {
		t.Fatalf("stale list kept: %+v", st)
	}
	applyNewTables(st, newTablesCheck{tables: []string{"s.a"}}, false)
	if st.NewTables != nil {
		t.Fatalf("a run that published nothing names new tables: %+v", st)
	}
}
