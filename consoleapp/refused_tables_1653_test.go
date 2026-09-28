package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// refusingFold stubs the two seams of a whole update: the snapshot lists
// tables, and the fold refuses the ones in failures. A nil failures is a fold
// that goes through.
func refusingFold(t *testing.T, tables []string, failures func(cfg reconstruct.FullTableConfig) []reconstruct.TableFailure) {
	t.Helper()
	realList, realFold := newestSnapshotTables, foldTables
	t.Cleanup(func() { newestSnapshotTables, foldTables = realList, realFold })
	newestSnapshotTables = func(context.Context, string) (time.Time, []string, error) {
		return refreshAt.Add(-time.Hour), tables, nil
	}
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		var failed []reconstruct.TableFailure
		if failures != nil {
			failed = failures(cfg)
		}
		if len(failed) == 0 {
			return []*reconstruct.TableReport{{Schema: "shop", Table: "orders"}}, nil, nil
		}
		var errs []error
		for _, f := range failed {
			errs = append(errs, f.Err)
		}
		return []*reconstruct.TableReport{{Schema: "shop", Table: "users", CarriedForward: true}}, failed, errors.Join(errs...)
	}
}

func refusedFixture(t *testing.T) *baselineSupervisor {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	h, err := console.OpenBaselineHistory(t.TempDir() + "/h.json")
	if err != nil {
		t.Fatal(err)
	}
	sup.history = h
	return sup
}

func runOnce(sup *baselineSupervisor, id, dir string) console.BaselineStatus {
	sup.mu.Lock()
	sup.refreshes[id] = &console.BaselineStatus{State: "running"}
	sup.mu.Unlock()
	sup.runRefresh(refreshRequest{ServerID: id, ServerName: id, BaselineDir: dir, Trigger: console.BaselineRunTriggerScheduled,
		IndexDSN: "idx:s3cr3t-pw@tcp(db.internal:3306)/bintrail_index"}, refreshAt, time.Hour)
	return sup.RefreshStatus(id)
}

func lastRecord(t *testing.T, sup *baselineSupervisor, id string) console.BaselineRunRecord {
	t.Helper()
	recs := sup.history.List(id)
	if len(recs) == 0 {
		t.Fatalf("no run recorded for %s", id)
	}
	return recs[len(recs)-1]
}

// The whole path, from the fold's per-table failures to the status the page
// polls and the record on disk: which tables, which verdict, which reason,
// decided by the rule the command line prints from.
func TestRunRefresh_saysWhichTablesRefusedAndWhy(t *testing.T) {
	tables := []string{"shop.orders", "shop.users", "crm.orders", "shop.with.dot"}
	refusingFold(t, tables, func(cfg reconstruct.FullTableConfig) []reconstruct.TableFailure {
		return []reconstruct.TableFailure{
			// Reported out of the run's order, and the same table name in
			// two schemas: each keeps its own verdict.
			{Schema: "crm", Table: "orders", Err: fmt.Errorf("crm.orders: lost 10:00 to 10:05 reading %s: %w", cfg.IndexDSN, reconstruct.ErrCaptureGap)},
			{Schema: "shop", Table: "orders", Err: fmt.Errorf("shop.orders: column added\nsecond line: %w", reconstruct.ErrSchemaChanged)},
			{Schema: "shop", Table: "with.dot", Err: errors.New("shop.with.dot: no space left on device")},
		}
	})
	sup := refusedFixture(t)
	st := runOnce(sup, "a", t.TempDir())

	want := []console.RefusedTable{
		{Name: "shop.orders", Verdict: "refused-ddl", Reason: "shop.orders: column added second line: schema changed since the baseline"},
		{Name: "crm.orders", Verdict: "refused-gap", Reason: "crm.orders: lost 10:00 to 10:05 reading <dsn>: capture gap in the reconstruction window"},
		{Name: "shop.with.dot", Verdict: "refused", Reason: "shop.with.dot: no space left on device"},
	}
	if st.State != "failed" || st.Published || st.Tables != 4 || st.Refused != 3 {
		t.Fatalf("status = %+v, want a failed run of 4 tables that published nothing, 3 refused", st)
	}
	if !reflect.DeepEqual(st.RefusedTables, want) || st.RefusedTablesOmitted != 0 {
		t.Errorf("status list = %+v\nwant %+v", st.RefusedTables, want)
	}
	rec := lastRecord(t, sup, "a")
	if !reflect.DeepEqual(rec.RefusedTables, want) || rec.Refused != 3 || rec.RefusedTablesOmitted != 0 {
		t.Errorf("recorded list = %+v (refused %d)\nwant %+v", rec.RefusedTables, rec.Refused, want)
	}
	// The error text is the fold's own, word for word: operators alert on it.
	if !strings.Contains(st.LastError, "shop.with.dot: no space left on device") || st.LastError != rec.Error {
		t.Errorf("the run's error changed: status %q, record %q", st.LastError, rec.Error)
	}
	for _, e := range append(st.RefusedTables, rec.RefusedTables...) {
		if strings.Contains(e.Reason, "s3cr3t-pw") {
			t.Errorf("a reason carries the index password: %q", e.Reason)
		}
	}
}

// Each run reports its own list. A run that goes through after a refused one
// reports none; a run that fails before any table is read reports none, and
// does not inherit the list of the run before it.
func TestRunRefresh_theListIsThisRunsNotTheOneBefore(t *testing.T) {
	var refuse []reconstruct.TableFailure
	refusingFold(t, []string{"shop.orders", "shop.users"}, func(reconstruct.FullTableConfig) []reconstruct.TableFailure { return refuse })
	sup := refusedFixture(t)
	dir := t.TempDir()

	refuse = []reconstruct.TableFailure{{Schema: "shop", Table: "orders", Err: fmt.Errorf("first: %w", reconstruct.ErrSchemaChanged)}}
	if st := runOnce(sup, "a", dir); len(st.RefusedTables) != 1 || st.RefusedTables[0].Name != "shop.orders" {
		t.Fatalf("first run = %+v", st)
	}
	refuse = []reconstruct.TableFailure{{Schema: "shop", Table: "users", Err: fmt.Errorf("second: %w", reconstruct.ErrCaptureGap)}}
	st := runOnce(sup, "a", dir)
	if len(st.RefusedTables) != 1 || st.RefusedTables[0] != (console.RefusedTable{Name: "shop.users", Verdict: "refused-gap", Reason: "second: capture gap in the reconstruction window"}) {
		t.Fatalf("second run reports %+v, want only shop.users", st.RefusedTables)
	}
	if rec := lastRecord(t, sup, "a"); len(rec.RefusedTables) != 1 || rec.RefusedTables[0].Name != "shop.users" {
		t.Fatalf("second record = %+v", rec.RefusedTables)
	}

	// Set by hand on the status slot, the way a finished run leaves it, so
	// the next two runs start from a slot that holds a list.
	stale := func() {
		sup.mu.Lock()
		sup.refreshes["a"] = &console.BaselineStatus{State: "running", Refused: 1,
			RefusedTables: []console.RefusedTable{{Name: "shop.users", Verdict: "refused-gap"}}, RefusedTablesOmitted: 7}
		sup.mu.Unlock()
	}
	refuse = nil
	stale()
	sup.runRefresh(refreshRequest{ServerID: "a", ServerName: "a", BaselineDir: dir, IndexDSN: "d"}, refreshAt.Add(time.Minute), time.Hour)
	if st := sup.RefreshStatus("a"); st.State != "succeeded" || st.RefusedTables != nil || st.RefusedTablesOmitted != 0 || st.Refused != 0 {
		t.Errorf("a run that went through still names refused tables: %+v", st)
	}
	if rec := lastRecord(t, sup, "a"); rec.RefusedTables != nil || rec.Error != "" {
		t.Errorf("its record = %+v", rec)
	}

	newestSnapshotTables = func(context.Context, string) (time.Time, []string, error) {
		return time.Time{}, nil, errors.New("the bucket did not answer")
	}
	stale()
	sup.runRefresh(refreshRequest{ServerID: "a", ServerName: "a", BaselineDir: dir, IndexDSN: "d"}, refreshAt.Add(2*time.Minute), time.Hour)
	st = sup.RefreshStatus("a")
	if st.State != "failed" || !strings.Contains(st.LastError, "the bucket did not answer") {
		t.Fatalf("status = %+v, want the listing's failure", st)
	}
	if st.RefusedTables != nil || st.RefusedTablesOmitted != 0 || st.Refused != 0 {
		t.Errorf("a run that failed before reading any table names refused tables: %+v", st)
	}
	if rec := lastRecord(t, sup, "a"); rec.RefusedTables != nil || rec.RefusedTablesOmitted != 0 {
		t.Errorf("its record = %+v", rec)
	}
}

// Two servers refused in the same minute: each reports its own table.
func TestRunRefresh_twoServersKeepTheirOwnTables(t *testing.T) {
	refusingFold(t, []string{"shop.orders", "crm.leads"}, func(cfg reconstruct.FullTableConfig) []reconstruct.TableFailure {
		if strings.HasSuffix(cfg.OutputDir, "-a") {
			return []reconstruct.TableFailure{{Schema: "shop", Table: "orders", Err: fmt.Errorf("a: %w", reconstruct.ErrSchemaChanged)}}
		}
		return []reconstruct.TableFailure{{Schema: "crm", Table: "leads", Err: fmt.Errorf("b: %w", reconstruct.ErrCaptureGap)}}
	})
	sup := refusedFixture(t)
	stA := runOnce(sup, "a", t.TempDir()+"-a")
	stB := runOnce(sup, "b", t.TempDir()+"-b")
	stA = sup.RefreshStatus("a") // read again, after b finished
	for _, tc := range []struct {
		id   string
		st   console.BaselineStatus
		want console.RefusedTable
	}{
		{"a", stA, console.RefusedTable{Name: "shop.orders", Verdict: "refused-ddl", Reason: "a: schema changed since the baseline"}},
		{"b", stB, console.RefusedTable{Name: "crm.leads", Verdict: "refused-gap", Reason: "b: capture gap in the reconstruction window"}},
	} {
		if len(tc.st.RefusedTables) != 1 || tc.st.RefusedTables[0] != tc.want {
			t.Errorf("server %s status = %+v, want only %+v", tc.id, tc.st.RefusedTables, tc.want)
		}
		if rec := lastRecord(t, sup, tc.id); len(rec.RefusedTables) != 1 || rec.RefusedTables[0] != tc.want {
			t.Errorf("server %s record = %+v, want only %+v", tc.id, rec.RefusedTables, tc.want)
		}
	}
}

// 5,000 refused tables: the status and the record keep the first by name and
// count the rest, so the history file does not grow with the table count.
func TestRunRefresh_aLongListIsCappedAndCounted(t *testing.T) {
	var tables []string
	var failed []reconstruct.TableFailure
	for i := range 5000 {
		name := fmt.Sprintf("t%04d", i)
		tables = append(tables, "shop."+name)
		failed = append(failed, reconstruct.TableFailure{Schema: "shop", Table: name, Err: fmt.Errorf("shop.%s: %w", name, reconstruct.ErrCaptureGap)})
	}
	refusingFold(t, tables, func(reconstruct.FullTableConfig) []reconstruct.TableFailure { return failed })
	sup := refusedFixture(t)
	st := runOnce(sup, "a", t.TempDir())
	if st.Refused != 5000 || len(st.RefusedTables) != console.RefusedTablesCap || st.RefusedTablesOmitted != 5000-console.RefusedTablesCap {
		t.Fatalf("status: refused %d, listed %d, omitted %d", st.Refused, len(st.RefusedTables), st.RefusedTablesOmitted)
	}
	rec := lastRecord(t, sup, "a")
	if len(rec.RefusedTables) != console.RefusedTablesCap || rec.RefusedTablesOmitted != 5000-console.RefusedTablesCap {
		t.Fatalf("record: listed %d, omitted %d", len(rec.RefusedTables), rec.RefusedTablesOmitted)
	}
}

// withRefusedTables, the seam itself.
func TestWithRefusedTables(t *testing.T) {
	refused := []reconstruct.RefreshOutcome{{Table: "shop.orders", Verdict: reconstruct.RefreshVerdictRefusedDDL, Detail: "changed"}}
	if err := withRefusedTables(nil, refused); err != nil {
		t.Errorf("a run that published got an error: %v", err)
	}
	if kept, omitted := refusedTablesIn(nil); kept != nil || omitted != 0 {
		t.Errorf("no error carries %+v", kept)
	}
	plain := errors.New("index unreachable")
	if err := withRefusedTables(plain, []reconstruct.RefreshOutcome{{Table: "shop.orders", Verdict: reconstruct.RefreshVerdictSkipped}}); err != plain {
		t.Errorf("a failure that was no table's was wrapped: %v", err)
	}
	if kept, _ := refusedTablesIn(plain); kept != nil {
		t.Errorf("a plain error carries %+v", kept)
	}

	// A run that was stopped: its tables report the stop, none refused.
	stopped := errors.Join(fmt.Errorf("shop.orders: %w", context.Canceled), fmt.Errorf("shop.users: %w", context.Canceled))
	if kept, _ := refusedTablesIn(withRefusedTables(stopped, []reconstruct.RefreshOutcome{
		{Table: "shop.orders", Verdict: reconstruct.RefreshVerdictRefused, Detail: "context canceled"}})); kept != nil {
		t.Errorf("a run that was stopped names tables as refused: %+v", kept)
	}

	run := fmt.Errorf("shop.orders: %w", reconstruct.ErrTouchedRowBudget)
	err := withRefusedTables(run, refused)
	if err.Error() != run.Error() {
		t.Errorf("the text changed: %q, want %q", err.Error(), run.Error())
	}
	if !errors.Is(err, reconstruct.ErrTouchedRowBudget) || !errors.Is(err, run) {
		t.Error("the sentinels are no longer reachable through the list")
	}
	// Wrapped again further up, the way the upload and the listing wrap.
	if kept, _ := refusedTablesIn(fmt.Errorf("refresh: %w", err)); len(kept) != 1 || kept[0].Name != "shop.orders" {
		t.Errorf("the list did not survive a wrap: %+v", kept)
	}
}

// applyFoldStatus is shared with the restore, whose status slot is reused the
// same way.
func TestApplyFoldStatus_setsAndClearsTheList(t *testing.T) {
	st := &console.BaselineStatus{State: "running"}
	refusal := withRefusedTables(errors.New("refused"),
		[]reconstruct.RefreshOutcome{{Table: "shop.orders", Verdict: reconstruct.RefreshVerdictRefusedGap, Detail: "gap"}})
	applyFoldStatus(st, 3, 1, reuseTally{}, refusal)
	if len(st.RefusedTables) != 1 || st.RefusedTables[0] != (console.RefusedTable{Name: "shop.orders", Verdict: "refused-gap", Reason: "gap"}) {
		t.Fatalf("status = %+v", st)
	}
	applyFoldStatus(st, 3, 0, reuseTally{}, nil)
	if st.RefusedTables != nil || st.RefusedTablesOmitted != 0 {
		t.Errorf("a run that went through kept the list of the one before: %+v", st)
	}
}

// A job that hit an internal error reports nothing about tables.
func TestFailPanickedJob_dropsTheList(t *testing.T) {
	sup := refusedFixture(t)
	sup.refreshes["a"] = &console.BaselineStatus{State: "running", Refused: 1,
		RefusedTables: []console.RefusedTable{{Name: "shop.orders", Verdict: "refused-gap"}}, RefusedTablesOmitted: 2}
	captureLog(t, slog.LevelError)
	sup.failPanickedJob(baselineJobRefresh, "a", "a", "boom")
	if st := sup.RefreshStatus("a"); st.State != "failed" || st.RefusedTables != nil || st.RefusedTablesOmitted != 0 {
		t.Errorf("status = %+v", st)
	}
}

// A scheduled update the fold refused falls back to a full read. The alarm
// the page shows for it names the tables that caused it.
func TestBackupScheduler_theFallbackNamesTheTables(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		err := fmt.Errorf("shop.orders: %w", reconstruct.ErrSchemaChanged)
		return nil, []reconstruct.TableFailure{{Schema: "shop", Table: "orders", Err: err}}, err
	})
	b, reg, _ := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	e.SourceDSN = "not a dsn" // the fallback full read fails fast
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	st := b.ScheduleState(e.ID)
	want := []console.RefusedTable{{Name: "shop.orders", Verdict: "refused-ddl", Reason: "shop.orders: schema changed since the baseline"}}
	if st.LastFallbackAt == "" || !reflect.DeepEqual(st.LastFallbackRefusedTables, want) {
		t.Fatalf("fallback = %+v, want it to name %+v", st, want)
	}
	if st.LastFallbackTables != 1 || st.LastFallbackRefused != 1 || st.LastFallbackRefusedOmitted != 0 {
		t.Errorf("fallback counts: tables %d, refused %d, omitted %d", st.LastFallbackTables, st.LastFallbackRefused, st.LastFallbackRefusedOmitted)
	}
}
