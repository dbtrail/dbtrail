package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/spf13/cobra"
)

// #2640, part 4: a scheduled update refused for tables that a read of their
// own fixes reads only those tables again, instead of the whole server. Every
// way that read can fail ends where it ends today: the full read in place of
// the refused update, under the same daily cap, with the same words.
//
// The dump is a stub, the conversion and the staged files are real, the fold
// is replaced so each case can say which tables it refuses and whether a
// staged read of a table cures it.

var rereadSchedTables = []string{"demo.mydumper_0", "demo.mydumper_1", "demo.orders", "demo.plain"}

// truncated is the refusal a TRUNCATE in the update's window gives a table.
func truncated(table string) reconstruct.TableFailure {
	return reconstruct.TableFailure{Schema: "demo", Table: table,
		Err: fmt.Errorf("demo.%s: TRUNCATE TABLE in the window: %w", table, reconstruct.ErrDestructiveDDL)}
}

func renumbered(table string) reconstruct.TableFailure {
	return reconstruct.TableFailure{Schema: "demo", Table: table,
		Err: fmt.Errorf("demo.%s: binlog.000001 then mysql-bin.000003: %w", table, reconstruct.ErrBinlogRenumbered)}
}

func gapIn(table string) reconstruct.TableFailure {
	return reconstruct.TableFailure{Schema: "demo", Table: table,
		Err: fmt.Errorf("demo.%s: %w", table, reconstruct.ErrCaptureGap)}
}

type rereadSched struct {
	b      *backupScheduler
	reg    *console.Registry
	sup    *baselineSupervisor
	e      console.ServerEntry
	cancel context.CancelFunc
	reads  *fakeFullReads
	dumps  *[]rereadDumpCall

	mu     sync.Mutex
	folds  []map[string]string
	refuse func(reconstruct.FullTableConfig) []reconstruct.TableFailure
}

// newRereadSched is a scheduler over a real supervisor, with the choice on or
// off, and the schedule already observed: the next slot fires.
func newRereadSched(t *testing.T, on bool) *rereadSched {
	t.Helper()
	r := &rereadSched{refuse: func(reconstruct.FullTableConfig) []reconstruct.TableFailure { return nil }}
	prevList, prevReached, prevWait, prevPoll := newestSnapshotTables, rereadCaptureReached, rereadCaptureWait, rereadCapturePoll
	t.Cleanup(func() {
		newestSnapshotTables, rereadCaptureReached, rereadCaptureWait, rereadCapturePoll = prevList, prevReached, prevWait, prevPoll
	})
	newestSnapshotTables = func(context.Context, string) (time.Time, []string, error) {
		return refreshAt.Add(-time.Hour), slices.Clone(rereadSchedTables), nil
	}
	rereadCaptureReached = func(context.Context, reconstruct.OverrideProbe) error { return nil }
	rereadCapturePoll = time.Millisecond
	holdFold(t, func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		r.mu.Lock()
		r.folds = append(r.folds, maps.Clone(cfg.BaselineOverride))
		refuse := r.refuse
		r.mu.Unlock()
		failed := refuse(cfg)
		if len(failed) == 0 {
			return foldOK(cfg)
		}
		var errs []error
		for _, f := range failed {
			errs = append(errs, f.Err)
		}
		return nil, failed, errors.Join(errs...)
	})
	r.dumps = stubRereadDump(t, func(_ int, c rereadDumpCall) error { writeRereadDump(t, c.dir, c.tables); return nil })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r.cancel = cancel
	r.sup = newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	h, err := console.OpenBaselineHistory(t.TempDir() + "/h.json")
	if err != nil {
		t.Fatal(err)
	}
	r.sup.history = h
	r.sup.rereadRefused = on
	r.reg, err = console.LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	r.b = newBackupScheduler(r.sup, r.reg, true, false)
	r.b.window = nil // see newScheduleFixture: the fixture snapshot is weeks old
	r.reads = stubFullReads(t, r.sup)
	r.e = addScheduled(t, r.reg, true)
	r.b.tick(context.Background(), time.Date(2026, 8, 28, 8, 0, 5, 0, time.UTC))
	return r
}

func (r *rereadSched) setRefuse(fn func(reconstruct.FullTableConfig) []reconstruct.TableFailure) {
	r.mu.Lock()
	r.refuse = fn
	r.mu.Unlock()
}

func (r *rereadSched) overrides() []map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.folds)
}

// curedByRead refuses fails, except a table the fold has a staged read of.
func curedByRead(fails ...reconstruct.TableFailure) func(reconstruct.FullTableConfig) []reconstruct.TableFailure {
	return func(cfg reconstruct.FullTableConfig) []reconstruct.TableFailure {
		var out []reconstruct.TableFailure
		for _, f := range fails {
			if cfg.BaselineOverride[f.Schema+"."+f.Table] == "" {
				out = append(out, f)
			}
		}
		return out
	}
}

// always refuses fails, read again or not.
func always(fails ...reconstruct.TableFailure) func(reconstruct.FullTableConfig) []reconstruct.TableFailure {
	return func(reconstruct.FullTableConfig) []reconstruct.TableFailure { return slices.Clone(fails) }
}

// slot fires the hour h and waits for every job it started, the read and a
// full read after it included.
func (r *rereadSched) slot(t *testing.T, h int) console.BackupScheduleState {
	t.Helper()
	return slotAt(t, r.b, r.e.ID, h)
}

func (r *rereadSched) dumpRecords() []console.BaselineRunRecord {
	var out []console.BaselineRunRecord
	for _, rec := range r.sup.history.List(r.e.ID) {
		if rec.Kind == console.BaselineRunDump && rec.SkipReason == "" {
			out = append(out, rec)
		}
	}
	return out
}

// wantNoReadLeft: no staged read is left in the working folder.
func (r *rereadSched) wantNoReadLeft(t *testing.T) {
	t.Helper()
	for _, name := range rereadLeft(t, r.sup.stagingDir) {
		if strings.HasPrefix(name, rereadDirPrefix) {
			t.Fatalf("a staged read is left in the working folder: %s", name)
		}
	}
}

// wantTodaysFullRead: the slot ended exactly as it ends today, in the full
// read standing in for the refused update, recorded under the update's own
// refusal and counted by the daily cap.
func (r *rereadSched) wantTodaysFullRead(t *testing.T, state console.BackupScheduleState, refused string) {
	t.Helper()
	if r.reads.count() != 1 {
		t.Fatalf("%d full reads, want the one that stands in for the refused update", r.reads.count())
	}
	if state.LastFallbackAt == "" || !strings.Contains(state.LastFallbackReason, "TRUNCATE TABLE in the window") {
		t.Fatalf("the fallback is not on record with the update's refusal: %+v", state)
	}
	if len(state.LastFallbackRefusedTables) != 1 || state.LastFallbackRefusedTables[0].Name != refused {
		t.Fatalf("the fallback names %+v, want the table the update refused, %s", state.LastFallbackRefusedTables, refused)
	}
	recs := r.dumpRecords()
	if len(recs) != 1 || recs[0].WhyCode != "fold_refused" {
		t.Fatalf("dump records %+v, want one full read whose reason starts like today's (fold_refused)", recs)
	}
	if r.sup.history.EmergencyStarted(r.e.ID) == "" {
		t.Fatal("the full read was not counted by the daily cap")
	}
}

// The case it exists for: one table truncated. Only that table is read from
// the source, every table is folded to one point, and nothing reads the
// whole server.
func TestScheduleReread_aTruncatedTableIsReadAgainAlone_2640(t *testing.T) {
	log := captureSlogFor(t)
	r := newRereadSched(t, true)
	r.setRefuse(curedByRead(truncated("orders")))
	st := r.slot(t, 9)

	if r.reads.count() != 0 || len(r.dumpRecords()) != 0 {
		t.Fatalf("the whole server was read (%d full reads): %+v", r.reads.count(), r.dumpRecords())
	}
	if len(*r.dumps) != 1 || !slices.Equal((*r.dumps)[0].tables, []string{"demo.orders"}) {
		t.Fatalf("dumps = %+v, want one of demo.orders alone", *r.dumps)
	}
	folds := r.overrides()
	if len(folds) != 2 || len(folds[0]) != 0 || len(folds[1]) != 1 || folds[1]["demo.orders"] == "" {
		t.Fatalf("folds = %v, want the refused update and then one fold with demo.orders read again", folds)
	}
	if st.LastMethod != console.BackupMethodRefresh || st.Last == nil || st.Last.State != "succeeded" || !st.Last.Published {
		t.Fatalf("state = %+v, want the update with the read on record, published", st)
	}
	if st.LastFallbackAt != "" || r.sup.history.EmergencyStarted(r.e.ID) != "" {
		t.Fatalf("a full read was recorded or counted: %+v", st)
	}
	// The page says which kind of update this was and why, counting the
	// tables: the history reaches sessions whose data profile hides names.
	if !strings.Contains(st.LastUpdateWhy, "1 table") || !strings.Contains(st.LastUpdateWhy, "read again") || strings.Contains(st.LastUpdateWhy, "orders") {
		t.Fatalf("LastUpdateWhy = %q, want the read of 1 table said, without its name", st.LastUpdateWhy)
	}
	var recorded bool
	for _, rec := range r.sup.history.List(r.e.ID) {
		if rec.Kind == console.BaselineRunRefresh && rec.Error == "" && rec.UpdateWhy == st.LastUpdateWhy {
			recorded = true
		}
	}
	if !recorded {
		t.Fatalf("the run history does not keep why this update read a table again: %+v", r.sup.history.List(r.e.ID))
	}
	if out := log.String(); !strings.Contains(out, "reading only the refused tables again") || !strings.Contains(out, "demo.orders") {
		t.Fatalf("the log does not name the read and its table:\n%s", out)
	}
	r.wantNoReadLeft(t)
}

// Turned off, the slot ends as it does today: the full read.
func TestScheduleReread_turnedOffReadsTheWholeServer_2640(t *testing.T) {
	r := newRereadSched(t, false)
	r.setRefuse(curedByRead(truncated("orders")))
	state := r.slot(t, 9)
	if len(*r.dumps) != 0 {
		t.Fatalf("turned off, the tables were read alone: %+v", *r.dumps)
	}
	r.wantTodaysFullRead(t, state, "demo.orders")
}

// What a read of some tables does not fix goes straight to the full read, as
// today, with no read of some tables first.
func TestScheduleReread_whatAReadDoesNotFixTakesTheFullRead_2640(t *testing.T) {
	for _, c := range []struct {
		name  string
		fails []reconstruct.TableFailure
	}{
		// The binary log was renumbered: the capture check cannot order the
		// two numberings (amendment 6).
		{"renumbered", []reconstruct.TableFailure{renumbered("orders")}},
		// A capture gap belongs to the server, not to one table.
		{"capture gap", []reconstruct.TableFailure{gapIn("orders")}},
		// The catch-all verdict holds causes a read does not cure.
		{"catch-all refusal", []reconstruct.TableFailure{notFound("orders")}},
		{"one curable and one not", []reconstruct.TableFailure{truncated("orders"), renumbered("plain")}},
		// Every table: an ordinary full read (amendment 2).
		{"every table", []reconstruct.TableFailure{truncated("mydumper_0"), truncated("mydumper_1"), truncated("orders"), truncated("plain")}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRereadSched(t, true)
			r.setRefuse(always(c.fails...))
			state := r.slot(t, 9)
			if len(*r.dumps) != 0 {
				t.Fatalf("tables were read alone: %+v", *r.dumps)
			}
			if r.reads.count() != 1 || state.LastFallbackAt == "" {
				t.Fatalf("want today's full read: %d reads, state %+v", r.reads.count(), state)
			}
		})
	}
}

// Every way the read of some tables can fail ends in today's full read, with
// the update's own refusal on record, and no staged read left behind.
func TestScheduleReread_aReadThatFailsTakesTodaysFullRead_2640(t *testing.T) {
	for _, c := range []struct {
		name  string
		setup func(t *testing.T, r *rereadSched)
		// inLog is what the log says about why the read did not publish.
		inLog string
	}{
		{"capture never reaches the read", func(t *testing.T, r *rereadSched) {
			rereadCaptureWait = 20 * time.Millisecond
			rereadCaptureReached = func(context.Context, reconstruct.OverrideProbe) error {
				return fmt.Errorf("orders at binlog.000002:900: %w", reconstruct.ErrBaselineOverrideAhead)
			}
		}, "capture did not reach the point of the read"},
		{"the capture check fails", func(t *testing.T, r *rereadSched) {
			rereadCaptureReached = func(context.Context, reconstruct.OverrideProbe) error { return errors.New("index connection refused") }
		}, "index connection refused"},
		{"the dump fails", func(t *testing.T, r *rereadSched) {
			r.dumps = stubRereadDump(t, func(int, rereadDumpCall) error { return errors.New("mydumper exit 2") })
		}, "mydumper exit 2"},
		{"the table is gone from the source", func(t *testing.T, r *rereadSched) {
			r.dumps = stubRereadDump(t, func(_ int, c rereadDumpCall) error { writeRereadDump(t, c.dir, []string{"demo.plain"}); return nil })
		}, "ended without a table it was asked for"},
		{"refused again after the read", func(t *testing.T, r *rereadSched) {
			r.setRefuse(always(truncated("orders")))
		}, "TRUNCATE TABLE in the window"},
		{"another table renumbered after the read", func(t *testing.T, r *rereadSched) {
			r.setRefuse(func(cfg reconstruct.FullTableConfig) []reconstruct.TableFailure {
				if len(cfg.BaselineOverride) == 0 {
					return []reconstruct.TableFailure{truncated("orders")}
				}
				return []reconstruct.TableFailure{renumbered("plain")}
			})
		}, "cannot be found by binlog position"},
	} {
		t.Run(c.name, func(t *testing.T) {
			log := captureSlogFor(t)
			r := newRereadSched(t, true)
			r.setRefuse(curedByRead(truncated("orders")))
			c.setup(t, r)
			state := r.slot(t, 9)
			r.wantTodaysFullRead(t, state, "demo.orders")
			r.wantNoReadLeft(t)
			out := log.String()
			if !strings.Contains(out, "reading the refused tables again did not publish") || !strings.Contains(out, c.inLog) {
				t.Fatalf("the log does not say the read failed and why (%q):\n%s", c.inLog, out)
			}
		})
	}
}

// Under the daily cap a read that fails is held back like today's: no full
// read, the card says when the next is allowed. The read itself is not held
// by the cap: reading one table is not reading the server.
func TestScheduleReread_theDailyCapHoldsTheFullReadNotTheRead_2640(t *testing.T) {
	r := newRereadSched(t, true)
	r.b.noteEmergency(r.e.ID, time.Now().UTC())
	r.setRefuse(curedByRead(truncated("orders")))
	rereadCaptureReached = func(context.Context, reconstruct.OverrideProbe) error { return errors.New("index connection refused") }
	state := r.slot(t, 9)
	if len(*r.dumps) != 1 {
		t.Fatalf("the cap on full reads held back the read of one table: %+v", *r.dumps)
	}
	if r.reads.count() != 0 {
		t.Fatalf("a full read inside the daily cap: %d", r.reads.count())
	}
	if !strings.Contains(state.LastSkipReason, "at most once a day") || !strings.Contains(state.LastSkipReason, "the next one is allowed after") {
		t.Fatalf("the card does not say the cap held the full read back: %q", state.LastSkipReason)
	}
}

// A table refused again right after its read, with no update through in
// between, is not read alone a second time: the full read, once a day.
func TestScheduleReread_refusedAgainRightAfterItsReadTakesTheFullRead_2640(t *testing.T) {
	noEmergencyCap(t)
	log := captureSlogFor(t)
	r := newRereadSched(t, true)
	r.setRefuse(curedByRead(truncated("orders")))
	if st := r.slot(t, 9); len(*r.dumps) != 1 || r.reads.count() != 0 || st.Last == nil || st.Last.State != "succeeded" {
		t.Fatalf("setup: dumps %d, reads %d, state %+v", len(*r.dumps), r.reads.count(), st)
	}
	state := r.slot(t, 10)
	if len(*r.dumps) != 1 || r.reads.count() != 1 || state.LastFallbackAt == "" {
		t.Fatalf("a second read of the same table in a row: dumps %d, full reads %d, state %+v", len(*r.dumps), r.reads.count(), state)
	}
	if out := log.String(); !strings.Contains(out, "was already read again alone") {
		t.Fatalf("the log does not say why the full read was chosen:\n%s", out)
	}
	// A failed read counts as a read: the next refusal takes the full read.
	r2 := newRereadSched(t, true)
	r2.setRefuse(curedByRead(truncated("orders")))
	rereadCaptureReached = func(context.Context, reconstruct.OverrideProbe) error { return errors.New("index connection refused") }
	r2.slot(t, 9)
	rereadCaptureReached = func(context.Context, reconstruct.OverrideProbe) error { return nil }
	r2.slot(t, 10)
	if len(*r2.dumps) != 1 || r2.reads.count() != 2 {
		t.Fatalf("after a failed read: dumps %d, full reads %d; want no second read and the full read each time", len(*r2.dumps), r2.reads.count())
	}
}

// An update that goes through ends it: the same table truncated an hour later
// is read alone again. The hourly DROP PARTITION case.
func TestScheduleReread_anUpdateThroughAllowsTheNextRead_2640(t *testing.T) {
	r := newRereadSched(t, true)
	r.setRefuse(curedByRead(truncated("orders")))
	r.slot(t, 9)
	nextSecond()
	r.setRefuse(always())
	if st := r.slot(t, 10); st.Last == nil || st.Last.State != "succeeded" {
		t.Fatalf("setup: the plain update did not go through: %+v", st)
	}
	nextSecond()
	r.setRefuse(curedByRead(truncated("orders")))
	r.slot(t, 11)
	if len(*r.dumps) != 2 || r.reads.count() != 0 {
		t.Fatalf("dumps %d, full reads %d; want the table read alone again, no full read", len(*r.dumps), r.reads.count())
	}
}

// A shutdown during the read starts nothing on the way out, as today.
func TestScheduleReread_shutdownDuringTheReadStartsNothing_2640(t *testing.T) {
	r := newRereadSched(t, true)
	r.setRefuse(curedByRead(truncated("orders")))
	started := make(chan struct{})
	var once sync.Once
	rereadCaptureWait = time.Minute
	rereadCaptureReached = func(context.Context, reconstruct.OverrideProbe) error {
		once.Do(func() { close(started) })
		return reconstruct.ErrBaselineOverrideAhead
	}
	r.b.tick(context.Background(), time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("the read of the refused table never started")
	}
	r.cancel()
	r.b.watchers.Wait()
	if r.reads.count() != 0 {
		t.Fatalf("a full read was started during shutdown: %d", r.reads.count())
	}
	// The run ends at the cancel and removes its read.
	for deadline := time.Now().Add(5 * time.Second); r.sup.RefreshStatus(r.e.ID).State == "running"; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the run did not end at the cancel")
		}
	}
	r.wantNoReadLeft(t)
}

// The full read after a failed read keeps the reason the daily cap counts
// by: its why starts with the refused-update prefix.
func TestScheduleReread_theFullReadAfterAReadIsAnEmergencyRead_2640(t *testing.T) {
	r := newRereadSched(t, true)
	r.setRefuse(curedByRead(truncated("orders")))
	rereadCaptureReached = func(context.Context, reconstruct.OverrideProbe) error { return errors.New("index connection refused") }
	r.slot(t, 9)
	if _, held := r.b.emergencyHeld(r.e.ID, time.Now().UTC()); !held {
		t.Fatal("the full read after a failed read is not counted by the daily cap")
	}
	b2 := newBackupScheduler(r.sup, r.reg, true, false)
	if _, held := b2.emergencyHeld(r.e.ID, time.Now().UTC()); !held {
		t.Fatal("after a restart the full read after a failed read no longer counts")
	}
}

// The choice on its own.
func TestRereadChoice_2640(t *testing.T) {
	ddl := func(name string) console.RefusedTable {
		return console.RefusedTable{Name: name, Verdict: reconstruct.RefreshVerdictRefusedDDL, Reason: "TRUNCATE"}
	}
	status := func(tables int, refused ...console.RefusedTable) console.BaselineStatus {
		return console.BaselineStatus{State: "failed", Tables: tables, Refused: len(refused), RefusedTables: refused}
	}
	for _, c := range []struct {
		name  string
		st    console.BaselineStatus
		tried map[string]bool
		want  []string
	}{
		{"one table", status(4, ddl("demo.orders")), nil, []string{"demo.orders"}},
		{"two tables", status(4, ddl("demo.orders"), ddl("demo.plain")), nil, []string{"demo.orders", "demo.plain"}},
		{"the same table twice", status(4, ddl("demo.orders"), ddl("demo.orders")), nil, []string{"demo.orders"}},
		{"no table named", status(4), nil, nil},
		{"every table", status(2, ddl("demo.a"), ddl("demo.b")), nil, nil},
		{"the table count is unknown", status(0, ddl("demo.orders")), nil, nil},
		{"more refused than named", console.BaselineStatus{State: "failed", Tables: 4, Refused: 2, RefusedTables: []console.RefusedTable{ddl("demo.orders")}}, nil, nil},
		{"the list was cut short", console.BaselineStatus{State: "failed", Tables: 40, Refused: 2, RefusedTables: []console.RefusedTable{ddl("demo.orders")}, RefusedTablesOmitted: 1}, nil, nil},
		{"renumbered", status(4, console.RefusedTable{Name: "demo.orders", Verdict: reconstruct.RefreshVerdictRefusedRenumbered}), nil, nil},
		{"capture gap", status(4, console.RefusedTable{Name: "demo.orders", Verdict: reconstruct.RefreshVerdictRefusedGap}), nil, nil},
		{"catch-all", status(4, console.RefusedTable{Name: "demo.orders", Verdict: reconstruct.RefreshVerdictRefused}), nil, nil},
		{"a comma in the name", status(4, ddl("demo.a,b")), nil, nil},
		{"a dot in the table name", status(4, ddl("demo.a.b")), nil, nil},
		{"no schema", status(4, ddl("orders")), nil, nil},
		{"already read again", status(4, ddl("demo.orders")), map[string]bool{rereadKey(ddl("demo.orders")): true}, nil},
		{"another table read again before", status(4, ddl("demo.orders")), map[string]bool{rereadKey(ddl("demo.plain")): true}, []string{"demo.orders"}},
		{"a succeeded update", console.BaselineStatus{State: "succeeded", Tables: 4}, nil, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, whyNot := rereadChoice(c.st, c.tried)
			if !slices.Equal(got, c.want) {
				t.Fatalf("tables = %v (%s), want %v", got, whyNot, c.want)
			}
			if got == nil && whyNot == "" {
				t.Fatal("no read chosen and no reason why")
			}
		})
	}
}

// The words: the tables are counted, never named, and the page's sentence
// reads as one.
func TestRereadUpdateWhy_2640(t *testing.T) {
	one, two := rereadUpdateWhy(1), rereadUpdateWhy(2)
	if !strings.Contains(one, "1 table ") || !strings.Contains(two, "2 tables ") {
		t.Fatalf("counts: %q / %q", one, two)
	}
	for _, w := range []string{one, two} {
		if strings.HasSuffix(w, ".") || strings.Contains(w, "—") || console.IsUnconfirmedUpdate(w) {
			t.Fatalf("%q: the page adds the period; no long dash; not an update taken instead of a skip", w)
		}
	}
}

// nextSecond waits for the next second: a snapshot folder is named to the
// second, and a run in the second of the previous one is refused for that.
func nextSecond() {
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(1100 * time.Millisecond)))
}

// On by default, and the flag beats the environment both ways; a value that
// is not true or false keeps the default.
func TestResolveUpConsoleEnv_rereadRefusedTables_2640(t *testing.T) {
	f := watchCmd.Flags().Lookup("baseline-reread-refused-tables")
	if f == nil || f.DefValue != "true" {
		t.Fatalf("--baseline-reread-refused-tables = %+v, want a flag on by default", f)
	}
	prev := upBaselineRereadRefused
	t.Cleanup(func() { upBaselineRereadRefused = prev })
	for _, tc := range []struct {
		env, flag string
		want      bool
	}{
		{"", "", true},
		{"false", "", false},
		{"0", "", false},
		{"true", "", true},
		{"flase", "", true},
		{"false", "true", true},
		{"true", "false", false},
	} {
		upBaselineRereadRefused = true
		t.Setenv("BINTRAIL_BASELINE_REREAD_REFUSED_TABLES", tc.env)
		cmd := &cobra.Command{Use: "watch"}
		cmd.Flags().BoolVar(&upBaselineRereadRefused, "baseline-reread-refused-tables", true, "")
		if tc.flag != "" {
			if err := cmd.Flags().Set("baseline-reread-refused-tables", tc.flag); err != nil {
				t.Fatal(err)
			}
		}
		if err := resolveUpConsoleEnv(cmd); err != nil {
			t.Fatal(err)
		}
		if upBaselineRereadRefused != tc.want {
			t.Errorf("env %q flag %q: %v, want %v", tc.env, tc.flag, upBaselineRereadRefused, tc.want)
		}
	}
}

// The daemon's supervisor carries the setting to the schedule.
func TestNewBaselineSupervisorFromConfig_carriesTheRereadChoice_2640(t *testing.T) {
	prev := upBaselineRereadRefused
	t.Cleanup(func() { upBaselineRereadRefused = prev })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for _, on := range []bool{true, false} {
		upBaselineRereadRefused = on
		if got := newBaselineSupervisorFromConfig(ctx, t.TempDir(), nil).rereadRefused; got != on {
			t.Errorf("rereadRefused = %v, want %v", got, on)
		}
	}
}

// A read that ran out of disk in the working folder takes no full read, which
// would stage a dump of every table there; the slot says so with both reasons.
func TestScheduleReread_aReadOutOfDiskIsSaidNotSilent_2640(t *testing.T) {
	r := newRereadSched(t, true)
	r.setRefuse(func(cfg reconstruct.FullTableConfig) []reconstruct.TableFailure {
		if len(cfg.BaselineOverride) == 0 {
			return []reconstruct.TableFailure{truncated("orders")}
		}
		return []reconstruct.TableFailure{{Schema: "demo", Table: "plain", Err: fmt.Errorf("demo.plain: %w", errFoldDiskFull)}}
	})
	state := r.slot(t, 9)
	if len(*r.dumps) != 1 || r.reads.count() != 0 {
		t.Fatalf("dumps %d, full reads %d; want the read and no full read", len(*r.dumps), r.reads.count())
	}
	if !strings.Contains(state.LastSkipReason, "ran out of disk space") || !strings.Contains(state.LastSkipReason, "TRUNCATE TABLE in the window") {
		t.Fatalf("the slot does not say why nothing stands in: %q", state.LastSkipReason)
	}
	r.wantNoReadLeft(t)
}

// A lock mode a read of some tables cannot use starts no read: the full read,
// as before, and the log says why.
func TestScheduleReread_aLockModeItCannotUseTakesTheFullRead_2640(t *testing.T) {
	log := captureSlogFor(t)
	r := newRereadSched(t, true)
	r.sup.lockMode, r.sup.lockModeChosen = baseline.LockModeNoLock, true
	r.setRefuse(curedByRead(truncated("orders")))
	state := r.slot(t, 9)
	if len(*r.dumps) != 0 || r.reads.count() != 1 || state.LastFallbackAt == "" {
		t.Fatalf("dumps %d, full reads %d, state %+v; want today's full read and no read of some tables", len(*r.dumps), r.reads.count(), state)
	}
	if !strings.Contains(log.String(), "lock mode no-lock") {
		t.Fatalf("the log does not say why the tables were not read alone:\n%s", log.String())
	}
}
