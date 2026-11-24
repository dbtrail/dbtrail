package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2640, the update that folds from a read of some tables alone. The dump is
// a stub, the conversion and the staged files are real, and the fold and the
// capture check are replaced so each case can say what they answer.

const rereadServer = "s1"

var rereadSnapshot = []string{"shop.orders", "shop.items", "shop.plain"}

type rereadFixture struct {
	sup   *baselineSupervisor
	req   refreshRequest
	stage string
	folds *[]reconstruct.FullTableConfig
	dumps *[]rereadDumpCall
}

func newRereadFixture(t *testing.T, fold func(reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error)) rereadFixture {
	t.Helper()
	stage, local := t.TempDir(), t.TempDir()
	dumps := stubRereadDump(t, func(_ int, c rereadDumpCall) error { writeRereadDump(t, c.dir, c.tables); return nil })

	prevList, prevSigners, prevMark, prevReached, prevWait, prevPoll :=
		newestSnapshotTables, snapshotSignersFunc, readIndexMark, rereadCaptureReached, rereadCaptureWait, rereadCapturePoll
	t.Cleanup(func() {
		newestSnapshotTables, snapshotSignersFunc, readIndexMark, rereadCaptureReached, rereadCaptureWait, rereadCapturePoll =
			prevList, prevSigners, prevMark, prevReached, prevWait, prevPoll
	})
	newestSnapshotTables = func(context.Context, string) (time.Time, []string, error) {
		return time.Now().UTC().Add(-time.Hour).Truncate(time.Second), slices.Clone(rereadSnapshot), nil
	}
	snapshotSignersFunc = func(string, time.Time) ([]string, bool, error) { return nil, false, nil }
	readIndexMark = func(context.Context, string) (indexMark, error) { return indexMark{}, errors.New("no index in this test") }
	rereadCaptureReached = func(context.Context, reconstruct.FullTableConfig) error { return nil }
	rereadCapturePoll = time.Millisecond

	folds := &[]reconstruct.FullTableConfig{}
	holdFold(t, func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		*folds = append(*folds, cfg)
		return fold(cfg)
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, stage, baseline.LockModeFTWRL)
	sup.refreshes[rereadServer] = &console.BaselineStatus{State: "running"}
	req := refreshRequest{ServerID: rereadServer, ServerName: "shop-db", IndexDSN: "index", BaselineDir: local,
		Reread: &rereadOrder{Tables: []string{"shop.orders", "shop.items"},
			Source: console.BaselineRequest{ServerID: rereadServer, ServerName: "shop-db", SourceDSN: "src", LocalDir: local}}}
	return rereadFixture{sup: sup, req: req, stage: stage, folds: folds, dumps: dumps}
}

func foldOK(cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
	var reports []*reconstruct.TableReport
	for _, st := range cfg.Tables {
		schema, table, _ := strings.Cut(st, ".")
		reports = append(reports, &reconstruct.TableReport{Schema: schema, Table: table})
	}
	return reports, nil, nil
}

func (f rereadFixture) run() console.BaselineStatus {
	f.sup.runRefresh(f.req, time.Time{}, time.Hour)
	return f.sup.RefreshStatus(rereadServer)
}

func (f rereadFixture) wantNothingPublished(t *testing.T, st console.BaselineStatus) {
	t.Helper()
	if st.State != "failed" || st.Published {
		t.Fatalf("status = %q published=%v (%s), want a failed run that published nothing", st.State, st.Published, st.LastError)
	}
	if left := rereadLeft(t, f.stage); len(left) != 0 {
		t.Fatalf("the working folder still holds %v", left)
	}
	snaps, err := os.ReadDir(f.req.BaselineDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range snaps {
		if baseline.SnapshotComplete(filepath.Join(f.req.BaselineDir, e.Name())) && e.IsDir() {
			t.Fatalf("a complete snapshot is in the server's folder: %s", e.Name())
		}
	}
}

// With the field unset the configuration carries no override: what every
// update does today.
func TestRefreshFoldConfig_carriesTheOverrideOnlyWhenSet_2640(t *testing.T) {
	at := time.Now().UTC()
	if got := refreshFoldConfig(refreshRequest{BaselineDir: "/b"}, at, rereadSnapshot).BaselineOverride; got != nil {
		t.Fatalf("an ordinary update has override %v", got)
	}
	want := map[string]string{"shop.orders": "/w/orders.parquet"}
	got := refreshFoldConfig(refreshRequest{BaselineDir: "/b", baselineOverride: want}, at, rereadSnapshot).BaselineOverride
	if len(got) != 1 || got["shop.orders"] != want["shop.orders"] {
		t.Fatalf("override = %v, want %v", got, want)
	}
}

func TestRunRefresh_withoutRereadDumpsNothing_2640(t *testing.T) {
	f := newRereadFixture(t, foldOK)
	f.req.Reread = nil
	if st := f.run(); st.State != "succeeded" {
		t.Fatalf("status = %q (%s)", st.State, st.LastError)
	}
	if len(*f.dumps) != 0 {
		t.Fatalf("an ordinary update read the source: %+v", *f.dumps)
	}
	if len(*f.folds) != 1 || (*f.folds)[0].BaselineOverride != nil {
		t.Fatalf("folds = %+v, want one with no override", *f.folds)
	}
}

// Two tables read again and one that is not: one fold, of all three, to one
// instant after the read, and the staged folder gone afterwards.
func TestRunRefresh_foldsTheReadTablesWithTheOthers_2640(t *testing.T) {
	var during []string
	var f rereadFixture
	f = newRereadFixture(t, func(cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		for _, p := range cfg.BaselineOverride {
			if _, err := os.Stat(p); err != nil {
				t.Errorf("a staged file is gone during the fold: %v", err)
			}
			meta, err := baseline.ReadParquetMetadata(p)
			if err != nil {
				t.Fatal(err)
			}
			if meta.SnapshotTimestamp.After(cfg.At) {
				t.Errorf("the read at %s is after the fold's target %s", meta.SnapshotTimestamp, cfg.At)
			}
		}
		during = rereadLeft(t, f.stage)
		return foldOK(cfg)
	})
	st := f.run()
	if st.State != "succeeded" || !st.Published {
		t.Fatalf("status = %q published=%v (%s)", st.State, st.Published, st.LastError)
	}
	if len(*f.dumps) != 1 || !slices.Equal((*f.dumps)[0].tables, []string{"shop.orders", "shop.items"}) {
		t.Fatalf("dumps = %+v, want one of the two tables", *f.dumps)
	}
	if len(*f.folds) != 1 {
		t.Fatalf("%d folds, want one", len(*f.folds))
	}
	cfg := (*f.folds)[0]
	if !slices.Equal(cfg.Tables, rereadSnapshot) {
		t.Fatalf("the fold has tables %v, want all of %v", cfg.Tables, rereadSnapshot)
	}
	if len(cfg.BaselineOverride) != 2 || cfg.BaselineOverride["shop.plain"] != "" {
		t.Fatalf("override = %v, want the two tables read", cfg.BaselineOverride)
	}
	if len(during) != 1 || !strings.HasPrefix(during[0], rereadDirPrefix) {
		t.Fatalf("during the fold the working folder held %v, want the read", during)
	}
	if left := rereadLeft(t, f.stage); len(left) != 0 {
		t.Fatalf("the working folder still holds %v", left)
	}
}

func TestRunRefresh_rereadWaitsForCaptureThenFolds_2640(t *testing.T) {
	f := newRereadFixture(t, foldOK)
	rereadCaptureWait = time.Minute
	var asked []time.Time
	rereadCaptureReached = func(_ context.Context, cfg reconstruct.FullTableConfig) error {
		asked = append(asked, cfg.At)
		if len(asked) < 3 {
			return fmt.Errorf("%w: not yet", reconstruct.ErrBaselineOverrideAhead)
		}
		return nil
	}
	if st := f.run(); st.State != "succeeded" {
		t.Fatalf("status = %q (%s)", st.State, st.LastError)
	}
	if len(asked) != 3 || len(*f.folds) != 1 {
		t.Fatalf("asked %d times and folded %d, want 3 and 1", len(asked), len(*f.folds))
	}
	// Each try asks about its own present, and the fold is not before the
	// instant the last one found reached.
	if !asked[2].After(asked[0]) {
		t.Fatalf("the instant asked about did not move: %v", asked)
	}
	if (*f.folds)[0].At.Before(asked[2]) {
		t.Fatalf("the fold's target %s is before the instant found reached %s", (*f.folds)[0].At, asked[2])
	}
}

// Capture stopped, or never reaching the read: the bound ends the wait, with
// an error that says so, nothing folded and the folder removed.
func TestRunRefresh_captureNeverArrives_2640(t *testing.T) {
	f := newRereadFixture(t, foldOK)
	rereadCaptureWait = 20 * time.Millisecond
	rereadCaptureReached = func(context.Context, reconstruct.FullTableConfig) error {
		return fmt.Errorf("%w: behind", reconstruct.ErrBaselineOverrideAhead)
	}
	st := f.run()
	f.wantNothingPublished(t, st)
	if len(*f.folds) != 0 {
		t.Fatalf("it folded %d times with capture behind the read", len(*f.folds))
	}
	if !strings.Contains(st.LastError, errRereadCaptureBehind.Error()) {
		t.Fatalf("error = %q", st.LastError)
	}
}

func TestRunRefresh_cancelledWhileWaitingForCapture_2640(t *testing.T) {
	f := newRereadFixture(t, foldOK)
	rereadCaptureWait = time.Hour
	rereadCapturePoll = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.sup.ctx = ctx
	rereadCaptureReached = func(context.Context, reconstruct.FullTableConfig) error {
		cancel()
		return fmt.Errorf("%w: behind", reconstruct.ErrBaselineOverrideAhead)
	}
	done := make(chan console.BaselineStatus, 1)
	go func() { done <- f.run() }()
	select {
	case st := <-done:
		f.wantNothingPublished(t, st)
		if !strings.Contains(st.LastError, context.Canceled.Error()) {
			t.Fatalf("error = %q", st.LastError)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the wait did not end when the daemon stopped")
	}
	if len(*f.folds) != 0 {
		t.Fatal("it folded after being stopped")
	}
}

// Any other answer of the check is not "wait": it ends the run at once.
func TestRunRefresh_aFailedCaptureCheckIsNotRetried_2640(t *testing.T) {
	f := newRereadFixture(t, foldOK)
	rereadCaptureWait = time.Hour
	n := 0
	rereadCaptureReached = func(context.Context, reconstruct.FullTableConfig) error {
		n++
		return errors.New("index unreachable")
	}
	st := f.run()
	f.wantNothingPublished(t, st)
	if n != 1 || len(*f.folds) != 0 || !strings.Contains(st.LastError, "index unreachable") {
		t.Fatalf("asked %d times, folded %d, error %q", n, len(*f.folds), st.LastError)
	}
}

// The fold refuses after the wait (the table was truncated again after the
// read, or the binary log was renumbered): one try, the refusal keeps its
// verdict for the schedule, nothing is published, the folder goes.
func TestRunRefresh_aFoldRefusedAfterTheReadPublishesNothing_2640(t *testing.T) {
	for _, tc := range []struct {
		name, verdict string
		err           error
	}{
		{"truncated again", reconstruct.RefreshVerdictRefusedDDL, reconstruct.ErrDestructiveDDL},
		{"renumbered", reconstruct.RefreshVerdictRefusedRenumbered, reconstruct.ErrBinlogRenumbered},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRereadFixture(t, func(reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
				failure := fmt.Errorf("%w: shop.orders", tc.err)
				return nil, []reconstruct.TableFailure{{Schema: "shop", Table: "orders", Err: failure}}, failure
			})
			st := f.run()
			f.wantNothingPublished(t, st)
			if len(*f.folds) != 1 {
				t.Fatalf("%d folds, want one", len(*f.folds))
			}
			if len(st.RefusedTables) != 1 || st.RefusedTables[0].Verdict != tc.verdict {
				t.Fatalf("refused = %+v, want verdict %s", st.RefusedTables, tc.verdict)
			}
		})
	}
}

func TestRunRefresh_aPanicInTheFoldStillRemovesTheRead_2640(t *testing.T) {
	f := newRereadFixture(t, func(reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		panic("boom")
	})
	st := f.run()
	f.wantNothingPublished(t, st)
}

// What cannot work is refused before production is read.
func TestRunRefresh_refusalsReadNothing_2640(t *testing.T) {
	for name, change := range map[string]func(*refreshRequest, *baselineSupervisor){
		"every table":        func(r *refreshRequest, _ *baselineSupervisor) { r.Reread.Tables = slices.Clone(rereadSnapshot) },
		"not in the snapshot": func(r *refreshRequest, _ *baselineSupervisor) { r.Reread.Tables = []string{"shop.new"} },
		"no table":           func(r *refreshRequest, _ *baselineSupervisor) { r.Reread.Tables = nil },
		"postgres request":   func(r *refreshRequest, _ *baselineSupervisor) { r.SourcePostgres = true },
		"postgres source":    func(r *refreshRequest, _ *baselineSupervisor) { r.Reread.Source.Flavor = console.FlavorPostgres },
		"another server":     func(r *refreshRequest, _ *baselineSupervisor) { r.Reread.Source.ServerID = "s2" },
		"working folder inside the snapshots": func(r *refreshRequest, s *baselineSupervisor) {
			s.stagingDir = filepath.Join(r.BaselineDir, "work")
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newRereadFixture(t, foldOK)
			change(&f.req, f.sup)
			st := f.run()
			if st.State != "failed" || st.Published {
				t.Fatalf("status = %q published=%v", st.State, st.Published)
			}
			if len(*f.dumps) != 0 || len(*f.folds) != 0 {
				t.Fatalf("dumps %d, folds %d, want none", len(*f.dumps), len(*f.folds))
			}
		})
	}
}

// The update goes on even when the gate would have skipped the cycle: the
// read cures a table that may see no new change.
func TestRunRefresh_theGateDoesNotSkipARead_2640(t *testing.T) {
	f := newRereadFixture(t, foldOK)
	mark := indexMark{events: 7}
	readIndexMark = func(context.Context, string) (indexMark, error) { return mark, nil }
	f.sup.foldedMarks[rereadServer] = foldMemo{mark: mark, publishedAt: time.Now().UTC(),
		destination: refreshDestination(f.req), indexDSN: f.req.IndexDSN}
	if st := f.run(); st.State != "succeeded" || len(*f.folds) != 1 {
		t.Fatalf("status %q, %d folds: the cycle was skipped with a read in hand", st.State, len(*f.folds))
	}
}

// A kill during the fold: the journal names the staged folder, and the next
// start removes it and records the run as interrupted.
func TestRunRefresh_aRestartRemovesTheReadOfAKilledRun_2640(t *testing.T) {
	jf := newJobsFixture(t)
	var f rereadFixture
	var jobs []console.BaselineJob
	f = newRereadFixture(t, func(cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		jobs = f.sup.history.Jobs()
		return foldOK(cfg)
	})
	real := jf.supervisor(t)
	f.sup.history, f.sup.jobsDir, f.sup.stagingDir = real.history, real.jobsDir, jf.staging
	f.stage = jf.staging
	if st := f.run(); st.State != "succeeded" {
		t.Fatalf("status = %q (%s)", st.State, st.LastError)
	}
	if len(jobs) != 1 {
		t.Fatalf("%d jobs journaled during the fold, want this run's", len(jobs))
	}
	var read console.BaselineJobDir
	for _, d := range jobs[0].Dirs {
		if strings.HasPrefix(d.Name, rereadDirPrefix) {
			read = d
		}
	}
	if read.Name == "" {
		t.Fatalf("the journal lists %+v and not the staged read", jobs[0].Dirs)
	}
	// What the folder looked like at the kill, and the next start's reclaim.
	if err := os.MkdirAll(filepath.Join(read.Root, read.Name, "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if res := reclaimJobDir(read); !res.removed || res.err != nil {
		t.Fatalf("reclaim = %+v", res)
	}
	if left := rereadLeft(t, jf.staging); len(left) != 0 {
		t.Fatalf("after the reclaim the working folder holds %v", left)
	}
}
