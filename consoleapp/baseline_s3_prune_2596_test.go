package consoleapp

import (
	"context"
	"errors"
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

// #2596 part 3: the cleanup removes snapshots from S3 past each
// destination's age, and nothing sends them back.

func stubPruneS3(t *testing.T) *[]reconstruct.PruneS3Options {
	t.Helper()
	var calls []reconstruct.PruneS3Options
	prev, prevBusy := pruneS3Fn, s3PruneBusyFn
	t.Cleanup(func() { pruneS3Fn, s3PruneBusyFn = prev, prevBusy })
	pruneS3Fn = func(_ context.Context, o reconstruct.PruneS3Options) (reconstruct.PruneS3Result, error) {
		calls = append(calls, o)
		return reconstruct.PruneS3Result{}, nil
	}
	s3PruneBusyFn = nil
	return &calls
}

func TestRunS3PruneCycle(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	week, month := 7*24*time.Hour, 30*24*time.Hour
	entries := []console.ServerEntry{
		{ID: "a", BaselineS3: "s3://b/a", BaselineS3Retain: "7d"},
		{ID: "a2", BaselineS3: "s3://b/a/", BaselineS3Retain: "30d"},
		{ID: "off", BaselineS3: "s3://b/off"},
		{ID: "mixed1", BaselineS3: "s3://b/mixed", BaselineS3Retain: "7d"},
		{ID: "mixed2", BaselineS3: "s3://b/mixed"},
		{ID: "daemon", BaselineS3: "s3://b/daemon", BaselineS3Retain: "7d"},
		{ID: "local"},
	}
	t.Run("each destination once, at its age; the rest untouched", func(t *testing.T) {
		calls := stubPruneS3(t)
		runS3PruneCycle(context.Background(), entries, "s3://b/daemon", 0, now)
		if len(*calls) != 1 || (*calls)[0].S3URL != "s3://b/a" || (*calls)[0].Retain != month || !(*calls)[0].Now.Equal(now) {
			t.Fatalf("calls = %+v, want one for s3://b/a at the longest age of its two servers", *calls)
		}
	})
	t.Run("never younger than the local age", func(t *testing.T) {
		calls := stubPruneS3(t)
		runS3PruneCycle(context.Background(), entries[:1], "", 40*24*time.Hour, now)
		if len(*calls) != 1 || (*calls)[0].Retain != 40*24*time.Hour {
			t.Fatalf("calls = %+v", *calls)
		}
		_ = week
	})
	t.Run("asked again before each snapshot", func(t *testing.T) {
		var got reconstruct.PruneS3Options
		prev, prevBusy := pruneS3Fn, s3PruneBusyFn
		t.Cleanup(func() { pruneS3Fn, s3PruneBusyFn = prev, prevBusy })
		busy := false
		s3PruneBusyFn = func([]string) bool { return busy }
		pruneS3Fn = func(_ context.Context, o reconstruct.PruneS3Options) (reconstruct.PruneS3Result, error) {
			got = o
			return reconstruct.PruneS3Result{}, nil
		}
		runS3PruneCycle(context.Background(), entries[:1], "", 0, now)
		if got.Proceed == nil || !got.Proceed() {
			t.Fatal("no Proceed, or it stops with nothing running")
		}
		busy = true
		if got.Proceed() {
			t.Fatal("Proceed does not stop once a restore starts")
		}
	})
	t.Run("skipped while a job reads it", func(t *testing.T) {
		calls := stubPruneS3(t)
		var asked []string
		s3PruneBusyFn = func(ids []string) bool { asked = ids; return true }
		runS3PruneCycle(context.Background(), entries[:2], "", 0, now)
		if len(*calls) != 0 || !slices.Equal(asked, []string{"a", "a2"}) {
			t.Fatalf("calls = %+v asked %v: want none while busy, asked for both servers", *calls, asked)
		}
	})
	t.Run("each cleanup is recorded for the settings page", func(t *testing.T) {
		prev, prevBusy := pruneS3Fn, s3PruneBusyFn
		t.Cleanup(func() { pruneS3Fn, s3PruneBusyFn = prev, prevBusy })
		s3PruneBusyFn = nil
		pruneS3Fn = func(_ context.Context, o reconstruct.PruneS3Options) (reconstruct.PruneS3Result, error) {
			return reconstruct.PruneS3Result{Pruned: []string{"a", "b"}, KeptNewest: 1, Failed: "c"}, errors.New("AccessDenied")
		}
		one := []console.ServerEntry{{ID: "r", Name: "r", BaselineS3: "s3://b/rec", BaselineS3Retain: "7d"}}
		runS3PruneCycle(context.Background(), one, "", 0, now)
		got := console.LastS3CleanupForTest("s3://b/rec")
		if got == nil || got.Removed != 2 || got.KeptNewest != 1 || got.Snapshot != "c" || got.Error == "" || got.At != now.UTC().Format(time.RFC3339) || got.AccessDenied {
			t.Fatalf("recorded %+v", got)
		}
		s3PruneBusyFn = func([]string) bool { return true }
		runS3PruneCycle(context.Background(), one, "", 0, now)
		if got := console.LastS3CleanupForTest("s3://b/rec"); got == nil || !got.Skipped {
			t.Fatalf("a skipped cleanup is not recorded: %+v", got)
		}
	})
	t.Run("a failure on one destination does not stop the others", func(t *testing.T) {
		var got []string
		prev := pruneS3Fn
		t.Cleanup(func() { pruneS3Fn = prev })
		pruneS3Fn = func(_ context.Context, o reconstruct.PruneS3Options) (reconstruct.PruneS3Result, error) {
			got = append(got, o.S3URL)
			return reconstruct.PruneS3Result{Failed: "x"}, errors.New("AccessDenied")
		}
		two := []console.ServerEntry{{ID: "a", BaselineS3: "s3://b/a", BaselineS3Retain: "7d"}, {ID: "c", BaselineS3: "s3://b/c", BaselineS3Retain: "7d"}}
		runS3PruneCycle(context.Background(), two, "", 0, now)
		if !slices.Equal(got, []string{"s3://b/a", "s3://b/c"}) {
			t.Fatalf("pruned %v", got)
		}
	})
}

func TestReadsS3Now(t *testing.T) {
	s := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	if s.readsS3Now([]string{"a"}) {
		t.Fatal("busy with nothing running")
	}
	s.restores = map[string]*console.BaselineStatus{"a": {State: "running"}}
	if !s.readsS3Now([]string{"x", "a"}) {
		t.Fatal("a running restore is not seen")
	}
}

// What the cleanup removed on purpose is not sent back: the prune's resend
// and the full read's sweep both ask the cut.
func TestResend_skipsWhatTheS3CleanupRemoves(t *testing.T) {
	now := time.Now().UTC()
	prev := s3PruneCutFn
	t.Cleanup(func() { s3PruneCutFn = prev })
	s3PruneCutFn = func(u string, at time.Time) (time.Time, bool) { return at.Add(-7 * 24 * time.Hour), u == "s3://bkt/p/" }

	root := t.TempDir()
	oldName := reconstruct.SnapshotDirName(now.Add(-10 * 24 * time.Hour).Truncate(time.Second))
	newName := reconstruct.SnapshotDirName(now.Add(-time.Hour).Truncate(time.Second))
	for _, n := range []string{oldName, newName} {
		writeSnapshotFiles(t, filepath.Join(root, n), baseline.SuccessMarker)
	}
	var sent []string
	prevUp := uploadSnapshot
	t.Cleanup(func() { uploadSnapshot = prevUp })
	uploadSnapshot = func(_ context.Context, _, dest, _ string, _ bool) (int, error) {
		sent = append(sent, dest)
		return 1, nil
	}
	prevPresent := s3ObjectPresent
	t.Cleanup(func() { s3ObjectPresent = prevPresent })
	var probed []string
	s3ObjectPresent = func(_ context.Context, url string) (bool, error) {
		probed = append(probed, url)
		return false, nil
	}
	s := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	if err := s.resendLacking(context.Background(), root, "s3://bkt/p/", oldName); !errors.Is(err, errPastS3Age) {
		t.Fatalf("past the cut: err %v, want errPastS3Age (not a send, still missing)", err)
	}
	if err := s.resendLacking(context.Background(), root, "s3://bkt/p/", newName); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(probed, "s3://bkt/p/"+newName+"/"+reconstruct.PruneMarker) {
		t.Fatalf("probed %v: a send where a cleanup runs must first ask whether the folder is being removed", probed)
	}
	if len(sent) != 1 || filepath.Base(sent[0]) != newName {
		t.Fatalf("sent %v, want only %s: the older one is past the S3 age", sent, newName)
	}
	// A destination with no age sends both, as before.
	sent = nil
	for _, n := range []string{oldName, newName} {
		if err := s.resendLacking(context.Background(), root, "s3://other/", n); err != nil {
			t.Fatal(err)
		}
	}
	if len(sent) != 2 {
		t.Fatalf("sent %v, want both where no age is set", sent)
	}
}

func TestDump_sweepSkipsWhatTheS3CleanupRemoves(t *testing.T) {
	local := t.TempDir()
	at := time.Now().UTC().Truncate(time.Second)
	old, recent := at.Add(-10*24*time.Hour), at.Add(-time.Hour)
	for _, ts := range []time.Time{old, recent} {
		writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(ts)), baseline.SuccessMarker)
	}
	prev := s3PruneCutFn
	t.Cleanup(func() { s3PruneCutFn = prev })
	s3PruneCutFn = func(_ string, now time.Time) (time.Time, bool) { return now.Add(-7 * 24 * time.Hour), true }
	root := "s3://bucket/backups/"
	ds := stubDumpUpload(t, map[string]bool{
		root + reconstruct.SnapshotDirName(old) + "/" + baseline.SuccessMarker:     false,
		root + reconstruct.SnapshotDirName(recent) + "/" + baseline.SuccessMarker:  false,
		root + reconstruct.SnapshotDirName(recent) + "/" + reconstruct.PruneMarker: false,
	}, nil)
	close(ds.hold)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: root}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	completeDumpOwn(sup, req, dumpOutcomeAt(t, local, at), nil)
	if st := sup.Status("a"); st.State != "succeeded" || st.Swept != 1 {
		t.Fatalf("status = %+v, want only the recent one swept", st)
	}
	for _, c := range ds.calls {
		if c.dest == root+reconstruct.SnapshotDirName(old) {
			t.Fatalf("the sweep sent a snapshot past the S3 age: %+v", ds.calls)
		}
	}
}

// The wiring: one sweep of the cleanup loop reads the age off the registry
// and cleans S3 after the local prune, with the local age as the floor; the
// resend cut is answered from the same registry.
func TestBaselinePruneSweep_cleansS3AfterTheLocalPrune(t *testing.T) {
	reg := liveRegistry(t)
	e, err := reg.Add(console.ServerEntry{Name: "prod", DSN: "u:p@tcp(127.0.0.1:3306)/idx",
		BaselineDir: t.TempDir(), BaselineS3: "s3://bucket/prefix/", BaselineS3Retain: "7d"})
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	prev := pruneS3Fn
	t.Cleanup(func() { pruneS3Fn = prev })
	pruneS3Fn = func(_ context.Context, o reconstruct.PruneS3Options) (reconstruct.PruneS3Result, error) {
		order = append(order, "s3 "+o.Retain.String())
		return reconstruct.PruneS3Result{}, nil
	}
	local := func(_ context.Context, _ baseline.PruneOptions) (baseline.PruneResult, error) {
		order = append(order, "local")
		return baseline.PruneResult{}, nil
	}
	baselinePruneSweep(context.Background(), reg, "", "", "10d", local)
	if !slices.Equal(order, []string{"local", "s3 240h0m0s"}) {
		t.Fatalf("order = %v, want the local prune first, then S3 at the local 10d floor", order)
	}
	now := time.Now()
	cut, on := s3PruneCut(reg, "", "10d", "s3://bucket/prefix", now)
	if !on || !cut.Equal(now.Add(-240*time.Hour)) {
		t.Fatalf("cut = %v, %v", cut, on)
	}
	e.BaselineS3Retain = ""
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	order = nil
	baselinePruneSweep(context.Background(), reg, "", "", "10d", local)
	if !slices.Equal(order, []string{"local"}) {
		t.Fatalf("order = %v: with the age cleared, S3 is not touched", order)
	}
	if _, on := s3PruneCut(reg, "", "10d", "s3://bucket/prefix", now); on {
		t.Fatal("the cut stays on after the age was cleared")
	}
}

// Past the cut but the newest local copy of a table: sent, since the S3
// cleanup keeps such a copy however old, and without it the bucket would
// hold no copy of that table.
func TestDump_sweepSendsAPastCutSnapshotThatIsATablesNewest(t *testing.T) {
	local := t.TempDir()
	at := time.Now().UTC().Truncate(time.Second)
	old, recent := at.Add(-10*24*time.Hour), at.Add(-time.Hour)
	oldDir := filepath.Join(local, reconstruct.SnapshotDirName(old))
	writeSnapshotFiles(t, oldDir, baseline.SuccessMarker)
	if err := os.WriteFile(filepath.Join(oldDir, "shop", "legacy.parquet"), []byte("rows"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(recent)), baseline.SuccessMarker)
	prev := s3PruneCutFn
	t.Cleanup(func() { s3PruneCutFn = prev })
	s3PruneCutFn = func(_ string, now time.Time) (time.Time, bool) { return now.Add(-7 * 24 * time.Hour), true }
	root := "s3://bucket/backups/"
	ds := stubDumpUpload(t, map[string]bool{
		root + reconstruct.SnapshotDirName(old) + "/" + baseline.SuccessMarker:     false,
		root + reconstruct.SnapshotDirName(old) + "/" + reconstruct.PruneMarker:    false,
		root + reconstruct.SnapshotDirName(recent) + "/" + baseline.SuccessMarker:  false,
		root + reconstruct.SnapshotDirName(recent) + "/" + reconstruct.PruneMarker: false,
	}, nil)
	close(ds.hold)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: root}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	completeDumpOwn(sup, req, dumpOutcomeAt(t, local, at), nil)
	if st := sup.Status("a"); st.Swept != 2 {
		t.Fatalf("status = %+v, want both swept: the old one holds the only shop.legacy", st)
	}
}

// A resend left out on purpose is not a send: not logged as one, and not
// counted against the bound the real resends share.
func TestResendLacking_aPurposefulSkipDoesNotUseTheBound(t *testing.T) {
	prev := resendSnapshotFn
	t.Cleanup(func() { resendSnapshotFn = prev })
	dir := t.TempDir()
	var names []string
	for i := range resendPerCycle + 2 {
		n := reconstruct.SnapshotDirName(time.Date(2026, 9, 1, i, 0, 0, 0, time.UTC))
		writeSnapshotFiles(t, filepath.Join(dir, n), baseline.SuccessMarker)
		names = append(names, n)
	}
	var sent []string
	resendSnapshotFn = func(_ context.Context, _, _, name string) error {
		if slices.Index(names, name) < resendPerCycle {
			return errNotResent
		}
		sent = append(sent, name)
		return nil
	}
	resendLacking(context.Background(), baselinePruneTarget{dir: dir, s3: "s3://b/p"}, baseline.PruneResult{NotAtDestination: names}, false)
	// The first resendPerCycle are skipped on purpose; the next ones still go.
	if !slices.Equal(sent, names[resendPerCycle:]) {
		t.Fatalf("sent %v, want %v: the skips used up the bound", sent, names[resendPerCycle:])
	}
}

// A probe of the cleanup's mark that fails is a failure, said and counted as
// one: on a role without s3:ListBucket every probe fails, and treating that
// as "left out on purpose" stopped every resend with no word.
func TestResend_aFailedPruneProbeIsAFailure(t *testing.T) {
	prev, prevPresent := s3PruneCutFn, s3ObjectPresent
	t.Cleanup(func() { s3PruneCutFn, s3ObjectPresent = prev, prevPresent })
	s3PruneCutFn = func(_ string, now time.Time) (time.Time, bool) { return now.Add(-7 * 24 * time.Hour), true }
	s3ObjectPresent = func(context.Context, string) (bool, error) { return false, errors.New("403 Forbidden") }
	root := t.TempDir()
	name := reconstruct.SnapshotDirName(time.Now().UTC().Add(-time.Hour).Truncate(time.Second))
	writeSnapshotFiles(t, filepath.Join(root, name), baseline.SuccessMarker)
	sent := 0
	prevUp := uploadSnapshot
	t.Cleanup(func() { uploadSnapshot = prevUp })
	uploadSnapshot = func(context.Context, string, string, string, bool) (int, error) { sent++; return 1, nil }
	s := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	err := s.resendLacking(context.Background(), root, "s3://bkt/p/", name)
	if err == nil || errors.Is(err, errNotResent) || !strings.Contains(err.Error(), "s3:ListBucket") || sent != 0 {
		t.Fatalf("err = %v, sends %d: want a real failure naming s3:ListBucket, and nothing sent into a folder that may be going", err, sent)
	}
}

// What is left out on purpose is not listed as being sent.
func TestResendLacking_purposefulSkipsLeaveTheRecord(t *testing.T) {
	prev := resendSnapshotFn
	t.Cleanup(func() { resendSnapshotFn = prev })
	dir := t.TempDir()
	a := reconstruct.SnapshotDirName(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	b := reconstruct.SnapshotDirName(time.Date(2026, 9, 1, 1, 0, 0, 0, time.UTC))
	for _, n := range []string{a, b} {
		writeSnapshotFiles(t, filepath.Join(dir, n), baseline.SuccessMarker)
	}
	resendSnapshotFn = func(_ context.Context, _, _, name string) error {
		if name == a {
			return errNotResent
		}
		return errors.New("network")
	}
	resendLacking(context.Background(), baselinePruneTarget{dir: dir, s3: "s3://b/p"}, baseline.PruneResult{NotAtDestination: []string{a, b}, KeptNotDurable: 2}, false)
	rec, ok, err := baseline.ReadNotInDestination(dir)
	if err != nil || !ok || !slices.Equal(rec.Names, []string{b}) || rec.Count != 1 {
		t.Fatalf("record = %+v ok=%v err=%v, want only %s, count 1", rec, ok, err, b)
	}
}

// Past the S3 age: not sent, not counted against the bound, but still listed
// as missing, since it stays on this disk without a copy in S3.
func TestResendLacking_pastTheAgeStaysInTheRecord(t *testing.T) {
	prev := resendSnapshotFn
	t.Cleanup(func() { resendSnapshotFn = prev })
	dir := t.TempDir()
	var names []string
	for i := range resendPerCycle + 1 {
		n := reconstruct.SnapshotDirName(time.Date(2026, 9, 1, i, 0, 0, 0, time.UTC))
		writeSnapshotFiles(t, filepath.Join(dir, n), baseline.SuccessMarker)
		names = append(names, n)
	}
	var tried []string
	resendSnapshotFn = func(_ context.Context, _, _, name string) error {
		if slices.Index(names, name) < resendPerCycle {
			return errPastS3Age
		}
		tried = append(tried, name)
		return errors.New("network")
	}
	resendLacking(context.Background(), baselinePruneTarget{dir: dir, s3: "s3://b/p"}, baseline.PruneResult{NotAtDestination: names, KeptNotDurable: len(names)}, false)
	if !slices.Equal(tried, names[resendPerCycle:]) {
		t.Fatalf("tried %v: the past-age skips used up the bound", tried)
	}
	rec, ok, err := baseline.ReadNotInDestination(dir)
	if err != nil || !ok || !slices.Equal(rec.Names, names) || rec.Count != len(names) {
		t.Fatalf("record = %+v, want every name still listed", rec)
	}
}
