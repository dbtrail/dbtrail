package reconstruct

import (
	"context"
	"errors"
	"io"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// fakePruneStore is a bucket prefix as a set of keys, with the operations
// made on it in order, and errors to inject.
type fakePruneStore struct {
	keys      map[string]bool
	ops       []string
	reads     int
	dirsErr   error
	readErr   map[string]error // by snapshot name
	failPut   string           // key suffix whose Put fails
	failDel   string           // key suffix whose Delete fails
	failOnce  bool             // the injected failure happens once
	emptyRead string           // snapshot whose folder read returns nothing
}

func newFakePruneStore(keys ...string) *fakePruneStore {
	f := &fakePruneStore{keys: map[string]bool{}, readErr: map[string]error{}}
	for _, k := range keys {
		f.keys[k] = true
	}
	return f
}

func (f *fakePruneStore) ListDirs(_ context.Context, prefix string) ([]string, error) {
	if f.dirsErr != nil {
		return nil, f.dirsErr
	}
	set := map[string]bool{}
	for k := range f.keys {
		if i := strings.IndexByte(k, '/'); i > 0 {
			set[k[:i]] = true
		}
	}
	var out []string
	for d := range set {
		out = append(out, d)
	}
	sort.Strings(out)
	return out, nil
}

func (f *fakePruneStore) ListInfoFrom(_ context.Context, prefix, _ string) ([]storage.ObjectInfo, error) {
	f.reads++
	if err := f.readErr[strings.TrimSuffix(prefix, "/")]; err != nil {
		return nil, err
	}
	if f.emptyRead != "" && strings.TrimSuffix(prefix, "/") == f.emptyRead {
		return nil, nil
	}
	var out []storage.ObjectInfo
	for k := range f.keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, storage.ObjectInfo{Key: k})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (f *fakePruneStore) Put(_ context.Context, key string, _ io.Reader) error {
	if f.failPut != "" && strings.HasSuffix(key, f.failPut) {
		if f.failOnce {
			f.failPut = ""
		}
		return errors.New("AccessDenied")
	}
	f.ops = append(f.ops, "put "+key)
	f.keys[key] = true
	return nil
}

func (f *fakePruneStore) Delete(_ context.Context, key string) error {
	if f.failDel != "" && strings.HasSuffix(key, f.failDel) {
		if f.failOnce {
			f.failDel = ""
		}
		return errors.New("AccessDenied")
	}
	f.ops = append(f.ops, "delete "+key)
	delete(f.keys, key)
	return nil
}

func (f *fakePruneStore) under(name string) []string {
	var out []string
	for k := range f.keys {
		if strings.HasPrefix(k, name+"/") {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func snapName(at time.Time) string { return snapshotdir.Name(at) }

// snapKeys is a complete snapshot holding the given schema.table names.
func snapKeys(at time.Time, marker string, tables ...string) []string {
	n := snapName(at)
	var out []string
	if marker != "" {
		out = append(out, n+"/"+marker)
	}
	for _, t := range tables {
		s, tb, _ := strings.Cut(t, ".")
		out = append(out, n+"/"+s+"/"+tb+".parquet")
	}
	return out
}

func TestPruneS3(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	week := 7 * 24 * time.Hour
	old := now.Add(-10 * 24 * time.Hour)
	older := now.Add(-12 * 24 * time.Hour)
	recent := now.Add(-time.Hour)
	run := func(t *testing.T, f *fakePruneStore, retain time.Duration) (PruneS3Result, error) {
		t.Helper()
		return pruneS3With(context.Background(), f, PruneS3Options{S3URL: "s3://b/p", Retain: retain, Now: now})
	}

	t.Run("a zero age is refused", func(t *testing.T) {
		f := newFakePruneStore(snapKeys(older, baseline.SuccessMarker, "shop.orders")...)
		if _, err := run(t, f, 0); err == nil || len(f.ops) != 0 {
			t.Fatalf("err=%v ops=%v, want a refusal and nothing touched", err, f.ops)
		}
	})

	t.Run("nothing older than the age: no snapshot is read", func(t *testing.T) {
		f := newFakePruneStore(snapKeys(recent, baseline.SuccessMarker, "shop.orders")...)
		res, err := run(t, f, week)
		if err != nil || len(res.Pruned) != 0 || f.reads != 0 || len(f.ops) != 0 {
			t.Fatalf("res=%+v err=%v reads=%d ops=%v", res, err, f.reads, f.ops)
		}
	})

	t.Run("an old snapshot a newer one covers is removed, in the safe order", func(t *testing.T) {
		keys := append(snapKeys(old, baseline.SuccessMarker, "shop.orders", "shop.items"), snapName(old)+"/views.sql")
		keys = append(keys, snapKeys(recent, baseline.SuccessMarker, "shop.orders", "shop.items")...)
		f := newFakePruneStore(keys...)
		res, err := run(t, f, week)
		if err != nil || !slices.Equal(res.Pruned, []string{snapName(old)}) {
			t.Fatalf("res=%+v err=%v", res, err)
		}
		if left := f.under(snapName(old)); len(left) != 0 {
			t.Fatalf("left behind: %v", left)
		}
		if len(f.under(snapName(recent))) != 3 {
			t.Fatalf("the newer snapshot was touched: %v", f.under(snapName(recent)))
		}
		n := snapName(old)
		// Marked first; the success marker is the first delete; the two
		// markers go last, the prune's own after _INCOMPLETE.
		want := []string{"put " + n + "/" + PruneMarker, "put " + n + "/" + baseline.IncompleteMarker, "delete " + n + "/" + baseline.SuccessMarker}
		if !slices.Equal(f.ops[:3], want) {
			t.Fatalf("first ops = %v, want %v", f.ops[:3], want)
		}
		tail := f.ops[len(f.ops)-2:]
		if !slices.Equal(tail, []string{"delete " + n + "/" + baseline.IncompleteMarker, "delete " + n + "/" + PruneMarker}) {
			t.Fatalf("last ops = %v", tail)
		}
	})

	t.Run("the newest snapshot of each table is kept however old", func(t *testing.T) {
		keys := snapKeys(older, baseline.SuccessMarker, "shop.orders", "shop.legacy")
		keys = append(keys, snapKeys(old, baseline.SuccessMarker, "shop.orders")...)
		keys = append(keys, snapKeys(recent, baseline.SuccessMarker, "shop.orders")...)
		f := newFakePruneStore(keys...)
		res, err := run(t, f, week)
		if err != nil || !slices.Equal(res.Pruned, []string{snapName(old)}) || res.KeptNewest != 1 {
			t.Fatalf("res=%+v err=%v: want only %s removed (older holds the only shop.legacy)", res, err, snapName(old))
		}
	})

	t.Run("everything old and alone: the newest stays", func(t *testing.T) {
		f := newFakePruneStore(append(snapKeys(older, baseline.SuccessMarker, "shop.orders"), snapKeys(old, baseline.SuccessMarker, "shop.orders")...)...)
		res, err := run(t, f, week)
		if err != nil || !slices.Equal(res.Pruned, []string{snapName(older)}) || len(f.under(snapName(old))) != 2 {
			t.Fatalf("res=%+v err=%v left=%v", res, err, f.under(snapName(old)))
		}
	})

	t.Run("an incomplete snapshot is never touched and covers nothing", func(t *testing.T) {
		// The newer one is an upload in progress or failed: it does not make
		// the old complete one redundant, and it is not ours to remove.
		keys := snapKeys(older, baseline.IncompleteMarker, "shop.orders")
		keys = append(keys, snapKeys(old, baseline.SuccessMarker, "shop.orders")...)
		keys = append(keys, snapKeys(recent, baseline.IncompleteMarker, "shop.orders")...)
		f := newFakePruneStore(keys...)
		res, err := run(t, f, week)
		if err != nil || len(res.Pruned) != 0 || res.KeptIncomplete != 1 || len(f.ops) != 0 {
			t.Fatalf("res=%+v err=%v ops=%v", res, err, f.ops)
		}
	})

	t.Run("a snapshot with no marker is complete by default", func(t *testing.T) {
		keys := snapKeys(old, "", "shop.orders")
		keys = append(keys, snapKeys(recent, "", "shop.orders")...)
		f := newFakePruneStore(keys...)
		res, err := run(t, f, week)
		if err != nil || !slices.Equal(res.Pruned, []string{snapName(old)}) {
			t.Fatalf("res=%+v err=%v", res, err)
		}
	})

	t.Run("keys that are not snapshot folders are never touched", func(t *testing.T) {
		keys := snapKeys(old, baseline.SuccessMarker, "shop.orders")
		keys = append(keys, snapKeys(recent, baseline.SuccessMarker, "shop.orders")...)
		keys = append(keys,
			"bintrail_id=abc/event_date=2026-01-01/x.parquet",
			"other-server/"+snapName(older)+"/_SUCCESS",
			"other-server/"+snapName(older)+"/shop/orders.parquet",
			"notes.txt/x")
		f := newFakePruneStore(keys...)
		if _, err := run(t, f, week); err != nil {
			t.Fatal(err)
		}
		for _, k := range []string{"bintrail_id=abc/event_date=2026-01-01/x.parquet", "other-server/" + snapName(older) + "/_SUCCESS", "notes.txt/x"} {
			if !f.keys[k] {
				t.Fatalf("%s was removed", k)
			}
		}
		for _, op := range f.ops {
			if !strings.HasPrefix(strings.Fields(op)[1], snapName(old)+"/") {
				t.Fatalf("an operation outside the pruned snapshot: %s", op)
			}
		}
	})

	t.Run("the listing fails: nothing is removed", func(t *testing.T) {
		f := newFakePruneStore(append(snapKeys(old, baseline.SuccessMarker, "shop.orders"), snapKeys(recent, baseline.SuccessMarker, "shop.orders")...)...)
		f.dirsErr = errors.New("AccessDenied")
		if _, err := run(t, f, week); err == nil || len(f.ops) != 0 {
			t.Fatalf("err=%v ops=%v", err, f.ops)
		}
	})

	t.Run("one snapshot folder cannot be read: nothing is removed", func(t *testing.T) {
		// It may be the newest copy of a table, which would make an older
		// one a keeper.
		f := newFakePruneStore(append(snapKeys(old, baseline.SuccessMarker, "shop.orders"), snapKeys(recent, baseline.SuccessMarker, "shop.orders")...)...)
		f.readErr[snapName(recent)] = errors.New("SlowDown")
		if _, err := run(t, f, week); err == nil || len(f.ops) != 0 {
			t.Fatalf("err=%v ops=%v", err, f.ops)
		}
	})

	t.Run("the mark cannot be written: nothing is removed", func(t *testing.T) {
		f := newFakePruneStore(append(snapKeys(old, baseline.SuccessMarker, "shop.orders"), snapKeys(recent, baseline.SuccessMarker, "shop.orders")...)...)
		f.failPut = PruneMarker
		res, err := run(t, f, week)
		if err == nil || len(f.ops) != 0 || len(res.Pruned) != 0 {
			t.Fatalf("res=%+v err=%v ops=%v", res, err, f.ops)
		}
		if res.Failed != snapName(old) {
			t.Fatalf("Failed = %q, want the snapshot it stopped on", res.Failed)
		}
	})

	t.Run("stopped midway: what is left reads as incomplete, and the next run finishes it", func(t *testing.T) {
		f := newFakePruneStore(append(snapKeys(old, baseline.SuccessMarker, "shop.orders", "shop.items"), snapKeys(recent, baseline.SuccessMarker, "shop.orders", "shop.items")...)...)
		f.failDel, f.failOnce = "items.parquet", true
		if _, err := run(t, f, week); err == nil {
			t.Fatal("a failed delete was not reported")
		}
		n := snapName(old)
		if !f.keys[n+"/"+baseline.IncompleteMarker] || f.keys[n+"/"+baseline.SuccessMarker] {
			t.Fatalf("a half-removed snapshot does not read as incomplete: %v", f.under(n))
		}
		// The S3 listing the page and reconstruct use leaves it out.
		x := &s3SnapshotIndex{prefix: "s3://b/p", lister: f}
		if _, _, ok, err := x.readDir(context.Background(), s3SnapshotDir{n, old}); err != nil || ok {
			t.Fatalf("readDir ok=%v err=%v: the half-removed snapshot is listed as complete", ok, err)
		}
		res, err := run(t, f, week)
		if err != nil || res.Resumed != 1 || len(f.under(n)) != 0 {
			t.Fatalf("second run: res=%+v err=%v left=%v", res, err, f.under(n))
		}
	})

	t.Run("a half-removed snapshot is finished though nothing else covers its table", func(t *testing.T) {
		// It is no longer a copy of anything: the mark is what decides.
		n := snapName(older)
		f := newFakePruneStore(n+"/"+PruneMarker, n+"/"+baseline.IncompleteMarker, n+"/shop/legacy.parquet")
		f.keys[snapName(recent)+"/"+baseline.SuccessMarker] = true
		f.keys[snapName(recent)+"/shop/orders.parquet"] = true
		res, err := run(t, f, week)
		if err != nil || res.Resumed != 1 || len(f.under(n)) != 0 {
			t.Fatalf("res=%+v err=%v left=%v", res, err, f.under(n))
		}
	})

	t.Run("a snapshot dated in the future covers nothing and is not removed", func(t *testing.T) {
		// A writer with its clock ahead: readers look at-or-before now, so
		// it is not the present, and must not make the real copy redundant.
		future := now.Add(2 * time.Hour)
		f := newFakePruneStore(append(snapKeys(old, baseline.SuccessMarker, "shop.orders"), snapKeys(future, baseline.SuccessMarker, "shop.orders")...)...)
		res, err := run(t, f, week)
		if err != nil || len(res.Pruned) != 0 || len(f.ops) != 0 {
			t.Fatalf("res=%+v err=%v ops=%v", res, err, f.ops)
		}
	})

	t.Run("schema and table are told apart when a name has a dot", func(t *testing.T) {
		// Schema "a.b" table "c" and schema "a" table "b.c" are two tables.
		n1, n2 := snapName(old), snapName(recent)
		f := newFakePruneStore(n1+"/"+baseline.SuccessMarker, n1+"/a/b.c.parquet", n2+"/"+baseline.SuccessMarker, n2+"/a.b/c.parquet")
		res, err := run(t, f, week)
		if err != nil || len(res.Pruned) != 0 {
			t.Fatalf("res=%+v err=%v: the only copy of a.b.c was taken as covered", res, err)
		}
	})

	t.Run("two writers sharing a prefix do not cover each other", func(t *testing.T) {
		keys := append(snapKeys(old, baseline.SuccessMarker, "shop.orders"), snapName(old)+"/"+baseline.WriterMarkerPrefix+"aaa")
		keys = append(keys, snapKeys(recent, baseline.SuccessMarker, "shop.orders")...)
		keys = append(keys, snapName(recent)+"/"+baseline.WriterMarkerPrefix+"bbb")
		f := newFakePruneStore(keys...)
		res, err := run(t, f, week)
		if err != nil || len(res.Pruned) != 0 {
			t.Fatalf("res=%+v err=%v: one writer's newer copy removed the other's only one", res, err)
		}
		// The same writer does cover itself.
		keys2 := append(snapKeys(old, baseline.SuccessMarker, "shop.orders"), snapName(old)+"/"+baseline.WriterMarkerPrefix+"aaa")
		keys2 = append(keys2, snapKeys(recent, baseline.SuccessMarker, "shop.orders")...)
		keys2 = append(keys2, snapName(recent)+"/"+baseline.WriterMarkerPrefix+"aaa")
		f2 := newFakePruneStore(keys2...)
		if res, err := run(t, f2, week); err != nil || !slices.Equal(res.Pruned, []string{snapName(old)}) {
			t.Fatalf("same writer: res=%+v err=%v", res, err)
		}
	})

	t.Run("a folder that lists empty is a doubt", func(t *testing.T) {
		// The directory listing said it holds objects; a read that returns
		// none is a contradiction (a lagging compatible store).
		f := newFakePruneStore(append(snapKeys(old, baseline.SuccessMarker, "shop.orders"), snapKeys(recent, baseline.SuccessMarker, "shop.orders")...)...)
		f.emptyRead = snapName(recent)
		if _, err := run(t, f, week); err == nil || len(f.ops) != 0 {
			t.Fatalf("err=%v ops=%v", err, f.ops)
		}
	})

	t.Run("a marked folder that is still complete is weighed again", func(t *testing.T) {
		// A stopped run marked it, and since then the newer copy of its
		// table went (removed by hand, or by a bucket rule): it is the newest
		// now, so it is kept, and its mark is taken off.
		n := snapName(older)
		f := newFakePruneStore(append(snapKeys(older, baseline.SuccessMarker, "shop.orders"), n+"/"+PruneMarker)...)
		res, err := run(t, f, week)
		if err != nil || len(res.Pruned) != 0 || !f.keys[n+"/shop/orders.parquet"] || f.keys[n+"/"+PruneMarker] || !f.keys[n+"/"+baseline.SuccessMarker] {
			t.Fatalf("res=%+v err=%v left=%v", res, err, f.under(n))
		}
	})

	t.Run("a marked folder that still reads whole covers nothing in that run", func(t *testing.T) {
		// On a store whose listing lags, a folder whose removal began can
		// still list _SUCCESS: it must not make an older copy redundant.
		n := snapName(recent)
		keys := append(snapKeys(old, baseline.SuccessMarker, "shop.orders"), snapKeys(recent, baseline.SuccessMarker, "shop.orders")...)
		keys = append(keys, n+"/"+PruneMarker)
		f := newFakePruneStore(keys...)
		res, err := run(t, f, week)
		if err != nil || len(res.Pruned) != 0 || f.keys[n+"/"+PruneMarker] {
			t.Fatalf("res=%+v err=%v marked=%v: the old copy went in the run that found the mark", res, err, f.keys[n+"/"+PruneMarker])
		}
		// The next run weighs it as any other.
		if res, err := run(t, f, week); err != nil || !slices.Equal(res.Pruned, []string{snapName(old)}) {
			t.Fatalf("next run: res=%+v err=%v", res, err)
		}
	})

	t.Run("a marked folder listing both markers keeps its mark", func(t *testing.T) {
		// _INCOMPLETE was written, so _SUCCESS may be gone and the listing
		// lagging: taking the mark off would leave a half-removed folder
		// every later run reads as a failed upload.
		n := snapName(recent)
		keys := append(snapKeys(old, baseline.SuccessMarker, "shop.orders"), snapKeys(recent, baseline.SuccessMarker, "shop.orders")...)
		keys = append(keys, n+"/"+PruneMarker, n+"/"+baseline.IncompleteMarker)
		f := newFakePruneStore(keys...)
		res, err := run(t, f, week)
		if err != nil || len(res.Pruned) != 0 || !f.keys[n+"/"+PruneMarker] {
			t.Fatalf("res=%+v err=%v: want nothing removed and the mark kept", res, err)
		}
	})

	t.Run("a run stops at its object budget", func(t *testing.T) {
		var keys []string
		for i := range 4 {
			keys = append(keys, snapKeys(old.Add(-time.Duration(i)*time.Hour), baseline.SuccessMarker, "shop.orders", "shop.items")...)
		}
		keys = append(keys, snapKeys(recent, baseline.SuccessMarker, "shop.orders", "shop.items")...)
		f := newFakePruneStore(keys...)
		// Each snapshot is 3 objects; 7 fits two, never splits a third.
		res, err := pruneS3With(context.Background(), f, PruneS3Options{S3URL: "s3://b/p", Retain: week, Now: now, MaxObjects: 7})
		if err != nil || len(res.Pruned) != 2 || res.Deferred != 2 || res.Stopped != "bound" {
			t.Fatalf("res=%+v err=%v", res, err)
		}
		// One larger than the whole budget still goes alone, or it never would.
		f = newFakePruneStore(keys...)
		if res, err := pruneS3With(context.Background(), f, PruneS3Options{S3URL: "s3://b/p", Retain: week, Now: now, MaxObjects: 1}); err != nil || len(res.Pruned) != 1 {
			t.Fatalf("budget 1: res=%+v err=%v", res, err)
		}
	})

	t.Run("a reader that starts mid-run stops the run before the next folder", func(t *testing.T) {
		var keys []string
		for i := range 3 {
			keys = append(keys, snapKeys(old.Add(-time.Duration(i)*time.Hour), baseline.SuccessMarker, "shop.orders")...)
		}
		keys = append(keys, snapKeys(recent, baseline.SuccessMarker, "shop.orders")...)
		f := newFakePruneStore(keys...)
		calls := 0
		res, err := pruneS3With(context.Background(), f, PruneS3Options{S3URL: "s3://b/p", Retain: week, Now: now,
			Proceed: func() bool { calls++; return calls < 2 }})
		if err != nil || len(res.Pruned) != 1 || res.Deferred != 2 || res.Stopped != "busy" {
			t.Fatalf("res=%+v err=%v", res, err)
		}
	})

	t.Run("at most MaxPerRun snapshots go in one run", func(t *testing.T) {
		var keys []string
		for i := range 5 {
			keys = append(keys, snapKeys(old.Add(-time.Duration(i)*time.Hour), baseline.SuccessMarker, "shop.orders")...)
		}
		keys = append(keys, snapKeys(recent, baseline.SuccessMarker, "shop.orders")...)
		f := newFakePruneStore(keys...)
		res, err := pruneS3With(context.Background(), f, PruneS3Options{S3URL: "s3://b/p", Retain: week, Now: now, MaxPerRun: 2})
		if err != nil || len(res.Pruned) != 2 || res.Deferred != 3 {
			t.Fatalf("res=%+v err=%v", res, err)
		}
		// Oldest first.
		if res.Pruned[0] != snapName(old.Add(-4*time.Hour)) {
			t.Fatalf("pruned %v, want the oldest first", res.Pruned)
		}
	})
}
