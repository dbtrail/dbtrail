package console

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// A statement needs the directory of the tails before it runs, and an update
// of the tails holds the keeper's lock for most of its length. A statement
// does not wait for that update while the directory is the very one the
// keeper made; any other thing under its name waits and goes the normal way.

func dirKeeper(t *testing.T) (*tailKeeper, string) {
	t.Helper()
	k := newTailKeeper(&fakeIndex{}, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
	t.Cleanup(k.close)
	dir, err := k.ensureDir()
	if err != nil {
		t.Fatal(err)
	}
	return k, dir
}

type dirAnswer struct {
	dir string
	err error
}

func askDir(k *tailKeeper) <-chan dirAnswer {
	ch := make(chan dirAnswer, 1)
	go func() {
		d, err := k.ensureDir()
		ch <- dirAnswer{d, err}
	}()
	return ch
}

func answeredAtOnce(t *testing.T, ch <-chan dirAnswer) dirAnswer {
	t.Helper()
	select {
	case a := <-ch:
		return a
	case <-time.After(2 * time.Second):
		t.Fatal("it waited for the update")
		return dirAnswer{}
	}
}

// waitsThenAnswers: no answer while the lock is held, one after release.
func waitsThenAnswers(t *testing.T, ch <-chan dirAnswer, release func()) dirAnswer {
	t.Helper()
	select {
	case a := <-ch:
		release()
		t.Fatalf("answered %q during the update, want it to wait", a.dir)
	case <-time.After(150 * time.Millisecond):
	}
	release()
	select {
	case a := <-ch:
		return a
	case <-time.After(5 * time.Second):
		t.Fatal("no answer after the update ended")
		return dirAnswer{}
	}
}

func TestEnsureDir_theSameDirectoryDoesNotWaitForAnUpdate(t *testing.T) {
	k, dir := dirKeeper(t)
	k.mu.Lock()
	defer k.mu.Unlock()
	a := answeredAtOnce(t, askDir(k))
	if a.err != nil || a.dir != dir {
		t.Errorf("%q %v, want %q", a.dir, a.err, dir)
	}
	// What is written in it changes its times, not what it is.
	if err := os.WriteFile(filepath.Join(dir, "a.parquet"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(dir, time.Now().Add(time.Hour), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if a := answeredAtOnce(t, askDir(k)); a.err != nil || a.dir != dir {
		t.Errorf("after a write in it: %q %v", a.dir, a.err)
	}
}

func TestEnsureDir_anythingElseUnderItsNameWaits(t *testing.T) {
	for _, tc := range []struct {
		name string
		swap func(t *testing.T, dir string)
		// samePath: after the update the keeper still takes it for its own.
		samePath bool
		// fastAfter: the next statement during an update does not wait.
		fastAfter bool
	}{
		{"removed", func(t *testing.T, dir string) {
			must(t, os.RemoveAll(dir))
		}, false, true},
		{"replaced by another directory", func(t *testing.T, dir string) {
			must(t, os.Rename(dir, dir+".old"))
			must(t, os.Mkdir(dir, 0o700))
		}, false, true},
		{"replaced by a file", func(t *testing.T, dir string) {
			must(t, os.Rename(dir, dir+".old"))
			must(t, os.WriteFile(dir, []byte("x"), 0o600))
		}, false, true},
		{"a link to another directory", func(t *testing.T, dir string) {
			must(t, os.Rename(dir, dir+".old"))
			must(t, os.Mkdir(dir+".other", 0o700))
			must(t, os.Symlink(dir+".other", dir))
		}, false, true},
		{"a link to the directory itself, moved", func(t *testing.T, dir string) {
			must(t, os.Rename(dir, dir+".moved"))
			must(t, os.Symlink(dir+".moved", dir))
		}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k, dir := dirKeeper(t)
			k.mu.Lock()
			tc.swap(t, dir)
			ch := askDir(k)
			a := waitsThenAnswers(t, ch, k.mu.Unlock)
			if a.err != nil {
				t.Fatal(a.err)
			}
			if (a.dir == dir) != tc.samePath {
				t.Errorf("after the update: %q, was %q, want the same path: %v", a.dir, dir, tc.samePath)
			}
			if fi, err := os.Stat(a.dir); err != nil || !fi.IsDir() {
				t.Errorf("what it answered is not a directory: %v", err)
			}
			k.mu.Lock()
			next := askDir(k)
			if tc.fastAfter {
				defer k.mu.Unlock()
				if b := answeredAtOnce(t, next); b.err != nil || b.dir != a.dir {
					t.Errorf("the directory made again: %q %v, want %q", b.dir, b.err, a.dir)
				}
				return
			}
			waitsThenAnswers(t, next, k.mu.Unlock)
		})
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// No directory yet, or none this keeper holds open: the identity could not
// be trusted, so it waits as it always did.
func TestEnsureDir_withNothingToCompareItWaits(t *testing.T) {
	t.Run("no directory yet", func(t *testing.T) {
		k := newTailKeeper(&fakeIndex{}, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
		t.Cleanup(k.close)
		k.mu.Lock()
		if a := waitsThenAnswers(t, askDir(k), k.mu.Unlock); a.err != nil || a.dir == "" {
			t.Errorf("%q %v", a.dir, a.err)
		}
	})
	t.Run("a directory it could not lock", func(t *testing.T) {
		k, dir := dirKeeper(t)
		k.dirNow.Store(nil)
		k.mu.Lock()
		if a := waitsThenAnswers(t, askDir(k), k.mu.Unlock); a.err != nil || a.dir != dir {
			t.Errorf("%q %v", a.dir, a.err)
		}
	})
}

// With no update running nothing changes: the check runs under the lock, and
// a directory that is gone is made again.
func TestEnsureDir_noUpdateRunning(t *testing.T) {
	k, dir := dirKeeper(t)
	if again, err := k.ensureDir(); err != nil || again != dir {
		t.Fatalf("%q %v", again, err)
	}
	must(t, os.RemoveAll(dir))
	// Found gone, it stops vouching for that directory before its handle is
	// closed: the number could go to another directory after.
	k.mu.Lock()
	k.dirCheckLocked()
	left := k.dirNow.Load()
	k.mu.Unlock()
	if left != nil {
		t.Errorf("still vouching for %q, which is gone", left.path)
	}
	made, err := k.ensureDir()
	if err != nil || made == dir {
		t.Fatalf("%q %v, want another directory", made, err)
	}
	if _, err := os.Stat(made); err != nil {
		t.Error(err)
	}
}

// Statements asking while updates come and go and the directory is replaced
// under them: every answer is one the keeper made (run with -race).
func TestEnsureDir_concurrent(t *testing.T) {
	k, first := dirKeeper(t)
	parent := filepath.Dir(first)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var asked, failed atomic.Int64
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				d, err := k.ensureDir()
				asked.Add(1)
				if err != nil || !strings.HasPrefix(filepath.Base(d), "bintrail-tails-") || filepath.Dir(d) != parent {
					failed.Add(1)
				}
			}
		}()
	}
	for i := range 150 {
		k.mu.Lock()
		if i%5 == 0 && k.dir != "" {
			_ = os.RemoveAll(k.dir)
		}
		time.Sleep(200 * time.Microsecond)
		k.mu.Unlock()
		time.Sleep(100 * time.Microsecond)
	}
	close(stop)
	wg.Wait()
	if asked.Load() == 0 || failed.Load() != 0 {
		t.Errorf("%d asked, %d wrong", asked.Load(), failed.Load())
	}
}

// The hook that holds a keeper's lock is for tests only.
func TestHoldTailsUpdateHookIsTestOnly(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	hook := filepath.Join(root, "internal", "console", "testing_tails.go")
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); path != root && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || path == hook {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), "HoldTailsUpdateForTest") {
			t.Errorf("%s names HoldTailsUpdateForTest: only tests may hold a tail keeper's lock", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func waitsForUpdate(k *tailKeeper, ctx context.Context, read []views.BaselineTable, limit time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		k.waitUpdateOver(ctx, read, limit, false)
	}()
	return done
}

func returnsSoon(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: still waiting", what)
	}
}

func stillWaiting(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
		t.Fatalf("%s: it did not wait", what)
	case <-time.After(150 * time.Millisecond):
	}
}

// publishTail says the last update gave t a version, as followOnce does.
func publishTail(k *tailKeeper, t views.BaselineTable) {
	p := &k.pub
	p.mu.Lock()
	defer p.mu.Unlock()
	key := tailKey(t.Schema, t.Table)
	if p.cuts == nil {
		p.cuts, p.views, p.building = map[string]string{}, map[string]tailView{}, map[string]bool{}
	}
	p.cuts[key], p.views[key] = tailCutOf(t), tailView{}
	p.pubGen = p.gen
	p.markPublishedLocked()
}

// updateRuns marks an update as running and holds its lock.
func updateRuns(k *tailKeeper) {
	k.mu.Lock()
	k.pub.mu.Lock()
	defer k.pub.mu.Unlock()
	k.pub.gen++
	k.pub.updating = true
}

// A statement waits for the running update only over a table with no version
// that may get one, for that update alone, and never past its limit, its own
// end or the keeper's.
func TestWaitUpdateOver(t *testing.T) {
	orders := []views.BaselineTable{keeperTable("orders")}
	const long = time.Minute
	t.Run("a table with a version does not wait", func(t *testing.T) {
		k, _ := dirKeeper(t)
		publishTail(k, orders[0])
		updateRuns(k)
		defer k.mu.Unlock()
		returnsSoon(t, waitsForUpdate(k, context.Background(), orders, long), "with a version")
	})
	t.Run("a newer cut of it waits", func(t *testing.T) {
		k, _ := dirKeeper(t)
		publishTail(k, orders[0])
		updateRuns(k)
		done := waitsForUpdate(k, context.Background(), []views.BaselineTable{newerCut(orders[0])}, long)
		stillWaiting(t, done, "a newer cut")
		k.pub.mu.Lock()
		k.pub.updating = false
		k.pub.mu.Unlock()
		k.mu.Unlock()
		returnsSoon(t, done, "after the update")
	})
	t.Run("a table being built waits", func(t *testing.T) {
		k, _ := dirKeeper(t)
		key := tailKey("shop", "orders")
		k.pub.cuts, k.pub.building = map[string]string{key: tailCutOf(orders[0])}, map[string]bool{key: true}
		updateRuns(k)
		done := waitsForUpdate(k, context.Background(), orders, long)
		stillWaiting(t, done, "being built")
		k.mu.Unlock()
		returnsSoon(t, done, "after the update")
	})
	t.Run("after an update that failed it waits", func(t *testing.T) {
		k, _ := dirKeeper(t)
		k.pub.cuts = map[string]string{tailKey("shop", "orders"): tailCutOf(orders[0])}
		updateRuns(k)
		done := waitsForUpdate(k, context.Background(), orders, long)
		stillWaiting(t, done, "after a failed update")
		k.mu.Unlock()
		returnsSoon(t, done, "after the update")
	})
	t.Run("a table that cannot have a tail does not wait, nor do those read with it", func(t *testing.T) {
		k, _ := dirKeeper(t)
		none := keeperTable("none")
		k.pub.cuts, k.pub.building = map[string]string{tailKey("shop", "none"): tailCutOf(none)}, map[string]bool{}
		updateRuns(k)
		defer k.mu.Unlock()
		returnsSoon(t, waitsForUpdate(k, context.Background(), []views.BaselineTable{orders[0], none}, long), "cannot have one")
	})
	t.Run("no table asks nothing and starts nothing", func(t *testing.T) {
		k, _ := dirKeeper(t)
		k.pub.mu.Lock()
		k.pub.auto = true
		k.pub.mu.Unlock()
		returnsSoon(t, waitsForUpdate(k, context.Background(), nil, long), "no table")
		k.pub.mu.Lock()
		defer k.pub.mu.Unlock()
		if len(k.pub.asked) != 0 || k.pub.running {
			t.Errorf("for a statement that reads no table: asked %v, follower started %v", k.pub.asked, k.pub.running)
		}
	})
	t.Run("no update running does not wait, and the table is recorded for the next", func(t *testing.T) {
		k, _ := dirKeeper(t)
		k.mu.Lock()
		defer k.mu.Unlock()
		returnsSoon(t, waitsForUpdate(k, context.Background(), orders, long), "no update")
		k.pub.mu.Lock()
		defer k.pub.mu.Unlock()
		if _, ok := k.pub.asked[tailKey("shop", "orders")]; !ok {
			t.Error("the next update does not know the statement reads this table")
		}
	})
	t.Run("a table with a version and one without waits", func(t *testing.T) {
		k, _ := dirKeeper(t)
		publishTail(k, orders[0])
		updateRuns(k)
		done := waitsForUpdate(k, context.Background(), []views.BaselineTable{orders[0], keeperTable("lines")}, long)
		stillWaiting(t, done, "one table has no version")
		k.mu.Unlock()
		returnsSoon(t, done, "after the update")
	})
	t.Run("ends when the update is over without a publish of its own", func(t *testing.T) {
		k, _ := dirKeeper(t)
		updateRuns(k)
		defer k.mu.Unlock()
		done := waitsForUpdate(k, context.Background(), orders, long)
		stillWaiting(t, done, "during the update")
		k.pub.mu.Lock()
		k.pub.updating = false // a follower that ended in a panic says so
		k.pub.markPublishedLocked()
		k.pub.mu.Unlock()
		returnsSoon(t, done, "after it ended")
	})
	t.Run("an update said to run that holds nothing does not wait", func(t *testing.T) {
		k, _ := dirKeeper(t)
		updateRuns(k)
		k.mu.Unlock()
		returnsSoon(t, waitsForUpdate(k, context.Background(), orders, long), "nothing held")
	})
	t.Run("ends when the update publishes", func(t *testing.T) {
		k, _ := dirKeeper(t)
		updateRuns(k)
		defer k.mu.Unlock()
		done := waitsForUpdate(k, context.Background(), orders, long)
		stillWaiting(t, done, "before the publish")
		publishTail(k, orders[0])
		returnsSoon(t, done, "after the publish")
	})
	t.Run("waits for the running update, not for the next one", func(t *testing.T) {
		k, _ := dirKeeper(t)
		updateRuns(k)
		defer k.mu.Unlock()
		done := waitsForUpdate(k, context.Background(), orders, long)
		stillWaiting(t, done, "during the update")
		k.pub.mu.Lock()
		// It published without this table, and the next one started.
		k.pub.pubGen = k.pub.gen
		k.pub.gen++
		k.pub.markPublishedLocked()
		k.pub.mu.Unlock()
		returnsSoon(t, done, "after the update it waited for")
	})
	t.Run("ends at its limit", func(t *testing.T) {
		k, _ := dirKeeper(t)
		updateRuns(k)
		defer k.mu.Unlock()
		done := waitsForUpdate(k, context.Background(), orders, 300*time.Millisecond)
		stillWaiting(t, done, "before its limit")
		returnsSoon(t, done, "past its limit")
	})
	t.Run("ends with the statement", func(t *testing.T) {
		k, _ := dirKeeper(t)
		updateRuns(k)
		defer k.mu.Unlock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := waitsForUpdate(k, ctx, orders, long)
		stillWaiting(t, done, "before the client left")
		cancel()
		returnsSoon(t, done, "after the client left")
	})
	t.Run("ends with the keeper", func(t *testing.T) {
		k, _ := dirKeeper(t)
		updateRuns(k)
		defer k.mu.Unlock()
		done := waitsForUpdate(k, context.Background(), orders, long)
		stillWaiting(t, done, "before the keeper closed")
		k.stop()
		returnsSoon(t, done, "after the keeper closed")
	})
}

// With the wait for the source on, a statement over a table with a version
// waits for the running update first: its 200 ms must not run out during it.
func TestWaitUpdateOver_evenWithAVersion(t *testing.T) {
	r := newWaitRig(t, 200*time.Millisecond)
	r.publish(t)
	updateRuns(r.k)
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.k.waitUpdateOver(context.Background(), r.read, time.Minute, true)
	}()
	stillWaiting(t, done, "the statement that waits for the source")
	r.k.pub.mu.Lock()
	r.k.pub.updating = false
	r.k.pub.markPublishedLocked()
	r.k.pub.mu.Unlock()
	returnsSoon(t, done, "after the update")
	r.k.mu.Unlock()

	// And at its limit, like any other.
	updateRuns(r.k)
	defer r.k.mu.Unlock()
	capped := make(chan struct{})
	go func() {
		defer close(capped)
		r.k.waitUpdateOver(context.Background(), r.read, 200*time.Millisecond, true)
	}()
	returnsSoon(t, capped, "past its limit")
}

// freshWaitWillAct is true exactly where waitForSource would wait.
func TestFreshWaitWillAct(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(r *waitRig) (context.Context, []views.BaselineTable)
		want bool
	}{
		{"on, a MySQL source, tails published", func(r *waitRig) (context.Context, []views.BaselineTable) {
			return withTailsFor(context.Background(), r.id), r.read
		}, true},
		{"the switch off", func(r *waitRig) (context.Context, []views.BaselineTable) {
			r.s.freshWait = 0
			return withTailsFor(context.Background(), r.id), r.read
		}, false},
		{"no way to read the source's position", func(r *waitRig) (context.Context, []views.BaselineTable) {
			r.s.sourcePos = nil
			return withTailsFor(context.Background(), r.id), r.read
		}, false},
		{"no way to read capture's position", func(r *waitRig) (context.Context, []views.BaselineTable) {
			r.s.freshSavedHook = nil
			return withTailsFor(context.Background(), r.id), r.read
		}, false},
		{"a statement of no server", func(r *waitRig) (context.Context, []views.BaselineTable) {
			return context.Background(), r.read
		}, false},
		{"the boot server", func(r *waitRig) (context.Context, []views.BaselineTable) {
			return withTailsFor(context.Background(), bootServerID), r.read
		}, false},
		{"a server not in the registry", func(r *waitRig) (context.Context, []views.BaselineTable) {
			return withTailsFor(context.Background(), "nosuch"), r.read
		}, false},
		{"a PostgreSQL source", func(r *waitRig) (context.Context, []views.BaselineTable) {
			must(r.t, r.s.cm.reg.UpdateWith(r.id, func(e ServerEntry) (ServerEntry, bool) { e.Flavor = FlavorPostgres; return e, true }))
			return withTailsFor(context.Background(), r.id), r.read
		}, false},
		{"no source", func(r *waitRig) (context.Context, []views.BaselineTable) {
			must(r.t, r.s.cm.reg.UpdateWith(r.id, func(e ServerEntry) (ServerEntry, bool) { e.SourceDSN = ""; return e, true }))
			return withTailsFor(context.Background(), r.id), r.read
		}, false},
		{"no table", func(r *waitRig) (context.Context, []views.BaselineTable) {
			return withTailsFor(context.Background(), r.id), nil
		}, false},
		{"a table with no version", func(r *waitRig) (context.Context, []views.BaselineTable) {
			return withTailsFor(context.Background(), r.id), []views.BaselineTable{r.read[0], keeperTable("lines")}
		}, false},
		{"a newer cut of the table", func(r *waitRig) (context.Context, []views.BaselineTable) {
			return withTailsFor(context.Background(), r.id), []views.BaselineTable{newerCut(r.read[0])}
		}, false},
		{"a source that refused its position", func(r *waitRig) (context.Context, []views.BaselineTable) {
			w := r.s.freshWaits.of(r.id)
			w.mu.Lock()
			w.refusedUntil = time.Now().Add(time.Hour)
			w.mu.Unlock()
			return withTailsFor(context.Background(), r.id), r.read
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newWaitRig(t, 200*time.Millisecond)
			r.t = t
			r.publish(t)
			ctx, read := tc.edit(r)
			if got := r.s.freshWaitWillAct(ctx, nil, r.k, read); got != tc.want {
				t.Errorf("%v, want %v", got, tc.want)
			}
		})
	}
}
