package console

import (
	"context"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"sync"
	"testing"
	"time"
)

func TestCopyAges(t *testing.T) {
	base := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	at := func(min int) time.Time { return base.Add(time.Duration(min) * time.Minute) }
	snap := func(dirs ...string) map[string][]string {
		out := map[string][]string{}
		for _, d := range dirs {
			out[d] = []string{d + "/t.parquet"}
		}
		return out
	}
	copyAt := func(ran time.Time, files map[string][]string) copyAnswer {
		return copyAnswer{side: "copy", files: files, ok: true, ranAt: ran}
	}

	t.Run("no answer from the copy: nothing", func(t *testing.T) {
		var c copyAges
		c.note("s", copyAnswer{side: "mysql"})
		if n := c.now("s"); n.known || n.anyMax {
			t.Fatalf("%+v", n)
		}
	})

	t.Run("an answer over tails knows its instant at once", func(t *testing.T) {
		var c copyAges
		c.note("s", copyAnswer{side: "copy", ranAt: at(10), tailThrough: at(9), allTailed: true})
		n := c.now("s")
		if !n.known || !n.asOf.Equal(at(9)) || n.age != time.Minute || n.worst != time.Minute {
			t.Fatalf("%+v", n)
		}
		if len(c.pendingFor()) != 0 {
			t.Error("an answer over tails waits on the index")
		}
	})

	t.Run("an answer over snapshots waits for the index, once per snapshot", func(t *testing.T) {
		var c copyAges
		c.note("s", copyAt(at(10), snap("/snap/1")))
		if n := c.now("s"); n.known || n.anyMax {
			t.Fatalf("known before the index was asked: %+v", n)
		}
		c.note("s", copyAt(at(20), snap("/snap/1"))) // the same snapshot, later: the worst
		p := c.pendingFor()
		if len(p["s"]) != 1 {
			t.Fatalf("pending %v", p)
		}
		for key := range p["s"] {
			c.settle("s", key, at(0), "")
		}
		n := c.now("s")
		if !n.known || !n.asOf.Equal(at(0)) || n.age != 20*time.Minute || n.worst != 20*time.Minute {
			t.Fatalf("%+v", n)
		}
		c.note("s", copyAt(at(25), snap("/snap/1")))
		if len(c.pendingFor()) != 0 {
			t.Error("a snapshot already asked is asked again")
		}
		if n := c.now("s"); !n.known || n.age != 25*time.Minute || n.worst != 25*time.Minute {
			t.Errorf("%+v", n)
		}
	})

	t.Run("an answer over two snapshots is as current as the older; its tails count too", func(t *testing.T) {
		var c copyAges
		a := copyAt(at(10), snap("/snap/2", "/snap/1"))
		a.tailThrough = at(8)
		c.note("s", a)
		for key := range c.pendingFor()["s"] {
			c.settle("s", key, at(9), "") // the older of the two directories, as copyIndexedThrough gives
		}
		if n := c.now("s"); !n.asOf.Equal(at(8)) || n.age != 2*time.Minute || n.worst != 2*time.Minute {
			t.Fatalf("%+v", n)
		}
		if copyAgesKey(snap("/a", "/b")) != copyAgesKey(snap("/b", "/a")) {
			t.Error("the same snapshots named in another order are another key")
		}
	})

	t.Run("an instant the index cannot give: unknown, and no worst", func(t *testing.T) {
		var c copyAges
		c.note("s", copyAnswer{side: "copy", ranAt: at(1), tailThrough: at(0), allTailed: true})
		c.note("s", copyAt(at(30), snap("/snap/1")))
		for key := range c.pendingFor()["s"] {
			c.settle("s", key, time.Time{}, "the index no longer holds the event")
		}
		n := c.now("s")
		if n.known || !n.anyMax || n.worst != time.Minute {
			t.Fatalf("an unknown answer shown, or counted toward the worst: %+v", n)
		}
		if n.unknown != 1 || n.unknownWhy != copyAgeWhyNotPlaced {
			t.Errorf("the unknown answer is not counted, with its reason: %+v", n)
		}
	})

	t.Run("files not known: unknown at once", func(t *testing.T) {
		var c copyAges
		c.note("s", copyAnswer{side: "copy", ranAt: at(1), ok: false})
		if n := c.now("s"); n.known || len(c.pendingFor()) != 0 || n.unknown != 1 {
			t.Fatalf("%+v", n)
		}
	})

	t.Run("a newer answer replaces a pending last; the pending one still counts toward the worst", func(t *testing.T) {
		var c copyAges
		c.note("s", copyAt(at(40), snap("/snap/1")))
		c.note("s", copyAnswer{side: "copy", ranAt: at(41), tailThrough: at(41), allTailed: true})
		for key := range c.pendingFor()["s"] {
			c.settle("s", key, at(0), "")
		}
		n := c.now("s")
		if !n.known || n.age != 0 || n.worst != 40*time.Minute {
			t.Fatalf("%+v", n)
		}
	})

	t.Run("an answer through the moving current link is unknown, never a number", func(t *testing.T) {
		var c copyAges
		c.note("s", copyAt(at(10), snap("/snap/current")))
		n := c.now("s")
		if n.known || n.pending || n.unknown != 1 || n.unknownWhy != copyAgeWhyMoving || len(c.pendingFor()) != 0 {
			t.Fatalf("%+v", n)
		}
	})

	t.Run("a waiting answer is said to wait, without an older answer's reason", func(t *testing.T) {
		var c copyAges
		c.note("s", copyAnswer{side: "copy", ranAt: at(1), ok: false}) // unknown, with a reason
		c.note("s", copyAt(at(2), snap("/snap/1")))
		n := c.now("s")
		if n.known || !n.pending || n.unknownWhy != "" {
			t.Fatalf("%+v", n)
		}
	})

	t.Run("every answer waiting on a set the index cannot place is counted", func(t *testing.T) {
		var c copyAges
		for i := range 5 {
			c.note("s", copyAt(at(i), snap("/snap/1")))
		}
		for key := range c.pendingFor()["s"] {
			c.settle("s", key, time.Time{}, "gone")
		}
		if n := c.now("s"); n.unknown != 5 || n.pending || n.unknownWhy != copyAgeWhyNotPlaced {
			t.Fatalf("%+v", n)
		}
	})

	t.Run("a source clock ahead of the router's: age zero, not negative", func(t *testing.T) {
		var c copyAges
		c.note("s", copyAnswer{side: "copy", ranAt: at(0), tailThrough: at(1), allTailed: true})
		if n := c.now("s"); n.age != 0 || n.worst != 0 {
			t.Fatalf("%+v", n)
		}
	})

	t.Run("servers apart", func(t *testing.T) {
		var c copyAges
		c.note("a", copyAnswer{side: "copy", ranAt: at(5), tailThrough: at(0), allTailed: true})
		if n := c.now("b"); n.known || n.anyMax {
			t.Fatalf("%+v", n)
		}
	})

	t.Run("pending sets are bounded when nothing resolves them", func(t *testing.T) {
		var c copyAges
		for i := range copyAgesCutKeep + 5 {
			c.note("s", copyAt(at(i), snap(time.Duration(i).String())))
		}
		if got := len(c.pendingFor()["s"]); got != copyAgesCutKeep {
			t.Fatalf("%d pending", got)
		}
		if n := c.now("s"); n.known || n.unknown != 5 {
			t.Errorf("an answer left out of the pending shown as known, or not counted: %+v", n)
		}
	})

	t.Run("concurrent answers and readings", func(t *testing.T) {
		var c copyAges
		var wg sync.WaitGroup
		for i := range 50 {
			wg.Add(2)
			go func() { defer wg.Done(); c.note("s", copyAt(at(i), snap("/snap/1"))) }()
			go func() {
				defer wg.Done()
				for key := range c.pendingFor()["s"] {
					c.settle("s", key, at(0), "")
				}
				c.now("s")
			}()
		}
		wg.Wait()
	})
}

// The router's document carries the three numbers per server.
func TestRouterNumbersCopyAges(t *testing.T) {
	s := &Server{routing: newRoutingStats(time.Now(), nil)}
	s.routing.record("s", "copy", "")
	if n := s.RouterNumbers().Servers["s"]; n.DataAsOf != "" || n.DataAgeSeconds != nil || n.DataAgeMaxSeconds != nil {
		t.Fatalf("numbers before any is known: %+v", n)
	}
	through := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	s.copyAges.note("s", copyAnswer{side: "copy", ranAt: through.Add(90 * time.Second), tailThrough: through, allTailed: true})
	n := s.RouterNumbers().Servers["s"]
	if n.DataAsOf != "2026-10-14T12:00:00Z" || n.DataAgeSeconds == nil || *n.DataAgeSeconds != 90 || n.DataAgeMaxSeconds == nil || *n.DataAgeMaxSeconds != 90 {
		t.Fatalf("%+v", n)
	}
}

// NoteRoute is what records a copy answer for the server.
func TestNoteRouteRecordsCopyAge(t *testing.T) {
	s := &Server{}
	q := &SQLOnCopy{s: s, id: "s"}
	through := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	q.noteCopyAnswer(nil, true, through.Add(time.Minute), through, true)
	q.NoteRoute("mysql")
	if n := s.copyAges.now("s"); n.known {
		t.Fatal("a run MySQL answered counted for the copy")
	}
	q.noteCopyAnswer(nil, true, through.Add(time.Minute), through, true)
	q.NoteRoute("copy")
	if n := s.copyAges.now("s"); !n.known || n.age != time.Minute {
		t.Fatalf("%+v", n)
	}
}

// The lag is rounded up in the document: never said smaller than it was.
func TestRouterNumbersRoundsLagUp(t *testing.T) {
	s := &Server{routing: newRoutingStats(time.Now(), nil)}
	s.routing.record("s", "copy", "")
	through := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	s.copyAges.note("s", copyAnswer{side: "copy", ranAt: through.Add(3900 * time.Millisecond), tailThrough: through, allTailed: true})
	s.copyAges.note("s", copyAnswer{side: "copy", ok: false, ranAt: through})
	n := s.RouterNumbers().Servers["s"]
	if n.DataAgeMaxSeconds == nil || *n.DataAgeMaxSeconds != 4 || n.DataAgeUnknown != 1 || n.DataAgeUnknownReason == "" || n.DataAsOf != "" {
		t.Fatalf("%+v", n)
	}
}

// One resolve at a time: a second call while one runs returns at once.
func TestResolveCopyAgesOneAtATime(t *testing.T) {
	s := &Server{}
	s.copyAges.resolving.Lock()
	done := make(chan struct{})
	go func() { s.ResolveCopyAges(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a resolve waited on another")
	}
	s.copyAges.resolving.Unlock()
}

func TestCopyAgesCurrentLinkIsTheBaselines(t *testing.T) {
	if copyAgesCurrentLink != baseline.CurrentLinkName {
		t.Fatalf("%q != %q", copyAgesCurrentLink, baseline.CurrentLinkName)
	}
}

// The reason in the status document is one of a fixed few: never an
// error's text, which can name an account or a host, nor a path.
func TestCopyAgesReasonIsNeverTheErrorText(t *testing.T) {
	var c copyAges
	files := map[string][]string{"/data/secret-dir/": {"/data/secret-dir/shop/t.parquet"}}
	for _, detail := range []string{
		copyAgeWhyNoIndex + ": Error 1045: Access denied for user 'capture'@'10.0.0.7'",
		"a file of the snapshot /data/secret-dir/ could not be read",
	} {
		c.note("s", copyAnswer{side: "copy", files: files, ok: true, ranAt: time.Now()})
		for key := range c.pendingFor()["s"] {
			c.settle("s", key, time.Time{}, detail)
		}
		why := c.now("s").unknownWhy
		if why != copyAgeWhyNoIndex && why != copyAgeWhyNotPlaced {
			t.Fatalf("a reason off the fixed list: %q", why)
		}
	}
}
