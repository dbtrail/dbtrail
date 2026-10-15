package console

import (
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
	})

	t.Run("files not known: unknown at once", func(t *testing.T) {
		var c copyAges
		c.note("s", copyAnswer{side: "copy", ranAt: at(1), ok: false})
		if n := c.now("s"); n.known || len(c.pendingFor()) != 0 {
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
		if n := c.now("s"); n.known {
			t.Errorf("an answer left out of the pending shown as known: %+v", n)
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
	s := &Server{routing: newRoutingStats(time.Now())}
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
