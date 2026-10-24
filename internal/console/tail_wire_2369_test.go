package console

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// Tails are opt-in (the fresh mode): unset, off and anything unknown answer
// from the snapshot alone.
func TestTailsOff(t *testing.T) {
	for v, want := range map[string]bool{
		"": true, "off": true, "OFF": true, "false": true, "0": true, "no": true, "maybe": true, "onn": true,
		"on": false, "ON": false, " On ": false, "true": false, "1": false, "yes": false, "fresh": false, "FRESH": false,
	} {
		t.Setenv("BINTRAIL_SQL_TAILS", v)
		if got := tailsOff(); got != want {
			t.Errorf("BINTRAIL_SQL_TAILS=%q: off %v, want %v", v, got, want)
		}
		if _, read := TailsSetting(); read != strings.TrimSpace(v) {
			t.Errorf("BINTRAIL_SQL_TAILS=%q: read %q", v, read)
		}
	}
}

// A server whose capture filters are not known here gets no keeper: no
// index read is paid for a tail it could not trust.
func TestTailKeeperForUnknownCapture(t *testing.T) {
	s := &Server{}
	s.tailCapturesHook = func(string) (func(string, string) bool, bool) { return nil, false }
	if k := s.tailKeeperFor("srv", &bundle{}); k != nil {
		t.Fatal("a keeper for a server whose capture is not known")
	}
	s.tailCapturesHook = func(string) (func(string, string) bool, bool) { return nil, true }
	b := &bundle{}
	k := s.tailKeeperFor("srv", b)
	if k == nil || s.tailKeeperFor("srv", b) != k {
		t.Fatal("no keeper, or a new one at every statement")
	}
	if s.tailKeeperFor("srv", &bundle{}) == k {
		t.Error("the keeper over a replaced bundle was kept")
	}
}

// All or nothing: a statement reads every table at one moment.
func TestStatementTailsAllOrNothing(t *testing.T) {
	ctx := context.Background()
	orders, customers := keeperTable("orders"), keeperTable("customers")
	s := &Server{}
	newKeeper := func(f *fakeIndex) *tailKeeper {
		return newTailKeeper(f, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
	}
	tables := []views.BaselineTable{orders, customers}
	// follow is the follower's update after the builds an earlier one
	// started are in, then another that publishes them.
	follow := func(k *tailKeeper) {
		k.followOnce(ctx, time.Minute)
		k.settle()
		k.followOnce(ctx, time.Minute)
	}
	// stmt is a statement after the follower published what an earlier one
	// asked for.
	stmt := func(k *tailKeeper, read []views.BaselineTable) ([]views.BaselineTable, time.Time, bool) {
		s.statementTails(ctx, k, tables, read)
		follow(k)
		return s.statementTails(ctx, k, tables, read)
	}

	t.Run("every table read has a tail: all of them laid over", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		f.add("customers", "9")
		out, through, ok := stmt(newKeeper(f), tables)
		if !ok || out[0].Tail == nil || out[1].Tail == nil || !through.Equal(f.at(2)) {
			t.Fatalf("%v %v %+v", ok, through, out)
		}
		if tables[0].Tail != nil {
			t.Error("the tables passed in were changed")
		}
	})

	t.Run("one table without a tail: none", func(t *testing.T) {
		f := &fakeIndex{startWhy: map[string]string{tailKey("shop", "customers"): "no key"}}
		f.add("orders", "1")
		out, through, ok := stmt(newKeeper(f), tables)
		if ok || !through.IsZero() || out[0].Tail != nil {
			t.Fatalf("%v %v %+v", ok, through, out)
		}
		if _, _, ok := stmt(newKeeper(f), tables[1:]); ok {
			t.Error("a statement over the one table without a tail said it read tails")
		}
	})

	t.Run("tails at two instants (one frozen): none", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		f.add("customers", "9")
		k := newKeeper(f)
		stmt(k, tables)
		for _, key := range []string{"2", "3", "4"} {
			f.add("orders", key) // over the budget of 3: frozen
		}
		f.add("customers", "8")
		out, _, ok := stmt(k, tables)
		if ok || out[0].Tail != nil || out[1].Tail != nil {
			t.Fatalf("a frozen tail read with a current one: %v %+v", ok, out)
		}
		if _, _, ok := stmt(k, tables[1:]); !ok {
			t.Error("a statement over the current table alone has no tail")
		}
	})

	t.Run("one table ready, one still building: none, and no wait", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		f.add("customers", "9")
		k := newKeeper(f)
		stmt(k, tables[:1]) // orders is in
		gate := make(chan struct{})
		f.fillGate = map[string]chan struct{}{tailKey("shop", "customers"): gate}
		s.statementTails(ctx, k, tables, tables)
		k.followOnce(ctx, time.Minute) // customers' build starts and waits on the gate
		out, through, ok := s.statementTails(ctx, k, tables, tables)
		if ok || !through.IsZero() || out[0].Tail != nil || out[1].Tail != nil {
			t.Fatalf("a statement read one table at the index's instant and one at its snapshot's: %v %v %+v", ok, through, out)
		}
		close(gate)
		follow(k)
		if out, _, ok := s.statementTails(ctx, k, tables, tables); !ok || out[0].Tail == nil || out[1].Tail == nil {
			t.Fatalf("both tails, once the build is in: %v %+v", ok, out)
		}
	})

	t.Run("no table read: none", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		if _, _, ok := stmt(newKeeper(f), nil); ok {
			t.Fatal("tails for a statement that reads no table")
		}
	})
}

func TestTailsSayTheInstant(t *testing.T) {
	for _, c := range []struct {
		stmt         string
		tailed, want bool
	}{
		{"SELECT * FROM shop.orders", true, true},
		{"SELECT * FROM shop.orders", false, false},
		{"SELECT * FROM shop.orders, events", true, false},
		{"SELECT * FROM shop.orders WHERE note = 'events'", true, true},
	} {
		if got := tailsSayTheInstant(c.stmt, c.tailed); got != c.want {
			t.Errorf("%q tailed %v: %v, want %v", c.stmt, c.tailed, got, c.want)
		}
	}
}

// Failures that leave every statement without tails are said once per reason.
func TestTailKeeperNote(t *testing.T) {
	k := newTailKeeper(&fakeIndex{}, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
	k.note(errors.New("denied"))
	if k.failing != "denied" {
		t.Fatalf("failing %q", k.failing)
	}
	k.note(errors.New("denied"))
	k.note(nil)
	if k.failing != "" {
		t.Fatalf("still failing %q after a read", k.failing)
	}
}

// An earlier process's tails directories are removed; a running one's, and
// anything else, stay.
func TestSweepTailDirs(t *testing.T) {
	parent := t.TempDir()
	old, recent, other := filepath.Join(parent, "bintrail-tails-1"), filepath.Join(parent, "bintrail-tails-2"), filepath.Join(parent, "keep-me")
	for _, d := range []string{old, recent, other} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	long := time.Now().Add(-2 * time.Hour)
	for _, d := range []string{old, other} {
		if err := os.Chtimes(d, long, long); err != nil {
			t.Fatal(err)
		}
	}
	sweepTailDirs(parent, time.Now().Add(-time.Hour))
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Error("an earlier process's directory stayed")
	}
	for _, d := range []string{recent, other} {
		if _, err := os.Stat(d); err != nil {
			t.Errorf("%s removed", d)
		}
	}
}
