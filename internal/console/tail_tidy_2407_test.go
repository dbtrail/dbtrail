package console

import (
	"os"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// The router's housekeeping (#2407): a removed server's keeper and lag record
// go; a kept server's idle tails are forgotten without any statement.
func TestTidyTails(t *testing.T) {
	dir := t.TempDir()
	reg, err := LoadRegistry(dir + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: &stubMonitorCtrl{}})
	if err != nil {
		t.Fatal(err)
	}
	kept, err := reg.Add(ServerEntry{Name: "kept", DSN: "idx:pw@tcp(127.0.0.1:3306)/idx"})
	if err != nil {
		t.Fatal(err)
	}
	gone, err := reg.Add(ServerEntry{Name: "gone", DSN: "idx:pw@tcp(127.0.0.1:3306)/idx2"})
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeIndex{}
	f.add("orders", "1")
	clock := time.Date(2026, 10, 15, 1, 0, 0, 0, time.UTC)
	keeper := func() *tailKeeper {
		k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
		k.now = func() time.Time { return clock }
		return k
	}
	kk, kg := keeper(), keeper()
	if _, err := kk.refresh(t.Context(), []views.BaselineTable{keeperTable("orders")}); err != nil {
		t.Fatal(err)
	}
	kk.settle() // its tail is built in the background
	// A build of the removed server's keeper, in flight when it goes.
	gate := make(chan struct{})
	defer close(gate)
	f.fillGate = map[string]chan struct{}{tailKey("shop", "items"): gate}
	f.fillEntered = make(chan string, 1)
	if _, err := kg.refresh(t.Context(), []views.BaselineTable{keeperTable("items")}); err != nil {
		t.Fatal(err)
	}
	<-f.fillEntered
	if _, err := kg.ensureDir(); err != nil {
		t.Fatal(err)
	}
	srv.tails.by = map[string]*tailKeeper{kept.ID: kk, gone.ID: kg}
	srv.copyAges.note(gone.ID, copyAnswer{side: "copy", ranAt: clock, tailThrough: clock, allTailed: true})
	if err := reg.Delete(gone.ID); err != nil {
		t.Fatal(err)
	}

	srv.TidyTails()
	if _, ok := srv.tails.by[gone.ID]; ok {
		t.Error("a removed server kept its keeper")
	}
	kg.settle() // returns only because the removal ended the build in flight
	if len(kg.tables) != 0 || len(kg.builds) != 0 {
		t.Errorf("a build of a removed server's keeper was installed: %d tables, %d builds", len(kg.tables), len(kg.builds))
	}
	if n := srv.copyAges.now(gone.ID); n.known {
		t.Error("a removed server kept its lag record")
	}
	if _, err := os.Stat(kg.dir); err != nil {
		t.Error("a removed server's files went at once, before a statement could stop reading them")
	}
	if len(kk.tables) != 1 {
		t.Fatalf("a used tail forgotten: %d tables", len(kk.tables))
	}
	clock = clock.Add(2 * time.Hour) // past the keeper's idle time, with no statement
	srv.TidyTails()
	if srv.tails.by[kept.ID] != kk || len(kk.tables) != 0 {
		t.Errorf("an idle tail of a kept server was not forgotten: %d tables", len(kk.tables))
	}
}
