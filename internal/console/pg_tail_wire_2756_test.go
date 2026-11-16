package console

import (
	"database/sql"
	"testing"
)

// A PostgreSQL server's keeper reads through pgTailSource only in a process
// that asked for PostgreSQL tails (#2756); everywhere else, and for every
// other server, tailKeeperFor makes what it always made.
func TestTailKeeperForPostgres(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	captured := func(string) (func(string, string) bool, bool) { return nil, true }
	isPG := func(k *tailKeeper) bool { _, ok := k.src.(*pgTailSource); return ok }
	isMySQL := func(k *tailKeeper) bool { _, ok := k.src.(*indexTailSource); return ok }

	// The boot server's family is its bundle's.
	pg, my := &bundle{pgSource: true}, &bundle{}

	t.Run("not asked for: a PostgreSQL server gets the keeper it always got", func(t *testing.T) {
		s := &Server{tailCapturesHook: captured}
		if s.pgTailsFor(bootServerID, pg) {
			t.Fatal("PostgreSQL tails in a process that did not ask for them")
		}
		if k := s.tailKeeperFor(bootServerID, pg); k == nil || !isMySQL(k) {
			t.Fatalf("keeper %v, want the one over indexTailSource", k)
		}
	})

	EnablePostgresTails(true)
	t.Cleanup(func() { EnablePostgresTails(false) })

	t.Run("asked for: a PostgreSQL server's keeper reads through pgTailSource, and is kept", func(t *testing.T) {
		s := &Server{tailCapturesHook: captured}
		k := s.tailKeeperFor(bootServerID, pg)
		if k == nil || !isPG(k) {
			t.Fatalf("keeper %v, want the one over pgTailSource", k)
		}
		if s.tailKeeperFor(bootServerID, pg) != k {
			t.Fatal("a new keeper at every statement")
		}
		// A new bundle over the same index connection keeps it; one over
		// another connection does not.
		if s.tailKeeperFor(bootServerID, &bundle{pgSource: true}) != k {
			t.Error("a new bundle over the same index connection replaced the keeper")
		}
		other, err := sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/idx") // never dialed
		if err != nil {
			t.Fatal(err)
		}
		defer other.Close()
		if again := s.tailKeeperFor(bootServerID, &bundle{pgSource: true, db: other}); again == k || !isPG(again) {
			t.Error("the keeper over another index connection was kept, or is not a PostgreSQL one")
		}
	})

	t.Run("asked for: a MySQL server is not touched", func(t *testing.T) {
		s := &Server{tailCapturesHook: captured}
		if s.pgTailsFor(bootServerID, my) {
			t.Fatal("a MySQL server taken for a PostgreSQL one")
		}
		k := s.tailKeeperFor(bootServerID, my)
		if k == nil || !isMySQL(k) || s.tailKeeperFor(bootServerID, my) != k {
			t.Fatalf("keeper %v, want one over indexTailSource, kept", k)
		}
	})

	t.Run("asked for: capture not known, or the server in fast mode, gets none", func(t *testing.T) {
		s := &Server{tailCapturesHook: func(string) (func(string, string) bool, bool) { return nil, false }}
		if k := s.tailKeeperFor(bootServerID, pg); k != nil {
			t.Fatal("a keeper for a server whose capture is not known")
		}
		t.Setenv("BINTRAIL_SQL_TAILS", "off")
		s = &Server{tailCapturesHook: captured}
		if k := s.tailKeeperFor(bootServerID, pg); k != nil {
			t.Fatal("a keeper for a server in fast mode")
		}
	})

	t.Run("a server that changes family gets the other keeper", func(t *testing.T) {
		s := &Server{tailCapturesHook: captured}
		k := s.tailKeeperFor(bootServerID, my)
		if again := s.tailKeeperFor(bootServerID, pg); again == k || !isPG(again) {
			t.Fatal("the MySQL keeper was kept for a PostgreSQL server")
		}
	})
}
