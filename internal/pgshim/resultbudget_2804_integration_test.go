//go:build integration

package pgshim

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A time-travel answer read from a real index is counted in the port's
// result budget (#2804) by its cells, given back once it is written, and
// refused while the port is full.
func TestPGShim_timeTravelAnswerIsCountedInTheResultBudget(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	hour := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{hour})
	snapTS := hour.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTS, "public", "users", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTS, "public", "users", "name", 2, "", "varchar", "YES")
	t0 := hour.Add(5 * time.Minute)
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, t0.Format("2006-01-02 15:04:05"), nil,
		"public", "users", uint8(event.EventInsert), "1", nil, nil, []byte(`{"id":1,"name":"alice"}`))

	b := NewResultBudget(1 << 20)
	addr := serveAddrWithDB(t, Config{
		IndexDB:      db,
		ShimConfig:   shim.Config{NoArchive: true, IndexDBName: dbName},
		Auth:         testAuth(t),
		ResultBudget: b,
	})
	conn, err := connectPGWire(t, addr, testUser, testPass)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	after := t0.Add(time.Minute).Format("2006-01-02 15:04:05")
	before := t0.Add(-time.Minute).Format("2006-01-02 15:04:05")
	if name := queryName(t, ctx, conn, "_flashback", after, "1"); name != "alice" {
		t.Fatalf("name = %q, want alice", name)
	}
	waitBudget(t, "the answer to be given back once it was written", func() bool { return b.Held() == 0 })
	// "1" and "alice".
	if b.Charged() != 6 {
		t.Fatalf("charged %d bytes for the row, want the 6 of its cells: a time-travel answer is counted", b.Charged())
	}

	// The row did not exist yet: an answer with no row counts nothing.
	rows, err := conn.Query(ctx, "SELECT * FROM _flashback.users AS OF '"+before+"' WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if rows.Err() != nil || n != 0 {
		t.Fatalf("before the row existed: %d rows, %v", n, rows.Err())
	}
	if b.Charged() != 6 {
		t.Fatalf("charged %d after an answer with no row, want 6 still", b.Charged())
	}

	// The port full: refused, not answered, and nothing counted.
	b.held.Store(b.Max())
	_, err = conn.Exec(ctx, "SELECT * FROM _flashback.users AS OF '"+after+"' WHERE id = 1")
	if pe := requirePgError(t, err); pe.Code != "53000" || !contains(pe.Message, "still sending") {
		t.Fatalf("on a full port: %s %q, want 53000", pe.Code, pe.Message)
	}
	if b.Charged() != 6 || b.Held() != b.Max() {
		t.Fatalf("a refused answer changed the count: charged %d, held %d", b.Charged(), b.Held())
	}
	b.held.Store(0)
	if name := queryName(t, ctx, conn, "_snapshot", after, "1"); name != "alice" {
		t.Fatalf("with room again, name = %q, want alice", name)
	}
	waitBudget(t, "the answer to be given back", func() bool { return b.Held() == 0 })
	if b.Charged() != 12 {
		t.Fatalf("charged %d, want 12: the two rows that were sent", b.Charged())
	}
}
