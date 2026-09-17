//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The resume-dedup floor (#1690) exists to make the pre-capture DELETE a
// primary-key range instead of a full scan of binlog_events. It is safe only
// while it stays a LOWER BOUND on the event_id of every row that delete must
// remove. A floor one row too high does not slow anything down — it leaves
// rows behind that the replay then inserts a second time, which is the damage
// #1117 did with a wrong predicate on this same statement.
//
// So these tests never assert the floor's VALUE. They drive the real
// streamLoop, take the checkpoint it actually persisted, and ask the database
// the only two questions that matter: is the floor at or below the lowest row
// the unbounded delete would remove, and does the bounded delete remove
// exactly the same rows.

// floorEvent builds one synthetic row event. stmtEnd marks the ROWS_EVENT
// carrying STMT_END_F, which is what advances the position-mode checkpoint.
func floorEvent(n int, gtid string, stmtEnd bool) parser.Event {
	return parser.Event{
		BinlogFile: "binlog.000007",
		StartPos:   uint64(n * 100),
		EndPos:     uint64((n + 1) * 100),
		Timestamp:  time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC),
		ReadAt:     time.Now().UTC(),
		GTID:       gtid,
		Schema:     "testdb",
		Table:      "orders",
		EventType:  parser.EventInsert,
		StmtEnd:    stmtEnd,
		PKValues:   strconv.Itoa(n),
		RowAfter:   map[string]any{"id": int64(n), "amount": 1.5},
	}
}

func floorCommit(n int, gtid string) parser.Event {
	ev := floorEvent(n, gtid, true)
	ev.EventType = parser.EventCommit
	return ev
}

// floorIndex stands up an index with a snapshot for testdb.orders and a small
// batch size, so batches flush MID-stream rather than only at the close. A
// floor is the first id of the last flushed batch; with everything flushed at
// once at the end there is nothing to bound and the traps below cannot appear.
func floorIndex(t *testing.T, batchSize int) (*indexer.Indexer, *sql.DB) {
	t.Helper()
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertSnapshot(t, db, 1, "2026-01-01 00:00:00", "testdb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-01-01 00:00:00", "testdb", "orders", "amount", 2, "", "decimal", "YES")
	return indexer.New(db, batchSize), db
}

// savedCheckpoint reads back what streamLoop persisted.
func savedCheckpoint(t *testing.T, db *sql.DB) *streamState {
	t.Helper()
	st, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if st == nil {
		t.Fatal("streamLoop persisted no checkpoint")
	}
	return st
}

// lowestRowTheDeleteMustRemove returns MIN(event_id) over the rows today's
// UNBOUNDED position predicate matches, and whether there are any. This is the
// ceiling the floor may not cross.
func lowestRowTheDeleteMustRemove(t *testing.T, db *sql.DB, file string, pos uint64) (int64, bool) {
	t.Helper()
	var id sql.NullInt64
	err := db.QueryRow(`
		SELECT MIN(event_id) FROM binlog_events
		WHERE (CHAR_LENGTH(binlog_file) > CHAR_LENGTH(?)
		    OR (CHAR_LENGTH(binlog_file) = CHAR_LENGTH(?) AND binlog_file > ?))
		    OR (binlog_file = ? AND start_pos >= ?)`, file, file, file, file, pos).Scan(&id)
	if err != nil {
		t.Fatalf("min target id: %v", err)
	}
	return id.Int64, id.Valid
}

// lowestStragglerRow is the same ceiling for the GTID straggler pass: the
// lowest row BELOW the checkpoint whose transaction the saved set does not
// cover. Computed in Go against the set, exactly as the pass itself decides.
func lowestStragglerRow(t *testing.T, db *sql.DB, file string, pos uint64, saved gomysql.GTIDSet) (int64, bool) {
	t.Helper()
	rows, err := db.Query(`SELECT event_id, gtid FROM binlog_events
		WHERE binlog_file = ? AND start_pos < ? AND gtid IS NOT NULL ORDER BY event_id`, file, pos)
	if err != nil {
		t.Fatalf("straggler scan: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var g string
		if err := rows.Scan(&id, &g); err != nil {
			t.Fatalf("scan: %v", err)
		}
		single, err := parseGTIDSetForFlavor(gomysql.MySQLFlavor, g)
		if err != nil || !saved.Contain(single) {
			return id, true
		}
	}
	return 0, false
}

// TestIntegrationDedupFloorStaysBelowEveryRowItMustNotHide drives the real
// streamLoop through the two shapes that break a naive floor, and checks the
// floor it persisted against the database.
func TestIntegrationDedupFloorStaysBelowEveryRowItMustNotHide(t *testing.T) {
	// Trap 1 — position mode. The persisted binlog_position is safePos, the
	// last STMT_END_F/commit/DDL boundary, and it TRAILS the last event read
	// (#775). Rows between the two are flushed by checkpoint()'s own flush, so
	// at the instant the checkpoint row is written they are already in the
	// table AND already match the delete. A floor taken at that instant — the
	// obvious implementation, MAX(event_id) as the checkpoint is saved — sits
	// above them, and they survive the next resume as duplicates.
	t.Run("position mode, rows already flushed past the persisted boundary", func(t *testing.T) {
		idx, db := floorIndex(t, 2)
		var evs []parser.Event
		for n := 1; n <= 9; n++ {
			evs = append(evs, floorEvent(n, "", n%3 == 0)) // a boundary every third event
		}
		// Two more AFTER the last boundary: these are the rows that must be
		// deleted and that the trap would hide.
		evs = append(evs, floorEvent(10, "", false), floorEvent(11, "", false))

		state := &streamState{mode: "position", serverID: 1}
		if err := streamLoop(t.Context(), feedClosed(evs), idx, db, time.Hour, state,
			observe.ForSource("floor-position"), nil); err != nil {
			t.Fatalf("streamLoop: %v", err)
		}

		saved := savedCheckpoint(t, db)
		if saved.dedupFloorID <= 0 {
			t.Fatal("no floor was persisted: the delete stays a full scan and this whole change is inert")
		}
		lowest, any := lowestRowTheDeleteMustRemove(t, db, saved.binlogFile, saved.binlogPos)
		if !any {
			t.Fatal("the fixture left nothing for the delete to remove; it cannot discriminate")
		}
		if saved.dedupFloorID > lowest {
			t.Fatalf("floor %d is ABOVE the lowest row the delete must remove (%d): those rows survive the resume and duplicate",
				saved.dedupFloorID, lowest)
		}
		assertBoundedDeleteMatchesUnbounded(t, db, saved.binlogFile, saved.binlogPos, saved.dedupFloorID, nil, "")
	})

	// Trap 2 — GTID mode. safePos also advances at STMT_END_F, which fires
	// INSIDE an open transaction. A floor keyed on that boundary would climb
	// past the open transaction's earlier statements — precisely the straggler
	// rows the GTID pass exists to find, since GTID replay restarts a
	// transaction from its beginning.
	t.Run("gtid mode, an open transaction spanning a flush", func(t *testing.T) {
		idx, db := floorIndex(t, 2)
		const g1 = "3f2a1c88-0000-0000-0000-000000000001:1"
		const g2 = "3f2a1c88-0000-0000-0000-000000000001:2"
		accGTID, err := parseGTIDSetForFlavor(gomysql.MySQLFlavor, "")
		if err != nil {
			t.Fatalf("empty gtid set: %v", err)
		}

		var evs []parser.Event
		// A committed transaction first, so the floor has somewhere to climb to.
		for n := 1; n <= 4; n++ {
			evs = append(evs, floorEvent(n, g1, n%2 == 0))
		}
		evs = append(evs, floorCommit(5, g1))
		// Then one that never commits. Its statements end (STMT_END_F) and its
		// rows flush, but the checkpoint's gtid_set will not contain it.
		for n := 6; n <= 11; n++ {
			evs = append(evs, floorEvent(n, g2, n%2 == 0))
		}

		state := &streamState{mode: "gtid", serverID: 1, accGTID: accGTID}
		if err := streamLoop(t.Context(), feedClosed(evs), idx, db, time.Hour, state,
			observe.ForSource("floor-gtid"), nil); err != nil {
			t.Fatalf("streamLoop: %v", err)
		}

		saved := savedCheckpoint(t, db)
		if saved.dedupFloorID <= 0 {
			t.Fatal("no floor was persisted in GTID mode")
		}
		savedSet, err := parseGTIDSetForFlavor(gomysql.MySQLFlavor, saved.gtidSet)
		if err != nil {
			t.Fatalf("parse saved set %q: %v", saved.gtidSet, err)
		}
		if savedSet.Contain(mustGTID(t, g2)) {
			t.Fatalf("the fixture's uncommitted transaction ended up in the saved set (%s); it cannot discriminate", saved.gtidSet)
		}
		lowest, any := lowestStragglerRow(t, db, saved.binlogFile, saved.binlogPos, savedSet)
		if !any {
			t.Fatal("the fixture produced no straggler rows; it cannot discriminate")
		}
		if saved.dedupFloorID > lowest {
			t.Fatalf("floor %d is ABOVE the lowest straggler (%d): the open transaction's early rows survive and duplicate on replay",
				saved.dedupFloorID, lowest)
		}
		assertBoundedDeleteMatchesUnbounded(t, db, saved.binlogFile, saved.binlogPos, saved.dedupFloorID, savedSet, gomysql.MySQLFlavor)
	})
}

// Trap 3 — the ORDERING inside advanceGTID. The floor advance sits AFTER the
// GTID set's Update, and that is not decoration: if the Update fails the set
// does NOT gain that transaction, so the checkpoint still regards it as
// uncommitted and its rows are stragglers the next resume must delete. A floor
// advanced before the Update would sit above them and hide them.
//
// A malformed GTID is the only way Update fails, and the parser never produces
// one — so this fixture feeds the loop something the parser cannot, which is
// exactly why the ordering needs a test rather than an argument.
func TestIntegrationDedupFloorIgnoresACommitTheGTIDSetRefused(t *testing.T) {
	idx, db := floorIndex(t, 2)
	const g1 = "3f2a1c88-0000-0000-0000-000000000001:1"
	const g2 = "3f2a1c88-0000-0000-0000-000000000001:2"
	accGTID, err := parseGTIDSetForFlavor(gomysql.MySQLFlavor, "")
	if err != nil {
		t.Fatalf("empty gtid set: %v", err)
	}

	var evs []parser.Event
	for n := 1; n <= 4; n++ {
		evs = append(evs, floorEvent(n, g1, n%2 == 0))
	}
	evs = append(evs, floorCommit(5, g1))
	for n := 6; n <= 9; n++ {
		evs = append(evs, floorEvent(n, g2, n%2 == 0))
	}
	// The commit the set will refuse. Its rows above still carry a valid g2,
	// so they remain identifiable stragglers rather than parse failures.
	evs = append(evs, floorCommit(10, "this-is-not-a-gtid"))
	evs = append(evs, floorEvent(11, g2, true))

	state := &streamState{mode: "gtid", serverID: 1, accGTID: accGTID}
	if err := streamLoop(t.Context(), feedClosed(evs), idx, db, time.Hour, state,
		observe.ForSource("floor-gtid-refused"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	saved := savedCheckpoint(t, db)
	savedSet, err := parseGTIDSetForFlavor(gomysql.MySQLFlavor, saved.gtidSet)
	if err != nil {
		t.Fatalf("parse saved set %q: %v", saved.gtidSet, err)
	}
	if savedSet.Contain(mustGTID(t, g2)) {
		t.Fatalf("the refused commit still landed in the saved set (%s); the fixture cannot discriminate", saved.gtidSet)
	}
	lowest, any := lowestStragglerRow(t, db, saved.binlogFile, saved.binlogPos, savedSet)
	if !any {
		t.Fatal("no stragglers after a refused commit; the fixture cannot discriminate")
	}
	if saved.dedupFloorID > lowest {
		t.Fatalf("floor %d is ABOVE the lowest straggler (%d): the floor advanced on a commit the GTID set refused, "+
			"so the transaction the checkpoint still calls uncommitted would survive the resume", saved.dedupFloorID, lowest)
	}
	assertBoundedDeleteMatchesUnbounded(t, db, saved.binlogFile, saved.binlogPos, saved.dedupFloorID, savedSet, gomysql.MySQLFlavor)
}

func mustGTID(t *testing.T, s string) gomysql.GTIDSet {
	t.Helper()
	g, err := parseGTIDSetForFlavor(gomysql.MySQLFlavor, s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return g
}

// assertBoundedDeleteMatchesUnbounded runs the delete twice over the same rows
// — once with the floor, once without — and compares what SURVIVES. This is
// the acceptance criterion of #1690: among rows at or beyond the checkpoint,
// the bounded delete must remove at least everything today's delete removes.
//
// Both runs happen inside a transaction that is rolled back, against a pool
// pinned to ONE connection so the rollback reaches the same session. Re-seeding
// instead would hand the second run different auto-increment ids and compare
// two different tables.
func assertBoundedDeleteMatchesUnbounded(t *testing.T, db *sql.DB, file string, pos uint64, floor int64, savedSet gomysql.GTIDSet, flavor string) {
	t.Helper()
	db.SetMaxOpenConns(1)
	saver := newTableSaver(t, db)

	run := func(f int64) []string {
		t.Helper()
		saver.restore(t)
		var err error
		if savedSet != nil {
			_, err = deleteEventsSinceCheckpointGTID(db, file, pos, savedSet, flavor, f)
		} else {
			_, err = deleteEventsSinceCheckpoint(db, file, pos, f)
		}
		if err != nil {
			t.Fatalf("delete with floor %d: %v", f, err)
		}
		// Keyed on the binlog coordinate, not event_id: identity here is the
		// EVENT, and the comparison must not depend on the surrogate key the
		// floor is built from.
		rows, qerr := db.Query(`SELECT binlog_file, start_pos, pk_values FROM binlog_events ORDER BY binlog_file, start_pos`)
		if qerr != nil {
			t.Fatalf("survivors: %v", qerr)
		}
		var out []string
		for rows.Next() {
			var bf, pk string
			var sp uint64
			if err := rows.Scan(&bf, &sp, &pk); err != nil {
				t.Fatalf("scan survivor: %v", err)
			}
			out = append(out, fmt.Sprintf("%s:%d/%s", bf, sp, pk))
		}
		rows.Close()
		return out
	}

	bounded, unbounded := run(floor), run(noDedupFloor)
	saver.restore(t)
	if strings.Join(bounded, ",") != strings.Join(unbounded, ",") {
		t.Fatalf("the floor changed which rows survive.\n bounded (floor=%d): %v\n unbounded:          %v",
			floor, bounded, unbounded)
	}
	// Two runs that both deleted NOTHING agree trivially. Without this the
	// comparison above would pass on a fixture with no work in it.
	if len(unbounded) == saver.want {
		t.Fatal("the delete removed nothing in either run; the fixture cannot tell a correct floor from a broken one")
	}
}

// TestIntegrationDedupFloorMakesTheDeleteARange is the acceptance criterion
// "EXPLAIN of the cleanup shows a bounded range, not a full scan", asserted
// against a real partitioned table rather than argued.
//
// The row count is large enough for the optimizer to have a real choice: on a
// table small enough to sit in a page it picks a scan regardless, and an
// EXPLAIN there would prove nothing about the 48 GB index this is for.
func TestIntegrationDedupFloorMakesTheDeleteARange(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	seedFloorScaleRows(t, db, 40000)

	const (
		file = "binlog.000007"
		pos  = uint64(3_900_000)
	)
	floor, any := lowestRowTheDeleteMustRemove(t, db, file, pos)
	if !any {
		t.Fatal("the scale fixture matched no rows; the EXPLAIN below would be vacuous")
	}

	unbounded := explainDelete(t, db, file, pos, noDedupFloor)
	bounded := explainDelete(t, db, file, pos, floor)
	t.Logf("unbounded: %s\nbounded:   %s", unbounded, bounded)

	if !strings.Contains(unbounded, "ALL") {
		t.Skipf("the unbounded delete is not a full scan on this server (%s); the comparison below has no baseline", unbounded)
	}
	if !strings.Contains(bounded, "range") || !strings.Contains(bounded, "PRIMARY") {
		t.Errorf("with a floor the delete is %q, want a range on PRIMARY — the floor is not reaching the optimizer", bounded)
	}
}

// explainDelete returns "type/key/rows" for the dedup DELETE, with and without
// the floor, built the same way deleteEventsSinceCheckpoint builds it.
func explainDelete(t *testing.T, db *sql.DB, file string, pos uint64, floor int64) string {
	t.Helper()
	where := `(CHAR_LENGTH(binlog_file) > CHAR_LENGTH(?)
	    OR (CHAR_LENGTH(binlog_file) = CHAR_LENGTH(?) AND binlog_file > ?))
	    OR (binlog_file = ? AND start_pos >= ?)`
	args := []any{file, file, file, file, pos}
	if floor > 0 {
		where = `event_id >= ? AND (` + where + `)`
		args = append([]any{floor}, args...)
	}
	rows, err := db.Query(`EXPLAIN DELETE FROM binlog_events WHERE `+where, args...)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	if !rows.Next() {
		t.Fatal("EXPLAIN returned no row")
	}
	cells := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range cells {
		ptrs[i] = &cells[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatalf("scan EXPLAIN: %v", err)
	}
	var out []string
	for i, c := range cols {
		switch c {
		case "type", "key", "rows":
			out = append(out, c+"="+cells[i].String)
		}
	}
	return strings.Join(out, " ")
}

// seedFloorScaleRows inserts n rows straight into binlog_events, bypassing the
// indexer: this fixture is about the optimizer's choice, not about capture.
func seedFloorScaleRows(t *testing.T, db *sql.DB, n int) {
	t.Helper()
	const chunk = 2000
	for start := 0; start < n; start += chunk {
		var b strings.Builder
		b.WriteString(`INSERT INTO binlog_events (binlog_file, start_pos, end_pos, event_timestamp, gtid, schema_name, table_name, event_type, pk_values, row_after) VALUES `)
		args := make([]any, 0, chunk*4)
		for i := start; i < start+chunk && i < n; i++ {
			if i > start {
				b.WriteString(",")
			}
			b.WriteString(`("binlog.000007", ?, ?, "2026-02-19 14:00:00", ?, "testdb", "orders", 1, ?, '{"id":1}')`)
			args = append(args, i*100, i*100+100, fmt.Sprintf("3f2a1c88-0000-0000-0000-000000000001:%d", i/4+1), strconv.Itoa(i))
		}
		if _, err := db.Exec(b.String(), args...); err != nil {
			t.Fatalf("seed rows: %v", err)
		}
	}
	if _, err := db.Exec("ANALYZE TABLE binlog_events"); err != nil {
		t.Fatalf("ANALYZE: %v", err)
	}
}

// binlogEventsColumns lists the columns that can be written back, read from
// information_schema at runtime so it cannot drift from the schema. Generated
// columns are excluded: pk_hash is STORED and MySQL refuses an explicit value.
func binlogEventsColumns(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events'
		  AND GENERATION_EXPRESSION = '' ORDER BY ORDINAL_POSITION`)
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatalf("scan column: %v", err)
		}
		out = append(out, "`"+c+"`")
	}
	if len(out) == 0 {
		t.Fatal("binlog_events reported no writable columns")
	}
	return out
}

// tableSaver runs a destructive operation repeatedly over the SAME rows, by
// copying the table aside and restoring it — event_ids included, which a
// re-seed would not preserve and which the floor is built from.
//
// It replaces a BEGIN/ROLLBACK pair that did not work and did not say so.
// database/sql hands `BEGIN` and the DELETE to the pool separately, the DELETE
// autocommits, and the ROLLBACK restores nothing — so the second measurement
// ran against an EMPTY table and reported a spectacular improvement that meant
// nothing. restore() verifies the row count every time rather than trusting
// the mechanism.
type tableSaver struct {
	db   *sql.DB
	cols string
	want int
}

func newTableSaver(t *testing.T, db *sql.DB) *tableSaver {
	t.Helper()
	cols := strings.Join(binlogEventsColumns(t, db), ",")
	if _, err := db.Exec("DROP TABLE IF EXISTS binlog_events_saved"); err != nil {
		t.Fatalf("drop save table: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE binlog_events_saved AS SELECT " + cols + " FROM binlog_events"); err != nil {
		t.Fatalf("create save table: %v", err)
	}
	s := &tableSaver{db: db, cols: cols}
	if err := db.QueryRow("SELECT COUNT(*) FROM binlog_events_saved").Scan(&s.want); err != nil {
		t.Fatalf("count saved: %v", err)
	}
	if s.want == 0 {
		t.Fatal("nothing was saved: the fixture is empty and every comparison below would be vacuous")
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS binlog_events_saved") })
	return s
}

func (s *tableSaver) restore(t *testing.T) {
	t.Helper()
	if _, err := s.db.Exec("DELETE FROM binlog_events"); err != nil {
		t.Fatalf("clear for restore: %v", err)
	}
	if _, err := s.db.Exec("INSERT INTO binlog_events (" + s.cols + ") SELECT " + s.cols + " FROM binlog_events_saved"); err != nil {
		t.Fatalf("restore: %v", err)
	}
	var got int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM binlog_events").Scan(&got); err != nil {
		t.Fatalf("count restored: %v", err)
	}
	if got != s.want {
		t.Fatalf("restored %d rows, want %d: the next measurement would not be over the same table", got, s.want)
	}
}

// rowsTouched reports how many rows the session read while fn ran, summed
// across every access path. Measured on the REAL function, not on a copy of
// its SQL.
//
// This exists because the equivalence test above CANNOT see this class of bug:
// if the floor never reaches the query, the bounded and unbounded deletes
// remove exactly the same rows and every assertion there passes — a completely
// inert floor would ship green. Safety and effectiveness need separate checks.
//
// All FOUR counters, not Handler_read_rnd_next alone. That was the first
// version and it measured nothing for the straggler pass: unbounded, that
// query walks idx_gtid, which is Handler_read_next, and the sequential-read
// counter stayed at 0 while the query touched 40,000 rows. A counter that
// reports zero for the worst pass reads exactly like a working bound.
func rowsTouched(t *testing.T, db *sql.DB, fn func()) int64 {
	t.Helper()
	// Callers pin the pool to one connection before their first BEGIN; these
	// counters are per-SESSION and would otherwise read a different one.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("FLUSH STATUS"); err != nil {
		t.Fatalf("FLUSH STATUS: %v", err)
	}
	fn()
	rows, err := db.Query(`SHOW SESSION STATUS WHERE Variable_name IN
		('Handler_read_rnd_next','Handler_read_next','Handler_read_key','Handler_read_first')`)
	if err != nil {
		t.Fatalf("read handler counters: %v", err)
	}
	defer rows.Close()
	var total int64
	for rows.Next() {
		var name string
		var value int64
		if err := rows.Scan(&name, &value); err != nil {
			t.Fatalf("scan counter: %v", err)
		}
		total += value
	}
	return total
}

// TestIntegrationDedupFloorActuallyBoundsTheQueries is the effectiveness half.
// #1690 is a performance issue: a floor that is perfectly safe and reaches no
// query fixes nothing, and reads exactly like a working one from the outside.
func TestIntegrationDedupFloorActuallyBoundsTheQueries(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	// BEFORE the first BEGIN, not inside the measuring helper. With a pool the
	// BEGIN and the DELETE land on different connections, the DELETE
	// autocommits, and the ROLLBACK restores nothing — which is exactly what
	// happened while this test was being written: the second, "bounded" run
	// measured an EMPTY table and reported a beautiful number that meant
	// nothing. countRows below is the guard that makes that visible instead of
	// flattering.
	db.SetMaxOpenConns(1)
	testutil.InitIndexTables(t, db)
	const rows = 20000
	seedFloorScaleRows(t, db, rows)

	const (
		file = "binlog.000007"
		pos  = uint64(1_980_000) // ~200 of the 20,000 rows are at or beyond it
	)
	floor, any := lowestRowTheDeleteMustRemove(t, db, file, pos)
	if !any {
		t.Fatal("the fixture matched no rows")
	}
	savedSet, err := parseGTIDSetForFlavor(gomysql.MySQLFlavor, "")
	if err != nil {
		t.Fatal(err)
	}

	saver := newTableSaver(t, db)

	t.Run("the position delete", func(t *testing.T) {
		saver.restore(t)
		unbounded := rowsTouched(t, db, func() {
			if _, err := deleteEventsSinceCheckpoint(db, file, pos, noDedupFloor); err != nil {
				t.Fatalf("unbounded delete: %v", err)
			}
		})
		saver.restore(t)
		bounded := rowsTouched(t, db, func() {
			if _, err := deleteEventsSinceCheckpoint(db, file, pos, floor); err != nil {
				t.Fatalf("bounded delete: %v", err)
			}
		})
		saver.restore(t)
		t.Logf("rows touched: unbounded=%d bounded=%d (%d rows in the table)", unbounded, bounded, rows)

		if unbounded < rows/2 {
			t.Skipf("the unbounded delete touched only %d rows: this server did not full-scan, so there is no baseline to improve on", unbounded)
		}
		// A ratio, not an absolute: the claim is "not a scan", and the honest
		// floor still touches the rows it deletes.
		if bounded > unbounded/4 {
			t.Errorf("with a floor the delete touched %d rows against %d unbounded: the floor is not reaching the statement",
				bounded, unbounded)
		}
	})

	t.Run("the gtid straggler scan", func(t *testing.T) {
		// The pass this one guards is the WORSE of the two: unbounded, DISTINCT
		// makes MySQL walk all of idx_gtid and fetch the clustered row behind
		// every entry. Measured in production at 18 minutes against 10 for the
		// full-table delete before it.
		saver.restore(t)
		unbounded := rowsTouched(t, db, func() {
			if _, err := deleteEventsSinceCheckpointGTID(db, file, pos, savedSet, gomysql.MySQLFlavor, noDedupFloor); err != nil {
				t.Fatalf("unbounded gtid pass: %v", err)
			}
		})
		saver.restore(t)
		bounded := rowsTouched(t, db, func() {
			if _, err := deleteEventsSinceCheckpointGTID(db, file, pos, savedSet, gomysql.MySQLFlavor, floor); err != nil {
				t.Fatalf("bounded gtid pass: %v", err)
			}
		})
		saver.restore(t)
		t.Logf("rows touched: unbounded=%d bounded=%d (%d rows in the table)", unbounded, bounded, rows)

		if unbounded < rows/2 {
			t.Skipf("the unbounded gtid pass touched only %d rows: no baseline to improve on", unbounded)
		}
		if bounded > unbounded/4 {
			t.Errorf("with a floor the gtid pass touched %d rows against %d unbounded: the floor is not reaching the straggler scan",
				bounded, unbounded)
		}
	})
}

// TestIntegrationOnePassesTheFloorToTheDedup closes the last hole the two tests
// above leave open: both of them call the delete functions DIRECTLY, so neither
// can tell whether One() actually hands them the checkpoint's floor. Replacing
// `saved.dedupFloorID` with a literal 0 at either call site leaves both of them
// green while shipping a floor that reaches nothing.
//
// The probe inverts the usual assertion. It seeds a checkpoint whose floor is
// absurdly HIGH — above every row in the index — and then requires the rows to
// SURVIVE. That is not behaviour anyone wants; it is the only observable that
// distinguishes "the floor arrived" from "the floor was dropped", because a
// correct floor and a missing one delete identical rows by construction. The
// daemon never produces a floor like this: streamState.dedupFloorID is only
// ever the first id of a batch already written.
func TestIntegrationOnePassesTheFloorToTheDedup(t *testing.T) {
	const (
		uuid           = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
		checkpointFile = "binlog.000042"
		checkpointPos  = 5000
		// Far above any event_id this fixture can reach.
		absurdFloor = int64(1) << 40
	)

	for _, tc := range []struct {
		name      string
		mode      string
		gtidSet   string
		rowGTID   *string
		wantAlive []string
	}{
		{
			name: "position mode", mode: "position",
			// Both rows are at or beyond the checkpoint, so the unbounded
			// delete removes both.
			wantAlive: []string{"at-checkpoint", "beyond-checkpoint"},
		},
		{
			name: "gtid mode", mode: "gtid", gtidSet: uuid + ":1-3",
			rowGTID:   ptr(uuid + ":10"),
			wantAlive: []string{"at-checkpoint", "beyond-checkpoint"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			indexDB, indexName := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, indexDB)
			sourceDB, sourceName := testutil.CreateTestDB(t)
			testutil.MustExec(t, sourceDB, `CREATE TABLE orders (id INT PRIMARY KEY, amount INT NOT NULL)`)

			if err := saveCheckpoint(indexDB, &streamState{
				mode:         tc.mode,
				binlogFile:   checkpointFile,
				binlogPos:    checkpointPos,
				gtidSet:      tc.gtidSet,
				flavor:       gomysql.MySQLFlavor,
				serverID:     99892,
				dedupFloorID: absurdFloor,
			}); err != nil {
				t.Fatalf("seed checkpoint: %v", err)
			}
			// Confirm the floor actually round-tripped through saveCheckpoint
			// and the column, or the probe below would pass for the wrong reason.
			if back := savedCheckpoint(t, indexDB); back.dedupFloorID != absurdFloor {
				t.Fatalf("the checkpoint did not persist the floor: got %d, want %d", back.dedupFloorID, absurdFloor)
			}

			const ts = "2026-02-19 14:00:00"
			testutil.InsertEvent(t, indexDB, checkpointFile, 5000, 6000, ts, tc.rowGTID, sourceName, "orders", 1, "at-checkpoint", nil, nil, []byte(`{"id":1}`))
			testutil.InsertEvent(t, indexDB, checkpointFile, 7000, 8000, ts, tc.rowGTID, sourceName, "orders", 1, "beyond-checkpoint", nil, nil, []byte(`{"id":2}`))

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			// One is expected to fail after the dedup (this source speaks
			// neither the saved GTID set nor that binlog file); the assertion
			// is on the database, so how it stops does not matter.
			_ = One(ctx, Config{
				IndexDSN:   testutil.IntegrationDSN(indexName),
				SourceDSN:  testutil.IntegrationDSN(sourceName),
				Flavor:     gomysql.MySQLFlavor,
				ServerID:   99893,
				BatchSize:  1,
				Schemas:    sourceName,
				Checkpoint: 1,
				GapTimeout: 30,
				Format:     "text",
				SSLMode:    "preferred",
				Deps:       testStreamDeps(),
			})

			alive := survivingPKs(t, indexDB, sourceName)
			assertPKs(t, alive, tc.wantAlive,
				"an unreachable floor must suppress the delete entirely; rows missing here mean One() dropped the floor "+
					"and ran the unbounded statement")

			// The floor must also reach the RUNNING state, not only the
			// delete. GTID mode gets as far as streamLoop here (this source
			// refuses the dump asynchronously, on the first GetEvent), so its
			// ticker writes a checkpoint during the run — and a checkpoint
			// written from a state that never inherited the floor persists 0,
			// throwing away on the first quiet tick exactly what this change
			// earned. Position mode stops before the loop, so it has nothing
			// to say here.
			if tc.mode == "gtid" {
				if got := savedCheckpoint(t, indexDB).dedupFloorID; got != absurdFloor {
					t.Errorf("after the run the persisted floor is %d, want %d: One did not carry it into the stream state, "+
						"so the first checkpoint of a quiet resume discards it", got, absurdFloor)
				}
			}
		})
	}
}

func ptr(s string) *string { return &s }

// TestIntegrationDedupFloorSurvivesAQuietResume pins the carry-across-restart.
// The checkpoint ticker fires even with zero events, so a daemon that resumes
// and sees no traffic checkpoints anyway. If the floor did not come along from
// the saved state, that first tick would overwrite a real floor with 0 and the
// next resume would be back to scanning the whole table — the outage this
// change removes, reintroduced by a quiet hour.
func TestIntegrationDedupFloorSurvivesAQuietResume(t *testing.T) {
	idx, db := floorIndex(t, 10)
	const earned = int64(4242)

	if err := saveCheckpoint(db, &streamState{
		mode: "position", binlogFile: "binlog.000007", binlogPos: 900,
		flavor: gomysql.MySQLFlavor, serverID: 1, dedupFloorID: earned,
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	// ONE event, and it is a boundary. With a batch size of 10 nothing has
	// flushed by the time the boundary fires, so noteDedupFloor is called with
	// the indexer's zero — the exact call the monotonicity guard exists for.
	// Without that guard the earned floor is overwritten with 0 here and the
	// next resume scans the whole table again.
	state := &streamState{mode: "position", binlogFile: "binlog.000007", binlogPos: 900,
		safeFile: "binlog.000007", safePos: 900, flavor: gomysql.MySQLFlavor, serverID: 1,
		dedupFloorID: earned}
	if err := streamLoop(t.Context(), feedClosed([]parser.Event{floorEvent(20, "", true)}), idx, db, time.Hour, state,
		observe.ForSource("floor-quiet"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	if got := savedCheckpoint(t, db).dedupFloorID; got != earned {
		t.Fatalf("a resume that indexed nothing left the floor at %d, want %d kept", got, earned)
	}
}

// TestIntegrationDedupFloorLeavesTheRolloverGuardIntact is the one case in the
// #1690 edge-case table that costs DATA rather than time. After
// mysql-bin.999999 the server continues with mysql-bin.1000000, and a plain
// lexicographic comparison inverts ('999999' > '1000000'), so the pre-rollover
// row looks like it sits AFTER the checkpoint — #840 permanently destroyed it
// that way. The floor added here is a second conjunct on the SAME statement,
// so this pins that the two are independent: bounding the delete by event_id
// must not weaken the length-then-lexicographic comparison that keeps that row
// alive.
//
// It runs at BOTH ends of the floor's legal range, because they fail
// differently:
//
//   - The tightest legal floor (the lowest row the delete must remove) is the
//     realistic ceiling, and it is the leg that proves the straggler is still
//     deleted when the bound is as narrow as it is ever allowed to be.
//   - A floor at the OLDEST row is the leg that actually exercises the
//     rollover comparison. With the tight floor, the pre-rollover row sits
//     BELOW the bound and the floor would mask a naive `binlog_file > ?` — the
//     row would survive for the wrong reason and the test would pass over the
//     #840 bug. Here every row clears the floor, so the comparison is the only
//     thing standing between the pre-rollover row and deletion.
func TestIntegrationDedupFloorLeavesTheRolloverGuardIntact(t *testing.T) {
	const (
		checkpointFile = "mysql-bin.1000000"
		checkpointPos  = uint64(300)
	)
	for _, tc := range []struct {
		name  string
		floor func(t *testing.T, db *sql.DB, oldest int64) int64
	}{
		{"a floor below every row", func(_ *testing.T, _ *sql.DB, oldest int64) int64 {
			return oldest
		}},
		{"the tightest legal floor", func(t *testing.T, db *sql.DB, _ int64) int64 {
			id, ok := lowestRowTheDeleteMustRemove(t, db, checkpointFile, checkpointPos)
			if !ok {
				t.Fatal("fixture has nothing for the delete to remove")
			}
			return id
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)

			ts := "2026-02-19 14:00:00"
			testutil.InsertEvent(t, db, "mysql-bin.999999", 100, 200, ts, nil, "mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1}`)) // pre-rollover, pre-checkpoint (must survive)
			testutil.InsertEvent(t, db, checkpointFile, 100, 200, ts, nil, "mydb", "orders", 1, "2", nil, nil, []byte(`{"id":2}`))     // checkpoint file, below pos (must survive)
			testutil.InsertEvent(t, db, checkpointFile, 300, 400, ts, nil, "mydb", "orders", 1, "3", nil, nil, []byte(`{"id":3}`))     // at-or-beyond pos (the straggler; must go)

			var oldest int64
			if err := db.QueryRow(`SELECT MIN(event_id) FROM binlog_events`).Scan(&oldest); err != nil {
				t.Fatalf("oldest event_id: %v", err)
			}
			floor := tc.floor(t, db, oldest)
			if floor <= 0 {
				t.Fatalf("this leg computed floor=%d, which is no floor at all — it would exercise the unbounded path", floor)
			}

			n, err := deleteEventsSinceCheckpoint(db, checkpointFile, checkpointPos, floor)
			if err != nil {
				t.Fatalf("deleteEventsSinceCheckpoint: %v", err)
			}
			if n != 1 {
				t.Fatalf("expected exactly 1 row deleted (the straggler), got %d", n)
			}

			rows, err := db.Query("SELECT pk_values FROM binlog_events ORDER BY pk_values")
			if err != nil {
				t.Fatalf("query surviving rows: %v", err)
			}
			defer rows.Close()
			var survivors []string
			for rows.Next() {
				var pk string
				if err := rows.Scan(&pk); err != nil {
					t.Fatalf("scan pk_values: %v", err)
				}
				survivors = append(survivors, pk)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("iterate surviving rows: %v", err)
			}
			if strings.Join(survivors, ",") != "1,2" {
				t.Fatalf("surviving pks %v, want [1 2] — the pre-rollover row must outlive a bounded delete exactly as it outlives an unbounded one", survivors)
			}
		})
	}
}
