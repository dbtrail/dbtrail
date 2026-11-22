//go:build integration

package pgstreamrun_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"sort"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// resumeRig is one PostgreSQL table, publication and slot captured into one
// index, for the resume tests below.
type resumeRig struct {
	t     *testing.T
	ctx   context.Context
	pg    *pgx.Conn
	index *sql.DB
	cfg   pgstreamrun.Config
	slot  string
	tbl   string
}

func newResumeRig(t *testing.T, name string, batch int) *resumeRig {
	t.Helper()
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	indexDB, dbName := testutil.CreateTestDB(t)
	r := &resumeRig{
		t: t, ctx: ctx, index: indexDB,
		slot: "bintrail_resend_" + name, tbl: "resend_" + name,
	}
	pub := r.slot + "_pub"
	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	r.pg = pg
	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+r.tbl)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", r.slot)
	}
	dropAll()
	t.Cleanup(func() {
		dropAll()
		pg.Close(context.Background())
	})
	r.exec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", r.tbl))
	r.exec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", r.tbl))
	r.exec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, r.tbl))
	r.cfg = pgstreamrun.Config{
		IndexDSN: testutil.BaseDSN() + "/" + dbName, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
		SlotName: r.slot, Publication: pub, ServerID: 61,
		Tables: "public." + r.tbl, BatchSize: batch, Checkpoint: 200 * time.Millisecond,
	}
	return r
}

func (r *resumeRig) exec(stmt string) {
	r.t.Helper()
	if _, err := r.pg.Exec(r.ctx, stmt); err != nil {
		r.t.Fatalf("exec %q: %v", stmt, err)
	}
}

// insert commits one transaction inserting the given ids.
func (r *resumeRig) insert(ids ...int) {
	r.t.Helper()
	vals := make([]string, len(ids))
	for i, id := range ids {
		vals[i] = fmt.Sprintf("(%d, 'v%d')", id, id)
	}
	r.exec(fmt.Sprintf("INSERT INTO %s VALUES %s", r.tbl, strings.Join(vals, ", ")))
}

func (r *resumeRig) slotActive() bool {
	var active bool
	if err := r.pg.QueryRow(r.ctx, "SELECT active AND confirmed_flush_lsn IS NOT NULL FROM pg_replication_slots WHERE slot_name=$1", r.slot).Scan(&active); err != nil {
		return false
	}
	return active
}

// start runs a capture until the returned stop is called; stop waits for
// it to end cleanly and for PostgreSQL to release the slot.
func (r *resumeRig) start() (stop func()) {
	r.t.Helper()
	stop, _ = r.startRun()
	return stop
}

// startRun is start that also hands over the capture's result, for a run
// that ends on its own.
func (r *resumeRig) startRun() (stop func(), ended <-chan error) {
	r.t.Helper()
	runCtx, cancel := context.WithCancel(r.ctx)
	r.t.Cleanup(cancel)
	runErr := make(chan error, 1)
	go func() { runErr <- pgstreamrun.One(runCtx, r.cfg) }()
	waitFor(r.t, 20*time.Second, r.slotActive, "replication slot active")
	return func() {
		r.t.Helper()
		cancel()
		if err := <-runErr; err != nil {
			r.t.Fatalf("capture returned error: %v", err)
		}
		waitFor(r.t, 15*time.Second, func() bool { return !r.slotActive() }, "replication slot inactive")
	}, runErr
}

// saved is the position the index's checkpoint holds (0: none).
func (r *resumeRig) saved() uint64 {
	var pos sql.NullInt64
	_ = r.index.QueryRow("SELECT binlog_position FROM stream_state WHERE id = 1").Scan(&pos)
	return uint64(pos.Int64)
}

// rowLSN is the position of the newest row of id (0: none).
func (r *resumeRig) rowLSN(id int) uint64 {
	var pos sql.NullInt64
	_ = r.index.QueryRow("SELECT MAX(start_pos) FROM binlog_events WHERE table_name = ? AND pk_values = ?", r.tbl, fmt.Sprint(id)).Scan(&pos)
	return uint64(pos.Int64)
}

// waitSavedAt waits until id is indexed and the saved position is its
// transaction's, and returns that position.
func (r *resumeRig) waitSavedAt(id int) uint64 {
	r.t.Helper()
	var lsn uint64
	waitFor(r.t, 20*time.Second, func() bool {
		lsn = r.rowLSN(id)
		return lsn != 0 && r.saved() == lsn
	}, fmt.Sprintf("id %d indexed and the position saved at its transaction", id))
	return lsn
}

// copies is how many rows the index holds for each id, as "id:n" sorted.
func (r *resumeRig) copies() string {
	r.t.Helper()
	rs, err := r.index.Query("SELECT pk_values, COUNT(*) FROM binlog_events WHERE table_name = ? GROUP BY pk_values", r.tbl)
	if err != nil {
		r.t.Fatal(err)
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		var pk string
		var n int
		if err := rs.Scan(&pk, &n); err != nil {
			r.t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%s:%d", pk, n))
	}
	if err := rs.Err(); err != nil {
		r.t.Fatal(err)
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

// tableRows is how many rows the index holds for table, and the newest
// one's position.
func (r *resumeRig) tableRows(table string) (n int, pos uint64) {
	r.t.Helper()
	var p sql.NullInt64
	if err := r.index.QueryRow("SELECT COUNT(*), MAX(start_pos) FROM binlog_events WHERE table_name = ?", table).Scan(&n, &p); err != nil {
		r.t.Fatalf("count the rows of %s: %v", table, err)
	}
	return n, uint64(p.Int64)
}

// copiesOf is copies() for another table.
func (r *resumeRig) copiesOf(table string) string {
	r.t.Helper()
	saved := r.tbl
	r.tbl = table
	defer func() { r.tbl = saved }()
	return r.copies()
}

// addUncaptured creates a second table in the publication, outside the
// capture's filter, and returns its name.
func (r *resumeRig) addUncaptured() string {
	r.t.Helper()
	other := r.tbl + "_b"
	r.exec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", other))
	r.exec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", other))
	r.exec(fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", r.cfg.Publication, other))
	r.t.Cleanup(func() { _, _ = r.pg.Exec(context.Background(), "DROP TABLE IF EXISTS "+other) })
	return other
}

// txn commits the statements as one transaction.
func (r *resumeRig) txn(stmts ...string) {
	r.t.Helper()
	tx, err := r.pg.Begin(r.ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(r.ctx, stmt); err != nil {
			r.t.Fatalf("exec %q: %v", stmt, err)
		}
	}
	if err := tx.Commit(r.ctx); err != nil {
		r.t.Fatal(err)
	}
}

// syncBuffer is a log destination the capture writes while the test reads.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// exactCopies is copies() for table, its name compared exactly (the
// index's collation folds case).
func (r *resumeRig) exactCopies(table string) string {
	r.t.Helper()
	rs, err := r.index.Query("SELECT pk_values, COUNT(*) FROM binlog_events WHERE BINARY table_name = ? GROUP BY pk_values", table)
	if err != nil {
		r.t.Fatal(err)
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		var pk string
		var n int
		if err := rs.Scan(&pk, &n); err != nil {
			r.t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%s:%d", pk, n))
	}
	if err := rs.Err(); err != nil {
		r.t.Fatal(err)
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

// exactMarkers is how many TRUNCATE markers schema_changes holds for
// table, its name compared exactly, and the newest one's position.
func (r *resumeRig) exactMarkers(table string) (n int, pos uint64) {
	r.t.Helper()
	var p sql.NullInt64
	if err := r.index.QueryRow("SELECT COUNT(*), MAX(binlog_pos) FROM schema_changes WHERE BINARY table_name = ? AND ddl_type = 'TRUNCATE TABLE'", table).Scan(&n, &p); err != nil {
		r.t.Fatalf("count the TRUNCATE markers of %s: %v", table, err)
	}
	return n, uint64(p.Int64)
}

// addCaseTwin creates a table whose name differs from the rig's only in
// case ("Resend_..." beside resend_...), in the publication and outside
// the capture's filter, and returns its name.
func (r *resumeRig) addCaseTwin() string {
	r.t.Helper()
	twin := strings.ToUpper(r.tbl[:1]) + r.tbl[1:]
	q := `"` + twin + `"`
	r.exec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", q))
	r.exec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", q))
	r.exec(fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", r.cfg.Publication, q))
	r.t.Cleanup(func() { _, _ = r.pg.Exec(context.Background(), "DROP TABLE IF EXISTS "+q) })
	return twin
}

// once is the copies() an index holding each id exactly once shows.
func once(ids ...int) string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = fmt.Sprintf("%d:1", id)
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

// A resumed capture starts from the commit LSN of the last transaction it
// saved, and PostgreSQL sends that transaction again (it decodes every
// commit that starts at or after the start). The index already holds it
// whole, so it must not hold it twice afterwards; and a slot that does not
// send it again must not lose it.
func TestIntegrationPGResumeIndexesTheSavedTransactionOnce(t *testing.T) {
	// The reported sequence: a first run on a quiet source, a second that
	// indexes two transactions and stops cleanly, a third that resumes from
	// the saved position and indexes one more.
	t.Run("clean stop", func(t *testing.T) {
		r := newResumeRig(t, "clean", 100)
		r.start()()
		stop := r.start()
		r.insert(1)
		r.insert(2)
		at := r.waitSavedAt(2)
		stop()
		// What the filter relies on: the saved position is the commit LSN
		// every row of that transaction carries.
		if got := r.saved(); got != at {
			t.Fatalf("saved position %X after the stop, the transaction of id 2 is at %X", got, at)
		}
		stop = r.start()
		r.insert(3)
		r.waitSavedAt(3)
		stop()
		if got, want := r.copies(), once(1, 2, 3); got != want {
			t.Fatalf("the index holds %q; want %q", got, want)
		}
		// The table's shape, sent inside the resent transaction, is still
		// recorded: the rows after it are stamped with it, not with 0.
		var version int
		if err := r.index.QueryRow("SELECT schema_version FROM binlog_events WHERE table_name = ? AND pk_values = '3'", r.tbl).Scan(&version); err != nil || version == 0 {
			t.Fatalf("id 3 has schema_version %d (%v); want the snapshot recorded on the resume", version, err)
		}
		// And a resume on a quiet source sends it again too: still once.
		stop = r.start()
		r.insert(4)
		r.waitSavedAt(4)
		stop()
		if got, want := r.copies(), once(1, 2, 3, 4); got != want {
			t.Fatalf("after another resume the index holds %q; want %q", got, want)
		}
	})

	// Several transactions in one batch, and a last one with more rows than
	// the index batch size (written to the index in several INSERT
	// statements) at the saved position. Not a transaction split past the
	// writer's caps: the position is saved only after its commit anyway.
	t.Run("several transactions per batch and a large one at the position", func(t *testing.T) {
		r := newResumeRig(t, "large", 3)
		stop := r.start()
		for id := 1; id <= 5; id++ {
			r.insert(id)
		}
		big := make([]int, 0, 10)
		for id := 10; id < 20; id++ {
			big = append(big, id)
		}
		r.insert(big...)
		r.waitSavedAt(19)
		stop()
		stop = r.start()
		r.insert(30)
		r.waitSavedAt(30)
		stop()
		want := once(append(append([]int{1, 2, 3, 4, 5}, big...), 30)...)
		if got := r.copies(); got != want {
			t.Fatalf("the index holds %q; want %q", got, want)
		}
	})

	// A stop after the rows of two transactions were committed to the index
	// and before their position was saved, as a kill -9 between the two
	// leaves it: the save is refused by a trigger, the capture ends with an
	// error, nothing is acknowledged past the old position.
	t.Run("stop between committing rows and saving the position", func(t *testing.T) {
		r := newResumeRig(t, "dirty", 100)
		_, ended := r.startRun()
		r.insert(1)
		r.insert(2)
		at := r.waitSavedAt(2)
		testutil.MustExec(t, r.index, `CREATE TRIGGER refuse_position BEFORE UPDATE ON stream_state FOR EACH ROW
			BEGIN
				IF NEW.binlog_position <> OLD.binlog_position THEN
					SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test: the position is not saved';
				END IF;
			END`)
		r.insert(3)
		r.insert(4)
		select {
		case err := <-ended:
			if err == nil {
				t.Fatal("the capture ended without an error; want the refused save")
			}
			t.Logf("the capture ended on the refused save: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("the capture did not end on the refused save")
		}
		waitFor(t, 15*time.Second, func() bool { return !r.slotActive() }, "replication slot inactive")
		if got := r.saved(); got != at {
			t.Fatalf("saved position %X; want it left at %X, before ids 3 and 4", got, at)
		}
		// The state a kill between the two leaves: a transaction's rows past
		// the saved position (id 4 may not have arrived before the refusal).
		if r.rowLSN(3) <= at {
			t.Fatalf("id 3 is not in the index past the saved position %X before the resume: %q", at, r.copies())
		}
		testutil.MustExec(t, r.index, `DROP TRIGGER refuse_position`)
		stop := r.start()
		r.insert(5)
		r.waitSavedAt(5)
		stop()
		if got, want := r.copies(), once(1, 2, 3, 4, 5); got != want {
			t.Fatalf("the index holds %q; want %q", got, want)
		}
	})

	// A slot already past the saved position (moved on by an operator, or an
	// index restored from an older copy) does not send the saved
	// transaction again: the index must keep it.
	t.Run("a slot past the saved position", func(t *testing.T) {
		r := newResumeRig(t, "ahead", 100)
		stop := r.start()
		r.insert(1)
		r.insert(2)
		r.waitSavedAt(2)
		stop()
		r.exec(fmt.Sprintf("SELECT pg_replication_slot_advance('%s', pg_current_wal_lsn())", r.slot))
		stop = r.start()
		r.insert(3)
		r.waitSavedAt(3)
		stop()
		if got, want := r.copies(), once(1, 2, 3); got != want {
			t.Fatalf("the index holds %q; want %q", got, want)
		}
	})

	// A table added to the capture's filter between two runs: the saved
	// transaction touched it, the index holds only the other table's part,
	// and the part of the added table must be indexed now, not dropped as a
	// copy of what the index holds.
	t.Run("a table added to the filter since the saved transaction", func(t *testing.T) {
		r := newResumeRig(t, "widen", 100)
		other := r.tbl + "_b"
		r.exec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", other))
		r.exec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", other))
		r.exec(fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", r.cfg.Publication, other))
		t.Cleanup(func() { _, _ = r.pg.Exec(context.Background(), "DROP TABLE IF EXISTS "+other) })
		stop := r.start()
		tx, err := r.pg.Begin(r.ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, stmt := range []string{
			fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", r.tbl),
			fmt.Sprintf("INSERT INTO %s VALUES (1, 'b')", other),
		} {
			if _, err := tx.Exec(r.ctx, stmt); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(r.ctx); err != nil {
			t.Fatal(err)
		}
		at := r.waitSavedAt(1)
		stop()
		otherRows := func() (n int, pos uint64) {
			return r.tableRows(other)
		}
		if n, _ := otherRows(); n != 0 {
			t.Fatalf("%d rows of %s indexed while it was not in the filter", n, other)
		}
		r.cfg.Tables += ",public." + other
		stop = r.start()
		r.insert(2)
		r.waitSavedAt(2)
		stop()
		if n, pos := otherRows(); n != 1 || pos != at {
			t.Fatalf("%s holds %d rows (newest at %X); want its row of the saved transaction, once, at %X", other, n, pos, at)
		}
		if got, want := r.copies(), once(1, 2); got != want {
			t.Fatalf("the index holds %q; want %q", got, want)
		}
	})

	// The saved transaction X touched a table the capture did not take. The
	// filter is widened, and the run that indexes that table's part makes
	// some of it visible past the writer's row cap, then stops before X's
	// commit (an INSERT refused by a trigger). The next start must remove
	// that part (rows at X written after the event X's save recorded) and
	// index the table's part whole: left, it would read as "held" and the
	// rest would be dropped.
	t.Run("a partial copy of the saved transaction from a stopped run", func(t *testing.T) {
		r := newResumeRig(t, "partial", 100)
		other := r.addUncaptured()
		stop := r.start()
		stmts := []string{fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", r.tbl)}
		var all []int
		for id := 1; id <= 40; id++ {
			stmts = append(stmts, fmt.Sprintf("INSERT INTO %s VALUES (%d, 'b')", other, id))
			all = append(all, id)
		}
		r.txn(stmts...)
		at := r.waitSavedAt(1)
		stop()
		if n, _ := r.tableRows(other); n != 0 {
			t.Fatalf("%d rows of %s indexed while it was not in the filter", n, other)
		}

		r.cfg.Tables += ",public." + other
		r.cfg.BatchSize = 3
		t.Cleanup(pgstreamrun.SetMaxTxnRowsForTest(3))
		testutil.MustExec(t, r.index, fmt.Sprintf(`CREATE TRIGGER refuse_row BEFORE INSERT ON binlog_events FOR EACH ROW
			BEGIN
				IF NEW.table_name = '%s' AND NEW.pk_values = '40' THEN
					SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test: this row is refused';
				END IF;
			END`, other))
		_, ended := r.startRun()
		select {
		case err := <-ended:
			if err == nil {
				t.Fatal("the capture ended without an error; want the refused row")
			}
			t.Logf("the capture ended on the refused row: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("the capture did not end on the refused row")
		}
		waitFor(t, 15*time.Second, func() bool { return !r.slotActive() }, "replication slot inactive")
		testutil.MustExec(t, r.index, `DROP TRIGGER refuse_row`)
		// The state the stop leaves: part of the table's share of X, at X.
		if n, pos := r.tableRows(other); n == 0 || pos != at {
			t.Fatalf("%d rows of %s at %X before the resume; want part of the saved transaction, at %X", n, other, pos, at)
		}

		stop = r.start()
		r.insert(2)
		r.waitSavedAt(2)
		stop()
		if got, want := r.copiesOf(other), once(all...); got != want {
			t.Fatalf("%s holds %q; want %q", other, got, want)
		}
		if got, want := r.copies(), once(1, 2); got != want {
			t.Fatalf("the index holds %q; want %q", got, want)
		}
	})

	// The read that decides whether a resent row is held fails, or does not
	// answer in time: the row is indexed again (a duplicate, never a drop),
	// a warning names the table and the position, and capture goes on.
	for name, breakRead := range map[string]func(r *resumeRig) (restore func()){
		"the held check fails": func(r *resumeRig) func() {
			testutil.MustExec(r.t, r.index, `ALTER TABLE binlog_events DROP INDEX idx_pk_hash`)
			return func() {}
		},
		"the held check does not answer in time": func(r *resumeRig) func() {
			return pgstreamrun.SetResendCheckTimeoutForTest(time.Nanosecond)
		},
	} {
		t.Run(name, func(t *testing.T) {
			slug := "readfail"
			if strings.Contains(name, "time") {
				slug = "readslow"
			}
			r := newResumeRig(t, slug, 100)
			logs := &syncBuffer{}
			r.cfg.Logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
			stop := r.start()
			r.insert(1)
			r.insert(2)
			at := r.waitSavedAt(2)
			stop()
			restore := breakRead(r)
			stop = r.start()
			r.insert(3)
			r.waitSavedAt(3)
			stop()
			restore()
			if got, want := r.copies(), "1:1 2:2 3:1"; got != want {
				t.Fatalf("the index holds %q; want %q (the saved transaction indexed again, nothing lost)", got, want)
			}
			out := logs.String()
			if !strings.Contains(out, "could not read whether the index holds this table's part") ||
				!strings.Contains(out, "table="+r.tbl) || !strings.Contains(out, pglogrepl.LSN(at).String()) {
				t.Fatalf("no warning naming %s and %s; the log was:\n%s", r.tbl, pglogrepl.LSN(at), out)
			}
		})
	}

	// Two tables whose names differ only in case ("Resend_case" and
	// resend_case are two tables in PostgreSQL; the index's collation folds
	// case). The saved transaction touched both under the same key while
	// only one was captured; the other, added to the filter, must not be
	// taken for held because its twin is.
	t.Run("tables whose names differ only in case", func(t *testing.T) {
		r := newResumeRig(t, "case", 100)
		twin := r.addCaseTwin()
		stop := r.start()
		r.txn(fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", r.tbl), fmt.Sprintf(`INSERT INTO "%s" VALUES (1, 'b')`, twin))
		r.waitSavedAt(1)
		stop()
		if got := r.exactCopies(twin); got != "" {
			t.Fatalf("%s holds %q while it was not in the filter", twin, got)
		}
		r.cfg.Tables += ",public." + twin
		stop = r.start()
		r.insert(2)
		r.waitSavedAt(2)
		stop()
		if got, want := r.exactCopies(twin), once(1); got != want {
			t.Fatalf("%s holds %q; want %q", twin, got, want)
		}
		if got, want := r.exactCopies(r.tbl), once(1, 2); got != want {
			t.Fatalf("%s holds %q; want %q", r.tbl, got, want)
		}
	})

	// The same for a TRUNCATE of both twins as the saved transaction.
	t.Run("a TRUNCATE of tables whose names differ only in case", func(t *testing.T) {
		r := newResumeRig(t, "casetr", 100)
		twin := r.addCaseTwin()
		stop := r.start()
		r.insert(1)
		r.waitSavedAt(1)
		r.exec(fmt.Sprintf(`TRUNCATE %s, "%s"`, r.tbl, twin))
		waitFor(t, 20*time.Second, func() bool {
			n, pos := r.exactMarkers(r.tbl)
			return n == 1 && r.saved() == pos
		}, "the TRUNCATE recorded and the position saved at it")
		stop()
		if n, _ := r.exactMarkers(twin); n != 0 {
			t.Fatalf("%d TRUNCATE markers of %s while it was not in the filter", n, twin)
		}
		r.cfg.Tables += ",public." + twin
		stop = r.start()
		r.insert(2)
		r.waitSavedAt(2)
		stop()
		if n, _ := r.exactMarkers(twin); n != 1 {
			t.Fatalf("%d TRUNCATE markers of %s after the resume; want 1", n, twin)
		}
		if n, _ := r.exactMarkers(r.tbl); n != 1 {
			t.Fatalf("%d TRUNCATE markers of %s after the resume; want 1", n, r.tbl)
		}
	})

	// A TRUNCATE as the transaction at the saved position: its marker is
	// recorded once, not again on each resume.
	t.Run("a TRUNCATE at the saved position", func(t *testing.T) {
		r := newResumeRig(t, "truncate", 100)
		markers := func() (n int, pos uint64) {
			var p sql.NullInt64
			if err := r.index.QueryRow("SELECT COUNT(*), MAX(binlog_pos) FROM schema_changes WHERE ddl_query = ?", "TRUNCATE TABLE public."+r.tbl).Scan(&n, &p); err != nil {
				t.Fatalf("count the TRUNCATE markers: %v", err)
			}
			return n, uint64(p.Int64)
		}
		stop := r.start()
		r.insert(1)
		r.waitSavedAt(1)
		r.exec("TRUNCATE " + r.tbl)
		waitFor(t, 20*time.Second, func() bool {
			n, pos := markers()
			return n == 1 && r.saved() == pos
		}, "the TRUNCATE recorded and the position saved at it")
		stop()
		stop = r.start()
		r.insert(2)
		r.waitSavedAt(2)
		stop()
		if n, _ := markers(); n != 1 {
			t.Fatalf("%d TRUNCATE markers after the resume; want 1", n)
		}
		if got, want := r.copies(), once(1, 2); got != want {
			t.Fatalf("the index holds %q; want %q", got, want)
		}
	})
}
