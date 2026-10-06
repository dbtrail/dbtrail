//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2156, `recover` and the single-row reconstruct, on changes captured from a
// real server through the real capture.
//
// Three rows, each changed by two sessions where the second one to COMMIT
// started its statement FIRST (it waited on the row lock the other held), so
// the binary log holds A then B and the times say B then A:
//
//	row 1  A updates, B updates          (seed, wait-A, wait-B)
//	row 2  A updates, B deletes          (seed, upd-A, gone)
//	row 3  A deletes, B inserts again    (seed, gone, ins-B)
//
// and row 9, changed twice by one session, before and after the others.
//
// For each row the test first pins what the order a fetch returns does (the
// defect: the applied script leaves A's value, or fails; the reconstruction
// ends on A), and then that the rule gives back the row as it was before both
// changes. The reversal scripts are APPLIED to the source and the table is
// read back.

const (
	order2156Seed = "1=seed,2=seed,3=seed,9=seed"
)

func order2156Shapes(t *testing.T, sourceDB *sql.DB) {
	t.Helper()
	ctx := context.Background()
	conn := func() *sql.Conn {
		c, err := sourceDB.Conn(ctx)
		if err != nil {
			t.Fatalf("conn: %v", err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	a, b := conn(), conn()
	blocked := func(q string) chan error {
		done := make(chan error, 1)
		go func() {
			_, err := b.ExecContext(ctx, q)
			done <- err
		}()
		return done
	}
	// lockWait: A locks the row, B's statement starts and waits, 2 s later A
	// changes the row and commits, B goes through.
	lockWait := func(id int, bStmt, needle, aStmt string) {
		stmtTimeExec(t, ctx, a, "BEGIN")
		stmtTimeExec(t, ctx, a, fmt.Sprintf("SELECT v FROM t WHERE id = %d FOR UPDATE", id))
		done := blocked(bStmt)
		waitStatementAge(t, sourceDB, needle, 2)
		stmtTimeExec(t, ctx, a, aStmt)
		stmtTimeExec(t, ctx, a, "COMMIT")
		if err := <-done; err != nil {
			t.Fatalf("%s: %v", bStmt, err)
		}
	}

	stmtTimeExec(t, ctx, a, "UPDATE t SET v = 'nine-1' WHERE id = 9")
	lockWait(1, "UPDATE t SET v = 'wait-B' WHERE id = 1", "wait-B", "UPDATE t SET v = 'wait-A' WHERE id = 1")
	lockWait(2, "DELETE FROM t WHERE id = 2 AND 'del-B' <> ''", "del-B", "UPDATE t SET v = 'upd-A' WHERE id = 2")
	lockWait(3, "INSERT INTO t VALUES (3, 'ins-B')", "ins-B", "DELETE FROM t WHERE id = 3")
	stmtTimeExec(t, ctx, a, "UPDATE t SET v = 'nine-2' WHERE id = 9")
}

func order2156Table(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query("SELECT id, v FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("read t: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id int
		var v string
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, fmt.Sprintf("%d=%s", id, v))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return strings.Join(out, ",")
}

// order2156Script is what `recover` does between its flags and its output:
// fetch newest first under a limit, put the rows back in ascending order,
// generate. withRule is the one line this issue adds to it.
func order2156Script(t *testing.T, indexDB *sql.DB, opts query.Options, withRule bool) (string, *recovery.Generator) {
	t.Helper()
	ctx := context.Background()
	opts.Order, opts.Limit = "DESC", 1000
	rows, err := query.New(indexDB).Fetch(ctx, opts)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	rows = query.MergeResults(rows, 0, "ASC")
	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	gen := recovery.New(indexDB, resolver)
	if withRule {
		gen.SetBinlogOrder(query.BinlogOrderProof(ctx, indexDB))
	}
	var buf strings.Builder
	if _, err := gen.GenerateSQLFromRows(rows, &buf); err != nil {
		t.Fatalf("GenerateSQLFromRows: %v", err)
	}
	return buf.String(), gen
}

// order2156Apply runs a script on its own connection, as `mysql < file` does,
// and returns the error of the first statement that failed. Closing the
// connection rolls back what a failed script left open.
func order2156Apply(t *testing.T, applyDSN, script string) error {
	t.Helper()
	db, err := sql.Open("mysql", applyDSN)
	if err != nil {
		t.Fatalf("open apply connection: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	_, err = db.Exec(script)
	return err
}

func order2156UndoIDs(script string) []string {
	var ids []string
	for _, line := range strings.Split(script, "\n") {
		if rest, ok := strings.CutPrefix(line, "-- ["); ok {
			id, _, _ := strings.Cut(rest, "]")
			ids = append(ids, id)
		}
	}
	return ids
}

func runBinlogOrder2156(t *testing.T, flavor, sourceDSN, applyDSN string, sourceDB, indexDB *sql.DB, sourceName string) {
	testutil.MustExec(t, sourceDB, "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(32) NOT NULL) ENGINE=InnoDB")
	testutil.MustExec(t, sourceDB, "INSERT INTO t VALUES (1,'seed'),(2,'seed'),(3,'seed'),(9,'seed')")
	if got := order2156Table(t, sourceDB); got != order2156Seed {
		t.Fatalf("seed = %s", got)
	}
	captureWhile(t, flavor, sourceDSN, sourceDB, indexDB, sourceName, 8, func() { order2156Shapes(t, sourceDB) })
	// This index is one a stream writes, and a stream records itself in
	// stream_state at its first checkpoint. The capture above stops before
	// one is due, so the row is written here: without it the index reads as
	// built with `bintrail index` only, which vouches for one file at a time.
	testutil.MustExec(t, indexDB, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, last_checkpoint, server_id)
		VALUES (1, 'position', 'binlog.000001', 4, NOW(), 1) ON DUPLICATE KEY UPDATE id = id`)
	if proof, err := query.IDsFollowBinlog(context.Background(), indexDB, time.Now()); err != nil || proof != query.IDsFollowStream {
		t.Fatalf("IDsFollowBinlog on the captured index = %v, %v; want IDsFollowStream", proof, err)
	}

	const after = "1=wait-B,3=ins-B,9=nine-2"
	if got := order2156Table(t, sourceDB); got != after {
		t.Fatalf("after the shapes the table is %s, want %s: the shapes did not happen", got, after)
	}
	restore := func() {
		t.Helper()
		testutil.MustExec(t, sourceDB, "DELETE FROM t")
		testutil.MustExec(t, sourceDB, "INSERT INTO t VALUES (1,'wait-B'),(3,'ins-B'),(9,'nine-2')")
	}

	// The premise, per row: of its two changes, the one the binary log holds
	// LAST carries the EARLIER time.
	ctx := context.Background()
	fetchRow := func(pk string) []query.ResultRow {
		t.Helper()
		rows, err := query.New(indexDB).Fetch(ctx, query.Options{Schema: sourceName, Table: "t", PKValues: pk, Limit: 100})
		if err != nil {
			t.Fatalf("fetch row %s: %v", pk, err)
		}
		return rows
	}
	for _, pk := range []string{"1", "2", "3"} {
		rows := fetchRow(pk)
		if len(rows) != 2 {
			t.Fatalf("row %s: %d indexed changes, want 2", pk, len(rows))
		}
		for _, r := range rows {
			t.Logf("row %s as fetched: event_id=%d at=%s:%d event_timestamp=%s type=%d before=%v after=%v",
				pk, r.EventID, r.BinlogFile, r.StartPos, r.EventTimestamp.UTC().Format("15:04:05"), r.EventType, r.RowBefore["v"], r.RowAfter["v"])
		}
		if !query.LaterInBinlog(&rows[0], &rows[1]) || !rows[0].EventTimestamp.Before(rows[1].EventTimestamp) {
			t.Fatalf("row %s: the fetch did not return the later change of the binary log first, with the earlier time: the shape did not happen", pk)
		}
	}

	rowOf := func(pk string) string {
		t.Helper()
		var v string
		err := sourceDB.QueryRow("SELECT v FROM t WHERE id = ?", pk).Scan(&v)
		if err == sql.ErrNoRows {
			return "(no row)"
		}
		if err != nil {
			t.Fatalf("read row %s: %v", pk, err)
		}
		return v
	}

	// ── recover, one row at a time (`recover --pk`) ─────────────────────────
	for _, tc := range []struct {
		pk string
		// What applying the script generated WITHOUT the rule does.
		defectRow string // the row afterwards, when the script applies
		defectErr string // or the error it fails with
	}{
		{"1", "wait-A", ""},
		{"2", "upd-A", ""},
		{"3", "", "Duplicate entry"},
	} {
		t.Run("recover row "+tc.pk, func(t *testing.T) {
			opts := query.Options{Schema: sourceName, Table: "t", PKValues: tc.pk}

			restore()
			plain, _ := order2156Script(t, indexDB, opts, false)
			err := order2156Apply(t, applyDSN, plain)
			switch {
			case tc.defectErr != "":
				if err == nil || !strings.Contains(err.Error(), tc.defectErr) {
					t.Fatalf("without the rule the script was expected to fail with %q; err = %v, row = %s\n%s", tc.defectErr, err, rowOf(tc.pk), plain)
				}
				t.Logf("without the rule: the script fails to apply: %v", err)
			case err != nil:
				t.Fatalf("without the rule the script failed to apply: %v\n%s", err, plain)
			default:
				if got := rowOf(tc.pk); got != tc.defectRow {
					t.Fatalf("without the rule the row is %q after the script; this test expects the defect to leave %q\n%s", got, tc.defectRow, plain)
				}
				t.Logf("without the rule: the script applies with no error and the row is %q, not \"seed\"", rowOf(tc.pk))
			}

			restore()
			fixed, gen := order2156Script(t, indexDB, opts, true)
			if !gen.OrderDecision().Sorted() || gen.OrderDecision().Warning() != "" {
				t.Fatalf("decision = %+v (warning %q), want sorted", gen.OrderDecision(), gen.OrderDecision().Warning())
			}
			if err := order2156Apply(t, applyDSN, fixed); err != nil {
				t.Fatalf("the script failed to apply: %v\n%s", err, fixed)
			}
			if got := rowOf(tc.pk); got != "seed" {
				t.Fatalf("row %s is %q after the script, want \"seed\" (the value before both changes)\n%s", tc.pk, got, fixed)
			}
		})
	}

	// ── recover, the whole table in one script ──────────────────────────────
	t.Run("recover the table", func(t *testing.T) {
		restore()
		fixed, gen := order2156Script(t, indexDB, query.Options{Schema: sourceName, Table: "t"}, true)
		if !gen.OrderDecision().Sorted() {
			t.Fatalf("decision = %+v (warning %q), want sorted", gen.OrderDecision(), gen.OrderDecision().Warning())
		}
		ids := order2156UndoIDs(fixed)
		if len(ids) != 8 {
			t.Fatalf("%d statements, want 8\n%s", len(ids), fixed)
		}
		// On an index a stream writes, the reverse of binary log order is
		// descending event_id.
		for i := 1; i < len(ids); i++ {
			prev, _ := strconv.ParseUint(ids[i-1], 10, 64)
			cur, _ := strconv.ParseUint(ids[i], 10, 64)
			if cur >= prev {
				t.Fatalf("the script does not undo from the last indexed change back: %v", ids)
			}
		}
		if !strings.Contains(fixed, "were written to the binary log in a different order than their") || strings.Contains(fixed, "WARNING") {
			t.Fatalf("header:\n%s", fixed)
		}
		if err := order2156Apply(t, applyDSN, fixed); err != nil {
			t.Fatalf("the script failed to apply: %v\n%s", err, fixed)
		}
		if got := order2156Table(t, sourceDB); got != order2156Seed {
			t.Fatalf("table after the script: %s, want %s\n%s", got, order2156Seed, fixed)
		}
		t.Logf("the script:\n%s", fixed)
	})

	// ── single-row reconstruct ──────────────────────────────────────────────
	// The rows existed before capture started, so the fold starts from the
	// seed row, as it does from a snapshot.
	seed := func(id float64) map[string]any { return map[string]any{"id": id, "v": "seed"} }
	now := time.Now().UTC().Add(time.Hour)
	value := func(state map[string]any) string {
		if state == nil {
			return "(no row)"
		}
		return fmt.Sprint(state["v"])
	}
	for _, tc := range []struct {
		pk      string
		id      float64
		defect  string   // the state the fetched order ends on
		want    string   // the state the database holds
		history []string // seed first
	}{
		{"1", 1, "wait-A", "wait-B", []string{"seed", "wait-A", "wait-B"}},
		{"2", 2, "upd-A", "(no row)", []string{"seed", "upd-A", "(no row)"}},
		{"3", 3, "(no row)", "ins-B", []string{"seed", "(no row)", "ins-B"}},
	} {
		t.Run("reconstruct row "+tc.pk, func(t *testing.T) {
			fetched := fetchRow(tc.pk)
			state, err := reconstruct.ApplyAt(seed(tc.id), fetched, now)
			if err != nil {
				t.Fatalf("ApplyAt: %v", err)
			}
			if got := value(state); got != tc.defect {
				t.Fatalf("folded as fetched the row is %q; this test expects the defect to give %q", got, tc.defect)
			}
			t.Logf("folded as fetched: %q; the database holds %q", value(state), tc.want)

			ordered, order := reconstruct.EventsInBinlogOrder(fetched, now, query.BinlogOrderProof(ctx, indexDB))
			if !order.Sorted() || order.Warning() != "" {
				t.Fatalf("decision = %+v (warning %q), want sorted", order, order.Warning())
			}
			state, err = reconstruct.ApplyAt(seed(tc.id), ordered, now)
			if err != nil {
				t.Fatalf("ApplyAt: %v", err)
			}
			if got := value(state); got != tc.want {
				t.Fatalf("row %s reconstructs to %q, want %q", tc.pk, got, tc.want)
			}
			entries, err := reconstruct.BuildHistory(seed(tc.id), now.Add(-24*time.Hour), ordered, now)
			if err != nil {
				t.Fatalf("BuildHistory: %v", err)
			}
			var hist []string
			for _, e := range entries {
				hist = append(hist, value(e.State))
			}
			if !slices.Equal(hist, tc.history) {
				t.Fatalf("history = %v, want %v", hist, tc.history)
			}
		})
	}

	// ── an index whose ids cannot vouch for the order ───────────────────────
	// The same changes, on an index that a stream writes and that also had a
	// binary log file indexed into it by `bintrail index`: ids no longer say
	// in which order the changes were written, so a restart of the source's
	// numbering inside the window could not be told from a lock wait. The
	// order stays the fetched one and the script says so. Once that file
	// indexing is well in the past of the window, ids vouch again.
	t.Run("files indexed beside the stream", func(t *testing.T) {
		opts := query.Options{Schema: sourceName, Table: "t", PKValues: "1"}
		plain, _ := order2156Script(t, indexDB, opts, false)

		testutil.MustExec(t, indexDB, `INSERT INTO index_state (binlog_file, file_size, last_position, status, started_at, completed_at)
			VALUES ('binlog.000001', 1, 1, 'completed', UTC_TIMESTAMP(), UTC_TIMESTAMP())`)
		got, gen := order2156Script(t, indexDB, opts, true)
		warn := gen.OrderDecision().Warning()
		if gen.OrderDecision().Sorted() || !strings.Contains(warn, "this index cannot show which one is right") {
			t.Fatalf("decision = %+v, warning %q", gen.OrderDecision(), warn)
		}
		if !slices.Equal(order2156UndoIDs(got), order2156UndoIDs(plain)) {
			t.Fatalf("order changed although it could not be proven: %v, without the rule %v", order2156UndoIDs(got), order2156UndoIDs(plain))
		}
		if !strings.Contains(got, "-- WARNING: order of the changes: the binary log and the statement times disagree") {
			t.Fatalf("the script header does not carry the warning:\n%s", got)
		}
		t.Logf("the header of a script whose order could not be proven:\n%s", got[:strings.Index(got, "BEGIN;")])

		fetched := fetchRow("1")
		ordered, order := reconstruct.EventsInBinlogOrder(fetched, now, query.BinlogOrderProof(ctx, indexDB))
		if order.Sorted() || order.Warning() == "" || ordered[0].EventID != fetched[0].EventID {
			t.Fatalf("reconstruct: decision %+v, first event %d (fetched first: %d)", order, ordered[0].EventID, fetched[0].EventID)
		}

		testutil.MustExec(t, indexDB, "UPDATE index_state SET started_at = UTC_TIMESTAMP() - INTERVAL 2 DAY, completed_at = UTC_TIMESTAMP() - INTERVAL 1 DAY")
		_, gen = order2156Script(t, indexDB, opts, true)
		if !gen.OrderDecision().Sorted() {
			t.Fatalf("file indexing ended a day before these changes and the order is still refused: %+v, %q", gen.OrderDecision(), gen.OrderDecision().Warning())
		}
	})
}

func TestIntegrationBinlogOrderRecoverAndReconstruct2156_mysql(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := testutil.CreateTestDB(t)
	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MySQL")
	}
	dsn := testutil.IntegrationDSN(sourceName)
	runBinlogOrder2156(t, gomysql.MySQLFlavor, dsn, dsn+"&multiStatements=true", sourceDB, indexDB, sourceName)
}

func TestIntegrationBinlogOrderRecoverAndReconstruct2156_mariadb(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	dsn := testutil.MariaDBBaseDSN() + "/" + sourceName + "?parseTime=true"
	runBinlogOrder2156(t, gomysql.MariaDBFlavor, dsn, dsn+"&multiStatements=true", sourceDB, indexDB, sourceName)
}
