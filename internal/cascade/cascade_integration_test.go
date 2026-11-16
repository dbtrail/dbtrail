//go:build integration

package cascade_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestCascadeRecoverySpike proves the end-to-end thesis of
// drafts/cascade-recovery-port-2026-06-21.md: from the bintrail binlog index
// (which has the child INSERTs/UPDATEs but NOT the cascade-deleted children,
// because InnoDB ≤8.x never binlogs them) plus the FK graph, we can synthesize
// the deleted children and emit reversal INSERTs that restore the table to its
// exact pre-delete state.
//
// Topology (two-level cascade):
//
//	parent ──ON DELETE CASCADE──▶ child ──ON DELETE CASCADE──▶ grandchild
//
// Edge cases baked in:
//   - child 13 is re-parented (pid 1→2) before the delete  → must SURVIVE
//   - child 14 is explicitly deleted before the cascade     → must NOT be restored
//   - grandchildren 100/101/102 are two levels down         → must be restored (recursion)
//
// Correctness gate (non-negotiable): CHECKSUM TABLE of parent/child/grandchild
// captured at T (just before the cascade delete) must equal the checksum after
// applying the synthesized recovery SQL.
func TestCascadeRecoverySpike(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	sourceDB, sourceName := testutil.CreateTestDB(t)

	skipIfSourceLogsCascades(t, sourceDB)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// ── Schema: parent → child → grandchild, both edges ON DELETE CASCADE ──
	testutil.MustExec(t, sourceDB, `CREATE TABLE parent (
		id   INT PRIMARY KEY,
		name VARCHAR(64)
	) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE child (
		id      INT PRIMARY KEY,
		pid     INT,
		payload VARCHAR(64),
		CONSTRAINT fk_child FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE grandchild (
		id   INT PRIMARY KEY,
		cid  INT,
		note VARCHAR(64),
		CONSTRAINT fk_gc FOREIGN KEY (cid) REFERENCES child(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)

	// Snapshot (columns + FK graph) → resolver for parsing/recovery.
	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	// Rotate to a clean file; everything below lands in `currentBinlog`.
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}

	// ── DML (all logged; the cascade child deletes will NOT be) ──
	testutil.MustExec(t, sourceDB, "INSERT INTO parent VALUES (1,'p1'),(2,'p2')")
	testutil.MustExec(t, sourceDB, "INSERT INTO child VALUES "+
		"(10,1,'c10'),(11,1,'c11'),(12,2,'c12'),(13,1,'c13'),(14,1,'c14')")
	testutil.MustExec(t, sourceDB, "INSERT INTO grandchild VALUES "+
		"(100,10,'g100'),(101,10,'g101'),(102,11,'g102')")
	testutil.MustExec(t, sourceDB, "UPDATE child SET pid=2 WHERE id=13") // re-parent → survives
	testutil.MustExec(t, sourceDB, "DELETE FROM child WHERE id=14")      // explicit pre-delete

	// Binlog timestamps are second-granular; ensure the parent delete is
	// strictly later so the Until=parentTs window cleanly includes the setup.
	time.Sleep(1100 * time.Millisecond)

	// Ground truth at T (state the recovery must reproduce exactly).
	wantParent := checksum(t, sourceDB, "parent")
	wantChild := checksum(t, sourceDB, "child")
	wantGrandchild := checksum(t, sourceDB, "grandchild")

	// The cascade: removes child 10,11 + grandchild 100,101,102 INTERNALLY
	// (no binlog), keeps child 12 (parent 2) and child 13 (re-parented to 2).
	testutil.MustExec(t, sourceDB, "DELETE FROM parent WHERE id=1")

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS") // seal the file

	// ── Parse + index the binlog into binlog_events ──
	tmpDir := t.TempDir()
	cp := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog))
	if out, cperr := cp.CombinedOutput(); cperr != nil {
		t.Fatalf("docker cp %s: %v\n%s", currentBinlog, cperr, out)
	}

	p := parser.New(tmpDir, resolver, parser.Filters{
		Schemas: map[string]bool{sourceName: true},
	}, nil)
	idx := indexer.New(indexDB, 100)
	events := make(chan parser.Event, 256)
	parseErr := make(chan error, 1)
	go func() {
		defer close(events)
		parseErr <- p.ParseFile(ctx, currentBinlog, events)
	}()
	if _, rerr := idx.Run(ctx, events); rerr != nil {
		t.Fatalf("indexer.Run: %v", rerr)
	}
	if perr := <-parseErr; perr != nil {
		t.Fatalf("ParseFile: %v", perr)
	}

	eng := query.New(indexDB)
	del := event.EventDelete

	// ── Prove the blind spot: the cascade child deletes are NOT in the index ──
	childDeletes := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "child", EventType: &del})
	for _, r := range childDeletes {
		if r.PKValues == "10" || r.PKValues == "11" {
			t.Fatalf("blind-spot assumption broken: child %s delete WAS binlogged", r.PKValues)
		}
	}
	if len(childDeletes) != 1 || childDeletes[0].PKValues != "14" {
		t.Fatalf("want only the explicit child 14 delete indexed, got %d: %v", len(childDeletes), pkList(childDeletes))
	}
	gcDeletes := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "grandchild", EventType: &del})
	if len(gcDeletes) != 0 {
		t.Fatalf("want 0 grandchild deletes indexed (all were cascade), got %d: %v", len(gcDeletes), pkList(gcDeletes))
	}
	t.Logf("blind spot confirmed: child cascade-deletes (10,11) and all grandchild deletes absent from the index")

	// ── Synthesize the victims ──
	// Load the FK graph (with rules) from the INDEX — the productionized,
	// source-less path. TakeSnapshot above populated fk_constraints.
	fks, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{sourceName}, time.Now())
	if err != nil {
		t.Fatalf("LoadCascadeFKs: %v", err)
	}
	parentDeletes := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "parent", EventType: &del})

	res, err := cascade.SynthesizeVictims(ctx, eng, fks, parentDeletes, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	// A clean cascade within the window must reconstruct completely.
	if !res.Complete() {
		t.Errorf("expected a complete reconstruction, got Incomplete=%v", res.Incomplete)
	}
	victims := res.Victims
	got := map[string]bool{}
	for _, v := range victims {
		got[v.TableName+":"+v.PKValues] = true
	}
	want := []string{"child:10", "child:11", "grandchild:100", "grandchild:101", "grandchild:102"}
	if len(victims) != len(want) {
		t.Fatalf("want %d victims %v, got %d %v", len(want), want, len(victims), victimList(victims))
	}
	for _, w := range want {
		if !got[w] {
			t.Fatalf("missing synthesized victim %s; got %v", w, victimList(victims))
		}
	}
	// Edge cases must NOT appear as victims.
	for _, bad := range []string{"child:12", "child:13", "child:14"} {
		if got[bad] {
			t.Fatalf("victim %s should have been excluded (survived/pre-deleted)", bad)
		}
	}
	t.Logf("synthesized %d victims: %v", len(victims), victimList(victims))

	// ── Generate reversal SQL (parent delete + synthetic child/grandchild deletes) ──
	gen := recovery.New(indexDB, resolver)
	rows := append(append([]query.ResultRow{}, parentDeletes...), victims...)
	var buf bytes.Buffer
	nStmts, err := gen.GenerateSQLFromRows(rows, &buf)
	if err != nil {
		t.Fatalf("GenerateSQLFromRows: %v", err)
	}
	if nStmts != len(rows) {
		t.Fatalf("want %d reversal statements, got %d", len(rows), nStmts)
	}
	t.Logf("recovery SQL (%d statements):\n%s", nStmts, buf.String())

	// ── Apply the recovery SQL, FK checks off (validates the topo-sort-free path) ──
	applyDB, err := sql.Open("mysql", testutil.BaseDSN()+"/"+sourceName+"?multiStatements=true")
	if err != nil {
		t.Fatalf("open apply conn: %v", err)
	}
	defer applyDB.Close()
	script := "SET FOREIGN_KEY_CHECKS=0;\n" + buf.String() + "\nSET FOREIGN_KEY_CHECKS=1;"
	if _, err := applyDB.ExecContext(ctx, script); err != nil {
		t.Fatalf("apply recovery SQL: %v", err)
	}

	// ── Correctness gate ──
	if got := checksum(t, sourceDB, "parent"); got != wantParent {
		t.Errorf("parent checksum mismatch: want %d, got %d", wantParent, got)
	}
	if got := checksum(t, sourceDB, "child"); got != wantChild {
		t.Errorf("child checksum mismatch: want %d, got %d", wantChild, got)
	}
	if got := checksum(t, sourceDB, "grandchild"); got != wantGrandchild {
		t.Errorf("grandchild checksum mismatch: want %d, got %d", wantGrandchild, got)
	}

	// Spot-check the actual recovered rows for human-readable confidence.
	assertRows(t, sourceDB, "SELECT id,pid FROM child ORDER BY id",
		[][2]int{{10, 1}, {11, 1}, {12, 2}, {13, 2}}) // 14 stays gone, 13 stays re-parented
	assertRows(t, sourceDB, "SELECT id,cid FROM grandchild ORDER BY id",
		[][2]int{{100, 10}, {101, 10}, {102, 11}})
	if !t.Failed() {
		t.Logf("correctness gate PASSED: parent/child/grandchild restored byte-exact to pre-cascade state")
	}
}

func checksum(t *testing.T, db *sql.DB, table string) int64 {
	t.Helper()
	var name string
	var sum sql.NullInt64
	if err := db.QueryRow("CHECKSUM TABLE "+table).Scan(&name, &sum); err != nil {
		t.Fatalf("CHECKSUM TABLE %s: %v", table, err)
	}
	if !sum.Valid {
		t.Fatalf("CHECKSUM TABLE %s returned NULL", table)
	}
	return sum.Int64
}

func mustFetch(t *testing.T, eng *query.Engine, opts query.Options) []query.ResultRow {
	t.Helper()
	rows, err := eng.Fetch(context.Background(), opts)
	if err != nil {
		t.Fatalf("Fetch %s.%s: %v", opts.Schema, opts.Table, err)
	}
	return rows
}

func assertRows(t *testing.T, db *sql.DB, q string, want [][2]int) {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	defer rows.Close()
	var got [][2]int
	for rows.Next() {
		var a, b int
		if err := rows.Scan(&a, &b); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, [2]int{a, b})
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("rows for %q: want %v, got %v", q, want, got)
	}
}

func pkList(rows []query.ResultRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.TableName + ":" + r.PKValues
	}
	return out
}

func victimList(rows []query.ResultRow) []string { return pkList(rows) }
