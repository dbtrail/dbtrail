//go:build integration

package cascade_test

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
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

// TestLoadCascadeFKsFromIndex pins the productionized, source-less loader: after
// a snapshot, the FK graph WITH rules is readable from the index's
// fk_constraints, scoped by schema, with both CASCADE and non-CASCADE edges.
func TestLoadCascadeFKsFromIndex(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE parent (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE child_c (
		id INT PRIMARY KEY, pid INT,
		CONSTRAINT fk_c FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE child_r (
		id INT PRIMARY KEY, pid INT,
		CONSTRAINT fk_r FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE RESTRICT
	) ENGINE=InnoDB`)
	// A composite (multi-column) FK must round-trip as multiple rows sharing one
	// constraint_name, so the synthesis composite-FK guard can detect it.
	testutil.MustExec(t, sourceDB, `CREATE TABLE pcomp (a INT, b INT, PRIMARY KEY (a, b)) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE child_comp (
		id INT PRIMARY KEY, fa INT, fb INT,
		CONSTRAINT fk_comp FOREIGN KEY (fa, fb) REFERENCES pcomp(a, b) ON DELETE CASCADE
	) ENGINE=InnoDB`)

	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	fks, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{sourceName}, time.Now())
	if err != nil {
		t.Fatalf("LoadCascadeFKs: %v", err)
	}
	byTable := map[string]cascade.CascadeFK{}
	for _, fk := range fks {
		byTable[fk.Table] = fk
		// A strict snapshot captures every table, so no edge may carry the
		// #1051 degraded-snapshot marker (a spurious true would fabricate
		// "provably partial" caveats over a complete recovery).
		if fk.ChildExcludedFromSnapshot {
			t.Errorf("edge %s.%s must not be marked ChildExcludedFromSnapshot under a strict snapshot", fk.Schema, fk.Table)
		}
	}
	if c, ok := byTable["child_c"]; !ok || c.DeleteRule != "CASCADE" ||
		c.ReferencedTable != "parent" || c.ReferencedColumn != "id" || c.Column != "pid" {
		t.Errorf("child_c edge missing/wrong: %+v (ok=%v)", c, ok)
	}
	if r, ok := byTable["child_r"]; !ok || r.DeleteRule != "RESTRICT" {
		t.Errorf("child_r edge missing/wrong: %+v (ok=%v)", r, ok)
	}

	// The composite FK must come back as 2 rows sharing constraint_name (and
	// referenced columns a,b) — what the synthesis composite-FK guard keys on.
	var comp []cascade.CascadeFK
	for _, fk := range fks {
		if fk.Table == "child_comp" {
			comp = append(comp, fk)
		}
	}
	if len(comp) != 2 {
		t.Fatalf("composite FK must load as 2 column rows, got %d: %+v", len(comp), comp)
	}
	if comp[0].ConstraintName != "fk_comp" || comp[1].ConstraintName != "fk_comp" {
		t.Errorf("composite FK rows must share constraint_name fk_comp, got %q/%q",
			comp[0].ConstraintName, comp[1].ConstraintName)
	}

	// Scope: an unrelated schema yields nothing.
	none, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{"no_such_schema"}, time.Now())
	if err != nil {
		t.Fatalf("LoadCascadeFKs(none): %v", err)
	}
	if len(none) != 0 {
		t.Errorf("want 0 edges for an unrelated schema, got %d", len(none))
	}
}

// TestSynthesizeVictims_excludedChildFlagged pins the #1051 review fix: a
// degraded snapshot (metadata.TakeSnapshotExcludingInvalid) KEEPS the
// fk_constraints rows of an excluded no-PK CASCADE child, the loaders mark the
// edge ChildExcludedFromSnapshot, and synthesis over a parent DELETE reports the
// recovery as provably partial — the child's row events were never captured,
// so its guaranteed zero-candidate scan must never read as a clean Complete.
// A valid sibling child on the same parent must stay unflagged.
func TestSynthesizeVictims_excludedChildFlagged(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE parent (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE ok_child (
		id INT PRIMARY KEY, pid INT,
		CONSTRAINT fk_ok FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE nopk_child (
		pid INT,
		CONSTRAINT fk_nopk FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)

	if _, err := metadata.TakeSnapshotExcludingInvalid(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshotExcludingInvalid: %v", err)
	}

	// The production loader (CLI/console path) must still surface the excluded
	// child's edge, marked absent; the valid child's edge stays unmarked.
	fks, _, _, err := cascade.LoadCascadeFKsForParent(ctx, indexDB, sourceName, time.Now())
	if err != nil {
		t.Fatalf("LoadCascadeFKsForParent: %v", err)
	}
	byTable := map[string]cascade.CascadeFK{}
	for _, fk := range fks {
		byTable[fk.Table] = fk
	}
	nopk, ok := byTable["nopk_child"]
	if !ok {
		t.Fatal("excluded child's CASCADE edge must still load from fk_constraints")
	}
	if !nopk.ChildExcludedFromSnapshot {
		t.Error("nopk_child edge must be marked ChildExcludedFromSnapshot")
	}
	if okc, ok := byTable["ok_child"]; !ok || okc.ChildExcludedFromSnapshot {
		t.Errorf("ok_child edge must load unmarked, got %+v (ok=%v)", okc, ok)
	}

	// The schema-scoped LoadCascadeFKs hand-duplicates the child_absent SELECT
	// (it has no other caller-driven coverage) — pin that it marks the same
	// edge, so the two loaders cannot drift.
	scoped, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{sourceName}, time.Now())
	if err != nil {
		t.Fatalf("LoadCascadeFKs: %v", err)
	}
	scopedByTable := map[string]cascade.CascadeFK{}
	for _, fk := range scoped {
		scopedByTable[fk.Table] = fk
	}
	if n, ok := scopedByTable["nopk_child"]; !ok || !n.ChildExcludedFromSnapshot {
		t.Errorf("LoadCascadeFKs must mark nopk_child ChildExcludedFromSnapshot, got %+v (ok=%v)", n, ok)
	}
	if okc, ok := scopedByTable["ok_child"]; !ok || okc.ChildExcludedFromSnapshot {
		t.Errorf("LoadCascadeFKs must load ok_child unmarked, got %+v (ok=%v)", okc, ok)
	}

	eng := query.New(indexDB)
	parentDel := query.ResultRow{
		SchemaName: sourceName, TableName: "parent", EventType: event.EventDelete,
		PKValues: "1", RowBefore: map[string]any{"id": float64(1)},
		EventTimestamp: time.Now(),
	}
	res, err := cascade.SynthesizeVictims(ctx, eng, fks,
		[]query.ResultRow{parentDel}, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if res.Complete() {
		t.Fatal("a parent with an excluded CASCADE child must NOT report Complete")
	}
	var flagged bool
	for _, msg := range res.Incomplete {
		if strings.Contains(msg, "nopk_child") {
			flagged = true
		}
		if strings.Contains(msg, "ok_child") {
			t.Errorf("valid child must not be flagged: %q", msg)
		}
	}
	if !flagged {
		t.Errorf("Incomplete must name the excluded child, got: %v", res.Incomplete)
	}
}

// TestSynthesizeVictims_ruleGate pins the deliberate non-bug: only ON DELETE
// CASCADE / SET NULL edges are synthesized. A pure RESTRICT edge and an
// ON-UPDATE-CASCADE-only edge (the dbtrail conflation) must yield nothing — and,
// being neither rule, never even hit the index, so no events are needed.
func TestSynthesizeVictims_ruleGate(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	eng := query.New(indexDB)

	parentDel := query.ResultRow{
		SchemaName: "app", TableName: "parent", EventType: event.EventDelete,
		PKValues: "1", RowBefore: map[string]any{"id": float64(1)},
		EventTimestamp: time.Now(),
	}
	fks := []cascade.CascadeFK{
		{Schema: "app", Table: "child_r", ConstraintName: "fk_r", Column: "pid",
			ReferencedSchema: "app", ReferencedTable: "parent", ReferencedColumn: "id",
			DeleteRule: "RESTRICT", UpdateRule: "RESTRICT"},
		{Schema: "app", Table: "child_u", ConstraintName: "fk_u", Column: "pid",
			ReferencedSchema: "app", ReferencedTable: "parent", ReferencedColumn: "id",
			DeleteRule: "RESTRICT", UpdateRule: "CASCADE"}, // ON UPDATE CASCADE only
	}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, fks,
		[]query.ResultRow{parentDel}, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("non-CASCADE/SET-NULL edges must yield no victims, got %d: %+v", len(res.Victims), res.Victims)
	}
	if len(res.SetNullRows) != 0 {
		t.Errorf("non-CASCADE/SET-NULL edges must yield no SET NULL restores, got %d: %+v", len(res.SetNullRows), res.SetNullRows)
	}
}

// TestSynthesizeVictims_childFKColumnSkew pins the child-side DDL-skew guard
// (#832): when a cascade OLDER than a child FK-column rename ("pid" -> "parent_id")
// is recovered, the candidate scan uses the LATEST snapshot's column name against
// events whose row-images still carry the old name, so ColumnEq matches 0 rows —
// indistinguishable from "no children existed". Synthesis must NOT report a clean
// Complete; it must flag skew (mirroring the parent-side "noref" caveat).
func TestSynthesizeVictims_childFKColumnSkew(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-30 * time.Minute).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	// Child events predate the rename: their row-images carry the OLD name "pid".
	// child 10 referenced parent 1 (via the column that is now called parent_id).
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pid":1}`))

	// The FK graph is the LATEST snapshot: the column is already renamed parent_id.
	fks := []cascade.CascadeFK{{
		Schema: dbName, Table: "child", ConstraintName: "fk", Column: "parent_id",
		ReferencedSchema: dbName, ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "CASCADE",
	}}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, fks, parentDelete(dbName, T), cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Fatalf("renamed FK column must match 0 candidates, got %d: %v", len(res.Victims), victimList(res.Victims))
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "absent from every sampled") {
		t.Errorf("child-side DDL-skew must flag incompleteness, not a clean Complete; Incomplete=%v", res.Incomplete)
	}
}

// TestSynthesizeVictims_zeroChildrenNotFlaggedAsSkew is the control for the skew
// guard: a parent with genuinely no matching children — but whose child table
// carries the FK column under its snapshot name (referencing OTHER parents) — must
// stay Complete. The skew probe must not turn a legitimate zero-victim result into
// a false caveat.
func TestSynthesizeVictims_zeroChildrenNotFlaggedAsSkew(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-30 * time.Minute).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	// child 10 references parent 2 (NOT the deleted parent 1); the FK column
	// parent_id is present under its snapshot name.
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"parent_id":2}`))

	fks := []cascade.CascadeFK{{
		Schema: dbName, Table: "child", ConstraintName: "fk", Column: "parent_id",
		ReferencedSchema: dbName, ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "CASCADE",
	}}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, fks, parentDelete(dbName, T), cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Fatalf("parent 1 has no children, got %d victims: %v", len(res.Victims), victimList(res.Victims))
	}
	if !res.Complete() {
		t.Errorf("a genuine no-children result (FK column present) must stay Complete, got Incomplete=%v", res.Incomplete)
	}
}

// TestSynthesizeVictims_setNull pins ON DELETE SET NULL: a child that referenced
// the deleted parent (and survives) becomes a SetNullRestore (not a victim), and
// it is NOT recursed (no row was deleted). A child re-pointed away is excluded.
func TestSynthesizeVictims_setNull(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-30 * time.Minute).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	// child 10 references parent 1 (SET NULL victim); child 13 re-pointed to 2.
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	testutil.InsertEvent(t, db, "b.000001", 20, 30, ts, nil, dbName, "child", 1, "13", nil, nil, []byte(`{"id":13,"pid":1}`))
	testutil.InsertEvent(t, db, "b.000001", 30, 40, ts, nil, dbName, "child", 2 /*UPDATE*/, "13", nil, []byte(`{"id":13,"pid":1}`), []byte(`{"id":13,"pid":2}`))

	fks := []cascade.CascadeFK{{
		Schema: dbName, Table: "child", ConstraintName: "fk", Column: "pid",
		ReferencedSchema: dbName, ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "SET NULL",
	}}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, fks, parentDelete(dbName, T), cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("SET NULL must produce no DELETE victims (rows survive), got %v", res.Victims)
	}
	if len(res.SetNullRows) != 1 || res.SetNullRows[0].PKValues != "10" || res.SetNullRows[0].Column != "pid" {
		t.Fatalf("want one SET NULL restore for child:10 column pid, got %+v", res.SetNullRows)
	}
	if got := cascadeValToStr(res.SetNullRows[0].Value); got != "1" {
		t.Errorf("restore value should be the parent key 1, got %q", got)
	}
	for _, sr := range res.SetNullRows {
		if sr.PKValues == "13" {
			t.Errorf("re-pointed child 13 must NOT be a SET NULL restore")
		}
	}
}

func cascadeValToStr(v any) string { return fmt.Sprintf("%v", v) }

// TestSynthesizeVictims_compositeFKSkipped pins the composite-FK guard: a
// multi-column CASCADE FK cannot be reconstructed by the single-column victim
// match, so it must be SKIPPED with a warning, never silently mis-synthesized.
func TestSynthesizeVictims_compositeFKSkipped(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	eng := query.New(indexDB)

	parentDel := query.ResultRow{
		SchemaName: "app", TableName: "parent", EventType: event.EventDelete,
		PKValues: "1|9", RowBefore: map[string]any{"k1": float64(1), "k2": float64(9)},
		EventTimestamp: time.Now(),
	}
	// One composite CASCADE FK = two rows sharing the constraint name.
	fks := []cascade.CascadeFK{
		{Schema: "app", Table: "child", ConstraintName: "fk_comp", Column: "pk1",
			ReferencedSchema: "app", ReferencedTable: "parent", ReferencedColumn: "k1", DeleteRule: "CASCADE"},
		{Schema: "app", Table: "child", ConstraintName: "fk_comp", Column: "pk2",
			ReferencedSchema: "app", ReferencedTable: "parent", ReferencedColumn: "k2", DeleteRule: "CASCADE"},
	}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, fks,
		[]query.ResultRow{parentDel}, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("composite FK must be skipped, not mis-synthesized; got %d victims", len(res.Victims))
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "composite FK") {
		t.Errorf("composite FK skip must flag incompleteness; Incomplete: %v", res.Incomplete)
	}
}

// TestSynthesizeVictims_selfRefAndCompositePK exercises two robustness cases in
// one real cascade: a SELF-REFERENCING tree (node.parent_id → node.id ON DELETE
// CASCADE) and a COMPOSITE-PK child (leaf PK (node_id, seq)). Deleting the tree
// root cascades through the whole subtree (none of it binlogged); synthesis must
// recurse over the self-ref without a false cycle and restore both tables
// byte-exact, including the pipe-delimited composite PKs.
func TestSynthesizeVictims_selfRefAndCompositePK(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	skipIfSourceLogsCascades(t, sourceDB)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE node (
		id        INT PRIMARY KEY,
		parent_id INT NULL,
		label     VARCHAR(32),
		CONSTRAINT fk_self FOREIGN KEY (parent_id) REFERENCES node(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE leaf (
		node_id INT,
		seq     INT,
		payload VARCHAR(32),
		PRIMARY KEY (node_id, seq),
		CONSTRAINT fk_leaf FOREIGN KEY (node_id) REFERENCES node(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}

	// Tree: 1(root) → {2,3}; 2 → {4}. Leaves hang off nodes 2 and 4.
	testutil.MustExec(t, sourceDB, "INSERT INTO node VALUES (1,NULL,'root'),(2,1,'a'),(3,1,'b'),(4,2,'c')")
	testutil.MustExec(t, sourceDB, "INSERT INTO leaf VALUES (2,1,'l21'),(2,2,'l22'),(4,1,'l41')")

	// Update the GRANDCHILD (node 4) and its composite-PK leaf in a strictly
	// LATER statement than their parent's last event. This is the discriminating
	// timing: the per-level window must end at the ROOT delete time T, not at
	// each victim's own timestamp — otherwise node 4's (and leaf 4|1's) post-T1
	// update falls outside the deeper window and a stale state is recovered.
	time.Sleep(1100 * time.Millisecond) // binlog ts is second-granular
	testutil.MustExec(t, sourceDB, "UPDATE node SET label='c2' WHERE id=4")
	testutil.MustExec(t, sourceDB, "UPDATE leaf SET payload='l41b' WHERE node_id=4 AND seq=1")

	time.Sleep(1100 * time.Millisecond) // parent delete strictly after every child event

	wantNode := checksum(t, sourceDB, "node")
	wantLeaf := checksum(t, sourceDB, "leaf")

	// Delete the root; InnoDB cascades to nodes 2,3,4 and all leaves INTERNALLY
	// (only the node id=1 delete is binlogged).
	testutil.MustExec(t, sourceDB, "DELETE FROM node WHERE id=1")
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	tmpDir := t.TempDir()
	cp := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog))
	if out, cperr := cp.CombinedOutput(); cperr != nil {
		t.Fatalf("docker cp %s: %v\n%s", currentBinlog, cperr, out)
	}

	p := parser.New(tmpDir, resolver, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
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
	nodeDeletes := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "node", EventType: &del})
	if len(nodeDeletes) != 1 || nodeDeletes[0].PKValues != "1" {
		t.Fatalf("want only the root node delete indexed, got %d: %v", len(nodeDeletes), pkList(nodeDeletes))
	}

	fks, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{sourceName}, time.Now())
	if err != nil {
		t.Fatalf("LoadCascadeFKs: %v", err)
	}
	res, err := cascade.SynthesizeVictims(ctx, eng, fks, nodeDeletes, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if !res.Complete() {
		t.Errorf("expected a complete reconstruction, got Incomplete=%v", res.Incomplete)
	}
	victims := res.Victims
	// Expect nodes 2,3,4 (self-ref recursion) + leaves 2|1, 2|2, 4|1 (composite PK).
	got := map[string]bool{}
	for _, v := range victims {
		got[v.TableName+":"+v.PKValues] = true
	}
	for _, want := range []string{"node:2", "node:3", "node:4", "leaf:2|1", "leaf:2|2", "leaf:4|1"} {
		if !got[want] {
			t.Errorf("missing synthesized victim %s; got %v", want, victimList(victims))
		}
	}
	if len(victims) != 6 {
		t.Errorf("want 6 victims, got %d: %v", len(victims), victimList(victims))
	}

	// node 4 was UPDATEd after its parent's last event; with the root-T window it
	// must be recovered at its LATEST label ('c2'), not the stale insert ('c').
	// The byte-exact checksum below also enforces this; assert it explicitly too.
	for _, v := range victims {
		if v.TableName == "node" && v.PKValues == "4" && v.RowBefore["label"] != "c2" {
			t.Errorf("node 4 recovered at stale label %v, want c2", v.RowBefore["label"])
		}
	}

	// Depth cap (read-only re-run on the same index): MaxDepth=1 must reconstruct
	// only the root's direct children and flag the deeper subtree as incomplete.
	resD, err := cascade.SynthesizeVictims(ctx, eng, fks, nodeDeletes, cascade.Options{MaxDepth: 1})
	if err != nil {
		t.Fatalf("SynthesizeVictims(MaxDepth=1): %v", err)
	}
	if resD.Complete() || !strings.Contains(strings.Join(resD.Incomplete, " "), "MaxDepth") {
		t.Errorf("MaxDepth=1 must flag depth incompleteness; Incomplete=%v", resD.Incomplete)
	}
	for _, v := range resD.Victims {
		if v.TableName == "node" && v.PKValues == "4" {
			t.Errorf("MaxDepth=1 must not reach grandchild node:4; got %v", victimList(resD.Victims))
		}
	}

	// Candidate cap: CandidateLimit=1 truncates node 1's two children and flags it.
	resC, err := cascade.SynthesizeVictims(ctx, eng, fks, nodeDeletes, cascade.Options{CandidateLimit: 1})
	if err != nil {
		t.Fatalf("SynthesizeVictims(CandidateLimit=1): %v", err)
	}
	if resC.Complete() || !strings.Contains(strings.Join(resC.Incomplete, " "), "more than 1") {
		t.Errorf("CandidateLimit=1 must flag truncation; Incomplete=%v", resC.Incomplete)
	}

	// Recover (root from binlog + synthesized subtree) and apply FK-checks-off.
	gen := recoveryNew(t, indexDB, resolver)
	rows := append(append([]query.ResultRow{}, nodeDeletes...), victims...)
	sqlText := generateSQL(t, gen, rows)
	applyFKOff(t, sourceName, sqlText)

	if got := checksum(t, sourceDB, "node"); got != wantNode {
		t.Errorf("node checksum mismatch: want %d, got %d", wantNode, got)
	}
	if got := checksum(t, sourceDB, "leaf"); got != wantLeaf {
		t.Errorf("leaf checksum mismatch: want %d, got %d", wantLeaf, got)
	}
}

// --- small shared helpers for the recovery+apply tail ---

func recoveryNew(t *testing.T, indexDB *sql.DB, resolver *metadata.Resolver) *recovery.Generator {
	t.Helper()
	return recovery.New(indexDB, resolver)
}

func generateSQL(t *testing.T, gen *recovery.Generator, rows []query.ResultRow) string {
	t.Helper()
	var buf strings.Builder
	if _, err := gen.GenerateSQLFromRows(rows, &buf); err != nil {
		t.Fatalf("GenerateSQLFromRows: %v", err)
	}
	return buf.String()
}

func applyFKOff(t *testing.T, dbName, sqlText string) {
	t.Helper()
	conn, err := sql.Open("mysql", testutil.BaseDSN()+"/"+dbName+"?multiStatements=true")
	if err != nil {
		t.Fatalf("open apply conn: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Exec("SET FOREIGN_KEY_CHECKS=0;\n" + sqlText + "\nSET FOREIGN_KEY_CHECKS=1;"); err != nil {
		t.Fatalf("apply recovery SQL: %v", err)
	}
}

// captureAndIndex flushes to a clean binlog, runs dml (which performs the
// inserts/updates/deletes to capture), seals + copies the file, and parses it
// into the index. One captured window per call.
// skipIfSourceLogsCascades skips a test that pins the synthesis of cascades
// the source does not log, on a source that logs them (MySQL 9.6 and later
// with innodb_native_foreign_keys = OFF): there the cascade's child rows are
// in the index and recover-cascade reverses them instead (Options.Logging),
// which TestIntegrationRecoverCascadeRestoresTheSource covers end to end.
// The test's premise ("the cascade is invisible in the binary log") does not
// hold on such a server, and the server's setting cannot change without a
// restart, so the synthesis is pinned on the cells whose server does not log
// them (8.0, 8.4).
func skipIfSourceLogsCascades(t *testing.T, sourceDB *sql.DB) {
	t.Helper()
	src, err := metadata.ReadSourceFKLogging(context.Background(), sourceDB)
	if err != nil {
		t.Fatalf("ReadSourceFKLogging: %v", err)
	}
	if src.LogsCascades() {
		t.Skip("the source writes its foreign-key cascades to the binary log (innodb_native_foreign_keys = OFF); " +
			"this test pins the synthesis of cascades a source does not log")
	}
}

func captureAndIndex(t *testing.T, sourceDB, indexDB *sql.DB, resolver *metadata.Resolver, sourceName string, dml func()) {
	t.Helper()
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	dml()
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	tmpDir := t.TempDir()
	cp := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog))
	if out, e := cp.CombinedOutput(); e != nil {
		t.Fatalf("docker cp %s: %v\n%s", currentBinlog, e, out)
	}
	p := parser.New(tmpDir, resolver, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	idx := indexer.New(indexDB, 100)
	events := make(chan parser.Event, 256)
	perr := make(chan error, 1)
	go func() { defer close(events); perr <- p.ParseFile(context.Background(), currentBinlog, events) }()
	if _, e := idx.Run(context.Background(), events); e != nil {
		t.Fatalf("indexer.Run: %v", e)
	}
	if e := <-perr; e != nil {
		t.Fatalf("ParseFile: %v", e)
	}
}

// TestSynthesizeVictims_multiPathDedup pins the multi-path dedup: a child
// reachable from the deleted parent via TWO CASCADE FKs must be emitted ONCE,
// or recovery would double-INSERT its PK and fail on apply.
func TestSynthesizeVictims_multiPathDedup(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	skipIfSourceLogsCascades(t, sourceDB)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE p (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE c (
		id INT PRIMARY KEY, ref1 INT, ref2 INT,
		CONSTRAINT fk1 FOREIGN KEY (ref1) REFERENCES p(id) ON DELETE CASCADE,
		CONSTRAINT fk2 FOREIGN KEY (ref2) REFERENCES p(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	captureAndIndex(t, sourceDB, indexDB, resolver, sourceName, func() {
		testutil.MustExec(t, sourceDB, "INSERT INTO p VALUES (1)")
		testutil.MustExec(t, sourceDB, "INSERT INTO c VALUES (100,1,1)") // references p=1 via BOTH FKs
		time.Sleep(1100 * time.Millisecond)
		testutil.MustExec(t, sourceDB, "DELETE FROM p WHERE id=1") // cascades c once
	})

	eng := query.New(indexDB)
	del := event.EventDelete
	parentDeletes := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "p", EventType: &del})
	fks, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{sourceName}, time.Now())
	if err != nil {
		t.Fatalf("LoadCascadeFKs: %v", err)
	}

	res, err := cascade.SynthesizeVictims(ctx, eng, fks, parentDeletes, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 1 {
		t.Fatalf("multi-path child must be emitted exactly once, got %d: %v", len(res.Victims), victimList(res.Victims))
	}
	if res.Victims[0].TableName != "c" || res.Victims[0].PKValues != "100" {
		t.Errorf("want victim c:100, got %+v", res.Victims[0])
	}
}

// TestSynthesizeVictims_multiRootIndependentT pins the breadth dimension of the
// root-T fix: two independent cascades deleted at T_A ≪ T_B, each must use its
// OWN root T. cb is updated BETWEEN T_A and T_B; only per-root T_B recovers its
// latest 'b1' — a collapse to a single global T (first/min/now) recovers stale 'b0'.
func TestSynthesizeVictims_multiRootIndependentT(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	skipIfSourceLogsCascades(t, sourceDB)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE pa (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE pb (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE ca (
		id INT PRIMARY KEY, pid INT, val VARCHAR(16),
		CONSTRAINT fka FOREIGN KEY (pid) REFERENCES pa(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE cb (
		id INT PRIMARY KEY, pid INT, val VARCHAR(16),
		CONSTRAINT fkb FOREIGN KEY (pid) REFERENCES pb(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	captureAndIndex(t, sourceDB, indexDB, resolver, sourceName, func() {
		testutil.MustExec(t, sourceDB, "INSERT INTO pa VALUES (1)")
		testutil.MustExec(t, sourceDB, "INSERT INTO pb VALUES (2)")
		testutil.MustExec(t, sourceDB, "INSERT INTO ca VALUES (10,1,'a0')")
		testutil.MustExec(t, sourceDB, "INSERT INTO cb VALUES (20,2,'b0')")
		testutil.MustExec(t, sourceDB, "DELETE FROM pa WHERE id=1") // T_A: cascades ca
		time.Sleep(1100 * time.Millisecond)
		testutil.MustExec(t, sourceDB, "UPDATE cb SET val='b1' WHERE id=20") // between T_A and T_B
		time.Sleep(1100 * time.Millisecond)
		testutil.MustExec(t, sourceDB, "DELETE FROM pb WHERE id=2") // T_B: cascades cb
	})

	eng := query.New(indexDB)
	del := event.EventDelete
	paDel := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "pa", EventType: &del})
	pbDel := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "pb", EventType: &del})
	parentDeletes := append(append([]query.ResultRow{}, paDel...), pbDel...)
	fks, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{sourceName}, time.Now())
	if err != nil {
		t.Fatalf("LoadCascadeFKs: %v", err)
	}

	res, err := cascade.SynthesizeVictims(ctx, eng, fks, parentDeletes, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	byPK := map[string]query.ResultRow{}
	for _, v := range res.Victims {
		byPK[v.TableName+":"+v.PKValues] = v
	}
	ca, ok := byPK["ca:10"]
	if !ok {
		t.Fatalf("ca:10 missing; got %v", victimList(res.Victims))
	}
	if ca.RowBefore["val"] != "a0" {
		t.Errorf("ca:10 val = %v, want a0", ca.RowBefore["val"])
	}
	cb, ok := byPK["cb:20"]
	if !ok {
		t.Fatalf("cb:20 missing; got %v", victimList(res.Victims))
	}
	if cb.RowBefore["val"] != "b1" {
		t.Errorf("cb:20 recovered at %v, want b1 (per-root T regression — using a collapsed global T?)", cb.RowBefore["val"])
	}
}

// TestSynthesizeVictims_setNullTwoColumnsSameRow is the regression for the #571
// review CRITICAL: a SetNullRestore is per-COLUMN, but the dedup key was per-ROW,
// so a child carrying TWO single-column SET NULL FKs to the same deleted parent
// (e.g. mgr + mentor → parent.id) had its second column silently dropped. Both
// columns must be restored, and the result must stay Complete.
func TestSynthesizeVictims_setNullTwoColumnsSameRow(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-30 * time.Minute).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	// child 10 points at parent 1 via BOTH mgr and mentor; the cascade nulls both.
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"mgr":1,"mentor":1}`))

	fks := []cascade.CascadeFK{
		{Schema: dbName, Table: "child", ConstraintName: "fk_mgr", Column: "mgr",
			ReferencedSchema: dbName, ReferencedTable: "parent", ReferencedColumn: "id", DeleteRule: "SET NULL"},
		{Schema: dbName, Table: "child", ConstraintName: "fk_mentor", Column: "mentor",
			ReferencedSchema: dbName, ReferencedTable: "parent", ReferencedColumn: "id", DeleteRule: "SET NULL"},
	}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, fks, parentDelete(dbName, T), cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("SET NULL produces no DELETE victims, got %v", res.Victims)
	}
	cols := map[string]bool{}
	for _, sr := range res.SetNullRows {
		if sr.PKValues != "10" {
			t.Errorf("unexpected restore for pk=%s", sr.PKValues)
		}
		cols[sr.Column] = true
	}
	if len(res.SetNullRows) != 2 || !cols["mgr"] || !cols["mentor"] {
		t.Fatalf("both mgr and mentor of child:10 must be restored (per-column key), got %+v", res.SetNullRows)
	}
	if !res.Complete() {
		t.Errorf("two-column SET NULL restore should be complete, got %v", res.Incomplete)
	}
}

// TestSynthesizeVictims_setNullGuardProtectsRepointedRow proves the `IS NULL`
// guard is load-bearing, not decorative. A child re-pointed to a NEW parent after
// the (unlogged) SET NULL still surfaces a STALE restore candidate — its pre-null
// INSERT image is the only event matching the fk=parent scan, so a SetNullRestore
// to the OLD parent IS emitted. Only the runtime `AND fk IS NULL` predicate stops
// it from clobbering the live re-pointed value. This applies the generated UPDATE
// against the real row and asserts the row is unchanged.
func TestSynthesizeVictims_setNullGuardProtectsRepointedRow(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	skipIfSourceLogsCascades(t, sourceDB)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE parent (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE child (
		id  INT PRIMARY KEY,
		mgr INT NULL,
		CONSTRAINT fk_mgr FOREIGN KEY (mgr) REFERENCES parent(id) ON DELETE SET NULL
	) ENGINE=InnoDB`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	testutil.MustExec(t, sourceDB, "INSERT INTO parent VALUES (1),(2)")

	// Capture the whole child history: its INSERT (mgr=1, the stale candidate),
	// the parent delete (InnoDB nulls child.mgr, UNLOGGED), then the operator's
	// re-point to parent 2 (logged UPDATE before=NULL/after=2).
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	testutil.MustExec(t, sourceDB, "INSERT INTO child VALUES (10,1)")
	testutil.MustExec(t, sourceDB, "DELETE FROM parent WHERE id=1")
	testutil.MustExec(t, sourceDB, "UPDATE child SET mgr=2 WHERE id=10")
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	tmpDir := t.TempDir()
	cp := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog))
	if out, cperr := cp.CombinedOutput(); cperr != nil {
		t.Fatalf("docker cp %s: %v\n%s", currentBinlog, cperr, out)
	}
	p := parser.New(tmpDir, resolver, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	idx := indexer.New(indexDB, 100)
	events := make(chan parser.Event, 256)
	parseErr := make(chan error, 1)
	go func() { defer close(events); parseErr <- p.ParseFile(ctx, currentBinlog, events) }()
	if _, rerr := idx.Run(ctx, events); rerr != nil {
		t.Fatalf("indexer.Run: %v", rerr)
	}
	if perr := <-parseErr; perr != nil {
		t.Fatalf("ParseFile: %v", perr)
	}

	// Live truth after the re-point: child 10 now points at parent 2.
	wantChild := checksum(t, sourceDB, "child")

	eng := query.New(indexDB)
	del := event.EventDelete
	parentDeletes := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "parent", EventType: &del})
	if len(parentDeletes) != 1 || parentDeletes[0].PKValues != "1" {
		t.Fatalf("want only the parent 1 delete indexed, got %v", pkList(parentDeletes))
	}
	fks, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{sourceName}, time.Now())
	if err != nil {
		t.Fatalf("LoadCascadeFKs: %v", err)
	}
	res, err := cascade.SynthesizeVictims(ctx, eng, fks, parentDeletes, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	// The synthesis can't distinguish the re-pointed child from a still-nulled
	// one, so it DOES emit a stale restore to mgr=1 — the guard is the defense.
	if len(res.SetNullRows) != 1 || res.SetNullRows[0].PKValues != "10" {
		t.Fatalf("expected one (stale) SET NULL restore for child:10, got %+v", res.SetNullRows)
	}

	tm, err := resolver.Resolve(sourceName, "child")
	if err != nil {
		t.Fatalf("resolve child: %v", err)
	}
	sr := res.SetNullRows[0]
	stmt, err := recovery.FormatSetNullRestore(sr.Schema, sr.Table, sr.Column, sr.Value, tm.PKColumnMetas(), sr.Row)
	if err != nil {
		t.Fatalf("FormatSetNullRestore: %v", err)
	}
	if !strings.Contains(stmt, "IS NULL") {
		t.Fatalf("restore must carry the IS NULL guard: %q", stmt)
	}
	applyFKOff(t, sourceName, stmt+";")

	// The guard must have made the UPDATE a no-op: child 10 still points at 2.
	if got := checksum(t, sourceDB, "child"); got != wantChild {
		t.Errorf("guard failed: re-pointed child was clobbered (checksum want %d, got %d)", wantChild, got)
	}
	var mgr sql.NullInt64
	if err := sourceDB.QueryRow("SELECT mgr FROM child WHERE id=10").Scan(&mgr); err != nil {
		t.Fatalf("read child: %v", err)
	}
	if !mgr.Valid || mgr.Int64 != 2 {
		t.Errorf("child 10 mgr should remain 2 (re-pointed), got %v", mgr)
	}
}

// TestSynthesizeVictims_sameParentPKDeletedTwice is the regression for #831:
// visited/emitted were keyed globally by (table, pk), so when the SAME parent
// PK was deleted, re-created, and deleted again within the window, the second
// root DELETE was silently skipped — children created between the two deletes
// were never reconstructed, a child re-deleted with a newer image kept the
// STALE first-root image, and NO Incomplete caveat was added (exit 0
// "complete"). Keys are now per-root and cross-root duplicates collapse to the
// newest image; the SET NULL sibling gets the same newest-wins treatment.
func TestSynthesizeVictims_sameParentPKDeletedTwice(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T2 := time.Now().UTC()
	T1 := T2.Add(-30 * time.Minute)
	h := T1.Add(-30 * time.Minute).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h, h.Add(time.Hour), h.Add(2 * time.Hour)})
	genA := T1.Add(-10 * time.Minute).Format("2006-01-02 15:04:05")
	genB := T1.Add(10 * time.Minute).Format("2006-01-02 15:04:05")

	// Generation A (cascade-deleted at T1): childc 10, 11; childn 50 nulled.
	testutil.InsertEvent(t, db, "b.000001", 10, 20, genA, nil, dbName, "childc", 1, "10", nil, nil, []byte(`{"id":10,"pid":1,"val":"a0"}`))
	testutil.InsertEvent(t, db, "b.000001", 20, 30, genA, nil, dbName, "childc", 1, "11", nil, nil, []byte(`{"id":11,"pid":1,"val":"gen-a"}`))
	testutil.InsertEvent(t, db, "b.000001", 30, 40, genA, nil, dbName, "childn", 1, "50", nil, nil, []byte(`{"id":50,"ref":1,"val":"n0"}`))
	// Parent 1 re-created between the deletes; generation B (cascade-deleted at
	// T2): childc 10 re-created with a NEWER image, childc 20 brand new, childn
	// 50 re-created with a newer image.
	testutil.InsertEvent(t, db, "b.000001", 40, 50, genB, nil, dbName, "childc", 1, "10", nil, nil, []byte(`{"id":10,"pid":1,"val":"a1"}`))
	testutil.InsertEvent(t, db, "b.000001", 50, 60, genB, nil, dbName, "childc", 1, "20", nil, nil, []byte(`{"id":20,"pid":1,"val":"gen-b"}`))
	testutil.InsertEvent(t, db, "b.000001", 60, 70, genB, nil, dbName, "childn", 1, "50", nil, nil, []byte(`{"id":50,"ref":1,"val":"n1"}`))

	fks := []cascade.CascadeFK{
		{Schema: dbName, Table: "childc", ConstraintName: "fkc", Column: "pid",
			ReferencedSchema: dbName, ReferencedTable: "parent", ReferencedColumn: "id", DeleteRule: "CASCADE"},
		{Schema: dbName, Table: "childn", ConstraintName: "fkn", Column: "ref",
			ReferencedSchema: dbName, ReferencedTable: "parent", ReferencedColumn: "id", DeleteRule: "SET NULL"},
	}
	parents := append(parentDelete(dbName, T1), parentDelete(dbName, T2)...)

	res, err := cascade.SynthesizeVictims(context.Background(), eng, fks, parents, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if !res.Complete() {
		t.Errorf("both roots are fully reconstructable; want complete, got %v", res.Incomplete)
	}
	byPK := map[string]query.ResultRow{}
	for _, v := range res.Victims {
		if _, dup := byPK[v.TableName+":"+v.PKValues]; dup {
			t.Errorf("victim %s:%s emitted more than once", v.TableName, v.PKValues)
		}
		byPK[v.TableName+":"+v.PKValues] = v
	}
	if len(res.Victims) != 3 {
		t.Errorf("want victims childc:10,11,20 exactly once each, got %v", victimList(res.Victims))
	}
	if _, ok := byPK["childc:20"]; !ok {
		t.Errorf("childc:20 (created between the two deletes) missing — second root silently skipped? victims: %v", victimList(res.Victims))
	}
	if _, ok := byPK["childc:11"]; !ok {
		t.Errorf("childc:11 (first cascade) missing; victims: %v", victimList(res.Victims))
	}
	if v, ok := byPK["childc:10"]; !ok || v.RowBefore["val"] != "a1" {
		t.Errorf("childc:10 must carry its NEWEST image val=a1, got %v — stale first-root image", v.RowBefore["val"])
	}
	// SET NULL sibling: exactly one restore for childn:50, built from the
	// newest pre-null image.
	if len(res.SetNullRows) != 1 {
		t.Fatalf("want exactly one SET NULL restore for childn:50, got %+v", res.SetNullRows)
	}
	if sr := res.SetNullRows[0]; sr.PKValues != "50" || sr.Row["val"] != "n1" {
		t.Errorf("childn:50 restore must use the newest image val=n1, got %+v", sr)
	}
}

// TestSynthesizeVictims_sameSecondRootsNotCollapsed is a follow-up regression
// for #831: visited/emitted keyed the same-parent-PK-deleted-twice case by
// event_timestamp truncated to whole seconds (DATETIME has no fractional
// component), so two GENUINELY DISTINCT root deletes landing in the same
// wall-clock second collided on an identical key and the second root's
// cascade was silently never walked — reproducing #831 at sub-second
// granularity despite the fix above. Roots are keyed by the root's own
// EventID (the binlog_events auto-increment PK, always unique) instead.
//
// The FK references a non-PK unique column (unique_key) whose value differs
// between the parent PK's two incarnations, so each root's cascade walk
// matches a DIFFERENT child — this makes a collision observable without
// relying on real-time sub-second timing: both DELETE events are inserted
// with the IDENTICAL stored timestamp, deterministically, and the test would
// be flaky-free either way.
func TestSynthesizeVictims_sameSecondRootsNotCollapsed(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h, h.Add(time.Hour)})
	sameTS := T.Format("2006-01-02 15:04:05")
	childTS := T.Add(-10 * time.Minute).Format("2006-01-02 15:04:05")

	// Children referencing each incarnation's unique_key, both well within
	// the lookback window of the (identical) root timestamp.
	testutil.InsertEvent(t, db, "b.000001", 10, 20, childTS, nil, dbName, "childc", 1, "10", nil, nil,
		[]byte(`{"id":10,"ref_key":"A","val":"a0"}`))
	testutil.InsertEvent(t, db, "b.000001", 20, 30, childTS, nil, dbName, "childc", 1, "20", nil, nil,
		[]byte(`{"id":20,"ref_key":"B","val":"b0"}`))

	// Two DISTINCT root deletes of parent pk=1, at the IDENTICAL stored
	// second — the same PK re-created between them with a DIFFERENT
	// unique_key ("A" then "B"), so each root's cascade is genuinely
	// different despite sharing a timestamp.
	testutil.InsertEvent(t, db, "b.000001", 30, 40, sameTS, nil, dbName, "parent", 3, "1", nil,
		[]byte(`{"id":1,"unique_key":"A"}`), nil)
	testutil.InsertEvent(t, db, "b.000001", 40, 50, sameTS, nil, dbName, "parent", 3, "1", nil,
		[]byte(`{"id":1,"unique_key":"B"}`), nil)

	del := event.EventDelete
	parentDeletes := mustFetch(t, eng, query.Options{Schema: dbName, Table: "parent", EventType: &del, Order: "ASC"})
	if len(parentDeletes) != 2 {
		t.Fatalf("want 2 parent DELETE events, got %d", len(parentDeletes))
	}
	if !parentDeletes[0].EventTimestamp.Equal(parentDeletes[1].EventTimestamp) {
		t.Fatalf("test setup requires both roots to share one stored second, got %v and %v",
			parentDeletes[0].EventTimestamp, parentDeletes[1].EventTimestamp)
	}
	if parentDeletes[0].EventID == parentDeletes[1].EventID {
		t.Fatalf("test setup requires two distinct EventIDs, got both = %d", parentDeletes[0].EventID)
	}

	fks := []cascade.CascadeFK{{
		Schema: dbName, Table: "childc", ConstraintName: "fkc", Column: "ref_key",
		ReferencedSchema: dbName, ReferencedTable: "parent", ReferencedColumn: "unique_key",
		DeleteRule: "CASCADE",
	}}

	res, err := cascade.SynthesizeVictims(context.Background(), eng, fks, parentDeletes, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if !res.Complete() {
		t.Errorf("both roots are fully reconstructable; want complete, got %v", res.Incomplete)
	}
	got := victimKeys(res.Victims)
	if !got["childc:10"] || !got["childc:20"] {
		t.Errorf("want both childc:10 (root A) and childc:20 (root B) reconstructed — a same-second root was silently skipped? got %v", victimList(res.Victims))
	}
}

// insertFKRow inserts one fk_constraints edge directly, bypassing TakeSnapshot,
// so tests can build a controlled FK snapshot history with explicit times.
func insertFKRow(t *testing.T, db *sql.DB, snapshotID int, constraint, schema, table, column, refSchema, refTable, refColumn, deleteRule string) {
	t.Helper()
	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, 'RESTRICT')`,
		snapshotID, constraint, schema, table, column, refSchema, refTable, refColumn, deleteRule)
}

// TestLoadCascadeFKs_snapshotInEffectAtDelete is the regression for #834: the
// FK graph must come from the snapshot in effect AT the root delete, not the
// latest one. An ON DELETE CASCADE FK dropped (here: re-created as RESTRICT)
// after the delete and re-snapshotted would otherwise silently erase its
// cascade victims from the synthesis with no caveat.
func TestLoadCascadeFKs_snapshotInEffectAtDelete(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	s1 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	s2 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	const fmtDT = "2006-01-02 15:04:05"
	// Snapshot 1 (10:00): child.pid -> parent.id is ON DELETE CASCADE.
	testutil.InsertSnapshot(t, indexDB, 1, s1.Format(fmtDT), "app", "child", "id", 1, "PRI", "int", "NO")
	insertFKRow(t, indexDB, 1, "fk_child", "app", "child", "pid", "app", "parent", "id", "CASCADE")
	// Snapshot 2 (12:00): the FK was dropped and re-added as RESTRICT.
	testutil.InsertSnapshot(t, indexDB, 2, s2.Format(fmtDT), "app", "child", "id", 1, "PRI", "int", "NO")
	insertFKRow(t, indexDB, 2, "fk_child", "app", "child", "pid", "app", "parent", "id", "RESTRICT")

	// A delete at 11:00 (between the snapshots) must see the CASCADE edge.
	atDelete := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	fks, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{"app"}, atDelete)
	if err != nil {
		t.Fatalf("LoadCascadeFKs(11:00): %v", err)
	}
	if len(fks) != 1 || fks[0].DeleteRule != "CASCADE" {
		t.Errorf("delete between snapshots must use snapshot 1 (CASCADE), got %+v", fks)
	}

	// A delete at 13:00 (after snapshot 2) sees the current RESTRICT edge.
	fks, err = cascade.LoadCascadeFKs(ctx, indexDB, []string{"app"}, s2.Add(time.Hour))
	if err != nil {
		t.Fatalf("LoadCascadeFKs(13:00): %v", err)
	}
	if len(fks) != 1 || fks[0].DeleteRule != "RESTRICT" {
		t.Errorf("delete after snapshot 2 must use snapshot 2 (RESTRICT), got %+v", fks)
	}

	// The production loader resolves the same way, without a caveat.
	fks, snapID, caveat, err := cascade.LoadCascadeFKsForParent(ctx, indexDB, "app", atDelete)
	if err != nil {
		t.Fatalf("LoadCascadeFKsForParent(11:00): %v", err)
	}
	if len(fks) != 1 || fks[0].DeleteRule != "CASCADE" {
		t.Errorf("ForParent between snapshots must use snapshot 1 (CASCADE), got %+v", fks)
	}
	if snapID != 1 {
		t.Errorf("delete between snapshots must resolve snapshot 1, got snapshotID=%d", snapID)
	}
	if caveat != "" {
		t.Errorf("a covered delete must carry no caveat, got %q", caveat)
	}

	// A delete BEFORE the first FK snapshot falls back to the earliest graph
	// (closest approximation) and must say so — never silently.
	fks, snapID, caveat, err = cascade.LoadCascadeFKsForParent(ctx, indexDB, "app", s1.Add(-time.Hour))
	if err != nil {
		t.Fatalf("LoadCascadeFKsForParent(09:00): %v", err)
	}
	if len(fks) != 1 || fks[0].DeleteRule != "CASCADE" {
		t.Errorf("pre-history delete must fall back to the EARLIEST graph, got %+v", fks)
	}
	if snapID != 1 {
		t.Errorf("pre-history fallback must resolve the earliest snapshot (1), got snapshotID=%d", snapID)
	}
	if caveat == "" {
		t.Error("pre-history fallback must surface a caveat")
	}
}

// TestGroupParentDeletesByFKGraph_multiRootTopologyChange is the regression
// for the follow-up to #834: a BATCH of parent deletes anchored on a single
// (the earliest) root's FK graph can silently mis-recover a LATER root when
// the FK topology changed mid-window. Root A's delete is correctly under
// RESTRICT (no real cascade); root B's later delete is under CASCADE and has
// a genuine cascade victim. Anchoring the whole batch on root A's (earlier,
// RESTRICT) snapshot would silently drop root B's real victim with no
// caveat — exit 0 "complete". Each root must resolve its OWN graph.
func TestGroupParentDeletesByFKGraph_multiRootTopologyChange(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	base := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	s1 := base                            // 09:00: RESTRICT
	s2 := base.Add(30 * time.Minute)      // 09:30: CASCADE
	rootA := base.Add(10 * time.Minute)   // 09:10: under RESTRICT, no real cascade
	rootB := base.Add(60 * time.Minute)   // 10:00: under CASCADE, real cascade victim
	childTS := base.Add(45 * time.Minute) // 09:45: after s2, before rootB
	const fmtDT = "2006-01-02 15:04:05"

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{
		base.Truncate(time.Hour), base.Add(time.Hour).Truncate(time.Hour),
	})

	testutil.InsertSnapshot(t, db, 1, s1.Format(fmtDT), dbName, "child", "id", 1, "PRI", "int", "NO")
	insertFKRow(t, db, 1, "fk_child", dbName, "child", "pid", dbName, "parent", "id", "RESTRICT")
	testutil.InsertSnapshot(t, db, 2, s2.Format(fmtDT), dbName, "child", "id", 1, "PRI", "int", "NO")
	insertFKRow(t, db, 2, "fk_child", dbName, "child", "pid", dbName, "parent", "id", "CASCADE")

	// Root B's real cascade victim: a child referencing parent pk=2, present
	// before root B's delete (and after the CASCADE snapshot).
	testutil.InsertEvent(t, db, "b.000001", 10, 20, childTS.Format(fmtDT), nil, dbName, "child", 1, "100", nil, nil,
		[]byte(`{"id":100,"pid":2,"val":"v0"}`))

	testutil.InsertEvent(t, db, "b.000001", 20, 30, rootA.Format(fmtDT), nil, dbName, "parent", 3, "1", nil,
		[]byte(`{"id":1}`), nil)
	testutil.InsertEvent(t, db, "b.000001", 30, 40, rootB.Format(fmtDT), nil, dbName, "parent", 3, "2", nil,
		[]byte(`{"id":2}`), nil)

	del := event.EventDelete
	parentDeletes := mustFetch(t, eng, query.Options{Schema: dbName, Table: "parent", EventType: &del, Order: "ASC"})
	if len(parentDeletes) != 2 {
		t.Fatalf("want 2 parent DELETE events, got %d", len(parentDeletes))
	}

	groups, caveats, err := cascade.GroupParentDeletesByFKGraph(ctx, db, dbName, parentDeletes)
	if err != nil {
		t.Fatalf("GroupParentDeletesByFKGraph: %v", err)
	}
	if len(caveats) != 0 {
		t.Errorf("both roots are covered by an FK snapshot; want no caveats, got %v", caveats)
	}
	if len(groups) != 2 {
		t.Fatalf("root A (RESTRICT) and root B (CASCADE) must resolve DIFFERENT graphs, want 2 groups, got %d", len(groups))
	}

	results := make([]cascade.Result, 0, len(groups))
	for _, g := range groups {
		r, serr := cascade.SynthesizeVictims(ctx, eng, g.FKs, g.Roots, cascade.Options{})
		if serr != nil {
			t.Fatalf("SynthesizeVictims: %v", serr)
		}
		results = append(results, r)
	}
	res := cascade.MergeResults(results...)

	if !res.Complete() {
		t.Errorf("both roots are fully reconstructable; want complete, got %v", res.Incomplete)
	}
	got := victimKeys(res.Victims)
	if !got["child:100"] {
		t.Errorf("root B's real cascade victim child:100 is missing — batch-anchored on root A's (RESTRICT) graph? got %v", victimList(res.Victims))
	}
	if len(res.Victims) != 1 {
		t.Errorf("root A (RESTRICT) must NOT fabricate a victim, want exactly 1 victim, got %v", victimList(res.Victims))
	}
}
