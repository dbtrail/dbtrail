//go:build integration

package cascade_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/cascaderecover"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The ON UPDATE cascade suite (#1002). InnoDB rewrites a child's FK when the
// parent's REFERENCED KEY is UPDATEd, and — exactly like the ON DELETE cascades
// — never binlogs those child updates, so reverting the parent alone leaves the
// child FKs dangling on the new value. These tests pin the synthesis of that
// missing half, and just as importantly its NEGATIVE space: an UPDATE that never
// touched the referenced key, and the strict separation from delete_rule.

// updEnv is one prepared index DB with an hourly partition around T.
type updEnv struct {
	db     *sql.DB
	dbName string
	eng    *query.Engine
	T      time.Time
	ts     string
}

func newUpdEnv(t *testing.T) updEnv {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	T := time.Now().UTC()
	h := T.Add(-30 * time.Minute).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	return updEnv{
		db: db, dbName: dbName, eng: query.New(db), T: T,
		ts: h.Add(10 * time.Minute).Format("2006-01-02 15:04:05"),
	}
}

// parentKeyUpdate is the ON UPDATE analog of parentDelete: a root UPDATE on
// parent pk=1 moving the referenced column `col` from oldVal to newVal.
//
// pkValues overrides the event's pk_values, which the indexer writes from the
// BEFORE image (parser.BuildPKValues over row_before). When the referenced
// column IS the parent's PK, a chain of key updates therefore lands under a
// DIFFERENT pk_values per link, which is precisely what the key-chain probe
// must not depend on.
func parentKeyUpdate(schema, col string, oldVal, newVal any, at time.Time, pkValues ...string) []query.ResultRow {
	pk := "1"
	if len(pkValues) > 0 {
		pk = pkValues[0]
	}
	return []query.ResultRow{{
		SchemaName: schema, TableName: "parent", EventType: 2 /* UPDATE */, PKValues: pk,
		RowBefore:      map[string]any{"id": json.Number("1"), col: oldVal},
		RowAfter:       map[string]any{"id": json.Number("1"), col: newVal},
		EventTimestamp: at,
	}}
}

func keyRestoreByPK(rows []cascade.FKKeyRestore) map[string]cascade.FKKeyRestore {
	m := map[string]cascade.FKKeyRestore{}
	for _, r := range rows {
		m[r.Table+":"+r.PKValues] = r
	}
	return m
}

// TestSynthesizeKeyUpdate_cascade is the headline case: a parent's referenced
// key moves 1 → 99 under ON UPDATE CASCADE, so InnoDB silently rewrote every
// child FK that held 1. The reversal must put each child back to 1, guarded on
// the value the cascade actually left there (99). A child re-pointed away before
// the update is excluded; a child deleted before it is excluded.
func TestSynthesizeKeyUpdate_cascade(t *testing.T) {
	e := newUpdEnv(t)
	// child 10 → parent 1 (cascade victim); child 13 re-pointed to parent 2;
	// child 14 deleted before the parent update.
	testutil.InsertEvent(t, e.db, "b.000001", 10, 20, e.ts, nil, e.dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	testutil.InsertEvent(t, e.db, "b.000001", 20, 30, e.ts, nil, e.dbName, "child", 1, "13", nil, nil, []byte(`{"id":13,"pid":1}`))
	testutil.InsertEvent(t, e.db, "b.000001", 30, 40, e.ts, nil, e.dbName, "child", 2, "13", nil, []byte(`{"id":13,"pid":1}`), []byte(`{"id":13,"pid":2}`))
	testutil.InsertEvent(t, e.db, "b.000001", 40, 50, e.ts, nil, e.dbName, "child", 1, "14", nil, nil, []byte(`{"id":14,"pid":1}`))
	testutil.InsertEvent(t, e.db, "b.000001", 50, 60, e.ts, nil, e.dbName, "child", 3, "14", nil, []byte(`{"id":14,"pid":1}`), nil)

	fks := []cascade.CascadeFK{{
		Schema: e.dbName, Table: "child", ConstraintName: "fk", Column: "pid",
		ReferencedSchema: e.dbName, ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "RESTRICT", UpdateRule: "CASCADE",
	}}
	roots := parentKeyUpdate(e.dbName, "id", json.Number("1"), json.Number("99"), e.T)
	res, err := cascade.SynthesizeVictims(context.Background(), e.eng, fks, roots, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 || len(res.SetNullRows) != 0 {
		t.Errorf("an ON UPDATE cascade deletes nothing and nulls nothing; got %d victims, %d set-null", len(res.Victims), len(res.SetNullRows))
	}
	if len(res.KeyUpdates) != 1 {
		t.Fatalf("want exactly one key restore (child 10), got %d: %+v", len(res.KeyUpdates), res.KeyUpdates)
	}
	kr := res.KeyUpdates[0]
	if kr.PKValues != "10" || kr.Column != "pid" {
		t.Errorf("want a restore for child pk=10 column pid, got %+v", kr)
	}
	if got := cascadeValToStr(kr.OldValue); got != "1" {
		t.Errorf("OldValue (what the FK goes back to) = %q, want 1", got)
	}
	if got := cascadeValToStr(kr.NewValue); got != "99" {
		t.Errorf("NewValue (the guard: what the cascade left) = %q, want 99", got)
	}
	if len(res.KeyUpdateParents) != 1 {
		t.Fatalf("the root UPDATE moved a cascading referenced key, so it must be reported as a parent to reverse; got %+v", res.KeyUpdateParents)
	}
	if !res.Complete() {
		t.Errorf("want Complete, got Incomplete=%v", res.Incomplete)
	}
}

// TestSynthesizeKeyUpdate_setNull pins the ON UPDATE SET NULL variant: the
// children were NULLed rather than re-pointed, so the restore's guard is
// "fk IS NULL" (NewValue nil) while the value restored is still the OLD key.
func TestSynthesizeKeyUpdate_setNull(t *testing.T) {
	e := newUpdEnv(t)
	testutil.InsertEvent(t, e.db, "b.000001", 10, 20, e.ts, nil, e.dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pid":1}`))

	fks := []cascade.CascadeFK{{
		Schema: e.dbName, Table: "child", ConstraintName: "fk", Column: "pid",
		ReferencedSchema: e.dbName, ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "RESTRICT", UpdateRule: "SET NULL",
	}}
	roots := parentKeyUpdate(e.dbName, "id", json.Number("1"), json.Number("99"), e.T)
	res, err := cascade.SynthesizeVictims(context.Background(), e.eng, fks, roots, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.KeyUpdates) != 1 {
		t.Fatalf("want one key restore, got %+v", res.KeyUpdates)
	}
	kr := res.KeyUpdates[0]
	if got := cascadeValToStr(kr.OldValue); got != "1" {
		t.Errorf("SET NULL must restore the OLD parent key, got %q", got)
	}
	if kr.NewValue != nil {
		t.Errorf("SET NULL leaves the column NULL, so NewValue must be nil (an IS NULL guard); got %#v", kr.NewValue)
	}
}

// TestSynthesizeKeyUpdate_unrelatedColumnNoop is the critical negative: an
// UPDATE that never touched the FK's REFERENCED column cannot have cascaded, so
// it must synthesize nothing AND must not be reported as a parent to reverse —
// otherwise `recover-cascade` would silently undo unrelated column edits.
func TestSynthesizeKeyUpdate_unrelatedColumnNoop(t *testing.T) {
	e := newUpdEnv(t)
	testutil.InsertEvent(t, e.db, "b.000001", 10, 20, e.ts, nil, e.dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pid":1}`))

	fks := []cascade.CascadeFK{{
		Schema: e.dbName, Table: "child", ConstraintName: "fk", Column: "pid",
		ReferencedSchema: e.dbName, ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "RESTRICT", UpdateRule: "CASCADE",
	}}
	// id (the referenced key) is untouched; only `name` changed.
	roots := []query.ResultRow{{
		SchemaName: e.dbName, TableName: "parent", EventType: 2, PKValues: "1",
		RowBefore:      map[string]any{"id": json.Number("1"), "name": "before"},
		RowAfter:       map[string]any{"id": json.Number("1"), "name": "after"},
		EventTimestamp: e.T,
	}}
	res, err := cascade.SynthesizeVictims(context.Background(), e.eng, fks, roots, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.KeyUpdates) != 0 {
		t.Errorf("an UPDATE that left the referenced key alone must synthesize NOTHING, got %+v", res.KeyUpdates)
	}
	if len(res.KeyUpdateParents) != 0 {
		t.Errorf("a non-cascading UPDATE must not be reported as a parent to reverse, got %+v", res.KeyUpdateParents)
	}
	if !res.Complete() {
		t.Errorf("want Complete, got Incomplete=%v", res.Incomplete)
	}
}

// TestSynthesizeKeyUpdate_deleteRuleNotConflated is the guard on "this adds a
// DISTINCT update path, it does not merge the two": a parent-key UPDATE against
// an ON DELETE CASCADE-only edge must synthesize nothing. (The mirror direction
// — a parent DELETE against an ON UPDATE-only edge — is TestSynthesizeVictims_ruleGate.)
func TestSynthesizeKeyUpdate_deleteRuleNotConflated(t *testing.T) {
	e := newUpdEnv(t)
	testutil.InsertEvent(t, e.db, "b.000001", 10, 20, e.ts, nil, e.dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pid":1}`))

	fks := []cascade.CascadeFK{{
		Schema: e.dbName, Table: "child", ConstraintName: "fk", Column: "pid",
		ReferencedSchema: e.dbName, ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "CASCADE", UpdateRule: "RESTRICT", // ON DELETE only
	}}
	roots := parentKeyUpdate(e.dbName, "id", json.Number("1"), json.Number("99"), e.T)
	res, err := cascade.SynthesizeVictims(context.Background(), e.eng, fks, roots, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.KeyUpdates) != 0 || len(res.Victims) != 0 || len(res.SetNullRows) != 0 {
		t.Errorf("delete_rule must never drive an UPDATE root; got %d key restores, %d victims, %d set-null",
			len(res.KeyUpdates), len(res.Victims), len(res.SetNullRows))
	}
	if len(res.KeyUpdateParents) != 0 {
		t.Errorf("no ON UPDATE edge → no parent to reverse, got %+v", res.KeyUpdateParents)
	}
}

// TestSynthesizeKeyUpdate_multiLevel walks the recursion: parent.id is
// referenced by child.pid, and child.pid is itself referenced by
// grandchild.cpid, both ON UPDATE CASCADE. Moving parent.id 1 → 99 rewrites
// child.pid AND then grandchild.cpid, all below the binlog — the reversal must
// restore both levels.
func TestSynthesizeKeyUpdate_multiLevel(t *testing.T) {
	e := newUpdEnv(t)
	testutil.InsertEvent(t, e.db, "b.000001", 10, 20, e.ts, nil, e.dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	testutil.InsertEvent(t, e.db, "b.000001", 20, 30, e.ts, nil, e.dbName, "grandchild", 1, "100", nil, nil, []byte(`{"id":100,"cpid":1}`))

	fks := []cascade.CascadeFK{
		{Schema: e.dbName, Table: "child", ConstraintName: "fk_c", Column: "pid",
			ReferencedSchema: e.dbName, ReferencedTable: "parent", ReferencedColumn: "id",
			DeleteRule: "RESTRICT", UpdateRule: "CASCADE"},
		{Schema: e.dbName, Table: "grandchild", ConstraintName: "fk_g", Column: "cpid",
			ReferencedSchema: e.dbName, ReferencedTable: "child", ReferencedColumn: "pid",
			DeleteRule: "RESTRICT", UpdateRule: "CASCADE"},
	}
	roots := parentKeyUpdate(e.dbName, "id", json.Number("1"), json.Number("99"), e.T)
	res, err := cascade.SynthesizeVictims(context.Background(), e.eng, fks, roots, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	byPK := keyRestoreByPK(res.KeyUpdates)
	if len(res.KeyUpdates) != 2 {
		t.Fatalf("want restores for child 10 AND grandchild 100, got %d: %+v", len(res.KeyUpdates), res.KeyUpdates)
	}
	gc, ok := byPK["grandchild:100"]
	if !ok {
		t.Fatalf("the second level was not reached: %+v", res.KeyUpdates)
	}
	if cascadeValToStr(gc.OldValue) != "1" || cascadeValToStr(gc.NewValue) != "99" {
		t.Errorf("grandchild restore should be 99 → 1, got old=%v new=%v", gc.OldValue, gc.NewValue)
	}
	if !res.Complete() {
		t.Errorf("want Complete, got Incomplete=%v", res.Incomplete)
	}
}

// TestSynthesizeKeyUpdate_maxDepthFlagged pins one of the WARN paths the issue
// asks for: the same two-level topology with MaxDepth=1 cannot reach the
// grandchild, and must SAY so rather than report a clean Complete.
func TestSynthesizeKeyUpdate_maxDepthFlagged(t *testing.T) {
	e := newUpdEnv(t)
	testutil.InsertEvent(t, e.db, "b.000001", 10, 20, e.ts, nil, e.dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	testutil.InsertEvent(t, e.db, "b.000001", 20, 30, e.ts, nil, e.dbName, "grandchild", 1, "100", nil, nil, []byte(`{"id":100,"cpid":1}`))

	fks := []cascade.CascadeFK{
		{Schema: e.dbName, Table: "child", ConstraintName: "fk_c", Column: "pid",
			ReferencedSchema: e.dbName, ReferencedTable: "parent", ReferencedColumn: "id",
			DeleteRule: "RESTRICT", UpdateRule: "CASCADE"},
		{Schema: e.dbName, Table: "grandchild", ConstraintName: "fk_g", Column: "cpid",
			ReferencedSchema: e.dbName, ReferencedTable: "child", ReferencedColumn: "pid",
			DeleteRule: "RESTRICT", UpdateRule: "CASCADE"},
	}
	roots := parentKeyUpdate(e.dbName, "id", json.Number("1"), json.Number("99"), e.T)
	res, err := cascade.SynthesizeVictims(context.Background(), e.eng, fks, roots, cascade.Options{MaxDepth: 1})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.KeyUpdates) != 1 {
		t.Fatalf("depth 1 reaches the child only, got %+v", res.KeyUpdates)
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "MaxDepth") {
		t.Errorf("an unreached level must be flagged, not silently dropped; Incomplete=%v", res.Incomplete)
	}
}

// TestSynthesizeKeyUpdate_repeatedKeyUpdateFlagged is the subtle correctness
// case. The parent key moved A → B and then B → C inside the window. The A → B
// cascade rewrote the children below the binlog, so their last INDEXED image
// still says A — the scan for this root's old key (B) matches nothing. A clean
// "0 children, Complete" there would be a lie; the run must be flagged.
func TestSynthesizeKeyUpdate_repeatedKeyUpdateFlagged(t *testing.T) {
	e := newUpdEnv(t)
	// The child's last logged image predates BOTH parent updates: pcode = 'A'.
	testutil.InsertEvent(t, e.db, "b.000001", 10, 20, e.ts, nil, e.dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pcode":"A"}`))
	// The EARLIER parent key update (A → B), which is in the index.
	testutil.InsertEvent(t, e.db, "b.000001", 20, 30, e.ts, nil, e.dbName, "parent", 2, "1",
		nil, []byte(`{"id":1,"code":"A"}`), []byte(`{"id":1,"code":"B"}`))

	fks := []cascade.CascadeFK{{
		Schema: e.dbName, Table: "child", ConstraintName: "fk", Column: "pcode",
		ReferencedSchema: e.dbName, ReferencedTable: "parent", ReferencedColumn: "code",
		DeleteRule: "RESTRICT", UpdateRule: "CASCADE",
	}}
	// The ROOT is the later update B → C.
	roots := parentKeyUpdate(e.dbName, "code", "B", "C", e.T)
	res, err := cascade.SynthesizeVictims(context.Background(), e.eng, fks, roots, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.KeyUpdates) != 0 {
		t.Fatalf("the child's indexed image still says A, so nothing matches B; got %+v", res.KeyUpdates)
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "EARLIER update of referenced column") {
		t.Errorf("a chained parent-key update hides children and MUST be flagged; Incomplete=%v", res.Incomplete)
	}
}

// TestSynthesizeKeyUpdate_repeatedKeyUpdateFlagged_referencedPK is the same
// chain as above with the ONE difference that matters in practice: the
// referenced column IS the parent's primary key (`REFERENCES parent(id)`, the
// overwhelmingly common shape), not a secondary unique column.
//
// The indexer writes pk_values from the BEFORE image, so the two links of the
// chain land under DIFFERENT pk_values ("1" for 1→99, "99" for the root 99→100).
// A probe scoped by the root's own pk_values therefore cannot see the earlier
// link at all, and the run reports 0 restores + Complete — the silent zero the
// key-chain probe exists to prevent. The probe must key off the referenced
// column's VALUE, not the parent's pk_values.
func TestSynthesizeKeyUpdate_repeatedKeyUpdateFlagged_referencedPK(t *testing.T) {
	e := newUpdEnv(t)
	// The child's last logged image predates BOTH parent updates: pid = 1.
	testutil.InsertEvent(t, e.db, "b.000001", 10, 20, e.ts, nil, e.dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	// The EARLIER parent key update (id 1 → 99). Its pk_values is the BEFORE
	// image's PK ("1"), not the root's ("99").
	testutil.InsertEvent(t, e.db, "b.000001", 20, 30, e.ts, nil, e.dbName, "parent", 2, "1",
		nil, []byte(`{"id":1}`), []byte(`{"id":99}`))

	fks := []cascade.CascadeFK{{
		Schema: e.dbName, Table: "child", ConstraintName: "fk", Column: "pid",
		ReferencedSchema: e.dbName, ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "RESTRICT", UpdateRule: "CASCADE",
	}}
	// The ROOT is the later update 99 → 100, indexed under pk_values "99".
	roots := parentKeyUpdate(e.dbName, "id", json.Number("99"), json.Number("100"), e.T, "99")
	res, err := cascade.SynthesizeVictims(context.Background(), e.eng, fks, roots, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.KeyUpdates) != 0 {
		t.Fatalf("the child's indexed image still says pid=1, so nothing matches 99; got %+v", res.KeyUpdates)
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "EARLIER update of referenced column") {
		t.Errorf("a chained parent-key update on the PRIMARY KEY hides children just as much as on a "+
			"secondary key, and MUST be flagged; Incomplete=%v", res.Incomplete)
	}
}

// TestSynthesizeKeyUpdate_singleKeyUpdateNotFlagged is the control for the probe
// above: an earlier UPDATE of the SAME parent row that left the referenced
// column alone is not a chain, so it must not manufacture a false caveat.
func TestSynthesizeKeyUpdate_singleKeyUpdateNotFlagged(t *testing.T) {
	e := newUpdEnv(t)
	testutil.InsertEvent(t, e.db, "b.000001", 10, 20, e.ts, nil, e.dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pcode":"B"}`))
	// An earlier parent UPDATE that changed `name`, NOT `code`.
	testutil.InsertEvent(t, e.db, "b.000001", 20, 30, e.ts, nil, e.dbName, "parent", 2, "1",
		nil, []byte(`{"id":1,"code":"B","name":"x"}`), []byte(`{"id":1,"code":"B","name":"y"}`))

	fks := []cascade.CascadeFK{{
		Schema: e.dbName, Table: "child", ConstraintName: "fk", Column: "pcode",
		ReferencedSchema: e.dbName, ReferencedTable: "parent", ReferencedColumn: "code",
		DeleteRule: "RESTRICT", UpdateRule: "CASCADE",
	}}
	roots := parentKeyUpdate(e.dbName, "code", "B", "C", e.T)
	res, err := cascade.SynthesizeVictims(context.Background(), e.eng, fks, roots, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.KeyUpdates) != 1 {
		t.Fatalf("want one restore for child 10, got %+v", res.KeyUpdates)
	}
	if !res.Complete() {
		t.Errorf("an unrelated earlier UPDATE is not a key chain; want Complete, got %v", res.Incomplete)
	}
}

// TestKeyUpdateCascade_endToEnd is the ON UPDATE counterpart of
// TestCascadeRecoverySpike, and the proof that the synthesis matches what InnoDB
// ACTUALLY does rather than what the manual says: against a real server, moving
// parent.id 1 -> 99 under ON UPDATE CASCADE rewrites the children's FK without
// logging a single child event, so a plain reversal of the parent restores
// parent but leaves child pointing at 99.
//
// Correctness gate (non-negotiable, same as the delete spike): CHECKSUM TABLE of
// parent and child captured just BEFORE the update must equal the checksum after
// applying the generated script.
func TestKeyUpdateCascade_endToEnd(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	sourceDB, sourceName := testutil.CreateTestDB(t)

	skipIfSourceLogsCascades(t, sourceDB)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE parent (
		id   INT PRIMARY KEY,
		name VARCHAR(64)
	) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE child (
		id      INT PRIMARY KEY,
		pid     INT,
		payload VARCHAR(64),
		CONSTRAINT fk_child FOREIGN KEY (pid) REFERENCES parent(id) ON UPDATE CASCADE
	) ENGINE=InnoDB`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	var beforeParent, beforeChild int64
	captureAndIndex(t, sourceDB, indexDB, resolver, sourceName, func() {
		testutil.MustExec(t, sourceDB, "INSERT INTO parent VALUES (1,'p1'),(2,'p2')")
		testutil.MustExec(t, sourceDB, "INSERT INTO child VALUES (10,1,'c10'),(11,1,'c11'),(12,2,'c12')")
		testutil.MustExec(t, sourceDB, "UPDATE child SET pid=2 WHERE id=11") // re-pointed → must NOT be touched
		// Second-granular binlog timestamps: keep the root strictly later so the
		// Until=rootTS window cleanly contains the setup above.
		time.Sleep(1100 * time.Millisecond)
		beforeParent = checksum(t, sourceDB, "parent")
		beforeChild = checksum(t, sourceDB, "child")
		// THE cascade: child 10 silently becomes pid=99, with no binlog event.
		testutil.MustExec(t, sourceDB, "UPDATE parent SET id=99 WHERE id=1")
	})

	// Sanity: the source really did cascade, and really did not log it.
	var pid int
	if err := sourceDB.QueryRow("SELECT pid FROM child WHERE id=10").Scan(&pid); err != nil {
		t.Fatalf("read child 10: %v", err)
	}
	if pid != 99 {
		t.Fatalf("InnoDB did not cascade the key update (child 10 pid=%d, want 99); the premise of this test is gone", pid)
	}
	eng := query.New(indexDB)
	upd := event.EventUpdate
	childEvents, err := eng.Fetch(ctx, query.Options{Schema: sourceName, Table: "child", PKValues: "10", EventType: &upd, Limit: 10})
	if err != nil {
		t.Fatalf("fetch child events: %v", err)
	}
	if len(childEvents) != 0 {
		t.Fatalf("the cascade must be INVISIBLE in the binlog; got %d child UPDATE events: %+v", len(childEvents), childEvents)
	}

	// The parent-key UPDATE we are reversing.
	roots, err := eng.Fetch(ctx, query.Options{Schema: sourceName, Table: "parent", EventType: &upd, Order: "ASC", Limit: 10})
	if err != nil {
		t.Fatalf("fetch parent updates: %v", err)
	}
	if len(roots) != 1 {
		t.Fatalf("want exactly the one parent key UPDATE, got %d: %+v", len(roots), roots)
	}

	fks, _, _, err := cascade.LoadCascadeFKsForParent(ctx, indexDB, sourceName, roots[0].EventTimestamp)
	if err != nil {
		t.Fatalf("LoadCascadeFKsForParent: %v", err)
	}
	res, err := cascade.SynthesizeVictims(ctx, eng, fks, roots, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if !res.Complete() {
		t.Fatalf("want a complete reconstruction, got Incomplete=%v", res.Incomplete)
	}
	if len(res.KeyUpdateParents) != 1 {
		t.Fatalf("the parent key UPDATE must be reversed alongside its children, got %+v", res.KeyUpdateParents)
	}
	if len(res.KeyUpdates) != 1 || res.KeyUpdates[0].PKValues != "10" {
		t.Fatalf("want exactly one FK restore, for child 10 (11 was re-pointed, 12 is another parent's); got %+v", res.KeyUpdates)
	}

	var script strings.Builder
	if _, err := cascaderecover.EmitSQL(&script, recoveryNew(t, indexDB, resolver),
		res.KeyUpdateParents, nil, res.KeyUpdates, resolver,
		cascaderecover.Header{Schema: sourceName, Table: "parent", Parents: len(res.KeyUpdateParents)}); err != nil {
		t.Fatalf("EmitSQL: %v", err)
	}
	applyFKOff(t, sourceName, script.String())

	if got := checksum(t, sourceDB, "parent"); got != beforeParent {
		t.Errorf("parent checksum after recovery = %d, want %d (pre-update state)", got, beforeParent)
	}
	if got := checksum(t, sourceDB, "child"); got != beforeChild {
		t.Errorf("child checksum after recovery = %d, want %d (pre-update state) — the ON UPDATE cascade was not fully reversed", got, beforeChild)
	}
}

// TestKeyUpdateCascade_endToEnd_fkInChildPK is the identifying-relationship
// variant of the test above, and the one shape where an `ON UPDATE CASCADE`
// restore can be self-contradictory: the child's FK column is itself part of
// its PRIMARY KEY (`PRIMARY KEY (pid, seq)` with `pid` REFERENCES parent(id)).
//
// The cascade moves the child's PK too, so a restore whose WHERE clause is
// built from the child's PRE-cascade image names `pid` twice with two different
// values (`WHERE pid = 1 AND seq = 1 AND pid = 99`) — a predicate no row can
// satisfy. It touches nothing while the synthesis reports a complete recovery,
// which is exactly the silent-no-op class this package refuses elsewhere.
// The checksum gate is what catches it.
func TestKeyUpdateCascade_endToEnd_fkInChildPK(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	sourceDB, sourceName := testutil.CreateTestDB(t)

	skipIfSourceLogsCascades(t, sourceDB)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE parent (
		id   INT PRIMARY KEY,
		name VARCHAR(64)
	) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE line (
		pid     INT NOT NULL,
		seq     INT NOT NULL,
		payload VARCHAR(64),
		PRIMARY KEY (pid, seq),
		CONSTRAINT fk_line FOREIGN KEY (pid) REFERENCES parent(id) ON UPDATE CASCADE
	) ENGINE=InnoDB`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	var beforeParent, beforeLine int64
	captureAndIndex(t, sourceDB, indexDB, resolver, sourceName, func() {
		testutil.MustExec(t, sourceDB, "INSERT INTO parent VALUES (1,'p1'),(2,'p2')")
		testutil.MustExec(t, sourceDB, "INSERT INTO line VALUES (1,1,'a'),(1,2,'b'),(2,1,'c')")
		// Second-granular binlog timestamps: keep the root strictly later so the
		// Until=rootTS window cleanly contains the setup above.
		time.Sleep(1100 * time.Millisecond)
		beforeParent = checksum(t, sourceDB, "parent")
		beforeLine = checksum(t, sourceDB, "line")
		// THE cascade: lines (1,1) and (1,2) silently become (99,1) and (99,2),
		// with no binlog event — and their PRIMARY KEY moved with them.
		testutil.MustExec(t, sourceDB, "UPDATE parent SET id=99 WHERE id=1")
	})

	// Sanity: the source really did cascade the PK, and really did not log it.
	var moved int
	if err := sourceDB.QueryRow("SELECT COUNT(*) FROM line WHERE pid=99").Scan(&moved); err != nil {
		t.Fatalf("count moved lines: %v", err)
	}
	if moved != 2 {
		t.Fatalf("InnoDB did not cascade the key update into the child PK (%d lines at pid=99, want 2); the premise of this test is gone", moved)
	}
	eng := query.New(indexDB)
	upd := event.EventUpdate
	lineEvents, err := eng.Fetch(ctx, query.Options{Schema: sourceName, Table: "line", EventType: &upd, Limit: 10})
	if err != nil {
		t.Fatalf("fetch line events: %v", err)
	}
	if len(lineEvents) != 0 {
		t.Fatalf("the cascade must be INVISIBLE in the binlog; got %d line UPDATE events: %+v", len(lineEvents), lineEvents)
	}

	roots, err := eng.Fetch(ctx, query.Options{Schema: sourceName, Table: "parent", EventType: &upd, Order: "ASC", Limit: 10})
	if err != nil {
		t.Fatalf("fetch parent updates: %v", err)
	}
	if len(roots) != 1 {
		t.Fatalf("want exactly the one parent key UPDATE, got %d: %+v", len(roots), roots)
	}

	fks, _, _, err := cascade.LoadCascadeFKsForParent(ctx, indexDB, sourceName, roots[0].EventTimestamp)
	if err != nil {
		t.Fatalf("LoadCascadeFKsForParent: %v", err)
	}
	res, err := cascade.SynthesizeVictims(ctx, eng, fks, roots, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if !res.Complete() {
		t.Fatalf("want a complete reconstruction, got Incomplete=%v", res.Incomplete)
	}
	if len(res.KeyUpdates) != 2 {
		t.Fatalf("want two FK restores, one per line under parent 1 (line (2,1) is another parent's); got %+v", res.KeyUpdates)
	}

	var script strings.Builder
	if _, err := cascaderecover.EmitSQL(&script, recoveryNew(t, indexDB, resolver),
		res.KeyUpdateParents, nil, res.KeyUpdates, resolver,
		cascaderecover.Header{Schema: sourceName, Table: "parent", Parents: len(res.KeyUpdateParents)}); err != nil {
		t.Fatalf("EmitSQL: %v", err)
	}
	applyFKOff(t, sourceName, script.String())

	if got := checksum(t, sourceDB, "parent"); got != beforeParent {
		t.Errorf("parent checksum after recovery = %d, want %d (pre-update state)", got, beforeParent)
	}
	if got := checksum(t, sourceDB, "line"); got != beforeLine {
		t.Errorf("line checksum after recovery = %d, want %d (pre-update state) — the restore's WHERE clause "+
			"named the FK column twice with contradictory values and touched nothing\n---\n%s", got, beforeLine, script.String())
	}
}
