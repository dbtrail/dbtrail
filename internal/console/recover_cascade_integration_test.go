//go:build integration

package console

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedCascadeConsole builds a Server over a fresh index holding a parent DELETE
// plus two child INSERTs that referenced it, and the fk_constraints row marking
// child.pid -> parent.id ON DELETE CASCADE. The cascade child deletes are NOT
// indexed — that is the blind spot the endpoint reconstructs. Extra Config is
// applied via the mutator (e.g. RBAC rules).
func seedCascadeConsole(t *testing.T, mutate func(*Config)) (*Server, string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	childTs := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	parentTs := h.Add(20 * time.Minute).Format("2006-01-02 15:04:05")

	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, childTs, nil,
		dbName, "child", 1 /*INSERT*/, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, childTs, nil,
		dbName, "child", 1 /*INSERT*/, "11", nil, nil, []byte(`{"id":11,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, parentTs, nil,
		dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk_child', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`,
		dbName, dbName)

	// A schema snapshot so the bundle's resolver is non-nil (PK columns known) —
	// required for the Phase-2 gate to engage and harmless to Phase-1 INSERT output.
	snapTs := h.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")

	cfg := Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true}
	if mutate != nil {
		mutate(&cfg)
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return srv, dbName
}

// TestIntegrationRecoverCascade_endToEnd drives POST /api/recover-cascade over a
// seeded cascade and checks the emitted SQL re-inserts the parent and its two
// cascade-deleted children inside the FK-checks wrapper, reports complete, and
// never leaks connection_id (text-only response, no event rows).
func TestIntegrationRecoverCascade_endToEnd(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, nil)

	rec, body := doReq(t, srv, "POST", "/api/recover-cascade", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverCascadeResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}

	for _, want := range []string{
		"SET FOREIGN_KEY_CHECKS=0;",
		"SET FOREIGN_KEY_CHECKS=1;",
		"Phase-1",
		"`" + dbName + "`.`parent`",
		"`" + dbName + "`.`child`",
	} {
		if !strings.Contains(resp.SQL, want) {
			t.Errorf("SQL missing %q\n---\n%s", want, resp.SQL)
		}
	}
	if c := strings.Count(resp.SQL, "`"+dbName+"`.`child`"); c != 2 {
		t.Errorf("want 2 child INSERTs, got %d\n---\n%s", c, resp.SQL)
	}
	if resp.VictimCount != 2 {
		t.Errorf("victim_count = %d, want 2", resp.VictimCount)
	}
	if !resp.Complete {
		t.Errorf("a clean cascade with no archives should be complete; incomplete=%v", resp.Incomplete)
	}
	// connection_id is no longer a redacted field on the events API (#701 D1),
	// but this assertion was never really about that boundary: the cascade
	// response carries SQL + counts only, never event rows, so the key has no
	// path onto the wire here regardless. Kept as a shape guard.
	if strings.Contains(string(body), "connection_id") {
		t.Errorf("connection_id leaked into the cascade response: %s", body)
	}

	// Capability is reported true (free tier, no profile).
	_, capsBody := doReq(t, srv, "GET", "/api/capabilities", "")
	var caps capabilitiesResponse
	if err := json.Unmarshal(capsBody, &caps); err != nil {
		t.Fatalf("decode caps: %v", err)
	}
	if !caps.RecoverCascade {
		t.Errorf("capability recover_cascade should be true, got: %s", capsBody)
	}
}

// TestIntegrationRecoverCascade_incompleteWithArchives: when archived partitions
// exist and no parent matches the live index, the result is flagged incomplete
// (the deleted parent may itself be archived — the dangerous "nothing found" case).
func TestIntegrationRecoverCascade_incompleteWithArchives(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, nil)
	// Register an archived partition (S3-resolvable: a key carrying bintrail_id=)
	// so the unconditional archive probe reports that archives physically exist.
	testutil.MustExec(t, srv.cm.boot.db, `INSERT INTO archive_state
		(bintrail_id, partition_name, local_path, s3_bucket, s3_key)
		VALUES ('bt', 'p_x', '', 'bucket', 'pfx/bintrail_id=bt/p_x.parquet')`)

	// A PK that matches no live parent → with archives present, that is a hard caveat.
	rec, body := doReq(t, srv, "POST", "/api/recover-cascade",
		`{"schema":"`+dbName+`","table":"parent","pk":"999"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverCascadeResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Complete {
		t.Errorf("result should be INCOMPLETE when archives exist and no parent matched")
	}
	if len(resp.Incomplete) == 0 {
		t.Errorf("incomplete[] should carry the archived-partition caveat")
	}
}

// TestIntegrationRecoverCascade_phase2HeaderWhenBaselineConfigured covers the
// console's Phase-2 GATING decision (the synthesis itself is the CLI's verbatim-
// ported, CLI-tested provider). With a baseline dir AND a resolver, the handler
// constructs the provider and the emitted SQL flips to the Phase-2 header — even
// though the empty baseline dir yields no extra rows (FindBaseline → no baseline
// → Phase-1). The capability advertises the sub-flag in lockstep.
func TestIntegrationRecoverCascade_phase2HeaderWhenBaselineConfigured(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, func(c *Config) {
		c.NoArchive = false         // required for baselineConfigured
		c.BaselineDir = t.TempDir() // empty dir → no baseline rows, provider still wired
	})

	rec, body := doReq(t, srv, "POST", "/api/recover-cascade", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverCascadeResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.Contains(resp.SQL, "Phase-2 baseline fallback ACTIVE") {
		t.Errorf("a baseline-configured bundle should emit the Phase-2 header\n---\n%s", resp.SQL)
	}

	// Capability sub-flag is true (baseline + resolver both present) — and equals
	// the handler's gate, closing the over-advertise seam.
	_, capsBody := doReq(t, srv, "GET", "/api/capabilities", "")
	var caps capabilitiesResponse
	if err := json.Unmarshal(capsBody, &caps); err != nil {
		t.Fatalf("decode caps: %v", err)
	}
	if !caps.RecoverCascadeBaseline {
		t.Errorf("recover_cascade_baseline should be true with baseline + resolver, got: %s", capsBody)
	}
}

// TestIntegrationRecover_autoCascade_endToEnd is the merge's core guarantee: a
// plain POST /api/recover on a foreign-key PARENT whose DELETE cascaded auto-
// detects the cascade and emits ONE combined script — the parent re-INSERT AND
// its two invisible children, inside the FK-checks wrapper — with cascade_detected
// + victim_count set. No separate tab, no extra request. The parent must appear
// exactly once (base rows carry it; synthesized victims are children-only).
func TestIntegrationRecover_autoCascade_endToEnd(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, nil)

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}

	if !resp.CascadeDetected {
		t.Errorf("cascade_detected should be true for a parent DELETE recover\n---\n%s", resp.SQL)
	}
	if resp.VictimCount != 2 {
		t.Errorf("victim_count = %d, want 2", resp.VictimCount)
	}
	for _, want := range []string{
		"SET FOREIGN_KEY_CHECKS=0;",
		"SET FOREIGN_KEY_CHECKS=1;",
		"recover (cascade-aware)",                   // the Combined preamble, not "recover-cascade"
		"Re-creates 2 cascade-deleted child row(s)", // Combined header counts the children
		"`" + dbName + "`.`parent`",
		"`" + dbName + "`.`child`",
	} {
		if !strings.Contains(resp.SQL, want) {
			t.Errorf("SQL missing %q\n---\n%s", want, resp.SQL)
		}
	}
	// Parent re-inserted exactly once (base rows), children exactly twice (victims).
	if c := strings.Count(resp.SQL, "`"+dbName+"`.`parent`"); c != 1 {
		t.Errorf("want the parent re-inserted once, got %d\n---\n%s", c, resp.SQL)
	}
	if c := strings.Count(resp.SQL, "`"+dbName+"`.`child`"); c != 2 {
		t.Errorf("want 2 child INSERTs, got %d\n---\n%s", c, resp.SQL)
	}
	// Same shape guard as the cascade endpoint above (#701 D1 note): the merged
	// response carries SQL + counts only, no event rows, so connection_id has
	// no path onto the wire here independent of the events-API boundary.
	if strings.Contains(string(body), "connection_id") {
		t.Errorf("connection_id leaked into the recover response: %s", body)
	}
}

// TestIntegrationRecover_autoCascade_skippedWhenNotParent locks the gate: a
// recover on the CHILD table (INSERTs only, not a cascade parent) takes the plain
// path — no cascade synthesis, no FK-checks wrapper — even though the table
// participates in the same FK. cascade_detected stays false.
func TestIntegrationRecover_autoCascade_skippedWhenNotParent(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, nil)

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"`+dbName+`","table":"child"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if resp.CascadeDetected {
		t.Errorf("cascade_detected must be false for a non-parent (child) recover")
	}
	if strings.Contains(resp.SQL, "SET FOREIGN_KEY_CHECKS=0;") {
		t.Errorf("plain recover should not emit the cascade FK-checks wrapper\n---\n%s", resp.SQL)
	}
}

// TestIntegrationRecover_autoCascade_rbacWarns: under an RBAC profile cascade
// synthesis is disabled (it cannot honor redaction), but a parent-DELETE recover
// must NOT silently emit a parent-only script as if whole — it warns. The recover
// itself still succeeds (unlike the cascade endpoint, which 403s).
func TestIntegrationRecover_autoCascade_rbacWarns(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, func(c *Config) {
		// A redact rule on an unrelated table is enough to activate the profile (the
		// guard checks presence, not scope), and leaves the parent recover working.
		c.RedactColumns = []query.SchemaTableColumn{{Schema: "app", Table: "child", Column: "pid"}}
	})

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if resp.CascadeDetected {
		t.Errorf("cascade synthesis must stay disabled under an RBAC profile")
	}
	var warned bool
	for _, w := range resp.Warnings {
		if strings.Contains(w, "RBAC redaction profile is active") {
			warned = true
		}
	}
	if !warned {
		t.Errorf("a parent-DELETE recover under RBAC must warn that cascade children are NOT included; warnings=%v", resp.Warnings)
	}
}

// TestIntegrationRecover_autoCascade_mixedEvents pins the combined path's
// data-integrity contract: a parent with BOTH a non-DELETE event (UPDATE) and a
// DELETE in the window. The combined script must reverse the UPDATE AND re-create
// the parent from the DELETE AND include the synthesized children — because
// cascadeRecover emits over the MERGED base rows (every event type), while the
// synthesis uses a DELETE-only parent fetch purely to discover children. A
// refactor that emitted over the synthesis's DELETE-only parents would silently
// drop the UPDATE reversal — a data-loss-shaped regression this test catches.
func TestIntegrationRecover_autoCascade_mixedEvents(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, nil)
	// A parent UPDATE between the children (h+10m) and the seeded parent DELETE
	// (h+20m), on the same pk=1.
	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	updTs := h.Add(15 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, srv.cm.boot.db, "binlog.000001", 250, 260, updTs, nil,
		dbName, "parent", 2 /*UPDATE*/, "1", []byte(`["status"]`),
		[]byte(`{"id":1,"status":"old"}`), []byte(`{"id":1,"status":"new"}`))

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if !resp.CascadeDetected || resp.VictimCount != 2 {
		t.Errorf("want cascade_detected + 2 victims, got detected=%v victims=%d", resp.CascadeDetected, resp.VictimCount)
	}
	// The non-DELETE base reversal must survive alongside the parent re-INSERT and
	// the 2 child INSERTs.
	if !strings.Contains(resp.SQL, "UPDATE `"+dbName+"`.`parent`") {
		t.Errorf("the parent UPDATE reversal was dropped from the combined script (regression: emitted over DELETE-only parents?)\n---\n%s", resp.SQL)
	}
	if !strings.Contains(resp.SQL, "INSERT INTO `"+dbName+"`.`parent`") {
		t.Errorf("the parent re-INSERT (from the DELETE) is missing\n---\n%s", resp.SQL)
	}
	if c := strings.Count(resp.SQL, "`"+dbName+"`.`child`"); c != 2 {
		t.Errorf("want 2 child INSERTs, got %d\n---\n%s", c, resp.SQL)
	}
}

// TestIntegrationRecover_autoCascade_gtidScoped is the regression test for #772:
// the combined recover path must scope its internal cascade parent-fetch to the
// SAME filters (here, GTID) as the triggering recover request, so it can never
// synthesize victims for a parent DELETE the operator did not ask to recover.
//
// Two independent parents are seeded on the same table, each deleted under its
// own transaction (distinct GTID) and each with two cascade-deleted children.
// Recovering by the FIRST parent's GTID alone (the natural "undo this one
// transaction" flow, no since/until) must synthesize victims ONLY for that
// parent's children. Before the fix, the internal parent-fetch dropped GTID and
// searched the entire table history, pulling in the second (unrelated) parent's
// deletion and synthesizing INSERTs for its children too — even though that
// parent itself is never re-created by this recover, orphaning them once
// FOREIGN_KEY_CHECKS is re-enabled.
func TestIntegrationRecover_autoCascade_gtidScoped(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	gtidA := "3e11fa47-71ca-11e1-9e33-c80aa9429562:5"
	gtidB := "3e11fa47-71ca-11e1-9e33-c80aa9429562:6"

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})

	// Transaction A: parent id=1 deleted under gtidA, cascading children pid=1
	// (id=10, id=11) — this is the ONLY transaction the recover request targets.
	childATs := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	parentATs := h.Add(20 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, childATs, nil,
		dbName, "child", 1 /*INSERT*/, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, childATs, nil,
		dbName, "child", 1 /*INSERT*/, "11", nil, nil, []byte(`{"id":11,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, parentATs, &gtidA,
		dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)

	// Transaction B: an UNRELATED parent id=2 deleted under gtidB, cascading its
	// own children pid=2 (id=20, id=21). The recover request below never
	// mentions gtidB, pk=2, or these children.
	childBTs := h.Add(11 * time.Minute).Format("2006-01-02 15:04:05")
	parentBTs := h.Add(21 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 400, 500, childBTs, nil,
		dbName, "child", 1 /*INSERT*/, "20", nil, nil, []byte(`{"id":20,"pid":2}`))
	testutil.InsertEvent(t, db, "binlog.000001", 500, 600, childBTs, nil,
		dbName, "child", 1 /*INSERT*/, "21", nil, nil, []byte(`{"id":21,"pid":2}`))
	testutil.InsertEvent(t, db, "binlog.000001", 600, 700, parentBTs, &gtidB,
		dbName, "parent", 3 /*DELETE*/, "2", nil, []byte(`{"id":2}`), nil)

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk_child', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`,
		dbName, dbName)

	snapTs := h.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}

	// Recover ONLY gtidA's transaction — no since/until, no pk.
	rec, body := doReq(t, srv, "POST", "/api/recover",
		`{"schema":"`+dbName+`","table":"parent","gtid":"`+gtidA+`"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if !resp.CascadeDetected {
		t.Errorf("cascade_detected should be true for a GTID-scoped parent DELETE recover")
	}
	if resp.VictimCount != 2 {
		t.Errorf("victim_count = %d, want 2 (only gtidA's children); a pre-fix regression would report 4 (both transactions' children)\n---\n%s", resp.VictimCount, resp.SQL)
	}
	for _, want := range []string{"VALUES (10, 1)", "VALUES (11, 1)"} {
		if !strings.Contains(resp.SQL, want) {
			t.Errorf("SQL missing %q (gtidA's own child)\n---\n%s", want, resp.SQL)
		}
	}
	for _, notWant := range []string{"VALUES (20, 2)", "VALUES (21, 2)", "VALUES (2)"} {
		if strings.Contains(resp.SQL, notWant) {
			t.Errorf("SQL must NOT include %q — gtidB's parent/children are outside the recover request's scope\n---\n%s", notWant, resp.SQL)
		}
	}
	if c := strings.Count(resp.SQL, "`"+dbName+"`.`child`"); c != 2 {
		t.Errorf("want exactly 2 child INSERTs (gtidA's own), got %d\n---\n%s", c, resp.SQL)
	}
	if c := strings.Count(resp.SQL, "`"+dbName+"`.`parent`"); c != 1 {
		t.Errorf("want the gtidA parent re-inserted exactly once (gtidB's parent must not appear), got %d\n---\n%s", c, resp.SQL)
	}
}

// TestIntegrationRecover_autoCascade_limitTruncated is the regression test for
// the residual #772 gap that matching the recover request's numeric Limit does
// NOT close: baseRows is an ALL-event-types fetch, while the internal cascade
// parent-fetch is DELETE-only. Over the identical window, non-DELETE rows on
// the table consume baseRows' Limit budget but never consume the DELETE-only
// fetch's budget — so a DELETE-only re-fetch using the SAME numeric Limit can
// rank (and include) a DELETE further into the window than baseRows' own
// cutoff reached, pulling in an unrelated parent the recover request never
// actually returned and synthesizing orphan children for it.
//
// Seeded on table "parent", in this exact chronological order, with the
// recover request's limit set to 3:
//  1. DELETE id=1         (oldest — excluded by baseRows' Limit=3 cutoff,
//     since #981 fetches DESC and keeps the NEWEST 3 of the 4 events)
//  2. INSERT id=99, noise
//  3. INSERT id=98, noise
//  4. DELETE id=2         (newest — kept)
//
// baseRows (Limit=3, effectively newest-first under #981, all event types)
// therefore contains only [noise, noise, DELETE id=2] — it never sees id=1's
// DELETE. A DELETE-only re-fetch with the same Limit=3 returns BOTH deletes.
// The combined recover must synthesize victims ONLY for id=2's children:
// id=1's parent is never re-created by this recover (it's outside baseRows),
// so synthesizing its children would orphan them once FOREIGN_KEY_CHECKS is
// re-enabled.
func TestIntegrationRecover_autoCascade_limitTruncated(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})

	childTs := h.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	del1Ts := h.Add(20 * time.Minute).Format("2006-01-02 15:04:05")
	noise1Ts := h.Add(21 * time.Minute).Format("2006-01-02 15:04:05")
	noise2Ts := h.Add(22 * time.Minute).Format("2006-01-02 15:04:05")
	del2Ts := h.Add(23 * time.Minute).Format("2006-01-02 15:04:05")

	// Children of BOTH parents (their own timestamps don't matter — the
	// parent-fetch is table="parent"-scoped and never sees table="child" rows).
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, childTs, nil,
		dbName, "child", 1 /*INSERT*/, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, childTs, nil,
		dbName, "child", 1 /*INSERT*/, "11", nil, nil, []byte(`{"id":11,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, childTs, nil,
		dbName, "child", 1 /*INSERT*/, "20", nil, nil, []byte(`{"id":20,"pid":2}`))
	testutil.InsertEvent(t, db, "binlog.000001", 400, 500, childTs, nil,
		dbName, "child", 1 /*INSERT*/, "21", nil, nil, []byte(`{"id":21,"pid":2}`))

	// The 4 "parent"-table events, in strict chronological order.
	testutil.InsertEvent(t, db, "binlog.000001", 500, 600, del1Ts, nil,
		dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)
	testutil.InsertEvent(t, db, "binlog.000001", 600, 700, noise1Ts, nil,
		dbName, "parent", 1 /*INSERT, noise*/, "99", nil, nil, []byte(`{"id":99}`))
	testutil.InsertEvent(t, db, "binlog.000001", 700, 800, noise2Ts, nil,
		dbName, "parent", 1 /*INSERT, noise*/, "98", nil, nil, []byte(`{"id":98}`))
	testutil.InsertEvent(t, db, "binlog.000001", 800, 900, del2Ts, nil,
		dbName, "parent", 3 /*DELETE*/, "2", nil, []byte(`{"id":2}`), nil)

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk_child', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`,
		dbName, dbName)

	snapTs := h.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}

	// Recover the whole table's history, capped at limit=3 — the exact numeric
	// Limit that (pre-#772-fix) let the internal DELETE-only parent-fetch see
	// id=1's DELETE even though baseRows' own Limit=3 cutoff (newest-first,
	// #981) never reaches it.
	rec, body := doReq(t, srv, "POST", "/api/recover",
		`{"schema":"`+dbName+`","table":"parent","limit":3}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if !resp.CascadeDetected {
		t.Errorf("cascade_detected should be true (baseRows contains id=2's DELETE)")
	}
	if resp.VictimCount != 2 {
		t.Errorf("victim_count = %d, want 2 (only id=2's children); a pre-fix regression would report 4 "+
			"(id=1's children too, even though id=1's own parent is never re-inserted)\n---\n%s", resp.VictimCount, resp.SQL)
	}
	for _, want := range []string{"VALUES (20, 2)", "VALUES (21, 2)"} {
		if !strings.Contains(resp.SQL, want) {
			t.Errorf("SQL missing %q (id=2's own child)\n---\n%s", want, resp.SQL)
		}
	}
	for _, notWant := range []string{"VALUES (10, 1)", "VALUES (11, 1)"} {
		if strings.Contains(resp.SQL, notWant) {
			t.Errorf("SQL must NOT include %q — id=1's parent fell outside baseRows' Limit-truncated "+
				"scope, so its children would be orphaned\n---\n%s", notWant, resp.SQL)
		}
	}
	if strings.Contains(resp.SQL, "INSERT INTO `"+dbName+"`.`parent` (`id`) VALUES (1)") {
		t.Errorf("SQL must NOT re-insert parent id=1 — it is outside this recover's Limit-truncated scope\n---\n%s", resp.SQL)
	}
	if c := strings.Count(resp.SQL, "`"+dbName+"`.`child`"); c != 2 {
		t.Errorf("want exactly 2 child INSERTs (id=2's own), got %d\n---\n%s", c, resp.SQL)
	}
}

// TestIntegrationRecover_autoCascade_setNull covers the SET NULL arm of the
// combined path: a parent whose child FK is ON DELETE SET NULL. Recover on the
// parent must fold an idempotent guarded UPDATE (… AND fk IS NULL) into the same
// FK-checks-wrapped script and report set_null_count, with zero victims (a
// SET NULL child survives — it is restored, not re-inserted). Self-seeded (not
// seedCascadeConsole, whose CASCADE counts other tests assert).
func TestIntegrationRecover_autoCascade_setNull(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	childTs := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	parentTs := h.Add(20 * time.Minute).Format("2006-01-02 15:04:05")

	// One child row pointing at parent.id=1 via a SET NULL FK column `pid`, plus
	// the parent DELETE (the cascade SET-NULL update InnoDB ran is NOT indexed).
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, childTs, nil,
		dbName, "snchild", 1 /*INSERT*/, "20", nil, nil, []byte(`{"id":20,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, parentTs, nil,
		dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk_snchild', ?, 'snchild', 'pid', 1, ?, 'parent', 'id', 'SET NULL', 'RESTRICT')`,
		dbName, dbName)

	snapTs := h.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "snchild", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "snchild", "pid", 2, "", "int", "YES")

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if !resp.CascadeDetected {
		t.Errorf("cascade_detected should be true for a SET NULL parent recover")
	}
	if resp.SetNullCount != 1 {
		t.Errorf("set_null_count = %d, want 1", resp.SetNullCount)
	}
	if resp.VictimCount != 0 {
		t.Errorf("victim_count = %d, want 0 (a SET NULL child survives — restored, not re-inserted)", resp.VictimCount)
	}
	for _, want := range []string{
		"SET FOREIGN_KEY_CHECKS=0;",
		"SET FOREIGN_KEY_CHECKS=1;",
		"`" + dbName + "`.`parent`",  // the parent re-INSERT
		"`" + dbName + "`.`snchild`", // the SET NULL restore UPDATE
		"IS NULL",                    // the idempotent guard
	} {
		if !strings.Contains(resp.SQL, want) {
			t.Errorf("SQL missing %q\n---\n%s", want, resp.SQL)
		}
	}
}

// TestIntegrationRecover_autoCascade_skippedForPostgres locks the dialect gate:
// cascade auto-detection is a MySQL/MariaDB binlog blind-spot fix. A PostgreSQL-
// flavored index captures cascade deletes as real events (no blind spot), so a
// parent recover takes the plain path — no synthesis, no misleading "0 victims".
func TestIntegrationRecover_autoCascade_skippedForPostgres(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, nil)
	// Stamp the index as PostgreSQL-sourced (DialectForIndex reads stream_state.flavor).
	testutil.MustExec(t, srv.cm.boot.db,
		`INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id)
		 VALUES (1, 'gtid', 'postgres', UTC_TIMESTAMP(), 1)
		 ON DUPLICATE KEY UPDATE flavor='postgres'`)

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if resp.CascadeDetected {
		t.Errorf("cascade auto-detection must be skipped for a PG-flavored index")
	}
	if strings.Contains(resp.SQL, "SET FOREIGN_KEY_CHECKS=0;") {
		t.Errorf("PG recover should not emit the MySQL cascade FK-checks wrapper\n---\n%s", resp.SQL)
	}
}

// TestIntegrationRecoverCascade_refusedUnderProfile: an active redact/deny
// profile both flips the capability to false and makes the endpoint 403 (cascade
// victim synthesis cannot honor redaction — the leak guard).
func TestIntegrationRecoverCascade_refusedUnderProfile(t *testing.T) {
	// The guard fires on any redact/deny rule (it checks presence, not scope), so
	// the schema literal here need not match the generated test DB name.
	srv, dbName := seedCascadeConsole(t, func(c *Config) {
		c.NoArchive = false
		c.RedactColumns = []query.SchemaTableColumn{{Schema: "app", Table: "child", Column: "pid"}}
	})

	rec, body := doReq(t, srv, "POST", "/api/recover-cascade", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 403 {
		t.Fatalf("status = %d, want 403 (body=%s)", rec.Code, body)
	}

	_, capsBody := doReq(t, srv, "GET", "/api/capabilities", "")
	var caps capabilitiesResponse
	if err := json.Unmarshal(capsBody, &caps); err != nil {
		t.Fatalf("decode caps: %v", err)
	}
	if caps.RecoverCascade {
		t.Errorf("capability recover_cascade must be false under an RBAC profile, got: %s", capsBody)
	}
}

// seedCascadeConsoleOversized is seedCascadeConsole's #849 sibling: the same
// parent-DELETE + child-INSERT + FK-CASCADE shape, except the child table
// carries MANY moderate-sized rows (oversizedChildCount rows of
// oversizedChildBlobBytes each, all referencing parent id=1) instead of a
// couple of small ones. The cascade engine copies each INSERT's row_after
// into the synthesized victim's RowBefore (internal/cascade/cascade.go:553,
// "last known state → INSERT target"), so this flows straight into cascade
// victim payload EstimateScriptBytes sums — pushing the COMBINED (parent +
// victims) script over recoverMaxScriptBytes (32 MiB) while the parent DELETE
// alone (`{"id":1}`) stays tiny.
//
// Deliberately MANY moderate rows, not one giant row: a single row wider than
// MySQL's default sort_buffer_size (256 KB) makes the cascade engine's own
// victim-lookup query — a JSON_EXTRACT filter combined with a
// ROW_NUMBER() OVER (PARTITION BY ...) window (eng.Fetch with LimitPerPK) —
// die with ER_OUT_OF_SORTMEMORY (1038) before cascade synthesis ever runs, an
// early version of this fixture hit exactly that. internal/query/query.go's
// late-materialization fix (narrow-key sort + join-back) doesn't cover the
// cascade engine's own query, so the fixture works around the cliff by
// staying under it — oversizedChildBlobBytes is comfortably below 256 KB —
// rather than raising a GLOBAL MySQL setting that would leak into whatever
// else shares this test server (concurrent packages, other tests) and
// silently mask the class of bug internal/query/query.go's fix addresses if
// left too high. All oversizedChildCount rows stay far under
// cascade.CandidateLimit (1000, the per-FK victim query LIMIT).
const (
	oversizedChildCount     = 260
	oversizedChildBlobBytes = 160 << 10 // 160 KiB/row: 260 * 160 KiB ≈ 40.6 MiB combined, over the 32 MiB budget; each row well under MySQL's default 256 KiB sort_buffer_size
)

func seedCascadeConsoleOversized(t *testing.T) (*Server, string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	childTs := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	parentTs := h.Add(20 * time.Minute).Format("2006-01-02 15:04:05")

	blob := strings.Repeat("x", oversizedChildBlobBytes)
	for i := 0; i < oversizedChildCount; i++ {
		childID := 100 + i
		rowAfter := []byte(fmt.Sprintf(`{"id":%d,"pid":1,"blob":"%s"}`, childID, blob))
		testutil.InsertEvent(t, db, "binlog.000001", uint64(100+i*10), uint64(100+i*10+5), childTs, nil,
			dbName, "child", 1 /*INSERT*/, fmt.Sprintf("%d", childID), nil, nil, rowAfter)
	}
	testutil.InsertEvent(t, db, "binlog.000001", 100000, 100100, parentTs, nil,
		dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk_child', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`,
		dbName, dbName)

	snapTs := h.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")

	cfg := Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true}
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return srv, dbName
}

// TestIntegrationRecoverCascade_overByteBudget pins recoverMaxScriptBytes
// wiring on the EXPLICIT endpoint (handleRecoverCascade's own
// recovery.NewForDialect(...) + gen.SetMaxScriptBytes(recoverMaxScriptBytes)
// in recover_cascade.go, #849 item 3): a combined parent+victims script far
// over budget must refuse with the same actionable 422 the plain /api/recover
// path uses (writeRecoverError), not render at the Generator's CLI-sized
// 2 GiB zero-config default.
func TestIntegrationRecoverCascade_overByteBudget(t *testing.T) {
	srv, dbName := seedCascadeConsoleOversized(t)

	rec, body := doReq(t, srv, "POST", "/api/recover-cascade", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422, body = %s", rec.Code, body)
	}
	var errBody map[string]string
	if err := json.Unmarshal(body, &errBody); err != nil {
		t.Fatalf("decode error body: %v (body=%s)", err, body)
	}
	msg := errBody["error"]
	for _, want := range []string{"MiB budget", "Narrow the recovery filter", "bintrail recover"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q: %s", want, msg)
		}
	}
	if strings.Contains(msg, "0 = unlimited") {
		t.Errorf("error message must not leak the CLI-only '0 = unlimited' phrasing: %s", msg)
	}
}

// TestIntegrationRecover_autoCascade_overByteBudget pins recoverMaxScriptBytes
// wiring on the AUTO-DETECTED cascade path (cascadeRecover's own
// gen.SetMaxScriptBytes(recoverMaxScriptBytes) in recover_cascade.go, #849
// item 3) together with the api.go warning fix (#849 item 2, the code-review
// follow-up): a combined script over budget must NOT render at 2 GiB, must
// degrade to the plain (parent-only) recovery — not fail the whole request —
// and the warning explaining why must say the budget was the reason (not
// "cascade synthesis failed", which would misdiagnose a refusal that happened
// AFTER synthesis succeeded) and must not leak the CLI-only "0 = unlimited"
// phrasing that has no console equivalent.
func TestIntegrationRecover_autoCascade_overByteBudget(t *testing.T) {
	srv, dbName := seedCascadeConsoleOversized(t)

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200 (over-budget cascade degrades to plain recover), body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if resp.CascadeDetected {
		t.Errorf("cascade_detected should be false: the combined script was over budget, so recover degraded to the plain path\n---\n%s", resp.SQL)
	}
	if !strings.Contains(resp.SQL, "`"+dbName+"`.`parent`") {
		t.Errorf("the plain fallback should still re-create the parent\n---\n%s", resp.SQL)
	}
	if strings.Contains(resp.SQL, "`"+dbName+"`.`child`") {
		t.Errorf("the plain fallback must NOT include the cascade-synthesized (oversized) children\n---\n%s", resp.SQL)
	}
	joined := strings.Join(resp.Warnings, " | ")
	for _, want := range []string{"combined script would hold", "MiB budget", "re-creates the parent only", "Narrow the recovery filter"} {
		if !strings.Contains(joined, want) {
			t.Errorf("warnings missing %q: %v", want, resp.Warnings)
		}
	}
	if strings.Contains(joined, "0 = unlimited") {
		t.Errorf("warnings must not leak the CLI-only '0 = unlimited' phrasing: %v", resp.Warnings)
	}
	if strings.Contains(joined, "Cascade synthesis failed") {
		t.Errorf("a budget refusal after successful synthesis must not be misdiagnosed as a synthesis failure: %v", resp.Warnings)
	}
}

// TestIntegrationRecover_autoCascade_noopUpdateFallsBackToPlain pins the
// auto-detect fallback. rowsContainCascadeTriggerOn's UPDATE arm is deliberately
// coarse — it cannot tell whether an UPDATE moved a referenced key without the
// FK graph — so ANY update undo on a table with an ON UPDATE child routes
// through the cascade path. When the synthesis then (correctly) rejects it, the
// response must NOT claim CASCADE with all counts zero, and above all must not
// hand back an ordinary reversal silently wrapped in SET FOREIGN_KEY_CHECKS=0/1:
// the operator asked for a plain recover and would get FK validation disabled
// without ever being told.
func TestIntegrationRecover_autoCascade_noopUpdateFallsBackToPlain(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	childTs := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	parentTs := h.Add(20 * time.Minute).Format("2006-01-02 15:04:05")

	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, childTs, nil,
		dbName, "child", 1 /*INSERT*/, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	// The parent UPDATE touches `name` only — the referenced key (id) never moved,
	// so InnoDB cascaded nothing.
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, parentTs, nil,
		dbName, "parent", 2 /*UPDATE*/, "1", nil,
		[]byte(`{"id":1,"name":"before"}`), []byte(`{"id":1,"name":"after"}`))

	// An ON UPDATE CASCADE edge: the table IS a cascade parent, which is what
	// makes the coarse arm route the undo through synthesis in the first place.
	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk_child', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'RESTRICT', 'CASCADE')`,
		dbName, dbName)

	snapTs := h.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if resp.CascadeDetected {
		t.Errorf("nothing cascaded (the UPDATE never moved the referenced key), so the response must not claim CASCADE: "+
			"victims=%d set_null=%d key_restores=%d", resp.VictimCount, resp.SetNullCount, resp.KeyRestoreCount)
	}
	if strings.Contains(resp.SQL, "FOREIGN_KEY_CHECKS") {
		t.Errorf("a plain UPDATE undo must not be wrapped in SET FOREIGN_KEY_CHECKS=0/1\n---\n%s", resp.SQL)
	}
	// The plain reversal itself must still be there.
	if !strings.Contains(resp.SQL, "`name` = 'before'") {
		t.Errorf("the plain UPDATE reversal is missing\n---\n%s", resp.SQL)
	}
}

// TestIntegrationRecoverCascade_archivesOutsideWindowKeepBaseline pins the
// console wiring of the #1615 gate: an archived partition exists, the baseline
// snapshot is dated inside the hour the live index holds, and the parent was
// deleted later in that hour. The window is live-contiguous, so the untouched
// baseline child (12) is recovered alongside the two binlog children and the
// run is complete. Before the fix the archive's existence alone skipped it.
func TestIntegrationRecoverCascade_archivesOutsideWindowKeepBaseline(t *testing.T) {
	dir := t.TempDir()
	srv, dbName := seedCascadeConsole(t, func(c *Config) {
		c.BaselineDir = dir
		c.NoArchive = false // baselineConfigured folds in NoArchive; the fixture defaults it on
	})
	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour) // the fixture's live hour
	writeChildBaselineParquet(t, dir, h.Add(5*time.Minute).Format("2006-01-02T15-04-05Z"), dbName,
		[][]string{{"10", "1"}, {"11", "1"}, {"12", "1"}}, nil)
	testutil.MustExec(t, srv.cm.boot.db, `INSERT INTO archive_state
		(bintrail_id, partition_name, local_path, s3_bucket, s3_key)
		VALUES ('bt', 'p_2026010100', '', 'bucket', 'pfx/bintrail_id=bt/p_2026010100.parquet')`)

	rec, body := doReq(t, srv, "POST", "/api/recover-cascade", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverCascadeResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if resp.VictimCount != 3 || !strings.Contains(resp.SQL, "12, 1") {
		t.Errorf("want the untouched baseline child 12 recovered (victim_count 3), got %d\n---\n%s", resp.VictimCount, resp.SQL)
	}
	if !resp.Complete {
		t.Errorf("a live-contiguous window must be complete despite an unrelated archive; incomplete=%v", resp.Incomplete)
	}
}
