//go:build integration

package cascade_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// fakeBaseline is a canned BaselineProvider — it lets the Phase-1/Phase-2 merge
// logic be tested without a real Parquet snapshot (the binlog side still uses a
// real but possibly-empty index DB).
type fakeBaseline struct {
	snap  time.Time
	rows  []cascade.BaselineRow
	ok    bool
	trunc bool
	err   error
	calls int
	stale string           // #618: canned StaleMessage, mirrors reconstruct.StaleWarning.Message
	pos   *query.BinlogPos // #797: canned SincePos, the baseline's recorded binlog position
}

func (f *fakeBaseline) BaselineChildren(_ context.Context, _, _, _, _ string, _ time.Time, _ int) (cascade.BaselineLookup, bool, error) {
	f.calls++
	if f.err != nil {
		return cascade.BaselineLookup{}, false, f.err
	}
	return cascade.BaselineLookup{SnapshotTime: f.snap, Rows: f.rows, Truncated: f.trunc, StaleMessage: f.stale, SincePos: f.pos}, f.ok, nil
}

func cascadeFK(schema string) []cascade.CascadeFK {
	return []cascade.CascadeFK{{
		Schema: schema, Table: "child", ConstraintName: "fk", Column: "pid",
		ReferencedSchema: schema, ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "CASCADE",
	}}
}

func parentDelete(schema string, at time.Time) []query.ResultRow {
	return []query.ResultRow{{
		SchemaName: schema, TableName: "parent", EventType: 3 /* DELETE */, PKValues: "1",
		RowBefore: map[string]any{"id": json.Number("1")}, EventTimestamp: at,
	}}
}

func victimKeys(rows []query.ResultRow) map[string]bool {
	m := map[string]bool{}
	for _, r := range rows {
		m[r.TableName+":"+r.PKValues] = true
	}
	return m
}

// TestPhase2_untouchedBaselineChildRecovered is the headline correctness case:
// a child present in the baseline with NO binlog event in the window (the gap
// Phase-1 misses) is recovered from its baseline row.
func TestPhase2_untouchedBaselineChildRecovered(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)
	T := time.Now().UTC()

	fb := &fakeBaseline{
		ok:   true,
		snap: T.Add(-2 * time.Hour),
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1), "payload": "keep"}}},
	}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if !res.Complete() {
		t.Errorf("a fully-covered baseline cascade should be complete, got Incomplete=%v", res.Incomplete)
	}
	if k := victimKeys(res.Victims); len(res.Victims) != 1 || !k["child:10"] {
		t.Fatalf("want exactly child:10 from baseline, got %v", res.Victims)
	}
	if res.Victims[0].RowBefore["payload"] != "keep" {
		t.Errorf("baseline victim should carry the baseline row, got %v", res.Victims[0].RowBefore)
	}
}

// TestPhase2_reparentedNotResurrected pins the touched-exclusion: a child that
// was re-parented away (X→Y) after the baseline appears in the binlog scan and
// is correctly filtered; the baseline augmentation must NOT resurrect it from
// its stale baseline (fk=X) state.
func TestPhase2_reparentedNotResurrected(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-30 * time.Minute).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	// child 10: inserted pointing at parent 1, then re-parented to parent 2.
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1 /*INSERT*/, "10", nil, nil, []byte(`{"id":10,"pid":1,"payload":"x"}`))
	testutil.InsertEvent(t, db, "b.000001", 20, 30, ts, nil, dbName, "child", 2 /*UPDATE*/, "10", nil, []byte(`{"id":10,"pid":1,"payload":"x"}`), []byte(`{"id":10,"pid":2,"payload":"x"}`))

	fb := &fakeBaseline{
		ok:   true,
		snap: h.Add(-time.Hour),
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1)}}},
	}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("re-parented child must NOT be a victim (neither binlog nor resurrected from baseline), got %v", res.Victims)
	}
}

// TestPhase2_windowWidenedToSnapshot pins the window widening: a child inserted
// after the baseline but BEFORE the (short) lookback window is still caught,
// because a configured baseline widens the binlog scan to the snapshot time.
func TestPhase2_windowWidenedToSnapshot(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-2 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	// child 11 inserted ~2h before T — outside a 1-minute lookback, after the snapshot.
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1 /*INSERT*/, "11", nil, nil, []byte(`{"id":11,"pid":1,"payload":"y"}`))

	fb := &fakeBaseline{ok: true, snap: h.Add(-time.Hour)} // child 11 NOT in baseline (inserted after)
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb, Lookback: time.Minute})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if k := victimKeys(res.Victims); len(res.Victims) != 1 || !k["child:11"] {
		t.Fatalf("widened window should catch child:11 (insert after snapshot, before lookback), got %v", res.Victims)
	}
}

// TestPhase2_truncationSkipsBaseline pins the stale-resurrection guard: when the
// binlog scan truncates, `touched` is incomplete, so baseline augmentation is
// skipped (a truncated-out re-parented child must not be resurrected) and flagged.
func TestPhase2_truncationSkipsBaseline(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-30 * time.Minute).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	testutil.InsertEvent(t, db, "b.000001", 20, 30, ts, nil, dbName, "child", 1, "11", nil, nil, []byte(`{"id":11,"pid":1}`))

	// stale is set here too (#618 review finding, mirrored from
	// TestPhase2_archivesSkipBaseline): the binlog-truncation skip means
	// augmentation never ran, so the stale-baseline warning must not fire
	// either — asserted below.
	fb := &fakeBaseline{ok: true, snap: h.Add(-time.Hour),
		rows:  []cascade.BaselineRow{{PKValues: "12", Row: map[string]any{"id": int64(12), "pid": int64(1)}}},
		stale: "baseline for " + dbName + ".child is stale: the table is absent from the newest snapshot"}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb, CandidateLimit: 1})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "baseline augmentation") {
		t.Errorf("binlog truncation must skip+flag baseline augmentation; Incomplete=%v", res.Incomplete)
	}
	if len(res.Warnings) != 0 {
		t.Errorf("the stale-baseline warning must NOT fire when binlog truncation skipped augmentation entirely; got Warnings=%v", res.Warnings)
	}
	// The baseline child 12 must NOT have been added under truncation.
	if victimKeys(res.Victims)["child:12"] {
		t.Errorf("baseline child must not be added when the binlog scan truncated")
	}
}

// TestPhase2_noBaselineCoverageFlagged: a provider configured but with no
// baseline for the table flags incompleteness; a provider error flags it too.
func TestPhase2_noBaselineCoverageFlagged(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)
	T := time.Now().UTC()

	notCovered := &fakeBaseline{ok: false}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: notCovered})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "no baseline covers") {
		t.Errorf("uncovered table must flag incompleteness; Incomplete=%v", res.Incomplete)
	}

	failing := &fakeBaseline{err: context.DeadlineExceeded}
	res2, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: failing})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if res2.Complete() || !strings.Contains(strings.Join(res2.Incomplete, " "), "baseline lookup failed") {
		t.Errorf("baseline lookup error must flag incompleteness; Incomplete=%v", res2.Incomplete)
	}
}

// TestPhase2_staleBaselineWarnsButStaysComplete pins #618's CORRECTED design
// (a review of the original #618 PR found the engine had routed this signal
// through Result.Incomplete, which both renderers treat as data loss — but
// the issue's own "why it's not urgent" analysis says no rows are lost in
// this scenario, only the advisory itself): a provider that covers the child
// table but fell back to an older snapshot (StaleMessage non-empty,
// mirroring reconstruct.StaleWarning.Message on a #466 fallback) must surface
// that as a "baseline-stale:<schema>.<table>" entry in Result.WARNINGS —
// never Incomplete — so Complete() stays true and a caller's exit code is
// unaffected. A non-stale lookup (empty StaleMessage) must not add the
// warning. Two parent roots pin the dedup the issue asked for (addWarning
// keyed on "baseline-stale:<schema>.<table>", not per-parent):
// BaselineChildren is called once per root, but the warning must appear
// exactly once.
func TestPhase2_staleBaselineWarnsButStaysComplete(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)
	T := time.Now().UTC()

	twoParents := append(parentDelete(dbName, T), query.ResultRow{
		SchemaName: dbName, TableName: "parent", EventType: 3, /* DELETE */
		PKValues: "2", RowBefore: map[string]any{"id": json.Number("2")}, EventTimestamp: T,
	})

	staleMsg := "baseline for " + dbName + ".child is stale: the table is absent from the newest snapshot"
	stale := &fakeBaseline{
		ok:    true,
		snap:  T.Add(-2 * time.Hour),
		rows:  []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1), "payload": "keep"}}},
		stale: staleMsg,
	}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), twoParents,
		cascade.Options{Baseline: stale})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if stale.calls != 2 {
		t.Fatalf("BaselineChildren calls = %d, want 2 (one per parent root) for the dedup below to be meaningful", stale.calls)
	}
	// The stale-fallback warning must not suppress the recovered victim itself —
	// staleness is a transparency signal, not a coverage gap (see the issue's
	// "why it's not urgent" analysis: no rows are lost, only the advisory).
	if k := victimKeys(res.Victims); !k["child:10"] {
		t.Fatalf("stale baseline lookup should still recover child:10, got %v", res.Victims)
	}
	// The core correction: this is COMPLETE, not Incomplete.
	if !res.Complete() {
		t.Fatalf("a stale-but-fully-augmented baseline fallback must stay Complete (staleness is advisory, not data loss); got Incomplete=%v", res.Incomplete)
	}
	if len(res.Incomplete) != 0 {
		t.Fatalf("the stale-baseline signal must NEVER appear in Incomplete, got %v", res.Incomplete)
	}
	count := 0
	for _, msg := range res.Warnings {
		if msg == staleMsg {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("want the stale-baseline warning deduped to exactly 1 entry despite %d BaselineChildren calls, got %d occurrences in Warnings=%v",
			stale.calls, count, res.Warnings)
	}

	// A non-stale lookup (empty StaleMessage) must not add the warning.
	fresh := &fakeBaseline{
		ok:   true,
		snap: T.Add(-2 * time.Hour),
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1), "payload": "keep"}}},
	}
	res2, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fresh})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if !res2.Complete() {
		t.Fatalf("a non-stale, fully-covered baseline cascade should stay Complete; got Incomplete=%v", res2.Incomplete)
	}
	if len(res2.Warnings) != 0 {
		t.Fatalf("a non-stale lookup must not add a warning, got Warnings=%v", res2.Warnings)
	}
}

// TestPhase2_staleBaselineSuppressedWhenNoRowsMatched pins the "gate" half of
// #618's correction: the review's second defect was that the stale-fallback
// signal fired even when the baseline provider covered the table but
// returned ZERO rows for this parent — i.e. the baseline never influenced the
// output at all. StaleMessage must be ignored in that case (no Warnings
// entry), same as it is ignored when augmentation is skipped for truncation
// or archives (see TestPhase2_truncationSkipsBaseline /
// TestPhase2_archivesSkipBaseline).
func TestPhase2_staleBaselineSuppressedWhenNoRowsMatched(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)
	T := time.Now().UTC()

	stale := &fakeBaseline{ok: true, snap: T.Add(-2 * time.Hour), stale: "baseline for " + dbName + ".child is stale"}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: stale})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Warnings) != 0 {
		t.Errorf("a covered-but-empty baseline lookup must not surface the stale warning (it never influenced the output), got Warnings=%v", res.Warnings)
	}
	if !res.Complete() {
		t.Errorf("an empty baseline lookup with no binlog events is trivially complete; got Incomplete=%v", res.Incomplete)
	}
}

// TestPhase2_archivesSkipBaseline pins the archive-gap guard (the #569 critical):
// when the index has archived partitions, the live [snapshot,T] scan may be
// gapped, so baseline augmentation is SKIPPED (a child re-parented/deleted in an
// archived partition must not be resurrected from its stale baseline row).
//
// The fake provider ALSO carries a non-empty StaleMessage here (a #618 review
// finding: this test previously passed only because StaleMessage happened to
// be empty, leaving the archives-present + stale-baseline interaction
// untested in both directions). Augmentation being skipped means the baseline
// never influenced the output, so the stale-fallback warning must NOT fire
// either — only the pre-existing "archived" Incomplete caveat should.
func TestPhase2_archivesSkipBaseline(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)
	T := time.Now().UTC()

	fb := &fakeBaseline{ok: true, snap: T.Add(-2 * time.Hour),
		rows:  []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1)}}},
		stale: "baseline for " + dbName + ".child is stale: the table is absent from the newest snapshot"}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb, ArchivesPresent: true})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("baseline augmentation must be skipped when archives present; got %v", res.Victims)
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "archived") {
		t.Errorf("archive-gap skip must flag incompleteness; Incomplete=%v", res.Incomplete)
	}
	if len(res.Warnings) != 0 {
		t.Errorf("the stale-baseline warning must NOT fire when archives-present skipped augmentation entirely (the baseline never influenced the output); got Warnings=%v", res.Warnings)
	}
}

// TestPhase2_baselineTruncationFlagged pins the baseTrunc branch: the baseline
// scan hit its cap (binlog NOT truncated) → the capped rows are still emitted but
// the run is flagged incomplete.
func TestPhase2_baselineTruncationFlagged(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)
	T := time.Now().UTC()

	fb := &fakeBaseline{ok: true, trunc: true, snap: T.Add(-2 * time.Hour),
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1)}}}}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if k := victimKeys(res.Victims); len(res.Victims) != 1 || !k["child:10"] {
		t.Errorf("capped baseline rows must still be emitted, got %v", res.Victims)
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "baseline children") {
		t.Errorf("baseline truncation must be flagged; Incomplete=%v", res.Incomplete)
	}
}

// setNullFK is cascadeFK's ON DELETE SET NULL sibling for the Phase-2 tests.
func setNullFK(schema string) []cascade.CascadeFK {
	return []cascade.CascadeFK{{
		Schema: schema, Table: "child", ConstraintName: "fk", Column: "pid",
		ReferencedSchema: schema, ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "SET NULL",
	}}
}

// TestPhase2_setNullUntouchedBaselineChild: a child of a SET NULL FK present in
// the baseline but untouched in the window becomes a SetNullRestore (its FK is
// nulled, the row survives), NOT a delete victim — the SET NULL analogue of
// TestPhase2_untouchedBaselineChildRecovered, exercising the Phase-2 br.Row path.
func TestPhase2_setNullUntouchedBaselineChild(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)
	T := time.Now().UTC()

	fb := &fakeBaseline{
		ok:   true,
		snap: T.Add(-2 * time.Hour),
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1), "payload": "keep"}}},
	}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, setNullFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("SET NULL must produce no delete victims, got %v", res.Victims)
	}
	if len(res.SetNullRows) != 1 || res.SetNullRows[0].PKValues != "10" || res.SetNullRows[0].Column != "pid" {
		t.Fatalf("want one baseline SET NULL restore for child:10/pid, got %+v", res.SetNullRows)
	}
	if res.SetNullRows[0].Row["payload"] != "keep" {
		t.Errorf("baseline SET NULL restore should carry the baseline row, got %v", res.SetNullRows[0].Row)
	}
	if !res.Complete() {
		t.Errorf("fully-covered baseline SET NULL should be complete, got %v", res.Incomplete)
	}
}

// contiguous / notContiguous / probeFails are canned Options.LiveWindowContiguous
// answers for the #1615 gate tests; each records the window it was asked about.
type liveWindowProbe struct {
	ok    bool
	err   error
	calls int
	since time.Time
	until time.Time
}

func (p *liveWindowProbe) probe(_ context.Context, since, until time.Time) (bool, error) {
	p.calls++
	p.since, p.until = since, until
	return p.ok, p.err
}

// TestPhase2_archivesOutsideWindowKeepBaseline is the #1615 headline: archives
// exist somewhere in the index, but the live partitions hold every hour of
// [snapshot, T], so the "may gap the window" premise is false and baseline
// augmentation must run. Before the fix the bare existence of an archive
// skipped it and the untouched child was lost.
func TestPhase2_archivesOutsideWindowKeepBaseline(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)
	T := time.Now().UTC()
	snap := T.Add(-30 * time.Minute)

	fb := &fakeBaseline{ok: true, snap: snap,
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1), "payload": "keep"}}}}
	probe := &liveWindowProbe{ok: true}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb, ArchivesPresent: true, LiveWindowContiguous: probe.probe})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if k := victimKeys(res.Victims); len(res.Victims) != 1 || !k["child:10"] {
		t.Fatalf("archives outside the window must not skip augmentation; want child:10 from baseline, got %v", res.Victims)
	}
	if !res.Complete() {
		t.Errorf("a live-contiguous window is complete, got Incomplete=%v", res.Incomplete)
	}
	if probe.calls != 1 || !probe.since.Equal(snap) || !probe.until.Equal(T) {
		t.Errorf("the probe must be asked about exactly [snapshot, T]; calls=%d since=%s until=%s", probe.calls, probe.since, probe.until)
	}
}

// TestPhase2_archivesInWindowSkipBaselineButScanWidened is the shape that
// failed on stage (2026-09-11): the children were INSERTed 30 minutes before
// the parent DELETE, a 5-minute backup schedule put a snapshot BETWEEN the two,
// and an unrelated archive existed. The [snapshot, T] scan saw no INSERT and
// the gate then skipped the very baseline that held the rows: parent only.
//
// With the window genuinely gapped, augmentation still must be skipped — but
// the scan then falls back to the plain Phase-1 window [T-lookback, T], which
// is what the tool would search with no baseline at all, and catches the
// pre-snapshot INSERT from the live index.
func TestPhase2_archivesInWindowSkipBaselineButScanWidened(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-2 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1 /*INSERT*/, "10", nil, nil, []byte(`{"id":10,"pid":1,"payload":"seeded"}`))

	// Snapshot taken AFTER the insert (the 5-minute schedule), holding the row.
	fb := &fakeBaseline{ok: true, snap: T.Add(-30 * time.Minute),
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1), "payload": "seeded"}}}}
	probe := &liveWindowProbe{ok: false}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb, ArchivesPresent: true, LiveWindowContiguous: probe.probe})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if k := victimKeys(res.Victims); len(res.Victims) != 1 || !k["child:10"] {
		t.Fatalf("a gapped window must fall back to the Phase-1 lookback scan and find the pre-snapshot INSERT; got %v", res.Victims)
	}
	if joined := strings.Join(res.Incomplete, " "); res.Complete() || !strings.Contains(joined, "cannot serve every hour") {
		t.Errorf("a verified gap must be flagged with the verified-gap wording; Incomplete=%v", res.Incomplete)
	}
	if fb.calls == 0 {
		t.Errorf("the baseline must still be consulted (its snapshot time decides the gate)")
	}
}

// TestPhase2_deletedBeforeSnapshotNotResurrected pins why the widened scan is
// ONLY the fallback: when augmentation runs, the baseline is authoritative at
// its snapshot instant, so a child whose live INSERT predates the snapshot but
// which is ABSENT from the snapshot (deleted in between — by an unlogged
// cascade, or in an hour that later rotated) must not come back from that
// stale INSERT image. A scan that widened past the snapshot in augment mode
// would resurrect it.
func TestPhase2_deletedBeforeSnapshotNotResurrected(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-2 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1 /*INSERT*/, "10", nil, nil, []byte(`{"id":10,"pid":1}`))

	fb := &fakeBaseline{ok: true, snap: T.Add(-30 * time.Minute)} // row 10 gone by the snapshot
	probe := &liveWindowProbe{ok: true}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb, ArchivesPresent: true, LiveWindowContiguous: probe.probe})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("a child absent from the snapshot must not be resurrected from its pre-snapshot INSERT; got %v", res.Victims)
	}
	if !res.Complete() {
		t.Errorf("nothing was skipped, got Incomplete=%v", res.Incomplete)
	}
}

// TestPhase2_liveWindowProbeErrorSkipsBaseline: a probe that cannot answer is
// never read as "contiguous" — augmentation is skipped and the caveat names
// the probe failure, so the operator knows it was the check, not the data.
func TestPhase2_liveWindowProbeErrorSkipsBaseline(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)
	T := time.Now().UTC()

	fb := &fakeBaseline{ok: true, snap: T.Add(-30 * time.Minute),
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1)}}}}
	probe := &liveWindowProbe{err: errors.New("partition list unreadable")}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb, ArchivesPresent: true, LiveWindowContiguous: probe.probe})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("a failed probe must skip augmentation; got %v", res.Victims)
	}
	joined := strings.Join(res.Incomplete, " ")
	if res.Complete() || !strings.Contains(joined, "could not verify") || !strings.Contains(joined, "partition list unreadable") {
		t.Errorf("the caveat must name the probe failure; Incomplete=%v", res.Incomplete)
	}
}

// TestPhase2_archivesSkipBaseline_noProbeCallsNothing: without a probe wired
// the gate keeps the pre-#1615 rule (any archive skips), and the window is the
// widened fallback — pinned so a call site that forgets to wire the probe
// degrades to the old behavior, never to a silent "contiguous".
func TestPhase2_archivesSkipBaselineWithoutProbeWidensScan(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-2 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1 /*INSERT*/, "10", nil, nil, []byte(`{"id":10,"pid":1}`))

	fb := &fakeBaseline{ok: true, snap: T.Add(-30 * time.Minute),
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1)}}}}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb, ArchivesPresent: true})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if k := victimKeys(res.Victims); len(res.Victims) != 1 || !k["child:10"] {
		t.Fatalf("no probe: augmentation skipped but the lookback scan must still find child:10; got %v", res.Victims)
	}
	if joined := strings.Join(res.Incomplete, " "); res.Complete() || !strings.Contains(joined, "no live-window check is wired") {
		t.Errorf("no probe must keep the old existence caveat and say the check is not wired; Incomplete=%v", res.Incomplete)
	}
}

// TestPhase2_probeGapWithoutArchivesSkipsBaseline pins that the probe is
// consulted even when ArchivesPresent is false: rotation that DROPS without
// archiving writes no archive_state row, and an unreadable archive_state leaves
// the flag false too — both over a window that IS gapped. The existence flag
// must not silence the probe.
func TestPhase2_probeGapWithoutArchivesSkipsBaseline(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)
	T := time.Now().UTC()

	fb := &fakeBaseline{ok: true, snap: T.Add(-30 * time.Minute),
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1)}}}}
	probe := &liveWindowProbe{ok: false}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb, ArchivesPresent: false, LiveWindowContiguous: probe.probe})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if probe.calls != 1 {
		t.Fatalf("the probe must run without archives present; calls=%d", probe.calls)
	}
	if len(res.Victims) != 0 {
		t.Errorf("a gapped window must skip augmentation even with no archive registered; got %v", res.Victims)
	}
	if joined := strings.Join(res.Incomplete, " "); res.Complete() || !strings.Contains(joined, "cannot serve every hour") {
		t.Errorf("the skip must be flagged with the verified-gap wording; Incomplete=%v", res.Incomplete)
	}
}

// TestLiveWindowProbe_captureGap pins the second half of the wired probe: with
// every hourly partition present, a permanent capture loss stamped inside the
// window (#765) still makes the window non-contiguous, and a legacy
// stream_state that cannot be evaluated is never read as clean.
func TestLiveWindowProbe_captureGap(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	ctx := context.Background()
	h := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h, h.Add(time.Hour), h.Add(2 * time.Hour)})
	since, until := h.Add(10*time.Minute), h.Add(2*time.Hour+10*time.Minute)
	probe := cascade.LiveWindowProbe(db, dbName)

	if ok, err := probe(ctx, since, until); err != nil || !ok {
		t.Fatalf("empty stream_state (file-mode index) must be contiguous: %v, %v", ok, err)
	}
	// The partition half through the composed probe: a window starting before
	// the oldest live partition (a baseline older than the index holds) is
	// gapped even with a clean stream_state.
	if ok, err := probe(ctx, h.Add(-time.Hour), until); err != nil || ok {
		t.Fatalf("a window reaching before the oldest live partition must not be contiguous: %v, %v", ok, err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, binlog_file, binlog_position, gtid_set, events_indexed, last_checkpoint, server_id, gap_lost_at, gap_lost_detail)
		VALUES (1, 'position', 'binlog.000001', 300, '', 2, NOW(), 1, ?, 'source binlogs purged')`,
		h.Add(time.Hour).Format("2006-01-02 15:04:05"))
	if ok, err := probe(ctx, since, until); err != nil || ok {
		t.Errorf("a capture loss stamped inside the window must not be contiguous: %v, %v", ok, err)
	}
	if ok, err := probe(ctx, h.Add(time.Hour+time.Minute), until); err != nil || !ok {
		t.Errorf("a capture loss before the window is out of scope: %v, %v", ok, err)
	}
	// Legacy index: the gap columns are gone → unevaluable → never clean.
	testutil.MustExec(t, db, `ALTER TABLE stream_state DROP COLUMN gap_lost_at, DROP COLUMN gap_lost_detail`)
	if ok, err := probe(ctx, h.Add(time.Hour+time.Minute), until); err != nil || ok {
		t.Errorf("an unevaluable capture-gap state must not be read as contiguous: %v, %v", ok, err)
	}
	if cascade.LiveWindowProbe(db, "") != nil || cascade.LiveWindowProbe(nil, dbName) != nil {
		t.Errorf("no database name or no handle must yield no probe (fail-closed default), not a probe that cannot run")
	}
}

// TestPhase2_skipModeDropsSincePosUnlessSnapshotIsTheBound pins the other half
// of the window decision: SincePos REPLACES the Since time filter (#797), so in
// skip mode it must travel with `since` only when the snapshot is the lower
// bound. A snapshot newer than T-lookback whose recorded position is past the
// seeded INSERT would otherwise hide that INSERT behind a widened-looking
// window — the on-stage failure through a second door.
func TestPhase2_skipModeDropsSincePosUnlessSnapshotIsTheBound(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-2 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1 /*INSERT*/, "10", nil, nil, []byte(`{"id":10,"pid":1}`))

	past := &query.BinlogPos{File: "b.000001", Pos: 1000} // beyond the INSERT at 10-20
	fb := &fakeBaseline{ok: true, snap: T.Add(-30 * time.Minute), pos: past,
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1)}}}}
	probe := &liveWindowProbe{ok: false}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb, LiveWindowContiguous: probe.probe})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if k := victimKeys(res.Victims); len(res.Victims) != 1 || !k["child:10"] {
		t.Fatalf("skip mode with the lookback as lower bound must not anchor on the snapshot's SincePos; got %v", res.Victims)
	}

	// Mirror: in augment mode the position IS the anchor and reaches the scan —
	// the INSERT sits before it, so the child is untouched and comes back from
	// the BASELINE row (payload tells which path emitted it).
	fb2 := &fakeBaseline{ok: true, snap: h, pos: past,
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1), "payload": "from-baseline"}}}}
	res, err = cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb2, LiveWindowContiguous: (&liveWindowProbe{ok: true}).probe})
	if err != nil {
		t.Fatalf("SynthesizeVictims (augment): %v", err)
	}
	if len(res.Victims) != 1 || res.Victims[0].RowBefore["payload"] != "from-baseline" {
		t.Fatalf("augment mode must anchor the scan on SincePos so the pre-position INSERT is not a candidate; got %v", res.Victims)
	}
}

// TestPhase2_skipModeWidensToOlderSnapshot pins the "widened to the snapshot
// when that is older" clause: with a short lookback and a snapshot older than
// it, a gapped window still scans from the snapshot, so a child touched
// between the snapshot and the lookback edge is found.
func TestPhase2_skipModeWidensToOlderSnapshot(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-2 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1 /*INSERT*/, "10", nil, nil, []byte(`{"id":10,"pid":1}`))

	fb := &fakeBaseline{ok: true, snap: T.Add(-3 * time.Hour)} // older than the 1h lookback; row 10 not in it
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb, Lookback: time.Hour, LiveWindowContiguous: (&liveWindowProbe{ok: false}).probe})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if k := victimKeys(res.Victims); len(res.Victims) != 1 || !k["child:10"] {
		t.Fatalf("skip mode must widen to an older snapshot; got %v", res.Victims)
	}
}
