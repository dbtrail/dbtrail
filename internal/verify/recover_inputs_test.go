package verify

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/status"
)

// ─── fixtures ─────────────────────────────────────────────────────────────────

// riCols is a small table: an INT primary key plus a VARCHAR and an INT.
func riCols() []metadata.ColumnMeta {
	return []metadata.ColumnMeta{
		{Name: "id", DataType: "int", ColumnType: "int", IsPK: true},
		{Name: "name", DataType: "varchar", ColumnType: "varchar(64)"},
		{Name: "qty", DataType: "int", ColumnType: "int"},
	}
}

func riInput(events []query.ResultRow) recoverChainInput {
	cols := riCols()
	byName := make(map[string]metadata.ColumnMeta, len(cols))
	for _, c := range cols {
		byName[c.Name] = c
	}
	var pk []metadata.ColumnMeta
	for _, c := range cols {
		if c.IsPK {
			pk = append(pk, c)
		}
	}
	return recoverChainInput{
		Schema: "shop", Table: "orders",
		Events:        events,
		PKCols:        pk,
		ColByName:     byName,
		BinariesTyped: true,
	}
}

// riRow builds a row image the way the query engine delivers one: numbers come
// back as json.Number (query.UnmarshalRowImage uses UseNumber), so the fixtures
// must too or the test would exercise a value shape production never sees.
func riRow(id int, name string, qty int) map[string]any {
	return map[string]any{
		"id":   json.Number(itoa(id)),
		"name": name,
		"qty":  json.Number(itoa(qty)),
	}
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

var riBase = time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)

func riEvent(id uint64, typ event.EventType, pk string, before, after map[string]any) query.ResultRow {
	return query.ResultRow{
		EventID:        id,
		EventTimestamp: riBase.Add(time.Duration(id) * time.Second),
		SchemaName:     "shop",
		TableName:      "orders",
		EventType:      typ,
		PKValues:       pk,
		RowBefore:      before,
		RowAfter:       after,
	}
}

// ─── the four required cases ─────────────────────────────────────────────────

// A chain that starts with an INSERT and whose every before-image matches the
// state the previous event left must pass, with the assertions actually counted
// (a "pass" that asserted nothing would be false assurance).
func TestCheckRecoverChains_CleanChainPasses(t *testing.T) {
	events := []query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riRow(7, "widget", 1)),
		riEvent(2, event.EventUpdate, "7", riRow(7, "widget", 1), riRow(7, "widget", 5)),
		riEvent(3, event.EventUpdate, "7", riRow(7, "widget", 5), riRow(7, "gadget", 5)),
		riEvent(4, event.EventDelete, "7", riRow(7, "gadget", 5), nil),
	}
	out := checkRecoverChains(riInput(events))

	if out.Status != StatusMatch {
		t.Fatalf("clean chain: got %s (%s), want %s", out.Status, out.Detail, StatusMatch)
	}
	if out.Assertions != 3 {
		t.Errorf("want 3 before-image assertions (2 UPDATEs + 1 DELETE), got %d", out.Assertions)
	}
	if out.Chains != 1 || out.Events != 4 {
		t.Errorf("want 1 chain / 4 events, got %d / %d", out.Chains, out.Events)
	}
	if out.ChainsNoPredecessor != 0 {
		t.Errorf("chain starts with an INSERT, so it has a predecessor: got %d", out.ChainsNoPredecessor)
	}
}

// A corrupt row_before on an UPDATE is invisible to a full-table content
// comparison (the latest row_after is untouched) but makes recover emit a
// reverse UPDATE that restores the wrong values. It must be a MISMATCH.
func TestCheckRecoverChains_CorruptUpdateBeforeImageMismatches(t *testing.T) {
	events := []query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riRow(7, "widget", 1)),
		// qty=99 was never the row's state — event 1 left qty=1.
		riEvent(2, event.EventUpdate, "7", riRow(7, "widget", 99), riRow(7, "widget", 5)),
	}
	out := checkRecoverChains(riInput(events))

	if out.Status != StatusMismatch {
		t.Fatalf("corrupt row_before: got %s (%s), want %s", out.Status, out.Detail, StatusMismatch)
	}
	if !strings.Contains(out.Detail, `"qty"`) {
		t.Errorf("detail should name the diverging column, got: %s", out.Detail)
	}
	if !strings.Contains(out.Detail, "event 2") {
		t.Errorf("detail should name the offending event, got: %s", out.Detail)
	}
}

// A corrupt DELETE pre-image is the row recover re-INSERTs. A content
// comparison never sees it (the row is absent from the reconstructed table
// either way), so only this mode can catch it.
func TestCheckRecoverChains_CorruptDeletePreImageMismatches(t *testing.T) {
	events := []query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riRow(7, "widget", 1)),
		// The row held name="widget"; recover would resurrect it as "WRONG".
		riEvent(2, event.EventDelete, "7", riRow(7, "WRONG", 1), nil),
	}
	out := checkRecoverChains(riInput(events))

	if out.Status != StatusMismatch {
		t.Fatalf("corrupt DELETE pre-image: got %s (%s), want %s", out.Status, out.Detail, StatusMismatch)
	}
	if !strings.Contains(out.Detail, `"name"`) {
		t.Errorf("detail should name the diverging column, got: %s", out.Detail)
	}
}

// A retained window that begins mid-history has no predecessor state for its
// first event. That is the NORMAL case, not corruption — reporting it as a
// mismatch would make the whole mode untrustworthy.
func TestCheckRecoverChains_WindowStartingMidHistoryIsInconclusive(t *testing.T) {
	events := []query.ResultRow{
		// No INSERT: the row already existed before the window opened.
		riEvent(9, event.EventUpdate, "7", riRow(7, "widget", 1), riRow(7, "widget", 5)),
		riEvent(10, event.EventUpdate, "7", riRow(7, "widget", 5), riRow(7, "widget", 6)),
	}
	out := checkRecoverChains(riInput(events))

	if out.Status == StatusMismatch {
		t.Fatalf("a window starting mid-history must never be a MISMATCH; got detail: %s", out.Detail)
	}
	if out.ChainsNoPredecessor != 1 {
		t.Errorf("want 1 chain reported as having no predecessor, got %d", out.ChainsNoPredecessor)
	}
	// The second event DOES have a predecessor (event 9 established it), so the
	// window is partially provable — the chain start alone must not suppress it.
	if out.Assertions != 1 {
		t.Errorf("want the post-start event asserted, got %d assertions", out.Assertions)
	}
	if out.Status != StatusMatch {
		t.Errorf("one clean assertion was made, so the table is proven: got %s (%s)", out.Status, out.Detail)
	}
}

// A chain whose ONLY event has no predecessor proves nothing, and must not read
// as a match.
func TestCheckRecoverChains_NothingAssertedIsInconclusive(t *testing.T) {
	events := []query.ResultRow{
		riEvent(9, event.EventUpdate, "7", riRow(7, "widget", 1), riRow(7, "widget", 5)),
	}
	out := checkRecoverChains(riInput(events))

	if out.Status != StatusInconclusive {
		t.Fatalf("nothing asserted: got %s (%s), want %s", out.Status, out.Detail, StatusInconclusive)
	}
	if !strings.Contains(out.Detail, "proved nothing") {
		t.Errorf("detail should say nothing was proven, got: %s", out.Detail)
	}
}

// ─── superseded intermediate events ──────────────────────────────────────────

// The regression this mode exists for: a corrupt event that a NEWER event on
// the same PK overwrote. Under the content modes' LimitPerPK=1 fetch only the
// last event survives, so the corruption is invisible. The walk must visit it.
func TestCheckRecoverChains_SupersededIntermediateEventIsVisited(t *testing.T) {
	latest := riRow(7, "final", 5)
	events := []query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riRow(7, "widget", 1)),
		// Corrupt, and superseded by event 3 below.
		riEvent(2, event.EventUpdate, "7", riRow(7, "CORRUPT", 1), riRow(7, "widget", 3)),
		riEvent(3, event.EventUpdate, "7", riRow(7, "widget", 3), latest),
	}

	out := checkRecoverChains(riInput(events))
	if out.Status != StatusMismatch {
		t.Fatalf("superseded corrupt event must be caught: got %s (%s)", out.Status, out.Detail)
	}
	if !strings.Contains(out.Detail, "event 2") {
		t.Errorf("the SUPERSEDED event (2) is the one that diverged, got: %s", out.Detail)
	}
	if out.Events != 3 {
		t.Errorf("all 3 events must be walked, got %d", out.Events)
	}

	// Prove the premise: keeping only the latest event per PK (what the content
	// modes fetch) hides this entirely — that chain reports no divergence.
	onlyLatest := checkRecoverChains(riInput([]query.ResultRow{events[2]}))
	if onlyLatest.Status == StatusMismatch {
		t.Fatal("premise broken: the latest-event-only view was expected to hide the corruption")
	}
}

// The fetch must not silently apply LimitPerPK — the whole mode collapses into
// the check it complements if it ever does.
func TestRecoverInputsFetchOptions_VisitsSupersededEvents(t *testing.T) {
	since := riBase
	until := riBase.Add(time.Hour)
	opts := recoverInputsFetchOptions("shop", "orders", since, until, 1000)

	if opts.LimitPerPK != 0 {
		t.Errorf("LimitPerPK must stay unset or superseded events are never fetched, got %d", opts.LimitPerPK)
	}
	if !strings.EqualFold(opts.Order, "ASC") {
		t.Errorf("the walk reconstructs state forward in time, so Order must be ASC, got %q", opts.Order)
	}
	if opts.Limit != 1001 {
		t.Errorf("Limit must be maxEvents+1 so truncation is detectable, got %d", opts.Limit)
	}
	if opts.Since == nil || !opts.Since.Equal(since) || opts.Until == nil || !opts.Until.Equal(until) {
		t.Error("the window bounds must be carried through to the fetch")
	}
}

// ─── truncation, nil images, PK moves, normalization ─────────────────────────

// A window that did not fit the event budget must not read as a clean pass:
// the tail was never walked.
func TestCheckRecoverChains_TruncatedCleanWindowIsInconclusive(t *testing.T) {
	in := riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riRow(7, "widget", 1)),
		riEvent(2, event.EventUpdate, "7", riRow(7, "widget", 1), riRow(7, "widget", 5)),
	})
	in.Truncated = true

	out := checkRecoverChains(in)
	if out.Status != StatusInconclusive {
		t.Fatalf("truncated clean window: got %s (%s), want %s", out.Status, out.Detail, StatusInconclusive)
	}
	if !strings.Contains(out.Detail, "--max-events") {
		t.Errorf("detail should tell the operator how to widen the budget, got: %s", out.Detail)
	}
}

// A mismatch found inside a truncated window is still conclusive: the events
// that WERE walked are real, and recover would consume them.
func TestCheckRecoverChains_TruncationDoesNotMaskAMismatch(t *testing.T) {
	in := riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riRow(7, "widget", 1)),
		riEvent(2, event.EventUpdate, "7", riRow(7, "widget", 99), riRow(7, "widget", 5)),
	})
	in.Truncated = true

	if out := checkRecoverChains(in); out.Status != StatusMismatch {
		t.Fatalf("truncation must not downgrade a real mismatch: got %s (%s)", out.Status, out.Detail)
	}
}

// recovery refuses the WHOLE reversal script on a nil image it must
// dereference (#784), so each of these is a recovery that cannot run.
func TestCheckRecoverChains_NilImagesRecoveryNeedsAreMismatches(t *testing.T) {
	tests := []struct {
		name  string
		event query.ResultRow
		want  string
	}{
		{"delete without before-image", riEvent(1, event.EventDelete, "7", nil, nil), "nil row_before"},
		{"update without before-image", riEvent(1, event.EventUpdate, "7", nil, riRow(7, "w", 1)), "nil row_before"},
		{"update without after-image", riEvent(1, event.EventUpdate, "7", riRow(7, "w", 1), nil), "nil row_after"},
		{"insert without after-image", riEvent(1, event.EventInsert, "7", nil, nil), "nil row_after"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := checkRecoverChains(riInput([]query.ResultRow{tc.event}))
			if out.Status != StatusMismatch {
				t.Fatalf("got %s (%s), want %s", out.Status, out.Detail, StatusMismatch)
			}
			if !strings.Contains(out.Detail, tc.want) {
				t.Errorf("detail should mention %q, got: %s", tc.want, out.Detail)
			}
		})
	}
}

// An INSERT legitimately has no before-image — flagging it would fail every
// clean chain in existence.
func TestCheckRecoverChains_InsertWithoutBeforeImageIsNormal(t *testing.T) {
	out := checkRecoverChains(riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riRow(7, "widget", 1)),
		riEvent(2, event.EventDelete, "7", riRow(7, "widget", 1), nil),
	}))
	if out.Status != StatusMatch {
		t.Fatalf("INSERT with nil row_before is normal: got %s (%s)", out.Status, out.Detail)
	}
}

// A PK-changing UPDATE is stored under the BEFORE-image PK, so the old key can
// be REUSED by a later INSERT. Carrying the moved row's after-image forward
// under the old key would false-mismatch that sequence.
func TestCheckRecoverChains_PKChangingUpdateThenKeyReuseIsNotAMismatch(t *testing.T) {
	out := checkRecoverChains(riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "1", nil, riRow(1, "widget", 1)),
		// pk 1 → 2: filed under "1", the before-image key.
		riEvent(2, event.EventUpdate, "1", riRow(1, "widget", 1), riRow(2, "widget", 1)),
		// The freed key is reused, then updated. Neither may report a divergence.
		riEvent(3, event.EventInsert, "1", nil, riRow(1, "fresh", 9)),
		riEvent(4, event.EventUpdate, "1", riRow(1, "fresh", 9), riRow(1, "fresh", 10)),
	}))
	if out.Status == StatusMismatch {
		t.Fatalf("PK move followed by key reuse must not be a mismatch: %s", out.Detail)
	}
	// The PK-changing UPDATE itself IS asserted (its before-image is under this
	// key), as is the post-reuse UPDATE.
	if out.Assertions != 2 {
		t.Errorf("want 2 assertions (events 2 and 4), got %d", out.Assertions)
	}
}

// An UPDATE against a key the chain established was DELETEd is the chaining
// assertion failing outright.
func TestCheckRecoverChains_EventAfterDeleteWithoutReinsertMismatches(t *testing.T) {
	out := checkRecoverChains(riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riRow(7, "widget", 1)),
		riEvent(2, event.EventDelete, "7", riRow(7, "widget", 1), nil),
		riEvent(3, event.EventUpdate, "7", riRow(7, "widget", 1), riRow(7, "widget", 2)),
	}))
	if out.Status != StatusMismatch {
		t.Fatalf("got %s (%s), want %s", out.Status, out.Detail, StatusMismatch)
	}
	if !strings.Contains(out.Detail, "an earlier change deleted this row") {
		t.Errorf("detail should explain the chain said the row was gone, got: %s", out.Detail)
	}
}

// Chains are independent: one PK's corruption must not contaminate another's,
// and both must be counted.
func TestCheckRecoverChains_ChainsAreIndependentPerPK(t *testing.T) {
	out := checkRecoverChains(riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "1", nil, riRow(1, "a", 1)),
		riEvent(2, event.EventInsert, "2", nil, riRow(2, "b", 1)),
		riEvent(3, event.EventUpdate, "1", riRow(1, "a", 1), riRow(1, "a", 2)),
		riEvent(4, event.EventUpdate, "2", riRow(2, "b", 1), riRow(2, "b", 2)),
	}))
	if out.Status != StatusMatch {
		t.Fatalf("interleaved clean chains: got %s (%s)", out.Status, out.Detail)
	}
	if out.Chains != 2 || out.Assertions != 2 {
		t.Errorf("want 2 chains / 2 assertions, got %d / %d", out.Chains, out.Assertions)
	}
}

// Two renderings of the same JSON object differing only in key order are the
// same data. A fresh reflect.DeepEqual over the decoded maps would be fine
// here, but the RENDERED comparison must not regress into a byte diff — this
// is the false-MISMATCH class the package's canonicalizer already closed.
func TestCompareImages_JSONKeyOrderIsNotADivergence(t *testing.T) {
	cols := []metadata.ColumnMeta{
		{Name: "id", DataType: "int", IsPK: true},
		{Name: "doc", DataType: "json"},
	}
	byName := map[string]metadata.ColumnMeta{"id": cols[0], "doc": cols[1]}
	in := recoverChainInput{ColByName: byName, BinariesTyped: true}

	prev := map[string]any{"id": json.Number("1"), "doc": `{"a":1,"b":2}`}
	cur := map[string]any{"id": json.Number("1"), "doc": `{"b":2,"a":1}`}

	equal, unresolved, diff := compareImages(prev, cur, 1, 1, in)
	if !equal {
		t.Fatalf("key-order-only difference must compare equal (unresolved=%q diff=%q)", unresolved, diff)
	}
}

// riDeferredInput is a table carrying the value classes whose EVENT-IMAGE
// representation differs from their at-rest one: TEXT and BLOB (stored base64),
// and ENUM (stored as an ordinal).
func riDeferredInput(events []query.ResultRow) recoverChainInput {
	cols := []metadata.ColumnMeta{
		{Name: "id", DataType: "int", ColumnType: "int", IsPK: true},
		{Name: "body", DataType: "text", ColumnType: "text"},
		{Name: "blob_col", DataType: "blob", ColumnType: "blob"},
		{Name: "state", DataType: "enum", ColumnType: "enum('new','done')"},
	}
	byName := make(map[string]metadata.ColumnMeta, len(cols))
	for _, c := range cols {
		byName[c.Name] = c
	}
	return recoverChainInput{
		Schema: "shop", Table: "notes",
		Events:        events,
		PKCols:        cols[:1],
		ColByName:     byName,
		BinariesTyped: true,
	}
}

func riDeferredRow(id int, body, blobVal, state string) map[string]any {
	return map[string]any{
		"id":       json.Number(itoa(id)),
		"body":     body,
		"blob_col": blobVal,
		"state":    state,
	}
}

// This mode is the FIRST consumer to compare a previous event's row_after
// against the next event's row_before, so the normalization passes it runs
// (MapEventEnumLabels / DecodeEventBinaries) must touch BOTH images. They do —
// and if that ever regressed to row_after-only, every table with a TEXT column
// would report a conclusive MISMATCH on every event (TEXT is deliberately NOT
// in the deferred set, so nothing would downgrade it to inconclusive). This
// test walks a clean chain over exactly those value classes so the regression
// cannot land silently.
func TestCheckRecoverChains_DeferredTypeColumnsCleanChainPasses(t *testing.T) {
	out := checkRecoverChains(riDeferredInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riDeferredRow(7, "hello", "\x00\x01raw", "new")),
		riEvent(2, event.EventUpdate, "7",
			riDeferredRow(7, "hello", "\x00\x01raw", "new"),
			riDeferredRow(7, "goodbye", "\x00\x01raw", "done")),
		riEvent(3, event.EventDelete, "7", riDeferredRow(7, "goodbye", "\x00\x01raw", "done"), nil),
	}))
	if out.Status != StatusMatch {
		t.Fatalf("clean chain over TEXT/BLOB/ENUM columns: got %s (%s), want %s", out.Status, out.Detail, StatusMatch)
	}
	if out.Assertions != 2 {
		t.Errorf("want 2 assertions, got %d", out.Assertions)
	}
}

// TEXT is deliberately outside the deferred set, so a genuine TEXT divergence
// in a before-image must stay a CONCLUSIVE mismatch — not be masked as
// inconclusive just because the table also holds BLOB and ENUM columns.
func TestCheckRecoverChains_TextDivergenceIsNotMaskedByDeferredNeighbours(t *testing.T) {
	out := checkRecoverChains(riDeferredInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riDeferredRow(7, "hello", "\x00\x01raw", "new")),
		riEvent(2, event.EventUpdate, "7",
			riDeferredRow(7, "WRONG", "\x00\x01raw", "new"),
			riDeferredRow(7, "goodbye", "\x00\x01raw", "done")),
	}))
	if out.Status != StatusMismatch {
		t.Fatalf("a real TEXT divergence must stay conclusive: got %s (%s)", out.Status, out.Detail)
	}
	if !strings.Contains(out.Detail, `"body"`) {
		t.Errorf("detail should name the TEXT column, got: %s", out.Detail)
	}
}

// An ENUM value the epoch-aware label mapping could NOT resolve (still a bare
// ordinal) must degrade a difference to inconclusive rather than fire a false
// mismatch — the deferred gate the content modes already apply.
func TestCheckRecoverChains_UnresolvedEnumOrdinalIsInconclusiveNotMismatch(t *testing.T) {
	out := checkRecoverChains(riDeferredInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riDeferredRow(7, "hello", "raw", "new")),
		riEvent(2, event.EventUpdate, "7",
			// state came back as an unmapped ordinal, not the label "new".
			map[string]any{"id": json.Number("7"), "body": "hello", "blob_col": "raw", "state": json.Number("1")},
			riDeferredRow(7, "hello", "raw", "done")),
	}))
	if out.Status == StatusMismatch {
		t.Fatalf("an unresolvable ENUM representation must not be a mismatch: %s", out.Detail)
	}
	if out.Status != StatusInconclusive {
		t.Fatalf("got %s (%s), want %s", out.Status, out.Detail, StatusInconclusive)
	}
	if !strings.Contains(out.Detail, "could not be settled") {
		t.Errorf("detail should explain why, got: %s", out.Detail)
	}
}

// A MariaDB UUID column across the upgrade that fixed its capture: the event
// captured before it holds the value as damaged raw text (invalid UTF-8 bytes
// replaced with U+FFFD), the one captured after holds base64 of the 16 bytes.
// The two spellings of the SAME value differ, so the chain must land on
// Inconclusive, never on a conclusive mismatch.
func TestCheckRecoverChains_MariaDBUUIDAcrossUpgradeIsNotAMismatch(t *testing.T) {
	cols := []metadata.ColumnMeta{
		{Name: "id", DataType: "int", ColumnType: "int", IsPK: true},
		{Name: "u", DataType: "uuid", ColumnType: "uuid"},
	}
	byName := map[string]metadata.ColumnMeta{"id": cols[0], "u": cols[1]}
	in := recoverChainInput{
		Schema: "shop", Table: "devices",
		PKCols: cols[:1], ColByName: byName, BinariesTyped: true,
	}
	// 12345678-9abc-1def-8012-3456789abcde, as each epoch stored it.
	preFix := "\x124Vx\uFFFD\uFFFD\x1d\uFFFD\uFFFD\x124Vx\uFFFD\uFFFD\uFFFD"
	postFix := "EjRWeJq8He+AEjRWeJq83g=="
	row := func(u string) map[string]any { return map[string]any{"id": json.Number("7"), "u": u} }
	in.Events = []query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, row(preFix)),
		riEvent(2, event.EventUpdate, "7", row(postFix), row(postFix)),
	}
	out := checkRecoverChains(in)
	if out.Status == StatusMismatch {
		t.Fatalf("the same UUID in its pre-fix and post-fix spelling must not be a mismatch: %s", out.Detail)
	}
	if out.Status != StatusInconclusive {
		t.Fatalf("got %s (%s), want %s", out.Status, out.Detail, StatusInconclusive)
	}
}

// A column set that differs across a schema-version boundary is DDL, not
// corruption; the same difference within one version is a real divergence.
func TestCompareImages_ColumnSetDifference(t *testing.T) {
	in := riInput(nil)
	prev := riRow(7, "widget", 1)
	cur := map[string]any{"id": json.Number("7"), "name": "widget"} // qty dropped

	if equal, unresolved, _ := compareImages(prev, cur, 1, 2, in); equal || unresolved != "qty" {
		t.Errorf("across a DDL boundary the column-set difference must be inconclusive, got equal=%v unresolved=%q", equal, unresolved)
	}
	if equal, unresolved, diff := compareImages(prev, cur, 1, 1, in); equal || unresolved != "" || diff == "" {
		t.Errorf("within one schema version it is a real divergence, got equal=%v unresolved=%q diff=%q", equal, unresolved, diff)
	}
}

// SNAPSHOT rows are read-only baseline state that recovery rejects outright —
// folding one into a chain would manufacture a mismatch from nothing.
func TestDropSnapshotRows(t *testing.T) {
	rows := []query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riRow(7, "widget", 1)),
		riEvent(2, event.EventSnapshot, "7", nil, riRow(7, "widget", 1)),
		riEvent(3, event.EventUpdate, "7", riRow(7, "widget", 1), riRow(7, "widget", 2)),
	}
	got := dropSnapshotRows(rows)
	if len(got) != 2 {
		t.Fatalf("want 2 rows after dropping the snapshot, got %d", len(got))
	}
	for _, r := range got {
		if r.EventType == event.EventSnapshot {
			t.Error("a SNAPSHOT row survived the filter")
		}
	}
	// A slice with no snapshot rows must come back untouched.
	clean := rows[:1]
	if len(dropSnapshotRows(clean)) != 1 {
		t.Error("a snapshot-free slice must pass through unchanged")
	}
}

// ─── capture gaps: a hole is not corruption ──────────────────────────────────

// The gate that keeps a PERMANENT capture loss from reading as a corrupt
// before-image. The partition-existence coverage check upstream cannot see a
// hole inside a partition that exists, so stream_state's stamp is the only
// durable record — and an index that cannot answer must read as UNKNOWN, never
// as "no gap".
func TestClassifyCaptureGap(t *testing.T) {
	since := riBase
	until := riBase.Add(2 * time.Hour)
	stamped := func(at time.Time, colsPresent bool) *status.StreamStateInfo {
		return &status.StreamStateInfo{
			GapColumnsPresent: colsPresent,
			GapLostAt:         sql.NullTime{Time: at, Valid: true},
			GapLostDetail:     sql.NullString{String: "unfillable binlog gap; auto-advanced", Valid: true},
		}
	}
	tests := []struct {
		name      string
		ss        *status.StreamStateInfo
		want      captureGapVerdict
		wantWhy   string // substring, "" = don't care
		wantNoWhy bool
	}{
		{name: "no stream_state row at all (file-mode index): nothing was ever stamped",
			ss: nil, want: captureGapNoneStamped, wantNoWhy: true},
		{name: "legacy index without the gap columns cannot conclude no gap",
			ss: &status.StreamStateInfo{GapColumnsPresent: false}, want: captureGapUnknown, wantWhy: "predates"},
		{name: "migrated index with no stamp",
			ss: &status.StreamStateInfo{GapColumnsPresent: true}, want: captureGapNoneStamped, wantNoWhy: true},
		{name: "stamped inside the window",
			ss: stamped(since.Add(time.Hour), true), want: captureGapStamped, wantWhy: "gap_lost_at"},
		{name: "stamped detail is carried through",
			ss: stamped(since.Add(time.Hour), true), want: captureGapStamped, wantWhy: "auto-advanced"},
		{name: "stamped before the window is out of scope",
			ss: stamped(since.Add(-time.Second), true), want: captureGapNoneStamped, wantNoWhy: true},
		{name: "stamped after the window is out of scope",
			ss: stamped(until.Add(time.Second), true), want: captureGapNoneStamped, wantNoWhy: true},
		{name: "stamped exactly at Since is in scope (both ends inclusive)",
			ss: stamped(since, true), want: captureGapStamped, wantWhy: "gap_lost_at"},
		{name: "stamped exactly at Until is in scope",
			ss: stamped(until, true), want: captureGapStamped, wantWhy: "gap_lost_at"},
		// A legacy index that somehow carries a stamp still reads as unknown:
		// GapColumnsPresent=false means the columns were never READ, so the
		// zero-valued fields say nothing.
		{name: "legacy index wins over an unread stamp",
			ss:   &status.StreamStateInfo{GapColumnsPresent: false, GapLostAt: sql.NullTime{Time: since.Add(time.Hour), Valid: true}},
			want: captureGapUnknown, wantWhy: "predates"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, why := classifyCaptureGap(tc.ss, since, until)
			if got != tc.want {
				t.Fatalf("verdict = %d, want %d (why=%q)", got, tc.want, why)
			}
			if tc.wantWhy != "" && !strings.Contains(why, tc.wantWhy) {
				t.Errorf("reason %q should contain %q", why, tc.wantWhy)
			}
			if tc.wantNoWhy && why != "" {
				t.Errorf("a no-gap verdict must carry no reason, got %q", why)
			}
		})
	}
}

// The detail a chain break emits must NOT assert corruption as the cause.
//
// A hole in capture and a corrupt image produce byte-identical evidence, and a
// hole is at least as likely: the coverage check upstream is partition-existence
// based, so every hole that falls inside a live partition (a table skipped after
// an un-re-snapshotted ALTER, a --tables filter change, `stream --reset`, a
// short outage) is invisible to it. This project has a real instance on record —
// a 10-hour capture gap where 301 deletes and 37 inserts were lost and every
// stored image was intact. Naming only "stale or corrupt" would rule out the
// true cause and send the operator hunting for corruption that is not there.
func TestCheckRecoverChains_ChainBreakDetailDoesNotAssertCorruption(t *testing.T) {
	// A chain with a HOLE: the updates that took qty 1 → 99 were never captured.
	// The images are perfectly intact; only the events between them are missing.
	holed := checkRecoverChains(riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riRow(7, "widget", 1)),
		riEvent(2, event.EventUpdate, "7", riRow(7, "widget", 99), riRow(7, "widget", 100)),
	}))
	// A DELETE against a key the chain says is gone: the re-INSERT went missing.
	deleted := checkRecoverChains(riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riRow(7, "widget", 1)),
		riEvent(2, event.EventDelete, "7", riRow(7, "widget", 1), nil),
		riEvent(3, event.EventDelete, "7", riRow(7, "widget", 2), nil),
	}))

	for _, tc := range []struct{ name, detail string }{
		{"before-image break", holed.Detail},
		{"event after a delete", deleted.Detail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(tc.detail, "never captured") {
				t.Errorf("detail must name the missing-events explanation, got: %s", tc.detail)
			}
			// The old wording asserted the cause outright.
			if strings.Contains(tc.detail, "from a stale or corrupt before-image") {
				t.Errorf("detail must not assert corruption as THE cause, got: %s", tc.detail)
			}
			if !strings.Contains(tc.detail, "bintrail status") {
				t.Errorf("detail must point at what would distinguish the two, got: %s", tc.detail)
			}
		})
	}
}

// ─── drift rows ──────────────────────────────────────────────────────────────

// Drift rows (pk_values NULL in the index, delivered as "" — #318) carry no
// chain identity. Keying them by pk_values folds every unrelated one into a
// single chain and compares one row's row_before against another's row_after —
// a guaranteed MISMATCH on any index that merely CONTAINS drift rows.
// internal/query/merge.go's LimitPerPK already buckets them per event for the
// same reason.
func TestCheckRecoverChains_DriftRowsAreNotFoldedIntoOneChain(t *testing.T) {
	out := checkRecoverChains(riInput([]query.ResultRow{
		// Two unrelated drift rows. Under a shared "" chain, event 2's
		// row_before (name="b") would be compared against event 1's row_after
		// (name="a") and reported as a divergence.
		riEvent(1, event.EventUpdate, "", riRow(1, "a", 1), riRow(1, "a", 2)),
		riEvent(2, event.EventUpdate, "", riRow(2, "b", 1), riRow(2, "b", 2)),
		riEvent(3, event.EventDelete, "", riRow(3, "c", 1), nil),
	}))

	if out.Status == StatusMismatch {
		t.Fatalf("drift rows must never manufacture a mismatch: %s", out.Detail)
	}
	if out.Chains != 0 {
		t.Errorf("a drift row belongs to no chain, got %d chain(s)", out.Chains)
	}
	if out.Assertions != 0 {
		t.Errorf("nothing can be asserted about a drift row, got %d assertion(s)", out.Assertions)
	}
	if out.UnwalkableEvents != 3 {
		t.Errorf("want 3 unwalkable drift events, got %d", out.UnwalkableEvents)
	}
	if out.Status != StatusInconclusive {
		t.Fatalf("a table with only drift rows proves nothing: got %s (%s)", out.Status, out.Detail)
	}
	if !strings.Contains(out.Detail, "no primary key") {
		t.Errorf("detail should explain drift rows were not walked, got: %s", out.Detail)
	}
}

// Drift rows must not contaminate the real chains either: a clean keyed chain
// alongside them still proves the table.
func TestCheckRecoverChains_DriftRowsDoNotContaminateRealChains(t *testing.T) {
	out := checkRecoverChains(riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riRow(7, "widget", 1)),
		riEvent(2, event.EventUpdate, "", riRow(1, "a", 1), riRow(1, "a", 2)),
		riEvent(3, event.EventUpdate, "7", riRow(7, "widget", 1), riRow(7, "widget", 5)),
	}))
	if out.Status != StatusMatch {
		t.Fatalf("a clean chain beside a drift row must still pass: got %s (%s)", out.Status, out.Detail)
	}
	if out.Chains != 1 || out.Assertions != 1 || out.UnwalkableEvents != 1 {
		t.Errorf("want 1 chain / 1 assertion / 1 unwalkable, got %d / %d / %d",
			out.Chains, out.Assertions, out.UnwalkableEvents)
	}
}

// ─── proportionate verdicts ──────────────────────────────────────────────────

// One value whose representation could not be normalized must not erase the
// conclusive assertions made on the SAME table. deferredValueUnresolved's json
// case rejects any document holding a numeric literal, so collapsing the table
// on the first one turns a clean index into a permanently red CI gate.
func TestCheckRecoverChains_UnresolvedValueDoesNotEraseConclusiveAssertions(t *testing.T) {
	events := []query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riDeferredRow(7, "hello", "raw", "new")),
		riEvent(2, event.EventUpdate, "7",
			riDeferredRow(7, "hello", "raw", "new"),
			riDeferredRow(7, "second", "raw", "new")),
		riEvent(3, event.EventUpdate, "7",
			riDeferredRow(7, "second", "raw", "new"),
			riDeferredRow(7, "third", "raw", "new")),
		// One unresolvable ENUM representation among otherwise clean events.
		riEvent(4, event.EventUpdate, "7",
			map[string]any{"id": json.Number("7"), "body": "third", "blob_col": "raw", "state": json.Number("1")},
			riDeferredRow(7, "fourth", "raw", "done")),
	}
	out := checkRecoverChains(riDeferredInput(events))

	if out.Status != StatusMatch {
		t.Fatalf("2 conclusive assertions must survive 1 unresolvable value: got %s (%s)", out.Status, out.Detail)
	}
	if out.Assertions != 2 {
		t.Errorf("want 2 conclusive assertions, got %d", out.Assertions)
	}
	// The unproven part is still reported — it is a note on the verdict, not a
	// silent omission.
	if !strings.Contains(out.Detail, "could not be settled") {
		t.Errorf("the unresolved comparison must still be reported, got: %s", out.Detail)
	}
}

// The flip side of the rule above: a table where NOTHING was conclusive is
// still inconclusive, and still says why.
func TestCheckRecoverChains_OnlyUnresolvedValuesStaysInconclusive(t *testing.T) {
	out := checkRecoverChains(riDeferredInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riDeferredRow(7, "hello", "raw", "new")),
		riEvent(2, event.EventUpdate, "7",
			map[string]any{"id": json.Number("7"), "body": "hello", "blob_col": "raw", "state": json.Number("1")},
			riDeferredRow(7, "hello", "raw", "done")),
	}))
	if out.Status != StatusInconclusive {
		t.Fatalf("got %s (%s), want %s", out.Status, out.Detail, StatusInconclusive)
	}
	if !strings.Contains(out.Detail, "proved nothing") || !strings.Contains(out.Detail, "could not be settled") {
		t.Errorf("detail should say nothing was proven AND why, got: %s", out.Detail)
	}
}

// The recover-input mode must ride #1109's report types and, critically, its
// single exit decision — not a second exit path.
func TestNewReport_RecoverInputsCarriesChainCountsAndExits(t *testing.T) {
	rep := NewReport(ModeRecoverInputs, []TableResult{
		{Schema: "shop", Table: "orders", Status: StatusMismatch, Detail: "row_before diverged",
			EventsChecked: 42, ChainsChecked: 7, ChainsInconclusive: 2},
	})
	if rep.Mode != ModeRecoverInputs {
		t.Errorf("mode = %q, want %q", rep.Mode, ModeRecoverInputs)
	}
	got := rep.Tables[0]
	if got.EventsChecked != 42 || got.ChainsChecked != 7 || got.ChainsInconclusive != 2 {
		t.Errorf("chain counts not carried through NewReport: %+v", got)
	}
	// The counts must NOT be smuggled into the row-count fields, which mean
	// "rows in a table" to every existing consumer.
	if got.SourceRows != 0 || got.ReconstructRows != 0 {
		t.Errorf("row-count fields must stay zero in this mode, got %d/%d", got.SourceRows, got.ReconstructRows)
	}
	if rep.Verdict != VerdictMismatch {
		t.Errorf("verdict = %q, want %q", rep.Verdict, VerdictMismatch)
	}
	if rep.ExitError() == nil {
		t.Error("a recover-input mismatch must fail the run through the shared ExitError")
	}

	// The new fields are omitempty, so the content modes' JSON is byte-identical
	// to what #1109 emits.
	blob, err := json.Marshal(NewReport(ModeBaselinePair, []TableResult{
		{Schema: "shop", Table: "orders", Status: StatusMatch},
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"events_checked", "chains_checked", "chains_inconclusive"} {
		if strings.Contains(string(blob), field) {
			t.Errorf("%q must be omitted from a content-mode report: %s", field, blob)
		}
	}
}

// ─── #1126: verdicts must not assert more than the data supports ─────────────

// On a LEGACY index every event carries SchemaVersion 0 ("0 for pre-migration
// data"), so prevVer == curVer holds across a real DDL and the column-set
// escape hatch never fired: an ADD COLUMN inside the window read as a
// conclusive MISMATCH on a healthy index. Zero on either side is an unknown
// epoch and must degrade to unresolved (inconclusive), exactly like the
// version-skew case.
func TestCheckRecoverChains_LegacyIndexDDLIsInconclusiveNotMismatch(t *testing.T) {
	// riEvent leaves SchemaVersion at its zero value — the legacy-index shape.
	// The qty column appears between event 1's after-image and event 2's
	// before-image: a real historical ADD COLUMN.
	events := []query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, map[string]any{"id": json.Number("7"), "name": "widget"}),
		riEvent(2, event.EventUpdate, "7", riRow(7, "widget", 1), riRow(7, "widget", 2)),
	}
	out := checkRecoverChains(riInput(events))
	if out.Status == StatusMismatch {
		t.Fatalf("a column-set difference on a legacy (SchemaVersion 0) index must not be a conclusive mismatch: %s", out.Detail)
	}
	if out.Status != StatusInconclusive {
		t.Fatalf("got %s (%s), want %s", out.Status, out.Detail, StatusInconclusive)
	}
	if !strings.Contains(out.Detail, `"qty"`) {
		t.Errorf("detail should still name the column it could not compare, got: %s", out.Detail)
	}

	// Control: the SAME events on a post-migration index (both epochs known
	// and equal) are a genuine structural divergence — the gate is
	// content-gated on 0, not a blanket downgrade of column-set differences.
	events[0].SchemaVersion = 1
	events[1].SchemaVersion = 1
	out = checkRecoverChains(riInput(events))
	if out.Status != StatusMismatch {
		t.Fatalf("within one KNOWN schema epoch a column-set difference stays conclusive: got %s (%s)", out.Status, out.Detail)
	}
}

// The pure-comparison view of the rule above: 0 on either side (or both) is an
// unknown epoch, never proof of corruption.
func TestCompareImages_UnknownEpochColumnSetDifference(t *testing.T) {
	in := riInput(nil)
	prev := riRow(7, "widget", 1)
	cur := map[string]any{"id": json.Number("7"), "name": "widget"} // qty absent

	for _, vers := range [][2]uint32{{0, 0}, {0, 2}, {1, 0}} {
		equal, unresolved, diff := compareImages(prev, cur, vers[0], vers[1], in)
		if equal || unresolved != "qty" || diff != "" {
			t.Errorf("versions %v: an unknown epoch must be inconclusive, got equal=%v unresolved=%q diff=%q",
				vers, equal, unresolved, diff)
		}
	}
}

// Two non-nil-but-EMPTY images hold zero evidence, so they must never count as
// a proven "equal". Unreachable in production today (the #493 guard refuses
// non-FULL binlog_row_image), but "proven" must not be reachable from
// emptiness.
func TestCompareImages_EmptyImagesAreNotProvenEqual(t *testing.T) {
	equal, unresolved, diff := compareImages(map[string]any{}, map[string]any{}, 1, 1, riInput(nil))
	if equal {
		t.Fatal("two empty images must not compare as proven equal")
	}
	if unresolved == "" || diff != "" {
		t.Errorf("empty images must be inconclusive, not a mismatch: unresolved=%q diff=%q", unresolved, diff)
	}
}

// The walk-level form: a chain of empty images asserts nothing, so the table
// reports inconclusive — not a match it never earned, and not a mismatch.
func TestCheckRecoverChains_EmptyImagesAssertNothing(t *testing.T) {
	out := checkRecoverChains(riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, map[string]any{}),
		riEvent(2, event.EventDelete, "7", map[string]any{}, nil),
	}))
	if out.Status == StatusMatch {
		t.Fatalf("empty images must not prove a table: %s", out.Detail)
	}
	if out.Status != StatusInconclusive || out.Assertions != 0 {
		t.Fatalf("got %s (%s) with %d assertion(s), want %s with 0", out.Status, out.Detail, out.Assertions, StatusInconclusive)
	}
	if !strings.Contains(out.Detail, "empty") {
		t.Errorf("detail should say the images were empty, got: %s", out.Detail)
	}
}

// Only the FIRST mismatch detail and the total are ever reported, so the walk
// must not retain every formatted string (up to MaxEvents of them on a
// systemically broken table). This pins the reported shape: first detail,
// full count, and the rest summarized.
func TestCheckRecoverChains_MismatchDetailKeepsFirstAndCountsRest(t *testing.T) {
	out := checkRecoverChains(riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "7", nil, riRow(7, "widget", 1)),
		riEvent(2, event.EventUpdate, "7", riRow(7, "widget", 99), riRow(7, "widget", 100)),
		riEvent(3, event.EventUpdate, "7", riRow(7, "widget", 55), riRow(7, "widget", 56)),
		riEvent(4, event.EventUpdate, "7", riRow(7, "widget", 42), riRow(7, "widget", 43)),
	}))
	if out.Status != StatusMismatch {
		t.Fatalf("got %s (%s), want %s", out.Status, out.Detail, StatusMismatch)
	}
	if !strings.Contains(out.Detail, "does not hold together in 3 place(s)") {
		t.Errorf("detail must carry the full count, got: %s", out.Detail)
	}
	if !strings.Contains(out.Detail, "event 2") {
		t.Errorf("detail must carry the FIRST finding, got: %s", out.Detail)
	}
	if strings.Contains(out.Detail, "event 3") || strings.Contains(out.Detail, "event 4") {
		t.Errorf("later findings are summarized, not listed, got: %s", out.Detail)
	}
	// The full count ("in 3 place(s)") is the summary of the rest; the old
	// "(and 2 more)" tail restated it and was cut with the plain-words pass.
}

// A coverage gap on an index YOUNGER than the window is not rotation: the
// hours before the index's first-ever partition were never captured at all,
// and the wording must say so — with the remedy that actually works (shorten
// --lookback below the index's age) instead of implying a retention loss.
func TestRecoverGapDetail(t *testing.T) {
	oldest := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	pre := []time.Time{oldest.Add(-3 * time.Hour), oldest.Add(-2 * time.Hour), oldest.Add(-time.Hour)}
	hole := oldest.Add(2 * time.Hour)

	t.Run("window predates the index's history", func(t *testing.T) {
		got := recoverGapDetail(&query.GapError{GapHours: pre, OldestKnownHour: oldest}, false)
		if strings.Contains(got, "rotated and not archived") {
			t.Errorf("pre-history hours never rotated, got: %s", got)
		}
		for _, want := range []string{"the index did not exist yet", "--lookback", "2026-07-20 10:00", "2026-07-20 07:00"} {
			if !strings.Contains(got, want) {
				t.Errorf("detail should contain %q, got: %s", want, got)
			}
		}
	})

	t.Run("a real interior hole keeps the rotation wording", func(t *testing.T) {
		got := recoverGapDetail(&query.GapError{GapHours: []time.Time{hole}, OldestKnownHour: oldest}, false)
		if !strings.Contains(got, "rotated and not archived") || strings.Contains(got, "predate") {
			t.Errorf("an in-history gap IS rotated-and-not-archived, got: %s", got)
		}
	})

	t.Run("mixed gaps report both, honestly", func(t *testing.T) {
		got := recoverGapDetail(&query.GapError{GapHours: append(append([]time.Time{}, pre...), hole), OldestKnownHour: oldest}, false)
		if !strings.Contains(got, "the index did not exist yet") || !strings.Contains(got, "rotated and not archived") {
			t.Errorf("mixed gaps must name both causes, got: %s", got)
		}
	})

	t.Run("unknown oldest hour falls back to the generic wording", func(t *testing.T) {
		got := recoverGapDetail(&query.GapError{GapHours: pre}, false)
		if !strings.Contains(got, "coverage gap in the window") {
			t.Errorf("with no oldest-hour knowledge the wording must not claim a young index, got: %s", got)
		}
	})

	t.Run("no-archive does not claim the index has no history", func(t *testing.T) {
		// Under --no-archive the planner never saw archive_state, so archived
		// history older than the oldest LIVE partition may exist — the
		// wording must not assert the index did not exist.
		got := recoverGapDetail(&query.GapError{GapHours: pre, OldestKnownHour: oldest}, true)
		if strings.Contains(got, "the index did not exist yet") {
			t.Errorf("--no-archive cannot prove the index had no history, got: %s", got)
		}
		if !strings.Contains(got, "--no-archive") || !strings.Contains(got, "--lookback") {
			t.Errorf("detail should name the exclusion and the remedy, got: %s", got)
		}
	})
}
