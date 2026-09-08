package recovery

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// newGen returns a Generator with no DB and no resolver (triggers all-cols fallback).
func newGen() *Generator { return New(nil, nil) }

// ─── FormatSQLValue ─────────────────────────────────────────────────────────────

func TestFormatValue_nil(t *testing.T) {
	if got := FormatSQLValue(nil); got != "NULL" {
		t.Errorf("expected NULL, got %q", got)
	}
}

func TestFormatValue_boolTrue(t *testing.T) {
	if got := FormatSQLValue(true); got != "1" {
		t.Errorf("expected 1, got %q", got)
	}
}

func TestFormatValue_boolFalse(t *testing.T) {
	if got := FormatSQLValue(false); got != "0" {
		t.Errorf("expected 0, got %q", got)
	}
}

func TestFormatSQLValue_jsonNumber(t *testing.T) {
	// Row images now come back as json.Number (query.UnmarshalRowImage), so large
	// integers survive exactly instead of rounding through float64 (#496).
	cases := []struct{ in, want string }{
		{"18446744073709551615", "18446744073709551615"}, // BIGINT UNSIGNED max
		{"9223372036854775807", "9223372036854775807"},   // BIGINT signed max
		{"-9223372036854775808", "-9223372036854775808"}, // BIGINT signed min
		{"1000000000000000007", "1000000000000000007"},   // > 2^53: float64 would round
		{"3.14", "3.14"}, // decimal preserved verbatim
		{"0", "0"},
	}
	for _, c := range cases {
		if got := FormatSQLValue(json.Number(c.in)); got != c.want {
			t.Errorf("FormatSQLValue(json.Number(%q)) = %q, want %q", c.in, got, c.want)
		}
	}
	// Contrast: the float64 path silently rounds the same large value — this is
	// exactly why the JSON read path must use json.Number.
	if got := FormatSQLValue(float64(1000000000000000007)); got == "1000000000000000007" {
		t.Errorf("float64 path unexpectedly exact for >2^53 (%q) — json.Number is required", got)
	}
}

func TestFormatValue_integerFloat(t *testing.T) {
	// JSON round-trip turns int64(12345) into float64(12345).
	got := FormatSQLValue(float64(12345))
	if got != "12345" {
		t.Errorf("expected '12345', got %q", got)
	}
}

func TestFormatValue_negativeInt(t *testing.T) {
	got := FormatSQLValue(float64(-7))
	if got != "-7" {
		t.Errorf("expected '-7', got %q", got)
	}
}

func TestFormatValue_decimal(t *testing.T) {
	got := FormatSQLValue(float64(3.14))
	if !strings.Contains(got, ".") {
		t.Errorf("expected decimal point in %q", got)
	}
	if got == "NULL" || got == "3" {
		t.Errorf("unexpected result for float 3.14: %q", got)
	}
}

func TestFormatValue_string_simple(t *testing.T) {
	got := FormatSQLValue("hello")
	if got != "'hello'" {
		t.Errorf("expected \"'hello'\", got %q", got)
	}
}

func TestFormatValue_string_singleQuote(t *testing.T) {
	got := FormatSQLValue("it's fine")
	if !strings.Contains(got, `\'`) {
		t.Errorf("expected escaped single quote in %q", got)
	}
}

func TestFormatValue_string_backslash(t *testing.T) {
	got := FormatSQLValue(`C:\path`)
	// Backslash must be doubled
	if !strings.Contains(got, `\\`) {
		t.Errorf("expected escaped backslash in %q", got)
	}
}

func TestFormatValue_jsonObject(t *testing.T) {
	got := FormatSQLValue(map[string]any{"key": "val"})
	// Should be a quoted JSON string
	if !strings.HasPrefix(got, "'") || !strings.HasSuffix(got, "'") {
		t.Errorf("expected single-quoted JSON, got %q", got)
	}
	if !strings.Contains(got, "key") {
		t.Errorf("expected JSON content in %q", got)
	}
}

// ─── QuoteName ────────────────────────────────────────────────────────────────

func TestQuoteName_simple(t *testing.T) {
	if got := QuoteName("orders"); got != "`orders`" {
		t.Errorf("expected `orders`, got %q", got)
	}
}

func TestQuoteName_withBacktick(t *testing.T) {
	if got := QuoteName("col`name"); got != "`col``name`" {
		t.Errorf("expected `col``name`, got %q", got)
	}
}

// ─── EscapeString ─────────────────────────────────────────────────────────────

func TestEscapeString_singleQuote(t *testing.T) {
	if got := EscapeString("O'Brien"); !strings.Contains(got, `\'`) {
		t.Errorf("single quote not escaped in %q", got)
	}
}

func TestEscapeString_backslash(t *testing.T) {
	if got := EscapeString(`a\b`); !strings.Contains(got, `\\`) {
		t.Errorf("backslash not escaped in %q", got)
	}
}

// ─── generateInsert (DELETE → INSERT) ────────────────────────────────────────

func TestGenerateInsert_basic(t *testing.T) {
	g := newGen()
	row := query.ResultRow{
		EventID:    1,
		SchemaName: "mydb",
		TableName:  "orders",
		EventType:  parser.EventDelete,
		RowBefore: map[string]any{
			"id":     float64(42),
			"status": "active",
			"amount": float64(99.99),
		},
	}
	stmt, err := g.generateInsert(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSQL(t, stmt, "INSERT INTO")
	assertSQL(t, stmt, "`mydb`")
	assertSQL(t, stmt, "`orders`")
	assertSQL(t, stmt, "`id`")
	assertSQL(t, stmt, "42")
	assertSQL(t, stmt, "'active'")
}

func TestGenerateInsert_nilRowBefore(t *testing.T) {
	g := newGen()
	_, err := g.generateInsert(query.ResultRow{EventID: 1, EventType: parser.EventDelete})
	if err == nil {
		t.Error("expected error for nil row_before, got nil")
	}
}

func TestGenerateInsert_columnsSorted(t *testing.T) {
	// Columns should appear in alphabetical order for determinism.
	g := newGen()
	row := query.ResultRow{
		EventID: 1, SchemaName: "db", TableName: "t", EventType: parser.EventDelete,
		RowBefore: map[string]any{"zzz": "z", "aaa": "a", "mmm": "m"},
	}
	stmt, _ := g.generateInsert(row)
	// Find positions of column names in the INSERT statement.
	posA := strings.Index(stmt, "`aaa`")
	posM := strings.Index(stmt, "`mmm`")
	posZ := strings.Index(stmt, "`zzz`")
	if !(posA < posM && posM < posZ) {
		t.Errorf("expected alphabetical column order in: %s", stmt)
	}
}

// ─── generateUpdate (UPDATE → reverse UPDATE) ─────────────────────────────────

func TestGenerateUpdate_basic(t *testing.T) {
	g := newGen() // nil resolver → all-cols WHERE fallback
	row := query.ResultRow{
		EventID:    2,
		SchemaName: "mydb",
		TableName:  "orders",
		EventType:  parser.EventUpdate,
		PKValues:   "42",
		RowBefore:  map[string]any{"id": float64(42), "status": "pending"},
		RowAfter:   map[string]any{"id": float64(42), "status": "shipped"},
	}
	stmt, err := g.generateUpdate(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSQL(t, stmt, "UPDATE")
	assertSQL(t, stmt, "SET")
	assertSQL(t, stmt, "WHERE")
	// SET clause must use row_before value "pending"
	assertSQL(t, stmt, "'pending'")
	// WHERE clause must use row_after value "shipped" (all-cols fallback)
	assertSQL(t, stmt, "'shipped'")
}

func TestGenerateUpdate_pgOriginSchemaVersion0(t *testing.T) {
	// A PostgreSQL-origin UPDATE recovers correctly through the real recovery path: PG
	// has no schema_snapshots, so the row carries SchemaVersion 0 and recovery runs
	// with a nil resolver (all-columns WHERE fallback). It must NOT crash and the SET
	// must restore the real before value — including the out-of-line TOAST value that
	// Option B resolved into both images at decode time. (PK-scoped WHERE is deferred
	// to #533, which adds the offline PG schema metadata recovery would otherwise need.)
	//
	// The no-sentinel-in-recovery guarantee is enforced UPSTREAM, not here: the RI-FULL
	// gate (validateReplicaIdentity + cacheRelation) ensures the before-image is always
	// complete, so the unchanged-TOAST marker is never produced for a supported source
	// and so can never reach recovery. The sentinel check below is therefore a cheap
	// belt-and-suspenders, not the proof of that property.
	const sentinel = event.UnchangedToastKey // canonical home since #592 (aliased by pgcapture)
	const bigVal = "BIG-OUT-OF-LINE-TOAST-VALUE"
	g := newGen() // nil resolver → all-cols WHERE fallback, like a PG-origin row
	row := query.ResultRow{
		EventID: 7, SchemaName: "public", TableName: "docs",
		EventType: parser.EventUpdate, SchemaVersion: 0, PKValues: "1",
		RowBefore: map[string]any{"id": "1", "title": "orig", "body": bigVal},
		RowAfter:  map[string]any{"id": "1", "title": "changed", "body": bigVal}, // Option B resolved body
	}
	stmt, err := g.generateUpdate(row)
	if err != nil {
		t.Fatalf("recovery must not fail on a PG-origin (SchemaVersion 0) row: %v", err)
	}
	assertSQL(t, stmt, "'orig'") // SET restores the before value
	assertSQL(t, stmt, bigVal)   // the TOAST value round-trips into recovery SQL
	if strings.Contains(stmt, sentinel) {
		t.Errorf("recovery SQL leaked the unchanged-TOAST sentinel into a predicate:\n%s", stmt)
	}
}

func TestGenerateUpdate_nilRowBefore(t *testing.T) {
	g := newGen()
	row := query.ResultRow{
		EventID: 2, EventType: parser.EventUpdate,
		RowAfter: map[string]any{"id": float64(1)},
	}
	_, err := g.generateUpdate(row)
	if err == nil {
		t.Error("expected error for nil row_before")
	}
}

func TestGenerateUpdate_nilRowAfter(t *testing.T) {
	g := newGen()
	row := query.ResultRow{
		EventID: 2, EventType: parser.EventUpdate,
		RowBefore: map[string]any{"id": float64(1)},
	}
	_, err := g.generateUpdate(row)
	if err == nil {
		t.Error("expected error for nil row_after")
	}
}

// ─── generateDelete (INSERT → DELETE) ────────────────────────────────────────

func TestGenerateDelete_basic(t *testing.T) {
	g := newGen()
	row := query.ResultRow{
		EventID:    3,
		SchemaName: "mydb",
		TableName:  "orders",
		EventType:  parser.EventInsert,
		RowAfter:   map[string]any{"id": float64(99), "status": "new"},
	}
	stmt, err := g.generateDelete(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSQL(t, stmt, "DELETE FROM")
	assertSQL(t, stmt, "`mydb`")
	assertSQL(t, stmt, "`orders`")
	assertSQL(t, stmt, "WHERE")
	assertSQL(t, stmt, "99")
}

func TestGenerateDelete_nilRowAfter(t *testing.T) {
	g := newGen()
	_, err := g.generateDelete(query.ResultRow{EventID: 3, EventType: parser.EventInsert})
	if err == nil {
		t.Error("expected error for nil row_after")
	}
}

// TestGenerateDelete_allColsFallbackNullColumn pins #762: without a resolver
// (all-columns WHERE fallback), a NULL column must render as `IS NULL`, never
// `= NULL` — the latter is never true in SQL and silently no-ops the reversal
// on any PK-less/unresolvable table with a NULL column.
func TestGenerateDelete_allColsFallbackNullColumn(t *testing.T) {
	g := newGen()
	row := query.ResultRow{
		EventID:    3,
		SchemaName: "mydb",
		TableName:  "orders",
		EventType:  parser.EventInsert,
		RowAfter:   map[string]any{"id": float64(99), "status": "new", "notes": nil},
	}
	stmt, err := g.generateDelete(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSQL(t, stmt, "`notes` IS NULL")
	if strings.Contains(stmt, "`notes` = NULL") {
		t.Errorf("WHERE clause emits `col = NULL` (never matches): %s", stmt)
	}
}

// TestGenerateUpdate_allColsFallbackNullColumn covers the same #762 fallback
// for UPDATE reversals, whose WHERE is built from row_after.
func TestGenerateUpdate_allColsFallbackNullColumn(t *testing.T) {
	g := newGen()
	row := query.ResultRow{
		EventID:    4,
		SchemaName: "mydb",
		TableName:  "orders",
		EventType:  parser.EventUpdate,
		RowBefore:  map[string]any{"id": float64(1), "status": "pending", "notes": "hi"},
		RowAfter:   map[string]any{"id": float64(1), "status": "shipped", "notes": nil},
	}
	stmt, err := g.generateUpdate(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSQL(t, stmt, "`notes` IS NULL")
	if strings.Contains(stmt, "`notes` = NULL") {
		t.Errorf("WHERE clause emits `col = NULL` (never matches): %s", stmt)
	}
}

// ─── #789: all-columns fallback caps reversal at one row (LIMIT 1) ───────────

// TestGenerateDelete_allColsFallbackLimit1 pins #789: with the all-columns
// WHERE fallback (no resolver), byte-identical duplicate rows all match — the
// reverse DELETE of ONE INSERT must not delete every copy, so the MySQL
// dialect emits a trailing LIMIT 1.
func TestGenerateDelete_allColsFallbackLimit1(t *testing.T) {
	g := newGen()
	row := query.ResultRow{
		EventID:    3,
		SchemaName: "mydb",
		TableName:  "orders",
		EventType:  parser.EventInsert,
		RowAfter:   map[string]any{"id": float64(99), "status": "new"},
	}
	stmt, err := g.generateDelete(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasSuffix(stmt, " LIMIT 1") {
		t.Errorf("all-columns fallback DELETE must end with LIMIT 1 (#789): %s", stmt)
	}
}

// TestGenerateUpdate_allColsFallbackLimit1 covers the same #789 cap for UPDATE
// reversals — an unbounded all-columns WHERE would modify every duplicate.
func TestGenerateUpdate_allColsFallbackLimit1(t *testing.T) {
	g := newGen()
	row := query.ResultRow{
		EventID:    4,
		SchemaName: "mydb",
		TableName:  "orders",
		EventType:  parser.EventUpdate,
		RowBefore:  map[string]any{"id": float64(1), "status": "pending"},
		RowAfter:   map[string]any{"id": float64(1), "status": "shipped"},
	}
	stmt, err := g.generateUpdate(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasSuffix(stmt, " LIMIT 1") {
		t.Errorf("all-columns fallback UPDATE must end with LIMIT 1 (#789): %s", stmt)
	}
}

// TestGenerateDelete_allColsFallbackPGWarning pins the PostgreSQL-dialect follow-up to
// #789: PostgreSQL has no DELETE ... LIMIT, so the all-columns fallback stays unbounded
// — the generated statement must carry a leading warning comment instead, since that's
// the only runtime signal that reaches an operator reviewing --dry-run/--output.
func TestGenerateDelete_allColsFallbackPGWarning(t *testing.T) {
	g := NewForDialect(nil, nil, PostgresDialect)
	row := query.ResultRow{
		EventID:    5,
		SchemaName: "mydb",
		TableName:  "orders",
		EventType:  parser.EventInsert,
		RowAfter:   map[string]any{"id": float64(99), "status": "new"},
	}
	stmt, err := g.generateDelete(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(stmt, "-- WARNING:") {
		t.Errorf("PG all-columns fallback DELETE must carry a leading warning comment: %s", stmt)
	}
	if strings.Contains(stmt, "LIMIT") {
		t.Errorf("PostgreSQL dialect must never emit LIMIT: %s", stmt)
	}
}

// TestGenerateUpdate_allColsFallbackPGWarning covers the same PG warning-comment
// requirement for UPDATE reversals.
func TestGenerateUpdate_allColsFallbackPGWarning(t *testing.T) {
	g := NewForDialect(nil, nil, PostgresDialect)
	row := query.ResultRow{
		EventID:    6,
		SchemaName: "mydb",
		TableName:  "orders",
		EventType:  parser.EventUpdate,
		RowBefore:  map[string]any{"id": float64(1), "status": "pending"},
		RowAfter:   map[string]any{"id": float64(1), "status": "shipped"},
	}
	stmt, err := g.generateUpdate(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(stmt, "-- WARNING:") {
		t.Errorf("PG all-columns fallback UPDATE must carry a leading warning comment: %s", stmt)
	}
	if strings.Contains(stmt, "LIMIT") {
		t.Errorf("PostgreSQL dialect must never emit LIMIT: %s", stmt)
	}
}

// TestGeneratePG_pkResolved_noWarningComment ensures the warning comment is scoped to
// the all-columns fallback only — a PK-resolved statement (the common case) must stay
// clean.
func TestGeneratePG_pkResolved_noWarningComment(t *testing.T) {
	g := NewForDialect(nil, identityGenResolver(), PostgresDialect)
	row := query.ResultRow{
		EventID:    7,
		SchemaName: "public",
		TableName:  "t",
		EventType:  parser.EventInsert,
		RowAfter:   map[string]any{"id": float64(99), "v": "new"},
	}
	stmt, err := g.generateDelete(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(stmt, "WARNING") {
		t.Errorf("PK-resolved PG statement must not carry the all-columns warning comment: %s", stmt)
	}
}

// TestPKScopedWhere_noLimit: a PK-scoped WHERE uniquely identifies the row, so
// no LIMIT is emitted — the clean statement shape documented in
// docs/query-and-recovery.md stays unchanged.
func TestPKScopedWhere_noLimit(t *testing.T) {
	g := newGenWithResolver()
	del := query.ResultRow{
		EventID:    5,
		SchemaName: "shop",
		TableName:  "order_items",
		EventType:  parser.EventInsert,
		RowAfter:   map[string]any{"order_id": float64(5), "quantity": float64(3)},
	}
	stmt, err := g.generateDelete(del)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(stmt, "LIMIT") {
		t.Errorf("PK-scoped DELETE must not carry LIMIT: %s", stmt)
	}
	upd := query.ResultRow{
		EventID:    6,
		SchemaName: "shop",
		TableName:  "order_items",
		EventType:  parser.EventUpdate,
		RowBefore:  map[string]any{"order_id": float64(5), "quantity": float64(2)},
		RowAfter:   map[string]any{"order_id": float64(5), "quantity": float64(3)},
	}
	stmt, err = g.generateUpdate(upd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(stmt, "LIMIT") {
		t.Errorf("PK-scoped UPDATE must not carry LIMIT: %s", stmt)
	}
}

// TestGenerateDelete_pgFallbackNoLimit: PostgreSQL has no DELETE/UPDATE ...
// LIMIT, so the PG-dialect fallback must stay unbounded (documented caveat)
// rather than emit invalid SQL.
func TestGenerateDelete_pgFallbackNoLimit(t *testing.T) {
	g := NewForDialect(nil, nil, PostgresDialect)
	row := query.ResultRow{
		EventID:    7,
		SchemaName: "mydb",
		TableName:  "orders",
		EventType:  parser.EventInsert,
		RowAfter:   map[string]any{"id": float64(99), "status": "new"},
	}
	stmt, err := g.generateDelete(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(stmt, "LIMIT") {
		t.Errorf("PostgreSQL DELETE must not carry LIMIT (invalid syntax): %s", stmt)
	}
}

// ─── GenerateSQL integration (no DB, exercising the output wrapper) ────────────

func TestGenerateSQL_noRows(t *testing.T) {
	// We can't call GenerateSQL without a DB, but we can test the no-events path
	// by calling the internal writer indirectly. We test the output format here
	// by wiring up a fake set of events.
	g := newGen()
	var buf bytes.Buffer

	// Manually exercise the output format.
	rows := []query.ResultRow{
		{
			EventID:    10,
			SchemaName: "db",
			TableName:  "tbl",
			EventType:  parser.EventDelete,
			PKValues:   "5",
			RowBefore:  map[string]any{"id": float64(5), "name": "Alice"},
		},
	}

	// Call the private emitter path by using GenerateStatement directly.
	stmt, err := g.generateStatement(rows[0])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	buf.WriteString("BEGIN;\n")
	buf.WriteString(stmt + ";\n")
	buf.WriteString("COMMIT;\n")

	out := buf.String()
	assertSQL(t, out, "BEGIN;")
	assertSQL(t, out, "INSERT INTO")
	assertSQL(t, out, "COMMIT;")
}

// TestGenerateStatement_snapshotRejected pins the defense-in-depth guard:
// if a ResultRow with EventType=EventSnapshot ever reaches the reversal
// generator, the error message must be specific ("read-only baseline rows")
// rather than the generic "unknown event type N" fallback. Future code that
// wires snapshot rows into the recover pipeline will fail loudly.
func TestGenerateStatement_snapshotRejected(t *testing.T) {
	g := newGen()
	row := query.ResultRow{
		EventID:    99,
		SchemaName: "db",
		TableName:  "tbl",
		EventType:  parser.EventSnapshot,
		PKValues:   "1",
		RowAfter:   map[string]any{"id": float64(1)},
	}
	_, err := g.generateStatement(row)
	if err == nil {
		t.Fatal("expected error for SNAPSHOT event; got nil")
	}
	if !strings.Contains(err.Error(), "SNAPSHOT") {
		t.Errorf("error should mention SNAPSHOT explicitly (not the generic fallback); got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "read-only") {
		t.Errorf("error should explain baseline rows are read-only; got %q", err.Error())
	}
}

// ─── Null / special value handling ───────────────────────────────────────────

func TestFormatValue_nullInRow(t *testing.T) {
	// A NULL column (Go nil) must produce SQL NULL.
	got := FormatSQLValue(nil)
	if got != "NULL" {
		t.Errorf("expected NULL, got %q", got)
	}
}

func TestGenerateInsert_withNullColumn(t *testing.T) {
	g := newGen()
	row := query.ResultRow{
		EventID:    5,
		SchemaName: "db",
		TableName:  "t",
		EventType:  parser.EventDelete,
		RowBefore:  map[string]any{"id": float64(1), "note": nil},
	}
	stmt, err := g.generateInsert(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(stmt, "NULL") {
		t.Errorf("expected NULL in INSERT for nil column: %s", stmt)
	}
}

// ─── Helper ───────────────────────────────────────────────────────────────────

// assertSQL checks that want appears in the SQL string stmt.
func assertSQL(t *testing.T, stmt, want string) {
	t.Helper()
	if !strings.Contains(stmt, want) {
		t.Errorf("expected %q in SQL:\n  %s", want, stmt)
	}
}

// ─── GenerateSQL output: BEGIN/COMMIT wrapper ─────────────────────────────────

func TestGenerateSQL_noEventsMessage(t *testing.T) {
	// Verify the exact text emitted when there are no matching events.
	var buf bytes.Buffer
	fmt.Fprintln(&buf, "-- No events matched the specified criteria.")
	if !strings.Contains(buf.String(), "No events matched") {
		t.Error("expected no-events message")
	}
}

// ─── FormatSQLValue edge cases ──────────────────────────────────────────────────

func TestFormatValue_arraySlice(t *testing.T) {
	// JSON array column: []any should be serialised as a quoted JSON array.
	val := []any{"a", float64(1), true}
	got := FormatSQLValue(val)
	if !strings.HasPrefix(got, "'") || !strings.HasSuffix(got, "'") {
		t.Errorf("expected single-quoted JSON array, got %q", got)
	}
	if !strings.Contains(got, `"a"`) {
		t.Errorf("expected array element 'a' in %q", got)
	}
}

func TestFormatValue_jsonRawMessage(t *testing.T) {
	raw := json.RawMessage(`{"key":"value"}`)
	got := FormatSQLValue(raw)
	if !strings.HasPrefix(got, "'") || !strings.HasSuffix(got, "'") {
		t.Errorf("expected quoted JSON, got %q", got)
	}
	if !strings.Contains(got, "key") {
		t.Errorf("expected JSON content in %q", got)
	}
}

func TestFormatValue_largeFloat(t *testing.T) {
	// float64 >= 1e15 takes the FormatFloat path (not int64 conversion).
	// FormatFloat('f', -1) for exact whole numbers still omits the decimal,
	// so the output looks like an integer — the guard is about int64 overflow
	// safety, not about output format.
	got := FormatSQLValue(float64(1e15))
	if got != "1000000000000000" {
		t.Errorf("expected 1000000000000000, got %q", got)
	}
}

func TestFormatValue_veryLargeFloat(t *testing.T) {
	// 1e18 exceeds the int64 guard but is representable in float64.
	got := FormatSQLValue(float64(1e18))
	if got != "1000000000000000000" {
		t.Errorf("expected 1000000000000000000, got %q", got)
	}
}

func TestFormatValue_beyondInt64Range(t *testing.T) {
	// 1e19 is beyond int64 max (~9.2e18). The guard prevents int64 overflow;
	// FormatFloat handles it correctly.
	got := FormatSQLValue(float64(1e19))
	if got == "" {
		t.Error("expected non-empty result for 1e19")
	}
	// Should not panic — the value is too large for int64 but FormatFloat
	// handles it safely.
}

func TestFormatValue_infinity(t *testing.T) {
	got := FormatSQLValue(math.Inf(1))
	if got == "NULL" {
		t.Errorf("expected float format for +Inf, got %q", got)
	}
	// Should not panic — just format somehow.
}

func TestFormatValue_nan(t *testing.T) {
	got := FormatSQLValue(math.NaN())
	if got == "NULL" {
		t.Errorf("expected float format for NaN, got %q", got)
	}
}

func TestFormatValue_negativeZero(t *testing.T) {
	got := FormatSQLValue(math.Copysign(0, -1))
	// -0 == 0, so Trunc(-0) == -0, and -0 == -0. math.Abs(-0) = 0 < 1e15.
	// It should format as "0" (integer format).
	if got != "0" {
		t.Errorf("expected '0' for negative zero, got %q", got)
	}
}

// ─── EscapeString edge cases ─────────────────────────────────────────────────

func TestEscapeString_nullByte(t *testing.T) {
	got := EscapeString("hello\x00world")
	if !strings.Contains(got, `\0`) {
		t.Errorf("expected \\0 for null byte, got %q", got)
	}
	if strings.Contains(got, "\x00") {
		t.Errorf("raw null byte should be replaced, got %q", got)
	}
}

func TestEscapeString_combined(t *testing.T) {
	got := EscapeString("it's a \\path\x00end")
	if !strings.Contains(got, `\'`) {
		t.Errorf("expected escaped quote in %q", got)
	}
	if !strings.Contains(got, `\\`) {
		t.Errorf("expected escaped backslash in %q", got)
	}
	if !strings.Contains(got, `\0`) {
		t.Errorf("expected escaped null in %q", got)
	}
}

// ─── Generated column filtering ──────────────────────────────────────────────

// newGenWithResolver returns a Generator backed by a resolver containing a
// table with one STORED generated column ("line_total") and one ordinary
// column with an expression default ("created_at TIMESTAMP DEFAULT
// CURRENT_TIMESTAMP", IsGenerated: false) — the #758 DEFAULT_GENERATED trap:
// MySQL's EXTRA column reports "DEFAULT_GENERATED" for created_at too, but it
// is a real, captured data column and must NEVER be treated like line_total.
func newGenWithResolver() *Generator {
	tm := &metadata.TableMeta{
		Schema: "shop",
		Table:  "order_items",
		Columns: []metadata.ColumnMeta{
			{Name: "order_id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "quantity", OrdinalPosition: 2, DataType: "int"},
			{Name: "unit_price", OrdinalPosition: 3, DataType: "decimal"},
			{Name: "line_total", OrdinalPosition: 4, DataType: "decimal", IsGenerated: true},
			{Name: "created_at", OrdinalPosition: 5, DataType: "timestamp", IsGenerated: false},
		},
		PKColumns: []string{"order_id"},
	}
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"shop.order_items": tm,
	})
	return New(nil, resolver)
}

func TestGenerateInsert_skipsGeneratedColumns(t *testing.T) {
	g := newGenWithResolver()
	row := query.ResultRow{
		EventID:    10,
		SchemaName: "shop",
		TableName:  "order_items",
		EventType:  parser.EventDelete,
		RowBefore: map[string]any{
			"order_id":   float64(5),
			"quantity":   float64(3),
			"unit_price": float64(68.81),
			"line_total": float64(206.43),       // STORED generated — must be excluded
			"created_at": "2024-01-01 00:00:00", // DEFAULT_GENERATED (expression default) — must be included (#758)
		},
	}
	stmt, err := g.generateInsert(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSQL(t, stmt, "INSERT INTO")
	assertSQL(t, stmt, "`order_id`")
	assertSQL(t, stmt, "`quantity`")
	assertSQL(t, stmt, "`unit_price`")
	assertSQL(t, stmt, "`created_at`")
	if strings.Contains(stmt, "line_total") {
		t.Errorf("generated column 'line_total' must not appear in INSERT: %s", stmt)
	}
}

func TestGenerateUpdate_skipsGeneratedColumns(t *testing.T) {
	g := newGenWithResolver()
	row := query.ResultRow{
		EventID:    11,
		SchemaName: "shop",
		TableName:  "order_items",
		EventType:  parser.EventUpdate,
		RowBefore: map[string]any{
			"order_id":   float64(5),
			"quantity":   float64(2),
			"unit_price": float64(68.81),
			"line_total": float64(137.62),       // STORED generated — must be excluded from SET
			"created_at": "2024-01-01 00:00:00", // DEFAULT_GENERATED — must be included (#758)
		},
		RowAfter: map[string]any{
			"order_id":   float64(5),
			"quantity":   float64(3),
			"unit_price": float64(68.81),
			"line_total": float64(206.43),
			"created_at": "2024-01-02 00:00:00",
		},
	}
	stmt, err := g.generateUpdate(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSQL(t, stmt, "UPDATE")
	assertSQL(t, stmt, "SET")
	assertSQL(t, stmt, "`quantity` = 2")
	setIdx := strings.Index(stmt, "SET")
	whereIdx := strings.Index(stmt, "WHERE")
	if setIdx < 0 || whereIdx < 0 {
		t.Fatalf("expected SET and WHERE in: %s", stmt)
	}
	setPart := stmt[setIdx:whereIdx]
	if strings.Contains(setPart, "line_total") {
		t.Errorf("generated column 'line_total' must not appear in SET clause: %s", setPart)
	}
	if !strings.Contains(setPart, "`created_at`") {
		t.Errorf("DEFAULT_GENERATED column 'created_at' must appear in SET clause (#758): %s", setPart)
	}
}

// ─── GenerateSQLFromRows ──────────────────────────────────────────────────────

func TestGenerateSQLFromRows_empty(t *testing.T) {
	g := newGen()
	var buf bytes.Buffer
	n, err := g.GenerateSQLFromRows(nil, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 statements, got %d", n)
	}
	assertSQL(t, buf.String(), "No events matched")
}

func TestGenerateSQLFromRows_reverseOrder(t *testing.T) {
	g := newGen()
	rows := []query.ResultRow{
		{
			EventID: 1, SchemaName: "db", TableName: "t", EventType: parser.EventDelete,
			PKValues:  "10",
			RowBefore: map[string]any{"id": float64(10), "name": "first"},
		},
		{
			EventID: 2, SchemaName: "db", TableName: "t", EventType: parser.EventInsert,
			PKValues: "20",
			RowAfter: map[string]any{"id": float64(20), "name": "second"},
		},
	}

	var buf bytes.Buffer
	n, err := g.GenerateSQLFromRows(rows, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 statements, got %d", n)
	}

	out := buf.String()
	assertSQL(t, out, "BEGIN;")
	assertSQL(t, out, "COMMIT;")

	// Event 2 (INSERT → DELETE) should appear before event 1 (DELETE → INSERT)
	// because GenerateSQLFromRows reverses the input order.
	deletePos := strings.Index(out, "DELETE FROM")
	insertPos := strings.Index(out, "INSERT INTO")
	if deletePos < 0 || insertPos < 0 {
		t.Fatalf("expected both DELETE and INSERT in output:\n%s", out)
	}
	if deletePos > insertPos {
		t.Errorf("expected reversed order (event 2 before event 1):\n%s", out)
	}
}

// TestGenerateSQLFromRows_unresolvedToastMarker is the #592 acceptance test: a
// synthetic row carrying the residual unchanged-TOAST marker must make recover
// fail LOUD — a hard error and zero bytes written — never a script that
// silently marshals the marker into the column ({"__bintrail_unchanged_toast__":true}).
// The refusal is up front because the render loop demotes per-statement errors
// to SQL comments. Checked in both dialects and both images: the marker is a
// capture-invariant violation wherever it appears.
func TestGenerateSQLFromRows_unresolvedToastMarker(t *testing.T) {
	marker := map[string]any{event.UnchangedToastKey: true}
	cases := []struct {
		name string
		gen  *Generator
		row  query.ResultRow
	}{
		{
			name: "pg dialect, marker in row_before of UPDATE",
			gen:  NewForDialect(nil, nil, PostgresDialect),
			row: query.ResultRow{
				EventID: 1, SchemaName: "public", TableName: "docs",
				EventType: parser.EventUpdate, PKValues: "7",
				RowBefore: map[string]any{"id": "7", "body": marker},
				RowAfter:  map[string]any{"id": "7", "body": "new"},
			},
		},
		{
			name: "pg dialect, marker in row_after of INSERT",
			gen:  NewForDialect(nil, nil, PostgresDialect),
			row: query.ResultRow{
				EventID: 2, SchemaName: "public", TableName: "docs",
				EventType: parser.EventInsert, PKValues: "8",
				RowAfter: map[string]any{"id": "8", "body": marker},
			},
		},
		{
			name: "mysql dialect, marker in row_before of DELETE",
			gen:  newGen(),
			row: query.ResultRow{
				EventID: 3, SchemaName: "db", TableName: "t",
				EventType: parser.EventDelete, PKValues: "9",
				RowBefore: map[string]any{"id": "9", "body": marker},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := tc.gen.GenerateSQLFromRows([]query.ResultRow{tc.row}, &buf)
			if err == nil {
				t.Fatalf("expected a loud error, got n=%d output:\n%s", n, buf.String())
			}
			for _, want := range []string{"unresolved unchanged-TOAST marker", "capture invariant violated", "body"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error missing %q:\n%s", want, err)
				}
			}
			if buf.Len() != 0 {
				t.Errorf("refusal must write NOTHING, got %d bytes:\n%s", buf.Len(), buf.String())
			}
			if n != 0 {
				t.Errorf("expected 0 statements, got %d", n)
			}
		})
	}

	// A clean sibling row still generates: the guard must not trip on ordinary
	// structured values that are not the marker (e.g. a MySQL JSON column).
	var buf bytes.Buffer
	clean := query.ResultRow{
		EventID: 4, SchemaName: "db", TableName: "t",
		EventType: parser.EventInsert, PKValues: "10",
		RowAfter: map[string]any{"id": "10", "attrs": map[string]any{"nested": true, "n": float64(1)}},
	}
	if _, err := newGen().GenerateSQLFromRows([]query.ResultRow{clean}, &buf); err != nil {
		t.Fatalf("non-marker structured value must not trip the guard: %v", err)
	}
}

// TestGenerateSQLFromRows_partialGenerationRefused is the #784 acceptance test: an
// event whose stored image is malformed (here row_before is nil for a DELETE, as a
// truncated/corrupt JSON payload would decode) must make recover FAIL LOUD — a hard
// error naming the event and zero bytes written — never a script where the failed
// event is demoted to a `-- ERROR ...` comment while the rest commits clean (a
// silently incomplete reversal). The happy-path sibling in the same call must not
// rescue it: one bad event refuses the whole script.
func TestGenerateSQLFromRows_partialGenerationRefused(t *testing.T) {
	g := newGen()
	rows := []query.ResultRow{
		{ // generatable
			EventID: 1, SchemaName: "db", TableName: "t", EventType: parser.EventInsert,
			PKValues: "1", RowAfter: map[string]any{"id": float64(1)},
		},
		{ // NOT generatable: DELETE with nil row_before (malformed/truncated image)
			EventID: 2, SchemaName: "db", TableName: "t", EventType: parser.EventDelete,
			PKValues: "2", RowBefore: nil,
		},
	}
	var buf bytes.Buffer
	n, err := g.GenerateSQLFromRows(rows, &buf)
	if err == nil {
		t.Fatalf("expected a loud refusal, got n=%d output:\n%s", n, buf.String())
	}
	if n != 0 {
		t.Errorf("expected 0 statements on refusal, got %d", n)
	}
	if buf.Len() != 0 {
		t.Errorf("refusal must write NOTHING, got %d bytes:\n%s", buf.Len(), buf.String())
	}
	for _, want := range []string{"refusing to emit reversal SQL", "event 2", "row_before is nil"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%s", want, err)
		}
	}
	// A malformed comment must never be the ONLY signal: the failed event must be
	// named in the error, not just left as a demoted `-- ERROR ...` line.
	if strings.Contains(err.Error(), "event 1") {
		t.Errorf("only the un-generatable event should be named, not the healthy one:\n%s", err)
	}
}

// TestGenerateSQLFromRows_multipleFailuresAllNamed proves the refusal lists EVERY
// un-generatable event, not just the first — the diagnosis is complete (#784).
func TestGenerateSQLFromRows_multipleFailuresAllNamed(t *testing.T) {
	g := newGen()
	rows := []query.ResultRow{
		{EventID: 7, SchemaName: "db", TableName: "t", EventType: parser.EventUpdate, PKValues: "7", RowBefore: nil, RowAfter: map[string]any{"id": float64(7)}},
		{EventID: 9, SchemaName: "db", TableName: "t", EventType: parser.EventInsert, PKValues: "9", RowAfter: nil},
	}
	var buf bytes.Buffer
	_, err := g.GenerateSQLFromRows(rows, &buf)
	if err == nil {
		t.Fatalf("expected refusal, got nil (output:\n%s)", buf.String())
	}
	for _, want := range []string{"event 7", "event 9", "2 of the matched event(s)"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%s", want, err)
		}
	}
}

// ─── resolverForRow ──────────────────────────────────────────────────────────

func TestResolverForRow_zeroVersionReturnsFallback(t *testing.T) {
	resolver := metadata.NewResolverFromTables(5, map[string]*metadata.TableMeta{
		"db.t": {Schema: "db", Table: "t", Columns: []metadata.ColumnMeta{{Name: "id", IsPK: true}}},
	})
	g := New(nil, resolver)
	row := query.ResultRow{SchemaVersion: 0, SchemaName: "db", TableName: "t"}
	got := g.resolverForRow(row)
	if got != resolver {
		t.Error("expected fallback resolver for SchemaVersion=0")
	}
}

func TestResolverForRow_nilDB_returnsFallback(t *testing.T) {
	resolver := metadata.NewResolverFromTables(5, nil)
	g := New(nil, resolver)
	row := query.ResultRow{SchemaVersion: 99}
	got := g.resolverForRow(row)
	if got != resolver {
		t.Error("expected fallback resolver when db is nil")
	}
}

func TestResolverForRow_matchingFallback(t *testing.T) {
	resolver := metadata.NewResolverFromTables(5, nil)
	g := New(nil, resolver)
	row := query.ResultRow{SchemaVersion: 5}
	got := g.resolverForRow(row)
	if got != resolver {
		t.Error("expected fallback resolver when SchemaVersion matches")
	}
}

func TestResolverForRow_cacheHit(t *testing.T) {
	cachedResolver := metadata.NewResolverFromTables(42, map[string]*metadata.TableMeta{
		"db.t": {Schema: "db", Table: "t", Columns: []metadata.ColumnMeta{{Name: "id", IsPK: true}}},
	})
	// db must be non-nil so resolverForRow doesn't short-circuit; the cache hit
	// prevents any actual DB access.
	g := New(new(sql.DB), nil)
	g.cache = map[uint32]*metadata.Resolver{42: cachedResolver}
	row := query.ResultRow{SchemaVersion: 42, SchemaName: "db", TableName: "t"}
	got := g.resolverForRow(row)
	if got != cachedResolver {
		t.Error("expected cached resolver for SchemaVersion=42")
	}
}

func TestGenerateSQLFromRows_differentSchemaVersions_differentPKs(t *testing.T) {
	// Simulate a schema change: snapshot 10 has PK=id, snapshot 20 has PK=uuid.
	// Rows with different SchemaVersion values should use different WHERE clauses.
	resolver10 := metadata.NewResolverFromTables(10, map[string]*metadata.TableMeta{
		"shop.orders": {Schema: "shop", Table: "orders", Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true},
			{Name: "status", OrdinalPosition: 2},
		}},
	})
	resolver20 := metadata.NewResolverFromTables(20, map[string]*metadata.TableMeta{
		"shop.orders": {Schema: "shop", Table: "orders", Columns: []metadata.ColumnMeta{
			{Name: "uuid", OrdinalPosition: 1, IsPK: true},
			{Name: "id", OrdinalPosition: 2},
			{Name: "status", OrdinalPosition: 3},
		}},
	})

	// db must be non-nil so resolverForRow doesn't short-circuit; the cache
	// pre-population prevents any actual DB access.
	g := New(new(sql.DB), resolver20)
	g.cache = map[uint32]*metadata.Resolver{10: resolver10, 20: resolver20}

	rows := []query.ResultRow{
		{
			EventID: 1, SchemaName: "shop", TableName: "orders",
			EventType: parser.EventInsert, SchemaVersion: 10,
			EventTimestamp: time.Now(),
			RowAfter:       map[string]any{"id": float64(1), "status": "new"},
		},
		{
			EventID: 2, SchemaName: "shop", TableName: "orders",
			EventType: parser.EventInsert, SchemaVersion: 20,
			EventTimestamp: time.Now(),
			RowAfter:       map[string]any{"uuid": "abc-123", "id": float64(2), "status": "new"},
		},
	}

	var buf bytes.Buffer
	n, err := g.GenerateSQLFromRows(rows, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 statements, got %d", n)
	}

	output := buf.String()
	// Row with SchemaVersion=10 → resolver10 (PK=id) → WHERE `id` = 1
	if !strings.Contains(output, "WHERE `id` = 1") {
		t.Errorf("expected WHERE `id` = 1 for SchemaVersion=10 row, got:\n%s", output)
	}
	// Row with SchemaVersion=20 → resolver20 (PK=uuid) → WHERE `uuid` = 'abc-123'
	if !strings.Contains(output, "WHERE `uuid` = 'abc-123'") {
		t.Errorf("expected WHERE `uuid` = 'abc-123' for SchemaVersion=20 row, got:\n%s", output)
	}
}

func TestGenerateInsert_noResolver_includesAllColumns(t *testing.T) {
	// Without a resolver, all columns (including any generated ones) are emitted —
	// the generator has no way to know which are generated.
	g := newGen()
	row := query.ResultRow{
		EventID:    12,
		SchemaName: "shop",
		TableName:  "order_items",
		EventType:  parser.EventDelete,
		RowBefore: map[string]any{
			"order_id":   float64(5),
			"line_total": float64(206.43),
		},
	}
	stmt, err := g.generateInsert(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSQL(t, stmt, "line_total")
}

// ─── FormatSQLValue extended types (DuckDB scan) ─────────────────────────────
// These exercise the int64/time.Time/[]byte cases added for the full-table
// reconstruct path (#187), where values come from DuckDB's database/sql driver
// rather than a JSON round-trip.

func TestFormatValue_int64(t *testing.T) {
	if got := FormatSQLValue(int64(9876543210)); got != "9876543210" {
		t.Errorf("int64: got %q", got)
	}
	if got := FormatSQLValue(int64(-42)); got != "-42" {
		t.Errorf("negative int64: got %q", got)
	}
}

func TestFormatValue_int32(t *testing.T) {
	if got := FormatSQLValue(int32(12345)); got != "12345" {
		t.Errorf("int32: got %q", got)
	}
}

func TestFormatValue_uint64(t *testing.T) {
	// Values above int64 max must round-trip unsigned.
	if got := FormatSQLValue(uint64(18446744073709551615)); got != "18446744073709551615" {
		t.Errorf("uint64: got %q", got)
	}
}

func TestFormatValue_timeTime(t *testing.T) {
	// Microsecond-precision UTC literal matching the indexer convention.
	val := time.Date(2026, 4, 11, 14, 30, 45, 123456000, time.UTC)
	got := FormatSQLValue(val)
	if got != "'2026-04-11 14:30:45.123456'" {
		t.Errorf("time.Time: got %q", got)
	}
}

func TestFormatValue_timeTimeNonUTC(t *testing.T) {
	// A time.Time in another zone must be normalised to UTC before formatting.
	loc, _ := time.LoadLocation("America/New_York")
	val := time.Date(2026, 4, 11, 10, 30, 45, 0, loc) // 14:30:45 UTC
	got := FormatSQLValue(val)
	if got != "'2026-04-11 14:30:45.000000'" {
		t.Errorf("time.Time non-UTC: got %q", got)
	}
}

func TestFormatValue_byteSlice(t *testing.T) {
	// Binary blob as MySQL hex literal.
	val := []byte{0xde, 0xad, 0xbe, 0xef}
	got := FormatSQLValue(val)
	if got != "X'deadbeef'" {
		t.Errorf("[]byte: got %q", got)
	}
}

func TestFormatValue_emptyByteSlice(t *testing.T) {
	// Empty slice must still emit a valid MySQL hex literal.
	got := FormatSQLValue([]byte{})
	if got != "X''" {
		t.Errorf("empty []byte: got %q", got)
	}
}

func TestFormatValue_byteSliceWithNullByte(t *testing.T) {
	// Arbitrary non-UTF-8 bytes must survive via hex encoding.
	val := []byte{0x00, 0xff, 0x7f, 0x80}
	got := FormatSQLValue(val)
	if got != "X'00ff7f80'" {
		t.Errorf("arbitrary []byte: got %q", got)
	}
}

// ─── PostgreSQL dialect (#533) ──────────────────────────────────────────────────

func TestQuoteNamePG(t *testing.T) {
	cases := map[string]string{
		"id":     `"id"`,
		"My Col": `"My Col"`,
		`we"ird`: `"we""ird"`, // embedded double-quote doubled
	}
	for in, want := range cases {
		if got := quoteNamePG(in); got != want {
			t.Errorf("quoteNamePG(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEscapePGString(t *testing.T) {
	// standard_conforming_strings=on: double the single quote, leave backslash literal.
	cases := map[string]string{
		"O'Brien":                "O''Brien", // MySQL would emit O\'Brien → PG syntax error
		`C:\path`:                `C:\path`,  // backslash NOT doubled — MySQL would → silent corruption
		"plain":                  "plain",
		"":                       "",
		"a'b'c":                  "a''b''c",
		`back\slash and 'quote'`: `back\slash and ''quote''`, // both together
	}
	for in, want := range cases {
		if got := escapePGString(in); got != want {
			t.Errorf("escapePGString(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFormatValuePG(t *testing.T) {
	if got := formatValuePG(nil); got != "NULL" {
		t.Errorf("nil → %q, want NULL", got)
	}
	if got := formatValuePG("O'Brien"); got != "'O''Brien'" {
		t.Errorf("quote string → %q, want 'O''Brien'", got)
	}
	if got := formatValuePG(`C:\win`); got != `'C:\win'` {
		t.Errorf("backslash string → %q, want '%s' (literal backslash, not doubled)", got, `C:\win`)
	}
	// Defensive json.Number path: >2^53 verbatim, no float64 rounding.
	if got := formatValuePG(json.Number("18446744073709551615")); got != "18446744073709551615" {
		t.Errorf("json.Number → %q, want verbatim", got)
	}
	if got := formatValuePG(true); got != "true" {
		t.Errorf("bool true → %q, want true", got)
	}
	// Defensive structured-value path (mirrors FormatSQLValue): a stray structured
	// value must marshal to valid, quoted JSON, never panic or emit a bare Go %v
	// rendering. The unchanged-TOAST marker specifically can no longer reach this
	// function through the real path — GenerateSQLFromRows refuses up front (#592,
	// TestGenerateSQLFromRows_unresolvedToastMarker) — so this pins only the
	// last-resort rendering of a direct call.
	if got := formatValuePG(map[string]any{"some": "object"}); got != `'{"some":"object"}'` {
		t.Errorf("map → %q, want quoted JSON", got)
	}
}

func TestGeneratePG_ReverseInsertDialect(t *testing.T) {
	// A PostgreSQL-dialect reverse INSERT (from a DELETE event): double-quoted
	// identifiers + standard-conforming string escaping; NO MySQL backticks / \' / X''.
	g := NewForDialect(nil, nil, PostgresDialect)
	row := query.ResultRow{
		EventID: 1, SchemaName: "public", TableName: "t",
		EventType: parser.EventDelete,
		RowBefore: map[string]any{
			"id":   "1",
			"name": "O'Brien",
			"path": `C:\win`,
			"num":  "18446744073709551615",
		},
	}
	stmt, err := g.generateInsert(row)
	if err != nil {
		t.Fatalf("generateInsert: %v", err)
	}
	if !strings.Contains(stmt, `INSERT INTO "public"."t"`) {
		t.Errorf("want double-quoted schema.table, got: %s", stmt)
	}
	if strings.Contains(stmt, "`") {
		t.Errorf("PG SQL must not contain backticks: %s", stmt)
	}
	if !strings.Contains(stmt, `'O''Brien'`) {
		t.Errorf("want ''-doubled quote, got: %s", stmt)
	}
	if !strings.Contains(stmt, `'C:\win'`) || strings.Contains(stmt, `C:\\win`) {
		t.Errorf("backslash must stay literal (not doubled), got: %s", stmt)
	}
}

func TestGeneratePG_ReverseDeleteWhereDialect(t *testing.T) {
	// PG reverse DELETE (from an INSERT event): PK-scoped WHERE with a double-quoted id.
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"public.t": {
			Schema: "public", Table: "t",
			Columns:   []metadata.ColumnMeta{{Name: "id", IsPK: true}, {Name: "v"}},
			PKColumns: []string{"id"},
		},
	})
	g := NewForDialect(nil, resolver, PostgresDialect)
	row := query.ResultRow{
		EventID: 2, SchemaName: "public", TableName: "t",
		EventType: parser.EventInsert, SchemaVersion: 0,
		RowAfter: map[string]any{"id": "5", "v": "x"},
	}
	stmt, err := g.generateDelete(row)
	if err != nil {
		t.Fatalf("generateDelete: %v", err)
	}
	if !strings.Contains(stmt, `DELETE FROM "public"."t" WHERE "id" = '5'`) {
		t.Errorf("want PG PK-scoped WHERE, got: %s", stmt)
	}
	if strings.Contains(stmt, "`") {
		t.Errorf("PG SQL must not contain backticks: %s", stmt)
	}
}

// TestGenerate_MySQLDialectUnchanged guards the additive change: the default (MySQL)
// generator still emits MySQL-dialect SQL — backtick identifiers and backslash quote
// escaping, NOT the PG double-quote / doubled-single-quote forms — so the additive PG
// path did not alter the shipping MySQL output.
func TestGenerate_MySQLDialectUnchanged(t *testing.T) {
	g := newGen() // New(nil,nil) → MySQLDialect
	row := query.ResultRow{
		EventID: 1, SchemaName: "db", TableName: "t",
		EventType: parser.EventDelete,
		RowBefore: map[string]any{"id": "1", "name": "O'Brien"},
	}
	stmt, err := g.generateInsert(row)
	if err != nil {
		t.Fatalf("generateInsert: %v", err)
	}
	if !strings.Contains(stmt, "INSERT INTO `db`.`t`") {
		t.Errorf("MySQL dialect must use backticks, got: %s", stmt)
	}
	if !strings.Contains(stmt, `'O\'Brien'`) {
		t.Errorf("MySQL dialect must backslash-escape the quote, got: %s", stmt)
	}
}

func TestDialectForFlavor(t *testing.T) {
	cases := map[string]Dialect{
		"postgres": PostgresDialect,
		"mysql":    MySQLDialect,
		"mariadb":  MySQLDialect, // MariaDB recovery SQL is MySQL-dialect
		"":         MySQLDialect, // absent/unknown → MySQL
		"pg":       MySQLDialect, // only the exact canonical literal maps to Postgres
	}
	for flavor, want := range cases {
		if got := DialectForFlavor(flavor); got != want {
			t.Errorf("DialectForFlavor(%q) = %v, want %v", flavor, got, want)
		}
	}
}

func TestDialectForIndex_nilDB(t *testing.T) {
	// A nil db (e.g. agent.IndexDB before it's opened) must not panic — DialectForIndex
	// returns MySQLDialect, the safe default (#573).
	if got := DialectForIndex(nil); got != MySQLDialect {
		t.Errorf("DialectForIndex(nil) = %v, want MySQLDialect", got)
	}
}

// TestGeneratePG_ScriptWrapper pins the standard_conforming_strings guard: a PG-dialect
// script SET LOCALs it (so the escaping is self-defending regardless of the target
// session), and the MySQL script does NOT emit it. Conversely, a MySQL-dialect script
// pins time_zone='+00:00' (#757 — the captured literals are UTC with no zone marker,
// and a non-UTC target session would otherwise reinterpret them), and the PG script
// does NOT emit that guard (PG's own SET LOCAL standard_conforming_strings covers its
// escaping; timestamps there carry their own tz awareness).
func TestGeneratePG_ScriptWrapper(t *testing.T) {
	row := query.ResultRow{
		EventID: 1, SchemaName: "public", TableName: "t",
		EventType: parser.EventDelete, EventTimestamp: time.Unix(0, 0).UTC(),
		RowBefore: map[string]any{"id": "1"},
	}
	const scs = "SET LOCAL standard_conforming_strings = on;"
	const tz = "SET time_zone = '+00:00';"

	var pgBuf bytes.Buffer
	if _, err := NewForDialect(nil, nil, PostgresDialect).GenerateSQLFromRows([]query.ResultRow{row}, &pgBuf); err != nil {
		t.Fatalf("PG GenerateSQLFromRows: %v", err)
	}
	if !strings.Contains(pgBuf.String(), scs) {
		t.Errorf("PG script must contain %q, got:\n%s", scs, pgBuf.String())
	}
	if strings.Contains(pgBuf.String(), tz) {
		t.Errorf("PG script must NOT emit the MySQL time_zone guard, got:\n%s", pgBuf.String())
	}

	var myBuf bytes.Buffer
	if _, err := New(nil, nil).GenerateSQLFromRows([]query.ResultRow{row}, &myBuf); err != nil {
		t.Fatalf("MySQL GenerateSQLFromRows: %v", err)
	}
	if strings.Contains(myBuf.String(), "standard_conforming_strings") {
		t.Errorf("MySQL script must NOT emit the PG SCS guard, got:\n%s", myBuf.String())
	}
	if !strings.Contains(myBuf.String(), tz) {
		t.Errorf("MySQL script must contain %q, got:\n%s", tz, myBuf.String())
	}
}

// TestGenerateMySQL_PinsSQLMode is the #786 acceptance test: the MySQL preamble
// must pin a sql_mode BEFORE the reversal statements that (a) excludes
// NO_BACKSLASH_ESCAPES (EscapeString uses backslash escapes, so it would misparse
// the literals) and (b) excludes NO_ZERO_DATE/NO_ZERO_IN_DATE (so captured
// 0000-00-00 values apply) — while KEEPING STRICT_TRANS_TABLES so a value that no
// longer fits a narrowed column fails loud instead of being silently coerced.
// PG never emits sql_mode.
func TestGenerateMySQL_PinsSQLMode(t *testing.T) {
	row := query.ResultRow{
		EventID: 1, SchemaName: "db", TableName: "t",
		EventType: parser.EventInsert, EventTimestamp: time.Unix(0, 0).UTC(),
		RowAfter: map[string]any{"id": "1"},
	}
	const pin = "SET sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';"

	var myBuf bytes.Buffer
	if _, err := New(nil, nil).GenerateSQLFromRows([]query.ResultRow{row}, &myBuf); err != nil {
		t.Fatalf("MySQL GenerateSQLFromRows: %v", err)
	}
	out := myBuf.String()
	if !strings.Contains(out, pin) {
		t.Errorf("MySQL script must contain %q, got:\n%s", pin, out)
	}
	// The pin must NOT disable backslash escapes (EscapeString relies on them) nor
	// reject zero-dates — but STRICT_TRANS_TABLES is kept so a captured value that
	// no longer fits a narrowed column fails loud instead of being silently coerced.
	for _, banned := range []string{"NO_BACKSLASH_ESCAPES", "NO_ZERO_DATE", "NO_ZERO_IN_DATE"} {
		if strings.Contains(out, banned) {
			t.Errorf("MySQL sql_mode pin must NOT include %q, got:\n%s", banned, out)
		}
	}
	if !strings.Contains(out, "STRICT_TRANS_TABLES") {
		t.Errorf("MySQL sql_mode pin must KEEP STRICT_TRANS_TABLES (fail-loud on bad values), got:\n%s", out)
	}
	// The pin must precede the reversal statement so the mode is active when the
	// literals are parsed.
	pinAt := strings.Index(out, pin)
	stmtAt := strings.Index(out, "DELETE FROM")
	if stmtAt < 0 {
		t.Fatalf("expected a DELETE reversal statement, got:\n%s", out)
	}
	if pinAt < 0 || pinAt > stmtAt {
		t.Errorf("sql_mode pin (index %d) must come before the reversal statement (index %d), got:\n%s", pinAt, stmtAt, out)
	}

	var pgBuf bytes.Buffer
	if _, err := NewForDialect(nil, nil, PostgresDialect).GenerateSQLFromRows([]query.ResultRow{row}, &pgBuf); err != nil {
		t.Fatalf("PG GenerateSQLFromRows: %v", err)
	}
	if strings.Contains(pgBuf.String(), "sql_mode") {
		t.Errorf("PG script must NOT emit a sql_mode pin, got:\n%s", pgBuf.String())
	}
}

// ─── FormatFKCascadeRestore (ON UPDATE cascades, #1002) ──────────────────────

// TestFormatFKCascadeRestore_guardsOnCascadedValue pins the ON UPDATE CASCADE
// shape: the FK goes back to the OLD parent key, guarded on the NEW one InnoDB
// wrote there — so a child re-pointed after the cascade is never clobbered.
func TestFormatFKCascadeRestore_guardsOnCascadedValue(t *testing.T) {
	pk := []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "int"}}
	row := map[string]any{"id": json.Number("10"), "pid": json.Number("99")}
	got, err := FormatFKCascadeRestore("app", "child", "pid", json.Number("1"), json.Number("99"), pk, row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "UPDATE `app`.`child` SET `pid` = 1 WHERE `id` = 10 AND `pid` = 99"
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

// TestFormatFKCascadeRestore_nilCascadedValueIsNullGuard covers ON UPDATE SET
// NULL (and an ON UPDATE CASCADE to a NULL key): the guard degrades to IS NULL,
// which is exactly FormatSetNullRestore's rendering — a `= NULL` comparison
// would silently match no row.
func TestFormatFKCascadeRestore_nilCascadedValueIsNullGuard(t *testing.T) {
	pk := []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "int"}}
	row := map[string]any{"id": json.Number("10"), "pid": nil}
	got, err := FormatFKCascadeRestore("app", "child", "pid", json.Number("1"), nil, pk, row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "UPDATE `app`.`child` SET `pid` = 1 WHERE `id` = 10 AND `pid` IS NULL"
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
	same, err := FormatSetNullRestore("app", "child", "pid", json.Number("1"), pk, row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if same != got {
		t.Errorf("FormatSetNullRestore must stay byte-identical to the nil-cascadedValue form:\n got  %q\n want %q", same, got)
	}
}

// TestFormatFKCascadeRestore_stringKeysQuoted checks a CHAR/VARCHAR referenced
// key: both the restored value and the guard must be quoted + escaped.
func TestFormatFKCascadeRestore_stringKeysQuoted(t *testing.T) {
	pk := []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "int"}}
	row := map[string]any{"id": json.Number("10"), "code": "n'ew"}
	got, err := FormatFKCascadeRestore("app", "child", "code", "o'ld", "n'ew", pk, row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "SET `code` = 'o\\'ld'") || !strings.Contains(got, "AND `code` = 'n\\'ew'") {
		t.Errorf("string key not quoted/escaped on both sides: %q", got)
	}
}

// TestFormatFKCascadeRestore_fkInsidePK is the identifying-relationship case:
// the FK column is part of the child's PRIMARY KEY, so the ON UPDATE CASCADE
// moved the child's PK too. Building the PK predicate from the pre-cascade image
// AND appending the guard would name `pid` twice with contradictory values
// (`pid = 1 AND seq = 1 AND pid = 99`) — a predicate no row satisfies, so the
// restore would silently touch nothing while reporting success. The FK's PK term
// must carry the POST-cascade value and double as the guard.
func TestFormatFKCascadeRestore_fkInsidePK(t *testing.T) {
	pk := []metadata.ColumnMeta{
		{Name: "pid", IsPK: true, DataType: "int"},
		{Name: "seq", IsPK: true, DataType: "int"},
	}
	row := map[string]any{"pid": json.Number("1"), "seq": json.Number("1")} // pre-cascade image
	got, err := FormatFKCascadeRestore("app", "line", "pid", json.Number("1"), json.Number("99"), pk, row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "UPDATE `app`.`line` SET `pid` = 1 WHERE `pid` = 99 AND `seq` = 1"
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
	if strings.Count(got, "`pid` =") != 2 { // once in SET, once in WHERE — never twice in WHERE
		t.Errorf("the FK column must appear exactly once in the WHERE clause: %q", got)
	}
}

// TestFormatFKCascadeRestore_fkInsidePKNullCascadedRefuses: a PK column cannot
// be NULL, so "the cascade nulled a PK column" means the snapshot's PK no longer
// matches the live table. Emitting `pid IS NULL` would be a guaranteed no-op
// dressed as a recovery, so the formatter refuses instead.
func TestFormatFKCascadeRestore_fkInsidePKNullCascadedRefuses(t *testing.T) {
	pk := []metadata.ColumnMeta{
		{Name: "pid", IsPK: true, DataType: "int"},
		{Name: "seq", IsPK: true, DataType: "int"},
	}
	row := map[string]any{"pid": json.Number("1"), "seq": json.Number("1")}
	_, err := FormatFKCascadeRestore("app", "line", "pid", json.Number("1"), nil, pk, row)
	if err == nil {
		t.Fatal("want an error when the cascade nulled a column the snapshot calls part of the PK")
	}
	if !strings.Contains(err.Error(), "PRIMARY KEY") {
		t.Errorf("the error must name the drift it detected, got %q", err)
	}
}

// ─── FormatSetNullRestore ────────────────────────────────────────────────────

func TestFormatSetNullRestore_singlePKIntValue(t *testing.T) {
	// An integer FK value (json.Number, as it arrives from the read path) must
	// render as a bare numeric literal, and the guard `... AND fk IS NULL` must
	// always be present so the UPDATE is idempotent.
	pk := []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "int"}}
	row := map[string]any{"id": json.Number("10"), "pid": nil}
	got, err := FormatSetNullRestore("app", "child", "pid", json.Number("1"), pk, row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "UPDATE `app`.`child` SET `pid` = 1 WHERE `id` = 10 AND `pid` IS NULL"
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

func TestFormatSetNullRestore_stringFKValueQuoted(t *testing.T) {
	// A string FK value must be quoted+escaped; the IS NULL guard is unchanged.
	pk := []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "int"}}
	row := map[string]any{"id": json.Number("7")}
	got, err := FormatSetNullRestore("app", "child", "owner", "o'brien", pk, row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `UPDATE ` + "`app`.`child`" + ` SET ` + "`owner`" + ` = 'o\'brien' WHERE ` + "`id`" + ` = 7 AND ` + "`owner`" + ` IS NULL`
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

func TestFormatSetNullRestore_compositePK(t *testing.T) {
	// Every PK column joins the WHERE with AND, before the IS NULL guard.
	pk := []metadata.ColumnMeta{
		{Name: "tenant_id", IsPK: true, DataType: "int"},
		{Name: "id", IsPK: true, DataType: "int"},
	}
	row := map[string]any{"tenant_id": json.Number("3"), "id": json.Number("42")}
	got, err := FormatSetNullRestore("app", "child", "pid", json.Number("9"), pk, row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "UPDATE `app`.`child` SET `pid` = 9 WHERE `tenant_id` = 3 AND `id` = 42 AND `pid` IS NULL"
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

func TestFormatSetNullRestore_errNoPKColumns(t *testing.T) {
	_, err := FormatSetNullRestore("app", "child", "pid", json.Number("1"), nil, map[string]any{"id": json.Number("10")})
	if err == nil {
		t.Fatal("expected error for empty pkCols, got nil")
	}
	if !strings.Contains(err.Error(), "no PK columns") {
		t.Errorf("error should name the missing PK columns: %v", err)
	}
}

func TestFormatSetNullRestore_errPKColumnAbsentFromRow(t *testing.T) {
	pk := []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "int"}}
	_, err := FormatSetNullRestore("app", "child", "pid", json.Number("1"), pk, map[string]any{"pid": nil})
	if err == nil {
		t.Fatal("expected error for PK column absent from row, got nil")
	}
	if !strings.Contains(err.Error(), "absent") {
		t.Errorf("error should report the absent PK column: %v", err)
	}
}

// ─── identity / generated columns (#557) ───────────────────────────────────────

// identityGenResolver builds a resolver for table "public.t" with an identity-ALWAYS
// PK (id), a plain column (v), and a STORED generated column (g).
func identityGenResolver() *metadata.Resolver {
	return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"public.t": {
			Schema: "public", Table: "t",
			Columns: []metadata.ColumnMeta{
				{Name: "id", OrdinalPosition: 1, IsPK: true, IsIdentityAlways: true},
				{Name: "v", OrdinalPosition: 2},
				{Name: "g", OrdinalPosition: 3, IsGenerated: true},
			},
			PKColumns: []string{"id"},
		},
	})
}

func TestGeneratePG_ReverseInsert_IdentityAndGenerated(t *testing.T) {
	// reverse INSERT (from a DELETE): emit OVERRIDING SYSTEM VALUE, KEEP the identity
	// column (the real id is the point of recovery), OMIT the generated column.
	g := NewForDialect(nil, identityGenResolver(), PostgresDialect)
	row := query.ResultRow{
		EventID: 1, SchemaName: "public", TableName: "t",
		EventType: parser.EventDelete, SchemaVersion: 1,
		RowBefore: map[string]any{"id": "5", "v": "x", "g": "1"},
	}
	stmt, err := g.generateInsert(row)
	if err != nil {
		t.Fatalf("generateInsert: %v", err)
	}
	if !strings.Contains(stmt, "OVERRIDING SYSTEM VALUE") {
		t.Errorf("PG reverse INSERT must emit OVERRIDING SYSTEM VALUE, got: %s", stmt)
	}
	if !strings.Contains(stmt, `"id"`) {
		t.Errorf("identity column must be KEPT in the reverse INSERT, got: %s", stmt)
	}
	if strings.Contains(stmt, `"g"`) {
		t.Errorf("generated column must be OMITTED from the reverse INSERT, got: %s", stmt)
	}
}

func TestGeneratePG_ReverseUpdate_OmitsIdentityAndGenerated(t *testing.T) {
	// reverse UPDATE (from an UPDATE): the SET must OMIT both the identity-ALWAYS and
	// the generated column (PostgreSQL rejects SET on either), keeping only `v`.
	g := NewForDialect(nil, identityGenResolver(), PostgresDialect)
	row := query.ResultRow{
		EventID: 2, SchemaName: "public", TableName: "t",
		EventType: parser.EventUpdate, SchemaVersion: 1,
		RowBefore: map[string]any{"id": "5", "v": "old", "g": "1"},
		RowAfter:  map[string]any{"id": "5", "v": "new", "g": "2"},
	}
	stmt, err := g.generateUpdate(row)
	if err != nil {
		t.Fatalf("generateUpdate: %v", err)
	}
	setClause, _, _ := strings.Cut(stmt, " WHERE ")
	if !strings.Contains(setClause, `"v" = 'old'`) {
		t.Errorf("SET must restore the plain column, got: %s", stmt)
	}
	if strings.Contains(setClause, `"id" =`) {
		t.Errorf("SET must OMIT the identity-ALWAYS column (PG rejects it), got: %s", stmt)
	}
	if strings.Contains(setClause, `"g" =`) {
		t.Errorf("SET must OMIT the generated column (PG rejects it), got: %s", stmt)
	}
	// The PK-scoped WHERE still references the identity column (that's allowed).
	if !strings.Contains(stmt, `WHERE "id" = '5'`) {
		t.Errorf("WHERE must be PK-scoped on the identity column, got: %s", stmt)
	}
}

// TestGeneratePG_ReverseUpdate_KeepsByDefaultIdentity pins the deliberate distinction
// between GENERATED ALWAYS (omitted from SET) and GENERATED BY DEFAULT identity (KEPT
// in SET). A BY DEFAULT column has attidentity='d' → IsIdentityAlways=false, so it
// flows into the SET — which PostgreSQL allows AND which is required, since a BY
// DEFAULT identity can be changed by an UPDATE (a PK-changing UPDATE must be
// reversible). Verified against live PG: `UPDATE … SET id=…` succeeds on BY DEFAULT.
func TestGeneratePG_ReverseUpdate_KeepsByDefaultIdentity(t *testing.T) {
	r := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"public.t": {
			Schema: "public", Table: "t",
			Columns: []metadata.ColumnMeta{
				// BY DEFAULT identity PK: a real PK, but NOT identity-ALWAYS.
				{Name: "id", OrdinalPosition: 1, IsPK: true, IsIdentityAlways: false},
				{Name: "v", OrdinalPosition: 2},
			},
			PKColumns: []string{"id"},
		},
	})
	g := NewForDialect(nil, r, PostgresDialect)
	row := query.ResultRow{
		EventID: 1, SchemaName: "public", TableName: "t",
		EventType: parser.EventUpdate, SchemaVersion: 1,
		RowBefore: map[string]any{"id": "1", "v": "old"},
		RowAfter:  map[string]any{"id": "5", "v": "new"}, // PK changed by the original UPDATE
	}
	stmt, err := g.generateUpdate(row)
	if err != nil {
		t.Fatalf("generateUpdate: %v", err)
	}
	setClause, _, _ := strings.Cut(stmt, " WHERE ")
	if !strings.Contains(setClause, `"id" = '1'`) {
		t.Errorf("BY DEFAULT identity must be KEPT in SET (reversible PK change), got: %s", stmt)
	}
	if !strings.Contains(stmt, `WHERE "id" = '5'`) {
		t.Errorf("WHERE must target the post-UPDATE PK value, got: %s", stmt)
	}
}

// TestGenerate_MySQLIdentityUnaffected guards that the identity/generated handling is
// PG-only: a MySQL-dialect reverse INSERT emits NO OVERRIDING SYSTEM VALUE (MySQL
// AUTO_INCREMENT accepts explicit values).
func TestGenerate_MySQLIdentityUnaffected(t *testing.T) {
	g := New(nil, nil) // MySQL dialect
	row := query.ResultRow{
		EventID: 1, SchemaName: "db", TableName: "t",
		EventType: parser.EventDelete,
		RowBefore: map[string]any{"id": "5", "v": "x"},
	}
	stmt, err := g.generateInsert(row)
	if err != nil {
		t.Fatalf("generateInsert: %v", err)
	}
	if strings.Contains(stmt, "OVERRIDING SYSTEM VALUE") {
		t.Errorf("MySQL reverse INSERT must NOT emit OVERRIDING SYSTEM VALUE, got: %s", stmt)
	}
}

// ─── schema-drift detection (#601) ──────────────────────────────────────────────

// tableMeta601 builds a TableMeta with the given column names (ordinals assigned in
// order). The first column is marked PK so PK-scoped paths resolve.
func tableMeta601(cols ...string) *metadata.TableMeta {
	tm := &metadata.TableMeta{Schema: "shop", Table: "orders"}
	for i, c := range cols {
		col := metadata.ColumnMeta{Name: c, OrdinalPosition: i + 1}
		if i == 0 {
			col.IsPK = true
		}
		tm.Columns = append(tm.Columns, col)
	}
	return tm
}

// TestDriftedColumns covers the pure detector: a column emitted by the reversal that the
// event-time schema had but the current schema dropped/renamed is flagged; everything
// else (still-present columns, columns added after the event, columns not emitted, and
// missing schema knowledge) is not. This is the core of the #601 fail-loud decision.
func TestDriftedColumns(t *testing.T) {
	evt := tableMeta601("id", "customer", "coupon_code", "total")
	cur := tableMeta601("id", "customer", "total") // coupon_code dropped after the event

	cases := []struct {
		name    string
		emitted []string
		evt     *metadata.TableMeta
		cur     *metadata.TableMeta
		want    []string
	}{
		{"dropped column flagged", []string{"coupon_code", "customer", "id", "total"}, evt, cur, []string{"coupon_code"}},
		{"renamed: old name flagged", []string{"id", "promo"}, tableMeta601("id", "promo"), tableMeta601("id", "discount"), []string{"promo"}},
		{"no drift when all emitted columns still present", []string{"customer", "id", "total"}, evt, cur, nil},
		// A reverse-DELETE emits only the PK in its WHERE; the dropped non-PK column is
		// never referenced, so the recovery is valid and must NOT be refused.
		{"PK-only WHERE: dropped non-PK not flagged", []string{"id"}, evt, cur, nil},
		// A column ADDED after the event (and not re-snapshotted) is absent from the
		// event-time schema too — the gate prevents flagging it.
		{"added-after column not flagged", []string{"id", "newcol"}, tableMeta601("id"), tableMeta601("id", "newcol"), nil},
		{"duplicate emitted column deduped", []string{"coupon_code", "coupon_code"}, evt, cur, []string{"coupon_code"}},
		{"nil event-time meta disables detection", []string{"coupon_code"}, nil, cur, nil},
		{"nil current meta disables detection", []string{"coupon_code"}, evt, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := driftedColumns(tc.emitted, tc.evt, tc.cur)
			if !slices.Equal(got, tc.want) {
				t.Errorf("driftedColumns(%v) = %v, want %v", tc.emitted, got, tc.want)
			}
		})
	}
}

// TestSchemaDriftError_namesTablesAndColumns asserts the refusal error is actionable:
// it names every affected schema.table with its (sorted) drifted columns, in first-seen
// table order, and carries the fail-loud framing.
func TestSchemaDriftError_namesTablesAndColumns(t *testing.T) {
	err := schemaDriftError(
		map[string]map[string]bool{
			"shop.orders":   {"coupon_code": true},
			"shop.invoices": {"old_total": true, "memo": true},
		},
		[]string{"shop.orders", "shop.invoices"},
	)
	msg := err.Error()
	for _, want := range []string{
		"shop.orders (coupon_code)",
		"shop.invoices (memo, old_total)", // columns sorted within a table
		"refusing to emit",
		"dropped or renamed",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q; got: %s", want, msg)
		}
	}
	// Table order follows the supplied order slice (orders before invoices).
	if strings.Index(msg, "shop.orders") > strings.Index(msg, "shop.invoices") {
		t.Errorf("tables not in first-seen order: %s", msg)
	}
}

// TestSchemaDriftError_advisesTheSchemaEraCase pins the REMEDIATION half of #601's
// refusal, which the naming test above does not read.
//
// Drift is not always an accident. In a long-lived index the matched event can simply
// predate a DDL, so the reversal is right for the shape that existed then and wrong for
// the shape now. Neither re-snapshotting nor hand reconciliation fixes that; narrowing
// the window in time does, by selecting an event from the current schema era. The
// message used to offer only the two accident-shaped causes, so the most common
// occurrence read as unrecoverable.
//
// "NOT reversed" is the load-bearing half of the remedy, not decoration. Narrowing the
// window EXCLUDES the drifted events, so they go unreversed, and no downstream warning
// reports that a PK lost coverage that way. Presenting narrowing as a clean fix would
// make this the one place in the file that recommends a silently-incomplete recovery.
//
// The second loop is the #1114 rule. GenerateSQLFromRows is shared by the CLI, MCP and
// console, so this error reaches MCP clients, and a --flag named here is one the client
// cannot pass on its own surface (precedent: internal/mcptools/recover_cascade_test.go).
// It is a FORWARD guard: the pre-#1617 message named no flags either, so this loop never
// saw the old text fail. It is armed against the next edit, which is why the list is the
// specific CLI spellings and not a blanket "--" ban (#1271 keeps prose naming an
// operator-addressed shell command legal).
func TestSchemaDriftError_advisesTheSchemaEraCase(t *testing.T) {
	msg := schemaDriftError(
		map[string]map[string]bool{"shop.orders": {"coupon_code": true}},
		[]string{"shop.orders"},
	).Error()

	for _, want := range []string{
		"earlier shape of this table",           // the schema-era cause is named at all
		"Narrowing the recovery window in time", // and carries its own remedy
		"since/until",                           // spelled for every surface, not just the CLI
		"NOT reversed",                          // ...and the remedy states what it costs
		"re-snapshot",                           // the stale-snapshot cause survives
		"by hand",                               // hand reconciliation stays the option for the rest
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("remediation missing %q; got: %s", want, msg)
		}
	}

	for _, flag := range []string{"--since", "--until", "--pk", "--pks", "--limit"} {
		if strings.Contains(msg, flag) {
			t.Errorf("error leaks CLI flag %q to non-CLI surfaces: %s", flag, msg)
		}
	}
}

// TestGenerateSQLFromRows_noResolverNoDriftRefusal proves detection degrades safely:
// with no resolver (no schema knowledge), a reversal still emits normally — never
// blocked. Without this, file-indexed DBs and the nil-resolver fallback would break.
func TestGenerateSQLFromRows_noResolverNoDriftRefusal(t *testing.T) {
	g := newGen() // db=nil, resolver=nil
	rows := []query.ResultRow{{
		EventID: 1, SchemaName: "shop", TableName: "orders",
		EventType: parser.EventDelete,
		RowBefore: map[string]any{"id": "1", "coupon_code": "SAVE10"},
	}}
	var buf bytes.Buffer
	n, err := g.GenerateSQLFromRows(rows, &buf)
	if err != nil {
		t.Fatalf("no-resolver path must not refuse: %v", err)
	}
	if n != 1 || !strings.Contains(buf.String(), "INSERT INTO") {
		t.Errorf("expected a normal INSERT, got n=%d out=%s", n, buf.String())
	}
}

func TestDecodeStoredBase64_stringUnaffected(t *testing.T) {
	// Normal base64-stored TEXT/BLOB values must round-trip exactly as before.
	encoded := base64.StdEncoding.EncodeToString([]byte("hello"))
	if got := decodeStoredBase64(encoded, false); got != "hello" {
		t.Errorf("text decode: got %v, want %q", got, "hello")
	}
	if got := decodeStoredBase64(encoded, true); string(got.([]byte)) != "hello" {
		t.Errorf("binary decode: got %v, want []byte(hello)", got)
	}
	if got := decodeStoredBase64("not-base64!!", false); got != "not-base64!!" {
		t.Errorf("undecodable string must pass through unchanged, got %v", got)
	}
}

func TestDecodeStoredBase64_boolRepaired(t *testing.T) {
	// #736: a TEXT/BLOB value mis-promoted to a JSON bool by the pre-fix
	// marshalRow (e.g. the literal string "false") must be restored to its
	// original textual literal, not left as a stray Go bool.
	if got := decodeStoredBase64(false, false); got != "false" {
		t.Errorf("text: got %v (%T), want %q", got, got, "false")
	}
	if got := decodeStoredBase64(true, false); got != "true" {
		t.Errorf("text: got %v (%T), want %q", got, got, "true")
	}
	if got := decodeStoredBase64(false, true); string(got.([]byte)) != "false" {
		t.Errorf("binary: got %v, want []byte(false)", got)
	}
}

func TestDecodeStoredBase64_jsonNumberRepaired(t *testing.T) {
	// #736: a TEXT/BLOB value mis-promoted to a JSON number (e.g. the literal
	// string "123") must be restored to its original textual literal.
	if got := decodeStoredBase64(json.Number("123"), false); got != "123" {
		t.Errorf("text: got %v (%T), want %q", got, got, "123")
	}
	if got := decodeStoredBase64(json.Number("0"), true); string(got.([]byte)) != "0" {
		t.Errorf("binary: got %v, want []byte(0)", got)
	}
}

func TestDecodeStoredBase64_nilNotGuessed(t *testing.T) {
	// A value that decoded to Go nil (originally the string "null", per the
	// pre-fix bug, OR a genuine SQL NULL — indistinguishable after the fact)
	// must be left as nil rather than guessed at.
	if got := decodeStoredBase64(nil, false); got != nil {
		t.Errorf("expected nil to pass through unchanged, got %v", got)
	}
}

func TestGenerateInsert_repairsHistoricalCorruptedTextValue(t *testing.T) {
	// End-to-end regression pin for #736's actual reported symptom: a TEXT
	// column value from an event indexed BEFORE the marshalRow fix, where the
	// literal string "false" was mis-promoted and decoded back as a Go bool.
	// Without the decodeStoredBase64 repair, FormatSQLValue's `case bool`
	// would render this as the unquoted numeral 0 — exactly the corruption
	// verify caught on the reporting user's WordPress index. This must
	// produce the quoted string 'false', not 0.
	tm := &metadata.TableMeta{
		Schema: "wordpress",
		Table:  "dbt_options",
		Columns: []metadata.ColumnMeta{
			{Name: "option_id", OrdinalPosition: 1, IsPK: true, DataType: "bigint"},
			{Name: "option_value", OrdinalPosition: 2, DataType: "longtext"},
		},
		PKColumns: []string{"option_id"},
	}
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"wordpress.dbt_options": tm,
	})
	g := New(nil, resolver)
	row := query.ResultRow{
		EventID:    26491,
		SchemaName: "wordpress",
		TableName:  "dbt_options",
		EventType:  parser.EventInsert,
		RowBefore: map[string]any{
			"option_id":    json.Number("1509"),
			"option_value": false, // historically-corrupted decode of the string "false"
		},
	}
	stmt, err := g.generateInsert(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(stmt, "'false'") {
		t.Errorf("expected repaired value 'false' in generated SQL, got: %s", stmt)
	}
	if strings.Contains(stmt, "(1509, 0)") {
		t.Errorf("generated SQL still shows the unrepaired numeral 0 instead of 'false': %s", stmt)
	}
}

func TestDecodeStoredBase64_jsonContainerUnaffected(t *testing.T) {
	// #736 added "json" to base64StoredKind, so decodeStoredBase64 is now
	// invoked for JSON-typed columns too — a genuine JSON object/array value
	// (the overwhelmingly common case) must pass through completely
	// unchanged, not just fall into some accidental no-op.
	obj := map[string]any{"a": json.Number("1")}
	if got := decodeStoredBase64(obj, false); fmt.Sprintf("%v", got) != fmt.Sprintf("%v", obj) {
		t.Errorf("expected map[string]any to pass through unchanged, got %v (%T)", got, got)
	}
	arr := []any{json.Number("1"), json.Number("2")}
	if got := decodeStoredBase64(arr, false); fmt.Sprintf("%v", got) != fmt.Sprintf("%v", arr) {
		t.Errorf("expected []any to pass through unchanged, got %v (%T)", got, got)
	}
}

func TestBase64StoredKind_json(t *testing.T) {
	binary, ok := base64StoredKind("json")
	if !ok || binary {
		t.Errorf("expected json => (binary=false, ok=true), got (%v, %v)", binary, ok)
	}
}

// TestBase64StoredKind_binaryVarbinary pins #756: metadata.MapRow now
// reinterprets BINARY/VARBINARY as []byte, so they take the same base64
// storage path as BLOB and must be reported as decodable+binary here too.
func TestBase64StoredKind_binaryVarbinary(t *testing.T) {
	for _, dt := range []string{"binary", "varbinary", "BINARY", "VarBinary"} {
		binary, ok := base64StoredKind(dt)
		if !ok || !binary {
			t.Errorf("base64StoredKind(%q) = (%v,%v), want (true,true)", dt, binary, ok)
		}
	}
}

// storedGeometry builds the at-rest MySQL geometry buffer (SRID little-endian + WKB)
// that marshalRow base64-encodes into binlog_events, plus the base64 string a
// recovery row would carry, for the #788 geometry tests.
func storedGeometry(srid uint32, wkb []byte) string {
	buf := make([]byte, 4+len(wkb))
	binary.LittleEndian.PutUint32(buf[:4], srid)
	copy(buf[4:], wkb)
	return base64.StdEncoding.EncodeToString(buf)
}

// TestGeometryLiteral pins the pure at-rest decode (#788): a base64(SRID+WKB) value
// renders as ST_GeomFromWKB(X'<wkb hex>', <srid>) with the SRID split off the front,
// and a nil/non-string/too-short value declines (ok=false) so the caller falls back.
func TestGeometryLiteral(t *testing.T) {
	wkb := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x9a, 0x99, 0x99, 0x3f}
	stored := storedGeometry(4326, wkb)
	want := "ST_GeomFromWKB(X'" + hex.EncodeToString(wkb) + "', 4326)"
	if got, ok := geometryLiteral(stored); !ok || got != want {
		t.Errorf("geometryLiteral(SRID 4326) = (%q,%v), want (%q,true)", got, ok, want)
	}
	// SRID 0 (the Cartesian default) is emitted explicitly, not dropped.
	if got, ok := geometryLiteral(storedGeometry(0, wkb)); !ok ||
		got != "ST_GeomFromWKB(X'"+hex.EncodeToString(wkb)+"', 0)" {
		t.Errorf("geometryLiteral(SRID 0) = (%q,%v), want explicit ', 0)'", got, ok)
	}
	// Declines: nil, non-string, non-base64, and a value with no room for the 4-byte SRID.
	for _, v := range []any{nil, 123, "!!not-base64!!", base64.StdEncoding.EncodeToString([]byte{0x01})} {
		if got, ok := geometryLiteral(v); ok {
			t.Errorf("geometryLiteral(%v) = (%q,true), want ok=false so the caller can fall back", v, got)
		}
	}
}

// resolverWith builds an in-memory resolver (no DB) for the #788 typed-column tests.
// With db=nil, resolverForRow returns this resolver directly for every row, so
// buildInsert/buildUpdate type their columns from it.
func resolverWith(tables map[string]*metadata.TableMeta) *metadata.Resolver {
	return metadata.NewResolverFromTables(1, tables)
}

// TestBuildInsert_geometryEmitsSTGeomFromWKB is the #788 acceptance for the geometry
// half: a reverse INSERT for a table with a GEOMETRY column emits ST_GeomFromWKB(...)
// — a loadable statement — instead of the raw base64 string, which a geometry column
// cannot accept (and would topple the whole BEGIN/COMMIT script).
func TestBuildInsert_geometryEmitsSTGeomFromWKB(t *testing.T) {
	wkb := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef}
	stored := storedGeometry(4326, wkb)
	g := New(nil, resolverWith(map[string]*metadata.TableMeta{
		"db.places": {
			Schema: "db", Table: "places",
			Columns: []metadata.ColumnMeta{
				{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
				{Name: "location", OrdinalPosition: 2, DataType: "geometry"},
			},
			PKColumns: []string{"id"},
		},
	}))
	row := query.ResultRow{
		EventID: 1, SchemaName: "db", TableName: "places",
		EventType: parser.EventDelete,
		RowBefore: map[string]any{"id": "1", "location": stored},
	}
	stmt, err := g.generateInsert(row)
	if err != nil {
		t.Fatalf("generateInsert: %v", err)
	}
	wantLit := "ST_GeomFromWKB(X'" + hex.EncodeToString(wkb) + "', 4326)"
	if !strings.Contains(stmt, wantLit) {
		t.Errorf("geometry column must emit %q, got:\n%s", wantLit, stmt)
	}
	if strings.Contains(stmt, stored) {
		t.Errorf("geometry column must NOT emit the raw base64 string %q, got:\n%s", stored, stmt)
	}
}

// TestBuildUpdate_geometryEmitsSTGeomFromWKB covers the reverse-UPDATE SET clause for
// a geometry column (#788).
func TestBuildUpdate_geometryEmitsSTGeomFromWKB(t *testing.T) {
	wkb := []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0xca, 0xfe}
	stored := storedGeometry(0, wkb)
	g := New(nil, resolverWith(map[string]*metadata.TableMeta{
		"db.places": {
			Schema: "db", Table: "places",
			Columns: []metadata.ColumnMeta{
				{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
				{Name: "location", OrdinalPosition: 2, DataType: "point"},
			},
			PKColumns: []string{"id"},
		},
	}))
	row := query.ResultRow{
		EventID: 2, SchemaName: "db", TableName: "places",
		EventType: parser.EventUpdate,
		RowBefore: map[string]any{"id": "1", "location": stored},
		RowAfter:  map[string]any{"id": "1", "location": storedGeometry(0, []byte{0x09})},
	}
	stmt, err := g.generateUpdate(row)
	if err != nil {
		t.Fatalf("generateUpdate: %v", err)
	}
	wantLit := "`location` = ST_GeomFromWKB(X'" + hex.EncodeToString(wkb) + "', 0)"
	if !strings.Contains(stmt, wantLit) {
		t.Errorf("geometry SET must emit %q, got:\n%s", wantLit, stmt)
	}
}

// TestBase64StoredKind_vector pins #1144: VECTOR is decodable and binary, so a
// reversal emits its packed-float bytes as X'hex' instead of the stored base64
// as a quoted string (which a VECTOR column rejects, ER 6136, failing the whole
// BEGIN/COMMIT script at apply time).
func TestBase64StoredKind_vector(t *testing.T) {
	for _, dt := range []string{"vector", "VECTOR"} {
		binary, ok := base64StoredKind(dt)
		if !ok || !binary {
			t.Errorf("base64StoredKind(%q) = (%v,%v), want (true,true)", dt, binary, ok)
		}
	}
}

// TestBuildInsert_vectorEmitsHexLiteral is the #1144 acceptance for the VECTOR
// half: a reverse INSERT for a table with a VECTOR column emits the at-rest
// packed little-endian float32 bytes as X'hex' — verified against MySQL 9.7
// that X'<HEX(v)>' round-trips byte-identically into a VECTOR column — instead
// of the stored base64 as a string literal, which VECTOR cannot load.
func TestBuildInsert_vectorEmitsHexLiteral(t *testing.T) {
	// STRING_TO_VECTOR('[1,2,3]') at rest: float32 LE 1.0, 2.0, 3.0.
	packed := []byte{
		0x00, 0x00, 0x80, 0x3f,
		0x00, 0x00, 0x00, 0x40,
		0x00, 0x00, 0x40, 0x40,
	}
	stored := base64.StdEncoding.EncodeToString(packed)
	g := New(nil, resolverWith(map[string]*metadata.TableMeta{
		"db.items": {
			Schema: "db", Table: "items",
			Columns: []metadata.ColumnMeta{
				{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
				{Name: "embedding", OrdinalPosition: 2, DataType: "vector"},
			},
			PKColumns: []string{"id"},
		},
	}))
	row := query.ResultRow{
		EventID: 3, SchemaName: "db", TableName: "items",
		EventType: parser.EventDelete,
		RowBefore: map[string]any{"id": "1", "embedding": stored},
	}
	stmt, err := g.generateInsert(row)
	if err != nil {
		t.Fatalf("generateInsert: %v", err)
	}
	if want := "X'0000803f0000004000004040'"; !strings.Contains(stmt, want) {
		t.Errorf("vector column must emit %q, got:\n%s", want, stmt)
	}
	if strings.Contains(stmt, "'"+stored+"'") {
		t.Errorf("vector column must NOT emit the base64 string literal %q, got:\n%s", stored, stmt)
	}
}

// TestBuildUpdate_vectorEmitsHexLiteral covers the reverse-UPDATE SET clause
// for a VECTOR column (#1144).
func TestBuildUpdate_vectorEmitsHexLiteral(t *testing.T) {
	packed := []byte{0xcd, 0xcc, 0xcc, 0x3d} // float32 LE 0.1
	stored := base64.StdEncoding.EncodeToString(packed)
	g := New(nil, resolverWith(map[string]*metadata.TableMeta{
		"db.items": {
			Schema: "db", Table: "items",
			Columns: []metadata.ColumnMeta{
				{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
				{Name: "embedding", OrdinalPosition: 2, DataType: "vector"},
			},
			PKColumns: []string{"id"},
		},
	}))
	row := query.ResultRow{
		EventID: 4, SchemaName: "db", TableName: "items",
		EventType: parser.EventUpdate,
		RowBefore: map[string]any{"id": "1", "embedding": stored},
		RowAfter:  map[string]any{"id": "1", "embedding": base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 0})},
	}
	stmt, err := g.generateUpdate(row)
	if err != nil {
		t.Fatalf("generateUpdate: %v", err)
	}
	if want := "`embedding` = X'cdcccc3d'"; !strings.Contains(stmt, want) {
		t.Errorf("vector SET must emit %q, got:\n%s", want, stmt)
	}
}
