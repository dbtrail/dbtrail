package event

import (
	"fmt"
	"sort"
	"strings"
)

// UnchangedToastKey is the single key of the structurally-distinct marker the
// PostgreSQL decoder (internal/pgcapture) emits for an unchanged-TOAST value
// ('u') that could NOT be resolved from the before-image. It is a one-key
// map[string]any, deliberately NOT a plain string, so it can never collide with
// a real text column that legitimately holds the literal
// "<unchanged-toast>": the indexer serializes a map as a JSON object, while
// every real PG-path text value is a Go string, so the two are structurally
// distinct on disk and any consumer can detect the marker by a type switch on
// the reserved key.
//
// Under REPLICA IDENTITY FULL — the mode bintrail requires (#531) — the before-
// image always carries the real unchanged value, so the decoder resolves 'u'
// from it (RowAfter[col] = RowBefore[col]) and this marker is never persisted.
// The marker is only reachable under a weaker replica identity, where it keeps
// the column visible rather than silently dropped (the never-drop floor).
//
// The constant lives HERE, in the source-neutral event package, rather than in
// internal/pgcapture where the marker is produced: the read side (recovery,
// reconstruct, shim) must detect a residual marker and fail loud (#592), and
// the #528 depguard bans those packages from linking pgcapture (which pulls
// pglogrepl/pgx). pgcapture aliases this constant so the decoder keeps reading
// naturally.
//
// Forward constraint: the collision-freedom argument ("every real value is a Go
// string, the marker is a map") holds only while #530 stores all PG values as
// text. When #533 introduces type-faithful rendering (e.g. parsing jsonb into a
// map), non-string values appear and this structural distinctness must be
// re-validated. IsUnchangedToastMarker already matches STRICTLY (one-key map,
// key present, value true) so a real jsonb object only collides when it is
// byte-for-byte the reserved marker object itself.
const UnchangedToastKey = "__bintrail_unchanged_toast__"

// IsUnchangedToastMarker reports whether v is the residual unchanged-TOAST
// marker: exactly the one-key map the decoder emits
// (map[string]any{UnchangedToastKey: true}), as it round-trips through the
// index's JSON columns and back out of query.UnmarshalRowImage. The match is
// strict — a map with extra keys, a missing key, or a non-true value is NOT the
// marker — so a MySQL JSON column carrying user data can only trip it by
// storing the reserved marker object itself, which is indistinguishable from a
// real leak by construction.
func IsUnchangedToastMarker(v any) bool {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return false
	}
	flag, ok := m[UnchangedToastKey].(bool)
	return ok && flag
}

// UnresolvedToastColumns returns, sorted, the columns of a row image whose
// value is the unchanged-TOAST marker. A nil or marker-free image returns nil.
func UnresolvedToastColumns(image map[string]any) []string {
	var cols []string
	for k, v := range image {
		if IsUnchangedToastMarker(v) {
			cols = append(cols, k)
		}
	}
	sort.Strings(cols)
	return cols
}

// UnresolvedToastError builds the canonical fail-loud error for a residual
// unchanged-TOAST marker on the read side (#592). schema, table, and pkValues
// are best-effort locator context — empty values are elided so callers without
// row context (e.g. the shim's pure resultset builders) still produce a clean
// message.
func UnresolvedToastError(schema, table, pkValues string, cols []string) error {
	var loc strings.Builder
	if schema != "" || table != "" {
		fmt.Fprintf(&loc, " in %s.%s", schema, table)
		if pkValues != "" {
			fmt.Fprintf(&loc, " (pk=%s)", pkValues)
		}
	}
	colList := ""
	if len(cols) > 0 {
		colList = ", column(s) " + strings.Join(cols, ", ")
	}
	return fmt.Errorf("unresolved unchanged-TOAST marker%s%s — capture invariant violated: "+
		"under REPLICA IDENTITY FULL the PostgreSQL decoder resolves every unchanged TOAST value at decode "+
		"time, so a persisted marker means these events were captured without it (or a capture bug); the "+
		"stored image does not contain the real value, and output built from it would silently replace the "+
		"column with the marker. Set REPLICA IDENTITY FULL on the source table and re-capture, or restore "+
		"the column from a baseline", loc.String(), colList)
}

// CheckUnresolvedToast returns a fail-loud error if any value in any of the
// given row images is one of the two markers that stand in for a value the
// index does not hold; nil images are skipped. This is the shared choke-point
// helper for the read side: recovery scans both images of every row before
// rendering a reversal script, reconstruct scans every event it folds.
//
// The not-captured marker is looked for first, across every image, and wins:
// its error names the one action that helps (REPLICA IDENTITY FULL, for later
// changes). Otherwise the unchanged-TOAST marker gives the canonical #592
// error, exactly as before the not-captured marker existed.
func CheckUnresolvedToast(schema, table, pkValues string, images ...map[string]any) error {
	for _, img := range images {
		if cols := NotCapturedColumns(img); len(cols) > 0 {
			return &NotCapturedError{Schema: schema, Table: table, PKValues: pkValues, Columns: cols}
		}
	}
	for _, img := range images {
		if cols := UnresolvedToastColumns(img); len(cols) > 0 {
			return UnresolvedToastError(schema, table, pkValues, cols)
		}
	}
	return nil
}

// NotCapturedKey is the single key of the marker that stands for a column of
// a PostgreSQL earlier row (row_before) that was never sent. A table below
// REPLICA IDENTITY FULL (DEFAULT, or USING INDEX) sends, for a DELETE and for
// an UPDATE, only the identity columns of the earlier row, or nothing at all;
// every other column of that earlier row is this marker, never nil, because
// nil would read as a real NULL. Like UnchangedToastKey it is a one-key map
// ({NotCapturedKey: true}), structurally distinct from every real PostgreSQL
// value, which the index stores as text.
//
// The key is persisted in binlog_events row images and must never change. A
// MySQL image never carries it.
const NotCapturedKey = "__bintrail_not_captured__"

// IsNotCapturedMarker reports whether v is exactly the not-captured marker
// (map[string]any{NotCapturedKey: true}), as built by the decoder or read back
// from a stored row image. The match is strict, like IsUnchangedToastMarker: a
// map with extra keys, a missing key, or a value other than true is not it.
func IsNotCapturedMarker(v any) bool {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return false
	}
	flag, ok := m[NotCapturedKey].(bool)
	return ok && flag
}

// NotCapturedColumns returns, sorted, the columns of a row image whose value
// is the not-captured marker. A nil or marker-free image returns nil.
func NotCapturedColumns(image map[string]any) []string {
	var cols []string
	for k, v := range image {
		if IsNotCapturedMarker(v) {
			cols = append(cols, k)
		}
	}
	sort.Strings(cols)
	return cols
}

// NotCapturedError is the read side's refusal for a change whose earlier row
// was only partly captured: the PostgreSQL table was not at REPLICA IDENTITY
// FULL when the change was captured. Everything that needs the whole earlier
// row (undo, above all) refuses with it. Schema, Table and PKValues are
// best-effort context; empty values are left out of the message.
type NotCapturedError struct {
	Schema, Table, PKValues string
	Columns                 []string // sorted
}

func (e *NotCapturedError) Error() string {
	var b strings.Builder
	b.WriteString("cannot undo a change")
	if e.Table != "" {
		b.WriteString(" in ")
		if e.Schema != "" {
			b.WriteString(e.Schema + ".")
		}
		b.WriteString(e.Table)
		if e.PKValues != "" {
			fmt.Fprintf(&b, " (pk=%s)", e.PKValues)
		}
	}
	what := "the earlier row"
	if len(e.Columns) > 0 {
		what = "the earlier value of column(s) " + strings.Join(e.Columns, ", ")
	}
	target := "<table>"
	if e.Table != "" {
		target = quotePGIdent(e.Table)
		if e.Schema != "" {
			target = quotePGIdent(e.Schema) + "." + target
		}
	}
	fmt.Fprintf(&b, ": %s was not captured, because the table was not at REPLICA IDENTITY FULL when the change "+
		"was captured (PostgreSQL sent at most the row's key). Undo needs the whole earlier row. Run ALTER TABLE %s "+
		"REPLICA IDENTITY FULL on the source to make later changes undoable; it takes effect for changes made "+
		"after it, not for this one", what, target)
	return b.String()
}

// quotePGIdent double-quotes a PostgreSQL identifier, doubling any quote inside
// it. Always quoting keeps the printed command right for every name: unquoted,
// PostgreSQL folds Orders to orders, and a reserved word or a name with a space
// or a dot would not parse.
func quotePGIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
