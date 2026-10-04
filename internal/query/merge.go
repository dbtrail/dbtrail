package query

import (
	"cmp"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
)

// maxDivergenceReports caps the per-merge divergence log (#841). One line per
// event would turn a systematically corrupt archive into a log flood that
// buries the first — and most useful — report; the tail is summarised instead.
const maxDivergenceReports = 5

// sameEvent reports whether two copies of one event_id carry the same event.
//
// It compares the fields that DEFINE the event and have existed since the
// original schema. The columns added later — connection_id (#701),
// query_text/query_hash (#699), commit_ts_us (#18) — are compared only when
// BOTH sides have a value: an archive written before a column existed loads it
// as NULL (see restore-index's per-file introspection), so treating
// "archive NULL, index set" as divergence would fire on every legacy archive
// in the fleet. That is the cry-wolf failure this warning exists to avoid
// becoming.
//
// SchemaVersion is deliberately NOT compared: it is the snapshot_id at index
// time, index-local bookkeeping rather than event content.
func sameEvent(a, b ResultRow) bool {
	if a.BinlogFile != b.BinlogFile || a.StartPos != b.StartPos || a.EndPos != b.EndPos ||
		!a.EventTimestamp.Equal(b.EventTimestamp) ||
		a.SchemaName != b.SchemaName || a.TableName != b.TableName ||
		a.EventType != b.EventType || a.PKValues != b.PKValues {
		return false
	}
	if !sameOptionalString(a.GTID, b.GTID) ||
		!sameOptionalString(a.QueryText, b.QueryText) ||
		!sameOptionalString(a.QueryHash, b.QueryHash) {
		return false
	}
	if a.ConnectionID != nil && b.ConnectionID != nil && *a.ConnectionID != *b.ConnectionID {
		return false
	}
	if a.CommitTsUS != nil && b.CommitTsUS != nil && *a.CommitTsUS != *b.CommitTsUS {
		return false
	}
	if !slices.Equal(a.ChangedColumns, b.ChangedColumns) {
		return false
	}
	return sameRowImage(a.RowBefore, b.RowBefore) && sameRowImage(a.RowAfter, b.RowAfter)
}

// sameOptionalString treats "one side absent" as agreement, for the same
// legacy-archive reason sameEvent documents; two present values must match.
func sameOptionalString(a, b *string) bool {
	if a == nil || b == nil {
		return true
	}
	return *a == *b
}

// sameRowImage compares two decoded row images.
//
// reflect.DeepEqual, not ==, and the reason is not stylistic: MySQL JSON
// columns decode to []any and map[string]any, and == on an `any` holding
// either PANICS with "comparing uncomparable type". A row image with a nested
// JSON array would take down every merged query.
//
// An earlier version rendered each value with fmt.Sprintf("%v") to dodge that.
// It dodged it, but %v erases type: json.Number("7") and "7" both render as 7,
// true and "true" both render as true. In MySQL JSON those are different
// values, and "an index row re-marshalled by a different generation of writer"
// is one of the two causes this warning names — so the comparison was blind to
// a case it exists to catch. It also allocated two strings per column per side
// on every duplicate row.
//
// (The %v version justified itself as bridging a json.Number/float64 split
// across the archive boundary. That split does not exist: both sides decode
// through UnmarshalRowImage, which sets dec.UseNumber(), so both are
// json.Number.)
//
// The length guard stays because DeepEqual(map[string]any{}, nil) is false,
// and a nil RowBefore on an INSERT must agree with an empty one.
func sameRowImage(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}

// MergeResults deduplicates rows by event_id, sorts by (event_timestamp, event_id)
// in the requested direction, and applies the limit. MySQL rows should be
// passed first so in the rare case of a duplicate event_id the index version
// is kept.
//
// order is normalised via OrderDirection — empty / "ASC" → ascending,
// "DESC" (case-insensitive) → descending. The same direction is applied to
// both sort keys so the ordering is total and deterministic.
func MergeResults(rows []ResultRow, limit int, order string) []ResultRow {
	merged, _ := MergeResultsReport(rows, limit, order)
	return merged
}

// MergeResultsReport is MergeResults plus the number of duplicate event_ids
// whose two copies DISAGREED (#1325). The slog.Warn below is the right channel
// for the CLI, but the console and MCP surfaces serve callers who never see
// the server log — for them a silent coin-flip between two disagreeing
// before-images is a data-integrity event that must land in the RESPONSE, the
// same split CaptureGapStatus makes for capture gaps (the finding travels even
// when policy lets the query proceed). Callers that log are fine with
// MergeResults, which discards the count.
func MergeResultsReport(rows []ResultRow, limit int, order string) ([]ResultRow, int) {
	seen := make(map[uint64]int, len(rows))
	unique := rows[:0]
	var diverged, reported int
	for _, r := range rows {
		if kept, dup := seen[r.EventID]; dup {
			// #841: which copy wins is a positional convention with nothing
			// enforcing it, and the losing copy used to be discarded in
			// silence. A duplicate is only supposed to happen while a
			// partition is archived-but-not-dropped, where the archive was
			// written byte-for-byte from the index — so the copies agreeing
			// is an INVARIANT, and an operator should hear about it breaking
			// rather than get an arbitrary one of two answers.
			//
			// The normal path pays one map lookup it already paid. The
			// comparison itself runs only on collision — usually rare, but
			// NOT always: a partition that is archived and then blocked from
			// dropping (a bucket-set/stamp-NULL archive_state row trips
			// pendingS3Uploads, and rotate then refuses to drop it) stays
			// duplicated in every query touching that hour until someone runs
			// `archive reconcile --repair`. That is why the comparison must
			// stay allocation-free.
			if !sameEvent(unique[kept], r) {
				diverged++
				if reported < maxDivergenceReports {
					reported++
					// No claim about WHICH source won: "index rows are
					// passed first" is the contract for the MySQL+archive
					// merge but false for the agent, which fetches its
					// buffer before the index (internal/agent/handler.go).
					// A BYOS operator told to trust the index copy would
					// reconcile against the wrong one of the two values.
					slog.Warn("merge: two copies of the same event disagree; keeping the one the caller passed first",
						"event_id", r.EventID,
						"schema", r.SchemaName, "table", r.TableName,
						"pk_values", r.PKValues,
						"detail", "an archived partition should be a byte-for-byte copy of the index rows; a mismatch means the index row changed after archiving, or two index generations wrote under the same bintrail_id")
				}
			}
			continue
		}
		seen[r.EventID] = len(unique)
		unique = append(unique, r)
	}
	if diverged > reported {
		slog.Warn("merge: further diverging duplicate events were not individually logged",
			"diverged_total", diverged, "logged", reported)
	}
	descending := OrderDirection(order) == "DESC"
	slices.SortFunc(unique, func(a, b ResultRow) int {
		c := a.EventTimestamp.Compare(b.EventTimestamp)
		if c == 0 {
			c = cmp.Compare(a.EventID, b.EventID)
		}
		if descending {
			return -c
		}
		return c
	})
	if limit > 0 && len(unique) > limit {
		unique = unique[:limit]
	}
	return unique, diverged
}

// MergeAndTrim runs the full post-fetch pipeline: dedup+sort, then the per-PK
// cap (which means "latest N per PK"), then the global cap. The sequence is
// load-bearing — applying the global cap before the per-PK cap can truncate
// early events for one PK and starve others.
//
// Internally we always sort ASC for the LimitPerPK pass (its reverse-walk
// "latest N per PK" semantics depend on ascending input — see LimitPerPK's
// precondition). After the per-PK trim, the rows are re-sorted in the
// caller's requested direction before the global cap is applied so the
// returned slice reflects the requested page (oldest N if ASC, newest N if
// DESC), not the wrong end of the ASC list.
//
// Used by FetchMerged and by the CLI merge path. Exposing this as a helper
// lets unit tests pin the ordering: a future refactor that swaps the sequence
// or drops the per-PK re-trim will fail TestMergeAndTrim_perPKBeforeGlobal.
//
// order is passed through to MergeResults (see its doc); pre-#1511 callers
// that pass "" get the historical ascending behavior.
func MergeAndTrim(rows []ResultRow, limit, limitPerPK int, order string) []ResultRow {
	merged, _ := MergeAndTrimReport(rows, limit, limitPerPK, order)
	return merged
}

// MergeAndTrimReport is MergeAndTrim plus the diverged-duplicate count from
// the dedup pass (#1325, see MergeResultsReport). Only the FIRST merge can
// observe a divergence — the optional DESC re-sort below runs over rows that
// are already deduplicated, so its count is structurally zero and is
// discarded.
func MergeAndTrimReport(rows []ResultRow, limit, limitPerPK int, order string) ([]ResultRow, int) {
	var diverged int
	if limitPerPK > 0 {
		// LimitPerPK requires ASC-sorted input to pick the timestamp-latest
		// events per PK.
		rows, diverged = MergeResultsReport(rows, 0, "ASC")
		rows = LimitPerPK(rows, limitPerPK)
		// Re-sort in the caller's direction before slicing — otherwise
		// "limit N + Order=DESC" would slice from the wrong end of the
		// ASC-ordered list.
		if OrderDirection(order) == "DESC" {
			rows = MergeResults(rows, 0, "DESC")
		}
	} else {
		rows, diverged = MergeResultsReport(rows, 0, order)
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, diverged
}

// LimitPerPK trims rows to keep at most n per pk_values, preserving the input
// ordering. Implementation: walk in reverse and keep the last n positional
// occurrences per PK, so the helper only returns the timestamp-latest events
// when the caller has already sorted the input ascending by
// (event_timestamp, event_id) — the shape produced by MergeResults.
//
// Precondition: input must be sorted ascending by (event_timestamp, event_id).
// When violated, the function still returns at most n rows per PK, but the
// kept rows are the last n *positionally*, not the timestamp-latest. The
// helper does not validate the sort order: the happy-path callers in this
// repo all feed it post-MergeResults output where the sort is guaranteed,
// and adding a sort check would hide a caller bug under a silent re-sort.
//
// n <= 0 returns rows unchanged.
//
// Used after MergeResults when each source has applied its own per-PK cap
// independently: the union can still exceed n per PK across sources, so a
// final post-merge trim enforces the contract.
func LimitPerPK(rows []ResultRow, n int) []ResultRow {
	if n <= 0 || len(rows) == 0 {
		return rows
	}
	counts := make(map[string]int, len(rows))
	keep := make([]bool, len(rows))
	// Walk in reverse so the latest events per PK are seen first and kept.
	for i := len(rows) - 1; i >= 0; i-- {
		pk := rows[i].PKValues
		// Drift rows (PKValues == "" from defensive scan of NULL
		// pk_values, dbtrail/bintrail#318) would otherwise all collapse
		// onto bucket "" and silently drop beyond the cap, AND collide
		// with any legitimate empty-PK rows. Give each its own per-row
		// bucket so they pass through unconditionally — the \x00
		// prefix can never appear in a real user-supplied PK string.
		if pk == "" {
			pk = fmt.Sprintf("\x00drift:%d", rows[i].EventID)
		}
		if counts[pk] < n {
			counts[pk]++
			keep[i] = true
		}
	}
	out := rows[:0]
	for i, r := range rows {
		if keep[i] {
			out = append(out, r)
		}
	}
	return out
}
