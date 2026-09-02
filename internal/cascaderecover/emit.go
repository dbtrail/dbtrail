// Package cascaderecover assembles the cascade-recovery SQL script (the
// documented preamble, the SET FOREIGN_KEY_CHECKS=0/1 wrapper, the CASCADE
// re-INSERTs, and the idempotent SET NULL restorations) from a synthesized
// cascade result.
//
// It is the binary-neutral home for the emission logic that used to live in the
// `recover-cascade` CLI command: factoring it out lets the CLI and the console
// (#577) produce BYTE-IDENTICAL scripts that cannot drift. The package depends
// on cascade (for the SetNullRestore rows), recovery (the reversal-SQL
// generator), metadata (the child PK columns), and query (the row type); nothing
// it depends on imports it back, so it stays a leaf composition layer (no import
// cycle) and keeps the cascade synthesis engine free of any emission/recovery
// coupling.
//
// It owns no cobra flags and no command globals. Callers keep their own flag
// parsing and exit-code mapping; the structured coverage (cascade.Result's
// Incomplete/Complete()) stays with the caller so each surface can present it.
package cascaderecover

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

// Header carries the values rendered into the SQL preamble. Parents and Children
// cannot be derived from EmitSQL's flattened rows (it cannot split the
// parents++victims concat back apart), so the caller supplies them; the SET NULL
// count is NOT carried here — EmitSQL derives it from len(setNullRows) so a
// caller can never desync the header count from the statements emitted.
type Header struct {
	Schema, Table string
	// Parents is the number of PARENT rows whose own change is reversed in this
	// script — deleted parents plus the parent-key UPDATEs that actually cascaded
	// (cascade.Result.KeyUpdateParents). Children is the cascade-deleted child
	// count.
	Parents, Children int
	// Caveats are reasons the recovery is PROVABLY PARTIAL (cascade.Result.
	// Incomplete) — rendered under the "!!! INCOMPLETE RECOVERY" banner below.
	// Never put an advisory-only note here; use Warnings instead (#618).
	Caveats []string
	// Warnings are advisory notes about an otherwise-COMPLETE recovery
	// (cascade.Result.Warnings — e.g. a Phase-2 baseline that fell back to an
	// older snapshot). Rendered visibly but WITHOUT the data-loss framing:
	// nothing here means a row is missing.
	Warnings       []string
	BaselineActive bool
	// Combined switches the preamble to the cascade-AWARE recover wording used
	// when the console auto-detects a cascade parent inside a normal recover: the
	// base reversal of the selected change(s) is composed with the synthesized
	// children in ONE script, so the parent count and the "recover-cascade"
	// framing no longer fit. The zero value (false) keeps the byte-identical
	// `recover-cascade` preamble used by the CLI and the explicit endpoint.
	Combined bool
}

// EmitSQL writes the documented preamble, the FK-checks-off wrapper, the CASCADE
// reversal statements (DELETE→INSERT via the generator), the ON DELETE SET NULL
// FK restorations and the ON UPDATE CASCADE / SET NULL key restorations (both
// idempotent guarded UPDATEs). Returns the total statement count. resolver
// supplies child PK columns for the restore WHERE clauses; gen must be built
// from the same (or an equivalent) resolver (recovery.New(db, resolver)).
func EmitSQL(w io.Writer, gen *recovery.Generator, rows []query.ResultRow, setNullRows []cascade.SetNullRestore, keyUpdates []cascade.FKKeyRestore, resolver *metadata.Resolver, hdr Header) (int, error) {
	n, _, err := EmitSQLIndexed(w, gen, rows, setNullRows, keyUpdates, resolver, hdr)
	return n, err
}

// EmitSQLIndexed is EmitSQL plus the byte offset just past each emitted
// statement, relative to the first byte written to w, in emission order (the
// generator's reversals first, then the SET NULL restorations, then the ON
// UPDATE key restorations). It lets a transport hand the script over in pieces
// without cutting a statement in half (#1438); see
// recovery.GenerateSQLFromRowsIndexed for why a scanner over the rendered text
// cannot do this safely. The script is byte-identical either way.
func EmitSQLIndexed(w io.Writer, gen *recovery.Generator, rows []query.ResultRow, setNullRows []cascade.SetNullRestore, keyUpdates []cascade.FKKeyRestore, resolver *metadata.Resolver, hdr Header) (int, []int, error) {
	// Enforce the recover script-size budget first (#654). GenerateSQLFromRows
	// re-checks it before rendering, but checking here keeps the refusal
	// precedence stable (budget outranks a SET NULL build error below) and also
	// bounds the pre-preamble render buffer (genBuf, further down).
	if err := gen.CheckScriptBudget(rows); err != nil {
		return 0, nil, err
	}

	// Fail loud on a residual unchanged-TOAST marker (#592) next, mirroring the
	// generator's own up-front refusal so its precedence over the SET NULL
	// build errors below is preserved.
	for _, row := range rows {
		if err := event.CheckUnresolvedToast(row.SchemaName, row.TableName, row.PKValues,
			row.RowBefore, row.RowAfter); err != nil {
			return 0, nil, err
		}
	}

	// Build every FK restoration — the ON DELETE SET NULL ones and the ON UPDATE
	// key ones — BEFORE writing a byte (all-or-nothing): a missing resolver, an
	// unresolvable table, or an absent PK column must abort the whole emit
	// cleanly. Returning mid-script would leave the parent/child INSERTs written
	// but drop the closing `SET FOREIGN_KEY_CHECKS=1`, handing the operator a
	// script that re-enables nothing.
	if resolver == nil && (len(setNullRows) > 0 || len(keyUpdates) > 0) {
		return 0, nil, fmt.Errorf("a schema snapshot is required to restore foreign keys an InnoDB cascade nulled or rewrote (run `bintrail snapshot`)")
	}
	var setNullStmts []string
	for _, sr := range setNullRows {
		tm, terr := resolver.Resolve(sr.Schema, sr.Table)
		if terr != nil {
			return 0, nil, fmt.Errorf("resolve %s.%s for SET NULL restore: %w", sr.Schema, sr.Table, terr)
		}
		stmt, ferr := recovery.FormatSetNullRestore(sr.Schema, sr.Table, sr.Column, sr.Value, tm.PKColumnMetas(), sr.Row)
		if ferr != nil {
			return 0, nil, ferr
		}
		setNullStmts = append(setNullStmts, stmt)
	}
	var keyUpdateStmts []string
	for _, kr := range keyUpdates {
		tm, terr := resolver.Resolve(kr.Schema, kr.Table)
		if terr != nil {
			return 0, nil, fmt.Errorf("resolve %s.%s for ON UPDATE cascade restore: %w", kr.Schema, kr.Table, terr)
		}
		stmt, ferr := recovery.FormatFKCascadeRestore(kr.Schema, kr.Table, kr.Column, kr.OldValue, kr.NewValue, tm.PKColumnMetas(), kr.Row)
		if ferr != nil {
			return 0, nil, ferr
		}
		keyUpdateStmts = append(keyUpdateStmts, stmt)
	}

	// Render the CASCADE reversal statements into a buffer BEFORE the preamble
	// touches w. GenerateSQLFromRows carries refusals that fire only while
	// rendering (per-event generation failures #784, schema drift #601); with
	// the preamble already on the writer such a refusal would strand a dangling
	// `SET FOREIGN_KEY_CHECKS=0` with no re-enable — in the CLI text path that
	// partial script is flushed to --output (#835). Buffering keeps EmitSQL
	// all-or-nothing for every generator refusal, present and future. Peak
	// memory stays bounded by the script budget enforced above.
	var genBuf bytes.Buffer
	n, genEnds, err := gen.GenerateSQLFromRowsIndexed(rows, &genBuf)
	if err != nil {
		return 0, nil, err
	}
	// Reconcile the generated statement count against the parent+victim rows
	// (#835): the generator emits exactly one statement per row or fails loud
	// (#784), so a deficit here means a row — e.g. a synthesized cascade victim
	// — vanished from the script without an error. Refusing makes a silently
	// incomplete cascade recovery impossible even if the generator regresses.
	if n != len(rows) {
		return 0, nil, fmt.Errorf("cascade recover: generator rendered %d of %d expected reversal statement(s); refusing to emit a script that silently drops row(s)", n, len(rows))
	}

	var b strings.Builder
	if hdr.Combined {
		fmt.Fprintf(&b, "-- bintrail recover (cascade-aware): undo %s.%s, including the foreign-key\n",
			recovery.SanitizeForComment(hdr.Schema), recovery.SanitizeForComment(hdr.Table))
		b.WriteString("-- ON DELETE / ON UPDATE CASCADE / SET NULL side effects InnoDB ran below the\n")
		b.WriteString("-- binlog (MySQL Bug #32506).\n")
		fmt.Fprintf(&b, "-- Re-creates %d cascade-deleted child row(s), restores %d SET NULL'd FK(s) and %d\n", hdr.Children, len(setNullRows), len(keyUpdates))
		b.WriteString("-- cascade-rewritten FK(s) alongside the reversal of the selected change(s).\n")
		b.WriteString("-- NEVER auto-applied.\n")
	} else {
		fmt.Fprintf(&b, "-- bintrail recover-cascade: reverse ON DELETE / ON UPDATE CASCADE / SET NULL side effects on %s.%s\n",
			recovery.SanitizeForComment(hdr.Schema), recovery.SanitizeForComment(hdr.Table))
		fmt.Fprintf(&b, "-- Reverses %d parent row change(s) and re-inserts %d cascade-deleted child row(s); restores\n", hdr.Parents, hdr.Children)
		fmt.Fprintf(&b, "-- %d SET NULL'd FK(s) and %d cascade-rewritten FK(s) that InnoDB removed/nulled/rewrote\n", len(setNullRows), len(keyUpdates))
		b.WriteString("-- below the binlog (MySQL Bug #32506). NEVER auto-applied.\n")
	}
	b.WriteString("--\n")
	if hdr.BaselineActive {
		b.WriteString("-- Phase-2 baseline fallback ACTIVE: children present in a covered baseline are\n")
		b.WriteString("-- reconstructed even if untouched within the window. Tables NOT covered by a\n")
		b.WriteString("-- baseline are flagged above. \"Complete\" means everything DETECTABLE was recovered.\n")
	} else {
		b.WriteString("-- Phase-1 (binlog-window) recovery: a child untouched within --lookback and not\n")
		b.WriteString("-- in a baseline is NOT reconstructed — pass --baseline-dir/--baseline-s3 to enable\n")
		b.WriteString("-- Phase-2 fallback. \"Complete\" means everything DETECTABLE was recovered.\n")
	}
	b.WriteString("--\n")
	b.WriteString("-- If you have already re-created a deleted parent, delete its INSERT below:\n")
	b.WriteString("-- SET FOREIGN_KEY_CHECKS=0 does NOT suppress PRIMARY KEY violations.\n")
	// Caveats and warnings are sanitized HERE, at the comment boundary, rather
	// than at their construction sites (#1120) — of which there are many, spread
	// across internal/cascade, internal/cli and internal/console, several
	// formatting in a schema/table name or an error string. Two reasons: the same
	// strings are also served as JSON by the console, where a line break is
	// harmless, so the boundary is the only place the constraint actually
	// applies; and a fix here cannot be bypassed by a future caveat added
	// anywhere upstream.
	if len(hdr.Caveats) > 0 {
		b.WriteString("--\n-- !!! INCOMPLETE RECOVERY — the result is provably partial:\n")
		for _, c := range hdr.Caveats {
			fmt.Fprintf(&b, "--   - %s\n", recovery.SanitizeForComment(c))
		}
	}
	// Warnings are deliberately NOT folded into the block above (#618): they are
	// advisory notes about a recovery that is otherwise COMPLETE, so they get a
	// distinct banner without the "INCOMPLETE" / "provably partial" language.
	if len(hdr.Warnings) > 0 {
		b.WriteString("--\n-- NOTE — advisory, recovery below is COMPLETE (nothing is missing):\n")
		for _, note := range hdr.Warnings {
			fmt.Fprintf(&b, "--   - %s\n", recovery.SanitizeForComment(note))
		}
	}
	b.WriteString("\nSET FOREIGN_KEY_CHECKS=0;\n\n")
	// Every write from here on goes through the counter, so a statement's end
	// offset is read off it rather than summed from the literals above — a
	// later added line cannot silently shift the offsets that follow it.
	cw := &countingWriter{w: w}
	if _, err := io.WriteString(cw, b.String()); err != nil {
		return 0, nil, err
	}

	// The generator's offsets are relative to genBuf, which starts here.
	ends := make([]int, 0, len(genEnds)+len(setNullStmts)+len(keyUpdateStmts))
	for _, e := range genEnds {
		ends = append(ends, cw.n+e)
	}
	if _, err := io.Copy(cw, &genBuf); err != nil {
		return 0, nil, err
	}

	// SET NULL restorations: idempotent UPDATEs (… AND fk IS NULL) that only
	// touch rows still in the post-cascade nulled state, so a re-run or a later
	// re-point of the child is never clobbered. Pre-built above, so nothing here
	// can fail after the INSERTs are already on disk.
	if len(setNullStmts) > 0 {
		if _, err := io.WriteString(cw, "\n-- SET NULL FK restorations (idempotent: only rows whose FK is still NULL):\n"); err != nil {
			return n, ends, err
		}
		for _, stmt := range setNullStmts {
			if _, werr := io.WriteString(cw, stmt+";\n"); werr != nil {
				return n, ends, werr
			}
			ends = append(ends, cw.n)
			n++
		}
	}

	// ON UPDATE cascade restorations: same idempotent shape as the SET NULL block
	// above, but guarded on the value the cascade actually left behind (the
	// parent's NEW key under ON UPDATE CASCADE, NULL under ON UPDATE SET NULL) —
	// so a child re-pointed after the cascade is never clobbered either.
	if len(keyUpdateStmts) > 0 {
		if _, err := io.WriteString(cw, "\n-- ON UPDATE cascade FK restorations (idempotent: only rows whose FK still holds the cascaded value):\n"); err != nil {
			return n, ends, err
		}
		for _, stmt := range keyUpdateStmts {
			if _, werr := io.WriteString(cw, stmt+";\n"); werr != nil {
				return n, ends, werr
			}
			ends = append(ends, cw.n)
			n++
		}
	}

	if _, err := io.WriteString(cw, "\nSET FOREIGN_KEY_CHECKS=1;\n"); err != nil {
		return n, ends, err
	}
	return n, ends, nil
}

// countingWriter tracks bytes delivered to w so EmitSQLIndexed can read a
// statement's end offset off the counter. Its recovery sibling exists for the
// same reason; the two are deliberately not shared, since exporting one would
// put a transport detail in the reversal generator's public API.
type countingWriter struct {
	w io.Writer
	n int
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += n
	return n, err
}

// MergeParentRoots combines the parent DELETE roots with the parent key-UPDATE
// roots into ONE chronological list, which is what EmitSQL's generator requires.
//
// recovery.GenerateSQLFromRows does not sort: it trusts the caller's order and
// reverses it, so the most recent change is undone first. Concatenating the two
// root sets — DELETEs, then UPDATEs — throws that away. A parent key-UPDATEd at
// t1 and DELETEd at t2 (both in the window) would emit the UPDATE-undo first,
// against a row the later-emitted INSERT has not re-created yet: it matches 0
// rows, and the INSERT then restores the POST-update image. The parent is left
// silently wrong.
//
// sort.SliceStable over the concatenation, rather than a two-list merge, so the
// result is correct even if a caller ever hands over a set that is not itself
// ascending; ties keep DELETEs before UPDATEs of the same (timestamp, id), which
// only synthetic rows without a real EventID can produce.
func MergeParentRoots(deletes, keyUpdates []query.ResultRow) []query.ResultRow {
	out := make([]query.ResultRow, 0, len(deletes)+len(keyUpdates))
	out = append(out, deletes...)
	out = append(out, keyUpdates...)
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].EventTimestamp.Equal(out[j].EventTimestamp) {
			return out[i].EventTimestamp.Before(out[j].EventTimestamp)
		}
		return out[i].EventID < out[j].EventID
	})
	return out
}
