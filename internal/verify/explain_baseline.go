package verify

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// maxExplainRows caps how many differing rows the drill-down prints in detail.
// It runs only on an already-flagged mismatch (rare, opt-in via --explain); a
// pathological all-rows-differ table would otherwise dump the whole table. The
// total count is still reported, so nothing is silently hidden.
const maxExplainRows = 100

const (
	diffChanged = "changed" // both sides have the PK; some columns differ
	diffMissing = "missing" // in the new baseline, not reproduced by the recovery
	diffExtra   = "extra"   // reconstructed by the recovery, absent from the new baseline
)

// deferredCaveat is printed once per drill-down when a deferred-type column
// (ENUM/SET/JSON/binary) is among the diffs: its reconstructed value may be a
// binlog event image (ordinal/base64/Go-JSON) rather than MySQL's source text, so
// a shown value pair is not necessarily corruption. Surfaced rather than blanked
// so a genuine baseline-vs-baseline drift in such a column is never hidden.
const deferredCaveat = "  note: a deferred-type column (ENUM/SET/JSON/binary) is among the diffs: its reconstructed value may be an event image (ordinal/base64/JSON), not the source text; not necessarily corruption."

// CellDiff is one column whose reconstructed value diverged from the new
// baseline's, rendered in the SAME canonical text form the content digest
// compares — so a reported diff is exactly what made the digests differ.
type CellDiff struct {
	Column   string
	Recovery string // reconstruct(prev baseline + binlog → anchor)
	Baseline string // the new baseline (the "truth" side)
}

// RowDiff is one primary key whose reconstructed row differs from the new
// baseline. Kind is changed/missing/extra (see the diff* constants).
type RowDiff struct {
	PK    string
	Kind  string
	Cells []CellDiff // populated only for Kind == diffChanged
}

// MismatchExplanation is the row-level drill-down for one mismatched table: which
// primary keys diverged and how. It is the "which rows" the one-way content
// digest cannot give — produced by re-running the SAME reconstruction function
// (SnapshotFullTableImages) over the SAME fixed window (the BaselinePair's
// snapshots/anchor are immutable), so the row stream is byte-identical to the
// digest's by construction and the diff and the verdict cannot disagree. No live
// source, no external tool: a full outer join on the PK in a capped, spilling
// DuckDB session (rowJoin), so neither side is held whole in memory (#2385).
type MismatchExplanation struct {
	Schema, Table, Anchor string
	Diffs                 []RowDiff
	Total                 int            // total differing rows (Diffs is capped at maxExplainRows)
	byKind                map[string]int // per-kind totals, for the overflow breakdown
	deferredSeen          bool           // a deferred-type column appeared in a diff (drives the caveat)
	// OrderNote is query.LatestPerPKOrder.Note for the changes the drill-down
	// replayed: some rows were taken at their latest change by statement time
	// because binary log order could not be established (#2156), so a diff on
	// such a row can be the order's, not the data's. "" when there is none.
	OrderNote string
	// CascadeNote is BaselinePair.NewCascadeNote: the table has foreign keys
	// the server applies without writing them to the binary log, the likely
	// cause of rows the replay kept and the database no longer has. "" when
	// it has none.
	CascadeNote string
}

// rowCells is one reconstructed row reduced to its canonical per-column bytes
// (the digest's comparison form) plus a human-readable primary-key label.
type rowCells struct {
	pk    string
	cells map[string][]byte
}

// ExplainBaselinePairMismatch drills into a table VerifyBaselinePair already
// reported as StatusMismatch, producing a row-level diff between the recovery
// side (prev baseline reconstructed forward to the new baseline's anchor) and the
// new baseline (truth). It re-runs the SAME SnapshotFullTableImages reconstruction
// over the SAME fixed window the digest used, so the row stream is byte-identical
// by construction and the diff is guaranteed consistent with the verdict.
//
// It re-derives the setup VerifyBaselinePair computed (resolver/columns/changes).
// Because it runs only after a confirmed mismatch, the guards VerifyBaselinePair
// applied (PK present and supported, usable anchor, prev→new order) have already
// passed, so any failure here is a hard error, not an inconclusive verdict.
func ExplainBaselinePairMismatch(ctx context.Context, cfg BaselineConfig, p BaselinePair) (*MismatchExplanation, error) {
	tm, err := cfg.Resolver.Resolve(p.Schema, p.Table)
	if err != nil {
		return nil, fmt.Errorf("resolve %s.%s: %w", p.Schema, p.Table, err)
	}
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return nil, fmt.Errorf("%s.%s has no primary key", p.Schema, p.Table)
	}

	// Same column set as the digest: exactly what the baseline Parquet holds, via
	// ReadBaselineColumns (not the is_generated flag — see VerifyBaselinePair).
	colNames, err := reconstruct.ReadBaselineColumns(ctx, p.NewPath, cfg.DuckDBTuning)
	if err != nil {
		return nil, fmt.Errorf("read new baseline columns %s.%s: %w", p.Schema, p.Table, err)
	}
	colByName := make(map[string]metadata.ColumnMeta, len(tm.Columns))
	for _, c := range tm.Columns {
		colByName[c.Name] = c
	}
	orderedCols := make([]metadata.ColumnMeta, 0, len(colNames))
	for _, name := range colNames {
		cm, ok := colByName[name]
		if !ok {
			return nil, fmt.Errorf("baseline column %q absent from the index schema snapshot", name)
		}
		orderedCols = append(orderedCols, cm)
	}

	// Latest event per PK in (prev anchor, new anchor] — the same window
	// VerifyBaselinePair reconstructs the recovery side over (#797: PrevAnchor,
	// when recorded, replaces the imprecise PrevSnapshot DATETIME lower bound —
	// see VerifyBaselinePair's comment).
	// A window whose binlog numbering started over cannot be read by position
	// (#2174); VerifyBaselinePair calls it inconclusive, and the drill-down,
	// which the console can request on its own, refuses the same way. A
	// check that could not tell (#2186) leaves the verdict inconclusive with
	// its reason; the drill-down, a diagnostic of that same pair, still shows
	// the rows.
	if _, err := pairNumbering(ctx, cfg, p); err != nil {
		return nil, err
	}
	pg := cfg.SourceFlavor == flavorPostgres
	engine := query.New(cfg.IndexDB)
	// Same window as VerifyBaselinePair (baselineFetchOptions) so the drill-down sees exactly the rows the verdict's digest saw.
	var order query.LatestPerPKOrder
	rows, _, err := query.FetchMerged(ctx, cfg.IndexDB, engine, baselineFetchMerged(cfg, p, pg, &order))
	if err != nil {
		return nil, fmt.Errorf("fetch changes %s.%s: %w", p.Schema, p.Table, err)
	}
	// The SAME normalization passes VerifyBaselinePair ran before its digest
	// (ENUM/SET labels #769, BLOB/TEXT base64 #672): without them this
	// drill-down's row stream would differ from the one the digest hashed and
	// the diff could disagree with the verdict.
	reconstruct.MapEventEnumLabels(cfg.IndexDB, cfg.Resolver, p.Schema, p.Table, rows)
	reconstruct.DecodeEventBinaries(cfg.IndexDB, p.Schema, p.Table, rows)
	changes := make(map[string]*query.ResultRow, len(rows))
	for i := range rows {
		changes[rows[i].PKValues] = &rows[i]
	}

	// Both sides go to a DuckDB join with a memory cap and a spill directory
	// (#2385): the Go heap holds one row at a time, never a table.
	join, err := newRowJoin(ctx, cfg.DuckDBTuning)
	if err != nil {
		return nil, err
	}
	defer join.close()
	err = streamRowsByPK(ctx, p.NewPath, p.Schema, p.Table, pkCols, orderedCols, map[string]*query.ResultRow{}, nil, pg, cfg.DuckDBTuning, func(key string, rec rowCells) error {
		return join.addTruth(key, rec, orderedCols)
	})
	if err != nil {
		return nil, fmt.Errorf("read new baseline %s.%s: %w", p.Schema, p.Table, err)
	}
	err = streamRowsByPK(ctx, p.PrevPath, p.Schema, p.Table, pkCols, orderedCols, changes, rows, pg, cfg.DuckDBTuning, func(key string, rec rowCells) error {
		return join.addRecovery(key, rec, orderedCols)
	})
	if err != nil {
		return nil, fmt.Errorf("reconstruct prev %s.%s: %w", p.Schema, p.Table, err)
	}

	ex := &MismatchExplanation{Schema: p.Schema, Table: p.Table, Anchor: anchorLabel(pg, p), OrderNote: order.Note(), CascadeNote: p.NewCascadeNote}
	if err := join.diff(ctx, orderedCols, ex); err != nil {
		return nil, fmt.Errorf("compare %s.%s: %w", p.Schema, p.Table, err)
	}
	return ex, nil
}

// streamRowsByPK reconstructs baselinePath+changes and invokes emit once per row
// with its PK key (for joining) and canonical per-column bytes. It is the digest
// row stream (reconstructDigest) re-pointed at a diff instead of a hash — using
// the SAME renderCellNormalized reconstructDigest's baseline-anchored callers
// use, so a row this drill-down shows as differing is exactly a row the digest
// that flagged the mismatch also saw as differing (cellEqual's invariant).
func streamRowsByPK(ctx context.Context, baselinePath, schema, table string, pkCols, orderedCols []metadata.ColumnMeta, changes map[string]*query.ResultRow, events []query.ResultRow, pgTextPK bool, tuning duckdbutil.Tuning, emit func(key string, r rowCells) error) error {
	return reconstruct.SnapshotFullTableImages(ctx, reconstruct.SnapshotFullTableInput{
		BaselinePath: baselinePath,
		Schema:       schema,
		Table:        table,
		PKCols:       pkCols,
		Changes:      changes,
		Events:       events,
		PGTextPK:     pgTextPK,
		DuckDBTuning: tuning,
	}, func(rowMap map[string]any) error {
		key, pk := pkKeyAndDisplay(rowMap, pkCols)
		cells := make(map[string][]byte, len(orderedCols))
		for _, c := range orderedCols {
			cells[c.Name] = renderCellNormalized(rowMap[c.Name], c)
		}
		return emit(key, rowCells{pk: pk, cells: cells})
	})
}

// pkKeyAndDisplay builds a join key and a human label ("col=val, …"). Each PK
// part is length-prefixed before concatenation so a composite key is unambiguous
// even if a value contains a NUL byte (a VARCHAR/CHAR PK legally can) — two
// different multi-column keys can never collide. Both sides render the PK through
// the same renderCell, so equal logical keys produce equal join keys.
func pkKeyAndDisplay(rowMap map[string]any, pkCols []metadata.ColumnMeta) (key, display string) {
	var b strings.Builder
	dispParts := make([]string, len(pkCols))
	for i, c := range pkCols {
		v := renderCell(rowMap[c.Name], c)
		fmt.Fprintf(&b, "%d:", len(v))
		b.Write(v)
		dispParts[i] = c.Name + "=" + displayCell(v)
	}
	return b.String(), strings.Join(dispParts, ", ")
}

// displayCell renders a canonical cell for human output: NULL for a SQL NULL
// (renderCell returns nil), the text form otherwise. The text form is MySQL's
// source bytes for non-deferred types; a deferred-type column (ENUM/SET/JSON/
// binary) may instead carry an event-image form (see deferredCaveat), shown raw
// so a real baseline-vs-baseline drift is never hidden.
func displayCell(b []byte) string {
	if b == nil {
		return "NULL"
	}
	return string(b)
}

// cellEqual compares two canonical cell renderings with the SAME NULL-vs-empty
// distinction the content digest uses (rowHasher tags a nil/NULL differently from
// a zero-length value). bytes.Equal alone treats nil and []byte("") as equal,
// which would make the drill-down miss a NULL↔” divergence the digest flagged —
// breaking the "the diff agrees with the verdict" invariant.
func cellEqual(a, b []byte) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return bytes.Equal(a, b)
}

// Write prints the drill-down: one line per differing primary key.
func (ex *MismatchExplanation) Write(w io.Writer) {
	fmt.Fprintf(w, "\n--- mismatch drill-down: %s.%s @ %s ---\n", ex.Schema, ex.Table, ex.Anchor)
	fmt.Fprintln(w, "  recovery = previous baseline reconstructed to the anchor; baseline = the new baseline (truth)")
	if ex.deferredSeen {
		fmt.Fprintln(w, deferredCaveat)
	}
	if ex.OrderNote != "" {
		fmt.Fprintln(w, "  "+ex.OrderNote)
	}
	if ex.CascadeNote != "" {
		fmt.Fprintln(w, "  "+ex.CascadeNote+".")
	}
	if ex.Total == 0 {
		// A mismatch can be flagged on row COUNT while every matched PK lines up
		// cell-for-cell. Say so rather than print nothing.
		fmt.Fprintln(w, "  no per-row content differences found; the mismatch is in row count, not row content")
		return
	}
	for _, d := range ex.Diffs {
		switch d.Kind {
		case diffChanged:
			parts := make([]string, len(d.Cells))
			for i, c := range d.Cells {
				parts[i] = fmt.Sprintf("%s: recovery=%s baseline=%s", c.Column, c.Recovery, c.Baseline)
			}
			fmt.Fprintf(w, "  ~ %s\t%s\n", d.PK, strings.Join(parts, "; "))
		case diffMissing:
			fmt.Fprintf(w, "  + %s\t(in the new baseline, NOT reproduced by the recovery)\n", d.PK)
		case diffExtra:
			fmt.Fprintf(w, "  - %s\t(reconstructed by the recovery, absent from the new baseline)\n", d.PK)
		}
	}
	if ex.Total > len(ex.Diffs) {
		over := ex.Total - len(ex.Diffs)
		// Break the overflow down by kind (missing first) so the data-loss class —
		// rows the recovery never reproduced — is never invisible behind the
		// changed/extra rows that filled the cap first.
		if bd := ex.overflowBreakdown(); bd != "" {
			fmt.Fprintf(w, "  … and %d more differing row(s): %s\n", over, bd)
		} else {
			fmt.Fprintf(w, "  … and %d more differing row(s)\n", over)
		}
	}
}

// overflowBreakdown summarizes the differing rows that exceeded the cap, by kind
// (missing/changed/extra). Returns "" when per-kind totals are unavailable (an
// explanation built without them), so Write falls back to a plain count.
func (ex *MismatchExplanation) overflowBreakdown() string {
	if len(ex.byKind) == 0 {
		return ""
	}
	shown := map[string]int{}
	for _, d := range ex.Diffs {
		shown[d.Kind]++
	}
	var parts []string
	for _, k := range []struct{ kind, label string }{
		{diffMissing, "missing (not reproduced)"},
		{diffChanged, "changed"},
		{diffExtra, "extra"},
	} {
		if n := ex.byKind[k.kind] - shown[k.kind]; n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, k.label))
		}
	}
	return strings.Join(parts, ", ")
}
