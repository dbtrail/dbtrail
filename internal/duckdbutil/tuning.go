package duckdbutil

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// Tuning holds the two DuckDB resource knobs bintrail sets on its Parquet query
// session (threads + memory budget). A zero-valued field means "leave it
// unset", so DuckDB applies its own native default: threads = one per CPU core,
// memory_limit = ~80% of physical RAM. Both spill to temp_directory when
// exceeded, so leaving them unset is "let DuckDB use more RAM before spilling",
// not "unbounded" — the temp_directory backstop still prevents the OOM-killer.
//
// bintrail's shipped default (DefaultTuning) caps both low for small-container
// safety. Ultrafast lifts the cap by leaving them unset and letting DuckDB
// self-tune to the host (#509). Apply only emits a SET for a non-zero field, so
// these two constructors are not special-cased anywhere — the same struct
// expresses "default", "ultrafast", and any explicit operator override.
//
// Note the polarity: a zero Tuning{} is host-greedy on threads/memory (both
// unset → DuckDB native defaults), so a caller that wants the container-safe
// budget must call DefaultTuning() explicitly rather than rely on a zero value.
// (Ultrafast also sets S3Direct, so it is no longer the bare zero value.)
//
// temp_directory and preserve_insertion_order are deliberately NOT in this
// struct: they are not memory-for-speed trade-offs (the former is the spill
// backstop, the latter is a win-win that is safe under our explicit ORDER BY),
// so every caller sets them unconditionally regardless of Tuning — see
// SetTempDirectory, which every Apply call MUST be paired with (below).
type Tuning struct {
	// Threads is the DuckDB thread count (SET threads = N). 0 leaves it unset,
	// so DuckDB defaults to one thread per CPU core.
	Threads int
	// MemoryLimit is the DuckDB memory budget (SET memory_limit = '...'), e.g.
	// "4GB". Empty leaves it unset, so DuckDB defaults to ~80% of physical RAM.
	MemoryLimit string
	// S3Direct routes S3 archive reads through DuckDB's httpfs extension as one
	// parallel multi-file scan, instead of downloading each file to disk first.
	// It is NOT a DuckDB SET — Apply ignores it; parquetquery.FetchWithTuning
	// reads it to pick the S3 path. httpfs holds each scanned file in memory
	// OUTSIDE memory_limit, so this is the risky ultrafast lever (only Ultrafast
	// sets it) with peak RAM ≈ largest_file × Threads (#511).
	S3Direct bool
}

// DefaultTuning is bintrail's conservative, container-safe DuckDB budget — the
// exact values parquetquery.Fetch hardcoded before this struct existed (#510).
// Used by every long-lived/shared caller (shim, console, agent) and as the
// base the CLI tuning flags build on.
func DefaultTuning() Tuning { return Tuning{Threads: 2, MemoryLimit: "4GB"} }

// Ultrafast trades the container-safe cap for speed: threads/memory unset so
// DuckDB self-tunes to the host (all cores, ~80% RAM, spilling to
// temp_directory), plus S3Direct so S3 archives are read via httpfs in one
// parallel scan rather than downloaded first. For big boxes running the offline
// query/recover/reconstruct commands, not the small shared containers the shim
// runs in. See #509/#511.
func Ultrafast() Tuning { return Tuning{S3Direct: true} }

// Apply emits the SET statements for the non-zero fields on db. Best-effort,
// matching the rest of duckdbutil: a failed SET is logged at WARN and the
// session falls back to DuckDB's default for that knob rather than aborting the
// query. Call it on a freshly opened DuckDB session.
//
// MUST be paired with a SetTempDirectory call on the same session (before or
// after — order doesn't matter). Apply's memory_limit is a hard cap with
// nowhere to spill on its own; without the temp_directory backstop, a query
// that exceeds the cap fails outright instead of spilling to disk, which is
// WORSE than leaving memory_limit unset entirely. See SetTempDirectory.
func (t Tuning) Apply(ctx context.Context, db *sql.DB) {
	for _, stmt := range t.Statements() {
		applyTuningStmt(ctx, db, stmt)
	}
}

// Statements renders the SET statements Apply would run, one per non-zero
// field, for a caller that needs them under a different failure policy. Apply
// is best-effort by design (a shared daemon session must not abort a query
// over a knob); the SQL sandbox worker (internal/sqlsandbox) runs the same
// statements but treats a failed cap as fatal, because there the cap IS the
// guarantee.
func (t Tuning) Statements() []string {
	var stmts []string
	if t.Threads > 0 {
		stmts = append(stmts, fmt.Sprintf("SET threads = %d", t.Threads))
	}
	if t.MemoryLimit != "" {
		// Mirror parquetquery's temp_directory concatenation; escape single
		// quotes so an operator-supplied value can't break out of the literal.
		stmts = append(stmts, "SET memory_limit = '"+strings.ReplaceAll(t.MemoryLimit, "'", "''")+"'")
	}
	return stmts
}

// SetTempDirectory points a DuckDB session's temp_directory at the OS temp
// directory, unconditionally — it is the spill backstop for Apply's
// memory_limit (see Apply's doc comment: the two MUST be paired on every
// session), not a memory-for-speed trade-off, so it stays on even under
// Ultrafast.
//
// DuckDB's own default is a ".tmp" directory relative to the process's
// CURRENT WORKING DIRECTORY, not the OS temp dir — which fails outright in a
// container with a read-only rootfs and CWD "/": DuckDB can't create the
// spill directory, so a query that exceeds memory_limit dies instead of
// spilling. Call this on every freshly opened DuckDB session that also calls
// Apply (previously only parquetquery.FetchWithTuning did this; every other
// Tuning.Apply call site must call this too — see the #842 fix that
// extracted this from parquetquery into this shared home, one definition
// instead of duplicated copies).
func SetTempDirectory(ctx context.Context, db *sql.DB) {
	applyTuningStmt(ctx, db, "SET temp_directory = '"+strings.ReplaceAll(os.TempDir(), "'", "''")+"'")
}

func applyTuningStmt(ctx context.Context, db *sql.DB, stmt string) {
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		slog.Warn("could not configure DuckDB", "statement", stmt, "error", err)
	}
}
