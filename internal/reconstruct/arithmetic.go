package reconstruct

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/dbtrail/dbtrail/internal/observe"
)

// The refresh's arithmetic (#2505). Every refresh with table deltas must
// leave, per table,
//
//	rows after = rows before + inserts - deletes
//
// where inserts and deletes are the window's INSERT and DELETE events (the
// fold counts them, foldResult.Inserts/Deletes), rows before is the resolved
// state the window was folded over and rows after the state it published.
// The numbers come from what the refresh already has; nothing here reads
// the base again. What closes against what differs by how the table was
// published:
//
// A pair (refreshPair). The pair holds one row per key the window touched:
// op "u" for a key the window leaves alive, a tombstone for one it leaves
// dead. Each INSERT event turns a key alive, each DELETE turns one dead and
// an UPDATE leaves it, so summed over the touched keys
//
//	alive after - alive before = inserts - deletes
//
// and alive after is the "u" rows, which gives the keys that were alive
// before the window, aliveBefore = upserts - inserts + deletes. That number
// must sit between 0 and the keys. At sequence 0 the base IS the state
// before the window, so the lookup must have found exactly those keys in
// it: dead == aliveBefore (a key the window inserted that sits in the base,
// or one it deleted that does not, is a delta over the wrong base; a key
// the lookup did not find is a dropped key). Later in a chain a touched key
// can also live in an earlier pair, or sit dead in the base, so only
// dead <= keys holds. Rows after is the running count rows before + inserts
// - deletes, stamped in the pair's footer for the next pair to start from.
//
// A rewrite (refreshRewrite). The merge read the resolved state before the
// window row by row (BaselineRows passed through, UpdatesApplied replaced,
// DeletesSkipped dropped) and wrote RowsWritten, so all four numbers are
// independent: rows before = BaselineRows + UpdatesApplied + DeletesSkipped,
// rows after = RowsWritten, which must equal BaselineRows + UpdatesApplied +
// InsertsEmitted and close against the events. When the chain carried a
// count, rows before must also equal it: that is where the running count of
// the pairs meets a state read in full.
//
// The worked case the rule must hold for: a key deleted and inserted again
// in one window. Events inserts = 1, deletes = 1; the pair holds the key
// once, alive ("u"), so aliveBefore = 1 - 1 + 1 = 1, the lookup found it in
// the base (dead = 1) and rows after = rows before. In a rewrite the key is
// an UpdatesApplied, InsertsEmitted - DeletesSkipped = 0 = inserts - deletes.
//
// A mismatch is logged at Error with every number and counted in
// bintrail_refresh_arithmetic_mismatch_total{table}; the table is published
// all the same, since a refresh that refuses on its own check turns a
// counting bug into a copy that silently ages, which is worse than a loud
// wrong number. Every table logs the numbers at Debug, so a mismatch can be
// read against its neighbours.

// refreshArithmetic is one table's numbers.
type refreshArithmetic struct {
	Schema, Table string
	// Snapshot names the snapshot being published.
	Snapshot string
	// Inserts and Deletes are the window's events.
	Inserts, Deletes int64
	// RowsBefore is the resolved rows before the window, -1 when unknown.
	RowsBefore int64

	// A pair: Seq is its sequence, Keys the rows of its upserts (alive and
	// tombstones), Upserts the alive ones, Dead the positions of its posdel.
	Pair                bool
	Seq                 int
	Keys, Upserts, Dead int64

	// Unchecked, when set, is why the numbers are logged but not checked:
	// a window across a known capture gap, whose lost events are the
	// mismatch, and one the gap's own warning already reports.
	Unchecked string

	// A rewrite: the merge's counters and the writer's tally.
	Rewrite                                                      bool
	BaselineRows, UpdatesApplied, DeletesSkipped, InsertsEmitted int64
	RowsWritten                                                  int64
}

// rowsAfter is the rows the published state holds, -1 when it cannot be
// told (a pair over an unknown count).
func (a refreshArithmetic) rowsAfter() int64 {
	if a.Rewrite {
		return a.RowsWritten
	}
	if a.RowsBefore < 0 {
		return -1
	}
	return a.RowsBefore + a.Inserts - a.Deletes
}

// rowsBefore is the resolved rows before the window as this table's
// publication knows them: a rewrite read them, a pair carries them.
func (a refreshArithmetic) rowsBefore() int64 {
	if a.Rewrite {
		return a.BaselineRows + a.UpdatesApplied + a.DeletesSkipped
	}
	return a.RowsBefore
}

// problems is what does not close, empty when everything does.
func (a refreshArithmetic) problems() []string {
	var out []string
	add := func(format string, args ...any) { out = append(out, fmt.Sprintf(format, args...)) }
	switch {
	case a.Rewrite:
		if got, want := a.RowsWritten, a.BaselineRows+a.UpdatesApplied+a.InsertsEmitted; got != want {
			add("rows written %d, want the %d the merge passed through, replaced and appended", got, want)
		}
		if got, want := a.InsertsEmitted-a.DeletesSkipped, a.Inserts-a.Deletes; got != want {
			add("the merge appended %d rows and dropped %d (net %d), but the window's events say net %d (%d inserts, %d deletes)",
				a.InsertsEmitted, a.DeletesSkipped, got, want, a.Inserts, a.Deletes)
		}
		if a.RowsBefore >= 0 && a.rowsBefore() != a.RowsBefore {
			add("the merge read %d rows of the state before the window, but the chain's count said %d", a.rowsBefore(), a.RowsBefore)
		}
	case a.Pair:
		aliveBefore := a.Upserts - a.Inserts + a.Deletes
		if a.Upserts > a.Keys {
			add("%d alive rows in a pair of %d keys", a.Upserts, a.Keys)
		}
		if aliveBefore < 0 || aliveBefore > a.Keys {
			add("%d keys were alive before the window by the events (%d alive after, %d inserts, %d deletes), which is not between 0 and the %d keys touched",
				aliveBefore, a.Upserts, a.Inserts, a.Deletes, a.Keys)
		}
		if a.Seq == 0 && a.Dead != aliveBefore {
			add("the lookup found %d of the touched keys in the base, but %d were alive before the window by the events (%d alive after, %d inserts, %d deletes): the base is not the state before the window, or a key was dropped",
				a.Dead, aliveBefore, a.Upserts, a.Inserts, a.Deletes)
		}
		if a.Seq > 0 && a.Dead > a.Keys {
			add("the lookup found %d base rows for %d touched keys", a.Dead, a.Keys)
		}
		if after := a.rowsAfter(); a.RowsBefore >= 0 && after < a.Upserts {
			add("rows after %d (%d before, %d inserts, %d deletes) is fewer than the %d keys the window leaves alive",
				after, a.RowsBefore, a.Inserts, a.Deletes, a.Upserts)
		}
	}
	return out
}

// report checks the numbers, fills rep and logs: Debug always, Error plus
// the counter on a mismatch. It never refuses the table.
func (a refreshArithmetic) report(rep *TableReport) {
	problems := a.problems()
	rep.WindowInserts, rep.WindowDeletes = a.Inserts, a.Deletes
	rep.RowsBefore, rep.RowsAfter = a.rowsBefore(), a.rowsAfter()
	rep.ArithmeticProblems = problems
	rep.ArithmeticChecked = a.Unchecked == ""
	how := "pair"
	if a.Rewrite {
		how = "rewrite"
	}
	attrs := []any{
		"schema", a.Schema, "table", a.Table, "snapshot", a.Snapshot, "published_as", how,
		"rows_before", rep.RowsBefore, "inserts", a.Inserts, "deletes", a.Deletes, "rows_after", rep.RowsAfter,
	}
	if a.Pair {
		attrs = append(attrs, "seq", a.Seq, "keys", a.Keys, "alive_rows", a.Upserts, "dead_rows", a.Dead)
	} else {
		// chain_rows is the count the chain carried into this rewrite (-1
		// for none), beside rows_before, the rows the merge read.
		attrs = append(attrs, "chain_rows", a.RowsBefore, "baseline_rows", a.BaselineRows, "updates_applied", a.UpdatesApplied,
			"deletes_skipped", a.DeletesSkipped, "inserts_emitted", a.InsertsEmitted, "rows_written", a.RowsWritten)
	}
	if a.Unchecked != "" {
		rep.ArithmeticProblems = nil
		slog.Debug("refresh arithmetic: rows after = rows before + inserts - deletes (not checked)", append(attrs, "unchecked", a.Unchecked)...)
		return
	}
	if len(problems) == 0 {
		slog.Debug("refresh arithmetic: rows after = rows before + inserts - deletes", attrs...)
		return
	}
	observe.RefreshArithmeticMismatch(a.Schema + "." + a.Table)
	slog.Error("refresh arithmetic does not close: rows after != rows before + inserts - deletes; the table is published as it is, check the copy against the source",
		append(attrs, "problems", strings.Join(problems, "; "))...)
}
