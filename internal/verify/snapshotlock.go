package verify

import (
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// What a difference means when a snapshot behind it may be torn (#1380).
//
// A snapshot taken with no locks copies its rows at different moments. Carry
// the older snapshot forward to the newer one's position and the two can
// differ with nothing wrong in the recorded changes: the difference is in the
// read. verify could not tell, and reported it as a mismatch of the chain.
//
// # The rule
//
// It applies to a MISMATCH only, and reads how each snapshot in the
// comparison was locked (baseline.ReadConsistencyOf):
//
//   - One of them is KNOWN to be torn: the table is inconclusive, and the
//     reason names the snapshot. The difference is kept in the reason.
//   - None is known torn, and one has NO RECORD of its locks: the table stays
//     a mismatch. The reason gains one sentence saying which snapshot has no
//     record.
//   - Every one is known consistent: a mismatch, as before.
//
// # Why no record does not excuse a difference
//
// Every snapshot written before the record existed has none. Reading that as
// "may be torn, so inconclusive" would turn every mismatch of every existing
// installation into an inconclusive on the day it upgrades, and hide the real
// ones. No record is no evidence, and a verdict is only ever softened on
// evidence. It costs an operator who did take no-lock snapshots before the
// upgrade a mismatch they have to read; the sentence added to it tells them
// where to look, and their next full snapshot carries a record.
//
// # Why this cannot become a run that never fails
//
// The exit code is not decided here. Report.ExitError keeps its rule: a run
// where no table was proven fails, so a source whose snapshots are all torn
// exits non-zero with every table inconclusive. A known torn snapshot softens
// only its own tables, and those count as inconclusive that need attention
// (no InconclusiveKind), never as "nothing to check".
type lockSide struct {
	// what names the snapshot in a sentence: "the snapshot of <time>".
	what string
	lock baseline.ReadConsistency
}

// withSnapshotLock applies the rule to one table's verdict and returns the
// verdict, the reason, and the worst lock of the snapshots compared.
func withSnapshotLock(st Status, detail string, sides ...lockSide) (Status, string, baseline.ReadConsistency) {
	locks := make([]baseline.ReadConsistency, len(sides))
	var torn, unrecorded []string
	for i, s := range sides {
		locks[i] = s.lock
		switch s.lock {
		case baseline.ReadTorn:
			torn = append(torn, s.what)
		case baseline.ReadConsistent:
		default:
			unrecorded = append(unrecorded, s.what)
		}
	}
	worst := baseline.WorstReadConsistency(locks...)
	if st != StatusMismatch {
		return st, detail, worst
	}
	detail = strings.TrimSuffix(strings.TrimSpace(detail), ".")
	switch {
	case len(torn) > 0:
		return StatusInconclusive, detail + ". " + capitalize(joinAnd(torn)) + " " + wasWere(torn) +
			" taken with no locks, so its rows were copied at different moments and the difference may come from that. " +
			"Take a full snapshot with locks to check this table", worst
	case len(unrecorded) > 0:
		return StatusMismatch, detail + ". " + capitalize(joinAnd(unrecorded)) + " " + doesDo(unrecorded) +
			" not record how it was locked, so it may have been taken with no locks. " +
			"A full snapshot taken with this version records it", worst
	}
	return st, detail, worst
}

func joinAnd(s []string) string {
	if len(s) == 2 {
		return s[0] + " and " + s[1]
	}
	return strings.Join(s, ", ")
}

func wasWere(s []string) string {
	if len(s) > 1 {
		return "were each"
	}
	return "was"
}

func doesDo(s []string) string {
	if len(s) > 1 {
		return "each do"
	}
	return "does"
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
