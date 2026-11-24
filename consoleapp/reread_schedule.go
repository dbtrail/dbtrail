package consoleapp

import (
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The schedule choosing the read of some tables (#2640, part 4).
//
// A scheduled update refused for tables that a statement changed without row
// changes to apply (a TRUNCATE, a table dropped and created again, a change
// of its columns: the refused-ddl verdict) is followed by an update that reads
// only those tables from the source and folds every table of the server to one
// point (reread_fold.go), instead of the full read of the whole server.
//
// It is tried at most once per table and verdict until an ordinary update goes
// through, and never in place of another read of the same kind. Anything else
// takes the path of before this part, unchanged: the full read in place of the
// refused update, under the daily cap (emergencyHeld), with the same words. So
// does every way the read can fail: capture not reaching it within its bound
// (a source that writes nothing after the read lands here too), an error, the
// fold refusing again, a renumbered binary log. A shutdown starts nothing, as
// today. The read itself is not held by the daily cap: it reads the refused
// tables, not the server.

// rereadKey is the bound's key for a refused table: one read per table and
// verdict, until an ordinary update goes through.
func rereadKey(t console.RefusedTable) string { return t.Name + "\x00" + t.Verdict }

// rereadChoice returns the tables a read of their own can fix after the update
// failed, or nil and why not. tried holds the keys already read again since
// the last ordinary update that went through.
func rereadChoice(failed console.BaselineStatus, tried map[string]bool) ([]string, string) {
	if failed.State != "failed" {
		return nil, "the update did not fail"
	}
	if len(failed.RefusedTables) == 0 {
		return nil, "the update was not refused for tables it named"
	}
	if failed.RefusedTablesOmitted > 0 || failed.Refused != len(failed.RefusedTables) {
		return nil, "the list of refused tables was cut short, so the tables to read are not all known"
	}
	var tables []string
	for _, rt := range failed.RefusedTables {
		if rt.Verdict != reconstruct.RefreshVerdictRefusedDDL {
			// refused-renumbered goes to a full read (#2640 amendment 6); a
			// capture gap is the server's, not one table's; the catch-all
			// verdict holds causes a read does not cure.
			return nil, fmt.Sprintf("%s was refused as %s, which a read of that table alone does not fix", rt.Name, rt.Verdict)
		}
		if tried[rereadKey(rt)] {
			return nil, fmt.Sprintf("%s was already read again alone since the last update that went through, and is refused again", rt.Name)
		}
		if !slices.Contains(tables, rt.Name) {
			tables = append(tables, rt.Name)
		}
	}
	if _, err := rereadTableList(tables); err != nil {
		return nil, err.Error()
	}
	if failed.Tables <= len(tables) {
		// Every table (or a count that was not reported): a run with nothing
		// folded the usual way has no snapshot to carry the record of skipped
		// views from (#2640 amendment 2), so it is an ordinary full read.
		return nil, "every table of the server was refused, which is a full read"
	}
	return tables, ""
}

// rereadUpdateWhy is the update's why (console.BackupScheduleState.
// LastUpdateWhy, the run history's update_why), worded as the clause the page
// puts after "This update ran because". The tables are counted, not named:
// the history reaches sessions whose data profile hides table names (see
// stuckReason). The log names them.
func rereadUpdateWhy(n int) string {
	what, those := "1 table", "that table alone was"
	if n != 1 {
		what, those = fmt.Sprintf("%d tables", n), "those tables alone were"
	}
	return "the update before it was refused for " + what + " changed by a statement that leaves no row changes to apply " +
		"(a TRUNCATE, a table dropped and created again, or a change of its columns), so " + those + " read again from the source and every other table was updated from the recorded changes, instead of a full read of the whole server"
}

// rereadInstead starts the read of some tables in place of the full read for
// the update that failed, when the choice allows it, and reports whether it
// started. false leaves the slot to fallBack's full read, as before. Called by
// fallBack with the entry it just read again.
func (b *backupScheduler) rereadInstead(e console.ServerEntry, failed console.BaselineStatus, now time.Time) bool {
	if !b.sup.rereadRefused || e.IsPostgres() {
		return false
	}
	b.mu.Lock()
	tried := maps.Clone(b.rereadTried[e.ID])
	_, observed := b.seen[e.ID]
	b.mu.Unlock()
	if !observed {
		// fallBack says it and starts nothing either.
		return false
	}
	tables, whyNot := rereadChoice(failed, tried)
	if tables == nil {
		if len(failed.RefusedTables) > 0 {
			slog.Info("snapshot schedule: the refused tables are not read again alone; the full read is taken in place of the update, as before",
				"server", e.Name, "id", e.ID, "reason", whyNot)
		}
		return false
	}
	// A read of some tables reads the source: where a full read may not
	// start, neither does it, and fallBack says why.
	if err := console.FullBackupPossible(e, b.gates()); err != nil {
		return false
	}
	p, err := e.BackupSchedule.Parse()
	if err != nil {
		return false
	}
	stamp := now.Format(time.RFC3339)
	req := b.rebuildRequest(e, p)
	req.Reread = &rereadOrder{Tables: tables, Source: console.BaselineRequestFor(e)}
	req.UpdateWhy = rereadUpdateWhy(len(tables))
	since, err := b.sup.TriggerRefresh(req, p.Every)
	if err != nil {
		// Another job took the server: the full read meets the same wall and
		// records the skip in today's words.
		slog.Warn("snapshot schedule: the read of the refused tables alone could not start; the full read is taken in place of the update, as before",
			"server", e.Name, "id", e.ID, "tables", tables, "error", err)
		return false
	}
	b.mu.Lock()
	if b.rereadTried[e.ID] == nil {
		b.rereadTried[e.ID] = make(map[string]bool)
	}
	for _, rt := range failed.RefusedTables {
		b.rereadTried[e.ID][rereadKey(rt)] = true
	}
	b.mu.Unlock()
	refused := failed
	b.recordStart(e, scheduledStart{method: console.BackupMethodRefresh, at: stamp, since: since, updateWhy: req.UpdateWhy,
		rereadFor: &refused, rereadTables: tables})
	slog.Warn("snapshot schedule: the update from the recorded changes was refused for tables a read of their own can fix; reading only the refused tables again instead of the whole server",
		"server", e.Name, "id", e.ID, "tables", tables, "reason", failed.LastError)
	b.watch(e, stamp, console.BackupMethodRefresh)
	return true
}

// afterReread is what follows the read of some tables, once it is terminal.
// A failure that published nothing takes the full read the refused update
// would have had, with that update's refusal: the same words and the same
// daily cap as before this part. A published snapshot whose upload failed and
// a disk that refused take no full read, as for any update.
func (b *backupScheduler) afterReread(e console.ServerEntry, own scheduledStart, last console.BaselineStatus) {
	if last.State == "succeeded" {
		slog.Info("snapshot schedule: the refused tables were read again alone and the update published",
			"server", e.Name, "id", e.ID, "tables", own.rereadTables)
		b.includeNewTables(e, last)
		return
	}
	if !fullReadStandsIn(last) {
		return
	}
	slog.Warn("snapshot schedule: reading the refused tables again did not publish a snapshot; the full read is taken in place of the refused update, as before",
		"server", e.Name, "id", e.ID, "tables", own.rereadTables, "reason", last.LastError)
	b.fallBack(e, *own.rereadFor, true)
}

// forgetRereads ends the bound: an ordinary update went through.
func (b *backupScheduler) forgetRereads(serverID string) {
	b.mu.Lock()
	delete(b.rereadTried, serverID)
	b.mu.Unlock()
}
