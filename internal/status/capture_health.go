// Package status — this file builds the operator-facing account of a DEGRADED
// capture verdict (#1296). It is shared on purpose: `bintrail status` prints
// these lines and the console renders the same strings from
// /api/status (stream.capture_health.explanation), so the two surfaces cannot
// drift into telling one operator a different story than the other.
//
// What the old single sentence got wrong, and what every line here defends:
//
//   - It read as operator error. A table appearing on the source (an
//     application or a plugin creating one) is ordinary operation, not
//     misconfiguration, and the text must not imply otherwise.
//   - It named a remedy with no location. "Take a fresh snapshot" said nothing
//     about WHICH table, WHERE the button is, or that a schema snapshot is a
//     different artifact from a baseline.
//   - It implied the remedy closed the hole. It does not: a fresh snapshot
//     fixes capture from that point on, and the already-skipped events stay
//     missing unless the source's binlogs still cover the window.
//   - It sent everyone to `bintrail snapshot`, including the operators for whom
//     that can never converge (a validation-excluded table is excluded again by
//     every future snapshot — #1051/#1199).
package status

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

// Capture-skip reason keys, mirroring the parser's persisted vocabulary. Kept
// as independent decls for the same reason CaptureSkipStat is: this display
// package deliberately does not import the binlog parser.
const (
	CaptureSkipReasonTableNotInSnapshot        = "table_not_in_snapshot"
	CaptureSkipReasonTableExcludedFromSnapshot = "table_excluded_from_snapshot"
	CaptureSkipReasonColumnCountMismatch       = "column_count_mismatch"
	CaptureSkipReasonNoResolver                = "no_resolver"
)

// ExplainCaptureSkips renders the DEGRADED verdict's explanation: one
// cause/remedy pair per active reason (most events first), then the two lines
// that apply to every reason — what a remedy does NOT recover, and where the
// per-event detail lives. Returns nil when nothing was skipped, so a caller can
// use a nil result as "there is nothing to explain".
//
// Pure and fixture-drivable: no clock, no IO. The caller supplies the decoded
// ledger; every claim here is derived from it.
func ExplainCaptureSkips(skips map[string]CaptureSkipStat, snapshotAt time.Time) []string {
	active := activeReasons(skips)
	if len(active) == 0 {
		return nil
	}
	var lines []string
	for _, reason := range active {
		lines = append(lines, causeLine(reason, skips[reason]))
		lines = append(lines, remedyLine(reason))
	}
	// The scope caveat is unconditional and deliberately blunt: without it an
	// operator applies the remedy, sees capture go green, and assumes the hole
	// closed. It never did — the remedy is forward-only.
	lines = append(lines, "None of this recovers what was already skipped: those changes are absent "+
		"from the index for good unless the source still has the binlogs covering that window, in "+
		"which case `bintrail index --binlog-dir <dir> --files <file>` can re-read them.")
	// Say that the verdict itself persists. The tallies are monotonic and
	// re-seeded across restarts precisely so a skip episode cannot be laundered
	// away by a restart — which means a WORKING fix does not turn this banner
	// green, and an operator who does not know that concludes the fix failed and
	// applies it again.
	lines = append(lines, acknowledgementLine(skips, snapshotAt))
	lines = append(lines, logLine(active[0]))
	return lines
}

// SkipsPredateSnapshot reports whether every recorded skip happened BEFORE the
// schema snapshot capture decodes against today.
//
// It exists because the tally is monotonic — it counts skips that HAPPENED, not
// skips still happening — so it reads identically before and after a successful
// re-snapshot, which left the console's own "Refresh schema snapshot" button
// with no observable effect at all (#1312). The snapshot's own timestamp is the
// acknowledgement: no new column, and nothing erased.
//
// False when the anchor is missing (no snapshot time, or a ledger entry with no
// last_at): absence of evidence is not a clean window, and this verdict decides
// whether an operator is shown an alarm.
//
// There is deliberately no `if snapshotAt.IsZero()` guard: it would be dead
// code. No recorded skip can predate the zero time, so a zero anchor already
// falls out of the comparison below as false — and a guard that cannot be made
// to fail is a guard nobody can trust. The zero case IS load-bearing in
// acknowledgementLine, where it selects a different paragraph, and it is tested
// there.
func SkipsPredateSnapshot(skips map[string]CaptureSkipStat, snapshotAt time.Time) bool {
	active := activeReasons(skips)
	if len(active) == 0 {
		return false
	}
	for _, r := range active {
		if st := skips[r]; st.LastAt.IsZero() || !st.LastAt.Before(snapshotAt) {
			return false
		}
	}
	return true
}

// ackAdvice is the one place that names how to retire a skip record (#1314),
// shared by every sentence that used to say "stop the daemon and clear
// stream_state.capture_skips". That advice was wrong twice over: a console
// operator cannot do it at all, and it DESTROYS the loss record — the tally is
// the only evidence those events were dropped. Acknowledging keeps it and adds
// a timestamp.
//
// The sentence must carry the re-arming property, not just the button. An
// operator who reads "mark as read" as "make it go away for good" would never
// trust it on a system they are responsible for; the reason it is safe is that
// it retires a COUNT, so the next skip alarms again by itself.
const ackAdvice = "mark it as read — in the console, the \"Mark as read\" button on this box; on the " +
	"command line, `bintrail status --index-dsn <index> --ack-capture-skips`. That records the count you " +
	"saw and the time you saw it, and erases nothing: the tally stays, and if anything is skipped after " +
	"this the warning comes back on its own."

// acknowledgementLine answers the one question the tally cannot: is this still
// happening, or am I looking at a record of something already fixed?
//
// Neither branch says "resolved", and that restraint is the point. stream_state
// does not record WHICH snapshot capture is running on, so a newer snapshot
// proves one exists — not that the stream reloaded onto it (refreshSchemaSnapshot
// already treats "snapshot taken, stream did NOT reload" as its own outcome).
// And on a source with no writes, "nothing skipped since" is true for the
// trivial reason. What both branches report is the comparison itself, which is
// a fact; the caller's own preceding line has already said the skipped events
// are missing for good.
func acknowledgementLine(skips map[string]CaptureSkipStat, snapshotAt time.Time) string {
	if snapshotAt.IsZero() {
		return "This warning does not clear on its own: the tally counts skips that happened, not skips " +
			"still happening, so it stays after a successful fix. Confirm the fix by watching the count " +
			"stop rising, then " + ackAdvice
	}
	if SkipsPredateSnapshot(skips, snapshotAt) {
		return "Nothing has been skipped since the current schema snapshot was taken (" +
			snapshotAt.Format(TSFmt) + "). That is not proof the fix took hold — capture does not record " +
			"which snapshot it is running on, and a source with no writes skips nothing either way — but " +
			"no drop has been recorded against the layout capture decodes against today."
	}
	return "Events were skipped AFTER the current schema snapshot was taken (" + snapshotAt.Format(TSFmt) +
		"), so this is not a stale tally left over from an already-fixed problem: rows are being dropped " +
		"against the layout capture decodes against today."
}

// activeReasons lists the reasons with a non-zero count, most events first
// (ties alphabetical) — the same ordering captureSkipReasons uses, so the
// summary line and this explanation agree on which reason dominates.
func activeReasons(skips map[string]CaptureSkipStat) []string {
	var reasons []string
	for r, st := range skips {
		if st.Count > 0 {
			reasons = append(reasons, r)
		}
	}
	sort.Slice(reasons, func(i, j int) bool {
		if skips[reasons[i]].Count != skips[reasons[j]].Count {
			return skips[reasons[i]].Count > skips[reasons[j]].Count
		}
		return reasons[i] < reasons[j]
	})
	return reasons
}

// causeLine states what happened for one reason, naming the tables when the
// ledger recorded them.
func causeLine(reason string, st CaptureSkipStat) string {
	subject := namedTables(st)
	switch reason {
	case CaptureSkipReasonTableNotInSnapshot:
		return subject + " changed on the source but " + isAre(st) + " missing from the schema snapshot " +
			"capture decodes against, so those row events could not be indexed. A table appearing on the " +
			"source is ordinary — an application or plugin creating one is not a misconfiguration. What is " +
			"unusual is that capture did not follow it: the stream takes its own snapshot when it sees the " +
			"CREATE/ALTER, so this points at that path not running — the DDL happened while capture was " +
			"stopped, or the automatic snapshot failed, or the table falls outside this stream's " +
			"schema/table filter."
	case CaptureSkipReasonTableExcludedFromSnapshot:
		detail := ""
		if st.LastDetail != "" {
			detail = " (" + st.LastDetail + ")"
		}
		return subject + " " + isAre(st) + " left out of the schema snapshot ON PURPOSE by snapshot " +
			"validation" + detail + ". bintrail addresses rows by primary key on an InnoDB table; without " +
			"one it cannot index or reverse a change, so those row events are dropped."
	case CaptureSkipReasonColumnCountMismatch:
		return subject + " " + hasHave(st) + " a different number of columns in the binlog than in the " +
			"schema snapshot, so values would map to the wrong column names. Capture drops the rows rather " +
			"than index them under wrong names — the snapshot is behind a schema change on the source."
	case CaptureSkipReasonNoResolver:
		return "Capture ran with no schema snapshot loaded at all, so no row event could be decoded."
	case CaptureSkipReasonStatementFormatDML:
		return "The source wrote these changes as STATEMENT/MIXED-format events, which carry no row images. " +
			"There is nothing in the binlog to index — this is a source configuration, not a bintrail fault."
	case CaptureSkipReasonUnreadablePreviousLedger:
		return "The capture ledger persisted by a previous run could not be parsed, so an earlier tally of " +
			"skipped events may have been lost. This entry preserves the fact; it is not itself a skipped event."
	default:
		return subject + " had row events read from the stream and dropped under \"" + reason + "\"."
	}
}

// remedyLine gives the action for one reason. The schema-snapshot remedies say
// what a schema snapshot IS: the console's other button creates a BASELINE (a
// full copy of the data), and an operator who confuses the two runs a dump they
// did not need while capture stays broken.
func remedyLine(reason string) string {
	switch reason {
	case CaptureSkipReasonTableNotInSnapshot, CaptureSkipReasonColumnCountMismatch, CaptureSkipReasonNoResolver:
		return "Fix: refresh the schema snapshot for this source — the record of each table's columns that " +
			"capture decodes against (not a baseline, which is a full copy of the data). In the console: " +
			"Overview → \"Refresh schema snapshot\". On the command line: `bintrail snapshot --source-dsn " +
			"<source> --index-dsn <index>`, then restart the stream so it picks the new snapshot up."
	case CaptureSkipReasonTableExcludedFromSnapshot:
		return "Fix: give each table this reason covers an explicit PRIMARY KEY on an InnoDB engine at the source. " +
			"Re-snapshotting is NOT the fix here — validation excludes these tables again every time, so a " +
			"fresh snapshot would leave capture exactly as it is now."
	case CaptureSkipReasonStatementFormatDML:
		return "Fix: set binlog_format=ROW server-wide on the source (a session-level override can also " +
			"produce row-less events)."
	case CaptureSkipReasonUnreadablePreviousLedger:
		return "Fix: there is nothing to repair — the lost tally cannot be recovered. Once you have taken " +
			"whatever the possibility of unrecorded loss calls for, " + ackAdvice
	default:
		return "Fix: see DBTrail's log for this reason's detail."
	}
}

// logLine names the log to read and the exact line to look for. "check the
// capture log" named neither a location nor a string to grep, which is what
// made it useless; anything replacing it must name both.
func logLine(reason string) string {
	needle := map[string]string{
		CaptureSkipReasonTableNotInSnapshot:        "table not in snapshot — skipping",
		CaptureSkipReasonTableExcludedFromSnapshot: "table not in snapshot — skipping",
		CaptureSkipReasonColumnCountMismatch:       "column count mismatch — skipping",
		CaptureSkipReasonNoResolver:                "no resolver available — skipping",
	}[reason]
	s := "Per-event detail is in the log of the process capturing this source — `bintrail stream` or " +
		"`bintrail-console watch` (with the bundled compose file: `docker compose logs bintrail`)"
	if needle != "" {
		s += ", on the lines reading \"" + needle + "\""
	}
	return s + "."
}

// ScopeCaptureSkips returns the ledger with every table name the reader may
// not see removed and counted (#1452): for each reason, Tables keeps only the
// "schema.table" names visible accepts, and TablesWithheld records how many
// were dropped. Counts, timestamps and attribution are untouched — a count is
// not a name. A nil predicate returns the input as is, so an unrestricted
// reader renders the ledger verbatim with no copy made.
//
// The name is split at its first dot, which is how the capture daemon joined
// it (parser.addLedgerTable). A name that does not split is withheld: this
// filter decides what a restricted reader learns, and withholding more is the
// safe direction.
//
// Every renderer consumes the RETURNED map: the explanation must be rebuilt
// from the scoped ledger, never from the original, or the prose carries the
// names the list was just cleared of.
func ScopeCaptureSkips(skips map[string]CaptureSkipStat, visible func(schema, table string) bool) map[string]CaptureSkipStat {
	if visible == nil {
		return skips
	}
	out := make(map[string]CaptureSkipStat, len(skips))
	for reason, st := range skips {
		var kept []string
		for _, name := range st.Tables {
			schema, table, ok := strings.Cut(name, ".")
			if ok && visible(schema, table) {
				kept = append(kept, name)
			}
		}
		st.TablesWithheld = len(st.Tables) - len(kept)
		st.Tables = kept
		out[reason] = st
	}
	return out
}

// namedTables renders the ledger's table names as the subject of a sentence, or
// a neutral subject when the ledger has none. A ledger written before per-table
// attribution has an EMPTY list, which must never render as "no tables" — the
// tables exist, this index just cannot name them.
//
// Names withheld by the reader's data scope (TablesWithheld, #1452) are
// rendered as a count, so the sentence stays honest about how many tables the
// skip covers without naming one the reader may not read: "app.users and 2
// tables outside your access". A reason whose every table is withheld is
// still a fact worth a sentence — "1 table outside your access is missing
// from the schema snapshot" — and must never fall into the legacy-ledger
// wording, which claims the INDEX cannot name them.
func namedTables(st CaptureSkipStat) string {
	if len(st.Tables) == 0 && st.TablesWithheld == 0 {
		return "Rows of one or more tables (this index's ledger predates per-table attribution, so it cannot name them)"
	}
	subject := strings.Join(st.Tables, ", ")
	if st.TablesWithheld > 0 {
		withheld := fmt.Sprintf("%d tables outside your access", st.TablesWithheld)
		if st.TablesWithheld == 1 {
			withheld = "1 table outside your access"
		}
		if subject == "" {
			subject = withheld
		} else {
			subject += " and " + withheld
		}
	}
	if st.TablesTruncated {
		subject += " and others"
	}
	return subject
}

// isAre / hasHave keep the sentences grammatical for one table vs several, and
// for the unnamed-tables subject (always plural). A withheld table still
// counts as a table for number agreement: the sentence is about it even when
// it is not named.
func isAre(st CaptureSkipStat) string {
	if len(st.Tables)+st.TablesWithheld == 1 {
		return "is"
	}
	return "are"
}

func hasHave(st CaptureSkipStat) string {
	if len(st.Tables)+st.TablesWithheld == 1 {
		return "has"
	}
	return "have"
}

// wrapAt breaks text into lines of at most width runes at word boundaries, for
// the fixed-width text status output. A word longer than width is emitted on
// its own line rather than split — breaking a table name or a command in half
// would make it uncopyable, which is worse than an over-long line.
func wrapAt(text string, width int) []string {
	var (
		out  []string
		line strings.Builder
	)
	for _, word := range strings.Fields(text) {
		switch {
		case line.Len() == 0:
			line.WriteString(word)
		case line.Len()+1+len(word) <= width:
			line.WriteString(" ")
			line.WriteString(word)
		default:
			out = append(out, line.String())
			line.Reset()
			line.WriteString(word)
		}
	}
	if line.Len() > 0 {
		out = append(out, line.String())
	}
	return out
}

// writeCaptureSkipExplanation prints the explanation into the text status
// output, indented and wrapped to the width the rest of the report uses.
func writeCaptureSkipExplanation(w io.Writer, skips map[string]CaptureSkipStat, snapshotAt time.Time) {
	for _, para := range ExplainCaptureSkips(skips, snapshotAt) {
		for _, line := range wrapAt(para, 76) {
			fmt.Fprintf(w, "  %s\n", line)
		}
	}
}
