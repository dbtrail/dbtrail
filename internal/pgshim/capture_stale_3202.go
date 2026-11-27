package pgshim

import (
	"context"
)

// A capture that stopped saving its state while the source kept writing
// (#3202).
//
// Whether a table's copy is current is decided from capture's record of the
// changes it dropped (a table below REPLICA IDENTITY FULL, a deferrable
// key), which capture saves with its position (not_captured.go reads it).
// A capture that saved its state once and then cannot save it again (the
// index refuses the write, the run fails before its save and starts again)
// keeps its later drops in its memory only. The record then reads as
// current, and a table capture drops reads as one it keeps whole.
//
// The host says when that may be the case (Binding.CaptureState): capture
// has saved nothing for longer than its bound, and the source's WAL is past
// the last position it saved, so the source wrote something capture has not
// recorded. Every table of the copy may then miss changes, so a statement
// that reads one goes to the source (ReasonCaptureStateStale), heavy or
// light: unrecorded drops can leave rows out or keep rows the source
// deleted, which is closer to wrong rows than to rows a little older, and
// table_not_captured, the rule for a drop on record, sends heavy statements
// there too.
//
// Where it cannot go to the source the copy answers it, with a NOTICE, as
// for table_not_captured: a server that hides tables (nothing is sent to its
// source), a statement routed to the copy because the source could not be
// reached for its plan (source_unreachable) or because the capture
// account's gate keeps it from the source (capture_gate), and SUMMARIZE, a
// command of the copy's own. The rows were true when read; they only miss
// what came after.
//
// When the host cannot tell (the source could not be asked where its WAL
// stands), the copy answers as it does without the check, with a NOTICE that
// says so: a source that cannot be asked is most likely away, and the copy
// is the way to read meanwhile.

// CaptureState is what the host says of capture's record of dropped changes
// for this connection's server (Binding.CaptureState).
type CaptureState struct {
	// Kind is "" when the record can be trusted (capture saved its state
	// lately, the source wrote nothing past its last save, or the rule does
	// not apply), CaptureStale or CaptureUnchecked.
	Kind string
	// Why says so in words a client may be shown: no address, no account.
	Why string
}

// The kinds of CaptureState, which are also what RouteNote.CaptureState
// counts for a statement the copy answered with a notice.
const (
	// CaptureStale: capture has not saved its state for longer than its
	// bound while the source wrote past its last saved position.
	CaptureStale = "stale"
	// CaptureUnchecked: capture has not saved its state for longer than its
	// bound, and the source could not be asked whether it wrote since.
	CaptureUnchecked = "unchecked"
)

// captureStateRefusal is the error of a statement for the copy while
// capture's record of dropped changes is not current, nil when the copy may
// run it. The statement then goes to the source (fallTo). Where it cannot go
// there, or the host could not tell, it runs on the copy with a NOTICE, and
// the statement's note says so (captureState).
func (s *session) captureStateRefusal(ctx context.Context, text string) *pgErr {
	if s.bound == nil || s.bound.CaptureState == nil {
		return nil
	}
	toks, _ := lexSQL(text)
	first := firstWord(toks)
	switch first {
	case "show", "describe", "explain":
		// The copy's own commands that read no rows of a table.
		return nil
	case "summarize":
		text = summarizedQuery(text)
	}
	if !readsATable(text) {
		return nil
	}
	st, err := s.bound.CaptureState(ctx)
	if perr := notCapturedStopped(ctx); perr != nil {
		return perr
	}
	if err != nil {
		// The error's own text (an address, an account of the index) goes
		// to the log, not to the client.
		warnEveryMinute(s.logger, "pgshim: capture state unread\x00"+s.bound.Actor,
			"pgshim: whether capture keeps saving its state could not be read; the copy answers, with a notice",
			"server", s.bound.Actor, "err", err)
		st = CaptureState{Kind: CaptureUnchecked, Why: "whether capture keeps saving its state could not be read; DBTrail's log has the details"}
	}
	switch st.Kind {
	case "":
		return nil
	case CaptureStale:
		because := s.captureStaleStaysOnCopy(first)
		if because == "" {
			s.logger.Debug("pgshim: capture's record of dropped changes is not current; the source answers", "why", st.Why)
			s.fallTo(ReasonCaptureStateStale)
			return &pgErr{"55000", st.Why + "; the copy may be missing changes of any of this server's tables, so it does not answer"}
		}
		s.notice("Capture's record of the changes it dropped is not current: " + st.Why +
			". The copy answered from what it holds, which may be missing changes made since to any of this server's tables. " + because)
	default:
		s.notice("DBTrail could not check whether capture's record of the changes it dropped is current (" + st.Why +
			"), so the copy answered as it does without that check: its tables may be missing changes made since capture last saved its state.")
		st.Kind = CaptureUnchecked
	}
	s.captureState = st.Kind
	return nil
}

// captureStaleStaysOnCopy says why a statement the copy is to run while
// capture is stale stays on the copy, "" when it goes to the source. first
// is the statement's first word.
func (s *session) captureStaleStaysOnCopy(first string) string {
	switch {
	case first == "summarize":
		return "SUMMARIZE is a command of the copy's own that your PostgreSQL does not have, so the copy's answer is the one given."
	case !s.canFallBack():
		return "This connection sends nothing to your PostgreSQL (no forwarding user, or the server hides tables), so the copy's answer is the one given."
	case s.copyReason == ReasonSourceUnreachable:
		return "Your PostgreSQL could not be reached for this statement, so the copy's answer is the one given."
	case s.copyReason == ReasonCaptureGate:
		return "The statement did not pass the capture account's check, so it is not sent to your PostgreSQL, and the copy's answer is the one given."
	}
	return ""
}

// readsATable reports whether the statement text (as the copy is handed it)
// may read a table: more, never fewer. A text not read whole reads one, and
// a WITH query counts as a table.
func readsATable(text string) bool {
	reads, why := readsOf(text, copyFuncs)
	return why != "" || len(reads.refs) > 0
}
