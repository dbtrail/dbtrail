package pgshim

import (
	"context"
	"strings"
	"sync"
	"time"
)

// A table whose capture dropped its changes. Capture records in its ledger
// the changes it drops (a table below REPLICA IDENTITY FULL, a deferrable
// key) and the loss of its slot; the copy's recent changes of such a table
// are not served (the host's tails refuse it), so the copy reads it from its
// snapshot alone, which no later change reaches. With a forwarding account
// the source answers a statement that reads one, as table_not_captured,
// before the copy runs: whatever the connection read before. Without one
// (no forwarding account, a server that hides tables) the copy answers as it
// did, and the client is sent a NOTICE that names the table and when its
// snapshot was read, so that old rows are not taken for current ones.
//
// The host says which tables (Binding.NotCaptured); this reads which tables
// a statement reads as apartFromSource does, by its text, and with more
// tables than it reads, never fewer: a text not read whole reads every
// table, a name without a schema is that name in any schema, and case is
// not compared.

// notCapturedRefusal is the error of a statement for the copy that reads a
// table capture does not keep whole, nil when it may run on the copy. The
// statement then goes to the source (fallTo). Where it cannot go there, it
// runs on the copy, with a NOTICE that says so.
func (s *session) notCapturedRefusal(ctx context.Context, text string) *pgErr {
	if s.bound == nil || s.bound.NotCaptured == nil {
		return nil
	}
	toks, _ := lexSQL(text)
	switch firstWord(toks) {
	case "show", "describe", "summarize", "explain":
		// The copy's own commands (copy_command): the source would refuse
		// them, and they are not the reads this is about.
		return nil
	}
	lost, err := s.bound.NotCaptured(ctx)
	if ctx.Err() != nil {
		// The client left, or the statement's time ran out, while the
		// capture's record was read: that, not a statement to send again.
		if perr := stoppedError(ctx); perr != nil {
			return perr
		}
		return &pgErr{"57014", "the statement was stopped"}
	}
	if !s.canFallBack() {
		if err != nil {
			warnNotCapturedUnread(s, "the copy answers, as nothing is sent to the source on this connection", err)
			return nil
		}
		if t, exact, ok := readsNotCapturedTable(text, lost); ok {
			s.notice(notCapturedNotice(t, exact))
		}
		return nil
	}
	if err != nil {
		// Not known which tables capture keeps whole: the source answers,
		// as for a table that is not.
		warnNotCapturedUnread(s, "the source answers the statement", err)
		s.fallTo(ReasonTableNotCaptured)
		// The error's own text (an address, an account of the index) goes
		// to the log, not to the client.
		return &pgErr{"55000", "which of the copy's tables capture keeps whole could not be read; DBTrail's log has the details"}
	}
	why := readsNotCaptured(text, lost)
	if why == "" {
		return nil
	}
	s.logger.Debug("pgshim: the statement reads a table capture does not keep whole; the source answers", "why", why)
	s.fallTo(ReasonTableNotCaptured)
	return &pgErr{"55000", why}
}

// readsNotCaptured says why the statement text (as the copy is handed it)
// may read one of lost, "" when it reads none of them.
func readsNotCaptured(text string, lost []TableNotCaptured) string {
	t, _, ok := readsNotCapturedTable(text, lost)
	if !ok {
		return ""
	}
	return t.Why
}

// notCapturedNotice is what a connection that cannot reach its source is
// told when the copy answers a statement that reads t; exact false: its text
// does not say which tables it reads, and t is one of the tables it may read.
func notCapturedNotice(t TableNotCaptured, exact bool) string {
	name := t.Schema + "." + t.Name
	taken := "its snapshot"
	if !t.SnapshotRead.IsZero() {
		taken = "its snapshot read from the source at " + t.SnapshotRead.UTC().Format("2006-01-02 15:04:05 UTC")
	}
	reads := "reads " + name
	if !exact {
		reads = "may read " + name
	}
	return "This statement " + reads + ", which capture did not keep whole (" + t.Why + "), so the copy answered it from " + taken +
		", without the changes made since. This connection sends nothing to your PostgreSQL (no forwarding user, or the server hides tables), so the copy's answer is the one given."
}

// readsNotCapturedTable is the table of lost the statement text may read,
// ok false when it reads none of them; exact false when its text is not read
// whole, and the table is the first of lost.
func readsNotCapturedTable(text string, lost []TableNotCaptured) (t TableNotCaptured, exact, ok bool) {
	if len(lost) == 0 {
		return TableNotCaptured{}, false, false
	}
	reads, why := readsOf(text, copyFuncs)
	if why != "" {
		// Which tables it reads is not told by its text: any of them.
		return lost[0], false, true
	}
	for _, ref := range reads.refs {
		schema, name := "", ref.parts[len(ref.parts)-1].text
		switch len(ref.parts) {
		case 1:
		case 2:
			schema = ref.parts[0].text
		default:
			// A database before the schema: not read here.
			return lost[0], false, true
		}
		// A WITH named as a lost table counts as that table: in doubt, the
		// source.
		for _, t := range lost {
			if strings.EqualFold(t.Name, name) && (schema == "" || strings.EqualFold(t.Schema, schema)) {
				return t, true, true
			}
		}
	}
	return TableNotCaptured{}, false, false
}

// notCapturedWarnEvery is how often a process says, at warn level, that it
// could not read which tables capture keeps whole: once per statement would
// flood the log while the index is down.
const notCapturedWarnEvery = time.Minute

var notCapturedWarned struct {
	mu sync.Mutex
	at time.Time
}

// warnNotCapturedUnread says, at most every notCapturedWarnEvery for the
// process (at debug level in between), that which tables capture keeps whole
// could not be read, and what was done instead.
func warnNotCapturedUnread(s *session, then string, err error) {
	notCapturedWarned.mu.Lock()
	due := time.Since(notCapturedWarned.at) >= notCapturedWarnEvery
	if due {
		notCapturedWarned.at = time.Now()
	}
	notCapturedWarned.mu.Unlock()
	msg := "pgshim: which of the copy's tables capture keeps whole could not be read; " + then
	if due {
		s.logger.Warn(msg+" (said once a minute)", "err", err)
		return
	}
	s.logger.Debug(msg, "err", err)
}
