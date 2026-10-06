package shim

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/readrouter"
)

// trackingRouter is a fakeRouter over a source that reports session changes
// (#2127): the part of readrouter.Forwarder the handler asks through
// sessionTracker, with the Forwarder's two rules restated (an answer the
// source marked, and any error, are a change to take).
type trackingRouter struct {
	*fakeRouter
	tracked bool
	changed bool
	// marks says which forwarded statements the source marks as having
	// changed a tracked setting; explainMarks the same for the EXPLAIN of a
	// decision.
	marks        func(stmt string) bool
	explainMarks func(stmt string) bool
	// readBackMarked: the read-back's own answer is marked (MySQL reports a
	// change made by a statement that failed with the next answer).
	readBackMarked bool
	// list is what the session's session_track_system_variables holds;
	// listCell, when set, answers that cell in its place (nil is NULL).
	list     string
	listCell *any
	// short: the tracked read-back comes back without the list's cell.
	short bool
	// readBackSQL is every read-back the source was sent.
	readBackSQL []string
	// retracked is what TrackSessionAgain was called with; retrackErr what
	// it answers.
	retracked  []string
	retrackErr error
}

func (r *trackingRouter) SessionTracked() bool { return r.tracked }

func (r *trackingRouter) TakeSessionChanged() bool {
	changed := r.changed
	r.changed = false
	return changed
}

func (r *trackingRouter) TrackSessionAgain(_ context.Context, has string) error {
	r.retracked = append(r.retracked, has)
	if r.retrackErr != nil {
		if !readrouter.IsLost(r.retrackErr) {
			r.tracked = false
		}
		return r.retrackErr
	}
	r.list = has + "," + readrouter.SessionTrackedVariables
	return nil
}

func (r *trackingRouter) Decide(ctx context.Context, stmt string) (readrouter.Decision, error) {
	d, err := r.fakeRouter.Decide(ctx, stmt)
	if err != nil || r.explainMarks != nil && r.explainMarks(stmt) {
		r.changed = true
	}
	return d, err
}

func (r *trackingRouter) Forward(ctx context.Context, stmt string, sink readrouter.RowSink) (*mysql.Result, error) {
	if !isSessionReadBack(stmt) {
		res, err := r.fakeRouter.Forward(ctx, stmt, sink)
		if err != nil || r.marks != nil && r.marks(stmt) {
			r.changed = true
		}
		return res, err
	}
	r.readBackSQL = append(r.readBackSQL, stmt)
	if !strings.HasSuffix(stmt, ", @@session.session_track_system_variables") {
		return r.fakeRouter.Forward(ctx, stmt, sink)
	}
	r.readBacks++
	if r.sessionErr != nil {
		r.changed = true
		return nil, r.sessionErr
	}
	if r.readBackMarked {
		r.changed = true
	}
	var cell any = []byte(r.list)
	if r.listCell != nil {
		cell = *r.listCell
	}
	names := []string{"tz", "mode", "lim", "lc", "div", "ain", "big", "cs", "csconn", "coll", "probe", "tracked"}
	row := append(r.src.sessionRow(stmt), cell)
	if r.short {
		names, row = names[:11], row[:11]
	}
	return sourceRows(sink, names, [][]any{row})
}

// trackedRig is sessRig over a source that reports session changes.
type trackedRig struct {
	*sessRig
	tr *trackingRouter
}

func newTrackedRig(t *testing.T) *trackedRig {
	t.Helper()
	rig := &sessRig{}
	rig.r = &fakeRouter{toCopy: true, reason: "expensive", src: stockSource()}
	tr := &trackingRouter{fakeRouter: rig.r, tracked: true, list: readrouter.SessionTrackedVariables}
	rig.f = &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	rig.logs = bytes.Buffer{}
	rig.h = NewHandler(nil, slog.New(slog.NewTextHandler(&rig.logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	rig.h.BindFreeSQL(rig.f)
	rig.h.BindRouter(tr, RouterConfig{MaxCopyAge: time.Minute, Observe: func(route RouteSide, reason RouteReason) {
		rig.seen = append(rig.seen, string(route)+"/"+string(reason))
	}})
	return &trackedRig{sessRig: rig, tr: tr}
}

// A statement that is a plain read by its text and calls a stored function.
const callsAFunction = "SELECT set_the_zone(a) FROM t WHERE id = 1"

// onMySQL runs a statement that MySQL answers (its plan is cheap).
func (rig *trackedRig) onMySQL(t *testing.T, stmt string) {
	t.Helper()
	rig.r.toCopy = false
	defer func() { rig.r.toCopy = true }()
	if got := firstCell(t, rig.run(t, stmt)); got != "mysql" {
		t.Fatalf("%q was answered by %s, want MySQL", stmt, got)
	}
}

func TestTrackedSession_theStatementIsAPlainReadByItsText(t *testing.T) {
	if !readrouter.PlainRead(callsAFunction) {
		t.Fatalf("%q is not a plain read: the tests below would pass by the rule for a SET", callsAFunction)
	}
	newTrackedRig(t).onMySQL(t, callsAFunction)
}

// The issue: a SELECT calls a stored function that runs SET time_zone. By its
// text the statement is a read; the source marks its answer, and the next
// statement bound for the copy reads the session back first and runs under
// the zone the function left.
func TestTrackedSession_aChangeInsideAFunctionIsReadBack(t *testing.T) {
	rig := newTrackedRig(t)
	if got := rig.side(t); got != "copy" || rig.r.readBacks != 1 {
		t.Fatalf("before: %s, %d read-back(s)", got, rig.r.readBacks)
	}
	rig.tr.marks = func(stmt string) bool { return stmt == callsAFunction }
	rig.r.onForward = func(stmt string) {
		if stmt == callsAFunction {
			rig.r.src.zone = "+05:00"
		}
	}
	rig.onMySQL(t, callsAFunction)
	if rig.r.readBacks != 1 {
		t.Fatalf("the statement itself cost a read-back (%d): nothing may follow it on the source", rig.r.readBacks)
	}
	if got := rig.side(t); got != "copy" {
		t.Fatalf("after: answered by %s (%s)", got, rig.last())
	}
	if rig.r.readBacks != 2 {
		t.Errorf("%d read-back(s), want 2: the copy answered under the session from before the function ran", rig.r.readBacks)
	}
	if rig.f.gotSess.TimeZone != "Etc/GMT-5" {
		t.Errorf("the copy ran under %+v, want the zone the function set (+05:00)", rig.f.gotSess)
	}
	// And only once: the statements after it find the session known.
	rig.side(t)
	if rig.r.readBacks != 2 {
		t.Errorf("%d read-back(s) after a second copy-served statement, want still 2", rig.r.readBacks)
	}
}

// The same when the function leaves a setting the copy does not reproduce:
// MySQL answers, under session_differs.
func TestTrackedSession_aModeSetInsideAFunctionKeepsTheCopyFromAnswering(t *testing.T) {
	rig := newTrackedRig(t)
	rig.side(t)
	rig.tr.marks = func(stmt string) bool { return stmt == callsAFunction }
	rig.r.onForward = func(string) { rig.r.src.mode = "PAD_CHAR_TO_FULL_LENGTH" }
	rig.onMySQL(t, callsAFunction)
	if got := rig.side(t); got != "mysql" || rig.last() != "mysql/session_differs" {
		t.Errorf("after the function set PAD_CHAR_TO_FULL_LENGTH: answered by %s (%s), want mysql/session_differs", got, rig.last())
	}
}

// A statement that changed nothing costs nothing: the source's answers are
// not marked, no read-back follows, and the source is sent the client's
// statements and no other.
func TestTrackedSession_noExtraRoundTripForStatementsThatChangeNothing(t *testing.T) {
	rig := newTrackedRig(t)
	rig.side(t)
	stmts := []string{callsAFunction, "SELECT * FROM t WHERE id = 1", "SHOW TABLES", "SELECT GROUP_CONCAT(a) FROM t", callsAFunction}
	rig.r.toCopy = false
	for range 20 {
		for _, stmt := range stmts {
			rig.run(t, stmt)
		}
	}
	rig.r.toCopy = true
	if got := rig.side(t); got != "copy" {
		t.Fatalf("answered by %s", got)
	}
	if rig.r.readBacks != 1 {
		t.Errorf("%d read-back(s), want only the one before the first copy-served statement", rig.r.readBacks)
	}
	if n := len(rig.r.forwarded); n != 20*len(stmts) {
		t.Errorf("the source was sent %d statements for the client's %d", n, 20*len(stmts))
	}
	if len(rig.tr.retracked) != 0 {
		t.Errorf("the tracked list was set again (%q) on a session that kept it", rig.tr.retracked)
	}
}

// The EXPLAIN of a decision can change the session as its statement would
// (MariaDB runs a deterministic function with constant arguments while it
// plans; MySQL a scalar subquery with an aggregate). The source marks the
// EXPLAIN's answer, and the statement it was for is not answered by the copy
// under the session from before it.
func TestTrackedSession_aChangeMadeByTheExplainIsReadBack(t *testing.T) {
	rig := newTrackedRig(t)
	rig.side(t)
	rig.tr.explainMarks = func(stmt string) bool {
		rig.r.src.zone = "+05:00"
		return true
	}
	if got := rig.side(t); got != "copy" {
		t.Fatalf("answered by %s (%s)", got, rig.last())
	}
	if rig.r.readBacks != 2 || rig.f.gotSess.TimeZone != "Etc/GMT-5" {
		t.Errorf("%d read-back(s), the copy ran under %+v; want 2 and the zone the EXPLAIN left (+05:00)", rig.r.readBacks, rig.f.gotSess)
	}
}

// A plain read that failed says nothing about the session (an error packet
// carries no mark; a function that ran SET and then raised an error has set
// it): the session is unknown after it.
func TestTrackedSession_unknownAfterAPlainReadThatFailed(t *testing.T) {
	rig := newTrackedRig(t)
	rig.side(t)
	rig.r.forwardErr = mysql.NewError(mysql.ER_SIGNAL_EXCEPTION, "raised by the function")
	rig.r.toCopy = false
	if _, err := rig.h.HandleQuery(callsAFunction); err == nil {
		t.Fatal("MySQL's error did not reach the client")
	}
	rig.r.forwardErr, rig.r.toCopy = nil, true
	rig.r.src.zone = "+09:00"
	if got := rig.side(t); got != "copy" || rig.r.readBacks != 2 || rig.f.gotSess.TimeZone != "Etc/GMT-9" {
		t.Errorf("after a failed plain read: answered by %s under %+v after %d read-back(s), want the copy under +09:00 after 2", got, rig.f.gotSess, rig.r.readBacks)
	}
}

// MySQL reports a change made by a failed statement with the NEXT answer,
// which can be the read-back's own. That mark is already in what was read:
// it must not send every later statement back for the session again.
func TestTrackedSession_aMarkOnTheReadBackItselfIsNotAChange(t *testing.T) {
	rig := newTrackedRig(t)
	rig.tr.readBackMarked = true
	for range 4 {
		if got := rig.side(t); got != "copy" {
			t.Fatalf("answered by %s", got)
		}
	}
	if rig.r.readBacks != 1 {
		t.Errorf("%d read-back(s) for four copy-served statements, want 1", rig.r.readBacks)
	}
}

// A source that does not track is asked the read-back as before (the cell
// with the tracked list is not in it: such a source may not have the
// variable), and a change inside a function is not seen there. That is the
// documented limit, pinned here so that it is a decision.
func TestTrackedSession_aSourceThatDoesNotTrack(t *testing.T) {
	rig := newTrackedRig(t)
	rig.tr.tracked = false
	rig.side(t)
	rig.r.onForward = func(string) { rig.r.src.zone = "+05:00" }
	rig.onMySQL(t, callsAFunction)
	if got := rig.side(t); got != "copy" || rig.r.readBacks != 1 {
		t.Errorf("answered by %s after %d read-back(s), want the copy after 1 (nothing said the session changed)", got, rig.r.readBacks)
	}
	for _, q := range rig.tr.readBackSQL {
		if strings.Contains(q, "session_track") {
			t.Errorf("a source that does not track was asked for its tracked list: %s", q[len(q)-80:])
		}
	}
	// A Router that is no tracker at all behaves the same.
	plain := newSessRig(t)
	plain.side(t)
	plain.r.toCopy = false
	plain.run(t, callsAFunction)
	plain.r.toCopy = true
	if got := plain.side(t); got != "copy" || plain.r.readBacks != 1 {
		t.Errorf("no tracker: answered by %s after %d read-back(s)", got, plain.r.readBacks)
	}
}

// A client's own statement can replace the session's tracked list (a
// connector that asks for the variables it follows). The read-back shows it,
// and the port's settings are put back, keeping the client's, before the
// session is called known.
func TestTrackedSession_aReplacedListIsPutBack(t *testing.T) {
	rig := newTrackedRig(t)
	rig.side(t)
	if len(rig.tr.retracked) != 0 {
		t.Fatalf("set again on a session that had the list: %q", rig.tr.retracked)
	}
	rig.tr.list = "autocommit,time_zone,transaction_isolation"
	rig.run(t, "SET autocommit = 1, session_track_system_variables = 'autocommit,time_zone,transaction_isolation'")
	if got := rig.side(t); got != "copy" {
		t.Fatalf("answered by %s (%s)", got, rig.last())
	}
	if want := []string{"autocommit,time_zone,transaction_isolation"}; !reflect.DeepEqual(rig.tr.retracked, want) {
		t.Errorf("TrackSessionAgain called with %q, want %q", rig.tr.retracked, want)
	}
	// From there the session is tracked as before.
	rig.tr.marks = func(stmt string) bool { return stmt == callsAFunction }
	rig.r.onForward = func(string) { rig.r.src.zone = "+05:00" }
	rig.onMySQL(t, callsAFunction)
	if got := rig.side(t); got != "copy" || rig.f.gotSess.TimeZone != "Etc/GMT-5" {
		t.Errorf("after the list was put back: answered by %s under %+v, want the copy under +05:00", got, rig.f.gotSess)
	}
	if len(rig.tr.retracked) != 1 {
		t.Errorf("the list was set %d times, want once", len(rig.tr.retracked))
	}
}

// A client can set the list to NULL: it is put back like an empty one.
func TestTrackedSession_aNullListIsPutBack(t *testing.T) {
	rig := newTrackedRig(t)
	null := any(nil)
	rig.tr.listCell = &null
	if got := rig.side(t); got != "copy" {
		t.Fatalf("answered by %s (%s)", got, rig.last())
	}
	if !reflect.DeepEqual(rig.tr.retracked, []string{""}) {
		t.Errorf("TrackSessionAgain called with %q, want once with an empty list", rig.tr.retracked)
	}
}

// A source that refuses to take the list back is one that does not track
// from there on: the session that was just read is still good, the copy
// answers, and the next read-back no longer asks for the list.
func TestTrackedSession_aSourceThatRefusesTheListAgain(t *testing.T) {
	rig := newTrackedRig(t)
	rig.tr.list = "time_zone"
	rig.tr.retrackErr = mysql.NewError(mysql.ER_UNKNOWN_SYSTEM_VARIABLE, "no")
	if got := rig.side(t); got != "copy" {
		t.Fatalf("answered by %s (%s)", got, rig.last())
	}
	rig.run(t, "SET time_zone = '+03:00'")
	rig.r.src.zone = "+03:00"
	if got := rig.side(t); got != "copy" || rig.f.gotSess.TimeZone != "Etc/GMT-3" {
		t.Errorf("answered by %s under %+v", got, rig.f.gotSess)
	}
	if len(rig.tr.retracked) != 1 {
		t.Errorf("asked %d times, want once", len(rig.tr.retracked))
	}
	if last := rig.tr.readBackSQL[len(rig.tr.readBackSQL)-1]; strings.Contains(last, "session_track") {
		t.Error("the read-back still asks a source that stopped tracking for its tracked list")
	}
}

// Whatever goes wrong while the session is read leaves it unknown: the copy
// does not answer, and the next statement asks again.
func TestTrackedSession_everyFailureLeavesTheSessionUnknown(t *testing.T) {
	cases := []struct {
		name  string
		fault func(rig *trackedRig)
		mend  func(rig *trackedRig)
	}{
		{"the read-back is refused",
			func(rig *trackedRig) { rig.r.sessionErr = mysql.NewError(mysql.ER_UNKNOWN_ERROR, "no") },
			func(rig *trackedRig) { rig.r.sessionErr = nil }},
		{"the read-back comes back a cell short",
			func(rig *trackedRig) { rig.tr.short = true },
			func(rig *trackedRig) { rig.tr.short = false }},
		{"the connection is lost while the list is put back",
			func(rig *trackedRig) {
				rig.tr.list = "time_zone"
				rig.tr.retrackErr = mysql.NewError(readrouter.CodeUpstreamLost, "gone")
			},
			func(rig *trackedRig) { rig.tr.retrackErr = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newTrackedRig(t)
			tc.fault(rig)
			if got := rig.side(t); got != "mysql" || rig.last() != "mysql/session_differs" {
				t.Fatalf("answered by %s (%s), want mysql/session_differs", got, rig.last())
			}
			rig.h.mu.Lock()
			known := rig.h.routeSess.known
			rig.h.mu.Unlock()
			if known {
				t.Fatal("the session is called known after a read-back that failed")
			}
			before := rig.r.readBacks
			tc.mend(rig)
			if got := rig.side(t); got != "copy" || rig.r.readBacks != before+1 {
				t.Errorf("once mended: answered by %s after %d more read-back(s), want the copy after 1", got, rig.r.readBacks-before)
			}
		})
	}
}

// Time travel asks for the session's zone the same way, so a zone a function
// changed is seen there too.
func TestTrackedSession_timeTravelHeedsTheMark(t *testing.T) {
	rig := newTrackedRig(t)
	ctx := context.Background()
	if err := rig.h.timeTravelZoneRefusal(ctx); err != nil {
		t.Fatalf("under UTC: %v", err)
	}
	rig.tr.marks = func(stmt string) bool { return stmt == callsAFunction }
	rig.r.onForward = func(string) { rig.r.src.zone = "+05:00" }
	rig.onMySQL(t, callsAFunction)
	err := rig.h.timeTravelZoneRefusal(ctx)
	if mysqlErrCode(err) != mysql.ER_NOT_SUPPORTED_YET || !strings.Contains(err.Error(), "+05:00") {
		t.Errorf("after a function moved the zone: %v, want 1235 naming +05:00", err)
	}
}

// Every session variable the read-back reads is one the source is asked to
// report changes of, and so is the collation the collation probe depends on:
// a variable read here and not tracked there is a change the port would not
// hear of.
func TestTrackedSession_everythingReadBackIsTracked(t *testing.T) {
	tracked := map[string]bool{}
	for name := range strings.SplitSeq(readrouter.SessionTrackedVariables, ",") {
		tracked[name] = true
	}
	read := regexp.MustCompile(`@@session\.(\w+)`).FindAllStringSubmatch(sessionReadBackSQL(zoneProbeInstants(time.Now())), -1)
	if len(read) < 9 {
		t.Fatalf("found %d session variables in the read-back, want at least 9: the pattern no longer matches it", len(read))
	}
	for _, m := range read {
		if !tracked[m[1]] {
			t.Errorf("the read-back reads @@session.%s and the source is not asked to report its changes (readrouter.SessionTrackedVariables)", m[1])
		}
	}
	for _, name := range []string{"collation_connection", "max_join_size", "session_track_system_variables"} {
		if !tracked[name] {
			t.Errorf("%s is not tracked", name)
		}
	}
	if !readrouter.TracksSession(readrouter.SessionTrackedVariables) {
		t.Error("the port's own list does not read as tracked")
	}
}
