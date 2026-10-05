package shim

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2085: a snapshot older than the limit no longer sends every expensive read
// to MySQL. The copy is asked for an answer only over tables with no change
// since their snapshot, with the limit as the bound on how long ago capture
// must be known to have been complete, and answers when that holds.
func TestRouter_oldSnapshotAsksTheCopyForUnchangedTables_2085(t *testing.T) {
	old := time.Now().Add(-time.Hour)
	r := &fakeRouter{toCopy: true, reason: "plan cost 20146 >= 10000"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: old, unchanged: true}
	var got []string
	h := observedHandler(t, r, f, time.Minute, &got)
	res, err := h.HandleQuery("SELECT status, count(*) FROM t GROUP BY status")
	if err != nil {
		t.Fatalf("HandleQuery: %v", err)
	}
	if side := firstCell(t, res); side != "copy" {
		t.Errorf("answered by %s, want the copy: its tables are unchanged since the snapshot", side)
	}
	if f.gotSess.UnchangedWithin != time.Minute {
		t.Errorf("the copy was asked with UnchangedWithin = %s, want the port's limit (1m0s)", f.gotSess.UnchangedWithin)
	}
	if len(r.forwarded) != 0 {
		t.Errorf("forwarded %v, want nothing", r.forwarded)
	}
	// Its own reason: the copy answering past the limit must be visible.
	if len(got) != 1 || got[0] != "copy/tables_unchanged" {
		t.Errorf("observed %v, want exactly [copy/tables_unchanged]", got)
	}
}

// The copy cannot say the tables are unchanged: today's rule, and today's
// reason, with what the copy said in the trace.
func TestRouter_oldSnapshotChangedTablesFollowTheAgeRule_2085(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now().Add(-time.Hour)}
	var got []string
	h := observedHandler(t, r, f, time.Minute, &got)
	var trace bytes.Buffer
	h.logger = slog.New(slog.NewTextHandler(&trace, &slog.HandlerOptions{Level: slog.LevelDebug}))
	res, err := h.HandleQuery("SELECT status, count(*) FROM t GROUP BY status")
	if err != nil {
		t.Fatalf("HandleQuery: %v", err)
	}
	if side := firstCell(t, res); side != "mysql" {
		t.Errorf("answered by %s, want MySQL", side)
	}
	if f.unchangedAsks != 1 {
		t.Errorf("the copy was asked %d times, want 1", f.unchangedAsks)
	}
	if len(got) != 1 || got[0] != "mysql/copy_too_old" {
		t.Errorf("observed %v, want exactly [mysql/copy_too_old]", got)
	}
	if out := trace.String(); !strings.Contains(out, "max 1m0s") || !strings.Contains(out, "shop.orders changed since its snapshot") {
		t.Errorf("debug trace = %q, want the age, the limit and what the copy said", out)
	}
}

// A snapshot within the limit is not asked the question: the lookup is paid
// only past the limit. And a cheap plan reaches neither.
func TestRouter_unchangedQuestionOnlyPastTheLimit_2085(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now().Add(-10 * time.Second)}
	var got []string
	h := observedHandler(t, r, f, time.Minute, &got)
	if _, err := h.HandleQuery("SELECT a, count(*) FROM t GROUP BY a"); err != nil {
		t.Fatal(err)
	}
	if f.gotSess.UnchangedWithin != 0 || f.unchangedAsks != 0 {
		t.Errorf("a snapshot within the limit was asked for unchanged tables (UnchangedWithin %s, asks %d)", f.gotSess.UnchangedWithin, f.unchangedAsks)
	}
	if len(got) != 1 || got[0] != "copy/expensive_plan" {
		t.Errorf("observed %v, want [copy/expensive_plan]", got)
	}
	f.updatedAt = time.Now().Add(-time.Hour)
	r.toCopy = false
	r.reason = "cheap"
	before := f.calls
	if _, err := h.HandleQuery("SELECT * FROM t WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if f.calls != before || f.unchangedAsks != 0 {
		t.Errorf("a cheap plan reached the copy (%d runs, %d asks): it must pay for no lookup", f.calls-before, f.unchangedAsks)
	}
}

// An unknown age stays MySQL's and asks nothing: with no snapshot time there
// is no snapshot to compare the index with.
func TestRouter_unknownAgeAsksNothing_2085(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), unchanged: true}
	var got []string
	h := observedHandler(t, r, f, time.Minute, &got)
	if _, err := h.HandleQuery("SELECT a, count(*) FROM t GROUP BY a"); err != nil {
		t.Fatal(err)
	}
	if f.calls != 0 {
		t.Errorf("the copy ran %d times with its age unknown", f.calls)
	}
	if len(got) != 1 || got[0] != "mysql/copy_age_unknown" {
		t.Errorf("observed %v, want [mysql/copy_age_unknown]", got)
	}
}

// Past the limit the other refusals keep their own reasons: a statement the
// copy cannot run, and a star it would answer with other columns.
func TestRouter_oldSnapshotKeepsTheOtherRefusals_2085(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"refused", errors.New("Binder Error: no such function"), "mysql/copy_refused"},
		{"columns differ", &sqlsandbox.ColumnsDifferError{Reason: "SELECT * on shop.gen"}, "mysql/copy_columns_differ"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &fakeRouter{toCopy: true, reason: "expensive"}
			f := &fakeFreeSQL{err: tc.err, updatedAt: time.Now().Add(-time.Hour), unchanged: true}
			var got []string
			h := observedHandler(t, r, f, time.Minute, &got)
			if _, err := h.HandleQuery("SELECT a, count(*) FROM t GROUP BY a"); err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[0] != tc.want {
				t.Errorf("observed %v, want [%s]", got, tc.want)
			}
		})
	}
}

// The prepared path takes the same rung.
func TestRouter_preparedOldSnapshotAsksTheCopy_2085(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("n", "BIGINT", int64(3)), updatedAt: time.Now().Add(-time.Hour), unchanged: true}
	var got []string
	h := observedHandler(t, r, f, time.Minute, &got)
	_, _, ctx, err := h.HandleStmtPrepare("SELECT count(*) FROM t WHERE a > ?")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(ctx, "SELECT count(*) FROM t WHERE a > ?", []any{mysql.TypedBytes{Type: mysql.MYSQL_TYPE_LONGLONG, Bytes: []byte("1")}}); err != nil {
		t.Fatal(err)
	}
	if f.gotSess.UnchangedWithin != time.Minute {
		t.Errorf("the prepared statement reached the copy with UnchangedWithin = %s, want 1m0s", f.gotSess.UnchangedWithin)
	}
	if len(got) == 0 || got[len(got)-1] != "copy/tables_unchanged" {
		t.Errorf("observed %v, want the last to be copy/tables_unchanged", got)
	}
}
