package verify

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #1380: a snapshot read with no locks can explain a difference. One with no
// record of its locks cannot: every snapshot written before the record
// existed has none, and excusing those would hide every real difference.

const lockDiff = "content digest differs at equal row count (in-place value divergence)"

func lockPair(newLock, prevLock baseline.ReadConsistency) BaselinePair {
	return BaselinePair{Schema: "shop", Table: "orders", NewSnapshot: lr2, PrevSnapshot: lr1, PrevDir: lr1,
		NewLock: newLock, PrevLock: prevLock}
}

func TestPairLockVerdict_mismatch(t *testing.T) {
	c, u, x := baseline.ReadConsistent, baseline.ReadUnknown, baseline.ReadTorn
	newer, older := "2026-09-03T03:00:00Z", "2026-09-02T03:00:00Z"
	cases := []struct {
		name       string
		newL, prev baseline.ReadConsistency
		want       Status
		lock       string
		reason     string
	}{
		{"both consistent: a difference is a mismatch", c, c, StatusMismatch, "consistent", lockDiff},
		{"the read is torn", x, c, StatusInconclusive, "torn",
			lockDiff + ". The snapshot of " + newer + " was read at different points in time, so its rows may not agree and the difference may come from that. A point-in-time database read checks this table"},
		{"the older side is torn", c, x, StatusInconclusive, "torn",
			lockDiff + ". The snapshot of " + older + " was read at different points in time, so its rows may not agree and the difference may come from that. A point-in-time database read checks this table"},
		{"both torn", x, x, StatusInconclusive, "torn",
			lockDiff + ". The snapshot of " + newer + " and the snapshot of " + older + " were read at different points in time, so their rows may not agree and the difference may come from that. A point-in-time database read checks this table"},
		{"torn beside unknown names the torn one", u, x, StatusInconclusive, "torn",
			lockDiff + ". The snapshot of " + older + " was read at different points in time, so its rows may not agree and the difference may come from that. A point-in-time database read checks this table"},
		{"no record on the read: still a mismatch", u, c, StatusMismatch, "unknown",
			lockDiff + ". The snapshot of " + newer + " does not record whether it is point-in-time, so it may have been read at different points in time. A database read with this version records it"},
		{"no record on the older side: still a mismatch", c, u, StatusMismatch, "unknown",
			lockDiff + ". The snapshot of " + older + " does not record whether it is point-in-time, so it may have been read at different points in time. A database read with this version records it"},
		{"no record on either, which is every installation on the day it upgrades", u, u, StatusMismatch, "unknown",
			lockDiff + ". Neither the snapshot of " + newer + " nor the snapshot of " + older + " records whether it is point-in-time, so they may have been read at different points in time. A database read with this version records it"},
		{"a value out of range is no record", baseline.ReadConsistency(42), c, StatusMismatch, "unknown",
			lockDiff + ". The snapshot of " + newer + " does not record whether it is point-in-time, so it may have been read at different points in time. A database read with this version records it"},
	}
	for _, tc := range cases {
		v := pairLockVerdict(lockPair(tc.newL, tc.prev), StatusMismatch, lockDiff)
		st, reason, lock := v.status, v.detail, v.lock.String()
		if wantKind := map[Status]string{StatusInconclusive: InconclusiveTornSnapshot}[tc.want]; v.kind != wantKind {
			t.Errorf("%s: kind %q, want %q", tc.name, v.kind, wantKind)
		}
		if st != tc.want || lock != tc.lock {
			t.Errorf("%s: %s with lock %q, want %s with lock %q", tc.name, st, lock, tc.want, tc.lock)
		}
		if reason != tc.reason {
			t.Errorf("%s:\n got %q\nwant %q", tc.name, reason, tc.reason)
		}
		if strings.ContainsAny(reason, "\u2014\u2013") {
			t.Errorf("%s: the reason carries a dash that is not a hyphen: %q", tc.name, reason)
		}
	}
}

// A pair built by hand sets no lock: that is unknown, and a difference over
// it is a mismatch.
func TestPairLockVerdict_zeroPairIsUnknown(t *testing.T) {
	v := pairLockVerdict(BaselinePair{Schema: "shop", Table: "orders"}, StatusMismatch, lockDiff)
	st, lock := v.status, v.lock.String()
	if st != StatusMismatch || lock != "unknown" {
		t.Fatalf("a pair that names no lock gave %s with lock %q, want a mismatch over unknown", st, lock)
	}
}

// Only a mismatch is touched: a match stays a match, an inconclusive keeps
// its own reason, an error stays an error, whatever the locks were.
func TestPairLockVerdict_onlyAMismatchIsTouched(t *testing.T) {
	for _, st := range []Status{StatusMatch, StatusInconclusive, StatusError, Status("other")} {
		for _, l := range []baseline.ReadConsistency{baseline.ReadConsistent, baseline.ReadUnknown, baseline.ReadTorn} {
			v := pairLockVerdict(lockPair(l, l), st, "as it was")
			if v.status != st || v.detail != "as it was" || v.kind != "" {
				t.Errorf("%s over %s became %s (%q, kind %q)", st, l, v.status, v.detail, v.kind)
			}
			if v.lock != l {
				t.Errorf("%s over %s reports lock %s", st, l, v.lock)
			}
			// A kind the table already had is kept.
			res := TableResult{Status: st, InconclusiveKind: InconclusiveNoActivity}
			v.apply(&res)
			if res.InconclusiveKind != InconclusiveNoActivity || res.SnapshotLock != l.String() {
				t.Errorf("%s over %s: applied as %+v", st, l, res)
			}
		}
	}
}

// A row count that differs is softened too, and stays in the reason.
func TestPairLockVerdict_keepsTheDifference(t *testing.T) {
	reason := pairLockVerdict(lockPair(baseline.ReadTorn, baseline.ReadConsistent), StatusMismatch,
		"row count differs: source=10 reconstructed=9").detail
	if !strings.HasPrefix(reason, "row count differs: source=10 reconstructed=9. ") {
		t.Fatalf("the difference is not what the reason starts with: %q", reason)
	}
	// An empty reason, or one that ends in a stop, makes no double stop.
	for _, d := range []string{"", "a difference.", " a difference. "} {
		reason := pairLockVerdict(lockPair(baseline.ReadTorn, baseline.ReadConsistent), StatusMismatch, d).detail
		if strings.Contains(reason, "..") || strings.HasPrefix(reason, " ") {
			t.Errorf("reason %q from %q", reason, d)
		}
	}
}

// The exit code is Report.ExitError's and does not move: a run where every
// table is over a torn snapshot proved nothing and fails; a torn table beside
// a proven one does not fail the run; a difference over consistent snapshots,
// or over ones with no record, fails it.
func TestSnapshotLock_exitCode(t *testing.T) {
	verdict := func(newL, prev baseline.ReadConsistency, st Status) TableResult {
		res := TableResult{Schema: "shop", Table: "orders"}
		pairLockVerdict(lockPair(newL, prev), st, lockDiff).apply(&res)
		return res
	}
	c, u, x := baseline.ReadConsistent, baseline.ReadUnknown, baseline.ReadTorn
	match := TableResult{Schema: "shop", Table: "users", Status: StatusMatch, SnapshotLock: "consistent"}
	cases := []struct {
		name    string
		results []TableResult
		verdict string
		fails   bool
	}{
		// The owner's rule (2026-09-28): a difference over a torn snapshot
		// fails the run, however many other tables matched.
		{"every table over a torn snapshot", []TableResult{verdict(x, c, StatusMismatch)}, VerdictDiffers, true},
		{"a torn difference beside a proven one", []TableResult{verdict(x, c, StatusMismatch), match}, VerdictDiffers, true},
		{"a torn difference beside a mismatch", []TableResult{verdict(x, c, StatusMismatch), verdict(c, c, StatusMismatch)}, VerdictMismatch, true},
		// A torn snapshot that MATCHES is not a problem.
		{"a torn match beside a proven one", []TableResult{verdict(x, x, StatusMatch), match}, VerdictVerified, false},
		// Any other inconclusive beside a match behaves as before.
		{"another inconclusive beside a proven one", []TableResult{
			{Schema: "shop", Table: "orders", Status: StatusInconclusive, Detail: "table has no primary key"}, match}, VerdictVerified, false},
		{"another inconclusive with a kind beside a proven one", []TableResult{
			{Schema: "shop", Table: "orders", Status: StatusInconclusive, InconclusiveKind: InconclusiveUnproven}, match}, VerdictVerified, false},
		{"another inconclusive alone", []TableResult{
			{Schema: "shop", Table: "orders", Status: StatusInconclusive, Detail: "table has no primary key"}}, VerdictUnproven, true},
		{"a difference over consistent snapshots", []TableResult{verdict(c, c, StatusMismatch), match}, VerdictMismatch, true},
		{"a difference over snapshots with no record", []TableResult{verdict(u, u, StatusMismatch), match}, VerdictMismatch, true},
		{"a difference over no record, alone", []TableResult{verdict(u, u, StatusMismatch)}, VerdictMismatch, true},
	}
	for _, tc := range cases {
		rep := NewReport(ModeBaselinePair, tc.results)
		if rep.Verdict != tc.verdict || (rep.ExitError() != nil) != tc.fails {
			t.Errorf("%s: verdict %s, exit error %v; want %s, fails=%v", tc.name, rep.Verdict, rep.ExitError(), tc.verdict, tc.fails)
		}
		// Softened by a torn snapshot is never "nothing to check", and the
		// report names the kind.
		if rep.Summary.InconclusiveNothingToCheck != 0 {
			t.Errorf("%s: %d counted as nothing to check", tc.name, rep.Summary.InconclusiveNothingToCheck)
		}
		for _, tr := range rep.Tables {
			if tr.Reason != "" && strings.Contains(tr.Reason, "different points in time") && (tr.Status == StatusInconclusive) != (tr.InconclusiveKind == InconclusiveTornSnapshot) {
				t.Errorf("%s: %s.%s is %s with kind %q", tc.name, tr.Schema, tr.Table, tr.Status, tr.InconclusiveKind)
			}
		}
		if InconclusiveKindBenign(InconclusiveTornSnapshot) {
			t.Errorf("a difference over a torn snapshot counts as nothing to check")
		}
	}
}

// The report carries the lock under snapshot_lock, and leaves it out for a
// table that was compared with no snapshot.
func TestSnapshotLock_inTheReport(t *testing.T) {
	rep := NewReport(ModeBaselinePair, []TableResult{
		{Schema: "shop", Table: "orders", Status: StatusInconclusive, Detail: "d", SnapshotLock: "torn"},
		{Schema: "shop", Table: "users", Status: StatusInconclusive, Detail: "no earlier snapshot"},
	})
	raw, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Tables []map[string]any `json:"tables"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if got := doc.Tables[0]["snapshot_lock"]; got != "torn" {
		t.Errorf("orders snapshot_lock = %v", got)
	}
	if _, has := doc.Tables[1]["snapshot_lock"]; has {
		t.Errorf("a table compared with no snapshot carries snapshot_lock: %v", doc.Tables[1])
	}
}

func lockStamp(md map[string]string, stamp string) map[string]string {
	if stamp != "" {
		md[baseline.MetaKeyLockMode] = stamp
	}
	return md
}

// The pairing reads each side's lock from its own footer, the three cases on
// each side.
func TestLastRead_readsTheLockOfEachSide(t *testing.T) {
	for _, tc := range []struct {
		name           string
		prev, newStamp string
		wantPrev       baseline.ReadConsistency
		wantNew        baseline.ReadConsistency
	}{
		{"both locked", "ftwrl", "lock-all", baseline.ReadConsistent, baseline.ReadConsistent},
		{"the read took no locks", "ftwrl", "no-lock", baseline.ReadConsistent, baseline.ReadTorn},
		{"the older side took no locks", "no-lock", "safe-no-lock", baseline.ReadTorn, baseline.ReadConsistent},
		{"written before the record", "", "", baseline.ReadUnknown, baseline.ReadUnknown},
		{"a record this program cannot read", "FTWRL", "locked-v2", baseline.ReadUnknown, baseline.ReadUnknown},
	} {
		root := t.TempDir()
		lrWrite(t, root, lr0, "orders", lockStamp(lrDump(lr0, 100), tc.prev))
		lrWrite(t, root, lr1, "orders", lockStamp(lrDump(lr1, 200), tc.newStamp))
		pairs, _ := lrFind(t, root)
		p := lrOne(t, pairs, "orders")
		wantCompared(t, p, lr0, lr1, 200)
		if p.PrevLock != tc.wantPrev || p.NewLock != tc.wantNew {
			t.Errorf("%s: older side %s, read %s; want %s, %s", tc.name, p.PrevLock, p.NewLock, tc.wantPrev, tc.wantNew)
		}
	}
}

// The newest snapshot is an update of a read that took no locks, carried by
// hard link: the read is still the torn one, found through the newest copy.
func TestLastRead_aTornReadBehindNewerSnapshots(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lockStamp(lrDump(lr0, 100), "ftwrl"))
	lrWrite(t, root, lr1, "orders", lockStamp(lrDump(lr1, 200), "no-lock"))
	lrCarry(t, root, lr1, lr2, "orders")
	lrWrite(t, root, lr3, "orders", lockStamp(lrFold(lr3, lr2, lr1, 1, 300), "no-lock"))
	pairs, _ := lrFind(t, root)
	p := lrOne(t, pairs, "orders")
	wantCompared(t, p, lr0, lr1, 200)
	if p.NewLock != baseline.ReadTorn || p.PrevLock != baseline.ReadConsistent {
		t.Fatalf("read %s, older side %s; want torn, consistent", p.NewLock, p.PrevLock)
	}
	v := pairLockVerdict(p, StatusMismatch, lockDiff)
	st, reason := v.status, v.detail
	if st != StatusInconclusive || !strings.Contains(reason, "The snapshot of "+lr1.Format(time.RFC3339)+" was read at different points in time") {
		t.Fatalf("%s: %q", st, reason)
	}
}

// The exit error names what happened, and the summary counts it apart.
func TestSnapshotLock_differsExitAndSummary(t *testing.T) {
	res := TableResult{Schema: "shop", Table: "orders"}
	pairLockVerdict(lockPair(baseline.ReadTorn, baseline.ReadConsistent), StatusMismatch, lockDiff).apply(&res)
	rep := NewReport(ModeBaselinePair, []TableResult{res, {Schema: "shop", Table: "users", Status: StatusMatch}})
	if rep.Summary.InconclusiveDiffers != 1 || rep.Summary.Inconclusive != 1 || rep.Summary.InconclusiveNothingToCheck != 0 {
		t.Fatalf("summary %+v", rep.Summary)
	}
	err := rep.ExitError()
	want := "1 table(s) differ from a snapshot read at different points in time; the difference may come from that read or from the recorded changes, and a point-in-time database read tells which"
	if err == nil || err.Error() != want {
		t.Fatalf("exit error %v, want %q", err, want)
	}
	raw, _ := json.Marshal(rep)
	if !strings.Contains(string(raw), `"verdict":"differs"`) || !strings.Contains(string(raw), `"inconclusive_differs":1`) {
		t.Fatalf("json %s", raw)
	}
}
