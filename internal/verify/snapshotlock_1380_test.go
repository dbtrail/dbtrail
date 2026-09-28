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
			lockDiff + ". The snapshot of " + newer + " was taken with no locks, so its rows were copied at different moments and the difference may come from that. Take a full snapshot with locks to check this table"},
		{"the older side is torn", c, x, StatusInconclusive, "torn",
			lockDiff + ". The snapshot of " + older + " was taken with no locks, so its rows were copied at different moments and the difference may come from that. Take a full snapshot with locks to check this table"},
		{"both torn", x, x, StatusInconclusive, "torn",
			lockDiff + ". The snapshot of " + newer + " and the snapshot of " + older + " were each taken with no locks, so its rows were copied at different moments and the difference may come from that. Take a full snapshot with locks to check this table"},
		{"torn beside unknown names the torn one", u, x, StatusInconclusive, "torn",
			lockDiff + ". The snapshot of " + older + " was taken with no locks, so its rows were copied at different moments and the difference may come from that. Take a full snapshot with locks to check this table"},
		{"no record on the read: still a mismatch", u, c, StatusMismatch, "unknown",
			lockDiff + ". The snapshot of " + newer + " does not record how it was locked, so it may have been taken with no locks. A full snapshot taken with this version records it"},
		{"no record on the older side: still a mismatch", c, u, StatusMismatch, "unknown",
			lockDiff + ". The snapshot of " + older + " does not record how it was locked, so it may have been taken with no locks. A full snapshot taken with this version records it"},
		{"no record on either, which is every installation on the day it upgrades", u, u, StatusMismatch, "unknown",
			lockDiff + ". The snapshot of " + newer + " and the snapshot of " + older + " each do not record how it was locked, so it may have been taken with no locks. A full snapshot taken with this version records it"},
		{"a value out of range is no record", baseline.ReadConsistency(42), c, StatusMismatch, "unknown",
			lockDiff + ". The snapshot of " + newer + " does not record how it was locked, so it may have been taken with no locks. A full snapshot taken with this version records it"},
	}
	for _, tc := range cases {
		st, reason, lock := pairLockVerdict(lockPair(tc.newL, tc.prev), StatusMismatch, lockDiff)
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
	st, _, lock := pairLockVerdict(BaselinePair{Schema: "shop", Table: "orders"}, StatusMismatch, lockDiff)
	if st != StatusMismatch || lock != "unknown" {
		t.Fatalf("a pair that names no lock gave %s with lock %q, want a mismatch over unknown", st, lock)
	}
}

// Only a mismatch is touched: a match stays a match, an inconclusive keeps
// its own reason, an error stays an error, whatever the locks were.
func TestPairLockVerdict_onlyAMismatchIsTouched(t *testing.T) {
	for _, st := range []Status{StatusMatch, StatusInconclusive, StatusError, Status("other")} {
		for _, l := range []baseline.ReadConsistency{baseline.ReadConsistent, baseline.ReadUnknown, baseline.ReadTorn} {
			got, reason, lock := pairLockVerdict(lockPair(l, l), st, "as it was")
			if got != st || reason != "as it was" {
				t.Errorf("%s over %s became %s (%q)", st, l, got, reason)
			}
			if lock != l.String() {
				t.Errorf("%s over %s reports lock %q", st, l, lock)
			}
		}
	}
}

// A row count that differs is softened too, and stays in the reason.
func TestPairLockVerdict_keepsTheDifference(t *testing.T) {
	_, reason, _ := pairLockVerdict(lockPair(baseline.ReadTorn, baseline.ReadConsistent), StatusMismatch,
		"row count differs: source=10 reconstructed=9")
	if !strings.HasPrefix(reason, "row count differs: source=10 reconstructed=9. ") {
		t.Fatalf("the difference is not what the reason starts with: %q", reason)
	}
	// An empty reason, or one that ends in a stop, makes no double stop.
	for _, d := range []string{"", "a difference.", " a difference. "} {
		_, reason, _ := pairLockVerdict(lockPair(baseline.ReadTorn, baseline.ReadConsistent), StatusMismatch, d)
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
		s, d, l := pairLockVerdict(lockPair(newL, prev), st, lockDiff)
		return TableResult{Schema: "shop", Table: "orders", Status: s, Detail: d, SnapshotLock: l}
	}
	c, u, x := baseline.ReadConsistent, baseline.ReadUnknown, baseline.ReadTorn
	match := TableResult{Schema: "shop", Table: "users", Status: StatusMatch, SnapshotLock: "consistent"}
	cases := []struct {
		name    string
		results []TableResult
		verdict string
		fails   bool
	}{
		{"every table over a torn snapshot", []TableResult{verdict(x, c, StatusMismatch)}, VerdictUnproven, true},
		{"a torn table beside a proven one", []TableResult{verdict(x, c, StatusMismatch), match}, VerdictVerified, false},
		{"a difference over consistent snapshots", []TableResult{verdict(c, c, StatusMismatch), match}, VerdictMismatch, true},
		{"a difference over snapshots with no record", []TableResult{verdict(u, u, StatusMismatch), match}, VerdictMismatch, true},
		{"a difference over no record, alone", []TableResult{verdict(u, u, StatusMismatch)}, VerdictMismatch, true},
	}
	for _, tc := range cases {
		rep := NewReport(ModeBaselinePair, tc.results)
		if rep.Verdict != tc.verdict || (rep.ExitError() != nil) != tc.fails {
			t.Errorf("%s: verdict %s, exit error %v; want %s, fails=%v", tc.name, rep.Verdict, rep.ExitError(), tc.verdict, tc.fails)
		}
		// Softened by a torn snapshot is never "nothing to check".
		if rep.Summary.InconclusiveNothingToCheck != 0 {
			t.Errorf("%s: %d counted as nothing to check", tc.name, rep.Summary.InconclusiveNothingToCheck)
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
	st, reason, _ := pairLockVerdict(p, StatusMismatch, lockDiff)
	if st != StatusInconclusive || !strings.Contains(reason, "The snapshot of "+lr1.Format(time.RFC3339)+" was taken with no locks") {
		t.Fatalf("%s: %q", st, reason)
	}
}
