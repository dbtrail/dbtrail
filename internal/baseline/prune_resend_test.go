package baseline

import (
	"testing"
	"time"
)

// A complete snapshot old enough to go, that the destination answered it does
// not have, is named in NotAtDestination so the caller can send it again; one
// whose probe failed (not in durable at all), one that is recent, a keeper, or
// incomplete is not: each stays for its own reason, and only a confirmed
// absence is something a resend can fix.
func TestPlanPrune_namesSnapshotsTheDestinationLacks(t *testing.T) {
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	old := now.Add(-7 * 24 * time.Hour)
	snap := func(name string, ts time.Time, complete bool) localSnapshot {
		return localSnapshot{name: name, ts: ts, complete: complete, tables: []string{"shop/" + name}}
	}
	snaps := []localSnapshot{
		snap("lacked", old, true),
		snap("unprobed", old, true),
		snap("there", old, true),
		snap("recent", now.Add(-time.Minute), true),
		snap("incomplete", old, false),
	}
	durable := map[string]bool{"lacked": false, "there": true, "recent": false}
	_, res := planPrune(snaps, map[string]bool{}, durable, 4*time.Hour, baselinePruneMinAge, now)
	if len(res.NotAtDestination) != 1 || res.NotAtDestination[0] != "lacked" {
		t.Fatalf("NotAtDestination %v, want [lacked]", res.NotAtDestination)
	}
	if res.KeptNotDurable != 2 {
		t.Fatalf("KeptNotDurable %d, want 2 (lacked + unprobed)", res.KeptNotDurable)
	}
}

// The record beside the snapshots names them, so the page can say which.
func TestNotInDestinationRecordNamesTheSnapshots(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	recordNotInDestination(PruneOptions{LocalDir: dir, S3URL: "s3://b/p/"},
		PruneResult{KeptNotDurable: 1, NotAtDestination: []string{"2026-10-19T03-15-06Z"}}, nil, now)
	rec, ok, err := ReadNotInDestination(dir)
	if err != nil || !ok || rec.Count != 1 || len(rec.Names) != 1 || rec.Names[0] != "2026-10-19T03-15-06Z" {
		t.Fatalf("record %+v ok=%v err=%v", rec, ok, err)
	}
}
