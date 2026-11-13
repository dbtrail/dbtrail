package consoleapp

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// The wait after a send is the snapshot's, not the folder's (#2691): a
// snapshot whose send fails for a reason of its own (damaged files) waits its
// hour, and the others the bucket lacks are still sent at each cleanup. The
// bounds of #2696 hold per snapshot: one S3 could not be asked about is sent
// at most once an hour, one S3 refuses to read back is not sent again.
func TestResendLacking_eachSnapshotWaitsOnItsOwn(t *testing.T) {
	d := t.TempDir()
	for _, n := range []string{"a", "b", "c", "r"} {
		snapDir(t, d, n, true)
	}
	clock := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	prevNow, prevSend := resendNow, resendSnapshotFn
	t.Cleanup(func() { resendNow, resendSnapshotFn = prevNow, prevSend })
	resendNow = func() time.Time { return clock }
	var sends []string
	resendSnapshotFn = func(_ context.Context, _, _, name string) error {
		sends = append(sends, name)
		if name == "a" {
			return errors.New("a damaged file")
		}
		return nil
	}
	logs := captureLog(t, slog.LevelInfo)
	target := baselinePruneTarget{dir: d, s3: "s3://bkt/p/", keepNewest: 1}
	cycle := func(res baseline.PruneResult) baseline.NotInDestination {
		t.Helper()
		resendLacking(context.Background(), target, res, false)
		rec, _, err := baseline.ReadNotInDestination(d)
		if err != nil {
			t.Fatal(err)
		}
		return rec
	}

	// 10:00: a fails.
	rec := cycle(baseline.PruneResult{KeptNotDurable: 1, NotAtDestination: []string{"a"}})
	if !slices.Equal(sends, []string{"a"}) || rec.NextSend == nil || len(rec.Waiting) != 1 || rec.Waiting[0].Name != "a" || rec.Waiting[0].ForAnswer {
		t.Fatalf("after a failed: sends %v, record %+v; want a waiting after a failure", sends, rec)
	}
	// 10:05: b is new. Before, the folder waited and b with it.
	clock = clock.Add(5 * time.Minute)
	rec = cycle(baseline.PruneResult{KeptNotDurable: 2, NotAtDestination: []string{"a", "b"}})
	if !slices.Equal(sends, []string{"a", "b"}) {
		t.Fatalf("sends %v, want b sent while a waits: one damaged snapshot holds the others", sends)
	}
	if !slices.Equal(rec.Names, []string{"a"}) || rec.NextSend == nil || !rec.NextSend.Equal(time.Date(2026, 11, 12, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("record %+v, want a alone, waiting until 11:00", rec)
	}
	// 10:10: c could not be asked about and is sent: it waits for the
	// answer; r is refused on read, not sent; a still waits.
	clock = clock.Add(5 * time.Minute)
	rec = cycle(baseline.PruneResult{KeptNotDurable: 3, NotAtDestination: []string{"a", "c", "r"}, NotAsked: []string{"c", "r"}, ReadRefused: []string{"r"}})
	if !slices.Equal(sends, []string{"a", "b", "c"}) {
		t.Fatalf("sends %v, want c sent and neither a (waiting) nor r (refused on read)", sends)
	}
	// c is sent, so it leaves the list; the next cleanup lists it again as
	// not asked, and its wait holds it.
	clock = clock.Add(5 * time.Minute)
	rec = cycle(baseline.PruneResult{KeptNotDurable: 3, NotAtDestination: []string{"a", "c", "r"}, NotAsked: []string{"c", "r"}, ReadRefused: []string{"r"}})
	if !slices.Equal(sends, []string{"a", "b", "c"}) {
		t.Fatalf("sends %v, want c not sent again inside its hour", sends)
	}
	if len(rec.Waiting) != 2 || rec.Waiting[0].Name != "a" || rec.Waiting[0].ForAnswer || rec.Waiting[1].Name != "c" || !rec.Waiting[1].ForAnswer ||
		!rec.Waiting[1].Until.Equal(time.Date(2026, 11, 12, 11, 10, 0, 0, time.UTC)) {
		t.Fatalf("waits %+v, want a after its failure and c for the answer until 11:10", rec.Waiting)
	}
	// Every one sent again waits (r is not sent again): NextSend is the
	// earliest, and not for an answer since a's is a failure.
	if rec.NextSend == nil || !rec.NextSend.Equal(time.Date(2026, 11, 12, 11, 0, 0, 0, time.UTC)) || rec.WaitsForAnswer {
		t.Fatalf("record %+v, want next_send 11:00, not for an answer", rec)
	}
	// 11:01: a's hour is over and it is tried again; c's is not.
	clock = time.Date(2026, 11, 12, 11, 1, 0, 0, time.UTC)
	cycle(baseline.PruneResult{KeptNotDurable: 3, NotAtDestination: []string{"a", "c", "r"}, NotAsked: []string{"c", "r"}, ReadRefused: []string{"r"}})
	if !slices.Equal(sends, []string{"a", "b", "c", "a"}) {
		t.Fatalf("sends %v, want a tried again after its hour, c still inside its own", sends)
	}
	// One failing snapshot is one warning, and the sends that worked meanwhile
	// do not say sending "works again" while a still fails.
	if n := strings.Count(logs.String(), "could not be sent again; the snapshots stay on this disk"); n != 1 {
		t.Fatalf("the failure was warned %d times, want once", n)
	}
	if strings.Contains(logs.String(), "works again") {
		t.Fatal("said sending works again while a snapshot still fails")
	}
}

// A mixed state on the record (some sent at the next cleanup, some waiting)
// leaves NextSend unset: an older page that reads NextSend alone would say
// all of them wait.
func TestNoteResending_nextSendOnlyWhenAllWait(t *testing.T) {
	d := t.TempDir()
	target := baselinePruneTarget{dir: d, s3: "s3://bkt/p/"}
	until := time.Date(2026, 11, 12, 11, 0, 0, 0, time.UTC)
	res := baseline.PruneResult{KeptNotDurable: 2, NotAtDestination: []string{"a", "b"}}
	noteResending(target, res, nil, nil, map[string]resendWait{"a": {until: until}})
	rec, ok, err := baseline.ReadNotInDestination(d)
	if err != nil || !ok || rec.NextSend != nil || len(rec.Waiting) != 1 || rec.Waiting[0].Name != "a" {
		t.Fatalf("record %+v ok=%v err=%v, want a waiting and no next_send", rec, ok, err)
	}
}
