package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
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
			// What the uploader returns for a file of the snapshot that will
			// not read: its own failure, not the bucket's.
			return fmt.Errorf("upload: %w", &fs.PathError{Op: "read", Path: filepath.Join(d, "a", "shop", "orders.parquet"), Err: syscall.EIO})
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
	if n := strings.Count(logs.String(), "could not be sent again; the snapshots stay on this disk, and this one is tried again in an hour, while the others are still sent"); n != 1 {
		t.Fatalf("the failure was warned %d times, want once:\n%s", n, logs.String())
	}
	if strings.Contains(logs.String(), "works again") {
		t.Fatal("said sending works again while a snapshot still fails")
	}
	// c pinned by a reader for one cycle is missing from the list: its wait
	// is not dropped, and it is not sent again the cycle after.
	clock = clock.Add(2 * time.Minute) // 11:03; c's hour ends at 11:10
	cycle(baseline.PruneResult{KeptNotDurable: 2, NotAtDestination: []string{"a", "r"}, NotAsked: []string{"r"}, ReadRefused: []string{"r"}})
	clock = clock.Add(3 * time.Minute) // 11:06
	cycle(baseline.PruneResult{KeptNotDurable: 3, NotAtDestination: []string{"a", "c", "r"}, NotAsked: []string{"c", "r"}, ReadRefused: []string{"r"}})
	if slices.Contains(sends[4:], "c") {
		t.Fatalf("sends %v: c, missing from one cycle's list, was sent again inside its hour", sends)
	}
	// a removed by hand: its failure is forgotten, and the next round that
	// sends says sending works again.
	if err := os.RemoveAll(filepath.Join(d, "a")); err != nil {
		t.Fatal(err)
	}
	snapDir(t, d, "e", true)
	clock = clock.Add(5 * time.Minute)
	cycle(baseline.PruneResult{KeptNotDurable: 1, NotAtDestination: []string{"e"}})
	if sends[len(sends)-1] != "e" || strings.Count(logs.String(), "works again") != 1 {
		t.Fatalf("sends %v, log %q: want e sent and sending said to work again once a is gone", sends, logs.String())
	}
}

// A failure that is not on a file of the snapshot (the bucket, the
// credentials, the network) makes the whole folder wait, as before (#2678):
// every other send would meet it too.
func TestResendLacking_aFailureAtTheDestinationHoldsTheFolder(t *testing.T) {
	d := t.TempDir()
	for _, n := range []string{"a", "b", "c", "d", "e", "f"} {
		snapDir(t, d, n, true)
	}
	clock := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	prevNow, prevSend := resendNow, resendSnapshotFn
	t.Cleanup(func() { resendNow, resendSnapshotFn = prevNow, prevSend })
	resendNow = func() time.Time { return clock }
	sends := 0
	resendSnapshotFn = func(context.Context, string, string, string) error {
		sends++
		return errors.New("api error AccessDenied: Access Denied")
	}
	logs := captureLog(t, slog.LevelInfo)
	target := baselinePruneTarget{dir: d, s3: "s3://bkt/p/", keepNewest: 1}
	res := baseline.PruneResult{KeptNotDurable: 6, NotAtDestination: []string{"a", "b", "c", "d", "e", "f"}}
	var per []int
	for i := 0; i < 12; i++ { // an hour of cleanups every 5 minutes
		resendLacking(context.Background(), target, res, false)
		per = append(per, sends)
		clock = clock.Add(5 * time.Minute)
	}
	if sends != 3 {
		t.Fatalf("an hour against a bucket that refuses every upload: %d sends (%v), want the first round's 3", sends, per)
	}
	if !strings.Contains(logs.String(), "and sending them is tried again in an hour, not after every snapshot") {
		t.Fatalf("the warning does not say the folder waits: %q", logs.String())
	}
	rec, _, _ := baseline.ReadNotInDestination(d)
	if rec.NextSend == nil || !rec.NextSend.Equal(time.Date(2026, 11, 12, 11, 0, 0, 0, time.UTC)) || len(rec.Waiting) != 6 {
		t.Fatalf("record %+v, want all six waiting until 11:00", rec)
	}
}

// Whose failure it is: a file under the snapshot's folder that would not
// open or read is the snapshot's own; anything else is the destination's.
func TestFailedOnItsOwnFile(t *testing.T) {
	dir := "/state/snapshots/s1"
	for _, c := range []struct {
		err  error
		want bool
	}{
		{fmt.Errorf("open: %w", &fs.PathError{Op: "open", Path: dir + "/2026-11-10T03-00-00Z/shop/orders.parquet", Err: fs.ErrPermission}), true},
		{&fs.PathError{Op: "read", Path: dir + "//2026-11-10T03-00-00Z/shop/orders.parquet", Err: syscall.EIO}, true},
		// A path outside the folder, the folder itself, a sibling folder
		// sharing its prefix: not the snapshot's file.
		{&fs.PathError{Op: "open", Path: "/etc/ssl/cert.pem", Err: fs.ErrNotExist}, false},
		{&fs.PathError{Op: "open", Path: dir, Err: fs.ErrPermission}, false},
		{&fs.PathError{Op: "open", Path: dir + "-old/x/shop/t.parquet", Err: fs.ErrPermission}, false},
		{errors.New("api error AccessDenied"), false},
		{nil, false},
	} {
		if got := failedOnItsOwnFile(c.err, dir); got != c.want {
			t.Errorf("failedOnItsOwnFile(%v) = %v, want %v", c.err, got, c.want)
		}
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
	noteResending(target, res, nil, nil, map[string]resendWait{"a": {until: until}}, time.Time{}, false)
	rec, ok, err := baseline.ReadNotInDestination(d)
	if err != nil || !ok || rec.NextSend != nil || len(rec.Waiting) != 1 || rec.Waiting[0].Name != "a" {
		t.Fatalf("record %+v ok=%v err=%v, want a waiting and no next_send", rec, ok, err)
	}
}
