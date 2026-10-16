package consoleapp

import (
	"errors"
	"fmt"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// fullReadStandsIn is the watcher's rule; pinned on the shapes it must refuse.
func TestFullReadStandsIn(t *testing.T) {
	for _, tc := range []struct {
		name string
		st   console.BaselineStatus
		want bool
	}{
		{"a refused update", console.BaselineStatus{State: "failed"}, true},
		{"published, only the upload failed (local folder)", console.BaselineStatus{State: "failed", Published: true}, false},
		{"disk refused", console.BaselineStatus{State: "failed", DiskRefused: true}, false},
		{"succeeded", console.BaselineStatus{State: "succeeded", Published: true}, false},
	} {
		if got := fullReadStandsIn(tc.st); got != tc.want {
			t.Errorf("%s: fullReadStandsIn = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The daemon-wide --baseline-refresh-interval loop never writes a server
// with no local folder: such a server stays skipped, with the warning.
func TestBaselineRefreshTargets_stillSkipsS3OnlyServers_2212(t *testing.T) {
	entries := []console.ServerEntry{
		{ID: "b", Name: "s3only", DSN: "dsn-b", BaselineS3: "s3://bucket/b/"},
	}
	got, skipped, _ := baselineRefreshTargets(entries, "", "")
	if len(got) != 0 || len(skipped) != 1 || skipped[0] != "s3only" {
		t.Fatalf("targets = %+v, skipped = %v, want the S3-only server skipped", got, skipped)
	}
}

// A download that fills the disk is a disk refusal, so no full read stands
// in for it. Classified in reconstruct,
// where the error is known to be a local write (ErrLocalDiskFull), never by
// the words: the index MySQL says the same "No space left on device" about
// ITS tmp disk, and that failure a full read would cure.
func TestFoldDiskRefused_onlyALocalWriteIsADiskRefusal(t *testing.T) {
	download := fmt.Errorf("shop.orders: materialize baseline: %w", reconstruct.ErrLocalDiskFull)
	if !foldDiskRefused(download) {
		t.Fatal("a full disk under the download is not a disk refusal")
	}
	st := console.BaselineStatus{}
	applyFoldStatus(&st, 1, 1, reuseTally{}, download)
	if !st.DiskRefused || fullReadStandsIn(st) {
		t.Fatalf("status = %+v, want a disk refusal no full read stands in for", st)
	}
	index := errors.New("shop.orders: fetch events: Error 3 (HY000): Error writing file '/tmp/MYfd=58' (OS errno 28 - No space left on device)")
	if foldDiskRefused(index) {
		t.Fatal("the index MySQL's own full tmp disk reads as a local disk refusal")
	}
	st = console.BaselineStatus{}
	applyFoldStatus(&st, 1, 1, reuseTally{}, index)
	if st.DiskRefused || !fullReadStandsIn(st) {
		t.Fatalf("status = %+v, want a refusal a full read stands in for", st)
	}
}
