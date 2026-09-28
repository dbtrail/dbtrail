package reconstruct

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
)

// A footer read that panics must not take the process with it: ReadBounds
// runs inside the daemon that also captures. The table whose read panicked
// comes back unread, which grades unknown, and the other tables of the same
// call are read as if nothing had happened. Twenty files, so that a slot the
// panic left taken would stall the rest (the limit is eight at a time).
func TestReadBounds_aPanickingReadIsUnreadAndTheRestAreRead(t *testing.T) {
	forgetS3ChainStarts(t)
	prev := chainStartAt
	t.Cleanup(func() { chainStartAt = prev })
	snap := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	start := snap.Add(-3 * time.Hour)
	chainStartAt = func(_ context.Context, upserts string) (time.Time, error) {
		if strings.Contains(upserts, "bad") {
			panic("footer reader blew up")
		}
		return start, nil
	}
	var files []BaselineFile
	for i := range 20 {
		name := "t" + string(rune('a'+i))
		if i%4 == 0 {
			name = "bad" + name
		}
		loc := "/data/2026-09-27T12-00-00Z/shop/"
		if i%2 == 1 {
			loc = "s3://b/base/2026-09-27T12-00-00Z/shop/"
		}
		files = append(files, BaselineFile{SnapshotTime: snap, Schema: "shop", Table: name,
			Path: loc + name + ".parquet", DeltaUpserts: name + ".000000.upserts"})
	}

	done := make(chan []status.ReadBound, 1)
	go func() { done <- ReadBounds(context.Background(), files) }()
	var got []status.ReadBound
	select {
	case got = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("ReadBounds did not return: a panicking read left the wait or a slot held")
	}
	if len(got) != len(files) {
		t.Fatalf("%d bounds for %d files", len(got), len(files))
	}
	floor := status.DeltaFloor{Hour: snap.Add(-10 * time.Hour)}
	for i, f := range files {
		bad := strings.HasPrefix(f.Table, "bad")
		want := status.ReadBound{ChainStart: start}
		verdict := status.BaselineOK
		if bad {
			want, verdict = status.ReadBound{Unread: true}, status.BaselineUnknown
		}
		if got[i] != want {
			t.Errorf("%s: bound %+v, want %+v", f.Table, got[i], want)
		}
		if v := floor.GradeTable(f.SnapshotTime, got[i], snap.Add(time.Minute)); v != verdict {
			t.Errorf("%s: graded %q, want %q", f.Table, v, verdict)
		}
		if _, kept := s3ChainStarts.Load(f.Path); bad && kept {
			t.Errorf("%s: a read that panicked was kept as an answer", f.Table)
		}
	}
}
