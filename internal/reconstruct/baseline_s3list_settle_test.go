package reconstruct

import (
	"context"
	"testing"
	"time"
)

// stamp dates every key of the fake as written at `at`.
func (f *fakeS3Snapshots) stamp(at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.modified == nil {
		f.modified = map[string]time.Time{}
	}
	for _, k := range f.keys {
		if _, ok := f.modified[k]; !ok {
			f.modified[k] = at
		}
	}
}

// A directory whose newest object was written moments ago is answered and
// not remembered: what was read of it can be short of what it ends up
// holding (a directory with no marker read while it is uploaded, or a read
// of several pages that an upload finished in between). The next call
// reads it again and sees the rest.
func TestListBaselinesS3_aDirectoryJustWrittenIsReadAgain(t *testing.T) {
	now := time.Date(2026, 9, 24, 3, 0, 0, 0, time.UTC)
	prev := s3Clock
	t.Cleanup(func() { s3Clock = prev })
	s3Clock = func() time.Time { return now }

	f := &fakeS3Snapshots{}
	// No marker: a directory an older version or another writer left.
	f.add(snapshotKeys(dirAt(1), []string{"shop/orders"})...)
	f.stamp(now.Add(-time.Minute))
	stubS3Snapshots(t, f)

	files, err := ListBaselines(context.Background(), "s3://b/base")
	if err != nil || len(files) != 1 {
		t.Fatalf("first read: files=%+v err=%v", files, err)
	}
	// The rest of the upload lands.
	f.add(dirAt(1) + "/shop/items.parquet")
	f.stamp(now.Add(-30 * time.Second))

	files, err = ListBaselines(context.Background(), "s3://b/base")
	if err != nil || len(files) != 2 {
		t.Fatalf("second read: got %d files, want the 2 the directory holds now (err=%v): %+v", len(files), err, files)
	}

	// Once nothing in it was written for s3DirSettle, it is remembered.
	now = now.Add(s3DirSettle)
	f.dirsRead()
	if _, err := ListBaselines(context.Background(), "s3://b/base"); err != nil {
		t.Fatal(err)
	}
	f.dirsRead()
	if files, err = ListBaselines(context.Background(), "s3://b/base"); err != nil || len(files) != 2 || len(f.asked()) != 0 {
		t.Fatalf("settled: files=%d err=%v, directories read again=%v, want none", len(files), err, f.asked())
	}
}

// The age is the NEWEST object's: one old file beside one just written is
// a directory still being written.
func TestListBaselinesS3_oneFreshObjectKeepsTheDirectoryUnsettled(t *testing.T) {
	now := time.Date(2026, 9, 24, 3, 0, 0, 0, time.UTC)
	prev := s3Clock
	t.Cleanup(func() { s3Clock = prev })
	s3Clock = func() time.Time { return now }

	f := &fakeS3Snapshots{}
	f.add(snapshotKeys(dirAt(1), []string{"shop/orders"}, "_SUCCESS")...)
	f.stamp(now.Add(-time.Hour))
	f.add(dirAt(1) + "/shop/items.parquet")
	f.stamp(now.Add(-time.Second))
	stubS3Snapshots(t, f)

	for i := range 2 {
		f.dirsRead()
		if _, err := ListBaselines(context.Background(), "s3://b/base"); err != nil {
			t.Fatal(err)
		}
		if got := f.asked(); len(got) != 1 {
			t.Fatalf("call %d read %v, want the one directory read each time", i+1, got)
		}
	}
}

// A store that reports no time for its objects says nothing about their
// age. Those directories are remembered as before: reading them on every
// call would bring back one request per directory per call.
func TestListBaselinesS3_noTimeReportedIsRemembered(t *testing.T) {
	f := &fakeS3Snapshots{}
	f.add(snapshotKeys(dirAt(1), []string{"shop/orders"}, "_SUCCESS")...)
	stubS3Snapshots(t, f)

	if _, err := ListBaselines(context.Background(), "s3://b/base"); err != nil {
		t.Fatal(err)
	}
	f.dirsRead()
	if _, err := ListBaselines(context.Background(), "s3://b/base"); err != nil || len(f.asked()) != 0 {
		t.Fatalf("err=%v, directories read again=%v, want none", err, f.asked())
	}
}
