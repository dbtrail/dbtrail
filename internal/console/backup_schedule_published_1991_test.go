package console

import "testing"

// A history record that names a snapshot is a published run, whatever its
// error: the schedule line must not call it unfinished (#1991 review).
func TestScheduleRunFromRecord_publishedWhenItNamesASnapshot(t *testing.T) {
	if d := scheduleRunFromRecord(&BaselineRunRecord{Error: "upload: s3 denied", SnapshotTime: "2026-10-01T09:00:00Z"}); !d.Published {
		t.Errorf("a record naming its snapshot is not published: %+v", d)
	}
	if d := scheduleRunFromRecord(&BaselineRunRecord{Error: "dump: refused"}); d.Published {
		t.Errorf("a record with no snapshot is published: %+v", d)
	}
}
