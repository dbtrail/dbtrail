package reconstruct

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// One run reads where its folds stop once (#1596): the tables of one snapshot
// are folded through the same LSN however far capture gets meanwhile, and
// each table only clamps that to its own floor.
func TestPGRunEnd_oneUntilPerRun(t *testing.T) {
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	baselineAt := at.Add(-6 * time.Hour)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("ORDER BY event_id DESC LIMIT 1").WillReturnRows(sqlmock.NewRows([]string{"end_pos", "event_timestamp"}).AddRow(uint64(0x8000), at.Add(-time.Hour)))
	mock.ExpectQuery("FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"binlog_position"}).AddRow(int64(0x8000)))
	withFirstEventPast(t, nil)
	run, err := resolvePGRunEnd(context.Background(), db, at)
	if err != nil {
		t.Fatal(err)
	}
	// Capture moves on after the read: nothing more is asked of the index.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	a, err := run.boundsFor(0x1000, baselineAt, baselineAt)
	if err != nil {
		t.Fatal(err)
	}
	b, err := run.boundsFor(0x2000, baselineAt, baselineAt)
	if err != nil {
		t.Fatal(err)
	}
	if a.Until == nil || b.Until == nil || a.Until.Pos != 0x8000 || b.Until.Pos != 0x8000 || a.Next != b.Next {
		t.Fatalf("two tables of one run: %+v %+v, want both through 8000", a, b)
	}
	// A table snapshotted after the newest change, with a floor past the
	// run's end, folds nothing and keeps its floor.
	c, err := run.boundsFor(0x9000, at.Add(-30*time.Minute), baselineAt)
	if err != nil {
		t.Fatal(err)
	}
	if !c.empty(0x9000) || c.Next != 0x9000 {
		t.Fatalf("floor past the end: %+v", c)
	}
	// An empty index folds nothing, whatever the floor, even 0 (which
	// checkPGFoldable refuses upstream).
	if e, err := (pgRunEnd{at: at, none: true}).boundsFor(0, baselineAt, baselineAt); err != nil || e.Until != nil || e.Next != 0 {
		t.Fatalf("empty index: %+v %v", e, err)
	}
	// The bound each table records rides with its bounds.
	if want := run.foldedDeltaSince(a, 0x1000, baselineAt); a.Since.IsZero() || !a.Since.Equal(want) {
		t.Fatalf("since %v, want %v", a.Since, want)
	}
	if !c.Since.Equal(baselineAt) {
		t.Fatalf("nothing folded: since %v, want the read's %v", c.Since, baselineAt)
	}
	// No time the changes were read from: no bound to record, refused.
	if _, err := run.boundsFor(0x1000, baselineAt, time.Time{}); err == nil || !strings.Contains(err.Error(), "no time its changes were read from") {
		t.Fatalf("zero read time: err = %v", err)
	}
	// The per-table refusals stay per table.
	if _, err := run.boundsFor(0x1000, at.Add(time.Second), baselineAt); err == nil || !strings.Contains(err.Error(), "before the snapshot") {
		t.Fatalf("a snapshot after the point: err = %v", err)
	}
	if _, err := run.boundsFor(0x9000, at.Add(-2*time.Hour), baselineAt); err == nil || !strings.Contains(err.Error(), "another PostgreSQL cluster") {
		t.Fatalf("newest change after the snapshot but below its floor: err = %v", err)
	}
}

// The bound a folded snapshot records on how far back the changes it has not
// folded reach (#1596, MetaKeyPGDeltaSince). A change the run did not fold
// either is in the index past the run's end, or has not reached the index
// yet; either one committed after the newest change the index holds, give or
// take PGDeltaMargin, and after the point when a change past it was found.
func TestPGFoldedDeltaSince(t *testing.T) {
	at := time.Date(2026, 10, 17, 12, 0, 0, 900_000_000, time.UTC)
	prev := time.Date(2026, 10, 17, 5, 59, 0, 0, time.UTC)
	const floor = 0x1000
	folded := pgFoldBounds{Until: lsnPos(0x8000), Next: 0x8001}
	cases := []struct {
		name string
		run  pgRunEnd
		b    pgFoldBounds
		want time.Time
	}{
		// Capture stopped an hour before the point: what it has not
		// delivered yet committed after its newest change, not after the
		// point. Recording the point would drop it once it arrives.
		{"capture behind the point", pgRunEnd{at: at, newestAt: at.Add(-time.Hour)}, folded,
			time.Date(2026, 10, 17, 10, 59, 0, 0, time.UTC)},
		{"a change past the point", pgRunEnd{at: at, newestAt: at.Add(time.Hour)}, folded,
			time.Date(2026, 10, 17, 11, 59, 0, 0, time.UTC)},
		// Nothing folded: the next run starts at the same floor, so the
		// bound the source snapshot read with still holds, and nothing
		// later does.
		{"nothing folded", pgRunEnd{at: at, newestAt: at.Add(-time.Minute)}, pgFoldBounds{Next: floor}, prev},
		{"an empty index", pgRunEnd{at: at, none: true}, pgFoldBounds{Next: floor}, prev},
		{"nothing folded, a bound with a fraction", pgRunEnd{at: at}, pgFoldBounds{Next: floor}, prev},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := prev
			if strings.Contains(c.name, "fraction") {
				p = prev.Add(400 * time.Millisecond)
			}
			got := c.run.foldedDeltaSince(c.b, floor, p)
			if !got.Equal(c.want) || got.Location() != time.UTC {
				t.Fatalf("since = %v, want %v", got, c.want)
			}
		})
	}
}

// What the folded snapshot records reads back through PGDeltaWindow as the
// same bound.
func TestPGFoldedDeltaSince_roundTrip(t *testing.T) {
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	run := pgRunEnd{at: at, newestAt: at.Add(-time.Hour)}
	since := run.foldedDeltaSince(pgFoldBounds{Until: lsnPos(0x8000), Next: 0x8001}, 0x1000, at)
	md := pgSnapshotFileMetadata(mergeInput{PG: true, PGCutLSN: 0x8001, PGDeltaSince: since}, map[string]string{})
	got, from, proven, err := PGDeltaWindow(parsePGFooter(t, md), at)
	if err != nil || !proven || from == nil || from.Pos != 0x8001 || !got.Equal(since) {
		t.Fatalf("window = %v %+v %v %v, want %v from 8001", got, from, proven, err, since)
	}
}

// parsePGFooter reads back the two keys of a PostgreSQL footer a reader
// starts from, as the baseline readers parse them.
func parsePGFooter(t *testing.T, md map[string]string) baseline.DumpMetadata {
	t.Helper()
	lsn, err := strconv.ParseUint(md[baseline.MetaKeyLSN], 10, 64)
	if err != nil {
		t.Fatalf("footer LSN %q: %v", md[baseline.MetaKeyLSN], err)
	}
	return baseline.DumpMetadata{LSN: lsn, PGDeltaSince: md[baseline.MetaKeyPGDeltaSince]}
}
