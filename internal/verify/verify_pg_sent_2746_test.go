package verify

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

// #2746: on a quiet database no commit moves the capture's checkpoint past
// the snapshot's consistent point, so the checkpoint alone cannot tell a
// quiet database from a capture that has not been sent a commit yet. The
// walsender's sent position the capture saves (pg_sent_through_lsn) can: every
// commit below it is indexed.

func pgSentPoint(ckpt uint64, saved string, sent uint64) pgStreamPoint {
	p := pgPoint(ckpt, saved)
	p.sentThrough, p.sentKnown = sent, true
	return p
}

func TestWaitPGCoverage_sentThrough_2746(t *testing.T) {
	defer func(p time.Duration) { coveragePoll = p }(coveragePoll)
	coveragePoll = time.Millisecond
	const anchor = 1000
	ctx := context.Background()
	run := func(t *testing.T, db *sql.DB, wait time.Duration, points ...pgStreamPoint) (bool, string, *fakeStream) {
		t.Helper()
		f := &fakeStream{points: points}
		covered, note, _, err := waitPGCoverage(ctx, f.read, db, anchor, time.Time{}, wait)
		if err != nil {
			t.Fatalf("waitPGCoverage: %v", err)
		}
		return covered, note, f
	}

	t.Run("a quiet database is proven at once", func(t *testing.T) {
		covered, note, f := run(t, &sql.DB{}, time.Minute, pgSentPoint(900, "s1", anchor))
		if !covered || note != "" || f.reads != 1 {
			t.Fatalf("covered %v note %q reads %d; want proven on the first read, no note", covered, note, f.reads)
		}
	})
	t.Run("the sent position reaching the anchor during the wait proves it", func(t *testing.T) {
		covered, note, _ := run(t, &sql.DB{}, time.Minute,
			pgSentPoint(900, "s1", 0), pgSentPoint(900, "s2", 950), pgSentPoint(900, "s3", 999), pgSentPoint(900, "s4", 1400))
		if !covered || note != "" {
			t.Fatalf("covered %v note %q; want proven once the sent position passes the anchor", covered, note)
		}
	})
	t.Run("one byte short is not a proof", func(t *testing.T) {
		covered, note, _ := run(t, &sql.DB{}, 30*time.Millisecond, pgSentPoint(900, "s1", anchor-1))
		if covered || !strings.Contains(note, "coverage not proven") || !strings.Contains(note, "it saved 0/3E7") {
			t.Fatalf("covered %v note %q; want inconclusive naming the saved position", covered, note)
		}
	})
	t.Run("a capture held back does not end the wait early and is inconclusive", func(t *testing.T) {
		// The same checkpoint saved again and again: before #2746 the second
		// unmoved save ended the wait as covered with a note.
		pts := make([]pgStreamPoint, 0, 4000)
		for i := range 4000 {
			pts = append(pts, pgSentPoint(900, fmt.Sprintf("s%d", i), 950))
		}
		start := time.Now()
		covered, note, f := run(t, &sql.DB{}, 60*time.Millisecond, pts...)
		if covered || !strings.Contains(note, "coverage not proven") || strings.Contains(note, "coverage unverified") {
			t.Fatalf("covered %v note %q; want inconclusive, never a covered verdict", covered, note)
		}
		if f.reads <= 4 || time.Since(start) < 60*time.Millisecond {
			t.Fatalf("%d reads in %s; want the wait to run to its deadline", f.reads, time.Since(start))
		}
	})
	t.Run("a capture that has not saved a sent position yet is inconclusive at the deadline", func(t *testing.T) {
		covered, note, _ := run(t, &sql.DB{}, 30*time.Millisecond, pgSentPoint(900, "s1", 0))
		if covered || !strings.Contains(note, "it has not saved one yet") {
			t.Fatalf("covered %v note %q", covered, note)
		}
	})
	t.Run("an earlier unproven wait does not skip this one", func(t *testing.T) {
		db := &sql.DB{}
		stuckCheckpoints.set(db, formatLSN(900))
		covered, note, f := run(t, db, time.Minute, pgSentPoint(900, "s1", 950), pgSentPoint(900, "s2", 950), pgSentPoint(900, "s3", 1000))
		if !covered || note != "" || f.reads != 3 {
			t.Fatalf("covered %v note %q reads %d; want the wait run and proven", covered, note, f.reads)
		}
	})
	t.Run("a sent position a whole wait did not move is not waited for again", func(t *testing.T) {
		db := &sql.DB{}
		if covered, _, _ := run(t, db, 20*time.Millisecond, pgSentPoint(900, "s1", 950)); covered {
			t.Fatal("the first wait was covered")
		}
		covered, note, f := run(t, db, time.Minute, pgSentPoint(900, "s9", 950))
		if covered || f.reads != 1 || !strings.Contains(note, "earlier wait of this run") {
			t.Fatalf("covered %v note %q reads %d; want inconclusive at once", covered, note, f.reads)
		}
		// A position that moved since is waited for, and proves it.
		covered, note, _ = run(t, db, time.Minute, pgSentPoint(900, "s10", 960), pgSentPoint(900, "s11", 2000))
		if !covered || note != "" {
			t.Fatalf("covered %v note %q; want the moved position waited for and proven", covered, note)
		}
	})
	t.Run("a checkpoint still moving at the deadline is behind", func(t *testing.T) {
		pts := make([]pgStreamPoint, 0, 4000)
		for i := range 4000 {
			pts = append(pts, pgSentPoint(uint64(101+i%800), fmt.Sprintf("s%d", i), 950))
		}
		covered, note, _ := run(t, &sql.DB{}, 50*time.Millisecond, pts...)
		if covered || !strings.HasPrefix(note, indexBehind) {
			t.Fatalf("covered %v note %q; want inconclusive: the capture is provably behind", covered, note)
		}
	})
	t.Run("hard stops stay hard whatever the sent position says", func(t *testing.T) {
		foreign := pgSentPoint(900, "s1", 5000)
		foreign.flavor = "mysql"
		none := pgSentPoint(0, "s1", 5000)
		lost := pgSentPoint(900, "s1", 5000)
		lost.gapLostAt = sql.NullTime{Time: time.Now(), Valid: true}
		for name, p := range map[string]pgStreamPoint{"foreign flavor": foreign, "no checkpoint": none, "loss in the window": lost} {
			covered, note, _ := run(t, &sql.DB{}, time.Minute, p)
			if covered || note == "" {
				t.Fatalf("%s: covered %v note %q; want inconclusive", name, covered, note)
			}
		}
	})
	t.Run("a negative wait still takes a proof that is already there", func(t *testing.T) {
		covered, note, _ := run(t, &sql.DB{}, -1, pgSentPoint(900, "s1", anchor))
		if !covered || note != "" {
			t.Fatalf("covered %v note %q", covered, note)
		}
	})
	t.Run("a capture without the sent position keeps the old early end", func(t *testing.T) {
		covered, note, f := run(t, &sql.DB{}, time.Minute, pgPoint(900, "s1"), pgPoint(900, "s2"), pgPoint(900, "s3"))
		if !covered || !strings.Contains(note, "saved its position twice") || f.reads != 3 {
			t.Fatalf("covered %v note %q reads %d", covered, note, f.reads)
		}
	})
}
