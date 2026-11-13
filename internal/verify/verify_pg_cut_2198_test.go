package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/consistency"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2198: the PostgreSQL live-source read is cut at the consistent point of the
// slot whose snapshot was read.

// The bound must be an LSN (a File with a slash) so both tiers compare end_pos
// by number; a File without one takes the binlog-file rule and matches no
// PostgreSQL row, leaving the reconstruction at the bare baseline with no
// error. Strict: at-or-before cut-1.
func TestPGLiveDeltaOptions_cut(t *testing.T) {
	since := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	asOf := since.Add(time.Hour)
	from := &query.BinlogPos{File: "0/100", Pos: 0x100}
	for _, tc := range []struct {
		name     string
		cut      uint64
		wantFile string
	}{
		{"low word", 0x16B3749, "0/16B3748"},
		{"across the high word", 1 << 32, "0/FFFFFFFF"},
		{"high word set", 1<<32 | 0x11, "1/10"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := pgLiveDeltaOptions("app", "orders", since, from, true, asOf, tc.cut)
			if o.UntilPos == nil {
				t.Fatal("UntilPos is nil: the read is not cut at the snapshot")
			}
			if !o.UntilPos.IsLSN() {
				t.Fatalf("UntilPos %+v is not an LSN: the binlog-file rule would match nothing", *o.UntilPos)
			}
			if o.UntilPos.Pos != tc.cut-1 || o.UntilPos.File != tc.wantFile {
				t.Fatalf("UntilPos = %+v, want {%s %d}: events strictly before the consistent point", *o.UntilPos, tc.wantFile, tc.cut-1)
			}
			if o.Until == nil || !o.Until.Equal(asOf) || o.Since == nil || !o.Since.Equal(since) || o.SincePos != from || o.LimitPerPK != 1 {
				t.Fatalf("the window around the cut changed: %+v", o)
			}
		})
	}
	if o := pgLiveDeltaOptions("app", "orders", since, from, false, asOf, 0); o.UntilPos != nil {
		t.Fatalf("an unpinned read got a position bound %+v", *o.UntilPos)
	}
}

// A slot that waited past its bound for an open transaction is inconclusive,
// with the reader's reason, never an error or a comparison.
func TestVerifyTablePG_snapshotWaitIsInconclusive(t *testing.T) {
	cfg := PGLiveConfig{Resolver: pgResolver(), SourceChecksum: func(context.Context, string, string, func([]byte) []byte) (consistency.TableChecksum, error) {
		return consistency.TableChecksum{}, fmt.Errorf("%w (waited 10s); the oldest open write transaction is pid 42", consistency.ErrPGSnapshotWait)
	}}
	res, err := VerifyTablePG(context.Background(), cfg, "app", "orders")
	if err != nil {
		t.Fatalf("VerifyTablePG: %v", err)
	}
	if res.Status != StatusInconclusive || !strings.Contains(res.Detail, "pid 42") || !strings.Contains(res.Detail, "could not be pinned") {
		t.Fatalf("status %q detail %q; want inconclusive naming the open transaction", res.Status, res.Detail)
	}
}

// fakeStream replays stream_state readings, the last one repeating.
type fakeStream struct {
	points []pgStreamPoint
	reads  int
}

func (f *fakeStream) read() (pgStreamPoint, error) {
	p := f.points[min(f.reads, len(f.points)-1)]
	f.reads++
	return p, nil
}

func pgPoint(ckpt uint64, saved string) pgStreamPoint {
	return pgStreamPoint{flavor: flavorPostgres, checkpoint: ckpt, age: sql.NullInt64{Int64: 1, Valid: true}, savedAt: saved}
}

func TestWaitPGCoverage(t *testing.T) {
	defer func(p time.Duration) { coveragePoll = p }(coveragePoll)
	coveragePoll = time.Millisecond
	const anchor = 1000
	ctx := context.Background()
	run := func(t *testing.T, wait time.Duration, points ...pgStreamPoint) (bool, string, *fakeStream) {
		t.Helper()
		f := &fakeStream{points: points}
		covered, note, _, err := waitPGCoverage(ctx, f.read, &sql.DB{}, anchor, time.Time{}, wait)
		if err != nil {
			t.Fatalf("waitPGCoverage: %v", err)
		}
		return covered, note, f
	}

	t.Run("reached at once", func(t *testing.T) {
		covered, note, f := run(t, time.Minute, pgPoint(anchor, "s1"))
		if !covered || note != "" || f.reads != 1 {
			t.Fatalf("covered %v note %q reads %d; want proven on the first read", covered, note, f.reads)
		}
	})
	t.Run("a capture behind catches up", func(t *testing.T) {
		covered, note, _ := run(t, time.Minute, pgPoint(900, "s1"), pgPoint(950, "s2"), pgPoint(980, "s3"), pgPoint(1200, "s4"))
		if !covered || note != "" {
			t.Fatalf("covered %v note %q; want proven once the checkpoint passes the anchor", covered, note)
		}
	})
	t.Run("a quiet database ends the wait after two unmoved saves", func(t *testing.T) {
		start := time.Now()
		covered, note, f := run(t, time.Minute, pgPoint(900, "s1"), pgPoint(900, "s1"), pgPoint(900, "s2"), pgPoint(900, "s3"))
		if !covered || !strings.Contains(note, "coverage unverified") || !strings.Contains(note, "saved its position twice") {
			t.Fatalf("covered %v note %q; want the unverified note and why the wait ended", covered, note)
		}
		if f.reads != 4 || time.Since(start) > 10*time.Second {
			t.Fatalf("%d reads in %s; want the wait to end at the second unmoved save", f.reads, time.Since(start))
		}
	})
	t.Run("a save that moves the position restarts the count", func(t *testing.T) {
		_, note, f := run(t, time.Minute, pgPoint(900, "s1"), pgPoint(900, "s2"), pgPoint(910, "s3"), pgPoint(910, "s4"), pgPoint(910, "s5"))
		if !strings.Contains(note, "saved its position twice") || f.reads != 5 {
			t.Fatalf("note %q after %d reads; want two unmoved saves after the move", note, f.reads)
		}
	})
	t.Run("a capture that is not running is not waited for", func(t *testing.T) {
		stale := pgPoint(900, "s1")
		stale.age = sql.NullInt64{Int64: 3600, Valid: true}
		covered, note, f := run(t, time.Minute, stale)
		if !covered || !strings.Contains(note, "is the capture running?") || f.reads != 1 {
			t.Fatalf("covered %v note %q reads %d", covered, note, f.reads)
		}
	})
	t.Run("a negative wait does not wait", func(t *testing.T) {
		covered, note, f := run(t, -1, pgPoint(900, "s1"))
		if !covered || !strings.Contains(note, "coverage unverified") || f.reads != 1 {
			t.Fatalf("covered %v note %q reads %d", covered, note, f.reads)
		}
	})
	t.Run("a hard stop is returned as it is", func(t *testing.T) {
		covered, note, _ := run(t, time.Minute, pgStreamPoint{flavor: "mysql", checkpoint: 900})
		if covered || !strings.Contains(note, "not a PostgreSQL capture") {
			t.Fatalf("covered %v note %q", covered, note)
		}
	})
	t.Run("a capture still moving at the end of the wait is behind", func(t *testing.T) {
		pts := make([]pgStreamPoint, 0, 4000)
		for i := range 4000 {
			pts = append(pts, pgPoint(uint64(101+i%800), fmt.Sprintf("s%d", i)))
		}
		covered, note, _ := run(t, 50*time.Millisecond, pts...)
		if covered || !strings.HasPrefix(note, indexBehind) || !strings.Contains(note, "still before the snapshot") {
			t.Fatalf("covered %v note %q; want inconclusive: the capture is provably behind", covered, note)
		}
	})
	t.Run("a capture that never saved during the wait keeps the note", func(t *testing.T) {
		covered, note, _ := run(t, 30*time.Millisecond, pgPoint(900, "s1"))
		if !covered || !strings.Contains(note, "coverage unverified") {
			t.Fatalf("covered %v note %q; want the unverified note at the deadline", covered, note)
		}
	})
	t.Run("the newest change's stamp is returned", func(t *testing.T) {
		at := time.Date(2026, 11, 12, 10, 0, 7, 0, time.UTC)
		p := pgPoint(anchor, "s1")
		p.lastEvent = at
		f := &fakeStream{points: []pgStreamPoint{p}}
		_, _, last, err := waitPGCoverage(ctx, f.read, &sql.DB{}, anchor, time.Time{}, time.Minute)
		if err != nil || !last.Equal(at) {
			t.Fatalf("last %v err %v; want %v", last, err, at)
		}
	})
	t.Run("an unmoved position is not waited for again in the same run", func(t *testing.T) {
		db := &sql.DB{}
		f := &fakeStream{points: []pgStreamPoint{pgPoint(700, "s1"), pgPoint(700, "s2"), pgPoint(700, "s3")}}
		if _, _, _, err := waitPGCoverage(ctx, f.read, db, anchor, time.Time{}, time.Minute); err != nil {
			t.Fatal(err)
		}
		g := &fakeStream{points: []pgStreamPoint{pgPoint(700, "s3")}}
		_, note, _, err := waitPGCoverage(ctx, g.read, db, anchor+50, time.Time{}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if g.reads != 1 || !strings.Contains(note, "not waited for again") {
			t.Fatalf("note %q after %d reads; want no second wait", note, g.reads)
		}
	})
	t.Run("a cancelled check is an error", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		f := &fakeStream{points: []pgStreamPoint{pgPoint(900, "s1")}}
		if _, _, _, err := waitPGCoverage(cctx, f.read, &sql.DB{}, anchor, time.Time{}, time.Minute); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})
	t.Run("a read error is the verdict's reason", func(t *testing.T) {
		failing := func() (pgStreamPoint, error) {
			return pgStreamPoint{}, errors.New("could not read index coverage: boom")
		}
		covered, note, _, err := waitPGCoverage(ctx, failing, &sql.DB{}, anchor, time.Time{}, time.Minute)
		if err != nil || covered || !strings.Contains(note, "boom") {
			t.Fatalf("covered %v note %q err %v", covered, note, err)
		}
	})
}
