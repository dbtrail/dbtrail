//go:build integration

package reconstruct_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgdelta"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Two snapshot updates that write a pair, against a real index, with every
// change made on a real PostgreSQL table too: after each update the
// snapshot's table (its file and the pair beside it) holds, row for row, what
// the source holds. The second update reads its base file through the hard
// link the first one carried into its folder, and does not read that file's
// key columns again for a NULL: the file is the one the first update found
// none in (#3082). Each update's line says how long each of its steps took,
// and the steps add up to no more than the whole.
func TestReconstructTables_PGPairStepTimesAndOneKeyPass(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	f := newPGFixture(t)
	ctx := context.Background()
	reconstruct.SetPGTableDeltas(t, true)

	src, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close(context.Background()) })
	srcTable := fmt.Sprintf("pair_steps_%d", time.Now().UnixNano())
	if _, err := src.Exec(ctx, "CREATE TABLE "+srcTable+" (id text PRIMARY KEY, status text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = src.Exec(context.Background(), "DROP TABLE IF EXISTS "+srcTable) })

	rows := pairRows()
	for i := 100; i < 140; i++ {
		rows = append(rows, [2]string{fmt.Sprint(i), "more"})
	}
	for _, r := range rows {
		if _, err := src.Exec(ctx, "INSERT INTO "+srcTable+" VALUES ($1, $2)", r[0], r[1]); err != nil {
			t.Fatal(err)
		}
	}
	root := t.TempDir()
	taken := f.at(10 * time.Minute)
	f.baseline(root, taken, 0x500, taken.Add(-time.Minute).Format(time.RFC3339), rows...)
	// Older than the margin a file must be to be remembered, as a base an
	// update reads usually is.
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(f.tableFile(root, taken), old, old); err != nil {
		t.Fatal(err)
	}
	f.other(0x600, 12*time.Minute)

	// change is one change, on the source and in the index alike.
	change := func(lsn uint64, d time.Duration, typ uint8, id string, after *string) {
		t.Helper()
		var q string
		var args []any
		switch typ {
		case 1:
			q, args = "INSERT INTO "+srcTable+" VALUES ($1, $2)", []any{id, *after}
		case 2:
			q, args = "UPDATE "+srcTable+" SET status = $2 WHERE id = $1", []any{id, *after}
		case 3:
			q, args = "DELETE FROM "+srcTable+" WHERE id = $1", []any{id}
		}
		if tag, err := src.Exec(ctx, q, args...); err != nil || tag.RowsAffected() != 1 {
			t.Fatalf("on the source: %s %v: %v, %v", q, args, tag, err)
		}
		f.event(lsn, d, typ, id, after)
	}
	source := func() map[string]string {
		t.Helper()
		rs, err := src.Query(ctx, "SELECT id, status FROM "+srcTable)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		m := map[string]string{}
		for rs.Next() {
			var id, st string
			if err := rs.Scan(&id, &st); err != nil {
				t.Fatal(err)
			}
			m[id] = st
		}
		if err := rs.Err(); err != nil {
			t.Fatal(err)
		}
		return m
	}

	var logs bytes.Buffer
	prevLog := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prevLog) })

	update := func(at time.Time) (passes uint64) {
		t.Helper()
		before := pgdelta.KeyNullPasses()
		if _, _, _, err := f.run(root, pgRun{at: at, carry: true, chainFloor: f.base}); err != nil {
			t.Fatalf("the update at %s: %v", at.Format("15:04"), err)
		}
		passes = pgdelta.KeyNullPasses() - before
		got, _ := f.state(root, at)
		if want := source(); !sameRows(got, want) {
			t.Fatalf("after the update at %s the snapshot holds %v, and the source %v", at.Format("15:04"), got, want)
		}
		if left := f.pairFiles(root, at); len(left) != 2 {
			t.Fatalf("the update at %s left delta files %v, want one pair", at.Format("15:04"), left)
		}
		if !f.sameFile(f.tableFile(root, taken), f.tableFile(root, at)) {
			t.Fatalf("the update at %s did not carry the full read's file", at.Format("15:04"))
		}
		return passes
	}

	change(0x700, 12*time.Minute, 2, "1", s("one"))
	change(0x710, 13*time.Minute, 3, "2", nil)
	change(0x720, 14*time.Minute, 1, "40", s("forty"))
	f.checkpoint(0x720)
	at1 := f.at(20 * time.Minute)
	if n := update(at1); n != 1 {
		t.Fatalf("the first update read the file's key columns for a NULL %d times, want once", n)
	}

	change(0x800, 32*time.Minute, 2, "40", s("forty again"))
	change(0x810, 33*time.Minute, 1, "2", s("two is back"))
	change(0x820, 34*time.Minute, 3, "100", nil)
	change(0x830, 35*time.Minute, 2, "5", s("five"))
	f.checkpoint(0x830)
	at2 := f.at(40 * time.Minute)
	if n := update(at2); n != 0 {
		t.Fatalf("the second update read the same file's key columns for a NULL %d times, want none", n)
	}

	lines := 0
	for _, line := range strings.Split(logs.String(), "\n") {
		if !strings.Contains(line, `msg="table published as its previous file and a delta"`) {
			continue
		}
		lines++
		fields := map[string]int64{}
		for _, m := range regexp.MustCompile(`\b(\w+_ms)=(-?\d+)`).FindAllStringSubmatch(line, -1) {
			n, _ := strconv.ParseInt(m[2], 10, 64)
			fields[m[1]] = n
		}
		var sum int64
		for _, k := range []string{"fetch_ms", "fold_ms", "positions_ms", "prev_dead_ms", "pair_write_ms"} {
			v, ok := fields[k]
			if !ok || v < 0 {
				t.Fatalf("the line has no %s: %s", k, line)
			}
			sum += v
		}
		if total, ok := fields["total_ms"]; !ok || sum > total {
			t.Fatalf("the steps add up to %d ms and the whole is %d ms (present %v): %s", sum, total, ok, line)
		}
	}
	if lines != 2 {
		t.Fatalf("%d lines of a table published with a delta, want 2:\n%s", lines, logs.String())
	}
}
