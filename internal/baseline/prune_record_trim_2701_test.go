package baseline

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

// #2701 point 4: the record cuts every list it names the way it cuts Names,
// and keeps how many each named in all, so the page's counts stay right.
func TestNotInDestination_listsAreCutAndCounted(t *testing.T) {
	dir := t.TempDir()
	var all []string
	for i := 0; i < 15; i++ {
		all = append(all, fmt.Sprintf("2026-11-%02dT00-00-00Z", i+1))
	}
	// 12 could not be asked about, listed newest first so the ones Names
	// keeps (the oldest five) come last; 3 of them were refused to read.
	notAsked := slices.Clone(all[3:])
	slices.Reverse(notAsked)
	refused := []string{all[14], all[13], all[2+1]}
	if err := writeNotInDestination(dir, NotInDestination{At: time.Now(), Count: 15, Names: all, NotAsked: notAsked, ReadRefused: refused}); err != nil {
		t.Fatal(err)
	}
	rec, ok, err := ReadNotInDestination(dir)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if len(rec.Names) != notInDestinationNamesKept || len(rec.NotAsked) != notInDestinationNamesKept || len(rec.ReadRefused) != 3 {
		t.Fatalf("lists kept: names %d, not asked %d, read refused %d", len(rec.Names), len(rec.NotAsked), len(rec.ReadRefused))
	}
	if rec.NotAskedCount != 12 || rec.ReadRefusedCount != 3 {
		t.Fatalf("counts: not asked %d, read refused %d; want 12 and 3", rec.NotAskedCount, rec.ReadRefusedCount)
	}
	// Every name shown that could not be asked about is still said so.
	for _, n := range rec.Names {
		if slices.Contains(notAsked, n) && !slices.Contains(rec.NotAsked, n) {
			t.Errorf("%s is shown and could not be asked about, and the cut list leaves it out (so the page would say S3 lacks it)", n)
		}
	}
	// A record with short lists is written as it was, counts filled in.
	if err := writeNotInDestination(dir, NotInDestination{At: time.Now(), Count: 2, Names: all[:2], NotAsked: all[:1]}); err != nil {
		t.Fatal(err)
	}
	rec, _, _ = ReadNotInDestination(dir)
	if !slices.Equal(rec.NotAsked, all[:1]) || rec.NotAskedCount != 1 || rec.ReadRefusedCount != 0 {
		t.Fatalf("a short record: %+v", rec)
	}
}
