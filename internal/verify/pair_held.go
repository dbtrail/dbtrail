package verify

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// FindBaselinePairHeld is FindBaselinePairWith with every local snapshot the
// pairs name held against the prune of any process until release is called
// (baseline.HoldSnapshot, #2675). The pairs are found once for the whole
// run and each is checked later, so a long run over many tables would
// otherwise find the snapshot of a late pair gone: the previous side of a
// pair is an older snapshot, the one a count removes first.
//
// A snapshot removed between the listing and the hold makes the pairs be
// found once more; removed again, the pairs are returned with what could be
// held, and the check of a pair whose snapshot is gone says so itself.
//
// release is never nil.
func FindBaselinePairHeld(ctx context.Context, source, other string) (pairs []BaselinePair, prevOnly []query.SchemaTable, release func(), err error) {
	release = func() {}
	for attempt := 0; attempt < 2; attempt++ {
		release()
		pairs, prevOnly, err = FindBaselinePairWith(ctx, source, other)
		if err != nil {
			return pairs, prevOnly, func() {}, err
		}
		var gone bool
		release, gone = holdPairSnapshots(pairs)
		if !gone {
			break
		}
	}
	return pairs, prevOnly, release, nil
}

// holdPairSnapshots holds each distinct local snapshot the pairs read from.
// gone reports that one of them was no longer there.
func holdPairSnapshots(pairs []BaselinePair) (release func(), gone bool) {
	var releases []func()
	seen := map[string]bool{}
	for _, p := range pairs {
		for _, path := range []string{p.PrevPath, p.NewPath} {
			if path == "" || strings.HasPrefix(path, "s3://") {
				continue
			}
			dir := filepath.Dir(filepath.Dir(path))
			if seen[dir] {
				continue
			}
			seen[dir] = true
			rel, g := baseline.HoldSnapshot(dir)
			releases = append(releases, rel)
			gone = gone || g
		}
	}
	return func() {
		for _, r := range releases {
			r()
		}
	}, gone
}
