package baseline

import (
	"context"
	"fmt"
	"time"
)

// TableDeltaStart reads where a chain of deltas began (#1707): the
// MetaKeyDeltaChainStart of its newest pair's upserts file. A nil chain is
// "no delta" and answers the zero time with no error.
//
// Every pair of a chain carries the same start; the last one is read because
// it is the one a refresh resumes from, so the two agree.
func TableDeltaStart(ctx context.Context, chain *TableDeltaChain) (time.Time, error) {
	if chain == nil {
		return time.Time{}, nil
	}
	return TableDeltaStartAt(ctx, chain.LastFileUpserts())
}

// TableDeltaStartAt is TableDeltaStart given the newest pair's upserts file
// (a path or an s3:// URL), for a listing that kept that one name and not the
// chain.
//
// It is THE read of that instant. reconstruct.FindBaseline bounds every
// reader's event fetch with it, and the backup-age verdict grades on it, so
// the two cannot come to name different instants for the same table.
func TableDeltaStartAt(ctx context.Context, upserts string) (time.Time, error) {
	um, err := ReadParquetMetadataAny(ctx, upserts)
	if err != nil {
		return time.Time{}, fmt.Errorf("read table delta %s: %w", upserts, err)
	}
	if um.DeltaChainStart.IsZero() {
		// The pair exists and cannot say where its chain began. Every safe
		// answer needs that instant, so there is none to give.
		return time.Time{}, fmt.Errorf("table delta %s records no chain start (%s); "+
			"it cannot be read safely; take a full snapshot to replace this one",
			upserts, MetaKeyDeltaChainStart)
	}
	return um.DeltaChainStart, nil
}

// NewestTableDeltaUpserts reads the file names of ONE directory and returns,
// for each table with a chain of deltas beside it, the path of its newest
// pair's upserts file, keyed by the table's stem (its file name without
// ".parquet"). A table whose delta files do not form a chain (half a pair, a
// missing sequence, two layouts) is in damaged instead, with the reason.
//
// Per table it is TableDeltaChainIn: the same names reach the same
// MarkTableDeltaFiles. It exists because a listing asks about every table of
// a directory at once, and asking TableDeltaChainIn once per table reads
// every name once per table.
func NewestTableDeltaUpserts(dir string, names []string) (newest map[string]string, damaged map[string]error) {
	byStem := map[string][]string{}
	for _, n := range names {
		if stem, _, _, ok := ParseTableDeltaName(n); ok {
			byStem[stem] = append(byStem[stem], n)
		}
	}
	newest = make(map[string]string, len(byStem))
	damaged = map[string]error{}
	for stem, mine := range byStem {
		chains, err := MarkTableDeltaFiles(dir, mine)
		if err != nil {
			damaged[stem] = err
			continue
		}
		if c := chains[dirJoin(dir, stem+".parquet")]; c != nil {
			newest[stem] = c.LastFileUpserts()
		}
	}
	return newest, damaged
}
