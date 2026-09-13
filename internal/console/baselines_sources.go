package console

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// baselineListTimeout bounds ONE location's listing.
//
// A local directory answers off the filesystem; an S3 one opens DuckDB, loads
// httpfs and globs a bucket, and this runs inside `bintrail-console watch`,
// which is also the capture process. The Backups page polls every ~10s while a
// run is in flight and the server sets no WriteTimeout, so an endpoint that
// accepts and never answers would hold a request open indefinitely.
//
// Generous rather than tight: a slow bucket that eventually answers must not be
// reported as unreadable, which would be this endpoint claiming an incomplete
// listing on a healthy configuration.
const baselineListTimeout = 15 * time.Second

// baselineSourceDTO is one location the listing read, and what came of reading
// it. Present for every configured location, including one that failed: a
// listing that quietly drops an unreadable bucket is the same lie as one that
// never looked at it (#1542).
type baselineSourceDTO struct {
	Source string `json:"source"`
	Kind   string `json:"kind"` // "dir" | "s3"
	// Count is how many baseline files this location held. Zero with no Error
	// is a real answer: the location is readable and empty.
	Count int `json:"count"`
	// Error is the reason this location contributed nothing. When it is set the
	// listing beside it is INCOMPLETE, and the page has to say so rather than
	// render a shorter list as if it were the whole set.
	Error string `json:"error,omitempty"`
}

// baselineKindOf classifies a source the way the rest of the console does.
func baselineKindOf(source string) string {
	if strings.HasPrefix(source, "s3://") {
		return "s3"
	}
	return "dir"
}

// baselineFileKey identifies the same snapshot file across locations. Time is
// carried as UnixNano so the key stays comparable; two locations holding the
// same snapshot agree on its directory timestamp, which is where both listers
// derive SnapshotTime from.
type baselineFileKey struct {
	unixNano int64
	schema   string
	table    string
}

func keyOf(f reconstruct.BaselineFile) baselineFileKey {
	return baselineFileKey{unixNano: f.SnapshotTime.UnixNano(), schema: f.Schema, table: f.Table}
}

// mergedBaselines is the union of every configured baseline location.
type mergedBaselines struct {
	// Files is the deduplicated union, newest snapshot first, then by
	// schema.table — the order a single lister already returns, preserved so
	// the grouping loop downstream is unchanged.
	Files []reconstruct.BaselineFile
	// Kinds maps a snapshot's timestamp to the sorted set of locations any of
	// its files were found in.
	Kinds map[int64][]string
	// Sources reports every location that was consulted, in consult order.
	Sources []baselineSourceDTO
	// Listed counts the locations that answered. Zero means nothing could be
	// read, which is the only case that is still a hard failure.
	Listed int
	// InS3 marks every file an s3 location listed, whichever path Files kept
	// for it. The coverage card needs it: Restore folds from the bucket on an
	// S3-backed server, and a file present in both locations keeps its LOCAL
	// path below, so the path alone cannot say whether the bucket has it.
	InS3 map[baselineFileKey]bool
}

// listBaselinesMerged lists every configured location and returns their union.
//
// Merged rather than "fall back when the primary is empty", because the empty
// case is not the only one that hides snapshots and not even the common one.
// Local retention prunes old snapshots while the durable S3 copies remain
// (#616/#766), so as soon as ONE local snapshot survives, a fallback-on-empty
// rule reports that single snapshot and hides every older one in the bucket.
// The union is the set the operator actually has.
//
// A location that fails does NOT fail the request when another answered. The
// bug this fixes is a listing that looks complete and is not; replacing it with
// a page that shows nothing because a bucket was briefly unreachable would be
// the same failure with worse manners. It is recorded on the source instead,
// and every caller has to render it.
// baselineLister is reconstruct.ListBaselines, taken as a parameter rather than
// called directly so a test can drive the merge across BOTH kinds. An s3://
// source cannot be listed from a unit test, and the three things most likely to
// break here — dedup across locations, the union of kinds, and preferring the
// local path for the footer read — are exactly the ones that only appear when
// the two locations are of different kinds.
type baselineLister func(ctx context.Context, source string) ([]reconstruct.BaselineFile, error)

func listBaselinesMerged(ctx context.Context, sources []string, list baselineLister) mergedBaselines {
	out := mergedBaselines{Kinds: map[int64][]string{}, InS3: map[baselineFileKey]bool{}}
	seen := map[baselineFileKey]int{}
	kindSeen := map[int64]map[string]bool{}

	for _, src := range sources {
		if src == "" {
			continue
		}
		kind := baselineKindOf(src)
		report := baselineSourceDTO{Source: src, Kind: kind}
		srcCtx, cancel := context.WithTimeout(ctx, baselineListTimeout)
		files, err := list(srcCtx, src)
		cancel()
		if err != nil {
			report.Error = err.Error()
			out.Sources = append(out.Sources, report)
			// Logged as well as returned. The response body is the ONLY other
			// copy, so without this a bucket that has been failing since a
			// credential expiry is visible to whoever has the tab open and to
			// nobody else — not cron, not alerting, not the journal. The
			// coverage endpoint already warns on the identical failure from the
			// identical call, and this handler already warns about the far
			// smaller failure of one unreadable Parquet footer.
			slog.Warn("console: a backup location could not be listed; the listing beside it is incomplete",
				"source", src, "kind", kind, "error", err)
			continue
		}
		out.Listed++
		report.Count = len(files)
		out.Sources = append(out.Sources, report)

		for _, f := range files {
			ts := f.SnapshotTime.UnixNano()
			if kindSeen[ts] == nil {
				kindSeen[ts] = map[string]bool{}
			}
			kindSeen[ts][kind] = true

			k := keyOf(f)
			if kind == "s3" {
				out.InS3[k] = true
			}
			if idx, dup := seen[k]; dup {
				// Keep the LOCAL path when the same file exists in both. The
				// footer read downstream opens Path directly, and doing that
				// over S3 is the latency this listing deliberately avoids.
				if kind == "dir" && baselineKindOf(out.Files[idx].Path) == "s3" {
					out.Files[idx].Path = f.Path
				}
				continue
			}
			seen[k] = len(out.Files)
			out.Files = append(out.Files, f)
		}
	}

	for ts, kinds := range kindSeen {
		list := make([]string, 0, len(kinds))
		for k := range kinds {
			list = append(list, k)
		}
		sort.Strings(list)
		out.Kinds[ts] = list
	}

	// One lister returns newest-first already; two concatenated do not. Sorted
	// here rather than left to the caller because the grouping loop downstream
	// starts a new snapshot every time the timestamp CHANGES, so an interleaved
	// order would emit the same snapshot twice.
	sort.SliceStable(out.Files, func(i, j int) bool {
		a, b := out.Files[i], out.Files[j]
		if !a.SnapshotTime.Equal(b.SnapshotTime) {
			return a.SnapshotTime.After(b.SnapshotTime)
		}
		if a.Schema != b.Schema {
			return a.Schema < b.Schema
		}
		return a.Table < b.Table
	})
	return out
}

// baselineSourcesOf lists the locations a bundle can read, primary first.
//
// Both come off the bundle rather than off the registry entry, because the
// bundle is where the local-wins-over-S3 resolution already happened and a
// second opinion about it here is how the listing and findBaseline would come
// to disagree about which files exist.
func baselineSourcesOf(b *bundle) []string {
	if b == nil || b.baselineSrc == "" {
		return nil
	}
	if b.baselineFallbackSrc == "" {
		return []string{b.baselineSrc}
	}
	return []string{b.baselineSrc, b.baselineFallbackSrc}
}

// bundleBaselineDir and bundleBaselineS3 split a bundle's resolved locations
// back by kind, for the one caller that needs to classify them the way a
// registry entry's own fields are classified (the coverage card's
// restoreReadsFrom). The bundle keeps them as primary/fallback because that is
// the order findBaseline consults them; which is which by KIND is what the
// Restore rule needs.
func bundleBaselineDir(b *bundle) string {
	for _, src := range baselineSourcesOf(b) {
		if baselineKindOf(src) == "dir" {
			return src
		}
	}
	return ""
}

func bundleBaselineS3(b *bundle) string {
	for _, src := range baselineSourcesOf(b) {
		if baselineKindOf(src) == "s3" {
			return src
		}
	}
	return ""
}
