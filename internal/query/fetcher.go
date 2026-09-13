package query

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
)

// Fetcher is the one read surface a consumer that walks events needs. The
// live *Engine satisfies it on its own; MergedFetcher satisfies it over the
// live index PLUS the Parquet archives. Cascade victim synthesis takes a
// Fetcher so the same walk serves an index whose history has rotated out
// (#1615): before it, the cascade scan was the one read path that never saw
// the archive tier while query and recover merged it automatically.
type Fetcher interface {
	Fetch(ctx context.Context, opts Options) ([]ResultRow, error)
}

// MergedFetcher answers Fetch through FetchMergedFull — the same discovery,
// planner routing and MergeAndTrim that recover and reconstruct use — so
// Options.LimitPerPK/Limit/Order/SincePos/ColumnEq hold across live and
// archived rows exactly as they do on those paths.
//
// Two policy choices are fixed here, not exposed:
//
//   - AllowGaps is true. Coverage of the window is the CALLER's business (the
//     cascade gate answers it per edge, and records what it could not cover);
//     a planner *GapError on every scan that reaches before the index's first
//     hour would turn the ordinary lookback window into a refusal.
//   - A source that resolved but could not be READ is an error. FetchMergedFull
//     under AllowGaps returns such sources as a list; a caller with a plain
//     Fetch contract cannot see that list, and a scan over a partial archive
//     set presented as a result is exactly the silent partial the cascade's
//     caveats exist to prevent. The cascade treats a fetch error as
//     "provably partial" and says so.
//   - A DISCOVERY failure (archive_state unreadable, or the SourceResolver
//     erroring) is not: the scan proceeds live-only with a warning, and the
//     consumer's own coverage probe is where "coverage unknown" is reported —
//     the cascade surfaces run exactly that probe beside every scan.
//
// Discovery runs ONCE per fetcher (memoized, error included): the cascade
// issues one scan per (edge, parent) and the archive set does not change
// under a single recovery.
//
// Elided archives (FetchMergedFull proved they could not change the result)
// are logged at debug: completeness-preserving, never a scope reduction.
type MergedFetcher struct {
	DB     *sql.DB
	Engine *Engine
	// DBName enables the planner. Empty = no routing proofs (every archive
	// is read) and no coverage classification; the merge is still correct.
	DBName string
	// NoArchive confines the read to the live index — the caller's
	// --no-archive / profile decision, mirrored so the coverage probe it
	// pairs with (cascade.WindowProbe) can be told the same thing.
	NoArchive bool
	// ArchiveFetcher reads one archive source; parquetquery.Fetch in
	// production, a tuned wrapper on the CLI. Required unless NoArchive.
	ArchiveFetcher ArchiveFetcher
	// SourceResolver overrides archive discovery (FetchMergedOptions.
	// SourceResolver); nil = archive_state via ResolveArchiveSources.
	SourceResolver SourceResolver

	once    sync.Once
	sources []string
	srcErr  error
}

// resolveOnce memoizes archive discovery for the fetcher's lifetime.
func (m *MergedFetcher) resolveOnce(ctx context.Context, db *sql.DB) ([]string, error) {
	m.once.Do(func() {
		resolve := m.SourceResolver
		if resolve == nil {
			resolve = ResolveArchiveSources
		}
		m.sources, m.srcErr = resolve(ctx, db)
	})
	return m.sources, m.srcErr
}

// Fetch implements Fetcher.
func (m *MergedFetcher) Fetch(ctx context.Context, opts Options) ([]ResultRow, error) {
	rows, _, skipped, _, elided, err := FetchMergedFull(ctx, m.DB, m.Engine, FetchMergedOptions{
		Opts:           opts,
		DBName:         m.DBName,
		NoArchive:      m.NoArchive,
		AllowGaps:      true,
		ArchiveFetcher: m.ArchiveFetcher,
		SourceResolver: m.resolveOnce,
	})
	if err != nil {
		return nil, err
	}
	unread := skipped[:0:0]
	for _, s := range skipped {
		if s != DiscoveryFailedSource {
			unread = append(unread, s)
		}
	}
	if len(unread) > 0 {
		return nil, fmt.Errorf("archive source(s) could not be read, so the scan is partial: %v", unread)
	}
	if elided {
		slog.Debug("merged fetch: registered archives provably could not change this scan; not read",
			"schema", opts.Schema, "table", opts.Table)
	}
	return rows, nil
}
