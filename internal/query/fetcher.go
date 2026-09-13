package query

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
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
//     erroring) is not: the scan proceeds live-only with a warning. The
//     failure is not lost — Sources and Scope return it on every call, so a
//     coverage probe built on this fetcher (cascade.WindowProbe) reports
//     "could not verify" instead of crediting archives the scan never opened.
//
// Discovery runs ONCE per fetcher (memoized, error included): the cascade
// issues one scan per (edge, parent) and the archive set does not change
// under a single recovery.
//
// Elided archives (FetchMergedFull proved they could not change the result)
// are logged at debug: completeness-preserving, never a scope reduction.
// Construct one per recovery: discovery and the gap/elision record are
// memoized for the fetcher's lifetime, so a cached instance would pin a stale
// archive set (and a stale discovery error) across requests.
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

	gapMu  sync.Mutex
	gaps   map[time.Time]struct{} // hours no scan so far could serve (see GapHours)
	elided bool                   // some scan proved the archives could not change it (#1353)
	// preIndex is the earliest hour any scan's planner knew the index ever
	// held; gap hours before it never rotated away (the index did not exist
	// yet, #1126) and are worded as such by GapNote.
	preIndex time.Time
}

// ArchiveReadError is the error Fetch returns when one or more resolved
// archive sources could not be read: the scan is partial and says which.
// Callers that offer an escape hatch (the CLI's --no-archive) match on it
// with errors.As rather than decorating every fetch error with the hint.
type ArchiveReadError struct{ Sources []string }

func (e *ArchiveReadError) Error() string {
	return fmt.Sprintf("archive source(s) could not be read, so the scan is partial: %v", e.Sources)
}

// Sources returns the archive sources this fetcher opens (memoized; the
// discovery error, if any, is returned on every call). Under NoArchive it
// resolves nothing and returns no sources.
func (m *MergedFetcher) Sources(ctx context.Context) ([]string, error) {
	if m.NoArchive {
		return nil, nil
	}
	return m.resolveOnce(ctx, m.DB)
}

// Scope is the ArchiveScope of what this fetcher's scans actually open —
// the same set resolveMergeSources hands the planner — so a coverage probe
// (cascade.WindowProbe → WindowCovered) credits exactly the archives the
// scan reads and no other. Under NoArchive the scope opens nothing; a
// discovery failure is an error, never "all archives".
func (m *MergedFetcher) Scope(ctx context.Context) (ArchiveScope, error) {
	srcs, err := m.Sources(ctx)
	if err != nil {
		return ArchiveScope{}, err
	}
	return ScopeFromPaths(srcs), nil
}

// GapHours lists, sorted, every hour inside a window some Fetch scanned that
// no tier could serve: not live, and not held by an archive the fetcher
// opens (under NoArchive an archived-only hour counts, since the scan will
// not read it). Known to the planner on every scan and, under AllowGaps,
// otherwise only logged — a consumer that reports coverage reads it here.
func (m *MergedFetcher) GapHours() []time.Time {
	m.gapMu.Lock()
	defer m.gapMu.Unlock()
	out := make([]time.Time, 0, len(m.gaps))
	for h := range m.gaps {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

// GapNote renders GapHours as ONE advisory sentence for the consumer's
// warnings, or "" when no scan crossed a gap. Advisory, not a coverage
// caveat: the cascade's lookback window is a heuristic reach into history
// (30 days by default), and on a young index or one that drops without
// archiving most of it is structurally unheld — flagging every run
// INCOMPLETE for that would bury the caveats that name real, specific
// losses. The note still says what the operator needs: which hours, and
// why, in the posture of this run.
func (m *MergedFetcher) GapNote() string {
	gaps := m.GapHours()
	if len(gaps) == 0 {
		return ""
	}
	first, last := GapRange(gaps)
	cause := "rotated out with no archive"
	if m.NoArchive {
		cause = "rotated out of the live index, and archives are excluded on this run"
	}
	if !m.preIndex.IsZero() && gaps[0].Before(m.preIndex) {
		cause += ", or before the index existed"
	}
	return fmt.Sprintf("%d hour(s) inside the scanned windows are not held by this scan (%s): %s – %s; "+
		"events that exist only there could not be searched", len(gaps), cause, first, last)
}

// Notes are the advisory sentences a human-facing consumer appends to its
// warnings after its scans: the gap note (GapNote) and, per #1353's audit
// requirement, the fact that registered archives went unread on some scan
// because the live index provably satisfied it. Never coverage caveats.
func (m *MergedFetcher) Notes() []string {
	var notes []string
	if n := m.GapNote(); n != "" {
		notes = append(notes, n)
	}
	m.gapMu.Lock()
	elided := m.elided
	m.gapMu.Unlock()
	if elided {
		notes = append(notes, "registered archives were not read for some scans: the live index provably satisfied them (a completeness-preserving shortcut, not a scope reduction)")
	}
	return notes
}

func (m *MergedFetcher) recordGaps(plan *QueryPlan) {
	if plan == nil || len(plan.GapHours) == 0 {
		return
	}
	m.gapMu.Lock()
	defer m.gapMu.Unlock()
	if m.gaps == nil {
		m.gaps = map[time.Time]struct{}{}
	}
	for _, h := range plan.GapHours {
		m.gaps[h] = struct{}{}
	}
	if !plan.OldestKnownHour.IsZero() && (m.preIndex.IsZero() || plan.OldestKnownHour.Before(m.preIndex)) {
		m.preIndex = plan.OldestKnownHour
	}
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
	rows, plan, skipped, _, elided, err := FetchMergedFull(ctx, m.DB, m.Engine, FetchMergedOptions{
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
	m.recordGaps(plan)
	unread := skipped[:0:0]
	for _, s := range skipped {
		if s != DiscoveryFailedSource {
			unread = append(unread, s)
		}
	}
	if len(unread) > 0 {
		return nil, &ArchiveReadError{Sources: unread}
	}
	if elided {
		m.gapMu.Lock()
		m.elided = true
		m.gapMu.Unlock()
		slog.Debug("merged fetch: registered archives provably could not change this scan; not read",
			"schema", opts.Schema, "table", opts.Table)
	}
	return rows, nil
}
