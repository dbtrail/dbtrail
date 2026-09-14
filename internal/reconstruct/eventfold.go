package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// This file holds the streamed event-window fold shared by both full-table
// reconstruct paths (#1097).
//
// The full-table merge is a hash join: the change map is the build side, the
// baseline Parquet scan is the probe side. The build side must be COMPLETE
// before the probe starts, because the probe visits each baseline row once and
// needs that PK's final image right then. What does NOT have to be complete
// first is the raw event window that the map is folded from — and that is what
// used to be materialized whole (one query.FetchMerged call for the entire
// window, every event carrying both decoded JSON row images).
//
// So the fetch is paged and folded incrementally: peak memory moves from
// "every event in the window" to "one page + one entry per distinct touched
// PK". The map itself is still unbounded in the number of touched PKs — see
// #1107, which bounds that half by hash-partitioning the join.

// retainEvent returns a heap copy of ev trimmed to what the merge downstream
// actually reads, for storing in the change map.
//
// The copy is the load-bearing part. Storing &page[i] would pin the ENTIRE
// page's backing array — including every row_before in it — for as long as the
// map lives, so a paged fetch would bound nothing: the map would keep every
// page alive anyway. Copying the struct out lets each page be collected as soon
// as the next one is fetched.
//
// Fields dropped, and why it is safe to drop them:
//
//   - RowBefore: mergeBaselineImages emits row_after only, and the THREE
//     consumers that read a before-image have all been served before this call
//     — #592 unresolved-TOAST and #782 PK-changing UPDATE run per event in
//     foldPage, and the #843 dropped-column guard gets what it needs from
//     foldResult.observeImages. Serving them upstream of the trim is precisely
//     what makes dropping it safe; none of them may be re-expressed against the
//     map, where they would read a nil before-image and silently pass.
//     Enumerating them is not decoration: the #843 reader was missed on the
//     first pass and its guard went dead while its test stayed green, because
//     the test built the map by hand. Before adding any consumer of this map,
//     check what it reads.
//   - QueryText/QueryHash: forensic capture (#699), capped at 16 KiB per event.
//     No merge stage reads them, and retaining them would let a
//     statement-logging source dominate the map.
//
// Anything a future merge stage needs must be added back here deliberately —
// a field read from a map entry that this function blanks reads as empty, not
// as missing.
func retainEvent(ev *query.ResultRow) *query.ResultRow {
	kept := *ev
	kept.RowBefore = nil
	kept.QueryText = nil
	kept.QueryHash = nil
	return &kept
}

// foldPage folds one fetched page into changes, running the per-event
// correctness guards on each untrimmed event first.
//
// It is the single place where an event is inspected before being trimmed, and
// therefore the only place a before-image guard can live once the window is
// paged. Split out from foldEventWindow so those guards are unit-testable
// without a MySQL index connection — including across a page BOUNDARY, which is
// the case a whole-slice scan could never get wrong and a paged fold could.
//
// Guards, per event:
//
//   - #592: a residual unchanged-TOAST marker would be written into the
//     reconstructed dump as the marker's own JSON — silent corruption.
//     NOTE this is stricter than the map-level check it replaced: that one
//     only saw the surviving last event per PK, this sees every event,
//     including ones a later event overwrote. Harmless today because the
//     marker is a PostgreSQL concept and full-table reconstruct refuses PG
//     sources outright (#597, gated in ReconstructTable) — so it cannot fire
//     on a MySQL window at all. If that PG gate is ever lifted, revisit this:
//     an overwritten event carrying a marker would then refuse a run whose
//     OUTPUT would have been clean.
//   - #782: the change map is keyed by the BEFORE-image PK, so folding an
//     UPDATE whose PK changed would duplicate, resurrect, or silently drop rows.
//     Checking per event rather than over the finished map catches the
//     permutation a map scan structurally cannot see (`UPDATE pk 1→2` followed
//     by `INSERT pk=1`, where the INSERT overwrites the UPDATE under key "1") —
//     and it catches it no matter which page each event landed on.
//
// Both refuse the whole run. That is deliberate and matches the pre-existing
// stance for these two conditions: the alternative is a dump that loads cleanly
// and is wrong.
func foldPage(
	page []query.ResultRow,
	schema, table string,
	pkCols []metadata.ColumnMeta,
	res *foldResult,
) error {
	for i := range page {
		ev := &page[i]
		if err := checkEventToast(*ev); err != nil {
			return err
		}
		if before, after, ok := pkChangedInEvent(ev, pkCols); ok {
			return pkChangingUpdateErr(schema, table, before, after)
		}
		// Sample both images for the #843 guard BEFORE the trim discards the
		// before-image. Order matters: retainEvent must be the last thing that
		// touches this event.
		res.observeImages(ev)
		res.Changes[ev.PKValues] = retainEvent(ev)
	}
	return nil
}

// observeImages narrows ImageColumns to the intersection with each of ev's
// non-nil row images.
//
// One deliberate difference from the collapsed-map scan this replaces: it sees
// EVERY event, not just the surviving last-event-per-PK. That makes it strictly
// more sensitive — a baseline column dropped and later re-added inside the same
// window is now flagged, where the map scan stayed quiet because the surviving
// image had the column back. Refusing there is defensible (the dump really
// would mix schema epochs: pass-through rows carry the pre-drop value while
// touched rows carry the post-re-add one) and it is the safe direction for a
// data-correctness guard, but it IS a behavior change — see the table cases in
// schema_drift_843_test.go.
func (r *foldResult) observeImages(ev *query.ResultRow) {
	for _, img := range []map[string]any{ev.RowAfter, ev.RowBefore} {
		if img == nil {
			continue
		}
		if !r.SawImage {
			r.SawImage = true
			r.ImageColumns = make(map[string]struct{}, len(img))
			for col := range img {
				r.ImageColumns[col] = struct{}{}
			}
			continue
		}
		for col := range r.ImageColumns {
			if _, ok := img[col]; !ok {
				delete(r.ImageColumns, col)
			}
		}
	}
}

// foldConfig drives foldEventWindow.
type foldConfig struct {
	DB       *sql.DB
	Engine   *query.Engine
	DBName   string
	Resolver *metadata.Resolver

	Schema string
	Table  string
	PKCols []metadata.ColumnMeta

	// Opts is the event filter for the window. Limit, LimitPerPK and AfterEvent
	// must be unset — the stream owns paging and rejects a caller that presets
	// any of them. Order must not be "DESC" (ascending is the only direction a
	// forward cursor can walk); leaving it empty is the norm.
	Opts query.Options

	AllowGaps bool
	// ArchiveFetcher is REQUIRED, not optional: FetchMergedOptions.validate
	// rejects a nil fetcher whenever NoArchive is false, and this fold always
	// passes NoArchive=false. Callers resolve the default
	// (parquetquery.Fetch, #510) themselves at their point of use.
	ArchiveFetcher query.ArchiveFetcher

	// BatchSize is the fetch page size; 0 → query.DefaultStreamBatchSize.
	BatchSize int

	// WarnEventThreshold / Parallelism drive the #654/#842 volume warning.
	WarnEventThreshold int64
	Parallelism        int
	// MaxTouchedRows is FullTableConfig.MaxTouchedRows, divided by Parallelism
	// at the check.
	MaxTouchedRows int64

	// RemediationHint is the advice attached to that warning; empty uses the
	// attended-CLI wording. See FullTableConfig.RemediationHint.
	RemediationHint string
}

// foldResult is the completed build side of the merge.
type foldResult struct {
	// Changes maps pk_values → the LAST event for that PK in the window,
	// trimmed by retainEvent. Last-write-wins survives paging because pages
	// arrive in ascending (event_timestamp, event_id) order and each page is
	// folded in order, so a PK touched in page 1 and again in page 3 ends up
	// holding page 3's image — the same result a single-slice fold produced.
	Changes map[string]*query.ResultRow

	// Total is the number of events folded across every page (the
	// archive-inclusive, deduplicated count — the same number the old
	// len(events) reported).
	Total int64

	// First is a copy of the very first event of the window, or nil when the
	// window was empty. Kept because the baseline-vs-first-event gap warning
	// (#781) needs it and the page it came from is long gone by then.
	First *query.ResultRow

	// ImageColumns is the INTERSECTION of the column key-sets of every non-nil
	// row image seen in the window, and SawImage reports whether any image was
	// seen at all (an intersection over nothing is not "everything").
	//
	// This exists because the #843 dropped-column guard reads BEFORE-images,
	// which retainEvent throws away — the guard's signal for a PK whose last
	// event is a DELETE is precisely that DELETE's before-image. Since the
	// guard also needs the baseline column list, which is not known until the
	// baseline is materialized (well after the fold), the fold cannot run the
	// check itself. So it carries out the only part that needs the untrimmed
	// event, in bounded form: a column is "missing from some image" exactly
	// when it is absent from this intersection, which is all
	// droppedBaselineColumns ever asked.
	//
	// Bounded by the table's column count, not by events or PKs.
	ImageColumns map[string]struct{}
	SawImage     bool
}

// foldEventWindow streams the event window described by fc and folds it into a
// change map, running the per-event correctness guards on the way.
//
// Per page, in order:
//
//  1. ENUM/SET ordinals → labels and BLOB/TEXT base64 → real values, both
//     resolved against the schema snapshot in effect at each event's OWN
//     timestamp (#475/#476/#668). A page is a valid unit for this because the
//     decoding is per-event, not window-relative.
//  2. The #592 unresolved-TOAST and #782 PK-changing-UPDATE guards, per event,
//     on the untrimmed event.
//  3. The fold, via retainEvent.
//
// Running the guards per event over the raw stream is strictly stronger than
// the map-level checks it replaces: the map only ever held the surviving last
// event per PK, so a PK-changing UPDATE whose old key a later event reused was
// invisible to it. That is why fulltable.go's baseline path no longer calls
// pkChangingUpdate/checkChangesToast on the map at all — a nil-before-image map
// would make those calls unconditionally pass while still reading as guards.
func foldEventWindow(ctx context.Context, fc foldConfig) (*foldResult, error) {
	res := &foldResult{Changes: make(map[string]*query.ResultRow)}
	warned := false
	// Captured so the error path can tell a refusal from the fold apart from a
	// fetch failure; FetchMergedStream returns fn's error verbatim, which makes
	// the two indistinguishable at the call site otherwise.
	var foldErr error
	// Built ONCE for the whole window, not per page: both decoding passes need
	// the snapshot-epoch list and a resolver per epoch touched, and rebuilding
	// that state per page would turn one schema_snapshots query into one per
	// page. Its typed-ness accumulates across pages for the same reason.
	dec := newEventDecoder(fc.DB, fc.Schema, fc.Table, fc.Resolver)

	_, err := query.FetchMergedStream(ctx, fc.DB, fc.Engine, query.FetchMergedOptions{
		Opts:           fc.Opts,
		DBName:         fc.DBName,
		NoArchive:      false,
		AllowGaps:      fc.AllowGaps,
		ArchiveFetcher: fc.ArchiveFetcher,
	}, fc.BatchSize, func(page []query.ResultRow) error {
		if len(page) == 0 {
			return nil
		}
		dec.mapEnums(page)
		dec.decodeBinaries(page)

		if res.First == nil {
			first := page[0]
			res.First = &first
		}

		if err := foldPage(page, fc.Schema, fc.Table, fc.PKCols, res); err != nil {
			foldErr = err
			return err
		}
		// Per page, not after the window: the memory is spent as the map
		// grows, so a check at the end would come after the harm (#1107).
		if limit := scaledEventThreshold(fc.MaxTouchedRows, fc.Parallelism); limit > 0 && int64(len(res.Changes)) > limit {
			// No table prefix: the run's error adds "schema.table: ".
			// No remedy here: it differs per surface (a scheduled update falls
			// back to a full backup, a restore needs a closer moment), and the
			// surfaces add their own.
			foldErr = fmt.Errorf("%w: more than %s distinct rows changed between the backup this starts from and the target moment, "+
				"more than one table may hold in memory while %d tables are processed at once",
				ErrTouchedRowBudget, groupThousands(limit), max(fc.Parallelism, 1))
			return foldErr
		}

		res.Total += int64(len(page))
		// Warn as soon as the running total crosses, not after the last page:
		// a run large enough to trip the threshold is exactly the run that may
		// not survive to reach the end, and a warning the operator never sees
		// is no warning. Latched so it fires once per table, not once per page.
		if !warned && shouldWarnEvents(res.Total, scaledEventThreshold(fc.WarnEventThreshold, fc.Parallelism)) {
			maybeWarnEventVolume(fc.Schema, fc.Table, res.Total, fc.WarnEventThreshold, fc.Parallelism, fc.RemediationHint)
			warned = true
		}
		return nil
	})
	if err != nil {
		// Only a TRANSPORT failure gets the "fetch events" label. fn's errors
		// are the #592/#782 refusals, which FetchMergedStream propagates
		// unchanged — prefixing those sends the operator to check DB/S3
		// connectivity instead of reading the actionable message underneath.
		if foldErr != nil {
			return nil, foldErr
		}
		// A coverage gap in a window anchored at the baseline usually means
		// the baseline went STALE — its anchor slid past rotation retention.
		// Name the fix (#1193): no flag recovers hours that were rotated out
		// unarchived.
		var gapErr *query.GapError
		if errors.As(err, &gapErr) && fc.Opts.Since != nil {
			return nil, fmt.Errorf("fetch events: %w — the delta window starts at this table's newest usable baseline anchor (%s); if archive_state drifted, `bintrail archive reconcile --repair` is the cheap fix, and if the missing hours were rotated out unarchived, take a fresh baseline (bintrail dump + bintrail baseline)",
				err, fc.Opts.Since.UTC().Format(time.RFC3339))
		}
		return nil, fmt.Errorf("fetch events: %w", err)
	}

	// The decoder degrades to leaving BLOB/TEXT as stored base64 when no
	// snapshot epoch covers an event or its resolver won't load. Until now that
	// was a Debug line and the verdict was discarded, so a dump could carry
	// base64 in place of real values with nothing above Debug to say so.
	if !dec.typed {
		slog.Warn("reconstruct: BLOB/TEXT values could not be typed against any schema snapshot and are "+
			"left BASE64-ENCODED in the output — the dump will not round-trip those columns",
			"schema", fc.Schema, "table", fc.Table,
			"hint", "run `bintrail snapshot` so the window's events resolve against a schema snapshot")
	}

	slog.Debug("event window folded",
		"schema", fc.Schema, "table", fc.Table,
		"events", res.Total, "touched_pks", len(res.Changes))
	return res, nil
}

// groupThousands renders n with comma separators, for a limit a person reads.
func groupThousands(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}
