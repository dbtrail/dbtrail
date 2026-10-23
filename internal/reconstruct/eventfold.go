package reconstruct

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dbtrail/dbtrail/internal/event"
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
// PK". Past the fold's in-memory limit, the map moves to disk in hash groups
// and the merge reads them a few at a time (#1107, changespill.go, which also
// says why DuckDB does not do this join).

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
//     — #592 unresolved-TOAST and the #782 PK-changing UPDATE split run per
//     event in foldPage, and the #843 dropped-column guard gets what it needs from
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
// BinlogFile and StartPos stay, and must: foldPage compares them to decide
// which change of a row is the last one (#2151). They cost nothing extra, the
// copy already carried them.
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
//     including ones a later event overwrote. The marker is a PostgreSQL
//     concept, so it cannot fire on a MySQL window. On a PostgreSQL fold
//     (#1596, reconstructPGTable) an overwritten event carrying a marker
//     refuses a run whose OUTPUT would have been clean: kept on purpose,
//     loud rather than clever, and documented in docs/postgres.md.
//   - #782/#2501: the change map is keyed by the BEFORE-image PK, so folding
//     an UPDATE whose PK changed under that one key would duplicate, resurrect,
//     or silently drop rows (the permutation a map scan structurally cannot
//     see: `UPDATE pk 1→2` followed by `INSERT pk=1`, where the INSERT
//     overwrites the UPDATE under key "1"). Since #2501 such an UPDATE is
//     folded as the two events it is, a DELETE of the old key and an INSERT
//     of the new one (splitPKChange), both at the UPDATE's own place in the
//     binary log, so the per-key "last in the binary log wins" rule below
//     orders each half against every other change of that key, on whichever
//     page it landed. Two shapes still refuse with the #782 error: a
//     PostgreSQL window (opts.pg: its rows have no binary log coordinate for
//     the ordering rule, and its before-image may hold only the key), and an
//     event whose decoded before-image does not spell the key exactly as the
//     parser stored it (splitPKChange says why that proof is the gate).
//
// The #592 guard and the two remaining #782 refusals refuse the whole run.
// That is deliberate and matches the pre-existing stance for these
// conditions: the alternative is a dump that loads cleanly and is wrong.
//
// sv is non-nil for a MariaDB system-versioned table (#2007): each event is
// first rewritten into its meaning for the current rows (or dropped, for the
// history versions the server keeps), so the guards above and the change map
// see an ordinary table keyed on the declared PK, which pkCols then is.
func foldPage(
	page []query.ResultRow,
	schema, table string,
	pkCols []metadata.ColumnMeta,
	sv *sysVersioned,
	opts foldOptions,
	res *foldResult,
) error {
	for i := range page {
		ev := &page[i]
		if sv != nil {
			keep, err := sv.normalize(ev)
			if err != nil {
				return err
			}
			if !keep {
				continue
			}
		}
		if err := checkEventToast(*ev); err != nil {
			return err
		}
		halves, err := splitPKChange(ev, schema, table, pkCols, opts.pg)
		if err != nil {
			return err
		}
		// Sample both images for the #843 guard BEFORE the trim discards the
		// before-image, and on the event itself, not on its halves: the guard
		// sees the event once, whole. Order matters: retainEvent must be the
		// last thing that touches this event.
		res.observeImages(ev)
		// The row keeps the change the binary log holds LAST, which is not
		// always the one that arrives last (#2151): pages come in
		// (event_timestamp, event_id) order and event_timestamp is when the
		// statement STARTED on the source. An UPDATE that waited on a row lock
		// commits after the change it waited for and carries an earlier time,
		// so it arrives first. Seen on MySQL 8.4 and MariaDB 11.4
		// (streamrun's statement_time_vs_binlog_order_2151 test). An event
		// that arrives after a change of the same row from further on in the
		// binary log is therefore dropped here, after every guard above has
		// read it.
		// Counted before the drop below: the binary log holds that event
		// whether or not the map keeps it, and the refresh's arithmetic
		// (#2505) is over what the log holds. A PK change counts ONCE, as the
		// UPDATE it is, never as its two halves: the window's inserts and
		// deletes are what the arithmetic closes on, and the row it moved is
		// neither (arithmetic.go works the cases).
		switch ev.EventType {
		case event.EventInsert:
			res.Inserts++
		case event.EventUpdate:
			res.Updates++
		case event.EventDelete:
			res.Deletes++
		}
		if halves == nil {
			res.fold(ev, opts.touched)
			continue
		}
		for h := range halves {
			res.fold(&halves[h], opts.touched)
		}
	}
	return nil
}

// fold files ev under its key unless the map already holds a later change of
// that key, and reports the key to touched either way.
func (r *foldResult) fold(ev *query.ResultRow, touched func(string)) {
	if touched != nil {
		touched(ev.PKValues)
	}
	if cur, ok := r.Changes[ev.PKValues]; ok && query.LaterInBinlog(cur, ev) {
		return
	}
	r.Changes[ev.PKValues] = retainEvent(ev)
}

// foldOptions is what a fold's caller decides for a whole table, not per page.
type foldOptions struct {
	// pg folds a PostgreSQL window (foldConfig.PG): a PK-changing UPDATE
	// keeps refusing there, see splitPKChange.
	pg bool
	// touched, when set, is called with every key a page files or declines
	// to file (the fold kept a later change): what a tail marks changed
	// (#2495). Both keys of a PK change are reported, which is why a tail
	// cannot take them from the page's own pk_values.
	touched func(key string)
}

// splitPKChange turns an UPDATE that changed the row's primary key (#782)
// into the two events it is: a DELETE of the old key and an INSERT of the new
// one, both at the UPDATE's own (file, pos, event_id, timestamp), so the
// per-key "last in the binary log wins" rule orders each half against every
// other change of that key in the window. It returns nil for any other event,
// and the #782 refusal where the split is not safe (#2501):
//
//   - pg: a PostgreSQL window. Its rows carry an LSN, not a binary log
//     coordinate, so LaterInBinlog cannot order the halves against other
//     changes of the same key, and under REPLICA IDENTITY DEFAULT the
//     before-image may hold only the key.
//   - the spelling proof fails (pkSpellingErr, its own text): the decoded
//     before-image, spelled by event.BuildPKValues, must equal the key the
//     parser STORED for this event (pk_values is built from the same values
//     that became row_before, parser.go emitUpdates). That equality proves
//     the storage and decode round-trip preserves the spelling for this
//     event's key columns, and the after-image went through the same path,
//     so the INSERT's key — spelled from the decoded after-image — is what a
//     later real event on that key carries. Without it the two could land
//     under two spellings of one key (a BINARY column left as stored base64
//     because no schema epoch covered the event, a MariaDB UUID/INET6 key
//     the decoder renders as text, an image decoded as float64 past 2^53)
//     and the #782 duplicate or resurrection would come back silently.
//   - the new key's values are not spellable from the decoded image
//     (pkKeyUnspellable): the proof above is value-independent only where
//     formatPKValue spells a decoded value as it spelled the parser's. A
//     TEXT-family key decodes to a Go STRING, on which the hex rule for bytes
//     that are not UTF-8 never applies, so an old key of plain ASCII proves
//     nothing about a new key holding a latin1 byte (the parser stores the
//     later DELETE of that key as 0x…); and TEXT bytes that look like a JSON
//     container are promoted to a JSON value by the indexer and decode as a
//     slice or a map, which print as `[1 2]`, not `[1,2]`. So every value of
//     the new key must be a scalar and, when a string, valid UTF-8.
//
// The halves share nothing the writer mutates: the DELETE has no RowAfter.
// Both carry no ChangedColumns, the INSERT/DELETE convention (and the fold's
// byte estimate would count the UPDATE's list twice otherwise).
func splitPKChange(ev *query.ResultRow, schema, table string, pkCols []metadata.ColumnMeta, pg bool) ([]query.ResultRow, error) {
	before, after, changed := pkChangedInEvent(ev, pkCols)
	if !changed {
		return nil, nil
	}
	if pg {
		return nil, pkChangingUpdateErr(schema, table, before, after)
	}
	if before != ev.PKValues {
		return nil, pkSpellingErr(schema, table, ev.PKValues, after,
			fmt.Sprintf("the index stores the old key as %q but its row image decodes as %q", ev.PKValues, before))
	}
	if col, why := pkKeyUnspellable(pkCols, ev.RowAfter); why != "" {
		return nil, pkSpellingErr(schema, table, ev.PKValues, after,
			fmt.Sprintf("the new key's column %s decodes to %s, which the index would spell another way", col, why))
	}
	del, ins := *ev, *ev
	del.EventType, del.PKValues, del.RowAfter, del.ChangedColumns = event.EventDelete, ev.PKValues, nil, nil
	ins.EventType, ins.PKValues, ins.RowBefore, ins.ChangedColumns = event.EventInsert, after, nil, nil
	return []query.ResultRow{del, ins}, nil
}

// pkKeyUnspellable names the first key column of row whose decoded value
// formatPKValue would spell differently from the parser's bytes (see
// splitPKChange), with why, or "" when every value is a scalar and every
// string is valid UTF-8.
func pkKeyUnspellable(pkCols []metadata.ColumnMeta, row map[string]any) (col, why string) {
	for _, c := range pkCols {
		switch v := row[c.Name].(type) {
		case []any, map[string]any:
			return c.Name, "a JSON value"
		case string:
			if !utf8.ValidString(v) {
				return c.Name, "text that is not valid UTF-8"
			}
		}
	}
	return "", ""
}

// pkSpellingErr is the #2501 refusal of a split the index's own spelling does
// not prove safe (splitPKChange). It names both keys as the fold sees them,
// the reason, and the remedy: a snapshot at or after the change, from which
// the refresh starts without this window.
func pkSpellingErr(schema, table, before, after, why string) error {
	return fmt.Errorf(
		"full-table reconstruct: %s.%s has a PK-changing UPDATE in the window (primary key %q → %q) "+
			"that the refresh cannot follow to the row's new key: %s. A BINARY key whose rows no schema "+
			"snapshot covers, or a MariaDB UUID/INET6 key, spells this way; take a new snapshot with "+
			"`bintrail baseline` at or after the change (a full backup from the web interface does the same) and refresh from it",
		schema, table, before, after, why)
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
	// SysVersioned is set for a MariaDB system-versioned table (#2007); PKCols
	// is then the declared key, without the ROW END column. See foldPage.
	SysVersioned *sysVersioned

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
	// MaxChangeBytes is FullTableConfig.MaxChangeBytes, divided by Parallelism
	// at the check.
	MaxChangeBytes int64
	// SpillOverBudget moves the changes to disk past MaxTouchedRows instead of
	// refusing (#1107), and past MaxChangeBytes (#2207), which never refuses. Only the merge over a baseline reads a spill; the
	// binlog-only fallback keeps the refusal.
	SpillOverBudget bool
	// PG folds a PostgreSQL window (#1596): its values are PostgreSQL's own
	// text as captured (bytea as hex), with no ENUM ordinals and no base64
	// to decode, so neither decoding pass runs.
	PG bool

	// Heads is the run's query.PartitionHeads, passed to the fetch so every
	// table reads the same picture of the index; nil has the fetch load one.
	Heads *query.PartitionHeads

	// RemediationHint is the advice attached to that warning; empty uses the
	// attended-CLI wording. See FullTableConfig.RemediationHint.
	RemediationHint string
}

// foldResult is the completed build side of the merge.
type foldResult struct {
	// Changes maps pk_values → the LAST event for that PK in the window,
	// trimmed by retainEvent. Last is by binary log position
	// (query.LaterInBinlog), not by arrival: pages arrive in ascending
	// (event_timestamp, event_id) order, and for one row that order can
	// disagree with the binary log (#2151, see foldPage). A PK touched in
	// page 1 and again in page 3 holds whichever of the two the binary log
	// holds later. Once the fold spills, this map only holds the pages since
	// the last drain and the same choice is made again when a group is read
	// back (changeSpill.load).
	Changes map[string]*query.ResultRow

	// Total is the number of events folded across every page (the
	// archive-inclusive, deduplicated count — the same number the old
	// len(events) reported).
	Total int64
	// Inserts, Updates and Deletes split Total by event type, every event
	// counted once (an event dropped for an earlier binary log position
	// still counts: the log holds it). A system-versioned table's events are
	// counted as normalized, in their meaning for the current rows. They are
	// the refresh's arithmetic (#2505): the window leaves Inserts - Deletes
	// more rows than it found.
	Inserts, Updates, Deletes int64

	// First is a copy of the very first event of the window, or nil when the
	// window was empty. Kept because the baseline-vs-first-event gap warning
	// (#781) needs it and the page it came from is long gone by then.
	First *query.ResultRow

	// LastEventID is the highest event_id folded, 0 for an empty window. It
	// is stamped into the files this fold writes (baseline.MetaKeyLastEventID)
	// so the next fold can floor its fetch on it (#1720).
	LastEventID uint64

	// FetchDuration and FoldDuration split the window's wall time between
	// waiting on the fetch (MySQL and archives) and folding pages in Go, so a
	// slow table can be attributed without external sampling (#1714, #1720).
	// The fold side includes the page decoders, which read schema_snapshots
	// the first time a page brings a new schema epoch: a slow snapshot read
	// lands in fold time, not fetch time.
	FetchDuration, FoldDuration time.Duration

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

	// Spill holds the changes on disk once they passed the fold's in-memory
	// limit (#1107), and Changes is then empty. Set only under
	// foldConfig.SpillOverBudget; the caller removes it with close.
	Spill *changeSpill
}

// Row images are estimated at what they take on the heap, not on disk: the
// constants are calibrated against the runtime's own count by
// TestApproxChangeBytes_tracksTheHeap, for the shapes a decoded image takes.
const (
	// changeRowBytes is a retained event without its image: the ResultRow
	// and its own small strings.
	changeRowBytes = 330
	// changeMapBytes is an image map's fixed cost: its header and its first
	// group of slots, paid by a map of one entry as much as of eight.
	changeMapBytes = 300
	// changeFieldBytes is one image entry besides its key's and its value's
	// bytes: the map slot and the boxed value.
	changeFieldBytes = 64
	// changeSampleRows is how many entries sampledChangeBytes reads.
	changeSampleRows = 64
)

// approxChangeBytes estimates the heap one retained change takes.
func approxChangeBytes(ev *query.ResultRow) int64 {
	n := int64(changeRowBytes) + int64(len(ev.PKValues)+len(ev.BinlogFile)+len(ev.SchemaName)+len(ev.TableName))
	if ev.GTID != nil {
		n += int64(len(*ev.GTID))
	}
	for _, c := range ev.ChangedColumns {
		n += 16 + int64(len(c))
	}
	return n + approxImageBytes(ev.RowAfter)
}

func approxImageBytes(m map[string]any) int64 {
	if m == nil {
		return 0
	}
	n := int64(changeMapBytes)
	for k, v := range m {
		n += changeFieldBytes + int64(len(k)) + approxValueBytes(v)
	}
	return n
}

func approxValueBytes(v any) int64 {
	switch x := v.(type) {
	case string:
		return int64(len(x))
	case json.Number:
		return int64(len(x))
	case []byte:
		return 8 + int64(len(x))
	case time.Time:
		return 24
	case map[string]any:
		return approxImageBytes(x)
	case []any:
		n := 24 + 16*int64(len(x))
		for _, e := range x {
			n += approxValueBytes(e)
		}
		return n
	default:
		return 8
	}
}

// sampledChangeBytes estimates the heap a change map takes from up to
// changeSampleRows of its entries: the total, and the average a row.
func sampledChangeBytes(m map[string]*query.ResultRow) (total, perRow int64) {
	if len(m) == 0 {
		return 0, 0
	}
	var sum, seen int64
	for _, ev := range m {
		sum += approxChangeBytes(ev)
		if seen++; seen == changeSampleRows {
			break
		}
	}
	perRow = max(1, sum/seen)
	return perRow * int64(len(m)), perRow
}

// admitPage is admit with no size budget.
func (r *foldResult) admitPage(limit int64, tables int, spill bool) error {
	return r.admit(limit, 0, tables, spill)
}

// admit runs after each page is folded into r.Changes. Up to limit
// distinct changed rows (0 = no limit) the changes stay in memory. Past it,
// with spill allowed, every change moves to disk and the map is replaced,
// and every later page follows it there; without spill it is the refusal.
//
// The map is replaced, not cleared: a cleared Go map keeps the memory it grew
// to, which is the memory this exists to give back.
//
// maxBytes (0 = none) is the same move by size (#2207): rows are a proxy for
// memory, and a wide table's rows cost several times a narrow one's. Past it
// the changes go to disk and the merge's passes are sized to it; it never
// refuses, so a fold that may not spill ignores it and the refusal stays on
// rows.
func (r *foldResult) admit(limit, maxBytes int64, tables int, spill bool) error {
	if r.Spill == nil {
		overRows := limit > 0 && int64(len(r.Changes)) > limit
		var total int64
		if spill && maxBytes > 0 {
			total, _ = sampledChangeBytes(r.Changes)
		}
		if !overRows && (maxBytes <= 0 || total <= maxBytes) {
			return nil
		}
		if !spill {
			return TouchedRowBudgetError(limit, tables, false)
		}
		s, err := newChangeSpill(limit)
		if err != nil {
			return err
		}
		s.tables = tables
		s.maxBytes = maxBytes
		r.Spill = s
	}
	if err := r.Spill.drain(r.Changes); err != nil {
		return err
	}
	r.Changes = make(map[string]*query.ResultRow)
	return nil
}

// changeCount is how many changes the fold kept: the map's size, or what a
// spilled fold wrote to disk (at least one per distinct changed row). Zero
// means the window changed nothing.
func (r *foldResult) changeCount() int {
	if r.Spill != nil {
		return int(r.Spill.records)
	}
	return len(r.Changes)
}

// close deletes a spilled fold's changes from disk. Safe on any fold result.
func (r *foldResult) close() {
	if r.Spill == nil {
		return
	}
	if err := r.Spill.remove(); err != nil {
		slog.Warn("reconstruct: could not remove changed rows written to disk", "dir", r.Spill.dir, "error", err)
	}
	r.Spill = nil
}

// foldWindow is foldEventWindow behind a variable so a test can count the
// index fetches one fold runs (#1689): a table whose positional window is
// empty by construction must not query the index at all, and "the fold
// returned no events" is true with or without that skip, so nothing else
// discriminates. Same seam, and the same reason, as resolveSnapshotCut
// (#1635).
var foldWindow = foldEventWindow

// windowEmptyByPosition reports whether the positional window a fold would
// fetch can hold no event at all, so querying the index for it is pure cost.
//
// The window admits an event whose start_pos is at-or-after since and whose
// end_pos is at-or-before until. On a MySQL/MariaDB source every event ends
// strictly after it starts, so when until is at-or-before since there is no
// room between the two bounds: no row can satisfy both predicates, whatever the
// index holds, and the time filters are conjunctive so they can only narrow the
// window further.
//
// That strictness is a SOURCE property, not a schema one, and it is held up by
// two belts elsewhere rather than by the column types. pgcapture writes
// StartPos == EndPos == the commit LSN for every row of a transaction
// (internal/pgcapture/decoder.go), so a PG-sourced index holds rows a
// since==until window would ADMIT; what keeps this sound is that a PostgreSQL
// table takes its own path before this is consulted (#1596,
// reconstructPGTable), which uses pgFoldBounds.empty instead. A MariaDB 11.4+
// row with a zero LogPos would underflow into a ~2^64 start_pos, which is why
// the parser refuses to index one (#1117) — a belt that is prospective, so rows
// an older build already wrote are not covered by it. Do not move this call
// ahead of the PG branch, and do not reuse it where neither belt runs.
//
// This is not a rare corner. A refresh anchors on the previous backup's
// coordinate and cuts at the newest indexed event, so any refresh that runs
// while capture is behind that coordinate has an empty window for every table
// anchored on that coordinate, and before #1689 each one walked the index to
// apply nothing. (Tables carried forward keep an older snapshot's footer, so
// one snapshot directory can hold several anchors.)
//
// Both bounds have to be known: mydumper output never sets a cut, and a
// baseline that recorded no coordinate has no anchor. Neither can be shown
// empty this way, so neither is skipped.
func windowEmptyByPosition(since, until *query.BinlogPos) bool {
	return since != nil && until != nil && until.AtOrBefore(*since)
}

// foldFetchOptions is the merged-fetch configuration a fold runs under, built
// in ONE place because two paths need it to be IDENTICAL: the fetch, and the
// coverage check that runs alone when the fetch is skipped (#1689). A second
// literal would let the check drift to a different window, a different set of
// archive sources or a different strictness than the fetch it stands in for —
// and a coverage check over the wrong window is worse than none, because it
// reads as proof.
//
// The builder equalises OPTIONS, not validation: FetchMergedStream also
// rejects a preset cursor, a DESC order and a row cap, and the coverage check
// has no counterpart for those. Unreachable today (a fold sets none of them),
// but a future cap on the fold's options would refuse loudly on one path and
// pass quietly on the other.
//
// NoArchive is false unconditionally, and it means something different to each
// consumer. For the fetch it lets the stream decide whether to query archives,
// which already handles the empty-archive case in its fast path (the pre-#1097
// `len(archSources)==0` gate was wrong: it disabled archive routing even when
// the fetch could have resolved sources itself). For the coverage check, which
// opens nothing, it is what tells the planner to count archived hours as
// COVERED instead of as gaps — buildPlan drops archivedHours under noArchive —
// so flipping it would refuse runs whose archives are perfectly readable.
func foldFetchOptions(fc foldConfig) query.FetchMergedOptions {
	return query.FetchMergedOptions{
		Opts:           fc.Opts,
		DBName:         fc.DBName,
		NoArchive:      false,
		AllowGaps:      fc.AllowGaps,
		ArchiveFetcher: fc.ArchiveFetcher,
		PartitionHeads: fc.Heads,
	}
}

// annotateFoldFetchError puts the operator's next step onto an error raised
// while gathering the window's events. Shared by the fetch and by the coverage
// check that replaces it when the window is provably empty (#1689): both raise
// the SAME *query.GapError, and wrapping only one of them would make the
// guidance an operator receives depend on whether capture happened to be behind
// that minute. baseline refresh puts this string in its per-table summary, so
// the difference is not only a log line.
//
// A coverage gap in a window anchored at the baseline usually means the
// baseline went STALE — its anchor slid past rotation retention. Name the fix
// (#1193): no flag recovers hours that were rotated out unarchived.
func annotateFoldFetchError(err error, fc foldConfig) error {
	var gapErr *query.GapError
	if errors.As(err, &gapErr) && fc.Opts.Since != nil {
		return fmt.Errorf("fetch events: %w — the delta window starts at this table's newest usable baseline anchor (%s); if archive_state drifted, `bintrail archive reconcile --repair` is the cheap fix, and if the missing hours were rotated out unarchived, take a fresh baseline (bintrail dump + bintrail baseline)",
			err, fc.Opts.Since.UTC().Format(time.RFC3339))
	}
	return fmt.Errorf("fetch events: %w", err)
}

// verifyFoldCoverage runs the half of a fold that happens BEFORE any event is
// read: archive discovery, the coverage planner and gap enforcement. A caller
// that skips the fetch calls this instead, because the fetch is the only thing
// on the full-table path that checks coverage at all.
func verifyFoldCoverage(ctx context.Context, fc foldConfig) error {
	return query.VerifyMergedCoverage(ctx, fc.DB, foldFetchOptions(fc))
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
//  2. The #592 unresolved-TOAST guard and the #782 PK-changing-UPDATE
//     handling, per event, on the untrimmed event: the former refuses, the
//     latter splits the UPDATE into a DELETE of the old key and an INSERT of
//     the new one (#2501, splitPKChange), refusing only on a PostgreSQL
//     window or an unproven key spelling.
//  3. The fold, via retainEvent.
//
// Running these per event over the raw stream is strictly stronger than the
// map-level checks it replaces: the map only ever held the surviving last
// event per PK, so a PK-changing UPDATE whose old key a later event reused was
// invisible to it. That is why fulltable.go's baseline path no longer calls
// pkChangingUpdate/checkChangesToast on the map at all — a nil-before-image map
// would make those calls unconditionally pass while still reading as guards.
func foldEventWindow(ctx context.Context, fc foldConfig) (*foldResult, error) {
	res := &foldResult{Changes: make(map[string]*query.ResultRow)}
	// A spill is deleted on every way out but success, a panic included: the
	// per-table recover in ReconstructTables keeps the daemon running, and a
	// spill left behind can hold gigabytes of temp disk.
	done := false
	defer func() {
		if !done {
			res.close()
		}
	}()
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

	walkStart := time.Now()
	_, err := query.FetchMergedStream(ctx, fc.DB, fc.Engine, foldFetchOptions(fc), fc.BatchSize, func(page []query.ResultRow) error {
		if len(page) == 0 {
			return nil
		}
		foldStart := time.Now()
		defer func() { res.FoldDuration += time.Since(foldStart) }()
		if !fc.PG {
			dec.mapEnums(page)
			dec.decodeBinaries(page)
		}

		if res.First == nil {
			first := page[0]
			res.First = &first
		}
		for i := range page {
			if page[i].EventID > res.LastEventID {
				res.LastEventID = page[i].EventID
			}
		}

		if err := foldPage(page, fc.Schema, fc.Table, fc.PKCols, fc.SysVersioned, foldOptions{pg: fc.PG}, res); err != nil {
			foldErr = err
			return err
		}
		// Per page, not after the window: the memory is spent as the map
		// grows, so a check at the end would come after the harm (#1107).
		limit := scaledEventThreshold(fc.MaxTouchedRows, fc.Parallelism)
		maxBytes := scaledEventThreshold(fc.MaxChangeBytes, fc.Parallelism)
		spilling := res.Spill != nil
		if err := res.admit(limit, maxBytes, fc.Parallelism, fc.SpillOverBudget); err != nil {
			// No table prefix: the run's error adds "schema.table: ".
			// No remedy here: it differs per surface (a restore needs a closer
			// moment, a scheduled update a full backup), and the surfaces add
			// their own.
			foldErr = err
			return foldErr
		}
		if !spilling && res.Spill != nil {
			slog.Info("reconstruct: more changes than this fold holds in memory; writing them to disk and merging in passes",
				"schema", fc.Schema, "table", fc.Table, "rows", res.Spill.records, "limit", limit,
				"est_bytes", res.Spill.heldBytes, "byte_limit", maxBytes, "dir", res.Spill.dir)
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
	res.FetchDuration = time.Since(walkStart) - res.FoldDuration
	if err != nil {
		// Only a TRANSPORT failure gets the "fetch events" label. fn's errors
		// are the #592/#782 refusals, which FetchMergedStream propagates
		// unchanged — prefixing those sends the operator to check DB/S3
		// connectivity instead of reading the actionable message underneath.
		if foldErr != nil {
			return nil, foldErr
		}
		return nil, annotateFoldFetchError(err, fc)
	}

	if res.Spill != nil {
		if err := res.Spill.finish(); err != nil {
			return nil, err
		}
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
		"events", res.Total, "changes", res.changeCount(), "on_disk", res.Spill != nil)
	done = true
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

// TouchedRowBudgetError is the changed-rows refusal, exported so the
// console's page tests render the exact text the daemon produces.
//
// Without spilled, limit is what one table may hold in memory and the fold
// refused past it. With spilled, the changes went to disk and one of the
// spillBuckets groups alone passed limit when read back, which takes about
// spillBuckets times limit changed rows (#1107).
func TouchedRowBudgetError(limit int64, tables int, spilled bool) error {
	during := ""
	if tables > 1 {
		during = fmt.Sprintf(" while %d tables are processed at once", tables)
	}
	if spilled {
		return fmt.Errorf("%w: about %s or more distinct rows changed between the backup this starts from and the target moment, "+
			"more than this run can rebuild from the recorded changes holding at most %s of them in memory at a time%s",
			ErrTouchedRowBudget, groupThousands(limit*spillBuckets), groupThousands(limit), during)
	}
	if tables <= 1 {
		return fmt.Errorf("%w: more than %s distinct rows changed between the backup this starts from and the target moment, "+
			"the most this run may hold in memory", ErrTouchedRowBudget, groupThousands(limit))
	}
	return fmt.Errorf("%w: more than %s distinct rows changed between the backup this starts from and the target moment, "+
		"the most one table may hold in memory%s",
		ErrTouchedRowBudget, groupThousands(limit), during)
}
