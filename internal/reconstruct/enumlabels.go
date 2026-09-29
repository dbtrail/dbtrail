package reconstruct

import (
	"database/sql"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// MapEventEnumLabels rewrites ENUM/SET ordinals in the events' row
// images back to their string labels in place (#472/#476), decoding
// each event with the snapshot in effect at its timestamp (#475).
// Reconstruction surfaces (console Time-travel, `bintrail reconstruct`,
// full-table merge) call this on fetched deltas BEFORE folding them
// onto a baseline, so the merged state carries labels exactly like the
// baseline rows do (mydumper dumps labels as strings; strings pass
// through the mapper untouched).
//
// The raw event-record surfaces — `bintrail query`, the MCP query tool,
// and the console events view — deliberately stay unmapped: the stored
// ordinal is the forensic ground truth, recoverable regardless of later
// enum reshapes.
//
// latest is the caller's already-loaded resolver, used as the fallback
// definition when the epoch lookup is unavailable (nil is fine: the
// remaining degradation is pass-through — raw ordinals, never a guessed
// label).
func MapEventEnumLabels(db *sql.DB, latest *metadata.Resolver, schema, table string, events []query.ResultRow) {
	newEventDecoder(db, schema, table, latest).mapEnums(events)
}

// DecodeEventBinaries reverses the storage-side base64 of BLOB/TEXT columns in
// the events' row images in place — the sibling of MapEventEnumLabels for the
// other class of values go-mysql delivers as []byte (#666, same root as recover
// #662 / shim #661). go-mysql delivers BLOB/TEXT as []byte, which marshalRow
// base64-encodes into the stored event JSON; the single-row reconstruction
// surfaces (`bintrail reconstruct`, console Time-travel) fold these events onto
// the baseline via ApplyAt/BuildHistory, which do NOT decode, so without this
// pass the client gets the base64 text instead of the real value.
//
// Call this on fetched deltas BEFORE folding them onto a baseline: event images
// only, never a baseline row (baseline rows are scanned raw from Parquet and
// must not be decoded — a baseline TEXT value that happens to be valid base64
// would be corrupted). The full-table merge path (ReconstructTable, #668) calls
// this too, on the full events slice before its change map is built — callers
// must call it exactly once per events slice, since decodeStoredBase64 is not
// idempotent and a second pass would double-decode — this now also covers
// the #736 bool/json.Number repair output ("true"/"false" is valid base64
// alphabet and would silently re-decode into unrelated bytes on a second pass).
//
// Each column is typed at the snapshot in effect at its event's timestamp
// (#475-style epoch awareness, matching MapEventEnumLabels): whether a value is
// base64-stored depends on whether the column was delivered as []byte (BLOB/TEXT)
// or string (VARCHAR/CHAR) when the event was captured, so a VARCHAR→TEXT
// widening must not make a wrong-epoch lookup wrongly decode an old plain-string
// value that happens to be valid base64.
//
// Unlike MapEventEnumLabels there is deliberately NO latest-snapshot fallback:
// relabeling an ENUM by the latest definition is harmless (strings pass through),
// but base64-decoding a value the wrong schema calls BLOB/TEXT is DESTRUCTIVE (a
// plain VARCHAR value that happens to be valid base64 would be corrupted to
// garbage bytes). So when the epoch
// typing is unavailable — no snapshots, or a per-epoch resolver that fails to
// load — the value is left as the base64 it was stored as, never decoded by a
// guess.
//
// The returned bool reports whether that degradation happened: true when every
// event's timestamp resolved to a snapshot epoch whose resolver and table
// lookup succeeded (so every base64-stored value was decoded), false when at
// least one event's typing was unavailable and its values may still be stored
// base64. Callers that only best-effort decode (recover, shim, reconstruct)
// ignore it; verify uses it to keep a digest comparison honest — an undecoded
// value is a representation gap, not a divergence (#769/#791).
func DecodeEventBinaries(db *sql.DB, schema, table string, events []query.ResultRow) bool {
	if len(events) == 0 {
		return true
	}
	if db == nil {
		return false
	}
	d := newEventDecoder(db, schema, table, nil)
	d.decodeBinaries(events)
	return d.typed
}

// eventDecoder holds the epoch-aware decoding state for ONE table: the snapshot
// epoch list and the per-epoch resolver/column memos both decoding passes need.
//
// It exists because the full-table window is fetched in pages (#1097). Both
// passes used to build this state from scratch on each call, which was free
// when a call meant "the whole window" and is not when it means "one page":
// each rebuild costs a schema_snapshots query plus a resolver load per epoch
// touched, so a 400-page window would pay it 400 times. Constructed once per
// table by foldEventWindow and reused across pages; the one-shot exported
// functions above wrap a throwaway instance, preserving their contract for
// callers that fetch in a single call (verify, single-row reconstruct, the
// console).
//
// NOT SAFE FOR CONCURRENT USE — one decoder per table run. This matters because
// the surrounding machinery is concurrent: ReconstructTables runs up to
// --parallelism table goroutines. It is safe today because each goroutine's
// foldEventWindow constructs its own decoder and never shares it, and the page
// callback runs sequentially on that goroutine.
type eventDecoder struct {
	db      *sql.DB
	schema  string
	table   string
	epochs  []metadata.SnapshotEpoch
	enum    metadata.EnumMapperSource
	binMemo map[int]binMemo
	// typed reports whether every event seen so far could be typed against a
	// real snapshot. It only ever goes false — a single untypable epoch means
	// the run cannot claim its BLOB/TEXT values were decoded. Accumulates
	// across pages, which is what makes it correct for a paged caller.
	typed bool
}

// binMemo caches one epoch's BLOB/TEXT column set. cols may be nil with ok
// true: that is "this epoch resolved fine and the table has no BLOB/TEXT
// columns", which is different from "the epoch could not be resolved".
// fixed holds the epoch's MariaDB UUID/INET4/INET6 columns (name to data
// type), rendered as text rather than decoded to bytes.
type binMemo struct {
	cols  map[string]bool
	fixed map[string]string
	ok    bool
}

func newEventDecoder(db *sql.DB, schema, table string, latest *metadata.Resolver) *eventDecoder {
	d := &eventDecoder{db: db, schema: schema, table: table, binMemo: map[int]binMemo{}, typed: true}
	if db == nil {
		d.typed = false
		return d
	}
	epochs, err := metadata.LoadSnapshotEpochs(db)
	if err != nil {
		slog.Debug("snapshot epoch lookup failed; decoding ENUM/SET with the latest snapshot and leaving BLOB/TEXT as stored base64",
			"schema", schema, "table", table, "err", err)
		epochs = nil
	}
	d.epochs = epochs
	d.enum = metadata.EnumMapperSource{
		Epochs:      epochs,
		ResolverFor: func(id int) (*metadata.Resolver, error) { return metadata.NewResolver(db, id) },
		Fallback:    latest,
	}
	return d
}

// mapEnums rewrites ENUM/SET ordinals to labels in place, per event, using the
// snapshot in effect at that event's own timestamp.
func (d *eventDecoder) mapEnums(events []query.ResultRow) {
	if len(events) == 0 || d.db == nil {
		return
	}
	for i := range events {
		m := d.enum.MapperAt(d.schema, d.table, events[i].EventTimestamp)
		m.MapImage(events[i].RowBefore)
		m.MapImage(events[i].RowAfter)
	}
}

// decodeBinaries reverses the storage-side base64 of BLOB/TEXT columns in
// place. Not idempotent — call it exactly once per event.
func (d *eventDecoder) decodeBinaries(events []query.ResultRow) {
	if len(events) == 0 || d.db == nil {
		return
	}
	unrendered := 0
	for i := range events {
		m := d.binColsAt(events[i].EventTimestamp)
		decodeImageBinaries(events[i].RowBefore, m.cols)
		decodeImageBinaries(events[i].RowAfter, m.cols)
		unrendered += renderImageMariaDBFixed(events[i].RowBefore, m.fixed)
		unrendered += renderImageMariaDBFixed(events[i].RowAfter, m.fixed)
	}
	if unrendered > 0 {
		slog.Warn("MariaDB UUID/INET values in the index have no correct text form (captured before this build could store them, or their schema snapshot could not be read); they are left as stored",
			"schema", d.schema, "table", d.table, "values", unrendered)
	}
}

// binColsAt returns the BLOB/TEXT column set for the snapshot epoch covering t.
// The memo check sits before NewResolver so a snapshot whose resolver fails to
// load is probed at most once, not once per row — and, since #1097, at most
// once for the whole window rather than once per page.
func (d *eventDecoder) binColsAt(t time.Time) binMemo {
	id, ok := metadata.EpochAt(d.epochs, t)
	if !ok {
		d.typed = false
		return binMemo{} // no snapshots → no safe typing → leave values as base64
	}
	if m, seen := d.binMemo[id]; seen {
		if !m.ok {
			d.typed = false
		}
		return m
	}
	var m binMemo
	if r, nerr := metadata.NewResolver(d.db, id); nerr == nil && r != nil {
		if tm, rerr := r.Resolve(d.schema, d.table); rerr == nil {
			m = binMemo{cols: binaryColsFromTableMeta(tm), fixed: mariaDBFixedColsFromTableMeta(tm), ok: true}
		}
	}
	d.binMemo[id] = m
	if !m.ok {
		d.typed = false
	}
	return m
}

// renderImageMariaDBFixed turns each MariaDB UUID/INET4/INET6 value of one
// event image from its stored base64 into the server's text form, in place
// (metadata.RenderStoredMariaDBFixed). A value that is not the stored form of
// a full-width value, such as one captured before #1944, is left as it is,
// and counted in the return value so the caller can say so. No-op when fixed
// is empty or image is nil.
func renderImageMariaDBFixed(image map[string]any, fixed map[string]string) int {
	if len(fixed) == 0 || image == nil {
		return 0
	}
	unrendered := 0
	for col, dt := range fixed {
		v, ok := image[col]
		if !ok || v == nil {
			continue
		}
		out, rendered := metadata.RenderStoredMariaDBFixed(dt, v)
		image[col] = out
		if !rendered {
			unrendered++
		}
	}
	return unrendered
}

// decodeImageBinaries decodes the storage-side base64 of every BLOB/TEXT column
// in one event image, in place. No-op when binCols is empty or image is nil.
func decodeImageBinaries(image map[string]any, binCols map[string]bool) {
	if len(binCols) == 0 || image == nil {
		return
	}
	for col, binary := range binCols {
		if v, ok := image[col]; ok {
			image[col] = decodeStoredBase64(v, binary)
		}
	}
}
