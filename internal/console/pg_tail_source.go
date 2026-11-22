package console

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/views"
)

// pgTailSource is the tailSource of a PostgreSQL server's tails (#2756):
// what indexTailSource is for a MySQL server, over the same index tables,
// with every rule read from what a PostgreSQL capture and a PostgreSQL
// snapshot record. A type of its own: each step of indexTailSource reads
// something they do not have (a binlog coordinate in the footer, a CREATE
// TABLE, an event mark, one schema snapshot for all tables).
//
// Positions. The keeper orders positions as binary log coordinates, so
// everything this source hands it is spelled by reconstruct.PGTailPos and
// reconstruct.PGTailEvent: one log name, the LSN as the offset. What it asks
// the index is asked in the index's own spelling (reconstruct.PGDeltaFloor):
// under the keeper's, the engine would read a binary log window.
//
// Where it stops. A PostgreSQL capture makes the rows of a transaction
// visible before it saves its position, and a large transaction in parts, so
// the newest rows of the index can be half a transaction. And after a
// restart PostgreSQL sends again what follows the saved position and the
// transaction at it, under new and higher ids: while that second delivery
// arrives, its rows carry the saved position's own LSN. So a tail is fed
// only up to the event the capture recorded for its saved position when a
// transaction last ended (Newest), and of the rows up to that event, only
// the ones whose LSN is at or before that event's: a row of an earlier
// delivery whose transaction is past the position comes again, whole.
//
// A transaction delivered twice reaches a read in id order with its LSN
// going back (a, b, then a and b again). The keeper takes a position that
// goes back for a source whose log started over and builds every tail
// again, so Since hands over no row below the LSN it has already handed:
// such a row is a second delivery of a transaction handed whole before it.
//
// What it does not see: the publication. A table with no shape in the index
// (never written since capture began, or never published) has no tail. A
// table that was captured and then left the publication is told only by the
// change of publication the capture inferred (CheckDestructiveDDL).
type pgTailSource struct {
	s        *Server
	b        *bundle
	pageSize int
	// captures, capturesUnknown, capturesKey: see indexTailSource.
	captures        func(schema, table string) bool
	capturesUnknown bool
	capturesKey     string

	// mu guards the three memos below.
	mu sync.Mutex
	// schema is the newest shape of every table, read again when a newer
	// schema snapshot appears (schemaID). A PostgreSQL stream writes one
	// snapshot per table, so the newest snapshot alone describes one table.
	schema   *metadata.Resolver
	schemaID int64
	// shapes are the shapes the stream recorded, read again on the same
	// signal (shapesID), from shapesFrom.
	shapes        []metadata.PGShape
	shapesID      int64
	shapesFromLSN uint64
	// sentThrough is the highest position Newest has read below which every
	// commit of the source is in the index (anchorReached).
	sentThrough uint64
	// broken remembers each table's answer to Broken for as long as nothing
	// it is made from changed (pgBrokenKey).
	broken map[string]pgBrokenMemo
	// unordered are the tables an event with no LSN was read for: Since
	// leaves such a row out and Broken ends the table's tail, so one bad row
	// costs one table its tail and not the server every tail.
	unordered map[string]string
	// skipped remembers a read by Since that handed nothing: from the event
	// from, everything up to the event to was read and left out, so the
	// next read from the same event starts after to. The keeper's own
	// position moves only with a row handed to it, and a long run of rows
	// left out (what a crash in the middle of a large transaction leaves)
	// would otherwise be read again at every refresh.
	skipped struct{ from, to uint64 }
	// started is, by tailKey, the tail Start built last, until its Fill
	// begins (pg_tail_copy.go).
	started map[string]pgStarted
	// copyBytes is how much text one read of a table's copy may bring into a
	// tail being built: the tails' memory budget, 0 for no bound. Set before
	// the source is used, never after.
	copyBytes int64
}

// pgBrokenMemo is one table's last answer to Broken and what it was made
// from.
type pgBrokenMemo struct {
	key    pgBrokenKey
	reason string
}

// pgBrokenKey is what a table's answer to Broken depends on: its file's
// floor, the newest schema snapshot and schema change the index holds, and
// the loss and the dropped changes capture recorded.
type pgBrokenKey struct {
	base        string // the file itself (baseFileID): two files can share a floor
	gapColumns  bool   // the record of a loss is readable at all
	floor       uint64
	snapshotID  int64
	schemaChgID int64
	schemaChgs  int64 // how many: a row removed below the newest moves no id
	lostAt      time.Time
	lostKnown   bool
	stateKnown  bool
	// skips is the capture's ledger of dropped changes as stored: a new drop
	// changes it, so an answer made before the drop is asked again.
	skips string
}

func newPGTailSource(s *Server, b *bundle) *pgTailSource {
	return &pgTailSource{s: s, b: b, pageSize: tailPageSize, capturesUnknown: true}
}

// capturing says which tables capture records (nil: every table).
func (x *pgTailSource) capturing(captures func(schema, table string) bool) *pgTailSource {
	x.captures, x.capturesUnknown = captures, false
	return x
}

// Newest is the event the capture recorded for its saved position: ID 0
// when the index records none (PGCheckpoint), or no longer holds that event
// as a PostgreSQL event at or before the position.
func (x *pgTailSource) Newest(ctx context.Context) (tailMark, error) {
	// How far capture has indexed the source, read BEFORE its saved position:
	// the position read next is of the same save or a later one, so every
	// commit below this is at or before the event that position names
	// (anchorReached).
	sent, sentOK, err := reconstruct.PGSentThrough(ctx, x.b.db)
	if err != nil {
		return tailMark{}, err
	}
	checkpoint, id, ok, err := reconstruct.PGCheckpoint(ctx, x.b.db)
	if err != nil || !ok {
		return tailMark{}, err
	}
	if sentOK {
		x.mu.Lock()
		// Only up: what was indexed stays indexed, and a capture that just
		// started has no position to give yet.
		x.sentThrough = max(x.sentThrough, sent)
		x.mu.Unlock()
	}
	var file sql.NullString
	var pos sql.NullInt64
	m := tailMark{ID: id}
	err = x.b.db.QueryRowContext(ctx, `SELECT binlog_file, start_pos, event_timestamp FROM binlog_events WHERE event_id = ?`, id).
		Scan(&file, &pos, &m.At)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return tailMark{}, nil
	case err != nil:
		return tailMark{}, fmt.Errorf("read event %d of the index: %w", id, err)
	}
	lsn, ok := pgEventLSN(file, pos)
	if !ok || lsn > checkpoint {
		// Not what the capture recorded: an index emptied and written
		// again, or another family's row.
		return tailMark{}, nil
	}
	m.Pos, m.At = reconstruct.PGTailPos(lsn), m.At.UTC()
	return m, nil
}

// pgEventLSN is the commit LSN of a row read as (binlog_file, start_pos);
// ok is false when the two do not spell one LSN.
func pgEventLSN(file sql.NullString, pos sql.NullInt64) (uint64, bool) {
	if !file.Valid || !pos.Valid || pos.Int64 <= 0 {
		return 0, false
	}
	return reconstruct.PGRowLSN(&query.ResultRow{BinlogFile: file.String, StartPos: uint64(pos.Int64)})
}

// lsnOf reads the commit LSN of event id; ok is false when the index no
// longer holds it, or holds another kind of row under it.
func (x *pgTailSource) lsnOf(ctx context.Context, id uint64) (uint64, bool, error) {
	var file sql.NullString
	var pos sql.NullInt64
	err := x.b.db.QueryRowContext(ctx, `SELECT binlog_file, start_pos FROM binlog_events WHERE event_id = ?`, id).Scan(&file, &pos)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("read event %d of the index: %w", id, err)
	}
	lsn, ok := pgEventLSN(file, pos)
	return lsn, ok, nil
}

// Holds reads m's event back by id.
func (x *pgTailSource) Holds(ctx context.Context, m tailMark) (bool, error) {
	lsn, ok, err := x.lsnOf(ctx, m.ID)
	if err != nil || !ok {
		return false, err
	}
	return m.Pos == reconstruct.PGTailPos(lsn), nil
}

// pgCut is where a PostgreSQL table's window starts, read from the footer of
// the file its view reads: the LSN floor and the time no change at or after
// the floor committed before, or why the table has no tail.
type pgCut struct {
	refusal string
	// gap: the refusal is a snapshot (or the table delta beside it)
	// published over a known loss of captured changes: a loss of capture,
	// for the PostgreSQL port (pgNotCapturedOf).
	gap     bool
	// footer: md is the file's footer, read; on a refusal, false when the
	// refusal came before it was read.
	footer bool
	md      baseline.DumpMetadata
	floor   uint64
	from    *query.BinlogPos
	since   time.Time
}

// readPoint is what the capture-loss rule compares the cut's file with
// (reconstruct.PGReadPoint): its WAL floor when it is a full read of the
// source, else 0, and when its rows were last read.
func (c pgCut) readPoint() (uint64, time.Time) { return reconstruct.PGReadPoint(c.md) }

// cut reads t's cut. The snapshot's time is its folder's, not the file's: a
// file carried forward was written by an earlier run.
func (x *pgTailSource) cut(t views.BaselineTable) pgCut {
	name := t.Schema + "." + t.Table
	refuse := func(format string, args ...any) pgCut { return pgCut{refusal: fmt.Sprintf(format, args...)} }
	if t.Delta || len(t.DeltaFiles) > 0 {
		return refuse("the copy of %s has a MySQL table delta beside it", name)
	}
	dir, ok := copySnapshotDir(t)
	if !ok {
		return refuse("the snapshot of %s is not a table file in a local snapshot folder", name)
	}
	at, ok := snapshotdir.ParseTime(filepath.Base(filepath.Clean(dir)))
	if !ok {
		return refuse("the folder of %s's snapshot does not say when it was taken", name)
	}
	md, err := x.s.copyCuts.footer(t.Path)
	if err != nil {
		return refuse("the snapshot file of %s could not be read: %v", name, err)
	}
	// From here a refusal carries the footer it was read from, for the
	// capture-loss rule of the PostgreSQL port (pgNotCapturedOf).
	refuse = func(format string, args ...any) pgCut {
		return pgCut{refusal: fmt.Sprintf(format, args...), md: md, footer: true}
	}
	if baseline.ReadConsistencyOf(md) != baseline.ReadConsistent {
		return refuse("the snapshot of %s does not say it was read at one moment of the source", name)
	}
	if md.CaptureGap != "" {
		c := refuse("the snapshot of %s was published over a known loss of captured changes", name)
		c.gap = true
		return c
	}
	since, from, proven, err := reconstruct.PGDeltaWindow(md, at)
	switch {
	case err != nil:
		return refuse("the snapshot of %s: %v", name, err)
	case from == nil || !proven || md.LSN == 0:
		return refuse("the snapshot of %s does not record an LSN to continue from", name)
	}
	// A table delta of PostgreSQL's own beside the file (#2757): the view
	// reads the file and the delta, which holds every change up to its own
	// cut. The tail starts THERE, not at the file's floor. From the file's
	// floor it would be right too, and it was written that way first: but
	// the file is not written again while it has a delta, so that tail grew
	// with every update, was built again from nothing at each one (the same
	// floor is not a later cut, so the old tail could not stand for it), and
	// statements were answered without it meanwhile.
	pair, why, gap := x.pairCut(t, name, md, at)
	if why != "" {
		c := refuse("%s", why)
		c.gap = gap
		return c
	}
	if pair != nil {
		return pgCut{md: md, footer: true, floor: pair.floor, from: pair.from, since: pair.since}
	}
	return pgCut{md: md, footer: true, floor: md.LSN, from: from, since: since}
}

// pairCut is the cut of the table delta beside t's file, nil when it has
// none: where the delta's own window ends, read from the footer of its
// .pgnew file, which is the footer a table file written by the same update
// would carry. Whatever keeps it from being read is a refusal, never the
// file's floor instead: a tail from there is the one this replaces.
//
// gap: the refusal is the delta's being published over a known loss.
func (x *pgTailSource) pairCut(t views.BaselineTable, name string, file baseline.DumpMetadata, at time.Time) (_ *pgCut, refusal string, gap bool) {
	dead, rows, err := pgdelta.Paths(t.Path, 0)
	if err != nil {
		return nil, "", false
	}
	_, deadErr := os.Lstat(dead)
	_, rowsErr := os.Lstat(rows)
	if errors.Is(deadErr, os.ErrNotExist) && errors.Is(rowsErr, os.ErrNotExist) {
		return nil, "", false
	}
	if deadErr != nil || rowsErr != nil {
		return nil, fmt.Sprintf("the table delta beside the copy of %s is not whole, or cannot be asked about", name), false
	}
	md, err := x.s.copyCuts.footer(rows)
	if err != nil {
		return nil, fmt.Sprintf("the table delta beside the copy of %s could not be read: %v", name, err), false
	}
	if md.CaptureGap != "" {
		return nil, fmt.Sprintf("the table delta beside the copy of %s was published over a known loss of captured changes", name), true
	}
	since, from, proven, err := reconstruct.PGDeltaWindow(md, at)
	switch {
	case err != nil:
		return nil, fmt.Sprintf("the table delta beside the copy of %s: %v", name, err), false
	case from == nil || !proven || md.LSN == 0:
		return nil, fmt.Sprintf("the table delta beside the copy of %s does not record an LSN to continue from", name), false
	case md.LSN < file.LSN:
		return nil, fmt.Sprintf("the table delta beside the copy of %s ends before its file was written", name), false
	}
	return &pgCut{floor: md.LSN, from: from, since: since}, "", false
}

// Cut is where Fill starts t's window, as the keeper orders it. A PostgreSQL
// footer records no event id.
//
// None for a full read's file capture has not passed (anchorReached): the
// tail the table had is then not laid over it.
func (x *pgTailSource) Cut(t views.BaselineTable) (tailFloor, string) {
	c := x.cut(t)
	if c.refusal != "" {
		return tailFloor{}, c.refusal
	}
	if why := x.anchorReached(t, c.md); why != "" {
		return tailFloor{}, why
	}
	return tailFloor{anchor: reconstruct.PGTailPos(c.floor)}, ""
}

// anchorReached says why t's file cannot have a tail laid over it yet: it
// was written by a full read, whose rows are the source as of a position
// PAST the file's floor (baseline.MetaKeyPGAnchorLSN, #2963), and capture has
// not yet indexed every commit below that position. A tail read from the
// floor up to a cursor short of it holds, for a row that changed twice in
// between, the first change, and would put it over the second, which the
// file holds: an older value, or a deleted row back, with nothing to say so.
// "" when the file records no such position (an update wrote it, or a full
// read before this was recorded) or capture has passed it.
//
// The position compared is the one Newest read with the keeper's cursor, not
// a later one: a tail is read up to that cursor.
func (x *pgTailSource) anchorReached(t views.BaselineTable, md baseline.DumpMetadata) string {
	if md.PGAnchorLSN == 0 {
		return ""
	}
	x.mu.Lock()
	sent := x.sentThrough
	x.mu.Unlock()
	if sent >= md.PGAnchorLSN {
		return ""
	}
	if sent == 0 {
		return fmt.Sprintf("the snapshot of %s.%s was read at %s, and the index does not record how far capture has indexed the source",
			t.Schema, t.Table, reconstruct.PGLSNText(md.PGAnchorLSN))
	}
	return fmt.Sprintf("the snapshot of %s.%s was read at %s, and capture has indexed the source through %s",
		t.Schema, t.Table, reconstruct.PGLSNText(md.PGAnchorLSN), reconstruct.PGLSNText(sent))
}

// BaseFile names what a tail of t was checked against. For a MySQL table
// that is the base file itself (indexTailSource): a refresh links it into
// each new snapshot, and a file written again may be another table. A
// PostgreSQL table is written again at every snapshot update, by an update
// from the recorded changes or by a full read, so by that rule its tail was
// built again from nothing at each one: every change since the new cut read
// back from the index, while statements waited three seconds and then read
// the snapshot alone, as old as the update took (minutes on a large
// database), a value going back on a connection that had read a newer one.
//
// What a PostgreSQL tail is checked against is the table's shape as its
// file records it (NewPGTail: the relation with its key, the settings its
// values were rendered under), and a file written again for the same table
// records the same. So that is the name: two files of one table with one
// shape are one base, and the keeper keeps the tail and prunes it to the
// newer cut (standsFor, pruneTo), with every other condition it puts on
// that, the cursor having reached the cut among them. A shape that changed
// (a column, the key, the table made again under the same name: the
// relation carries its OID) is another name, and the tail is built again as
// before.
func (x *pgTailSource) BaseFile(t views.BaselineTable) string {
	md, err := x.s.copyCuts.footer(t.Path)
	if err != nil || md.PGRelation == "" {
		return baseFileID(t.Path)
	}
	sum := sha256.Sum256([]byte(t.Schema + "\x00" + t.Table + "\x00" + md.PGRelation + "\x00" + md.RenderGUCs))
	return "pgshape:" + hex.EncodeToString(sum[:16])
}

// maxSnapshotID is the newest schema snapshot the index holds, 0 for none.
func (x *pgTailSource) maxSnapshotID(ctx context.Context) (int64, error) {
	var id int64
	if err := x.b.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(snapshot_id), 0) FROM schema_snapshots").Scan(&id); err != nil {
		return 0, fmt.Errorf("read the newest schema snapshot of the index: %w", err)
	}
	return id, nil
}

// resolver is the newest shape of every table of the index.
func (x *pgTailSource) resolver(ctx context.Context) (*metadata.Resolver, error) {
	id, err := x.maxSnapshotID(ctx)
	if err != nil {
		return nil, err
	}
	if id == 0 {
		return nil, nil
	}
	x.mu.Lock()
	have, haveID := x.schema, x.schemaID
	x.mu.Unlock()
	if have != nil && haveID == id {
		return have, nil
	}
	// Outside the lock: the read has a deadline of its own, longer than a
	// refresh's.
	r, err := metadata.NewLatestPerTableResolver(x.b.db)
	if err != nil {
		return nil, fmt.Errorf("read the index's schema snapshots: %w", err)
	}
	x.mu.Lock()
	x.schema, x.schemaID = r, id
	x.mu.Unlock()
	return r, nil
}

// Start builds t's tail from the footer of the file its view reads (the
// recorded shape), the file's own columns, and the index's newest shape of
// the table (its key, in the table's column order, and its generated
// columns).
func (x *pgTailSource) Start(ctx context.Context, t views.BaselineTable) (tailTable, string, error) {
	name := t.Schema + "." + t.Table
	if x.capturesUnknown {
		return nil, "which tables capture records is not known here", nil
	}
	if x.captures != nil && !x.captures(t.Schema, t.Table) {
		return nil, name + " is outside what capture records", nil
	}
	c := x.cut(t)
	if c.refusal != "" {
		return nil, c.refusal, nil
	}
	// An error, not a refusal: a refusal stands until the next snapshot, and
	// this ends when capture passes the position, so the build is tried
	// again.
	if why := x.anchorReached(t, c.md); why != "" {
		return nil, "", errors.New(why + "; its tail waits for capture to pass it")
	}
	// A table that entered capture after the snapshot's floor: its changes
	// between the two were never captured.
	if e := query.PGTableEnteredCaptureAfter(x.b.db, t.Schema, t.Table, c.floor); e.Err != nil {
		return nil, "", fmt.Errorf("read when %s entered capture: %w", name, e.Err)
	} else if e.Late {
		return nil, fmt.Sprintf("%s entered capture (%s at %s) after its snapshot's position", name, e.Kind, reconstruct.PGLSNText(e.LSN)), nil
	}
	resolver, err := x.resolver(ctx)
	if err != nil {
		return nil, "", err
	}
	// The index's shape of the table, or, when it has none, the one the
	// file records. The stream describes a table when it first carries a
	// change of it, so a table nothing has written to since capture began (a
	// lookup table) has no shape in the index and never gets one. Its tail
	// is then empty, which is right, and it must HAVE one: a statement
	// answers fresh only when every table it reads does, and such a table is
	// in most joins. A change of it brings its shape with it, and Broken
	// holds the tail to that shape from then on.
	var tm *metadata.TableMeta
	why := "the index has no schema snapshot to read " + name + "'s key from"
	if resolver != nil {
		if tm, err = resolver.Resolve(t.Schema, t.Table); err != nil {
			tm, why = nil, fmt.Sprintf("%s could not be found in the index's schema snapshots: %v", name, err)
		}
	}
	if tm == nil {
		if rel, ok, perr := baseline.ParsePGRelation(c.md.PGRelation); perr == nil && ok {
			tm = reconstruct.PGShapeFromFile(t.Schema, t.Table, rel)
		}
		if tm == nil {
			return nil, why, nil
		}
	}
	var generated []string
	for _, col := range tm.Columns {
		if col.IsGenerated {
			generated = append(generated, col.Name)
		}
	}
	fileColumns, err := reconstruct.ReadBaselineColumns(ctx, t.Path, duckdbutil.DefaultTuning())
	if err != nil {
		// A read cut short by the build's deadline is an error, tried
		// again; a file that cannot be read is a refusal, which stands
		// until the next snapshot, where retrying it would never end.
		if ctx.Err() != nil {
			return nil, "", fmt.Errorf("read the columns of %s's snapshot file: %w", name, err)
		}
		return nil, fmt.Sprintf("the columns of %s's snapshot file could not be read: %v", name, err), nil
	}
	tail, err := reconstruct.NewPGTail(t.Schema, t.Table, c.md, fileColumns, tm.PKColumnMetas(), generated)
	if err != nil {
		var r *reconstruct.TailRefusal
		if errors.As(err, &r) {
			return nil, r.Reason, nil
		}
		return nil, "", err
	}
	// A large value PostgreSQL did not send and no change of the window
	// holds is read from the table's copy: by Fill for the window, by the
	// keeper's updates after (pg_tail_copy.go).
	tail.AwaitCopyValues()
	x.noteStarted(t, tail)
	return tail, "", nil
}

// windowOptions is the read of t's window, as the refresh reads it: from the
// floor, bounded below by the time the footer proves.
func (c pgCut) windowOptions(t views.BaselineTable) query.Options {
	since := c.since
	return query.Options{Schema: t.Schema, Table: t.Table, Since: &since, SincePos: c.from}.SinceProven()
}

// Fill reads t's events from its floor up to upTo, a page at a time, and
// then asks whether the index still holds every hour of the window.
func (x *pgTailSource) Fill(ctx context.Context, t views.BaselineTable, upTo uint64, page func([]query.ResultRow) error) (string, error) {
	name := t.Schema + "." + t.Table
	// The tail Start built for this cut, taken now so that no way out of
	// this function leaves it held.
	started := x.takeStarted(t)
	c := x.cut(t)
	if c.refusal != "" {
		return c.refusal, nil
	}
	through, ok, err := x.lsnOf(ctx, upTo)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("the index no longer holds event %d, which the tail of %s was to be read up to", upTo, name)
	}
	// By time alone, from the snapshot's own bound, and the floor applied to
	// the rows here. A fetch that is given the position too reads from up to
	// two hours before the bound (query.CoarseSinceFloor: the engine does
	// not take a time for exact under a position) and drops, row by row,
	// every change of the table in those hours: millions on a busy table,
	// seconds each time, and a statement waits three for its tail. The bound
	// is exact here (cut refuses a snapshot that does not prove it): no
	// change at or after the floor committed before it.
	since := c.since
	opts := query.Options{Schema: t.Schema, Table: t.Table, Since: &since, Limit: x.pageSize}
	for {
		rows, err := x.b.engine.Fetch(ctx, opts)
		if err != nil {
			return "", fmt.Errorf("read the changes of %s: %w", name, err)
		}
		kept, bad := pgTailPage(rows, upTo, through, c.floor)
		if len(bad) > 0 {
			return fmt.Sprintf("event %d of %s has no LSN, so its order against the others cannot be told", bad[0].EventID, name), nil
		}
		if len(kept) > 0 {
			if err := page(kept); err != nil {
				return "", err
			}
		}
		if len(rows) < x.pageSize {
			break
		}
		last := rows[len(rows)-1]
		opts.AfterEvent = &query.EventCursor{Timestamp: last.EventTimestamp, EventID: last.EventID}
	}
	// After the reads: rotation drops the oldest hours, so an hour that was
	// there for the read and is gone now is seen here. Live partitions
	// only: an hour moved to an archive is one the tail did not read.
	cover := c.windowOptions(t)
	err = query.VerifyMergedCoverage(ctx, x.b.db, query.FetchMergedOptions{
		Opts: cover, DBName: x.b.dbName, NoArchive: true,
		NeverPartitionedBefore: reconstruct.PGNeverPartitionedBefore(ctx, x.b.db, x.b.dbName, t.Schema, t.Table, c.floor),
	})
	var gap *query.GapError
	switch {
	case errors.As(err, &gap):
		return "the index no longer holds every change since the snapshot of " + name + " (older partitions were rotated out, or the index began after the snapshot)", nil
	case err != nil:
		return "", fmt.Errorf("check the index holds %s's changes since its snapshot: %w", name, err)
	}
	// Last, over a window known to be whole: the large values no change of
	// it holds, read from the cut the window starts at.
	return x.fillStartedFromCopy(ctx, t, started)
}

// pgTailPage is rows as a tail is fed them: the ones with an id up to upTo
// and a commit LSN from atLeast up to through, each spelled for the keeper.
// bad are the rows that carry no LSN, which have no order among the others.
func pgTailPage(rows []query.ResultRow, upTo, through, atLeast uint64) (kept, bad []query.ResultRow) {
	kept = rows[:0:0]
	for _, r := range rows {
		if r.EventID > upTo {
			continue
		}
		lsn, ok := reconstruct.PGRowLSN(&r)
		if !ok {
			bad = append(bad, r)
			continue
		}
		if lsn > through || lsn < atLeast {
			continue
		}
		reconstruct.PGTailEvent(&r)
		kept = append(kept, r)
	}
	return kept, bad
}

// Since reads the events of tables with ids in (from, upTo], in id order.
// The paging goes by the rows read, not by the ones handed over: a page of
// which none is kept is not the end.
//
// The rows are handed with their LSN never going back: one below the
// highest already handed (from's own, then each page's) is a transaction
// arriving a second time, whose first delivery was handed whole. A row with
// no LSN is left out, and its table's tail ends at the next Broken.
func (x *pgTailSource) Since(ctx context.Context, tables []views.BaselineTable, from, upTo uint64, page func([]query.ResultRow) error) error {
	through, ok, err := x.lsnOf(ctx, upTo)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("the index no longer holds event %d, which the tails were to be read up to", upTo)
	}
	// Where the hand-over stands: the event read up to last time. When the
	// index no longer holds it, nothing is known to have been handed.
	var handed uint64
	if from > 0 {
		if handed, _, err = x.lsnOf(ctx, from); err != nil {
			return err
		}
	}
	refs := make([]query.SchemaTable, len(tables))
	for i, t := range tables {
		refs[i] = query.SchemaTable{Schema: t.Schema, Table: t.Table}
	}
	// Where the last read from this same event stopped, having handed
	// nothing. What it left out stays left out: a row then past the LSN
	// read up to was the beginning of a transaction that has to come again
	// whole, and that whole delivery is above it.
	cur := from
	x.mu.Lock()
	if x.skipped.from == from && x.skipped.to > cur {
		cur = min(x.skipped.to, upTo)
	}
	x.mu.Unlock()
	handedAny := false
	for cur < upTo {
		rows, err := query.FetchEventRange(ctx, x.b.db, refs, cur, upTo, x.pageSize)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			cur = upTo
			break
		}
		kept, bad := pgTailPage(rows, upTo, through, handed)
		x.noteUnordered(bad)
		// In id order a page's LSNs can still step back inside it.
		ordered := kept[:0]
		for _, r := range kept {
			if r.StartPos < handed {
				continue
			}
			handed = r.StartPos
			ordered = append(ordered, r)
		}
		if len(ordered) > 0 {
			if err := page(ordered); err != nil {
				return err
			}
			handedAny = true
		}
		if len(rows) < x.pageSize {
			cur = upTo
			break
		}
		cur = rows[len(rows)-1].EventID
		if len(ordered) == 0 {
			// A whole page left out: ask the index where the next row to
			// hand is, and do not read the ones before it.
			next, found, err := x.nextToHand(ctx, cur, upTo, handed, through)
			if err != nil {
				return err
			}
			if !found {
				cur = upTo
				break
			}
			cur = next - 1
		}
	}
	x.mu.Lock()
	if handedAny {
		x.skipped.from, x.skipped.to = 0, 0
	} else {
		x.skipped.from, x.skipped.to = from, cur
	}
	x.mu.Unlock()
	return nil
}

// nextToHand is the first event after id after, up to upTo, whose LSN is
// from atLeast up to through: where Since goes on after a page it left out
// whole. The index walks the rows between; none of them is read here.
func (x *pgTailSource) nextToHand(ctx context.Context, after, upTo, atLeast, through uint64) (uint64, bool, error) {
	var id uint64
	err := x.b.db.QueryRowContext(ctx,
		`SELECT event_id FROM binlog_events FORCE INDEX (PRIMARY)
		 WHERE event_id > ? AND event_id <= ? AND start_pos >= ? AND start_pos <= ? ORDER BY event_id LIMIT 1`,
		after, upTo, atLeast, through).Scan(&id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("find the next event to read after %d: %w", after, err)
	}
	return id, true, nil
}

// noteUnordered remembers the tables of rows that carry no LSN.
func (x *pgTailSource) noteUnordered(bad []query.ResultRow) {
	if len(bad) == 0 {
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.unordered == nil {
		x.unordered = map[string]string{}
	}
	for _, r := range bad {
		x.unordered[tailKey(r.SchemaName, r.TableName)] = fmt.Sprintf("event %d of %s.%s has no LSN, so its order against the others cannot be told", r.EventID, r.SchemaName, r.TableName)
	}
}

// Broken says why each table's tail cannot go on, asked at every refresh: a
// loss capture recorded since the table's rows were read from the source, a
// shape of the table the stream recorded after its floor that is not its
// snapshot's (a column added, dropped or retyped, the table made again, its
// key changed), or a TRUNCATE, a DROP, a rename or a change of the
// publication since.
//
// Each table's answer is kept while what it is made from stays the same, so
// a refresh with nothing new costs three small reads for the server.
func (x *pgTailSource) Broken(ctx context.Context, tables []views.BaselineTable) (map[string]string, error) {
	cuts := make([]pgCut, len(tables))
	var oldest uint64
	any := false
	for i, t := range tables {
		cuts[i] = x.cut(t)
		if cuts[i].refusal != "" {
			continue // no tail: Start refuses it
		}
		if !any || cuts[i].floor < oldest {
			oldest = cuts[i].floor
		}
		any = true
	}
	if !any {
		return nil, nil
	}
	st, err := status.LoadStreamState(ctx, x.b.db)
	if err != nil {
		return nil, fmt.Errorf("read the capture's state: %w", err)
	}
	if err := pgReadLedgerStrict(ctx, x.b.db, st); err != nil {
		return nil, fmt.Errorf("read the capture's record of dropped changes: %w", err)
	}
	snapshotID, err := x.maxSnapshotID(ctx)
	if err != nil {
		return nil, err
	}
	var schemaChgID, schemaChgs int64
	if err := x.b.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(id), 0), COUNT(*) FROM schema_changes").Scan(&schemaChgID, &schemaChgs); err != nil {
		return nil, fmt.Errorf("read the newest schema change of the index: %w", err)
	}
	base := pgBrokenKey{snapshotID: snapshotID, schemaChgID: schemaChgID, schemaChgs: schemaChgs, stateKnown: st != nil,
		gapColumns: st != nil && st.GapColumnsPresent}
	if st != nil && st.GapColumnsPresent && st.GapLostAt.Valid {
		base.lostAt, base.lostKnown = st.GapLostAt.Time.UTC(), true
	}
	if st != nil && st.CaptureSkips.Valid {
		base.skips = "v" + st.CaptureSkips.String
	}

	// The memo is read and written under the lock; the index is asked
	// outside it, so a slow read holds up no other reader of this source.
	out := map[string]string{}
	keys := make([]pgBrokenKey, len(tables))
	var ask []int
	x.mu.Lock()
	for i, t := range tables {
		if cuts[i].refusal != "" {
			continue
		}
		key := tailKey(t.Schema, t.Table)
		if why, bad := x.unordered[key]; bad {
			out[key] = why
			continue
		}
		keys[i] = base
		keys[i].floor, keys[i].base = cuts[i].floor, baseFileID(t.Path)
		if memo, ok := x.broken[key]; ok && memo.key == keys[i] && keys[i].base != "" {
			if memo.reason != "" {
				out[key] = memo.reason
			}
			continue
		}
		ask = append(ask, i)
	}
	x.mu.Unlock()
	if len(ask) == 0 {
		return out, nil
	}
	shapes, err := x.shapesFrom(ctx, snapshotID, oldest)
	if err != nil {
		return nil, err
	}
	answers := make(map[int]string, len(ask))
	for _, i := range ask {
		reason, err := pgTailBroken(ctx, x.b.db, tables[i], cuts[i], st, shapes)
		if err != nil {
			return nil, err
		}
		answers[i] = reason
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.broken == nil {
		x.broken = map[string]pgBrokenMemo{}
	}
	for i, reason := range answers {
		key := tailKey(tables[i].Schema, tables[i].Table)
		x.broken[key] = pgBrokenMemo{key: keys[i], reason: reason}
		if reason != "" {
			out[key] = reason
		}
	}
	if len(x.broken) > 4*len(tables)+64 {
		// Tables no longer asked about: keep the memo from growing.
		asked := make(map[string]bool, len(tables))
		for _, t := range tables {
			asked[tailKey(t.Schema, t.Table)] = true
		}
		for key := range x.broken {
			if !asked[key] {
				delete(x.broken, key)
			}
		}
	}
	return out, nil
}

// shapesFrom is the shapes the stream recorded from the position from on,
// read again when a newer schema snapshot appears or an older position is
// asked for.
func (x *pgTailSource) shapesFrom(ctx context.Context, snapshotID int64, from uint64) ([]metadata.PGShape, error) {
	x.mu.Lock()
	have, id, at := x.shapes, x.shapesID, x.shapesFromLSN
	x.mu.Unlock()
	if have != nil && id == snapshotID && at <= from {
		return have, nil
	}
	shapes, err := metadata.LoadPGShapes(ctx, x.b.db, from)
	if err != nil {
		return nil, fmt.Errorf("read the table shapes the capture recorded: %w", err)
	}
	if shapes == nil {
		shapes = []metadata.PGShape{}
	}
	x.mu.Lock()
	x.shapes, x.shapesID, x.shapesFromLSN = shapes, snapshotID, from
	x.mu.Unlock()
	return shapes, nil
}

// pgTailBroken is one table's answer to Broken, "" when its tail can go on.
func pgTailBroken(ctx context.Context, db *sql.DB, t views.BaselineTable, c pgCut, st *status.StreamStateInfo, shapes []metadata.PGShape) (string, error) {
	name := t.Schema + "." + t.Table
	lsn, read := c.readPoint()
	if why := pgTailCaptureLoss(st, lsn, read, t.Schema, t.Table); why != "" {
		return why, nil
	}
	rel, ok, err := baseline.ParsePGRelation(c.md.PGRelation)
	if err != nil || !ok {
		return fmt.Sprintf("the snapshot of %s records no table shape to compare with", name), nil
	}
	key, err := reconstruct.PGShapeKey(t.Schema, t.Table, rel, c.floor, math.MaxUint64, shapes)
	if err != nil {
		return fmt.Sprintf("%s changed since its snapshot: %v", name, err), nil
	}
	if key != nil && len(rel.PrimaryKey) > 0 && !sameStringSet(key, rel.PrimaryKey) {
		return fmt.Sprintf("the key of %s is now (%s), and its copy was taken under (%s)", name, strings.Join(key, ", "), strings.Join(rel.PrimaryKey, ", ")), nil
	}
	// No end: the refresh stops at its own time, a tail has none, and the
	// answer is kept until a schema change is recorded, so one dated ahead
	// of this host's clock must be inside the window the first time.
	err = reconstruct.CheckDestructiveDDL(ctx, db, t.Schema, t.Table, reconstruct.DDLWindow{Since: c.since, Until: time.Now().UTC().AddDate(100, 0, 0)})
	switch {
	case errors.Is(err, reconstruct.ErrDestructiveDDL):
		return fmt.Sprintf("%s had a schema change since its snapshot: %v", name, err), nil
	case err != nil:
		return "", fmt.Errorf("read the schema changes of %s: %w", name, err)
	}
	return "", nil
}

// sameStringSet: the same strings, in any order.
func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	in := make(map[string]bool, len(a))
	for _, s := range a {
		in[s] = true
	}
	for _, s := range b {
		if !in[s] {
			return false
		}
	}
	return true
}

// pgTailCaptureLoss is tailCaptureLoss for a PostgreSQL capture and one
// table: a loss on record (a lost slot) since the table's rows were read from
// the source refuses, and so does a record that cannot be read. An
// acknowledged loss still refuses.
//
// Dropped changes refuse too. A PostgreSQL capture records them in
// capture_skips (a table below REPLICA IDENTITY FULL has its changes dropped
// while the other tables keep flowing), and a tail built from the index would
// serve such a table with old rows. That rule is the one the snapshot update
// applies, reconstruct.PGSkippedSince: by WAL position (readLSN, the floor of
// a file that is a full read, against the drop's commit LSN) where both are
// known, by the clock with a margin where not. readLSN and sourceRead are
// reconstruct.PGReadPoint of the file's footer (pgCut.readPoint): a folded
// file's floor is not what its rows were read at. The snapshot's read time is the source
// server's clock and a drop's the capture daemon's, so the clocks alone would
// let a daemon behind the source date a new drop before the read. st's
// ledger must have been read strictly (pgReadLedgerStrict).
//
// A snapshot that records neither its LSN nor when its rows were read
// refuses: nothing says what its rows reach. One with only its LSN is the
// rule's to judge.
//
// A NULL ledger is no drop: PostgreSQL captures from before the ledger never
// wrote one, and they stopped instead of dropping. One that is there and does
// not parse refuses.
func pgTailCaptureLoss(st *status.StreamStateInfo, readLSN uint64, sourceRead time.Time, schema, table string) string {
	if st != nil && st.GapColumnsPresent && sourceRead.IsZero() && readLSN == 0 {
		return "the snapshot does not say when its rows were read from the source, nor the position they reach"
	}
	return pgCaptureLossOf(st, readLSN, sourceRead, schema, table)
}

// pgCaptureLossOf is pgTailCaptureLoss without its refusal of a snapshot
// that records neither its LSN nor its read time: the loss of capture alone,
// judged with what is known of the rows (readLSN 0 and a zero sourceRead
// when nothing is), for the PostgreSQL port's list of tables
// (pgNotCapturedOf). With nothing known, any loss on record, and any drop
// the ledger covers the table with, refuses.
//
// The lost slot records no position, only the daemon's clock, and the read
// time is the source server's: compared with reconstruct.PGSkipClockMargin,
// so a daemon behind the source by less does not date the loss before the
// read.
func pgCaptureLossOf(st *status.StreamStateInfo, readLSN uint64, sourceRead time.Time, schema, table string) string {
	switch {
	case st == nil:
		return "the capture's state is not recorded in the index"
	case !st.GapColumnsPresent:
		return "the capture's record of lost events is not readable"
	case st.GapLostAt.Valid && !st.GapLostAt.Time.Before(sourceRead.Add(-reconstruct.PGSkipClockMargin)):
		return "capture lost events since the snapshot's rows were read from the source"
	}
	return reconstruct.PGSkippedSince(st, readLSN, sourceRead, schema, table)
}

// pgReadLedgerStrict reads st's ledger of dropped changes again, strictly:
// status.LoadStreamState reads it best effort and leaves it NULL on an error,
// which reconstruct.PGSkippedSince takes for no drop. A column the index does
// not have yet (an index from before the ledger) is no ledger, as there; the
// row gone between the two reads keeps what was read. st nil, or without
// the loss columns: nothing to read.
func pgReadLedgerStrict(ctx context.Context, db *sql.DB, st *status.StreamStateInfo) error {
	if st == nil || !st.GapColumnsPresent {
		return nil
	}
	var skips sql.NullString
	err := db.QueryRowContext(ctx, "SELECT capture_skips FROM stream_state WHERE id = 1").Scan(&skips)
	var my *mysql.MySQLError
	switch {
	case err == nil:
		st.CaptureSkips = skips
	case errors.As(err, &my) && my.Number == 1054, errors.Is(err, sql.ErrNoRows):
	default:
		return err
	}
	return nil
}

var _ tailSource = (*pgTailSource)(nil)
var _ tailTable = (*reconstruct.PGTail)(nil)
