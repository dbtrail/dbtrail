package console

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
// the newest rows of the index can be half a transaction. A tail is fed only
// up to the capture's saved position (Newest), which is after a whole
// transaction. After a crash the capture receives again what follows that
// position, under new and higher ids, so rows of an earlier delivery can sit
// below the newest row at or before the saved position while their own
// transaction is past it: every read here also drops the rows whose LSN is
// above the LSN of the event it reads up to. Those come again, whole.
//
// What it does not see: the publication. A table the snapshot holds and the
// publication does not has no events, and its tail would say the copy is
// current. What the index records of a table entering capture late
// (query.PGTableEnteredCaptureAfter) and of a change of the publication
// (CheckDestructiveDDL) refuse; a table never published is not told apart.
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
	shapes     []metadata.PGShape
	shapesID   int64
	shapesFrom uint64
	// broken remembers each table's answer to Broken for as long as nothing
	// it is made from changed (pgBrokenKey).
	broken map[string]pgBrokenMemo
}

// pgBrokenMemo is one table's last answer to Broken and what it was made
// from.
type pgBrokenMemo struct {
	key    pgBrokenKey
	reason string
}

// pgBrokenKey is what a table's answer to Broken depends on: its file's
// floor, the newest schema snapshot and schema change the index holds, and
// the loss capture recorded.
type pgBrokenKey struct {
	floor       uint64
	snapshotID  int64
	schemaChgID int64
	lostAt      time.Time
	lostKnown   bool
}

func newPGTailSource(s *Server, b *bundle) *pgTailSource {
	return &pgTailSource{s: s, b: b, pageSize: tailPageSize, capturesUnknown: true}
}

// capturing says which tables capture records (nil: every table).
func (x *pgTailSource) capturing(captures func(schema, table string) bool) *pgTailSource {
	x.captures, x.capturesUnknown = captures, false
	return x
}

// Newest is the newest event at or before the capture's saved position: ID 0
// when the index records no PostgreSQL capture's position, or holds no event
// at or before it. The rows past the position are what was committed since
// it was saved, so the walk back from the newest row is short.
func (x *pgTailSource) Newest(ctx context.Context) (tailMark, error) {
	checkpoint, ok, err := reconstruct.PGCheckpoint(ctx, x.b.db)
	if err != nil || !ok {
		return tailMark{}, err
	}
	// The position first, the row after: every row at or before a saved
	// position was committed before it was saved.
	var m tailMark
	var file sql.NullString
	var pos sql.NullInt64
	err = x.b.db.QueryRowContext(ctx,
		`SELECT event_id, binlog_file, start_pos, event_timestamp FROM binlog_events FORCE INDEX (PRIMARY)
		 WHERE start_pos <= ? ORDER BY event_id DESC LIMIT 1`, checkpoint).
		Scan(&m.ID, &file, &pos, &m.At)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return tailMark{}, nil
	case err != nil:
		return tailMark{}, fmt.Errorf("read the newest event of the index: %w", err)
	}
	lsn, ok := pgEventLSN(file, pos)
	if !ok {
		// A row that is not a PostgreSQL event (an index another family
		// wrote before): nothing here can be ordered against it.
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
	md      baseline.DumpMetadata
	floor   uint64
	from    *query.BinlogPos
	since   time.Time
}

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
	if baseline.ReadConsistencyOf(md) != baseline.ReadConsistent {
		return refuse("the snapshot of %s does not say it was read at one moment of the source", name)
	}
	if md.CaptureGap != "" {
		return refuse("the snapshot of %s was published over a known loss of captured changes", name)
	}
	since, from, proven, err := reconstruct.PGDeltaWindow(md, at)
	switch {
	case err != nil:
		return refuse("the snapshot of %s: %v", name, err)
	case from == nil || !proven || md.LSN == 0:
		return refuse("the snapshot of %s does not record an LSN to continue from", name)
	}
	// A table delta of PostgreSQL's own (#2757): the table is its file and
	// the pair, and the views read the file alone.
	if dead, rows, perr := pgdelta.Paths(t.Path, 0); perr == nil {
		for _, f := range []string{dead, rows} {
			if _, serr := os.Lstat(f); serr == nil || !errors.Is(serr, os.ErrNotExist) {
				return refuse("the copy of %s has a table delta beside its file, which SQL on the copy does not read yet", name)
			}
		}
	}
	return pgCut{md: md, floor: md.LSN, from: from, since: since}
}

// Cut is where Fill starts t's window, as the keeper orders it. A PostgreSQL
// footer records no event id.
func (x *pgTailSource) Cut(t views.BaselineTable) (tailFloor, string) {
	c := x.cut(t)
	if c.refusal != "" {
		return tailFloor{}, c.refusal
	}
	return tailFloor{anchor: reconstruct.PGTailPos(c.floor)}, ""
}

// BaseFile names t's base file as indexTailSource does.
func (x *pgTailSource) BaseFile(t views.BaselineTable) string { return baseFileID(t.Path) }

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
	x.mu.Lock()
	defer x.mu.Unlock()
	if id == 0 {
		return nil, nil
	}
	if x.schema == nil || id != x.schemaID {
		r, err := metadata.NewLatestPerTableResolver(x.b.db)
		if err != nil {
			return nil, fmt.Errorf("read the index's schema snapshots: %w", err)
		}
		x.schema, x.schemaID = r, id
	}
	return x.schema, nil
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
	if resolver == nil {
		return nil, "the index has no schema snapshot to read " + name + "'s key from", nil
	}
	tm, err := resolver.Resolve(t.Schema, t.Table)
	if err != nil {
		return nil, fmt.Sprintf("%s could not be found in the index's schema snapshots: %v", name, err), nil
	}
	var generated []string
	for _, col := range tm.Columns {
		if col.IsGenerated {
			generated = append(generated, col.Name)
		}
	}
	fileColumns, err := reconstruct.ReadBaselineColumns(ctx, t.Path, duckdbutil.DefaultTuning())
	if err != nil {
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
	c := x.cut(t)
	if c.refusal != "" {
		return c.refusal, nil
	}
	through, ok, err := x.lsnOf(ctx, upTo)
	if err != nil {
		return "", err
	}
	if !ok {
		return fmt.Sprintf("the index no longer holds the event the tail of %s was to be read up to", name), nil
	}
	opts := c.windowOptions(t)
	opts.Limit = x.pageSize
	for {
		rows, err := x.b.engine.Fetch(ctx, opts)
		if err != nil {
			return "", fmt.Errorf("read the changes of %s: %w", name, err)
		}
		kept, why := pgTailPage(rows, upTo, through, name)
		if why != "" {
			return why, nil
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
	return "", nil
}

// pgTailPage is rows as a tail is fed them: the ones with an id up to upTo
// and a commit LSN up to through, each spelled for the keeper. A row that
// carries no LSN refuses: it has no order among the others.
func pgTailPage(rows []query.ResultRow, upTo, through uint64, name string) (kept []query.ResultRow, why string) {
	kept = rows[:0:0]
	for _, r := range rows {
		if r.EventID > upTo {
			continue
		}
		lsn, ok := reconstruct.PGRowLSN(&r)
		if !ok {
			return nil, fmt.Sprintf("event %d of %s has no LSN, so its order against the others cannot be told", r.EventID, name)
		}
		if lsn > through {
			continue
		}
		reconstruct.PGTailEvent(&r)
		kept = append(kept, r)
	}
	return kept, ""
}

// Since reads the events of tables with ids in (from, upTo], in id order.
// The paging goes by the rows read, not by the ones handed over: a page of
// which none is kept is not the end.
func (x *pgTailSource) Since(ctx context.Context, tables []views.BaselineTable, from, upTo uint64, page func([]query.ResultRow) error) error {
	through, ok, err := x.lsnOf(ctx, upTo)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("the index no longer holds event %d, which the tails were to be read up to", upTo)
	}
	refs := make([]query.SchemaTable, len(tables))
	for i, t := range tables {
		refs[i] = query.SchemaTable{Schema: t.Schema, Table: t.Table}
	}
	for from < upTo {
		rows, err := query.FetchEventRange(ctx, x.b.db, refs, from, upTo, x.pageSize)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		kept, why := pgTailPage(rows, upTo, through, "a table of this server")
		if why != "" {
			return errors.New(why)
		}
		if len(kept) > 0 {
			if err := page(kept); err != nil {
				return err
			}
		}
		if len(rows) < x.pageSize {
			return nil
		}
		from = rows[len(rows)-1].EventID
	}
	return nil
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
	snapshotID, err := x.maxSnapshotID(ctx)
	if err != nil {
		return nil, err
	}
	var schemaChgID int64
	if err := x.b.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(id), 0) FROM schema_changes").Scan(&schemaChgID); err != nil {
		return nil, fmt.Errorf("read the newest schema change of the index: %w", err)
	}
	base := pgBrokenKey{snapshotID: snapshotID, schemaChgID: schemaChgID}
	if st != nil && st.GapColumnsPresent && st.GapLostAt.Valid {
		base.lostAt, base.lostKnown = st.GapLostAt.Time.UTC(), true
	}

	x.mu.Lock()
	defer x.mu.Unlock()
	if x.broken == nil {
		x.broken = map[string]pgBrokenMemo{}
	}
	out := map[string]string{}
	seen := make(map[string]bool, len(tables))
	var shapes []metadata.PGShape
	shapesRead := false
	for i, t := range tables {
		if cuts[i].refusal != "" {
			continue
		}
		key := tailKey(t.Schema, t.Table)
		seen[key] = true
		k := base
		k.floor = cuts[i].floor
		if memo, ok := x.broken[key]; ok && memo.key == k {
			if memo.reason != "" {
				out[key] = memo.reason
			}
			continue
		}
		if !shapesRead {
			if shapes, err = x.shapesLocked(ctx, snapshotID, oldest); err != nil {
				return nil, err
			}
			shapesRead = true
		}
		reason, err := pgTailBroken(ctx, x.b.db, t, cuts[i], st, shapes)
		if err != nil {
			return nil, err
		}
		x.broken[key] = pgBrokenMemo{key: k, reason: reason}
		if reason != "" {
			out[key] = reason
		}
	}
	for key := range x.broken {
		if !seen[key] {
			delete(x.broken, key)
		}
	}
	return out, nil
}

// shapesLocked is the shapes the stream recorded from the position from on,
// read again when a newer schema snapshot appears or an older position is
// asked for. x.mu is held.
func (x *pgTailSource) shapesLocked(ctx context.Context, snapshotID int64, from uint64) ([]metadata.PGShape, error) {
	if x.shapes != nil && x.shapesID == snapshotID && x.shapesFrom <= from {
		return x.shapes, nil
	}
	shapes, err := metadata.LoadPGShapes(ctx, x.b.db, from)
	if err != nil {
		return nil, fmt.Errorf("read the table shapes the capture recorded: %w", err)
	}
	if shapes == nil {
		shapes = []metadata.PGShape{}
	}
	x.shapes, x.shapesID, x.shapesFrom = shapes, snapshotID, from
	return shapes, nil
}

// pgTailBroken is one table's answer to Broken, "" when its tail can go on.
func pgTailBroken(ctx context.Context, db *sql.DB, t views.BaselineTable, c pgCut, st *status.StreamStateInfo, shapes []metadata.PGShape) (string, error) {
	name := t.Schema + "." + t.Table
	if why := pgTailCaptureLoss(st, baseline.SourceReadOf(c.md).At); why != "" {
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
	// Until now: the refresh stops at its own time, a tail has none.
	err = reconstruct.CheckDestructiveDDL(ctx, db, t.Schema, t.Table, reconstruct.DDLWindow{Since: c.since, Until: time.Now().UTC().Add(time.Hour)})
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

// pgTailCaptureLoss is tailCaptureLoss for a PostgreSQL capture, which keeps
// no record of dropped events (capture_skips is MySQL's): a loss on record
// since the table's rows were read from the source refuses, and so does a
// record that cannot be read. An acknowledged loss still refuses.
func pgTailCaptureLoss(st *status.StreamStateInfo, sourceRead time.Time) string {
	switch {
	case st == nil:
		return "the capture's state is not recorded in the index"
	case !st.GapColumnsPresent:
		return "the capture's record of lost events is not readable"
	case sourceRead.IsZero():
		return "the snapshot does not say when its rows were read from the source"
	case st.GapLostAt.Valid && !st.GapLostAt.Time.Before(sourceRead):
		return "capture lost events since the snapshot's rows were read from the source"
	}
	return ""
}

var _ tailSource = (*pgTailSource)(nil)
var _ tailTable = (*reconstruct.PGTail)(nil)
