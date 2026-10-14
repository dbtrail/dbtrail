package console

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/views"
)

// indexTailSource is a server's tails' tailSource: its index, through the
// connection b holds, and the copy's files, through the console's memo of
// their footers.
//
// What a table's tail starts from is the window the refresh would fold next,
// read by the same rules the port uses to say a table is unchanged since its
// snapshot (copyUnchanged, #2085): a tail that misses a change is worse than
// no tail. Fill applies its lookups and its checks of the index in its order;
// a schema change, a loss in capture and a change of source server are
// Broken's, asked at every refresh, since they can happen after a tail was
// built. Two of copyUnchanged's rules are not applied, on purpose: the
// capture watermark (this process does not read the source, so a gap in
// capture that the index does not record is not seen, an accepted risk of
// #2369), and its GTID-mode rule, which serves the watermark's comparison
// with the source.
type indexTailSource struct {
	s        *Server
	b        *bundle
	pageSize int
	// captures says whether capture records a table (nil: every table);
	// capturesUnknown that this process cannot tell, and then no table has
	// a tail: a table capture does not record has no events, and its tail
	// would claim a copy as current as the index.
	captures        func(schema, table string) bool
	capturesUnknown bool
}

// tailPageSize is how many events one read of the index returns.
const tailPageSize = 5000

// newIndexTailSource gives no table a tail until capturing says which tables
// capture records: forgetting it must not vouch for tables it never does.
func newIndexTailSource(s *Server, b *bundle) *indexTailSource {
	return &indexTailSource{s: s, b: b, pageSize: tailPageSize, capturesUnknown: true}
}

// capturing says which tables capture records (nil: every table).
func (x *indexTailSource) capturing(captures func(schema, table string) bool) *indexTailSource {
	x.captures, x.capturesUnknown = captures, false
	return x
}

// Newest reads the newest event of the index. An index `bintrail index` also
// wrote has none for the tails: its ids are not its order in the binary log.
func (x *indexTailSource) Newest(ctx context.Context) (tailMark, error) {
	if indexBackfilled(ctx, x.b.db) {
		return tailMark{}, nil
	}
	var m tailMark
	var file sql.NullString
	var pos sql.NullInt64
	err := x.b.db.QueryRowContext(ctx,
		`SELECT event_id, binlog_file, start_pos, event_timestamp FROM binlog_events FORCE INDEX (PRIMARY) ORDER BY event_id DESC LIMIT 1`).
		Scan(&m.ID, &file, &pos, &m.At)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return tailMark{}, nil
	case err != nil:
		return tailMark{}, fmt.Errorf("read the newest event of the index: %w", err)
	}
	m.Pos = query.BinlogPos{File: file.String}
	if pos.Valid && pos.Int64 >= 0 {
		m.Pos.Pos = uint64(pos.Int64)
	}
	m.At = m.At.UTC()
	return m, nil
}

// Holds reads m's event back by id.
func (x *indexTailSource) Holds(ctx context.Context, m tailMark) (bool, error) {
	var file sql.NullString
	var pos sql.NullInt64
	err := x.b.db.QueryRowContext(ctx, `SELECT binlog_file, start_pos FROM binlog_events WHERE event_id = ?`, m.ID).Scan(&file, &pos)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("read event %d of the index: %w", m.ID, err)
	}
	return file.String == m.Pos.File && pos.Valid && pos.Int64 >= 0 && uint64(pos.Int64) == m.Pos.Pos, nil
}

// decodingTail decodes each page as the refresh does before folding it.
type decodingTail struct {
	*reconstruct.Tail
	decode func([]query.ResultRow)
}

func (d decodingTail) Apply(page []query.ResultRow) error {
	d.decode(page)
	return d.Tail.Apply(page)
}

// Start builds t's tail from the footer of the file its view reads first
// (the table's definition) and the schema snapshot (its key, which must be
// the definition's: a snapshot loaded before the key changed would drop
// published rows by a part of it).
func (x *indexTailSource) Start(_ context.Context, t views.BaselineTable) (tailTable, string, error) {
	name := t.Schema + "." + t.Table
	if x.capturesUnknown {
		return nil, "which tables capture records is not known here", nil
	}
	if x.captures != nil && !x.captures(t.Schema, t.Table) {
		return nil, name + " is outside what capture records", nil
	}
	cut := copyCutOf(t, x.s.copyCuts.footer)
	if cut.refusal != "" {
		return nil, cut.refusal, nil
	}
	md, err := x.s.copyCuts.footer(t.Path)
	if err != nil {
		return nil, fmt.Sprintf("the snapshot file of %s could not be read", name), nil
	}
	if x.b.resolver == nil {
		return nil, "the index has no schema snapshot to read " + name + "'s key from", nil
	}
	tm, err := x.b.resolver.Resolve(t.Schema, t.Table)
	if err != nil {
		return nil, fmt.Sprintf("%s is not in the index's schema snapshot", name), nil
	}
	pk := tm.PKColumnMetas()
	if !samePrimaryKey(md.CreateTableSQL, pk) {
		return nil, fmt.Sprintf("the key of %s in the index's schema snapshot is not the key of its copy", name), nil
	}
	tail, err := reconstruct.NewTail(t.Schema, t.Table, md.CreateTableSQL, t.Path, pk)
	if err != nil {
		var r *reconstruct.TailRefusal
		if errors.As(err, &r) {
			return nil, r.Reason, nil
		}
		return nil, "", err
	}
	return decodingTail{tail, reconstruct.NewTailDecoder(x.b.db, t.Schema, t.Table, x.b.resolver)}, "", nil
}

// Fill reads t's events after its cut up to upTo, a page at a time, then
// rules out what would make them incomplete. The order is copyUnchanged's:
// the lookups first, the index's record of itself after, partitions and
// archives last, so a partition rotated away while the lookups ran is seen.
func (x *indexTailSource) Fill(ctx context.Context, t views.BaselineTable, upTo uint64, page func([]query.ResultRow) error) (string, error) {
	db := x.b.db
	name := t.Schema + "." + t.Table
	cut := copyCutOf(t, x.s.copyCuts.footer)
	if cut.refusal != "" {
		return cut.refusal, nil
	}
	// A loss in capture is Broken's, asked before any tail is built and at
	// every refresh after.
	since, why, err := copyLookupSince(ctx, db, t, cut)
	if err != nil {
		return "", err
	}
	if why != "" {
		return why, nil
	}
	floor := copyTimeFloor(since)
	// The window, as the refresh reads it.
	if err := x.pages(ctx, query.Options{Schema: t.Schema, Table: t.Table,
		Since: &since, SincePos: &cut.anchor, SinceEventID: cut.lastEventID}.SearchesBelowItsOwnFloor(), upTo, page); err != nil {
		return "", err
	}
	// And the hours below its time floor, where a statement that began long
	// before the snapshot and committed after it sits (copyUnchanged says
	// why). On an index only a stream wrote, one row says whether to look.
	search := indexBackfilled(ctx, db)
	if !search {
		newest, found, err := newestEventOlderThan(ctx, db, floor)
		if err != nil {
			return "", err
		}
		search = found && cut.anchor.AtOrBefore(newest)
	}
	if search {
		if err := x.pages(ctx, query.Options{Schema: t.Schema, Table: t.Table,
			Until: &floor, SincePos: &cut.anchor, SinceEventID: cut.lastEventID}, upTo, page); err != nil {
			return "", err
		}
	}
	if why, err := numberingStartedOver(ctx, db, []views.BaselineTable{t}, []copyCut{cut}); err != nil {
		return "", err
	} else if why != "" {
		return why, nil
	}
	var changedServer int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bintrail_server_changes WHERE UNIX_TIMESTAMP(detected_at) >= ?`, floor.Unix()).Scan(&changedServer); err != nil {
		return "", fmt.Errorf("read the source's identity record: %w", err)
	}
	if changedServer > 0 {
		return "the source server's identity changed since the snapshot of " + name, nil
	}
	parts, err := status.LoadPartitionStats(ctx, db, x.b.dbName)
	if err != nil {
		return "", fmt.Errorf("read the index's partitions: %w", err)
	}
	if oldest := status.OldestLivePartitionHour(parts); oldest.IsZero() || oldest.After(floor) {
		return "the index no longer holds every change since the snapshot of " + name + " (older partitions were rotated out)", nil
	}
	archives, present, err := query.LoadArchivesWrittenSince(ctx, db, since)
	if err != nil {
		return "", fmt.Errorf("read the index's archives: %w", err)
	}
	if !present {
		return "the index keeps no record of its archives", nil
	}
	if n := archives.MayHoldAfter(since, cut.anchor); n > 0 {
		return fmt.Sprintf("%d archive(s) of older hours may hold changes of %s since its snapshot", n, name), nil
	}
	return "", nil
}

// pages runs opts a page at a time, by the (time, id) cursor the engine
// pages with, handing page the events up to upTo. Bounded by upTo, the
// cursor cannot skip an event: one indexed later than the read began is past
// upTo whatever its time.
func (x *indexTailSource) pages(ctx context.Context, opts query.Options, upTo uint64, page func([]query.ResultRow) error) error {
	opts.Limit = x.pageSize
	for {
		rows, err := x.b.engine.Fetch(ctx, opts)
		if err != nil {
			return fmt.Errorf("read the changes of %s.%s: %w", opts.Schema, opts.Table, err)
		}
		kept := rows[:0:0]
		for _, r := range rows {
			if r.EventID <= upTo {
				kept = append(kept, r)
			}
		}
		if len(kept) > 0 {
			if err := page(kept); err != nil {
				return err
			}
		}
		if len(rows) < x.pageSize {
			return nil
		}
		last := rows[len(rows)-1]
		opts.AfterEvent = &query.EventCursor{Timestamp: last.EventTimestamp, EventID: last.EventID}
	}
}

// Since reads the events of tables with ids in (from, upTo], in id order.
func (x *indexTailSource) Since(ctx context.Context, tables []views.BaselineTable, from, upTo uint64, page func([]query.ResultRow) error) error {
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
		if err := page(rows); err != nil {
			return err
		}
		if len(rows) < x.pageSize {
			return nil
		}
		from = rows[len(rows)-1].EventID
	}
	return nil
}

// Broken says why each table's tail cannot go on, asked at every refresh:
// a schema change placed after its cut (as copyUnchanged places them), a
// loss in capture recorded since its rows were read from the source, or a
// change of the source server since. What Fill ruled out once can happen
// after the tail was built.
func (x *indexTailSource) Broken(ctx context.Context, tables []views.BaselineTable) (map[string]string, error) {
	cuts := make([]copyCut, len(tables))
	var oldest query.BinlogPos
	var read time.Time
	any := false
	for i, t := range tables {
		cuts[i] = copyCutOf(t, x.s.copyCuts.footer)
		if cuts[i].refusal != "" {
			continue // no tail: Start refuses it
		}
		if !any || cuts[i].anchor.AtOrBefore(oldest) {
			oldest = cuts[i].anchor
		}
		if !any || cuts[i].sourceRead.Before(read) {
			read = cuts[i].sourceRead
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
	var changedServer int
	if err := x.b.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bintrail_server_changes WHERE UNIX_TIMESTAMP(detected_at) >= ?`, read.Unix()).Scan(&changedServer); err != nil {
		return nil, fmt.Errorf("read the source's identity record: %w", err)
	}
	ddl, complete, err := loadDDLAfter(ctx, x.b.db, oldest)
	if err != nil {
		return nil, fmt.Errorf("read the schema changes: %w", err)
	}
	out := map[string]string{}
	for i, t := range tables {
		if cuts[i].refusal != "" {
			continue
		}
		key := tailKey(t.Schema, t.Table)
		switch {
		case !complete:
			out[key] = "too many schema changes since the snapshot to check each one"
			continue
		case changedServer > 0:
			out[key] = "the source server's identity changed since the snapshot"
			continue
		}
		if why := tailCaptureLoss(st, cuts[i].sourceRead); why != "" {
			out[key] = why
			continue
		}
		for _, r := range ddl {
			if ddlTouches(r, t.Schema, t.Table, cuts[i].anchor) {
				out[key] = fmt.Sprintf("%s.%s had a schema change since its snapshot", t.Schema, t.Table)
				break
			}
		}
	}
	return out, nil
}

// samePrimaryKey says whether pk names the columns of the PRIMARY KEY
// createTableSQL declares (MySQL compares column names without case). As
// sets: the snapshot lists a key in its columns' order, and so does
// event.BuildPKValues, which the tail splits its keys by, while the
// definition lists it in the key's own. One it cannot read is not the same.
func samePrimaryKey(createTableSQL string, pk []metadata.ColumnMeta) bool {
	m := primaryKeyRE.FindStringSubmatch(createTableSQL)
	if m == nil {
		return false
	}
	parts := strings.Split(m[1], ",")
	if len(parts) != len(pk) {
		return false
	}
	want := map[string]bool{}
	for _, c := range pk {
		want[strings.ToLower(c.Name)] = true
	}
	for _, p := range parts {
		name := strings.TrimSpace(p)
		if len(name) < 2 || name[0] != '`' || name[len(name)-1] != '`' {
			return false
		}
		key := strings.ToLower(strings.ReplaceAll(name[1:len(name)-1], "``", "`"))
		if !want[key] {
			return false
		}
		delete(want, key)
	}
	return true
}

// primaryKeyRE is the column list of a CREATE TABLE's PRIMARY KEY, as SHOW
// CREATE TABLE prints it: backquoted names, no prefix lengths for an
// integer key (a list it cannot read refuses the tail).
var primaryKeyRE = regexp.MustCompile("(?i)PRIMARY KEY \\(((?:`(?:[^`]|``)*`\\s*,?\\s*)+)\\)")

var _ tailSource = (*indexTailSource)(nil)

// tailCaptureLoss is captureLossSince without its GTID rule, which is there
// to compare a watermark with the source: a tail compares nothing with the
// source, so a capture in position mode has one too. A loss on record since
// the table's rows were last read from the source, or a record that cannot
// be read, still refuses.
func tailCaptureLoss(st *status.StreamStateInfo, sourceRead time.Time) string {
	switch {
	case st == nil:
		return "the index has no live capture on record"
	case !st.GapColumnsPresent:
		return "the index predates the capture's record of lost events"
	case st.GapLostAt.Valid && !st.GapLostAt.Time.Before(sourceRead):
		return "capture lost events to a binlog gap since the snapshot"
	}
	skips, readable := st.ParseCaptureSkips()
	if !readable {
		return "the capture's record of dropped events is not readable"
	}
	for _, sk := range skips {
		if sk.Count > 0 && (sk.LastAt.IsZero() || !sk.LastAt.Before(sourceRead)) {
			return "capture dropped events since the snapshot"
		}
	}
	return ""
}
