package console

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/views"
)

// Whether the tables a statement reads are unchanged since the snapshot the
// copy holds of them (#2085).
//
// Read routing sends a heavy read to the copy while the copy's snapshot is
// younger than a limit, one age for the whole server. A table nobody wrote
// since its snapshot reads the same on the copy as on MySQL whatever that
// age, so past the limit the port asks this instead of giving up: the copy
// answers when every table the statement reads is unchanged, and the
// statement goes to MySQL otherwise, as before.
//
// "Unchanged" is a claim about the source made from the index, and the index
// is only what capture delivered. Saying it of a table that did change serves
// old rows as current ones, with no error anywhere. So the claim is made only
// when ALL of these hold, and anything short of them answers "cannot say",
// which is the rule the port had before:
//
//   - capture is known to have recorded everything the source had executed at
//     some instant within the limit (CaptureWatermark). The answer is then
//     the source's as of that instant: never older than the limit allows;
//   - each table's cut is on record: the binlog position in the footer of the
//     file its view reads (the last pair of its chain of deltas, or the table
//     file), which is exactly where a refresh would resume it from;
//   - the index holds no row event of the table at or after that position,
//     asked the way the refresh asks (query.Options.SincePos, with its coarse
//     time floor and its event-id floor), and no DDL that names it;
//   - the index still holds that whole window (no partition of it rotated
//     away), captured it without a loss (no gap, no dropped event since the
//     table's rows were last read from the source), from the same server;
//   - the table is one capture records (not outside the capture's filters)
//     and one whose rows only change through its own events (no foreign key
//     that cascades into it: InnoDB applies a cascade without logging it);
//   - the snapshot was taken at one point in time and is not knowingly
//     incomplete.
//
// What is NOT covered, because no record of it exists anywhere: a write made
// with binary logging off (SET sql_log_bin = 0), and a statement-format
// write issued under a system schema as default database. Neither reaches
// the index, here or for any other reader of it.

// CaptureWatermark is what is known about how far capture has read a
// server's source.
type CaptureWatermark struct {
	// Through is the newest instant, on this host's clock, at which the
	// source was asked what it had executed and capture's saved position was
	// later seen to include all of it. Zero: not known.
	Through time.Time
	// Detail says why Through is zero. No DSN, no error text.
	Detail string
	// Captures reports whether capture records a table at all; nil means it
	// records every table. A table outside the capture's filters has no
	// event in the index however much it changes.
	Captures func(schema, table string) bool
}

// CaptureWatermarkReporter is the optional half of a CaptureStatusReporter
// that answers CaptureWatermark. The watch daemon's reporter implements it;
// a process with no reporter, or one without it, never has a watermark.
type CaptureWatermarkReporter interface {
	// CaptureWatermark answers for e, like CaptureStatus.
	CaptureWatermark(ctx context.Context, e ServerEntry) CaptureWatermark
}

// copyUnchangedTimeout bounds every read the question makes (footers, the
// index). Past it the answer is "cannot say".
const copyUnchangedTimeout = 2 * time.Second

// copyCutMemoMax bounds the memo of footers; past it the memo starts over.
const copyCutMemoMax = 4096

// copyCut is where one table's view stops: the position a refresh would
// resume it from, and what else its files say that the question needs.
type copyCut struct {
	// anchor is the binlog coordinate; since the coarse time floor beside it
	// (the writing run's stamp); lastEventID the index floor (0: none).
	anchor      query.BinlogPos
	since       time.Time
	lastEventID uint64
	// sourceRead is when the table's rows last came from a real read of the
	// source: a dropped event older than that was read again.
	sourceRead time.Time
	// refusal says why this table cannot be vouched for, whatever the index
	// holds; "" when it can.
	refusal string
}

// copyCutKey identifies the bytes a cut was read from: a snapshot's files are
// never rewritten, but the size and time cost nothing and a file replaced by
// hand must not answer from its predecessor's footer.
type copyCutKey struct {
	path          string
	size, modNano int64
}

type copyCutMemo struct {
	mu   sync.Mutex
	cuts map[copyCutKey]baseline.DumpMetadata
}

// footer reads one file's footer, from the memo when the same bytes were
// read before.
func (m *copyCutMemo) footer(path string) (baseline.DumpMetadata, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return baseline.DumpMetadata{}, err
	}
	key := copyCutKey{path, fi.Size(), fi.ModTime().UnixNano()}
	m.mu.Lock()
	md, ok := m.cuts[key]
	m.mu.Unlock()
	if ok {
		return md, nil
	}
	md, err = baseline.ReadParquetMetadata(path)
	if err != nil {
		return baseline.DumpMetadata{}, err
	}
	m.mu.Lock()
	if m.cuts == nil || len(m.cuts) >= copyCutMemoMax {
		m.cuts = map[copyCutKey]baseline.DumpMetadata{}
	}
	m.cuts[key] = md
	m.mu.Unlock()
	return md, nil
}

// cascadingForeignKey matches a foreign key whose action changes the rows of
// the table that declares it when its parent changes. InnoDB applies those
// without writing a row event for the child, so the index never sees them.
// Matched on the CREATE TABLE text: a comment holding the same words refuses
// a table that could have been answered, never the reverse.
var cascadingForeignKey = regexp.MustCompile(`(?i)\bON\s+(DELETE|UPDATE)\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT)\b`)

// copyCutOf reads where t's view stops. It mirrors the refresh's own rule
// (reconstruct.fetchFloor): the chain's last pair when the table has one,
// else the table file. Every way of not knowing is a refusal.
func copyCutOf(t views.BaselineTable, footer func(path string) (baseline.DumpMetadata, error)) copyCut {
	refuse := func(format string, args ...any) copyCut {
		return copyCut{refusal: fmt.Sprintf(format, args...)}
	}
	name := t.Schema + "." + t.Table
	base, err := footer(t.Path)
	if err != nil {
		slog.Warn("read routing: a snapshot file's footer could not be read; its table is not vouched for as unchanged",
			"table", name, "path", t.Path, "error", err)
		return refuse("the snapshot file of %s could not be read", name)
	}
	at := base
	var last *baseline.DumpMetadata
	switch {
	case t.Delta && len(t.DeltaFiles) == 0:
		// The v0.83.0 pair, or a mark with no files: nothing here says where
		// that pair stops.
		return refuse("%s has changes stored in a layout that does not record where they stop", name)
	case t.Delta:
		pair, err := footer(t.DeltaFiles[len(t.DeltaFiles)-1].Upserts)
		if err != nil {
			slog.Warn("read routing: a snapshot file's footer could not be read; its table is not vouched for as unchanged",
				"table", name, "path", t.DeltaFiles[len(t.DeltaFiles)-1].Upserts, "error", err)
			return refuse("the snapshot files of %s could not be read", name)
		}
		at, last = pair, &pair
	}
	switch {
	case at.BinlogFile == "" || at.BinlogPos <= 0:
		return refuse("the snapshot of %s does not record its binlog position", name)
	case at.SnapshotTimestamp.IsZero():
		return refuse("the snapshot of %s does not record when it was written", name)
	case base.CaptureGap != "" || at.CaptureGap != "":
		return refuse("the snapshot of %s was built across a gap in capture", name)
	case baseline.ReadConsistencyOf(base) != baseline.ReadConsistent:
		return refuse("the snapshot of %s was not taken at one point in time (its read is %s)", name, baseline.ReadConsistencyOf(base))
	case base.CreateTableSQL == "":
		return refuse("the snapshot of %s does not record the table's definition", name)
	case cascadingForeignKey.MatchString(base.CreateTableSQL):
		return refuse("%s has a foreign key that changes its rows when another table changes, with no event of its own", name)
	}
	read := baseline.ChainSourceRead(base, last)
	if !read.Known() {
		return refuse("the snapshot of %s does not record when its rows were last read from the source", name)
	}
	return copyCut{
		anchor:      query.BinlogPos{File: at.BinlogFile, Pos: uint64(at.BinlogPos)},
		since:       at.SnapshotTimestamp,
		lastEventID: at.LastEventID,
		sourceRead:  read.At,
	}
}

// copyTimeFloor is the oldest event_timestamp the lookup for a cut can
// reach: buildQuery's own floor for a position-anchored fetch (the stamp's
// hour, minus one more hour). Everything from there on must still be in the
// index for "no event found" to mean "no event".
func copyTimeFloor(since time.Time) time.Time {
	return since.UTC().Truncate(time.Hour).Add(-time.Hour)
}

// captureLossSince says why the capture's own record rules out vouching for
// tables last read from the source at sourceRead, or "": a loss on record at
// or after it, or a record that cannot be read. The rule is the snapshot
// schedule's (consoleapp.captureComparable): a dropped event is dated against
// the last read of the SOURCE, because a refresh is built from the index and
// never brings one back.
func captureLossSince(st *status.StreamStateInfo, sourceRead time.Time) string {
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

// ddlRow is one schema_changes row, as much of it as the question reads.
type ddlRow struct {
	schema, table, query string
	at                   query.BinlogPos
}

// ddlTouches reports whether r may have changed schema.table after cut. The
// names in schema_changes are as the statement typed them, so they are
// compared without case; a row with no schema (no default database, or one
// written by an older build) matches any. The statement's text is searched
// too: ALTER TABLE a RENAME TO b is recorded under a alone.
func ddlTouches(r ddlRow, schema, table string, cut query.BinlogPos) bool {
	if r.at.AtOrBefore(cut) {
		return false
	}
	if strings.EqualFold(r.table, table) && (r.schema == "" || strings.EqualFold(r.schema, schema)) {
		return true
	}
	return containsIdentifier(r.query, table)
}

// containsIdentifier reports whether text holds name as a whole identifier,
// without case.
func containsIdentifier(text, name string) bool {
	if name == "" {
		return false
	}
	lt, ln := strings.ToLower(text), strings.ToLower(name)
	isWord := func(b byte) bool {
		return b == '_' || b == '$' || b >= 0x80 || (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z')
	}
	for from := 0; ; {
		i := strings.Index(lt[from:], ln)
		if i < 0 {
			return false
		}
		start, end := from+i, from+i+len(ln)
		if (start == 0 || !isWord(lt[start-1])) && (end == len(lt) || !isWord(lt[end])) {
			return true
		}
		from = start + 1
	}
}

// copyDDLScanMax bounds the schema_changes rows one question reads; more
// than that since the oldest cut is answered "cannot say".
const copyDDLScanMax = 2000

// loadDDLSince reads the schema_changes rows detected at or after floor,
// which is a time floor only: the caller places each by position.
func loadDDLSince(ctx context.Context, db *sql.DB, floor time.Time) (rows []ddlRow, complete bool, err error) {
	rs, err := db.QueryContext(ctx,
		`SELECT schema_name, table_name, ddl_query, binlog_file, binlog_pos
		   FROM schema_changes WHERE detected_at >= ? ORDER BY id LIMIT ?`, floor, copyDDLScanMax+1)
	if err != nil {
		return nil, false, err
	}
	defer rs.Close()
	for rs.Next() {
		var r ddlRow
		if err := rs.Scan(&r.schema, &r.table, &r.query, &r.at.File, &r.at.Pos); err != nil {
			return nil, false, err
		}
		rows = append(rows, r)
	}
	if err := rs.Err(); err != nil {
		return nil, false, err
	}
	return rows, len(rows) <= copyDDLScanMax, nil
}

// captureWatermarkFor asks the reporter about server id, or says why nobody
// can be asked.
func (s *Server) captureWatermarkFor(ctx context.Context, id string) CaptureWatermark {
	reporter, ok := s.captureStatus.(CaptureWatermarkReporter)
	if !ok {
		return CaptureWatermark{Detail: "this process is not connected to the source"}
	}
	var entry ServerEntry
	if id == bootServerID {
		b, dsn := s.cm.bootInfo()
		if b == nil {
			return CaptureWatermark{Detail: "the server is not known"}
		}
		entry = ServerEntry{ID: bootServerID, DSN: dsn}
	} else {
		found := false
		if s.cm.reg != nil {
			entry, found = s.cm.reg.Get(id)
		}
		if !found {
			return CaptureWatermark{Detail: "the server is not known"}
		}
	}
	return reporter.CaptureWatermark(ctx, entry)
}

// captureBehind says why capture of server id is not known to be complete
// as of some instant in the last `within`, or "" when it is. now is the
// caller's clock reading, taken before the index is asked anything.
func captureBehind(wm CaptureWatermark, now time.Time, within time.Duration) string {
	if wm.Through.IsZero() {
		detail := wm.Detail
		if detail == "" {
			detail = "the source has not confirmed it"
		}
		return "capture is not known to be up to date (" + detail + ")"
	}
	if age := now.Sub(wm.Through); age > within {
		return fmt.Sprintf("capture was last known to be up to date %s ago, more than the limit of %s", age.Round(time.Second), within)
	}
	return ""
}

// copyUnchanged answers the question for one statement: "" when every table
// in tables is unchanged on the source since the snapshot the copy holds of
// it, as of an instant no longer ago than within; else why not. tables are
// the ones the statement's views read, from the views input that will run.
func (s *Server) copyUnchanged(ctx context.Context, b *bundle, id string, tables []views.BaselineTable, within time.Duration) string {
	ctx, cancel := context.WithTimeout(ctx, copyUnchangedTimeout)
	defer cancel()
	// The watermark FIRST, and the index after: what capture had recorded at
	// the watermark is in the index by the time it is asked.
	now := time.Now()
	wm := s.captureWatermarkFor(ctx, id)
	if why := captureBehind(wm, now, within); why != "" {
		return why
	}
	if len(tables) == 0 {
		return ""
	}
	cuts := make([]copyCut, len(tables))
	var floor, sourceRead time.Time
	for i, t := range tables {
		if wm.Captures != nil && !wm.Captures(t.Schema, t.Table) {
			return fmt.Sprintf("%s.%s is outside what capture records", t.Schema, t.Table)
		}
		cuts[i] = copyCutOf(t, s.copyCuts.footer)
		if cuts[i].refusal != "" {
			return cuts[i].refusal
		}
		if f := copyTimeFloor(cuts[i].since); floor.IsZero() || f.Before(floor) {
			floor = f
		}
		if r := cuts[i].sourceRead; sourceRead.IsZero() || r.Before(sourceRead) {
			sourceRead = r
		}
	}
	unreadable := func(what string, err error) string {
		slog.Warn("read routing: the index could not answer whether a statement's tables changed since their snapshot; the statement is not vouched for",
			"server", id, "reading", what, "error", err)
		return "the index could not be read (" + what + ")"
	}
	st, err := status.LoadStreamState(ctx, b.db)
	if err != nil {
		return unreadable("the capture's state", err)
	}
	if why := captureLossSince(st, sourceRead); why != "" {
		return why
	}
	// The event-id floor is only an order where a stream wrote the index.
	// LoadStreamState found its row, so one did.
	var changedServer int
	err = b.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bintrail_server_changes WHERE detected_at >= ?`, floor).Scan(&changedServer)
	if err != nil {
		return unreadable("the source's identity record", err)
	}
	if changedServer > 0 {
		return "the source server's identity changed since the snapshot, so binlog positions before and after cannot be compared"
	}
	ddl, complete, err := loadDDLSince(ctx, b.db, floor)
	if err != nil {
		return unreadable("the schema changes", err)
	}
	if !complete {
		return "too many schema changes since the snapshot to check each one"
	}
	for i, t := range tables {
		for _, r := range ddl {
			if ddlTouches(r, t.Schema, t.Table, cuts[i].anchor) {
				return fmt.Sprintf("%s.%s had a schema change since its snapshot", t.Schema, t.Table)
			}
		}
		since := cuts[i].since
		rows, err := b.engine.Fetch(ctx, query.Options{
			Schema: t.Schema, Table: t.Table,
			Since: &since, SincePos: &cuts[i].anchor, SinceEventID: cuts[i].lastEventID,
			Limit: 1,
		})
		if err != nil {
			return unreadable("the events of "+t.Schema+"."+t.Table, err)
		}
		if len(rows) > 0 {
			return fmt.Sprintf("%s.%s changed since its snapshot", t.Schema, t.Table)
		}
	}
	// Last, and after the lookups on purpose: rotation only ever drops the
	// oldest partitions, so a window the index still holds now is one it
	// held while it was being read. Asked first, a partition dropped between
	// the two would turn its events into "none found".
	parts, err := status.LoadPartitionStats(ctx, b.db, b.dbName)
	if err != nil {
		return unreadable("the index's partitions", err)
	}
	oldest := status.OldestLivePartitionHour(parts)
	if oldest.IsZero() || oldest.After(floor) {
		return "the index no longer holds every change since the snapshot (older partitions were rotated out), so it cannot say nothing changed"
	}
	return ""
}
