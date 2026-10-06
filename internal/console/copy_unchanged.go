package console

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
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
//     time floor and its event-id floor), and no DDL that names it, placed
//     by position alone;
//   - the index still holds that whole window (no partition of it rotated
//     away), captured it without a loss (no gap, no dropped event since the
//     table's rows were last read from the source), from the same server;
//   - the table is one capture records (not outside the capture's filters)
//     and one whose rows only change through its own events (no foreign key
//     that cascades into it: InnoDB applies a cascade without logging it);
//   - the snapshot was taken at one point in time and is not knowingly
//     incomplete.
//
// What is NOT covered:
//
//   - a write no record of exists anywhere: one made with binary logging off
//     (SET sql_log_bin = 0), and a statement-format write issued under a
//     system schema as default database. Neither reaches the index, for any
//     reader of it, and a snapshot refresh is built from the index: the copy
//     lacks such a write until the next FULL snapshot under the age rule
//     too. What this rule changes is that the copy also answers between the
//     limit and the next snapshot, so where every snapshot is a full read
//     the limit no longer caps how stale that answer is. A source that
//     filters its binlog (binlog-do-db, binlog-ignore-db) has no watermark
//     for that reason;
//   - a statement that began before the oldest partition the index still
//     holds and committed after the cut. The lookup by position starts at a
//     time floor (the engine's, #797), and a second one covers the hours
//     before it (newestEventOlderThan), so a long statement is found while
//     the hour it began in is in the index, and not after rotation drops it.

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
	// StillHolds reports whether a GTID set capture has saved (the index's
	// stream_state.gtid_set, read now) still contains what capture was
	// shown to hold at Through. A capture restarted from an earlier point
	// deletes the rows past it and saves a smaller set: until it has read
	// its way back, the index is short of what the watermark was proven
	// for. nil: not checked (a reporter that keeps no set).
	StillHolds func(savedGTIDSet string) bool
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

// copyFaultEvery is how often one server's faults are logged at Warn: the
// question is asked per statement, and a fault that lasts would otherwise
// write a line for each.
const copyFaultEvery = time.Minute

// copyFaultLog logs a fault of the question at Warn once per copyFaultEvery
// and server, and at Debug in between.
type copyFaultLog struct {
	mu   sync.Mutex
	last map[string]time.Time
}

func (l *copyFaultLog) warn(server, msg string, args ...any) {
	now := time.Now()
	l.mu.Lock()
	quiet := now.Sub(l.last[server]) < copyFaultEvery
	if !quiet {
		if l.last == nil {
			l.last = map[string]time.Time{}
		}
		l.last[server] = now
	}
	l.mu.Unlock()
	if quiet {
		slog.Debug(msg, args...)
		return
	}
	slog.Warn(msg, append(args, "note", "logged at most once a minute per server")...)
}

// copyCutMemoMax bounds the memo of footers; past it the memo starts over.
const copyCutMemoMax = 4096

// copyCut is where one table's view stops: the position a refresh would
// resume it from, and what else its files say that the question needs.
type copyCut struct {
	// anchor is the binlog coordinate, and lastEventID the id of the last
	// event of the table folded into these files (0: none on record).
	anchor      query.BinlogPos
	lastEventID uint64
	// stamp is when the files were written, on the WRITER's clock. Not a
	// time the index can be searched from: see copyLookupSince.
	stamp time.Time
	// sourceRead is when the table's rows last came from a real read of the
	// source: a dropped event older than that was read again.
	sourceRead time.Time
	// refusal says why this table cannot be vouched for, whatever the index
	// holds; "" when it can. fault is set with it when the reason is a file
	// that could not be read, which is worth a line in the log.
	refusal string
	fault   error

	// mark is the event mark beside the anchor (#2160), the zero mark when the
	// footer has none.
	mark reconstruct.EventMark
	// folded: the anchor is a refresh's cut, a position of an event the index
	// holds, rather than where the source stood when a dump read it.
	folded bool
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
		return copyCut{refusal: fmt.Sprintf("the snapshot file of %s could not be read", name), fault: fmt.Errorf("%s: %w", t.Path, err)}
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
			return copyCut{refusal: fmt.Sprintf("the snapshot files of %s could not be read", name),
				fault: fmt.Errorf("%s: %w", t.DeltaFiles[len(t.DeltaFiles)-1].Upserts, err)}
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
		stamp:       at.SnapshotTimestamp,
		lastEventID: at.LastEventID,
		sourceRead:  read.At,
		mark:        eventMarkOf(at),
		folded:      at.Producer == baseline.ProducerReconstruct,
	}
}

// copyTimeFloor is the oldest event_timestamp the lookup that starts from
// since can reach: buildQuery's own floor for a position-anchored fetch
// (since's hour, minus one more hour). Everything from there on must still
// be in the index for "no event found" to mean "no event".
func copyTimeFloor(since time.Time) time.Time {
	return since.UTC().Truncate(time.Hour).Add(-time.Hour)
}

// copyLookupSince is the time the lookup for one table starts from: an
// instant on the SOURCE's clock that no event positioned after the cut can
// have run more than the engine's margin before.
//
// The writer's stamp is not that instant. A refresh stamps its own wall
// clock, and cuts where the index stood: when capture was hours behind, the
// cut sits at an event that ran hours before the stamp, and the events
// capture indexed afterwards ran between the two. A lookup that starts at
// the stamp never sees them. So the time comes from the source's side:
//
//   - a table with a last folded event on record: that event's own
//     execution time, read from the index. Every event after it in the
//     binlog committed after it, so none ran earlier than it by more than
//     the length of its own transaction, which is the margin the engine
//     keeps. The event must still be in the index, be this table's, and sit
//     before the cut; otherwise nothing places the cut in time;
//   - a table with none: when its rows were last read from the source, a
//     dump's own start. A dump reads the source itself, so capture's lag
//     plays no part. For a file a refresh rewrote without ever folding an
//     event that is an earlier instant than the cut needs, which only
//     widens the search.
//
// The stamp still caps the result: it cannot be later than the files.
func copyLookupSince(ctx context.Context, db *sql.DB, t views.BaselineTable, cut copyCut) (since time.Time, refusal string, err error) {
	name := t.Schema + "." + t.Table
	since = cut.sourceRead
	if cut.lastEventID > 0 {
		var schema, table string
		var at query.BinlogPos
		err := db.QueryRowContext(ctx,
			`SELECT event_timestamp, schema_name, table_name, binlog_file, start_pos FROM binlog_events WHERE event_id = ?`,
			cut.lastEventID).Scan(&since, &schema, &table, &at.File, &at.Pos)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return time.Time{}, fmt.Sprintf("the index no longer holds the last change the snapshot of %s includes, so later ones cannot be placed", name), nil
		case err != nil:
			return time.Time{}, "", err
		case !strings.EqualFold(schema, t.Schema) || !strings.EqualFold(table, t.Table):
			return time.Time{}, fmt.Sprintf("the last change the snapshot of %s records is another table's in this index", name), nil
		case cut.anchor.AtOrBefore(at):
			return time.Time{}, fmt.Sprintf("the last change the snapshot of %s records is not before its binlog position in this index", name), nil
		}
	}
	if cut.stamp.Before(since) {
		since = cut.stamp
	}
	return since, "", nil
}

// copyChangedMemo remembers, per server and table, that the table changed
// after one cut. That verdict cannot become false: the cut is a fixed binlog
// position and the change is positioned after it. Without it every heavy
// statement over such a table repeats the lookups, and the one over the
// older hours can take the whole budget each time. "Unchanged" is never
// remembered: it is only true until the next change. A new cut for the table
// (a newer snapshot file) replaces the entry.
type copyChangedMemo struct {
	mu   sync.Mutex
	seen map[copyChangedKey]copyChangedAt
}

type copyChangedKey struct{ server, schema, table string }

type copyChangedAt struct {
	cut query.BinlogPos
	why string
}

// copyChangedMemoMax bounds the memo; past it the memo starts over.
const copyChangedMemoMax = 4096

func (m *copyChangedMemo) get(server string, t views.BaselineTable, cut query.BinlogPos) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if at, ok := m.seen[copyChangedKey{server, t.Schema, t.Table}]; ok && at.cut == cut {
		return at.why
	}
	return ""
}

func (m *copyChangedMemo) put(server string, t views.BaselineTable, cut query.BinlogPos, why string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seen == nil || len(m.seen) >= copyChangedMemoMax {
		m.seen = map[copyChangedKey]copyChangedAt{}
	}
	m.seen[copyChangedKey{server, t.Schema, t.Table}] = copyChangedAt{cut, why}
	return why
}

// indexBackfilled reports whether `bintrail index` ever wrote into this
// index: it is the only writer of index_state. Files indexed that way get
// the NEWEST ids whatever their position, so "the event with the highest id"
// is no longer "the event furthest into the binlog". A read that fails
// counts as yes: here that only sends the read to MySQL.
func indexBackfilled(ctx context.Context, db *sql.DB) bool {
	backfilled, err := query.IndexBackfilled(ctx, db)
	return backfilled || err != nil
}

// newestEventOlderThan is the binlog position of the event with the highest
// id among those that ran before floor, in any table; found is false when
// the index holds none.
//
// It answers, in one row, whether the slow search below is needed at all,
// and only on an index written by a stream ALONE. There ids rise with
// position over the whole index, so when the newest of the events that ran
// before floor sits before a cut, every one of them does. That is a stronger
// premise than the one Options.SinceEventID rests on (events indexed later
// have higher ids), and `bintrail index` breaks it: older binlog files loaded
// into a stream's index carry old dates, old positions and the newest ids.
// The caller does not ask this on such an index (indexBackfilled) and
// searches by position every time. The primary key leads with event_id and
// the time predicate prunes partitions, so the server reads the last row of
// each partition older than floor and nothing else.
func newestEventOlderThan(ctx context.Context, db *sql.DB, floor time.Time) (at query.BinlogPos, found bool, err error) {
	err = db.QueryRowContext(ctx,
		`SELECT binlog_file, start_pos FROM binlog_events FORCE INDEX (PRIMARY)
		  WHERE event_timestamp < ? ORDER BY event_id DESC LIMIT 1`, floor.UTC()).Scan(&at.File, &at.Pos)
	if errors.Is(err, sql.ErrNoRows) {
		return query.BinlogPos{}, false, nil
	}
	return at, err == nil, err
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
	case st.Mode != "gtid" || !st.GTIDSet.Valid || strings.TrimSpace(st.GTIDSet.String) == "":
		// The watermark compares GTID sets. A capture that is not saving
		// one now (a --reset to position mode since the watermark was
		// proven) is not the capture it was proven for, and the reporter
		// only learns that at its next read of the source.
		return "the capture is not in GTID mode, so how far it has read cannot be compared with the source"
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
	// A row with no binlog file cannot be placed: it counts as after.
	if r.at.File != "" && r.at.AtOrBefore(cut) {
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
// than that after the oldest cut is answered "cannot say".
const copyDDLScanMax = 2000

// loadDDLAfter reads the schema_changes rows positioned after cut, which is
// the oldest cut of the tables asked about (and any row with no position at
// all): the caller places each row against each table's own. By position
// and by nothing else. A row's
// detected_at is when its statement STARTED, and an ALTER that ran for hours
// before the snapshot and finished after it carries a time far below any
// time floor, while its position is after the cut. The table is small (one
// row per DDL statement and table), so reading it with no index costs
// little; "later file" is length first, then name, as in
// query.BinlogPos.
func loadDDLAfter(ctx context.Context, db *sql.DB, cut query.BinlogPos) (rows []ddlRow, complete bool, err error) {
	rs, err := db.QueryContext(ctx,
		`SELECT schema_name, table_name, ddl_query, binlog_file, binlog_pos
		   FROM schema_changes
		  WHERE binlog_file = ''
		     OR CHAR_LENGTH(binlog_file) > CHAR_LENGTH(?)
		     OR (CHAR_LENGTH(binlog_file) = CHAR_LENGTH(?) AND binlog_file > ?)
		     OR (binlog_file = ? AND binlog_pos > ?)
		  ORDER BY id LIMIT ?`, cut.File, cut.File, cut.File, cut.File, cut.Pos, copyDDLScanMax+1)
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

// captureWatermarkFor asks the reporter about server id, whose index b is a
// connection to, or says why nobody can be asked.
func (s *Server) captureWatermarkFor(ctx context.Context, id string, b *bundle) CaptureWatermark {
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
	if b != nil && b.dsn != "" && entry.DSN != b.dsn {
		// The watermark is read through the server's index as it is
		// registered now, and the events through the connection this caller
		// holds: after an edit of the index they are two indexes.
		return CaptureWatermark{Detail: "the server's index connection changed; reconnect"}
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
	// The watermark FIRST, and the index after: what capture had recorded at
	// the watermark is in the index by the time it is asked. Outside the
	// budget below: the answer is normally a remembered one, and when it is
	// not, the read of the source has its own bound.
	now := time.Now()
	wm := s.captureWatermarkFor(ctx, id, b)
	if why := captureBehind(wm, now, within); why != "" {
		return why
	}
	if len(tables) == 0 {
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, copyUnchangedTimeout)
	defer cancel()
	cuts := make([]copyCut, len(tables))
	var sourceRead time.Time
	var oldestCut query.BinlogPos
	for i, t := range tables {
		if wm.Captures != nil && !wm.Captures(t.Schema, t.Table) {
			return fmt.Sprintf("%s.%s is outside what capture records", t.Schema, t.Table)
		}
		cuts[i] = copyCutOf(t, s.copyCuts.footer)
		if cuts[i].fault != nil {
			s.copyFaults.warn(id, "read routing: a snapshot file's footer could not be read; statements over its table are not vouched for and follow the age rule",
				"server", id, "table", t.Schema+"."+t.Table, "error", cuts[i].fault)
		}
		if cuts[i].refusal != "" {
			return cuts[i].refusal
		}
		if why := s.copyChanged.get(id, t, cuts[i].anchor); why != "" {
			return why
		}
		if i == 0 || cuts[i].anchor.AtOrBefore(oldestCut) {
			oldestCut = cuts[i].anchor
		}
		if r := cuts[i].sourceRead; sourceRead.IsZero() || r.Before(sourceRead) {
			sourceRead = r
		}
	}
	unreadable := func(what string, err error) string {
		s.copyFaults.warn(id, "read routing: the index could not answer whether a statement's tables changed since their snapshot; such statements are not vouched for and follow the age rule",
			"server", id, "reading", what, "error", err)
		return "the index could not be read (" + what + ")"
	}
	st, err := status.LoadStreamState(ctx, b.db)
	if err != nil {
		return unreadable("the capture's state", err)
	}
	// A row in stream_state is also what makes the event-id floor an order:
	// a stream wrote this index (query.StreamCaptured).
	if why := captureLossSince(st, sourceRead); why != "" {
		return why
	}
	if wm.StillHolds != nil && !wm.StillHolds(st.GTIDSet.String) {
		// The reporter learns of a restart at its next read of the source,
		// up to its answer's lifetime away; the index says so now.
		return "capture's saved position no longer includes what it was confirmed to hold: it was restarted from an earlier point and has not read its way back yet"
	}
	ddl, complete, err := loadDDLAfter(ctx, b.db, oldestCut)
	if err != nil {
		return unreadable("the schema changes", err)
	}
	if !complete {
		return "too many schema changes since the snapshot to check each one"
	}
	backfilled := indexBackfilled(ctx, b.db)
	var floor time.Time
	for i, t := range tables {
		for _, r := range ddl {
			if ddlTouches(r, t.Schema, t.Table, cuts[i].anchor) {
				return fmt.Sprintf("%s.%s had a schema change since its snapshot", t.Schema, t.Table)
			}
		}
		since, refusal, err := copyLookupSince(ctx, b.db, t, cuts[i])
		if err != nil {
			return unreadable("the last change of "+t.Schema+"."+t.Table, err)
		}
		if refusal != "" {
			return refusal
		}
		if f := copyTimeFloor(since); floor.IsZero() || f.Before(floor) {
			floor = f
		}
		// Marked as searching below its own floor (#2138): the engine would
		// otherwise look at the newest row of every partition for each table
		// of each statement, to learn what the second lookup below already
		// establishes, with its own one-row proof on an index only a stream
		// wrote and by a search of every older hour otherwise.
		rows, err := b.engine.Fetch(ctx, query.Options{
			Schema: t.Schema, Table: t.Table,
			Since: &since, SincePos: &cuts[i].anchor, SinceEventID: cuts[i].lastEventID,
			Limit: 1,
		}.SearchesBelowItsOwnFloor())
		if err != nil {
			return unreadable("the events of "+t.Schema+"."+t.Table, err)
		}
		if len(rows) > 0 {
			return s.copyChanged.put(id, t, cuts[i].anchor, fmt.Sprintf("%s.%s changed since its snapshot", t.Schema, t.Table))
		}
		// The lookup above starts at a time floor, as the engine requires
		// of a search by position (#797). A statement or transaction that
		// began before that floor and committed after the cut is positioned
		// after the cut and dated before the floor: a DELETE that starts at
		// 00:30, a snapshot at 02:00, a commit at 03:00. So the hours
		// before the floor are searched too, by position alone. On an
		// index only a stream wrote that is almost never needed, which one
		// row settles: the newest event that ran before the floor is
		// positioned before the cut. On an index `bintrail index` also
		// wrote, that row says nothing, and the search always runs.
		tableFloor := copyTimeFloor(since)
		search := backfilled
		if !search {
			newest, found, err := newestEventOlderThan(ctx, b.db, tableFloor)
			if err != nil {
				return unreadable("the events older than the snapshot of "+t.Schema+"."+t.Table, err)
			}
			search = found && cuts[i].anchor.AtOrBefore(newest)
		}
		if search {
			rows, err := b.engine.Fetch(ctx, query.Options{
				Schema: t.Schema, Table: t.Table,
				Until: &tableFloor, SincePos: &cuts[i].anchor, SinceEventID: cuts[i].lastEventID,
				Limit: 1,
			})
			if err != nil {
				return unreadable("the older events of "+t.Schema+"."+t.Table, err)
			}
			if len(rows) > 0 {
				return s.copyChanged.put(id, t, cuts[i].anchor, fmt.Sprintf("%s.%s changed since its snapshot (a change that began long before it and committed after)", t.Schema, t.Table))
			}
		}
	}
	// No event after any position: that means "unchanged" only while the
	// source kept one binlog numbering (#2160).
	if why, err := numberingStartedOver(ctx, b.db, tables, cuts); err != nil {
		return unreadable("where the newest captured change sits", err)
	} else if why != "" {
		return why
	}
	// A source that was replaced since the oldest of those times: its binlog
	// files are numbered anew, and a position on one server says nothing
	// about the other. Compared as an instant, not as the session's wall
	// clock: detected_at is a TIMESTAMP, which the index server reads in its
	// own time zone.
	var changedServer int
	err = b.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bintrail_server_changes WHERE UNIX_TIMESTAMP(detected_at) >= ?`, floor.Unix()).Scan(&changedServer)
	if err != nil {
		return unreadable("the source's identity record", err)
	}
	if changedServer > 0 {
		return "the source server's identity changed since the snapshot, so binlog positions before and after cannot be compared"
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

// numberingStartedOver answers "" when nothing shows the source's binlog
// numbering started over since any of the tables' snapshots (#2160), else
// why not. After a RESET MASTER, a failover or a shorter log_bin base name,
// changes sort BELOW a snapshot's position, so "no event after the position"
// would read as "unchanged" for a table that changed.
//
// Two rules, either one enough to send the statement to MySQL:
//   - The snapshot's event mark: an event indexed after it sorts before it,
//     the check the refresh refuses on (reconstruct.CheckNumberingContinues).
//   - For a snapshot a refresh wrote, whose position is the end of an event
//     the index held: the newest event sorts before that position, or carries
//     another base name. The index only grows past an event it holds, so this
//     is a numbering that started over, or a stream briefly replaying after a
//     restart, and the cost of a false answer here is only that MySQL
//     answers. It covers what a refresh wrote before the event mark existed.
//     A dump's position is where the SOURCE stood, which capture reaches only
//     with the next change of a table it records, so it is never compared
//     this way.
func numberingStartedOver(ctx context.Context, db *sql.DB, tables []views.BaselineTable, cuts []copyCut) (string, error) {
	newest, err := reconstruct.ReadEventMark(ctx, db)
	if err != nil {
		return "", err
	}
	checked := map[reconstruct.EventMark]bool{}
	for i, t := range tables {
		at := cuts[i].anchor
		if cuts[i].folded && newest != nil && (reconstruct.BinlogBaseName(newest.File) != reconstruct.BinlogBaseName(at.File) ||
			!at.AtOrBefore(query.BinlogPos{File: newest.File, Pos: newest.End})) {
			return fmt.Sprintf("the newest change capture recorded (%s:%d) sorts before the snapshot of %s.%s (%s:%d): "+
				"the source's binary log may have started again from another numbering, so no change since can be ruled out by position",
				newest.File, newest.End, t.Schema, t.Table, at.File, at.Pos), nil
		}
		m := cuts[i].mark
		if m.ID == 0 || checked[m] {
			continue
		}
		checked[m] = true
		if err := reconstruct.CheckNumberingContinues(ctx, db, &m, at); errors.Is(err, reconstruct.ErrBinlogRenumbered) {
			return fmt.Sprintf("the source's binary log started again from another numbering after the snapshot of %s.%s was taken, so no change since can be ruled out by position", t.Schema, t.Table), nil
		} else if err != nil {
			return "", err
		}
	}
	return "", nil
}

// eventMarkOf is the footer's event mark, the zero mark when it has none.
func eventMarkOf(md baseline.DumpMetadata) reconstruct.EventMark {
	if m := reconstruct.ParseEventMark(md.EventMark); m != nil {
		return *m
	}
	return reconstruct.EventMark{}
}
