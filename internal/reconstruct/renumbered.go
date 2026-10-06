package reconstruct

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// A binlog numbering that started over (#2160).
//
// A refresh reads the changes after a snapshot by binlog POSITION: every event
// at or after the snapshot's file and offset. That holds only while the source
// keeps one numbering. A RESET MASTER (RESET BINARY LOGS AND GTIDS), a
// failover to another server, or a shorter log_bin base name starts a new one,
// and every change after it sorts BELOW the snapshot's position. A window read
// by position then looks empty, the refresh publishes the old rows, and every
// later refresh does the same: the copy stops moving and nothing says so.
//
// Capture does not always leave a trace of it. In position mode a restart
// finds its saved file gone and stamps a capture loss (stream_state.gap_lost_at),
// which already refuses the refresh. In GTID mode on MySQL the source refuses
// the stream until its new numbering passes the old one, then streams on from
// binlog.000001 with no stamp at all. A failover to a server that already
// holds the GTIDs streams on at once.
//
// The event mark is the index-side record that tells the two apart, on the
// model of the DDL mark (ddl_mark.go). binlog_events.event_id says in which
// order rows reached the index. A run records the newest row it saw; every
// event indexed after that row sorts at or after its end, while the source
// keeps one numbering. An event indexed after it that sorts BEFORE it is a
// numbering that started over, and the run refuses: positions on the two
// sides cannot be compared, so no window can be read between them. The
// remedy is a new full snapshot, anchored in the new numbering.

// ErrBinlogRenumbered: the source's binlog numbering started over since the
// snapshot a run is anchored on (a RESET MASTER, a failover to another
// server, a change of the log_bin base name), so the changes since cannot be
// found by binlog position. Remedy: a new full snapshot. Not bypassed by
// --allow-gaps: nothing was lost that a flag could accept, the window is
// unreadable.
var ErrBinlogRenumbered = errors.New("the changes since the snapshot cannot be found by binlog position")

// renumberedRemedy ends every ErrBinlogRenumbered message, in words a
// scheduled refresh and the command line share: the daemon takes the full
// snapshot by itself after a refused update.
const renumberedRemedy = "A new full snapshot is needed."

// EventMark names one binlog_events row: its id, and where it ends, so the
// check can tell a row that is gone from a row whose id now names another
// event.
type EventMark struct {
	ID   uint64 `json:"id"`
	File string `json:"binlog_file"`
	End  uint64 `json:"end_pos"`
	// ServerUUID is the server capture was reading when the mark was taken
	// (bintrail_servers.server_uuid of the stream's bintrail_id); "" when the
	// index does not say.
	ServerUUID string `json:"server_uuid,omitempty"`
}

// Encode is the mark as baseline.MetaKeyEventMark stores it.
func (m EventMark) Encode() string {
	b, _ := json.Marshal(m) // a struct of a string and two numbers: cannot fail
	return string(b)
}

// ParseEventMark reads a footer's mark. "" is no mark; one that does not read
// is no mark too, said out loud.
func ParseEventMark(raw string) *EventMark {
	return parseEventMark(raw, nil)
}

func parseEventMark(raw string, w *ReadWindow) *EventMark {
	if raw == "" {
		return nil
	}
	var m EventMark
	if err := json.Unmarshal([]byte(raw), &m); err != nil || m.ID == 0 || m.File == "" {
		w.notice(slog.LevelWarn, "baseline footer: unreadable event mark; a binlog numbering that started over is not detected from it",
			"value", raw, "error", err)
		return nil
	}
	return &m
}

// ReadEventMark reads the newest indexed event that carries a coordinate. nil
// when the index holds none.
func ReadEventMark(ctx context.Context, db *sql.DB) (*EventMark, error) {
	var m EventMark
	err := db.QueryRowContext(ctx, `SELECT event_id, binlog_file, end_pos FROM binlog_events
		WHERE binlog_file IS NOT NULL AND end_pos IS NOT NULL
		ORDER BY event_id DESC LIMIT 1`).Scan(&m.ID, &m.File, &m.End)
	switch {
	case err == nil:
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	default:
		return nil, fmt.Errorf("read the newest indexed event: %w", err)
	}
	if m.ServerUUID, err = captureServerUUID(ctx, db); err != nil {
		return nil, err
	}
	return &m, nil
}

// captureServerUUID is the server capture is reading: the server_uuid
// bintrail_servers records for the stream's bintrail_id. "" when the index
// does not say (no stream, no identity resolved, tables missing). Capture
// updates it when it reconnects to another server, so two different values
// at two moments mean two different servers, whose binary logs are numbered
// apart (#2160).
func captureServerUUID(ctx context.Context, db *sql.DB) (string, error) {
	var id sql.NullString
	err := db.QueryRowContext(ctx, `SELECT s.server_uuid FROM stream_state st
		JOIN bintrail_servers s ON s.bintrail_id = st.bintrail_id WHERE st.id = 1`).Scan(&id)
	switch {
	case err == nil:
		return id.String, nil
	case errors.Is(err, sql.ErrNoRows):
		return "", nil
	default:
		var me *mysqldriver.MySQLError
		if errors.As(err, &me) && (me.Number == 1146 || me.Number == 1054) {
			return "", nil
		}
		return "", fmt.Errorf("read the server capture is reading: %w", err)
	}
}

// ReadStreamEventMark is ReadEventMark on an index a stream wrote, encoded:
// "" anywhere else, where ascending id is not the order rows were indexed in
// and a mark would prove nothing. A failed read leaves the snapshot without
// one, which only means the next run cannot use it.
func ReadStreamEventMark(ctx context.Context, db *sql.DB) string {
	captured, err := query.StreamCaptured(ctx, db)
	if err != nil {
		slog.Warn("could not tell whether a stream wrote the index; the snapshot is published without an event mark", "error", err)
		return ""
	}
	if !captured {
		return ""
	}
	if backfilled, err := query.IndexBackfilled(ctx, db); err != nil || backfilled {
		if err != nil {
			slog.Warn("could not tell whether `bintrail index` wrote into the index; the snapshot is published without an event mark", "error", err)
		} else {
			slog.Warn("`bintrail index` also wrote into this index, so its ids do not follow the binary log; the snapshot is published without an event mark, and a binlog numbering that started over is not detected from it")
		}
		return ""
	}
	m, err := ReadEventMark(ctx, db)
	if err != nil {
		slog.Warn("could not read the newest indexed event; the snapshot is published without an event mark", "error", err)
		return ""
	}
	if m == nil {
		return ""
	}
	return m.Encode()
}

// readRunEventMark is a fold's mark. Read BEFORE the cut: the mark is a row
// at or before the cut's own, so a numbering that starts over between the two
// is an event indexed after the mark that sorts before it, which the next run
// finds. Read after the cut, such a change could sit between a cut in the old
// numbering and a mark in the new one, where no comparison sees it.
func readRunEventMark(ctx context.Context, db *sql.DB) *EventMark {
	return ParseEventMark(ReadStreamEventMark(ctx, db))
}

func encodeMark(m *EventMark) string {
	if m == nil {
		return ""
	}
	return m.Encode()
}

// markAtOrBeforePages bounds markAtOrBefore's walk: pages of 500 events,
// newest first, from the mark down to the cut.
const markAtOrBeforePages = 20

// markAtOrBefore is the mark a run stamps beside its cut: an event that ENDS
// at or before the cut. The mark read before the cut is the newest event,
// which is that event when the cut is the newest event's end. When the cut is
// the START of the first event past the run's target (a source clock ahead
// of the daemon, a fixed --at), the newest event ends after it, and the next
// run would drop such a mark as newer than its own position. The events
// between the two are the ones past the target: walked back, newest first,
// to the first that ends at or before the cut. The server the mark names is
// kept: it was read with it. No such event within the bound: no mark, said.
func markAtOrBefore(ctx context.Context, db *sql.DB, m *EventMark, cut *query.BinlogPos) *EventMark {
	if m == nil || cut == nil {
		return m
	}
	at := func(file string, end uint64) bool {
		return BinlogBaseName(file) == BinlogBaseName(cut.File) && (query.BinlogPos{File: file, Pos: end}).AtOrBefore(*cut)
	}
	if at(m.File, m.End) {
		return m
	}
	below := m.ID
	for range markAtOrBeforePages {
		rows, err := db.QueryContext(ctx, `SELECT event_id, binlog_file, end_pos FROM binlog_events
			WHERE event_id < ? AND binlog_file IS NOT NULL AND end_pos IS NOT NULL
			ORDER BY event_id DESC LIMIT 500`, below)
		if err != nil {
			slog.Warn("could not read the events before the snapshot's cut; the snapshot is published without an event mark", "error", err)
			return nil
		}
		n := 0
		var found *EventMark
		for rows.Next() {
			var e EventMark
			if err := rows.Scan(&e.ID, &e.File, &e.End); err != nil {
				rows.Close()
				slog.Warn("could not read the events before the snapshot's cut; the snapshot is published without an event mark", "error", err)
				return nil
			}
			n++
			below = e.ID
			if at(e.File, e.End) {
				e.ServerUUID = m.ServerUUID
				found = &e
				break
			}
		}
		rerr := rows.Err()
		rows.Close()
		if rerr != nil {
			slog.Warn("could not read the events before the snapshot's cut; the snapshot is published without an event mark", "error", rerr)
			return nil
		}
		if found != nil {
			return found
		}
		if n == 0 {
			break
		}
	}
	slog.Warn("no indexed event ends at or before the snapshot's cut within reach; the snapshot is published without an event mark",
		"cut", fmt.Sprintf("%s:%d", cut.File, cut.Pos))
	return nil
}

// BinlogBaseName is a binlog file name without its numeric extension:
// "binlog.000007" is "binlog". A name with no numeric extension is its own
// base name.
func BinlogBaseName(file string) string {
	i := strings.LastIndexByte(file, '.')
	if i < 0 || i == len(file)-1 {
		return file
	}
	for _, c := range file[i+1:] {
		if c < '0' || c > '9' {
			return file
		}
	}
	return file[:i]
}

// sortsBefore reports whether p is strictly before q in binlog order.
func sortsBefore(p, q query.BinlogPos) bool {
	return !q.AtOrBefore(p)
}

// indexedEvent is one row read for the check.
type indexedEvent struct {
	id   uint64
	file string
	end  uint64
}

func (e indexedEvent) String() string {
	return fmt.Sprintf("%s:%d", e.file, e.end)
}

// startedOverAfter reports whether e, indexed after the mark, shows the
// numbering started over: a different base name, or an END before the
// mark's end. Ends, not starts: every row of one binlog rows event carries
// that event's start and end, and capture may commit them in several
// batches, so a row indexed after the mark can start before the mark's end
// while belonging to the very event the mark is a row of.
func startedOverAfter(e indexedEvent, m *EventMark) bool {
	if BinlogBaseName(e.file) != BinlogBaseName(m.File) {
		return true
	}
	return sortsBefore(query.BinlogPos{File: e.file, Pos: e.end}, query.BinlogPos{File: m.File, Pos: m.End})
}

// CheckNumberingContinues returns an error wrapping ErrBinlogRenumbered when
// an event indexed after the mark sorts before it. nil mark, no event after
// it, or a mark this index can no longer vouch for: nil.
//
// Two events are read, both by primary key: the first one indexed after the
// mark, and the newest. The first catches a numbering that started over and
// has since grown past the mark; the newest catches one that started over
// later. A numbering that started over in between, and passed the mark again
// before either was read, is not seen; nor is one that started ABOVE the mark
// (a new server whose files are numbered higher), which is what the check of
// the source's identity is for (CheckSourceReplaced).
//
// The mark's own row decides whether the ids can be trusted:
//   - Present and the same event: yes.
//   - Present and another event: the index was rebuilt and its ids name
//     other rows. No check.
//   - Gone, with older rows still in the index: deleted by the resume cleanup
//     of a restarted stream, which captures those events again under new
//     ids, AT their old positions, so they sort before the mark without any
//     renumbering. No check: capture records the loss that comes with a reset
//     in that mode (gap_lost_at), and the refresh refuses on it.
//   - Gone with every older row (rotation dropped its hour): the events after
//     it are still in order. Checked.
func CheckNumberingContinues(ctx context.Context, db *sql.DB, m *EventMark, anchor query.BinlogPos) error {
	_, err := checkNumberingContinues(ctx, db, m, anchor, nil)
	return err
}

// CheckNumberingContinuesNote is CheckNumberingContinues with the note of a
// check that could not tell (uncheckedNote), for a caller that must not read
// "cannot tell" as "continues": the read router's "unchanged since the
// snapshot" check (#2186). It reads no archive and adds no note for one.
func CheckNumberingContinuesNote(ctx context.Context, db *sql.DB, m *EventMark, anchor query.BinlogPos) (string, error) {
	return checkNumberingContinues(ctx, db, m, anchor, nil)
}

// uncheckedNote is the "cannot check" note of CheckNumberingFromRead: why the
// check could not tell, and what that leaves open. Plain words with no
// command-line flag: verify, the MySQL port, cascade recovery and the MCP
// tools hand it to people and programs that cannot pass one (#2186).
func uncheckedNote(cause string) string {
	return "binlog numbering not checked: " + cause + ", so whether the source's binary log started again after the snapshot " +
		"(a RESET MASTER, a failover to another server) cannot be told, and changes made after such a restart would be missing from this result"
}

// checkNumberingContinues is CheckNumberingContinues; w, when it bounds a
// read (ReadWindow.bounded), narrows the two probes to it, and adds the
// window's archived hours (checkArchives). unchecked is the note of a check
// that could not be run (uncheckedNote), "" when it ran or had nothing to
// check.
func checkNumberingContinues(ctx context.Context, db *sql.DB, m *EventMark, anchor query.BinlogPos, w *ReadWindow) (unchecked string, err error) {
	if m == nil {
		return "", nil
	}
	// A mark is only a reference for the position it was stamped beside when
	// that position is at or after it in ONE numbering. A refresh reads its
	// mark just before its cut, so that always holds there. A full snapshot
	// read right after a RESET MASTER or a failover, before capture indexed
	// anything new, has a mark from the old numbering beside a position in
	// the new one: reading the window from that position is right, and the
	// mark says nothing about it.
	if BinlogBaseName(anchor.File) != BinlogBaseName(m.File) || sortsBefore(anchor, query.BinlogPos{File: m.File, Pos: m.End}) {
		w.notice(slog.LevelInfo, "the snapshot's event mark is from before its own binlog position's numbering; it is not used",
			"mark", m.Encode(), "anchor", fmt.Sprintf("%s:%d", anchor.File, anchor.Pos))
		return uncheckedNote("the snapshot's event mark is from an older binlog numbering than the snapshot's own position, so it says nothing about a later restart"), nil
	}
	// Rows `bintrail index` backfilled get ids above the mark with positions
	// below it, which reads exactly like a numbering that started over.
	backfilled, err := query.IndexBackfilled(ctx, db)
	if err != nil {
		return "", err
	}
	if backfilled {
		w.notice(slog.LevelWarn, "`bintrail index` also wrote into this index, so its ids do not follow the binary log; a binlog numbering that started over is not checked",
			"mark", m.Encode())
		return uncheckedNote("`bintrail index` also wrote into this index, so the order of its event ids is not the order of the binary log"), nil
	}
	var file string
	var end uint64
	// markRotated: rotation took the mark's hour out of the live index. The
	// unbounded probes below read only the live index, so a restart whose
	// changes went to the archives with it is not seen there (#2186).
	markRotated := false
	err = db.QueryRowContext(ctx, `SELECT binlog_file, end_pos FROM binlog_events WHERE event_id = ?`, m.ID).Scan(&file, &end)
	switch {
	case err == nil:
		if file != m.File || end != m.End {
			w.notice(slog.LevelWarn, "the event the snapshot's event mark names is now another event (the index was rebuilt?); a binlog numbering that started over is not checked from it",
				"mark", m.Encode(), "now", fmt.Sprintf("%s:%d", file, end))
			return uncheckedNote("the event the snapshot's event mark names is now another event (the index was rebuilt?)"), nil
		}
	case errors.Is(err, sql.ErrNoRows):
		if w.bounded() {
			// The bounded check reads the archives from the mark on, so the
			// mark must be one the archives can vouch for too (#2186).
			st, note, err := markInArchives(ctx, db, m, w.Since)
			switch {
			case err != nil:
				return "", err
			case st == markUnreadable, st == markBeyondCap:
				return note, nil
			case st == markOtherEvent:
				w.notice(slog.LevelWarn, "the event the snapshot's event mark names is now another event in the archives (the index was rebuilt?); a binlog numbering that started over is not checked from it",
					"mark", m.Encode())
				return uncheckedNote("the event the snapshot's event mark names is now another event (the index was rebuilt?)"), nil
			case st == markAbsent:
				w.notice(slog.LevelWarn, "the event the snapshot's event mark names is neither in the index nor in the archives that could hold it (a restarted stream's cleanup); a binlog numbering that started over is not checked from it",
					"mark", m.Encode())
				return uncheckedNote("the event the snapshot's event mark names was deleted from the index while older ones remain (a restarted capture's cleanup)"), nil
			case st == markArchived:
				return w.checkLiveAndArchives(ctx, db, m, anchor)
			}
		}
		var oldest sql.NullInt64
		if err := db.QueryRowContext(ctx, `SELECT MIN(event_id) FROM binlog_events`).Scan(&oldest); err != nil {
			return "", fmt.Errorf("read the oldest indexed event: %w", err)
		}
		if !oldest.Valid {
			if w.bounded() {
				// Nothing is live: the window's changes, if any, are in the
				// archives.
				floor, err := w.readFloor(ctx, db, anchor)
				if err != nil {
					return "", err
				}
				return w.checkArchives(ctx, db, m, floor)
			}
			if w.wantsArchiveNote() {
				return archivedAfterMarkNote(ctx, db, m)
			}
			return "", nil
		}
		if uint64(oldest.Int64) < m.ID {
			w.notice(slog.LevelWarn, "the event the snapshot's event mark names was deleted while older ones remain (a restarted stream's cleanup); a binlog numbering that started over is not checked from it",
				"mark", m.Encode())
			return uncheckedNote("the event the snapshot's event mark names was deleted from the index while older ones remain (a restarted capture's cleanup)"), nil
		}
		markRotated = true
	default:
		return "", fmt.Errorf("read the event the snapshot's event mark names: %w", err)
	}
	if w.bounded() {
		return w.checkLiveAndArchives(ctx, db, m, anchor)
	}
	for _, q := range []string{
		`SELECT event_id, binlog_file, end_pos FROM binlog_events
			WHERE event_id > ? AND binlog_file IS NOT NULL AND end_pos IS NOT NULL
			ORDER BY event_id ASC LIMIT 1`,
		`SELECT event_id, binlog_file, end_pos FROM binlog_events
			WHERE event_id > ? AND binlog_file IS NOT NULL AND end_pos IS NOT NULL
			ORDER BY event_id DESC LIMIT 1`,
	} {
		var e indexedEvent
		err := db.QueryRowContext(ctx, q, m.ID).Scan(&e.id, &e.file, &e.end)
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		if err != nil {
			return "", fmt.Errorf("read the events indexed after the snapshot's event mark: %w", err)
		}
		if startedOverAfter(e, m) {
			return "", startedOverErr(e, m)
		}
	}
	if markRotated && w.wantsArchiveNote() {
		return archivedAfterMarkNote(ctx, db, m)
	}
	return "", nil
}

// checkLiveAndArchives is the bounded check: the live index, then the
// window's archived hours.
func (w *ReadWindow) checkLiveAndArchives(ctx context.Context, db *sql.DB, m *EventMark, anchor query.BinlogPos) (string, error) {
	floor, err := w.readFloor(ctx, db, anchor)
	if err != nil {
		return "", err
	}
	if err := w.check(ctx, db, m, floor); err != nil {
		return "", err
	}
	return w.checkArchives(ctx, db, m, floor)
}

// archivedAfterMarkNote is the unbounded check's note when the mark's hour
// was rotated out of the live index and an archive may hold changes indexed
// after the mark (its newest event id is above the mark's, or not recorded):
// the unbounded form reads no archive, so it cannot tell (#2186). The bounded
// form reads them (checkArchives). No archive_state table: no archives, no
// note.
func archivedAfterMarkNote(ctx context.Context, db *sql.DB, m *EventMark) (string, error) {
	var one int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM archive_state WHERE max_event_id > ? OR max_event_id IS NULL LIMIT 1`, m.ID).Scan(&one)
	var me *mysqldriver.MySQLError
	if errors.As(err, &me) && me.Number == 1054 {
		// An archive_state no build with the record migrated: any archive may.
		err = db.QueryRowContext(ctx, `SELECT 1 FROM archive_state LIMIT 1`).Scan(&one)
	}
	switch {
	case err == nil:
		return uncheckedNote("the snapshot's event mark is in an hour already moved to the Parquet archives, which this form of the check does not read"), nil
	case errors.Is(err, sql.ErrNoRows):
		return "", nil
	case errors.As(err, &me) && me.Number == 1146:
		return "", nil
	}
	return "", fmt.Errorf("read archive_state: %w", err)
}

func startedOverErr(e indexedEvent, m *EventMark) error {
	return fmt.Errorf("%w: the source's binary log started again from another numbering (a RESET MASTER, a failover to another server, or a new log_bin name): a change ending at %s reached the index after the change ending at %s:%d and sorts before it. %s",
		ErrBinlogRenumbered, e, m.File, m.End, renumberedRemedy)
}

// CheckSourceReplaced returns an error wrapping ErrBinlogRenumbered when
// capture recorded, at or after since, that the source answering at its
// address is another server (a new server_uuid). The two servers' binlog files
// are numbered apart, even when the new one's numbers are higher, which the
// event mark cannot tell. A change of host, port or user keeps the same server
// and the same files, so it does not count. A zero since, or an index without
// the table: nil.
//
// MariaDB has no server_uuid: capture derives its identity from the address,
// so a failover behind the same address records nothing here.
func CheckSourceReplaced(ctx context.Context, db *sql.DB, since time.Time) error {
	return checkSourceReplaced(ctx, db, since, time.Time{})
}

// checkSourceReplaced is CheckSourceReplaced counting only changes recorded
// at or before until (zero: up to now).
func checkSourceReplaced(ctx context.Context, db *sql.DB, since, until time.Time) error {
	if since.IsZero() {
		return nil
	}
	return checkSourceReplacedSince(ctx, db, since, until)
}

// CheckNumberingFrom runs the two checks that read a snapshot's event mark
// (rawMark, its footer value) against the index, for a window read by
// position from anchor (nil: the snapshot records none): CheckNumberingContinues,
// then CheckSameServer. A snapshot without a mark: nil, whatever happened.
// Shared by every reader that starts at a snapshot's position: the refresh,
// verify and the shim's _snapshot (#2174). CheckSourceReplaced stays with the
// refresh, which alone also refuses on a snapshot without a mark.
//
// w describes the read. A zero w (the refresh, live verify) checks the whole
// index up to now. A w with Until set (a read that stops in the past: AS OF, a
// verify pair) checks only what that read can see; see ReadWindow.
func CheckNumberingFrom(ctx context.Context, db *sql.DB, anchor *query.BinlogPos, rawMark string, w ReadWindow) error {
	_, err := checkNumberingFromRead(ctx, db, anchor, rawMark, w)
	return err
}

// CheckNumberingFromRead is CheckNumberingFrom for a reader that hands its
// result to someone (#2186): verify, the shim's _snapshot, cascade recovery,
// reconstruct --at. Besides a refusal (an error wrapping ErrBinlogRenumbered)
// or a failure, it returns unchecked, a note for the result, when the check
// could not tell: the mark does not read, the index was rebuilt or backfilled
// (`bintrail index`), the mark's event was deleted while older ones remain,
// or one of the window's archived hours cannot be read. Each reader must put
// it in its result (an inconclusive verdict, a warning, an incomplete
// marker): passing it on silently is how a result is published as complete
// over a window nobody could check. "" when the check ran, or had nothing to
// check (no mark, no position, nothing indexed after the mark). A refusal or
// failure comes with no note.
func CheckNumberingFromRead(ctx context.Context, db *sql.DB, anchor *query.BinlogPos, rawMark string, w ReadWindow) (unchecked string, err error) {
	w.archiveNote = true
	return checkNumberingFromRead(ctx, db, anchor, rawMark, w)
}

// checkNumberingFromRead is CheckNumberingFromRead with w as given: a
// snapshot refresh (CheckNumberingFrom) leaves archiveNote off, so its run
// reads nothing it did not read before.
func checkNumberingFromRead(ctx context.Context, db *sql.DB, anchor *query.BinlogPos, rawMark string, w ReadWindow) (unchecked string, err error) {
	m := parseEventMark(rawMark, &w)
	positioned := anchor != nil && anchor.File != "" && anchor.Pos > 0
	if positioned {
		if m == nil && rawMark != "" {
			unchecked = uncheckedNote("the snapshot's event mark cannot be read")
		}
		note, err := checkNumberingContinues(ctx, db, m, *anchor, &w)
		if err != nil {
			return "", err
		}
		if unchecked == "" {
			unchecked = note
		}
	}
	if w.bounded() {
		err = checkSameServerUntil(ctx, db, m, w.Since, w.Until)
	} else {
		err = CheckSameServer(ctx, db, m)
	}
	if err != nil {
		return "", err
	}
	return unchecked, nil
}

// CheckSameServer returns an error wrapping ErrBinlogRenumbered when the mark
// names the server capture was reading when the snapshot was taken and
// capture now reads another one. Identities, not times: it holds for a full
// backup's position too, which the time of a change record cannot be read
// against. A mark without one (written before it was recorded) or an index
// that does not say: nil.
func CheckSameServer(ctx context.Context, db *sql.DB, m *EventMark) error {
	if m == nil || m.ServerUUID == "" {
		return nil
	}
	now, err := captureServerUUID(ctx, db)
	if err != nil || now == "" || now == m.ServerUUID {
		return err
	}
	return anotherServerErr(now, m)
}

func anotherServerErr(now string, m *EventMark) error {
	return fmt.Errorf("%w: capture now reads another server (server_uuid %s) than when the snapshot was taken (%s), and two servers number their binary logs apart. %s",
		ErrBinlogRenumbered, now, m.ServerUUID, renumberedRemedy)
}

func checkSourceReplacedSince(ctx context.Context, db *sql.DB, since, until time.Time) error {
	var oldUUID, newUUID string
	var at time.Time
	// Compared as an instant: detected_at is a TIMESTAMP the index server
	// reads in its own time zone.
	q := `SELECT old_value, new_value, detected_at FROM bintrail_server_changes
		WHERE field_changed = 'server_uuid' AND UNIX_TIMESTAMP(detected_at) >= ?`
	args := []any{since.Unix()}
	if !until.IsZero() {
		q += ` AND UNIX_TIMESTAMP(detected_at) <= ?`
		args = append(args, until.Unix())
	}
	err := db.QueryRowContext(ctx, q+` ORDER BY id DESC LIMIT 1`, args...).Scan(&oldUUID, &newUUID, &at)
	switch {
	case err == nil:
		return fmt.Errorf("%w: capture found another server answering at the source's address at %s (server_uuid %s, before it %s), and two servers number their binary logs apart. %s",
			ErrBinlogRenumbered, at.UTC().Format(time.RFC3339), newUUID, oldUUID, renumberedRemedy)
	case errors.Is(err, sql.ErrNoRows):
		return nil
	default:
		var me *mysqldriver.MySQLError
		if errors.As(err, &me) && me.Number == 1146 {
			return nil
		}
		return fmt.Errorf("read the source's identity changes: %w", err)
	}
}

// capturedBackBelow returns an error wrapping ErrBinlogRenumbered for a
// window that holds a recorded capture loss and whose cut sorts strictly
// before its anchor: capture restarted from a position lower than the one the
// snapshot already holds. That is how a position-mode stream comes back after
// a RESET MASTER (its saved file is gone, it starts at the oldest file the
// source has, which is binlog.000001 again), and when the resume cleanup took
// the event mark's own row, this is all that is left to see it by. It runs
// only where the loss was accepted (--allow-gaps): without it the loss itself
// refuses. A loss that left capture behind a full snapshot's position looks
// the same and is refused the same way, with the same remedy.
func capturedBackBelow(gap *CaptureGap, anchor, cut *query.BinlogPos) error {
	if gap == nil || gap.Unevaluable || anchor == nil || cut == nil || !sortsBefore(*cut, *anchor) {
		return nil
	}
	return fmt.Errorf("%w: capture lost events at %s and came back at %s:%d, below the snapshot's %s:%d, as it does when the source's binary log started again from another numbering (a RESET MASTER). %s",
		ErrBinlogRenumbered, gap.At.UTC().Format(time.RFC3339), cut.File, cut.Pos, anchor.File, anchor.Pos, renumberedRemedy)
}
