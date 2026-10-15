package console

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

// How far the data a statement on the copy read had been indexed (#2369).
//
// The copy is as current as the snapshot it was read from, and a snapshot is
// current to the cut of the run that PUBLISHED it, for every table in it. A
// file's own footer is not that: an unchanged table is carried into the next
// snapshot with the footer of the run that last wrote it, so a table nobody
// writes would make every statement that touches it read as days behind.
//
// No file records the publishing run's cut on its own, so it is bounded from
// the footers of the directory (copyDirMark), by a rule that can only err
// OLD: a number newer than the truth loses trust on its first miss, and one
// that is sometimes older only reads as cautious. The instant is the time the
// SOURCE ran the event the bound names, read from the index: never a file's
// stamp, which is the writer's clock and can be hours past the cut when
// capture lagged. What it says is "what capture had indexed", never "the
// source at T": nothing in this process has read the source.

// copyDirMark bounds how far one snapshot directory's data reaches, from the
// footers of its files, and returns the event mark that names the bound, or
// why there is none. The rule, from the audit of every path that places a
// file in a complete directory:
//
//   - every table of a complete refresh directory (its file and its chain)
//     reaches the run's cut, and every file a refresh wrote stops at or
//     before it: at it when written by this run, before it when carried from
//     an earlier one, since a refresh's cut only moves forward. So the
//     refresh-written file that reaches furthest is a lower bound, and the
//     mark beside its position, which the writer pinned at or before it, is
//     the one to use;
//   - a dump's file carried into a refresh directory is NOT a bound: its
//     position is where the source stood when it was dumped, which can be
//     past the run's cut, and its mark with it. So a refresh file, when there
//     is one, decides;
//   - a directory with no refresh file is a dump's, and every table in it
//     reaches at least the mark the dump read before it began, which it
//     writes on every file. Two different marks there are not one dump:
//     unknown, since an event id is not an order of time on an index that
//     `bintrail index` also wrote.
//
// Every way of not knowing is "unknown": two binlog numberings (positions
// that cannot be compared), a missing mark, a mark past its own file's
// position, a file whose writer is not recorded where it would decide.
func copyDirMark(footers []baseline.DumpMetadata) (reconstruct.EventMark, string) {
	if len(footers) == 0 {
		return reconstruct.EventMark{}, "no file of the snapshot was read"
	}
	var best *baseline.DumpMetadata
	var bestAt query.BinlogPos
	for i := range footers {
		md := &footers[i]
		if md.Producer != baseline.ProducerReconstruct || md.BinlogFile == "" || md.BinlogPos <= 0 {
			// Not a bound: a dump's position, a file of unknown origin, or
			// one that records no position. Leaving it out only lowers the
			// bound.
			continue
		}
		at := query.BinlogPos{File: md.BinlogFile, Pos: uint64(md.BinlogPos)}
		if best != nil && reconstruct.BinlogBaseName(at.File) != reconstruct.BinlogBaseName(bestAt.File) {
			return reconstruct.EventMark{}, "the snapshot's files are on two binlog numberings, so where it stops cannot be told"
		}
		if best == nil || bestAt.AtOrBefore(at) {
			best, bestAt = md, at
		}
	}
	if best != nil {
		return markWithin(*best)
	}
	var dumped reconstruct.EventMark
	for _, md := range footers {
		if md.Producer != baseline.ProducerDump {
			return reconstruct.EventMark{}, "the snapshot has a file whose footer does not say who wrote it"
		}
		m, why := markWithin(md)
		if why != "" {
			return reconstruct.EventMark{}, why
		}
		if dumped.ID != 0 && m != dumped {
			return reconstruct.EventMark{}, "the snapshot's files carry different event marks"
		}
		dumped = m
	}
	return dumped, ""
}

// markWithin is md's event mark when it sits at or before md's own position
// on the same numbering, which is how every writer pins it.
func markWithin(md baseline.DumpMetadata) (reconstruct.EventMark, string) {
	m := reconstruct.ParseEventMark(md.EventMark)
	if m == nil || m.ID == 0 {
		return reconstruct.EventMark{}, "the snapshot's footer has no event mark"
	}
	at := query.BinlogPos{File: md.BinlogFile, Pos: uint64(md.BinlogPos)}
	if md.BinlogFile == "" || reconstruct.BinlogBaseName(m.File) != reconstruct.BinlogBaseName(at.File) ||
		!(query.BinlogPos{File: m.File, Pos: m.End}).AtOrBefore(at) {
		return reconstruct.EventMark{}, "the snapshot's event mark is past its own binlog position"
	}
	return *m, ""
}

// copyReadFrom groups the files whose footers bound a statement's data, by
// snapshot directory: for each directory a table the statement read lives
// in, every table of in in that directory, its file and the last pair of its
// chain. ok is false when there is nothing to bound (the statement read no
// table of the copy) or a directory cannot be told (no Rel to strip, a copy
// in S3, whose footers this process does not read per statement).
func copyReadFrom(in views.Input, named []views.BaselineTable) (map[string][]string, bool) {
	if len(named) == 0 {
		return nil, false
	}
	out := map[string][]string{}
	for _, t := range named {
		dir, ok := copySnapshotDir(t)
		if !ok {
			return nil, false
		}
		out[dir] = nil
	}
	for _, t := range in.Baselines {
		dir, ok := copySnapshotDir(t)
		if !ok {
			continue
		}
		files, wanted := out[dir]
		if !wanted {
			continue
		}
		files = append(files, t.Path)
		if n := len(t.DeltaFiles); n > 0 {
			files = append(files, t.DeltaFiles[n-1].Upserts)
		}
		out[dir] = files
	}
	for _, files := range out {
		if len(files) == 0 {
			return nil, false
		}
	}
	return out, true
}

// copySnapshotDir is the snapshot directory t's file is in, from the layout
// every snapshot has, <dir>/<schema>/<table>.parquet, checked against t's own
// names: Rel is set only for a following view, so it cannot be relied on. A
// path of another shape is not guessed at.
func copySnapshotDir(t views.BaselineTable) (string, bool) {
	if strings.HasPrefix(t.Path, "s3://") || filepath.Base(t.Path) != t.Table+".parquet" {
		return "", false
	}
	schemaDir := filepath.Dir(t.Path)
	if filepath.Base(schemaDir) != t.Schema {
		return "", false
	}
	dir := filepath.Dir(schemaDir)
	if dir == schemaDir || dir == "." || dir == string(filepath.Separator) {
		return "", false
	}
	return dir + string(filepath.Separator), true
}

// copyMarkTime is when the source ran the event m names, read from the index
// by id, or why it cannot be said. The row must still be the event the mark
// names: ids are not reused by a stream, but an index rebuilt or loaded by
// hand can give one to another event.
func copyMarkTime(ctx context.Context, db *sql.DB, m reconstruct.EventMark) (time.Time, string) {
	var at time.Time
	var file string
	var end uint64
	err := db.QueryRowContext(ctx, `SELECT event_timestamp, binlog_file, end_pos FROM binlog_events WHERE event_id = ?`, m.ID).Scan(&at, &file, &end)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return time.Time{}, "the index no longer holds the event the snapshot was cut at"
	case err != nil:
		slog.Debug("router status: the snapshot's cut event could not be read", "event_id", m.ID, "error", err)
		return time.Time{}, "the index could not be read"
	case file != m.File || end != m.End:
		return time.Time{}, "the snapshot's cut event id names another event in this index"
	}
	return at.UTC(), ""
}

// copyIndexedThrough is the oldest of the bounds of the directories in
// files, or why one of them has none: a statement over two snapshots is as
// current as the older one.
func (s *Server) copyIndexedThrough(ctx context.Context, db *sql.DB, files map[string][]string) (time.Time, string) {
	var oldest time.Time
	for dir, paths := range files {
		footers := make([]baseline.DumpMetadata, 0, len(paths))
		for _, p := range paths {
			md, err := s.copyCuts.footer(p)
			if err != nil {
				// Leaving the file out could RAISE a dump directory's bound.
				slog.Debug("router status: a snapshot file's footer could not be read", "file", p, "error", err)
				return time.Time{}, fmt.Sprintf("a file of the snapshot %s could not be read", dir)
			}
			footers = append(footers, md)
		}
		m, why := copyDirMark(footers)
		if why != "" {
			return time.Time{}, why
		}
		at, why := copyMarkTime(ctx, db, m)
		if why != "" {
			return time.Time{}, why
		}
		if oldest.IsZero() || at.Before(oldest) {
			oldest = at
		}
	}
	if oldest.IsZero() {
		return time.Time{}, "no file of the snapshot was read"
	}
	return oldest, ""
}

// copyAnswer is what a connection's last statement was answered by: side is
// "copy", "mysql" or "refused" ("" before the first), and for the copy, the
// files that bound how far its data reached (ok false: they could not be
// told) and when it finished.
type copyAnswer struct {
	side  string
	files map[string][]string
	ok    bool
	ranAt time.Time
	// tailThrough is the oldest instant the tails it read are indexed
	// through, zero for none; allTailed says every table read had one.
	tailThrough time.Time
	allTailed   bool
}

// lastAnswerState is the SQLOnCopy's record of its connection's last answer.
//
// A run on the copy is only PENDING: the port decides what answered, after
// the copy ran. Under routing a result the router refuses goes to MySQL, and
// the router also asks the copy things no client sent (the zone probe);
// where the copy answers alone the port can still refuse a result (the row
// cap, the results held). The port says which (NoteRoute). Statements on the
// time-travel schemas are neither, as in the statement counts.
type lastAnswerState struct {
	mu      sync.Mutex
	a       copyAnswer
	pending *copyAnswer
}

// noteCopyAnswer records that the copy ran a statement on the connection,
// pending the port's decision.
func (q *SQLOnCopy) noteCopyAnswer(files map[string][]string, ok bool, ranAt, tailThrough time.Time, allTailed bool) {
	if q == nil {
		return
	}
	a := copyAnswer{side: "copy", files: files, ok: ok, ranAt: ranAt, tailThrough: tailThrough, allTailed: allTailed}
	q.last.mu.Lock()
	q.last.pending = &a
	q.last.mu.Unlock()
}

// noteCopyRefused records that the copy could not run a statement: nothing
// it ran is pending.
func (q *SQLOnCopy) noteCopyRefused() {
	if q == nil {
		return
	}
	q.last.mu.Lock()
	q.last.pending = nil
	q.last.mu.Unlock()
}

// NoteRoute records the port's decision for the connection's statement
// ("copy", "mysql" or "refused"). "copy" makes the copy's pending run the
// answer; any other answer forgets it.
func (q *SQLOnCopy) NoteRoute(side string) {
	if q == nil {
		return
	}
	q.last.mu.Lock()
	defer q.last.mu.Unlock()
	if side == "copy" && q.last.pending != nil {
		q.last.a = *q.last.pending
		if q.s != nil {
			q.s.copyAges.note(q.id, q.last.a)
		}
	} else {
		q.last.a = copyAnswer{side: side}
	}
	q.last.pending = nil
}

func (q *SQLOnCopy) lastAnswer() copyAnswer {
	if q == nil {
		return copyAnswer{}
	}
	q.last.mu.Lock()
	defer q.last.mu.Unlock()
	return q.last.a
}

// lastAnswerStatus is SHOW ROUTER STATUS's part about the connection's last
// statement. Read here, when asked, and not per statement: a statement pays
// nothing for it.
func (q *SQLOnCopy) lastAnswerStatus(ctx context.Context) [][2]string {
	a := q.lastAnswer()
	var through time.Time
	switch {
	case a.side != "copy":
	case a.allTailed:
		// Every table it read had its tail over it: the tails' instant.
		through = a.tailThrough
	case a.ok && q.s != nil && q.b != nil:
		ctx, cancel := context.WithTimeout(ctx, copyUnchangedTimeout)
		defer cancel()
		var why string
		if through, why = q.s.copyIndexedThrough(ctx, q.b.db, a.files); why != "" {
			slog.Debug("router status: how far the last statement's data was indexed is not known", "server", q.id, "reason", why)
		}
		// Tables with a tail and tables without: the older of the two.
		if !through.IsZero() && !a.tailThrough.IsZero() && a.tailThrough.Before(through) {
			through = a.tailThrough
		}
	}
	return lastAnswerRows(a, through)
}

// lastAnswerRows words a: who answered, and for the copy, the instant its
// data was indexed through and how old that was when the statement finished,
// or "unknown". Never a number for MySQL's answers. The age is the router's
// clock minus the source's, clamped at zero.
func lastAnswerRows(a copyAnswer, through time.Time) [][2]string {
	side := a.side
	if side == "" {
		side = "none"
	}
	out := [][2]string{{"last_answer", side}}
	if side != "copy" {
		return out
	}
	if through.IsZero() {
		return append(out, [2]string{"last_answer_indexed_through", "unknown"}, [2]string{"last_answer_age_seconds", "unknown"})
	}
	age := max(a.ranAt.Sub(through), 0)
	return append(out,
		[2]string{"last_answer_indexed_through", through.UTC().Format(time.RFC3339)},
		[2]string{"last_answer_age_seconds", strconv.FormatInt(int64(age.Seconds()), 10)})
}
