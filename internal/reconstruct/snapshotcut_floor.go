package reconstruct

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// Where a search for the snapshot's cut may start (#2269).
//
// firstEventPast walks binlog_events upward by event_id and stops at the
// first event stamped past the refresh's time. A refresh targets "now", so
// nearly every event is before it and the walk reads them all: bounded to
// the partition of the current hour (#1692), it read every event the hour
// had indexed so far, and a refresh took longer with every minute of the
// hour (measured at 1.5 M events an hour, the index in 2 CPUs and a 1 GB
// buffer pool: 0.1 s at the hour's first refresh, 11.9 s at its last, on a
// refresh whose tables take 5 s; with the floor, 0.08 s and 0.41 s).
//
// What one search establishes holds for every later one. A search for the
// time T that ends on the event F (or, finding none, having seen everything
// up to the newest event N) has shown that no event below F (up to N) is
// stamped past T. An event not past T is not past any later time. So a
// search for T2 >= T may start at F (at N+1): it reads the events indexed
// since the search before it, one refresh interval's worth, whatever the
// minute of the hour.
//
// This is a statement about rows, not about clocks: it assumes nothing of
// how timestamps relate to commit order, which ResolveSnapshotCut's rule
// does not either.
//
// It rests on two things about the index. That an event_id keeps naming the
// event it named is CHECKED: the floor is remembered beside the newest event
// read with it, and used only while that row still holds that file and
// position. A row gone (the stream's cleanup after a restart deletes what
// was indexed since its checkpoint; an index emptied or rebuilt) or naming
// another event means no floor, and the search runs as it did before. So
// does a search for a time before the remembered one, and the first search
// a process makes.
//
// That no event appears later BELOW the floor is NOT checked; it holds for
// the index's own writers. Capture and `bintrail index` never name an id, so
// each row they add gets one above every row there (the counter survives a
// server restart on MySQL 8.0 and later, the supported index). The one
// writer that names ids is `bintrail restore-index`, which puts archived
// rows back under their original ids and refuses an index that is not
// empty. A restore under a running daemon that refreshes the same index is
// the case this does not cover: rows restored below a floor already
// remembered are not searched until the daemon restarts. For the same
// reason only a fold uses the floor (resolveRefreshCut); the exported
// ResolveSnapshotCut, which an export run against any index calls, never
// reads or leaves one.
//
// Kept in memory, per index (the host, port and database the connection is
// on): the watch daemon resolves a cut every refresh and is the caller this
// is for. A command-line run resolves one and gains nothing, as before.
type cutFloorStore struct {
	mu sync.Mutex
	m  map[string]cutFloor
}

// cutFloor is what one search established: no event of this index with an
// id below floor is stamped past at. witness is the newest event read with
// that search.
type cutFloor struct {
	at      time.Time
	floor   uint64
	witness newestEvent
}

var cutFloors = &cutFloorStore{m: map[string]cutFloor{}}

// remember records what a search for at established on index, unless a
// search for a later time already did. Replacing it would lose no row (what
// an earlier time's search established also holds for later times); it would
// cost the next refresh its floor, lowered to where a run for a time in the
// past (a fixed --at) happened to end.
func (s *cutFloorStore) remember(index string, at time.Time, floor uint64, witness *newestEvent) {
	if index == "" || witness == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if have, ok := s.m[index]; ok && have.at.After(at) {
		return
	}
	s.m[index] = cutFloor{at: at, floor: floor, witness: *witness}
}

// floorFor returns the event_id a search for at on index may start at, or 0
// for "from the start". Not zero only when an earlier search was for a time
// at or before at AND the event read with it is still the row it was.
func (s *cutFloorStore) floorFor(ctx context.Context, db *sql.DB, index string, at time.Time) uint64 {
	if index == "" {
		return 0
	}
	s.mu.Lock()
	have, ok := s.m[index]
	s.mu.Unlock()
	if !ok || have.at.After(at) {
		return 0
	}
	var file sql.NullString
	var end sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT binlog_file, end_pos FROM binlog_events WHERE event_id = ?`, have.witness.id).Scan(&file, &end)
	if err != nil || !file.Valid || !end.Valid || file.String != have.witness.file || uint64(end.Int64) != have.witness.end {
		// Gone, or another event under that id: nothing remembered about
		// this index holds any more.
		s.forget(index)
		return 0
	}
	return have.floor
}

func (s *cutFloorStore) forget(index string) {
	s.mu.Lock()
	delete(s.m, index)
	s.mu.Unlock()
}
